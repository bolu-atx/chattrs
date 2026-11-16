//! chattrs — SQLite storage for messages.
//!
//! One `Connection` behind a mutex, shared by every broker task. Each call
//! runs on a blocking thread so the broker never stalls. The broker commits
//! a row before it writes `sent`, and messages are never deleted.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::model::{Agent, LoggedMessage, Message, MAX_TOPIC_BACKLOG};

/// SQLite store for messages, safe to share across broker tasks.
#[derive(Debug, Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

/// Latest schema version, tracked with `PRAGMA user_version`. Version 1 is
/// the tables without the `topic` column; version 2 adds it.
const SCHEMA_VERSION: i64 = 2;

/// The columns of one stored message. Every query that returns messages
/// selects this list and maps it with `message_from_row`.
const MESSAGE_COLUMNS: &str = "id, sender, recipient, body, ts, reply_to, topic";

impl Store {
    /// Open (creating parent directories) or create the database at `path`.
    pub async fn open(path: PathBuf) -> Result<Store, String> {
        tokio::task::spawn_blocking(move || open_store(&path))
            .await
            .map_err(|e| format!("db task failed: {e}"))?
    }

    /// Run `f` against the connection on a blocking thread.
    async fn with<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let mut locked = conn.lock().map_err(|_| "db lock poisoned".to_string())?;
            f(&mut locked)
        })
        .await
        .map_err(|e| format!("db task failed: {e}"))?
    }

    /// Insert a message and return it with its id and timestamp filled in.
    /// A `None` topic is the general chat; a `None` recipient announces.
    pub async fn insert(
        &self,
        sender: String,
        recipient: Option<String>,
        body: String,
        reply_to: Option<u64>,
        topic: Option<String>,
    ) -> Result<Message, String> {
        self.with(move |conn| {
            let ts = now_ms();
            conn.execute(
                "INSERT INTO messages (sender, recipient, body, ts, reply_to, topic)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    sender,
                    recipient.as_deref().unwrap_or(""),
                    body,
                    ts as i64,
                    reply_to.map(|id| id as i64),
                    topic
                ],
            )
            .map_err(|e| e.to_string())?;
            Ok(Message {
                id: conn.last_insert_rowid() as u64,
                from: sender,
                to: recipient,
                body,
                ts,
                reply_to,
                topic,
            })
        })
        .await
    }

    /// Fetch one message by id, or `None` when no such id exists.
    pub async fn get(&self, id: u64) -> Result<Option<Message>, String> {
        self.with(move |conn| {
            select_first(
                conn,
                &format!("SELECT {MESSAGE_COLUMNS} FROM messages WHERE id = ?1"),
                [id as i64],
                message_from_row,
            )
        })
        .await
    }

    /// List every unread message for `recipient`, in id order.
    pub async fn inbox(&self, recipient: String) -> Result<Vec<Message>, String> {
        self.with(move |conn| {
            select_all(
                conn,
                &format!("SELECT {MESSAGE_COLUMNS} FROM messages WHERE recipient = ?1 AND acked_at IS NULL ORDER BY id"),
                [recipient],
                message_from_row,
            )
        })
        .await
    }

    /// Fetch the lowest-id message for `recipient` with this `reply_to`,
    /// whether it is acked or not. `wait` also matches acked messages.
    pub async fn find_reply(
        &self,
        recipient: String,
        reply_to: u64,
    ) -> Result<Option<Message>, String> {
        self.with(move |conn| {
            select_first(
                conn,
                &format!("SELECT {MESSAGE_COLUMNS} FROM messages WHERE recipient = ?1 AND reply_to = ?2 ORDER BY id LIMIT 1"),
                rusqlite::params![recipient, reply_to as i64],
                message_from_row,
            )
        })
        .await
    }

    /// Mark the listed ids as read when they belong to `recipient` and are
    /// unread; return the changed ids in input order. One transaction.
    pub async fn ack(&self, recipient: String, ids: Vec<u64>) -> Result<Vec<u64>, String> {
        self.with(move |conn| {
            let now = now_ms() as i64;
            let tx = conn.transaction().map_err(|e| e.to_string())?;
            let mut changed = Vec::new();
            for id in ids {
                let n = tx
                    .execute(
                        "UPDATE messages SET acked_at = ?1 WHERE id = ?2 AND recipient = ?3 AND acked_at IS NULL",
                        rusqlite::params![now, id as i64, recipient],
                    )
                    .map_err(|e| e.to_string())?;
                if n == 1 {
                    changed.push(id);
                }
            }
            tx.commit().map_err(|e| e.to_string())?;
            Ok(changed)
        })
        .await
    }

    /// Record that `address` connected now, creating its row on first `hello`.
    pub async fn touch_agent(&self, address: String) -> Result<(), String> {
        self.with(move |conn| {
            conn.execute(
                "INSERT INTO agents (address, last_seen) VALUES (?1, ?2) ON CONFLICT(address) DO UPDATE SET last_seen = excluded.last_seen",
                rusqlite::params![address, now_ms() as i64],
            )
            .map_err(|e| e.to_string())?;
            Ok(())
        })
        .await
    }

    /// List message history with id above `after`, in id order, at most
    /// `limit` rows. With `address`, only messages it sent or received.
    pub async fn log(
        &self,
        address: Option<String>,
        after: u64,
        limit: u64,
    ) -> Result<Vec<LoggedMessage>, String> {
        self.with(move |conn| {
            select_all(
                conn,
                &format!("SELECT {MESSAGE_COLUMNS}, acked_at FROM messages WHERE id > ?1 AND (?2 IS NULL OR sender = ?2 OR recipient = ?2) ORDER BY id LIMIT ?3"),
                rusqlite::params![after as i64, address, limit as i64],
                |row| {
                    Ok(LoggedMessage {
                        message: message_from_row(row)?,
                        acked_at: row.get::<_, Option<i64>>(7)?.map(|ts| ts as u64),
                    })
                },
            )
        })
        .await
    }

    /// Replay the most recent announcements per topic, merged in id
    /// order. Direct messages that carry a topic never replay.
    pub async fn topic_backlog(&self, topics: Vec<String>) -> Result<Vec<Message>, String> {
        self.with(move |conn| {
            let mut backlog = Vec::new();
            for topic in &topics {
                backlog.extend(select_all(
                    conn,
                    &format!("SELECT {MESSAGE_COLUMNS} FROM messages WHERE topic = ?1 AND recipient = '' ORDER BY id DESC LIMIT ?2"),
                    rusqlite::params![topic, MAX_TOPIC_BACKLOG as i64],
                    message_from_row,
                )?);
            }
            backlog.sort_by_key(|message| message.id);
            Ok(backlog)
        })
        .await
    }

    /// List every address that ever said `hello`, sorted by address.
    pub async fn agents(&self) -> Result<Vec<Agent>, String> {
        self.with(move |conn| {
            select_all(conn, "SELECT address, last_seen FROM agents ORDER BY address", [], |row| {
                Ok(Agent { address: row.get(0)?, last_seen: row.get::<_, i64>(1)? as u64 })
            })
        })
        .await
    }

    /// Default database path: `%APPDATA%/chattrs/chattrs.db` on Windows,
    /// else `$XDG_DATA_HOME/chattrs/chattrs.db`, else
    /// `~/.local/share/chattrs/chattrs.db`.
    pub fn default_path() -> PathBuf {
        #[cfg(windows)]
        {
            nonempty_env("APPDATA").unwrap_or_else(|| PathBuf::from(".")).join("chattrs/chattrs.db")
        }
        #[cfg(not(windows))]
        {
            if let Some(dir) = nonempty_env("XDG_DATA_HOME") {
                dir.join("chattrs/chattrs.db")
            } else {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
                PathBuf::from(home).join(".local/share/chattrs/chattrs.db")
            }
        }
    }
}

fn open_store(path: &Path) -> Result<Store, String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create db dir {}: {e}", parent.display()))?;
        }
    }
    let conn = Connection::open(path).map_err(|e| format!("open db {}: {e}", path.display()))?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
        .map_err(|e| e.to_string())?;
    migrate(&conn)?;
    Ok(Store { conn: Arc::new(Mutex::new(conn)) })
}

/// Bring the database to the latest schema: read `user_version`, apply
/// the missing steps in order, then stamp the version. Version 0 with no
/// `messages` table is new; version 0 with the table predates versioning.
fn migrate(conn: &Connection) -> Result<(), String> {
    let version: i64 =
        conn.query_row("PRAGMA user_version", [], |row| row.get(0)).map_err(|e| e.to_string())?;
    if version > SCHEMA_VERSION {
        return Err(format!("unsupported schema version {version}"));
    }
    if version == 0 {
        create_v1(conn)?;
        let mut stmt = conn.prepare("PRAGMA table_info(messages)").map_err(|e| e.to_string())?;
        let names = stmt.query_map([], |row| row.get::<_, String>(1)).map_err(|e| e.to_string())?;
        let mut missing = true;
        for name in names {
            if name.map_err(|e| e.to_string())? == "topic" {
                missing = false;
            }
        }
        if missing {
            add_topic(conn)?;
        }
    } else if version == 1 {
        add_topic(conn)?;
    }
    conn.execute(&format!("PRAGMA user_version = {SCHEMA_VERSION}"), [])
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Create the version-1 tables: messages and agents without the `topic`
/// column, plus the unread index. `IF NOT EXISTS` also repairs a database
/// from before versioning that lacks the agents table or the index.
fn create_v1(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, sender TEXT NOT NULL, recipient TEXT NOT NULL, body TEXT NOT NULL, ts INTEGER NOT NULL, reply_to INTEGER, acked_at INTEGER);
         CREATE INDEX IF NOT EXISTS messages_unread ON messages(recipient, id) WHERE acked_at IS NULL;
         CREATE TABLE IF NOT EXISTS agents (address TEXT PRIMARY KEY, last_seen INTEGER NOT NULL);",
    )
    .map_err(|e| e.to_string())
}

/// The version-2 step: add the `topic` column.
fn add_topic(conn: &Connection) -> Result<(), String> {
    conn.execute("ALTER TABLE messages ADD COLUMN topic TEXT", []).map_err(|e| e.to_string())?;
    Ok(())
}

/// Read an env var, treating a missing or blank value as absent.
fn nonempty_env(name: &str) -> Option<PathBuf> {
    let value = std::env::var(name).ok()?;
    if value.trim().is_empty() {
        return None;
    }
    Some(PathBuf::from(value))
}

/// Collect every row of a read query into a vector.
fn select_all<T>(
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
    map: fn(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> Result<Vec<T>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let rows = stmt.query_map(params, map).map_err(|e| e.to_string())?;
    rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string())
}

/// Fetch the first row of a read query, or `None` when there is none.
fn select_first<T>(
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
    map: fn(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> Result<Option<T>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let mut rows = stmt.query_map(params, map).map_err(|e| e.to_string())?;
    match rows.next() {
        None => Ok(None),
        Some(row) => row.map(Some).map_err(|e| e.to_string()),
    }
}

/// Map one message row to a `Message`. Announcements are stored with an
/// empty recipient, which never matches an inbox; map it back to `None`.
fn message_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Message> {
    let recipient: String = row.get(2)?;
    Ok(Message {
        id: row.get::<_, i64>(0)? as u64,
        from: row.get(1)?,
        to: if recipient.is_empty() { None } else { Some(recipient) },
        body: row.get(3)?,
        ts: row.get::<_, i64>(4)? as u64,
        reply_to: row.get::<_, Option<i64>>(5)?.map(|id| id as u64),
        topic: row.get(6)?,
    })
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_row(db: &Path, id: u64) -> (String, String, String) {
        rusqlite::Connection::open(db)
            .unwrap()
            .query_row(
                "SELECT sender, recipient, body FROM messages WHERE id = ?1",
                [id as i64],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    }

    #[test]
    fn nonempty_env_ignores_missing_and_blank_values() {
        std::env::remove_var("CHATTRS_TEST_PROBE_DIR");
        assert_eq!(nonempty_env("CHATTRS_TEST_PROBE_DIR"), None);
        std::env::set_var("CHATTRS_TEST_PROBE_DIR", "   ");
        assert_eq!(nonempty_env("CHATTRS_TEST_PROBE_DIR"), None);
        std::env::set_var("CHATTRS_TEST_PROBE_DIR", "/tmp/probe");
        assert_eq!(nonempty_env("CHATTRS_TEST_PROBE_DIR"), Some(PathBuf::from("/tmp/probe")));
        std::env::remove_var("CHATTRS_TEST_PROBE_DIR");
    }

    #[cfg(windows)]
    #[test]
    fn default_path_uses_appdata_on_windows() {
        std::env::set_var("APPDATA", "C:\\Users\\tester\\AppData\\Roaming");
        assert_eq!(
            Store::default_path(),
            PathBuf::from("C:\\Users\\tester\\AppData\\Roaming").join("chattrs/chattrs.db")
        );
        std::env::remove_var("APPDATA");
    }

    #[tokio::test]
    async fn inbox_lists_unread_in_id_order_and_ack_marks_read() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let store = Store::open(db.clone()).await.unwrap();
        let first = store
            .insert("a@h".into(), Some("b@h".into()), "one".into(), None, None)
            .await
            .unwrap()
            .id;
        let second = store
            .insert("a@h".into(), Some("b@h".into()), "two".into(), None, None)
            .await
            .unwrap()
            .id;
        let other = store
            .insert("a@h".into(), Some("c@h".into()), "other".into(), None, None)
            .await
            .unwrap()
            .id;

        let messages = store.inbox("b@h".into()).await.unwrap();
        let ids: Vec<u64> = messages.iter().map(|message| message.id).collect();
        assert_eq!(ids, vec![first, second]);
        assert_eq!(messages[0].from, "a@h");
        assert_eq!(messages[0].body, "one");

        let changed = store.ack("b@h".into(), vec![first, other]).await.unwrap();
        assert_eq!(changed, vec![first]);

        let messages = store.inbox("b@h".into()).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, second);
        let kept = store.inbox("c@h".into()).await.unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].id, other);
    }

    #[tokio::test]
    async fn insert_persists_a_row_across_reopens() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("sub").join("chattrs.db");
        let store = Store::open(db.clone()).await.unwrap();
        let stored =
            store.insert("a@h".into(), Some("b@h".into()), "hi".into(), None, None).await.unwrap();
        let id = stored.id;
        assert_eq!(id, 1);
        assert_eq!(stored.from, "a@h");
        assert_eq!(stored.to.as_deref(), Some("b@h"));
        assert_eq!(stored.body, "hi");
        assert!(stored.ts > 0);
        assert_eq!(read_row(&db, id), ("a@h".into(), "b@h".into(), "hi".into()));
        drop(store);

        let store = Store::open(db.clone()).await.unwrap();
        let next = store
            .insert("b@h".into(), Some("a@h".into()), "yo".into(), Some(id), None)
            .await
            .unwrap()
            .id;
        assert_eq!(next, id + 1);
        assert_eq!(read_row(&db, id), ("a@h".into(), "b@h".into(), "hi".into()));
    }

    #[tokio::test]
    async fn topic_backlog_returns_most_recent_500_per_topic_in_id_order() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let store = Store::open(db.clone()).await.unwrap();
        let mut first_kept = 0;
        for i in 0..crate::model::MAX_TOPIC_BACKLOG + 5 {
            let id = store
                .insert("a@h".into(), None, format!("note {i}"), None, Some("release-26".into()))
                .await
                .unwrap()
                .id;
            if i == 5 {
                first_kept = id;
            }
            if i >= 5 {
                assert!(id >= first_kept, "ids grow with send order");
            }
        }
        let other = store
            .insert("a@h".into(), None, "other room".into(), None, Some("other-topic".into()))
            .await
            .unwrap()
            .id;
        let direct = store
            .insert(
                "a@h".into(),
                Some("b@h".into()),
                "direct".into(),
                None,
                Some("release-26".into()),
            )
            .await
            .unwrap()
            .id;

        let backlog =
            store.topic_backlog(vec!["release-26".into(), "other-topic".into()]).await.unwrap();
        assert_eq!(backlog.len(), crate::model::MAX_TOPIC_BACKLOG as usize + 1);
        let ids: Vec<u64> = backlog.iter().map(|message| message.id).collect();
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]), "backlog is in id order");
        assert_eq!(ids[0], first_kept, "only the most recent 500 replay per topic");
        assert!(ids.contains(&other), "the other room replays too");
        assert!(!ids.contains(&direct), "a direct message never replays as an announcement");
        for message in &backlog {
            assert_eq!(message.to, None, "backlog rows belong to no inbox");
        }
    }

    #[tokio::test]
    async fn old_schema_database_opens_and_migrates() {
        for stamp in [0, 1] {
            let dir = tempfile::TempDir::new().unwrap();
            let db = dir.path().join("chattrs.db");
            {
                let conn = rusqlite::Connection::open(&db).unwrap();
                conn.execute_batch(
                    "CREATE TABLE messages (
                       id        INTEGER PRIMARY KEY AUTOINCREMENT,
                       sender    TEXT NOT NULL,
                       recipient TEXT NOT NULL,
                       body      TEXT NOT NULL,
                       ts        INTEGER NOT NULL,
                       reply_to  INTEGER,
                       acked_at  INTEGER
                     );
                     CREATE TABLE agents (
                       address   TEXT PRIMARY KEY,
                       last_seen INTEGER NOT NULL
                     );",
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO messages (sender, recipient, body, ts, reply_to, acked_at)
                     VALUES ('a@h', 'b@h', 'old', 1763200000000, NULL, NULL)",
                    [],
                )
                .unwrap();
                conn.execute(&format!("PRAGMA user_version = {stamp}"), []).unwrap();
            }
            let store = Store::open(db.clone()).await.unwrap();
            let old = store.get(1).await.unwrap().expect("old row survives");
            assert_eq!(old.body, "old");
            assert_eq!(old.topic, None, "old rows read back with no topic");
            let id = store
                .insert(
                    "a@h".into(),
                    Some("b@h".into()),
                    "new".into(),
                    None,
                    Some("release-26".into()),
                )
                .await
                .unwrap()
                .id;
            let fresh = store.get(id).await.unwrap().unwrap();
            assert_eq!(fresh.topic.as_deref(), Some("release-26"));
            let version: i64 = rusqlite::Connection::open(&db)
                .unwrap()
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(version, 2, "open stamps the latest version");
        }

        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.execute_batch(
                "CREATE TABLE messages (
                   id        INTEGER PRIMARY KEY AUTOINCREMENT,
                   sender    TEXT NOT NULL,
                   recipient TEXT NOT NULL,
                   body      TEXT NOT NULL,
                   ts        INTEGER NOT NULL,
                   reply_to  INTEGER,
                   topic     TEXT,
                   acked_at  INTEGER
                 );",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO messages (sender, recipient, body, ts, reply_to, topic, acked_at)
                 VALUES ('a@h', 'b@h', 'old', 1763200000000, NULL, 'release-26', NULL)",
                [],
            )
            .unwrap();
        }
        let store = Store::open(db.clone()).await.unwrap();
        let old = store.get(1).await.unwrap().expect("old row survives");
        assert_eq!(old.topic.as_deref(), Some("release-26"), "a migrated row keeps its topic");
        let inbox = store.inbox("b@h".into()).await.unwrap();
        assert_eq!(inbox.len(), 1, "the row still belongs to its inbox");
    }
}
