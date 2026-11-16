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

impl Store {
    /// Open (creating parent directories) or create the database at `path`.
    pub async fn open(path: PathBuf) -> Result<Store, String> {
        tokio::task::spawn_blocking(move || open_blocking(&path))
            .await
            .map_err(|e| format!("db task failed: {e}"))?
    }

    /// Insert a message and return it, with its id and timestamp filled
    /// in. The id grows with send order. A `None` topic is the general
    /// chat. A `None` recipient is a topic announcement: it is stored with
    /// no owning inbox and survives a restart like any message.
    pub async fn insert(
        &self,
        sender: String,
        recipient: Option<String>,
        body: String,
        reply_to: Option<u64>,
        topic: Option<String>,
    ) -> Result<Message, String> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            insert_blocking(&conn, &sender, recipient.as_deref(), &body, reply_to, topic.as_deref())
        })
        .await
        .map_err(|e| format!("db task failed: {e}"))?
    }

    /// Fetch one message by id, or `None` when no such id exists.
    pub async fn get(&self, id: u64) -> Result<Option<Message>, String> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || get_blocking(&conn, id))
            .await
            .map_err(|e| format!("db task failed: {e}"))?
    }

    /// List every unread message for `recipient`, in id order.
    pub async fn inbox(&self, recipient: String) -> Result<Vec<Message>, String> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || inbox_blocking(&conn, &recipient))
            .await
            .map_err(|e| format!("db task failed: {e}"))?
    }

    /// Fetch the lowest-id message for `recipient` with this `reply_to`,
    /// whether it is acked or not. Used by `wait`, which also matches
    /// messages that `inbox` no longer lists.
    pub async fn find_reply(
        &self,
        recipient: String,
        reply_to: u64,
    ) -> Result<Option<Message>, String> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || find_reply_blocking(&conn, &recipient, reply_to))
            .await
            .map_err(|e| format!("db task failed: {e}"))?
    }

    /// Mark the listed ids as read when they belong to `recipient` and are
    /// unread, and return the ids that changed. Other ids are ignored.
    pub async fn ack(&self, recipient: String, ids: Vec<u64>) -> Result<Vec<u64>, String> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || ack_blocking(&conn, &recipient, &ids))
            .await
            .map_err(|e| format!("db task failed: {e}"))?
    }

    /// Record that `address` connected now on the broker clock, creating
    /// its row when this is its first `hello`.
    pub async fn touch_agent(&self, address: String) -> Result<(), String> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || touch_agent_blocking(&conn, &address))
            .await
            .map_err(|e| format!("db task failed: {e}"))?
    }

    /// List message history with id above `after`, in id order, at most
    /// `limit` rows. With `address`, only messages it sent or received.
    /// Reading history never acks.
    pub async fn log(
        &self,
        address: Option<String>,
        after: u64,
        limit: u64,
    ) -> Result<Vec<LoggedMessage>, String> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || log_blocking(&conn, address.as_deref(), after, limit))
            .await
            .map_err(|e| format!("db task failed: {e}"))?
    }

    /// Replay the most recent announcements per topic, merged in id
    /// order, at most `MAX_TOPIC_BACKLOG` rows per topic. Direct
    /// messages that merely carry a topic are never announcements.
    pub async fn topic_backlog(&self, topics: Vec<String>) -> Result<Vec<Message>, String> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || topic_backlog_blocking(&conn, &topics))
            .await
            .map_err(|e| format!("db task failed: {e}"))?
    }

    /// List every address that ever said `hello`, sorted by address.
    pub async fn agents(&self) -> Result<Vec<Agent>, String> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || agents_blocking(&conn))
            .await
            .map_err(|e| format!("db task failed: {e}"))?
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

fn open_blocking(path: &Path) -> Result<Store, String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create db dir {}: {e}", parent.display()))?;
        }
    }
    let conn = Connection::open(path).map_err(|e| format!("open db {}: {e}", path.display()))?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=FULL;
         CREATE TABLE IF NOT EXISTS messages (
           id        INTEGER PRIMARY KEY AUTOINCREMENT,
           sender    TEXT NOT NULL,
           recipient TEXT NOT NULL,
           body      TEXT NOT NULL,
           ts        INTEGER NOT NULL,
           reply_to  INTEGER,
           topic     TEXT,
           acked_at  INTEGER
         );
         CREATE INDEX IF NOT EXISTS messages_unread
           ON messages(recipient, id) WHERE acked_at IS NULL;
         CREATE TABLE IF NOT EXISTS agents (
           address   TEXT PRIMARY KEY,
           last_seen INTEGER NOT NULL
         );",
    )
    .map_err(|e| e.to_string())?;
    if !has_topic_column(&conn)? {
        conn.execute("ALTER TABLE messages ADD COLUMN topic TEXT", [])
            .map_err(|e| e.to_string())?;
    }
    Ok(Store { conn: Arc::new(Mutex::new(conn)) })
}

/// True when the `messages` table already has a `topic` column.
/// Databases created before the topic story have no column; the broker
/// adds it on open so old rows read back with a null topic.
fn has_topic_column(conn: &Connection) -> Result<bool, String> {
    let mut stmt = conn.prepare("PRAGMA table_info(messages)").map_err(|e| e.to_string())?;
    let names = stmt.query_map([], |row| row.get::<_, String>(1)).map_err(|e| e.to_string())?;
    for name in names {
        if name.map_err(|e| e.to_string())? == "topic" {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Read an env var, treating a missing or blank value as absent.
fn nonempty_env(name: &str) -> Option<PathBuf> {
    let value = std::env::var(name).ok()?;
    if value.trim().is_empty() {
        return None;
    }
    Some(PathBuf::from(value))
}

fn lock(conn: &Arc<Mutex<Connection>>) -> Result<std::sync::MutexGuard<'_, Connection>, String> {
    conn.lock().map_err(|_| "db lock poisoned".to_string())
}

/// Announcements have no owning inbox, so they are stored with an empty
/// recipient. No valid address is empty, so they never match an inbox.
/// Readers map the empty recipient back to `None`.
fn insert_blocking(
    conn: &Arc<Mutex<Connection>>,
    sender: &str,
    recipient: Option<&str>,
    body: &str,
    reply_to: Option<u64>,
    topic: Option<&str>,
) -> Result<Message, String> {
    let conn = lock(conn)?;
    let ts = now_ms();
    conn.execute(
        "INSERT INTO messages (sender, recipient, body, ts, reply_to, topic)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![
            sender,
            recipient.unwrap_or(""),
            body,
            ts as i64,
            reply_to.map(|id| id as i64),
            topic
        ],
    )
    .map_err(|e| e.to_string())?;
    Ok(Message {
        id: conn.last_insert_rowid() as u64,
        from: sender.to_string(),
        to: recipient.map(|recipient| recipient.to_string()),
        body: body.to_string(),
        ts,
        reply_to,
        topic: topic.map(|topic| topic.to_string()),
    })
}

/// Map a stored recipient back to the wire: the empty string means the
/// row is an announcement with no owning inbox.
fn read_recipient(raw: String) -> Option<String> {
    if raw.is_empty() {
        None
    } else {
        Some(raw)
    }
}

fn get_blocking(conn: &Arc<Mutex<Connection>>, id: u64) -> Result<Option<Message>, String> {
    let conn = lock(conn)?;
    let mut stmt = conn
        .prepare(
            "SELECT id, sender, recipient, body, ts, reply_to, topic FROM messages
             WHERE id = ?1",
        )
        .map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query_map([id as i64], |row| {
            Ok(Message {
                id: row.get::<_, i64>(0)? as u64,
                from: row.get(1)?,
                to: read_recipient(row.get(2)?),
                body: row.get(3)?,
                ts: row.get::<_, i64>(4)? as u64,
                reply_to: row.get::<_, Option<i64>>(5)?.map(|id| id as u64),
                topic: row.get(6)?,
            })
        })
        .map_err(|e| e.to_string())?;
    match rows.next() {
        None => Ok(None),
        Some(row) => row.map(Some).map_err(|e| e.to_string()),
    }
}

fn inbox_blocking(conn: &Arc<Mutex<Connection>>, recipient: &str) -> Result<Vec<Message>, String> {
    let conn = lock(conn)?;
    let mut stmt = conn
        .prepare(
            "SELECT id, sender, recipient, body, ts, reply_to, topic FROM messages
             WHERE recipient = ?1 AND acked_at IS NULL ORDER BY id",
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([recipient], |row| {
            Ok(Message {
                id: row.get::<_, i64>(0)? as u64,
                from: row.get(1)?,
                to: read_recipient(row.get(2)?),
                body: row.get(3)?,
                ts: row.get::<_, i64>(4)? as u64,
                reply_to: row.get::<_, Option<i64>>(5)?.map(|id| id as u64),
                topic: row.get(6)?,
            })
        })
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string())
}

fn find_reply_blocking(
    conn: &Arc<Mutex<Connection>>,
    recipient: &str,
    reply_to: u64,
) -> Result<Option<Message>, String> {
    let conn = lock(conn)?;
    let mut stmt = conn
        .prepare(
            "SELECT id, sender, recipient, body, ts, reply_to, topic FROM messages
             WHERE recipient = ?1 AND reply_to = ?2 ORDER BY id LIMIT 1",
        )
        .map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query_map(rusqlite::params![recipient, reply_to as i64], |row| {
            Ok(Message {
                id: row.get::<_, i64>(0)? as u64,
                from: row.get(1)?,
                to: read_recipient(row.get(2)?),
                body: row.get(3)?,
                ts: row.get::<_, i64>(4)? as u64,
                reply_to: row.get::<_, Option<i64>>(5)?.map(|id| id as u64),
                topic: row.get(6)?,
            })
        })
        .map_err(|e| e.to_string())?;
    match rows.next() {
        None => Ok(None),
        Some(row) => row.map(Some).map_err(|e| e.to_string()),
    }
}

fn ack_blocking(
    conn: &Arc<Mutex<Connection>>,
    recipient: &str,
    ids: &[u64],
) -> Result<Vec<u64>, String> {
    let conn = lock(conn)?;
    let mut changed = Vec::new();
    for id in ids {
        let n = conn
            .execute(
                "UPDATE messages SET acked_at = ?1
                 WHERE id = ?2 AND recipient = ?3 AND acked_at IS NULL",
                rusqlite::params![now_ms() as i64, *id as i64, recipient],
            )
            .map_err(|e| e.to_string())?;
        if n == 1 {
            changed.push(*id);
        }
    }
    Ok(changed)
}

fn log_blocking(
    conn: &Arc<Mutex<Connection>>,
    address: Option<&str>,
    after: u64,
    limit: u64,
) -> Result<Vec<LoggedMessage>, String> {
    let conn = lock(conn)?;
    let read = |row: &rusqlite::Row<'_>| {
        Ok(LoggedMessage {
            id: row.get::<_, i64>(0)? as u64,
            from: row.get(1)?,
            to: read_recipient(row.get(2)?),
            body: row.get(3)?,
            ts: row.get::<_, i64>(4)? as u64,
            reply_to: row.get::<_, Option<i64>>(5)?.map(|id| id as u64),
            topic: row.get(6)?,
            acked_at: row.get::<_, Option<i64>>(7)?.map(|ts| ts as u64),
        })
    };
    if let Some(address) = address {
        let mut stmt = conn
            .prepare(
                "SELECT id, sender, recipient, body, ts, reply_to, topic, acked_at
                 FROM messages
                 WHERE id > ?1 AND (sender = ?2 OR recipient = ?2)
                 ORDER BY id LIMIT ?3",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map(rusqlite::params![after as i64, address, limit as i64], read)
            .map_err(|e| e.to_string())?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string())
    } else {
        let mut stmt = conn
            .prepare(
                "SELECT id, sender, recipient, body, ts, reply_to, topic, acked_at
                 FROM messages
                 WHERE id > ?1 ORDER BY id LIMIT ?2",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map(rusqlite::params![after as i64, limit as i64], read)
            .map_err(|e| e.to_string())?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string())
    }
}

fn topic_backlog_blocking(
    conn: &Arc<Mutex<Connection>>,
    topics: &[String],
) -> Result<Vec<Message>, String> {
    let conn = lock(conn)?;
    let mut backlog = Vec::new();
    for topic in topics {
        let mut stmt = conn
            .prepare(
                "SELECT id, sender, recipient, body, ts, reply_to, topic FROM messages
                 WHERE topic = ?1 AND recipient = '' ORDER BY id DESC LIMIT ?2",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map(rusqlite::params![topic, MAX_TOPIC_BACKLOG as i64], |row| {
                Ok(Message {
                    id: row.get::<_, i64>(0)? as u64,
                    from: row.get(1)?,
                    to: read_recipient(row.get(2)?),
                    body: row.get(3)?,
                    ts: row.get::<_, i64>(4)? as u64,
                    reply_to: row.get::<_, Option<i64>>(5)?.map(|id| id as u64),
                    topic: row.get(6)?,
                })
            })
            .map_err(|e| e.to_string())?;
        for row in rows {
            backlog.push(row.map_err(|e| e.to_string())?);
        }
    }
    backlog.sort_by_key(|message| message.id);
    Ok(backlog)
}

fn touch_agent_blocking(conn: &Arc<Mutex<Connection>>, address: &str) -> Result<(), String> {
    let conn = lock(conn)?;
    conn.execute(
        "INSERT INTO agents (address, last_seen) VALUES (?1, ?2)
         ON CONFLICT(address) DO UPDATE SET last_seen = excluded.last_seen",
        rusqlite::params![address, now_ms() as i64],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn agents_blocking(conn: &Arc<Mutex<Connection>>) -> Result<Vec<Agent>, String> {
    let conn = lock(conn)?;
    let mut stmt = conn
        .prepare("SELECT address, last_seen FROM agents ORDER BY address")
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |row| {
            Ok(Agent { address: row.get(0)?, last_seen: row.get::<_, i64>(1)? as u64 })
        })
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string())
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
}
