//! chattrs — SQLite storage for messages.
//!
//! One `Connection` behind a mutex, shared by every broker task. Each call
//! runs on a blocking thread so the broker never stalls. The broker commits
//! a row before it writes `sent`, and messages are never deleted.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::model::{Agent, LoggedMessage, Message, ProjectInfo, ALL_PROJECTS, MAX_TOPIC_BACKLOG};

/// SQLite store for messages, safe to share across broker tasks.
#[derive(Debug, Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

/// Latest schema version, tracked with `PRAGMA user_version`. Version 1 is
/// the tables without the `topic` column; version 2 adds it; version 3 adds
/// the `tags` column to `agents`; version 4 adds `state` and
/// `state_at`; version 5 adds `project` to both tables and keys `agents`
/// by (project, address); version 6 adds `from_role` to `messages`.
const SCHEMA_VERSION: i64 = 6;

/// The columns of one stored message. Every query that returns messages
/// selects this list and maps it with `message_from_row`.
const MESSAGE_COLUMNS: &str =
    "id, sender, recipient, body, ts, reply_to, topic, project, from_role";

/// Who sends a message: the project and address of the connection, and
/// the role its token carries, if the broker has a token table.
#[derive(Debug, Clone)]
pub struct Sender {
    pub project: String,
    pub address: String,
    pub role: Option<String>,
}

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
        sender: Sender,
        recipient: Option<String>,
        body: String,
        reply_to: Option<u64>,
        topic: Option<String>,
    ) -> Result<Message, String> {
        self.with(move |conn| {
            let ts = now_ms();
            let Sender { project, address: sender, role: from_role } = sender;
            conn.execute(
                "INSERT INTO messages (sender, recipient, body, ts, reply_to, topic, project, from_role)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    sender,
                    recipient.as_deref().unwrap_or(""),
                    body,
                    ts as i64,
                    reply_to.map(|id| id as i64),
                    topic,
                    project,
                    from_role
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
                project,
                from_role,
            })
        })
        .await
    }

    /// Fetch one message of `project` by id, or `None` when the project has
    /// no such id.
    pub async fn get(&self, project: String, id: u64) -> Result<Option<Message>, String> {
        self.with(move |conn| {
            select_first(
                conn,
                &format!("SELECT {MESSAGE_COLUMNS} FROM messages WHERE id = ?1 AND project = ?2"),
                rusqlite::params![id as i64, project],
                message_from_row,
            )
        })
        .await
    }

    /// List every unread message for `recipient`, in id order.
    pub async fn inbox(&self, project: String, recipient: String) -> Result<Vec<Message>, String> {
        self.with(move |conn| {
            select_all(
                conn,
                &format!("SELECT {MESSAGE_COLUMNS} FROM messages WHERE project = ?1 AND recipient = ?2 AND acked_at IS NULL ORDER BY id"),
                rusqlite::params![project, recipient],
                message_from_row,
            )
        })
        .await
    }

    /// Fetch the lowest-id message for `recipient` with this `reply_to`,
    /// whether it is acked or not. `wait` also matches acked messages.
    pub async fn find_reply(
        &self,
        project: String,
        recipient: String,
        reply_to: u64,
    ) -> Result<Option<Message>, String> {
        self.with(move |conn| {
            select_first(
                conn,
                &format!("SELECT {MESSAGE_COLUMNS} FROM messages WHERE project = ?1 AND recipient = ?2 AND reply_to = ?3 ORDER BY id LIMIT 1"),
                rusqlite::params![project, recipient, reply_to as i64],
                message_from_row,
            )
        })
        .await
    }

    /// Mark the listed ids as read when they belong to `recipient` and are
    /// unread; return the changed ids in input order. One transaction.
    pub async fn ack(
        &self,
        project: String,
        recipient: String,
        ids: Vec<u64>,
    ) -> Result<Vec<u64>, String> {
        self.with(move |conn| {
            let now = now_ms() as i64;
            let tx = conn.transaction().map_err(|e| e.to_string())?;
            let mut changed = Vec::new();
            for id in ids {
                let n = tx
                    .execute(
                        "UPDATE messages SET acked_at = ?1 WHERE id = ?2 AND project = ?3 AND recipient = ?4 AND acked_at IS NULL",
                        rusqlite::params![now, id as i64, project, recipient],
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
    /// Non-empty `tags` replace the stored tags; empty `tags` keep them. A
    /// `state` replaces the stored state and stamps `state_at`; `None`
    /// keeps both.
    pub async fn touch_agent(
        &self,
        project: String,
        address: String,
        tags: Vec<String>,
        state: Option<String>,
    ) -> Result<(), String> {
        let tags_json = if tags.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&tags).map_err(|e| e.to_string())?)
        };
        self.with(move |conn| {
            let now = now_ms() as i64;
            let state_at = state.as_ref().map(|_| now);
            conn.execute(
                "INSERT INTO agents (project, address, last_seen, tags, state, state_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(project, address) DO UPDATE SET last_seen = excluded.last_seen, tags = COALESCE(excluded.tags, tags), state = COALESCE(excluded.state, state), state_at = COALESCE(excluded.state_at, state_at)",
                rusqlite::params![project, address, now, tags_json, state, state_at],
            )
            .map_err(|e| e.to_string())?;
            Ok(())
        })
        .await
    }

    /// Whether `address` has said `hello` in `project` at least once.
    pub async fn is_known_agent(&self, project: String, address: String) -> Result<bool, String> {
        self.with(move |conn| {
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM agents WHERE project = ?1 AND address = ?2)",
                rusqlite::params![project, address],
                |row| row.get(0),
            )
            .map_err(|e| e.to_string())
        })
        .await
    }

    /// List message history with id above `after`, in id order, at most
    /// `limit` rows. With `address`, only messages it sent or received.
    pub async fn log(
        &self,
        project: String,
        address: Option<String>,
        after: u64,
        limit: u64,
    ) -> Result<Vec<LoggedMessage>, String> {
        self.with(move |conn| {
            select_all(
                conn,
                &format!("SELECT {MESSAGE_COLUMNS}, acked_at FROM messages WHERE id > ?1 AND (?2 IS NULL OR sender = ?2 OR recipient = ?2) AND (?4 = '{ALL_PROJECTS}' OR project = ?4) ORDER BY id LIMIT ?3"),
                rusqlite::params![after as i64, address, limit as i64, project],
                |row| {
                    Ok(LoggedMessage {
                        message: message_from_row(row)?,
                        acked_at: row.get::<_, Option<i64>>(9)?.map(|ts| ts as u64),
                    })
                },
            )
        })
        .await
    }

    /// Replay the most recent announcements per topic, merged in id
    /// order. Direct messages that carry a topic never replay.
    pub async fn topic_backlog(
        &self,
        project: String,
        topics: Vec<String>,
    ) -> Result<Vec<Message>, String> {
        self.with(move |conn| {
            let mut backlog = Vec::new();
            for topic in &topics {
                backlog.extend(select_all(
                    conn,
                    &format!("SELECT {MESSAGE_COLUMNS} FROM messages WHERE topic = ?1 AND recipient = '' AND (?3 = '{ALL_PROJECTS}' OR project = ?3) ORDER BY id DESC LIMIT ?2"),
                    rusqlite::params![topic, MAX_TOPIC_BACKLOG as i64, project],
                    message_from_row,
                )?);
            }
            backlog.sort_by_key(|message| message.id);
            Ok(backlog)
        })
        .await
    }

    /// List every address in `project` that ever said `hello`, sorted by
    /// project then address. The project `*` lists every project.
    pub async fn agents(&self, project: String) -> Result<Vec<Agent>, String> {
        self.with(move |conn| {
            select_all(
                conn,
                &format!("SELECT project, address, last_seen, tags, state, state_at FROM agents WHERE ?1 = '{ALL_PROJECTS}' OR project = ?1 ORDER BY project, address"),
                [project],
                |row| {
                    let tags: Option<String> = row.get(3)?;
                    Ok(Agent {
                        project: row.get(0)?,
                        address: row.get(1)?,
                        last_seen: row.get::<_, i64>(2)? as u64,
                        online: false,
                        tags: tags
                            .and_then(|json| serde_json::from_str(&json).ok())
                            .unwrap_or_default(),
                        state: row.get(4)?,
                        state_at: row.get::<_, Option<i64>>(5)?.map(|at| at as u64),
                    })
                },
            )
        })
        .await
    }

    /// The (project, address) pairs that sent or received a message at or
    /// after `since`, in ms since the epoch.
    pub async fn active_since(
        &self,
        project: String,
        since: u64,
    ) -> Result<HashSet<(String, String)>, String> {
        self.with(move |conn| {
            let pairs = select_all(
                conn,
                &format!("SELECT project, sender FROM messages WHERE ts >= ?1 AND (?2 = '{ALL_PROJECTS}' OR project = ?2)
                          UNION SELECT project, recipient FROM messages WHERE ts >= ?1 AND recipient <> '' AND (?2 = '{ALL_PROJECTS}' OR project = ?2)"),
                rusqlite::params![since as i64, project],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )?;
            Ok(pairs.into_iter().collect())
        })
        .await
    }

    /// Agent and message counts per project, sorted by project. The
    /// project `*` counts every project.
    pub async fn projects(&self, project: String) -> Result<Vec<ProjectInfo>, String> {
        self.with(move |conn| {
            select_all(
                conn,
                &format!("SELECT project, SUM(agents), SUM(messages) FROM (
                            SELECT project, COUNT(*) AS agents, 0 AS messages FROM agents GROUP BY project
                            UNION ALL
                            SELECT project, 0, COUNT(*) FROM messages GROUP BY project)
                          WHERE ?1 = '{ALL_PROJECTS}' OR project = ?1 GROUP BY project ORDER BY project"),
                [project],
                |row| {
                    Ok(ProjectInfo {
                        project: row.get(0)?,
                        agents: row.get::<_, i64>(1)? as u64,
                        messages: row.get::<_, i64>(2)? as u64,
                    })
                },
            )
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
    if version < 3 {
        add_tags(conn)?;
    }
    if version < 4 {
        add_state(conn)?;
    }
    if version < 5 {
        add_project(conn)?;
    }
    if version < 6 {
        add_from_role(conn)?;
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

/// The version-3 step: add the `tags` column to `agents`. It holds a JSON
/// array; NULL means no tags.
fn add_tags(conn: &Connection) -> Result<(), String> {
    conn.execute("ALTER TABLE agents ADD COLUMN tags TEXT", []).map_err(|e| e.to_string())?;
    Ok(())
}

/// The version-4 step: add `state` and `state_at` to `agents`. NULL means
/// the agent never reported a state.
fn add_state(conn: &Connection) -> Result<(), String> {
    conn.execute("ALTER TABLE agents ADD COLUMN state TEXT", []).map_err(|e| e.to_string())?;
    conn.execute("ALTER TABLE agents ADD COLUMN state_at INTEGER", [])
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// The version-5 step: add `project` to `messages` and `agents`, with
/// every old row in `default`. SQLite cannot change a primary key in
/// place, so `agents` is rebuilt as a new table keyed by (project,
/// address): create, copy, drop, rename. One transaction, so a failure
/// leaves the version-4 tables untouched.
fn add_project(conn: &Connection) -> Result<(), String> {
    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    tx.execute_batch(
        "ALTER TABLE messages ADD COLUMN project TEXT NOT NULL DEFAULT 'default';
         CREATE TABLE agents_v5 (project TEXT NOT NULL DEFAULT 'default', address TEXT NOT NULL, last_seen INTEGER NOT NULL, tags TEXT, state TEXT, state_at INTEGER, PRIMARY KEY (project, address));
         INSERT INTO agents_v5 (project, address, last_seen, tags, state, state_at) SELECT 'default', address, last_seen, tags, state, state_at FROM agents;
         DROP TABLE agents;
         ALTER TABLE agents_v5 RENAME TO agents;
         DROP INDEX IF EXISTS messages_unread;
         CREATE INDEX messages_unread ON messages(project, recipient, id) WHERE acked_at IS NULL;
         CREATE INDEX messages_topic ON messages(project, topic, id) WHERE recipient = '';
         CREATE INDEX messages_ts ON messages(ts);",
    )
    .map_err(|e| e.to_string())?;
    tx.commit().map_err(|e| e.to_string())
}

/// The version-6 step: add `from_role` to `messages`. NULL means the
/// broker had no token table when it stored the message.
fn add_from_role(conn: &Connection) -> Result<(), String> {
    conn.execute("ALTER TABLE messages ADD COLUMN from_role TEXT", [])
        .map_err(|e| e.to_string())?;
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
        project: row.get(7)?,
        from_role: row.get(8)?,
    })
}

pub(crate) fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sender(project: &str, address: &str) -> Sender {
        Sender { project: project.into(), address: address.into(), role: None }
    }

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
            .insert(sender("default", "a@h"), Some("b@h".into()), "one".into(), None, None)
            .await
            .unwrap()
            .id;
        let second = store
            .insert(sender("default", "a@h"), Some("b@h".into()), "two".into(), None, None)
            .await
            .unwrap()
            .id;
        let other = store
            .insert(sender("default", "a@h"), Some("c@h".into()), "other".into(), None, None)
            .await
            .unwrap()
            .id;

        let messages = store.inbox("default".into(), "b@h".into()).await.unwrap();
        let ids: Vec<u64> = messages.iter().map(|message| message.id).collect();
        assert_eq!(ids, vec![first, second]);
        assert_eq!(messages[0].from, "a@h");
        assert_eq!(messages[0].body, "one");

        let changed = store.ack("default".into(), "b@h".into(), vec![first, other]).await.unwrap();
        assert_eq!(changed, vec![first]);

        let messages = store.inbox("default".into(), "b@h".into()).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, second);
        let kept = store.inbox("default".into(), "c@h".into()).await.unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].id, other);
    }

    #[tokio::test]
    async fn insert_persists_a_row_across_reopens() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("sub").join("chattrs.db");
        let store = Store::open(db.clone()).await.unwrap();
        let stored = store
            .insert(sender("default", "a@h"), Some("b@h".into()), "hi".into(), None, None)
            .await
            .unwrap();
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
            .insert(sender("default", "b@h"), Some("a@h".into()), "yo".into(), Some(id), None)
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
                .insert(
                    sender("default", "a@h"),
                    None,
                    format!("note {i}"),
                    None,
                    Some("release-26".into()),
                )
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
            .insert(
                sender("default", "a@h"),
                None,
                "other room".into(),
                None,
                Some("other-topic".into()),
            )
            .await
            .unwrap()
            .id;
        let direct = store
            .insert(
                sender("default", "a@h"),
                Some("b@h".into()),
                "direct".into(),
                None,
                Some("release-26".into()),
            )
            .await
            .unwrap()
            .id;

        let backlog = store
            .topic_backlog("default".into(), vec!["release-26".into(), "other-topic".into()])
            .await
            .unwrap();
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
            let old = store.get("default".into(), 1).await.unwrap().expect("old row survives");
            assert_eq!(old.body, "old");
            assert_eq!(old.topic, None, "old rows read back with no topic");
            let id = store
                .insert(
                    sender("default", "a@h"),
                    Some("b@h".into()),
                    "new".into(),
                    None,
                    Some("release-26".into()),
                )
                .await
                .unwrap()
                .id;
            let fresh = store.get("default".into(), id).await.unwrap().unwrap();
            assert_eq!(fresh.topic.as_deref(), Some("release-26"));
            let version: i64 = rusqlite::Connection::open(&db)
                .unwrap()
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(version, 6, "open stamps the latest version");
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
        let old = store.get("default".into(), 1).await.unwrap().expect("old row survives");
        assert_eq!(old.topic.as_deref(), Some("release-26"), "a migrated row keeps its topic");
        let inbox = store.inbox("default".into(), "b@h".into()).await.unwrap();
        assert_eq!(inbox.len(), 1, "the row still belongs to its inbox");
    }

    #[tokio::test]
    async fn version_two_database_migrates_to_agent_tags() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.execute_batch(
                "CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, sender TEXT NOT NULL, recipient TEXT NOT NULL, body TEXT NOT NULL, ts INTEGER NOT NULL, reply_to INTEGER, topic TEXT, acked_at INTEGER);
                 CREATE TABLE agents (address TEXT PRIMARY KEY, last_seen INTEGER NOT NULL);
                 INSERT INTO agents (address, last_seen) VALUES ('a@h', 1763200000000);
                 PRAGMA user_version = 2;",
            )
            .unwrap();
        }
        let store = Store::open(db.clone()).await.unwrap();
        let agents = store.agents("default".into()).await.unwrap();
        assert_eq!(agents.len(), 1);
        assert!(agents[0].tags.is_empty(), "an old row has no tags");
        store.touch_agent("default".into(), "a@h".into(), vec!["gpu".into()], None).await.unwrap();
        store.touch_agent("default".into(), "a@h".into(), vec![], None).await.unwrap();
        assert_eq!(store.agents("default".into()).await.unwrap()[0].tags, ["gpu"]);
        let version: i64 = rusqlite::Connection::open(&db)
            .unwrap()
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 6);
    }

    #[tokio::test]
    async fn version_three_database_migrates_to_agent_state() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.execute_batch(
                "CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, sender TEXT NOT NULL, recipient TEXT NOT NULL, body TEXT NOT NULL, ts INTEGER NOT NULL, reply_to INTEGER, topic TEXT, acked_at INTEGER);
                 CREATE TABLE agents (address TEXT PRIMARY KEY, last_seen INTEGER NOT NULL, tags TEXT);
                 INSERT INTO agents (address, last_seen) VALUES ('a@h', 1763200000000);
                 PRAGMA user_version = 3;",
            )
            .unwrap();
        }
        let store = Store::open(db.clone()).await.unwrap();
        let agent = store.agents("default".into()).await.unwrap().remove(0);
        assert_eq!((agent.state, agent.state_at), (None, None), "an old row has no state");
        store
            .touch_agent("default".into(), "a@h".into(), vec![], Some("working".into()))
            .await
            .unwrap();
        let set = store.agents("default".into()).await.unwrap().remove(0);
        assert_eq!(set.state.as_deref(), Some("working"));
        assert!(set.state_at.is_some());
        store.touch_agent("default".into(), "a@h".into(), vec![], None).await.unwrap();
        let kept = store.agents("default".into()).await.unwrap().remove(0);
        assert_eq!(
            (kept.state, kept.state_at),
            (set.state, set.state_at),
            "no-state hello keeps both"
        );
        let version: i64 = rusqlite::Connection::open(&db)
            .unwrap()
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 6);
    }

    #[tokio::test]
    async fn version_four_database_migrates_every_row_into_default() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.execute_batch(
                "CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, sender TEXT NOT NULL, recipient TEXT NOT NULL, body TEXT NOT NULL, ts INTEGER NOT NULL, reply_to INTEGER, topic TEXT, acked_at INTEGER);
                 CREATE INDEX messages_unread ON messages(recipient, id) WHERE acked_at IS NULL;
                 CREATE TABLE agents (address TEXT PRIMARY KEY, last_seen INTEGER NOT NULL, tags TEXT, state TEXT, state_at INTEGER);
                 INSERT INTO messages (sender, recipient, body, ts, topic) VALUES ('a@h', 'b@h', 'direct', 1763200000000, NULL);
                 INSERT INTO messages (sender, recipient, body, ts, topic) VALUES ('a@h', '', 'room note', 1763200000001, 'release-26');
                 INSERT INTO agents (address, last_seen, tags, state, state_at) VALUES ('a@h', 1763200000000, '[\"gpu\"]', 'idle', 1763200000000);
                 INSERT INTO agents (address, last_seen) VALUES ('b@h', 1763200000000);
                 PRAGMA user_version = 4;",
            )
            .unwrap();
        }
        let store = Store::open(db.clone()).await.unwrap();
        let default = || "default".to_string();
        let agents = store.agents(default()).await.unwrap();
        assert_eq!(agents.len(), 2);
        assert!(agents.iter().all(|agent| agent.project == "default"));
        assert_eq!(
            (agents[0].tags.clone(), agents[0].state.as_deref()),
            (vec!["gpu".to_string()], Some("idle"))
        );
        let inbox = store.inbox(default(), "b@h".into()).await.unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].project, "default");
        let backlog = store.topic_backlog(default(), vec!["release-26".into()]).await.unwrap();
        assert_eq!(backlog.len(), 1);
        assert_eq!(backlog[0].project, "default");
        assert!(store.agents("other".into()).await.unwrap().is_empty());
        assert!(store.inbox("other".into(), "b@h".into()).await.unwrap().is_empty());

        // The key is (project, address): the same address can join another project.
        store.touch_agent("other".into(), "a@h".into(), vec![], None).await.unwrap();
        assert_eq!(store.agents("*".into()).await.unwrap().len(), 3);
        let counts = store.projects("*".into()).await.unwrap();
        let rows: Vec<(&str, u64, u64)> =
            counts.iter().map(|p| (p.project.as_str(), p.agents, p.messages)).collect();
        assert_eq!(rows, [("default", 2, 2), ("other", 1, 0)]);
        let version: i64 = rusqlite::Connection::open(&db)
            .unwrap()
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 6);
    }

    #[tokio::test]
    async fn version_five_database_migrates_to_from_role() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.execute_batch(
                "CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, sender TEXT NOT NULL, recipient TEXT NOT NULL, body TEXT NOT NULL, ts INTEGER NOT NULL, reply_to INTEGER, topic TEXT, acked_at INTEGER, project TEXT NOT NULL DEFAULT 'default');
                 CREATE TABLE agents (project TEXT NOT NULL DEFAULT 'default', address TEXT NOT NULL, last_seen INTEGER NOT NULL, tags TEXT, state TEXT, state_at INTEGER, PRIMARY KEY (project, address));
                 INSERT INTO messages (sender, recipient, body, ts) VALUES ('a@h', 'b@h', 'old', 1763200000000);
                 PRAGMA user_version = 5;",
            )
            .unwrap();
        }
        let store = Store::open(db.clone()).await.unwrap();
        let old = store.get("default".into(), 1).await.unwrap().expect("old row survives");
        assert_eq!(old.from_role, None, "an old row has no role");
        let sender =
            Sender { project: "default".into(), address: "a@h".into(), role: Some("lead".into()) };
        let id =
            store.insert(sender, Some("b@h".into()), "new".into(), None, None).await.unwrap().id;
        let fresh = store.get("default".into(), id).await.unwrap().unwrap();
        assert_eq!(fresh.from_role.as_deref(), Some("lead"));
        let logged = store.log("default".into(), None, 0, 10).await.unwrap();
        assert_eq!(logged[1].message.from_role.as_deref(), Some("lead"));
        assert_eq!(logged[1].acked_at, None);
        let version: i64 = rusqlite::Connection::open(&db)
            .unwrap()
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 6);
    }
}
