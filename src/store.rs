//! chattrs — SQLite storage for messages.
//!
//! One `Connection` behind a mutex, shared by every broker task. Each call
//! runs on a blocking thread so the broker never stalls. The broker commits
//! a row before it writes `sent`, and messages are never deleted.

use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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

    /// Insert a message and return its id, which grows with send order.
    pub async fn insert(
        &self,
        sender: String,
        recipient: String,
        body: String,
        reply_to: Option<u64>,
    ) -> Result<u64, String> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            insert_blocking(&conn, &sender, &recipient, &body, reply_to)
        })
        .await
        .map_err(|e| format!("db task failed: {e}"))?
    }

    /// Default database path: `$XDG_DATA_HOME/chattrs/chattrs.db`, else
    /// `~/.local/share/chattrs/chattrs.db`.
    pub fn default_path() -> PathBuf {
        if let Ok(dir) = std::env::var("XDG_DATA_HOME") {
            if !dir.trim().is_empty() {
                return PathBuf::from(dir).join("chattrs/chattrs.db");
            }
        }
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        PathBuf::from(home).join(".local/share/chattrs/chattrs.db")
    }
}

fn open_blocking(path: &Path) -> Result<Store, String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create db dir {}: {e}", parent.display()))?;
        }
    }
    let conn =
        Connection::open(path).map_err(|e| format!("open db {}: {e}", path.display()))?;
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
           acked_at  INTEGER
         );
         CREATE INDEX IF NOT EXISTS messages_unread
           ON messages(recipient, id) WHERE acked_at IS NULL;",
    )
    .map_err(|e| e.to_string())?;
    Ok(Store { conn: Arc::new(Mutex::new(conn)) })
}

fn lock(conn: &Arc<Mutex<Connection>>) -> Result<std::sync::MutexGuard<'_, Connection>, String> {
    conn.lock().map_err(|_| "db lock poisoned".to_string())
}

fn insert_blocking(
    conn: &Arc<Mutex<Connection>>,
    sender: &str,
    recipient: &str,
    body: &str,
    reply_to: Option<u64>,
) -> Result<u64, String> {
    let conn = lock(conn)?;
    conn.execute(
        "INSERT INTO messages (sender, recipient, body, ts, reply_to)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![sender, recipient, body, now_ms() as i64, reply_to.map(|id| id as i64)],
    )
    .map_err(|e| e.to_string())?;
    Ok(conn.last_insert_rowid() as u64)
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

    #[tokio::test]
    async fn insert_persists_a_row_across_reopens() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("sub").join("chattrs.db");
        let store = Store::open(db.clone()).await.unwrap();
        let id = store.insert("a@h".into(), "b@h".into(), "hi".into(), None).await.unwrap();
        assert_eq!(id, 1);
        assert_eq!(read_row(&db, id), ("a@h".into(), "b@h".into(), "hi".into()));
        drop(store);

        let store = Store::open(db.clone()).await.unwrap();
        let next = store.insert("b@h".into(), "a@h".into(), "yo".into(), Some(id)).await.unwrap();
        assert_eq!(next, id + 1);
        assert_eq!(read_row(&db, id), ("a@h".into(), "b@h".into(), "hi".into()));
    }
}
