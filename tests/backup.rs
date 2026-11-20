//! Black-box tests for `chattrs backup` (OPS-1): a real broker holds the
//! database open while the built binary copies it.

mod common;

use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Output, Stdio};
use std::time::{Duration, Instant};

fn binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_chattrs"))
}

/// A broker that dies with the test.
struct Broker {
    child: Child,
    server: String,
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_broker(db: &Path) -> Broker {
    let server = format!("127.0.0.1:{}", common::free_port());
    let child = common::command(binary())
        .args(["serve", "--listen", &server, "--db", db.to_str().expect("db path")])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn the broker");
    let broker = Broker { child, server };
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(&broker.server).is_err() {
        assert!(Instant::now() < deadline, "broker never accepted on {}", broker.server);
        std::thread::sleep(Duration::from_millis(50));
    }
    broker
}

fn send(broker: &Broker, body: &str) {
    let output = common::command(binary())
        .args(["send", "--server", &broker.server, "--to", "b@h", body])
        .env("CHATTRS_ADDRESS", "a@h")
        .output()
        .expect("run send");
    assert!(output.status.success(), "send failed: {output:?}");
}

fn backup(file: &Path, db: &Path, extra: &[&str]) -> Output {
    common::command(binary())
        .arg("backup")
        .arg(file)
        .arg("--db")
        .arg(db)
        .args(extra)
        .output()
        .expect("run backup")
}

fn bodies(db: &Path) -> Vec<String> {
    let conn =
        rusqlite::Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open the copy");
    let mut stmt = conn.prepare("SELECT body FROM messages ORDER BY id").expect("prepare");
    let rows = stmt.query_map([], |row| row.get::<_, String>(0)).expect("query");
    rows.map(|row| row.expect("row")).collect()
}

#[test]
fn copies_a_live_database() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("broker.db");
    let broker = start_broker(&db);
    send(&broker, "one");
    send(&broker, "two");
    let file = dir.path().join("copy.db");
    let output = backup(&file, &db, &[]);
    assert!(output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("2 messages") && stderr.contains("copy.db"), "{stderr}");
    assert_eq!(bodies(&file), ["one", "two"]);
    // The broker still runs and the source still takes writes.
    send(&broker, "three");
    assert_eq!(bodies(&db).len(), 3);
    assert_eq!(bodies(&file).len(), 2);
}

#[test]
fn refuses_an_existing_file_unless_forced() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("broker.db");
    let broker = start_broker(&db);
    send(&broker, "one");
    let file = dir.path().join("copy.db");
    std::fs::write(&file, "keep me").expect("write");
    let output = backup(&file, &db, &[]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--force"));
    assert_eq!(std::fs::read_to_string(&file).expect("read"), "keep me");

    let output = backup(&file, &db, &["--force"]);
    assert!(output.status.success(), "{output:?}");
    assert_eq!(bodies(&file), ["one"]);
    // No temp file stays behind.
    let names: Vec<_> =
        std::fs::read_dir(dir.path()).expect("ls").map(|e| e.expect("e").file_name()).collect();
    assert!(names.iter().all(|n| !n.to_string_lossy().contains(".tmp-")), "{names:?}");
}

#[cfg(unix)]
#[test]
fn copy_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("broker.db");
    let broker = start_broker(&db);
    send(&broker, "one");
    let file = dir.path().join("copy.db");
    assert!(backup(&file, &db, &[]).status.success());
    let mode = std::fs::metadata(&file).expect("stat").permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
}

#[test]
fn missing_source_fails_and_creates_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = dir.path().join("copy.db");
    let output = backup(&file, &dir.path().join("nope.db"), &[]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("no database at"));
    let count = std::fs::read_dir(dir.path()).expect("ls").count();
    assert_eq!(count, 0);
}
