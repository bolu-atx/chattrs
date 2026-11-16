//! Broker tests: auto-start.

use super::helpers::*;
use crate::client;
use tempfile::TempDir;

fn pid_file(db: &std::path::Path) -> std::path::PathBuf {
    db.with_file_name("chattrs.pid")
}

fn log_file(db: &std::path::Path) -> std::path::PathBuf {
    db.with_file_name("chattrs.log")
}

fn read_test_pid(db: &std::path::Path) -> Option<u32> {
    std::fs::read_to_string(pid_file(db)).ok()?.trim().parse().ok()
}

/// Run a client command with `CHATTRS_DB` pointing at `db`, so an
/// auto-start uses this test's database.
fn run_client(bin: &std::path::Path, db: &std::path::Path, args: &[&str]) -> std::process::Output {
    std::process::Command::new(bin).args(args).env("CHATTRS_DB", db).output().expect("run chattrs")
}

fn run_stop(bin: &std::path::Path, db: &std::path::Path) -> std::process::Output {
    std::process::Command::new(bin)
        .arg("stop")
        .env("CHATTRS_DB", db)
        .output()
        .expect("run chattrs stop")
}

/// Wait until `addr` accepts a raw connection, without touching the
/// client (which would start a broker itself).
async fn wait_for_port(addr: &str) {
    let start = tokio::time::Instant::now();
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(
            start.elapsed() < std::time::Duration::from_secs(15),
            "broker never answered at {addr}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// True when nothing listens at `addr` anymore.
async fn port_is_closed(addr: &str) -> bool {
    tokio::net::TcpStream::connect(addr).await.is_err()
}

async fn wait_for_pid(db: &std::path::Path) -> u32 {
    let start = tokio::time::Instant::now();
    loop {
        if let Some(pid) = read_test_pid(db) {
            return pid;
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(10), "pidfile never appeared");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Stops the auto-started broker for `db` on drop, killing the pid
/// when `stop` cannot. Every auto-start test owns one so a failure
/// never leaks a broker.
struct AutostartGuard {
    bin: std::path::PathBuf,
    db: std::path::PathBuf,
}

impl AutostartGuard {
    fn new(bin: &std::path::Path, db: &std::path::Path) -> AutostartGuard {
        AutostartGuard { bin: bin.to_path_buf(), db: db.to_path_buf() }
    }
}

impl Drop for AutostartGuard {
    fn drop(&mut self) {
        let _ = run_stop(&self.bin, &self.db);
        if let Some(pid) = read_test_pid(&self.db) {
            force_kill(pid);
        }
    }
}

#[cfg(unix)]
fn force_kill(pid: u32) {
    let _ = std::process::Command::new("kill").arg("-9").arg(pid.to_string()).status();
}

#[cfg(windows)]
fn force_kill(pid: u32) {
    let _ = std::process::Command::new("taskkill").args(["/F", "/PID", &pid.to_string()]).status();
}

#[tokio::test]
async fn send_starts_broker_on_demand() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _guard = AutostartGuard::new(&bin, &db);

    let out = run_client(
        &bin,
        &db,
        &["send", "--server", addr.as_str(), "--address", "a@h", "--to", "b@h", "hi"],
    );
    assert!(out.status.success(), "send starts a broker and exits zero: {out:?}");
    let lines = stdout_lines(&out);
    assert_eq!(lines, vec!["1"], "first send prints id 1 and nothing else: {lines:?}");
    let stderr = String::from_utf8(out.stderr.clone()).expect("stderr is utf8");
    assert!(stderr.contains("started a broker"), "the notice goes to stderr: {stderr}");

    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].from, "a@h");
    assert_eq!(messages[0].body, "hi");
}

#[tokio::test]
async fn watch_starts_broker_on_demand() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _guard = AutostartGuard::new(&bin, &db);

    let out_path = dir.path().join("watch.out");
    let file = std::fs::File::create(&out_path).expect("watch output file");
    let child = std::process::Command::new(&bin)
        .arg("watch")
        .arg("--server")
        .arg(&addr)
        .arg("--address")
        .arg("b@h")
        .env("CHATTRS_DB", &db)
        .stdout(file)
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn watch");
    let _watcher = ProcChild { child };

    wait_for_port(&addr).await;
    let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hello")).await.unwrap();
    let lines = wait_for_lines(&out_path, 1).await;
    assert_eq!(lines.len(), 1, "watch prints the message sent after it started");
    let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
    assert_eq!(value["id"].as_u64(), Some(id));
    assert_eq!(value["body"].as_str(), Some("hello"));
}

#[tokio::test]
async fn second_command_reuses_the_running_broker() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _guard = AutostartGuard::new(&bin, &db);

    let first = run_client(
        &bin,
        &db,
        &["send", "--server", addr.as_str(), "--address", "a@h", "--to", "b@h", "one"],
    );
    assert!(first.status.success(), "first send exits zero: {first:?}");
    let first_id: u64 = stdout_lines(&first)[0].parse().expect("first id");
    let pid = wait_for_pid(&db).await;
    assert!(pid > 0, "the broker records one pid");

    let second = run_client(
        &bin,
        &db,
        &["send", "--server", addr.as_str(), "--address", "a@h", "--to", "b@h", "two"],
    );
    assert!(second.status.success(), "second send exits zero: {second:?}");
    let second_id: u64 = stdout_lines(&second)[0].parse().expect("second id");
    assert!(second_id > first_id, "ids increase on the same broker");
    assert_eq!(read_test_pid(&db), Some(pid), "both commands share one broker");
}

#[tokio::test]
async fn concurrent_first_commands_start_one_broker() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _guard = AutostartGuard::new(&bin, &db);

    let mut tasks = Vec::new();
    for i in 0..8 {
        let (bin, db, addr) = (bin.clone(), db.clone(), addr.clone());
        tasks.push(tokio::task::spawn_blocking(move || {
            let body = format!("msg {i}");
            run_client(
                &bin,
                &db,
                &[
                    "send",
                    "--server",
                    addr.as_str(),
                    "--address",
                    "a@h",
                    "--to",
                    "b@h",
                    body.as_str(),
                ],
            )
        }));
    }
    let mut ids = Vec::new();
    for task in tasks {
        let out = task.await.expect("send task");
        assert!(out.status.success(), "every parallel send succeeds: {out:?}");
        ids.push(stdout_lines(&out)[0].parse::<u64>().expect("id line"));
    }
    ids.sort_unstable();
    let expected: Vec<u64> = (1..=8).collect();
    assert_eq!(ids, expected, "no message is lost or duplicated: {ids:?}");
    let pid = wait_for_pid(&db).await;
    assert!(pid > 0, "exactly one broker pid exists afterwards");
    let text = std::fs::read_to_string(pid_file(&db)).expect("pidfile");
    assert_eq!(text.lines().count(), 1, "the pidfile names one broker: {text:?}");
}

#[tokio::test]
async fn remote_connect_failure_starts_nothing() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let bin = ensure_bin();
    let _guard = AutostartGuard::new(&bin, &db);
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    let port = probe.local_addr().expect("probe addr").port();
    drop(probe);
    let remote = format!("192.0.2.1:{port}");

    let mut child = std::process::Command::new(&bin)
        .arg("send")
        .arg("--server")
        .arg(&remote)
        .arg("--address")
        .arg("a@h")
        .arg("--to")
        .arg("b@h")
        .arg("hi")
        .env("CHATTRS_DB", &db)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn send");
    let start = tokio::time::Instant::now();
    loop {
        if child.try_wait().expect("poll send").is_some() {
            break;
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(20), "remote send hangs");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let out = child.wait_with_output().expect("send output");
    assert_eq!(out.status.code(), Some(1), "remote failure exits 1: {out:?}");
    let stderr = String::from_utf8(out.stderr).expect("stderr is utf8");
    assert!(stderr.contains("chattrs:"), "the error names chattrs: {stderr}");
    assert!(!pid_file(&db).exists(), "no pidfile for a remote failure");
    assert!(!log_file(&db).exists(), "no log file for a remote failure");
}

#[tokio::test]
async fn autostart_broker_is_stopped_by_stop() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _guard = AutostartGuard::new(&bin, &db);

    let out = run_client(
        &bin,
        &db,
        &["send", "--server", addr.as_str(), "--address", "a@h", "--to", "b@h", "hi"],
    );
    assert!(out.status.success(), "send starts a broker: {out:?}");
    let pid = wait_for_pid(&db).await;
    assert!(pid > 0, "the broker records its pid");

    let stopped = run_stop(&bin, &db);
    assert!(stopped.status.success(), "stop exits zero: {stopped:?}");
    assert!(stdout_lines(&stopped).is_empty(), "stop prints nothing: {stopped:?}");
    assert!(!pid_file(&db).exists(), "stop removes the pidfile");
    assert!(port_is_closed(&addr).await, "the port no longer answers");

    let again = run_stop(&bin, &db);
    assert_eq!(again.status.code(), Some(1), "a second stop exits 1: {again:?}");
    let stderr = String::from_utf8(again.stderr).expect("stderr is utf8");
    assert!(
        stderr.contains("no auto-started broker running"),
        "the error names the missing broker: {stderr}"
    );
}

#[tokio::test]
async fn already_running_broker_is_untouched() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;

    let out = run_client(
        &bin,
        &db,
        &["send", "--server", addr.as_str(), "--address", "a@h", "--to", "b@h", "hi"],
    );
    assert!(out.status.success(), "send exits zero against the running broker: {out:?}");
    assert!(!pid_file(&db).exists(), "a manual broker leaves no pidfile");
    assert!(!log_file(&db).exists(), "no auto-start means no log file");
    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body, "hi");
}
