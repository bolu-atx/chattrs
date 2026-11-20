//! chattrs — on-demand broker start for loopback servers.
//!
//! A client command whose first connect fails with connection refused, on
//! a loopback server, spawns the same binary as `serve --listen` on that
//! address, detached with its log next to the DB, then the caller retries on a fresh
//! connection until the broker answers. A remote server is
//! never started: the original connect error returns.

use std::path::{Path, PathBuf};
use std::process::Stdio;

/// Marker set only on the spawned broker command: that broker owns the
/// pidfile next to the DB. Never a public CLI surface.
pub const AUTOSTART_ENV: &str = "CHATTRS_AUTOSTART";

/// True only for the spawned broker: it owns the pidfile next to the DB.
pub fn is_autostart_child() -> bool {
    std::env::var(AUTOSTART_ENV).as_deref() == Ok("1")
}

/// DB the client knows: `CHATTRS_DB` when set, else the default path.
pub fn client_db_path() -> PathBuf {
    match std::env::var("CHATTRS_DB") {
        Ok(value) if !value.trim().is_empty() => PathBuf::from(value),
        _ => crate::store::Store::default_path(),
    }
}

/// Broker log next to the DB file.
pub fn log_path(db: &Path) -> PathBuf {
    sidecar(db, "chattrs.log")
}

/// Broker pid next to the DB file.
pub fn pid_path(db: &Path) -> PathBuf {
    sidecar(db, "chattrs.pid")
}

fn sidecar(db: &Path, name: &str) -> PathBuf {
    match db.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(name),
        _ => PathBuf::from(name),
    }
}

/// True when the server address is loopback (`127.0.0.0/8`, `::1`, or
/// `localhost`). Only a loopback server may start a broker on demand.
pub fn is_loopback_server(server: &str) -> bool {
    if let Ok(addr) = server.parse::<std::net::SocketAddr>() {
        return addr.ip().is_loopback();
    }
    let host = server.rsplit_once(':').map(|part| part.0).unwrap_or(server);
    let bare = host.strip_prefix('[').and_then(|rest| rest.strip_suffix(']')).unwrap_or(host);
    bare.eq_ignore_ascii_case("localhost")
}

/// Start the same binary as a detached broker on `server` with `db`,
/// appending its output to the log next to the DB. Detached means no
/// terminal file stays open: stdin is null and both outputs append to
/// the log file. Dropping the child without waiting leaves the broker
/// running after this command exits; the OS re-parents it.
fn spawn_broker(server: &str, db: &Path, token: Option<&str>) -> Result<(), String> {
    let exe = std::env::current_exe().map_err(|e| format!("find own binary: {e}"))?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path(db))
        .map_err(|e| format!("open log {}: {e}", log_path(db).display()))?;
    let out = log.try_clone().map_err(|e| format!("open log: {e}"))?;
    let mut command = std::process::Command::new(exe);
    command.arg("serve").arg(format!("--listen={server}")).arg(format!("--db={}", db.display()));
    if let Some(token) = token {
        command.arg(format!("--token={token}"));
    }
    command
        .env(AUTOSTART_ENV, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(log));
    command.spawn().map_err(|e| format!("start broker: {e}"))?;
    Ok(())
}

/// How long a client waits for a starting broker to answer.
const WAIT_WINDOW: std::time::Duration = std::time::Duration::from_secs(5);

/// Pause between connect attempts while a broker starts.
pub const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(50);

/// The instant the wait for a starting broker ends.
pub fn wait_deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + WAIT_WINDOW
}

/// Start a loopback broker on demand. The caller retries its connect
/// until the broker answers. The race loser fails its bind and exits, so
/// parallel callers still end with one broker on the same DB file.
pub async fn ensure_started(server: &str, token: Option<&str>) -> Result<(), String> {
    let db = client_db_path();
    spawn_broker(server, &db, token)?;
    eprintln!("chattrs: started a broker for {server}, see {}", log_path(&db).display());
    Ok(())
}

/// Record this broker's pid next to the DB. Written after the bind
/// succeeds, so a race loser never leaves one.
pub fn write_pidfile(db: &Path) -> Result<(), String> {
    let path = pid_path(db);
    std::fs::write(&path, format!("{}\n", std::process::id()))
        .map_err(|e| format!("write pidfile {}: {e}", path.display()))
}

/// Remove the pidfile next to the DB, ignoring a missing file.
pub fn remove_pidfile(db: &Path) {
    let _ = std::fs::remove_file(pid_path(db));
}

/// Read the pid from the pidfile, or `None` when there is no pidfile or
/// it names no number.
pub fn read_pid(db: &Path) -> Option<u32> {
    std::fs::read_to_string(pid_path(db)).ok()?.trim().parse().ok()
}

/// Stop the auto-started broker named by the pidfile next to `db`:
/// terminate that process, wait until it is gone, and remove the
/// pidfile. Prints nothing; errors read `no auto-started broker
/// running`, including for a stale pid whose process is gone.
pub fn stop_auto_started(db: &Path) -> Result<(), String> {
    let pid = read_pid(db).ok_or_else(|| "no auto-started broker running".to_string())?;
    if !process_alive(pid) {
        return Err("no auto-started broker running".to_string());
    }
    terminate(pid)?;
    let end = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while process_alive(pid) {
        if std::time::Instant::now() >= end {
            return Err(format!("broker {pid} did not stop"));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    remove_pidfile(db);
    Ok(())
}

/// True when a process with this pid exists. Runs the `kill` lookup so
/// stopping needs no new dependency.
#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// Ask the process to exit. A pid that died in between reads as stale.
#[cfg(unix)]
fn terminate(pid: u32) -> Result<(), String> {
    let stopped = std::process::Command::new("kill")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    if stopped {
        Ok(())
    } else {
        Err("no auto-started broker running".to_string())
    }
}

/// True when a process with this pid exists.
#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    let out = std::process::Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH"])
        .output();
    match out {
        Ok(out) => String::from_utf8_lossy(&out.stdout).contains(&pid.to_string()),
        Err(_) => false,
    }
}

/// Ask the process to exit.
#[cfg(windows)]
fn terminate(pid: u32) -> Result<(), String> {
    let stopped = std::process::Command::new("taskkill")
        .args(["/F", "/PID", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    if stopped {
        Ok(())
    } else {
        Err("no auto-started broker running".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_covers_ipv4_octet_loopback_and_localhost() {
        for server in [
            "127.0.0.1:4646",
            "127.0.0.9:1",
            "127.1.2.3:4646",
            "localhost:4646",
            "LOCALHOST:4646",
            "[::1]:4646",
        ] {
            assert!(is_loopback_server(server), "{server} is loopback");
        }
    }

    #[test]
    fn non_loopback_never_starts_a_broker() {
        for server in [
            "192.168.1.5:4646",
            "10.0.0.1:4646",
            "0.0.0.0:4646",
            "[::]:4646",
            "example.com:4646",
            "broker.tailnet:4646",
        ] {
            assert!(!is_loopback_server(server), "{server} is remote");
        }
    }

    #[test]
    fn sidecars_live_next_to_the_db() {
        let db = PathBuf::from("/tmp/chat/chattrs.db");
        assert_eq!(log_path(&db), PathBuf::from("/tmp/chat/chattrs.log"));
        assert_eq!(pid_path(&db), PathBuf::from("/tmp/chat/chattrs.pid"));
    }
}
