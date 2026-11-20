//! Helpers shared by the CLI integration tests: a broker child, a client
//! runner, and output readers. Each test crate uses a subset.
#![allow(dead_code)]

use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Path of the binary under test.
pub fn binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_chattrs"))
}

/// A running broker. Killed on drop so no test leaks a server.
pub struct Broker {
    pub child: Child,
    _dir: tempfile::TempDir,
    pub addr: String,
}

impl Broker {
    pub fn start() -> Broker {
        Broker::start_with(&[])
    }

    pub fn start_with(extra: &[&str]) -> Broker {
        let dir = tempfile::TempDir::new().expect("make a temp dir");
        let db = dir.path().join("c.db");
        let mut child = crate::common::command(binary())
            .args(["serve", "--listen", "127.0.0.1:0", "--db", db.to_str().expect("db path")])
            .args(extra)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the broker");
        let mut stderr = child.stderr.take().expect("broker stderr");
        let (sender, receiver) = mpsc::channel::<String>();
        std::thread::spawn(move || {
            let mut sent = false;
            for line in BufReader::new(&mut stderr).lines() {
                let Ok(line) = line else { break };
                if !sent {
                    if let Some(addr) = line.strip_prefix("chattrs: serving on ") {
                        let _ = sender.send(addr.trim().to_string());
                        sent = true;
                    }
                }
            }
        });
        let addr = receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("broker prints its serving address");
        let mut broker = Broker { child, _dir: dir, addr };
        broker.wait_until_ready();
        broker
    }

    /// Poll until the broker accepts a connection, or panic when it exits.
    fn wait_until_ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if TcpStream::connect(&self.addr).is_ok() {
                return;
            }
            if let Some(status) = self.child.try_wait().expect("poll the broker") {
                panic!("broker exited early: {status}");
            }
            if Instant::now() > deadline {
                panic!("broker never accepted on {}", self.addr);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A child killed on drop, so a failed test cannot leak it.
pub struct KillOnDrop(pub Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Run one client command with `--server` and `CHATTRS_ADDRESS` set.
pub fn run_client(server: &str, address: &str, subcommand: &str, args: &[&str]) -> Output {
    let mut command = crate::common::command(binary());
    command.arg(subcommand).arg("--server").arg(server).args(args);
    command.env("CHATTRS_ADDRESS", address);
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    command.output().expect("run the client")
}

/// Parse one output line as JSON, panicking with the line on failure.
pub fn parse_line(line: &str) -> serde_json::Value {
    serde_json::from_str(line).unwrap_or_else(|error| panic!("not JSON: {line:?}: {error}"))
}

pub fn stdout_text(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout is utf-8")
}

pub fn stderr_text(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("stderr is utf-8")
}
/// Point every path the client reads at `home` and clear the overrides.
pub fn clean_home_env(command: &mut Command, home: &std::path::Path) {
    command
        .env("HOME", home)
        .env("CODEX_HOME", home.join(".codex"))
        .env_remove("XDG_DATA_HOME")
        .env_remove("XDG_CONFIG_HOME")
        .env_remove("CHATTRS_ADDRESS")
        .current_dir(home);
}

/// Parse `export NAME='value'` lines, undoing the `'\''` escape.
pub fn parse_exports(text: &str) -> std::collections::HashMap<String, String> {
    text.lines()
        .filter_map(|line| line.strip_prefix("export ")?.split_once('='))
        .map(|(name, value)| {
            let inner =
                value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')).unwrap_or(value);
            (name.to_string(), inner.replace("'\\''", "'"))
        })
        .collect()
}
