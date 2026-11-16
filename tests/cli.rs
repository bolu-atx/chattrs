//! Black-box CLI tests: a real broker plus the built binary as a child.
//!
//! Each test starts `chattrs serve` on a loopback port with a fresh temp
//! database, then runs client commands against it and checks stdout,
//! stderr, and the exit code. Nothing here touches `src/`.

use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, ChildStdout, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Path of the binary under test.
fn binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_chattrs"))
}

/// A free loopback port: bind `:0`, read the port, drop the listener.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind a free port")
        .local_addr()
        .expect("read the bound port")
        .port()
}

/// A running broker. Killed on drop so no test leaks a server.
struct Broker {
    child: Child,
    _dir: tempfile::TempDir,
    addr: String,
}

impl Broker {
    fn start() -> Broker {
        let dir = tempfile::TempDir::new().expect("make a temp dir");
        let db = dir.path().join("c.db");
        let addr = format!("127.0.0.1:{}", free_port());
        let child = Command::new(binary())
            .args(["serve", "--listen", addr.as_str(), "--db", db.to_str().expect("db path")])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the broker");
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
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Clear the `CHATTRS_*` overrides that would let the developer's shell
/// change the result. The address always comes from the caller.
fn clear_env(command: &mut Command) {
    command
        .env_remove("CHATTRS_TOKEN")
        .env_remove("CHATTRS_AGENT")
        .env_remove("CHATTRS_SERVER")
        .env_remove("CHATTRS_DB");
}

/// Run one client command with `--server` and `CHATTRS_ADDRESS` set.
fn run_client(server: &str, address: &str, subcommand: &str, args: &[&str]) -> Output {
    let mut command = Command::new(binary());
    command.arg(subcommand).arg("--server").arg(server).args(args);
    command.env("CHATTRS_ADDRESS", address);
    clear_env(&mut command);
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    command.output().expect("run the client")
}

/// Parse one output line as JSON, panicking with the line on failure.
fn parse_line(line: &str) -> serde_json::Value {
    serde_json::from_str(line).unwrap_or_else(|error| panic!("not JSON: {line:?}: {error}"))
}

fn stdout_text(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout is utf-8")
}

fn stderr_text(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("stderr is utf-8")
}

#[test]
fn send_prints_only_the_id_on_stdout() {
    let broker = Broker::start();
    let output = run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "hello"]);
    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_text(&output);
    let id: u64 = stdout.trim().parse().expect("stdout is the id");
    assert_eq!(stdout, format!("{id}\n"));
    assert!(output.stderr.is_empty(), "stderr is empty: {:?}", stderr_text(&output));
}

#[test]
fn send_json_prints_a_sent_line() {
    let broker = Broker::start();
    let output = run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "--json", "hello"]);
    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_text(&output);
    assert_eq!(stdout.lines().count(), 1);
    let value = parse_line(stdout.trim());
    assert_eq!(value["type"].as_str(), Some("sent"));
    assert!(value["id"].as_u64().is_some(), "sent line carries an id: {value}");
}

#[test]
fn ask_exits_two_on_timeout_and_names_the_id_on_stderr() {
    let broker = Broker::start();
    let output =
        run_client(&broker.addr, "a@h", "ask", &["--to", "nobody@h", "--timeout", "1", "are you?"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty(), "stdout is empty");
    let stderr = stderr_text(&output);
    let prefix = "timeout: no reply to ";
    let pos = stderr.find(prefix).expect("stderr names the timeout");
    stderr[pos + prefix.len()..].trim().parse::<u64>().expect("timeout names an id");
}

#[test]
fn ask_json_timeout_prints_a_timeout_line() {
    let broker = Broker::start();
    let output = run_client(
        &broker.addr,
        "a@h",
        "ask",
        &["--to", "nobody@h", "--timeout", "1", "--json", "are you?"],
    );
    assert_eq!(output.status.code(), Some(2));
    let stdout = stdout_text(&output);
    assert_eq!(stdout.lines().count(), 1);
    let value = parse_line(stdout.trim());
    assert_eq!(value["type"].as_str(), Some("timeout"));
    let id = value["id"].as_u64().expect("timeout line carries an id");
    let stderr = stderr_text(&output);
    assert!(stderr.contains(&format!("timeout: no reply to {id}")));
}

#[test]
fn inbox_json_lines_have_type_message_and_untrusted() {
    let broker = Broker::start();
    for body in ["one", "two"] {
        let sent = run_client(&broker.addr, "writer@h", "send", &["--to", "reader@h", body]);
        assert_eq!(sent.status.code(), Some(0));
    }
    let output = run_client(&broker.addr, "reader@h", "inbox", &["--json"]);
    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_text(&output);
    assert_eq!(stdout.lines().count(), 2);
    for line in stdout.lines() {
        let value = parse_line(line);
        assert_eq!(value["type"].as_str(), Some("message"));
        assert_eq!(value["untrusted"].as_bool(), Some(true));
    }
}

#[test]
fn agents_json_lines_have_type_agent() {
    let broker = Broker::start();
    let sent = run_client(&broker.addr, "writer@h", "send", &["--to", "reader@h", "hi"]);
    assert_eq!(sent.status.code(), Some(0));
    let output = run_client(&broker.addr, "observer@h", "agents", &["--json"]);
    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_text(&output);
    assert!(stdout.lines().next().is_some(), "agents lists someone");
    let mut saw_writer = false;
    for line in stdout.lines() {
        let value = parse_line(line);
        assert_eq!(value["type"].as_str(), Some("agent"));
        saw_writer |= value["address"].as_str() == Some("writer@h");
    }
    assert!(saw_writer, "agents lists the sender");
}

#[test]
fn log_json_lines_have_type_log() {
    let broker = Broker::start();
    let sent = run_client(&broker.addr, "writer@h", "send", &["--to", "reader@h", "hi"]);
    assert_eq!(sent.status.code(), Some(0));
    // A fresh address has an empty inbox, so the preamble prints nothing
    // and every line is a log row.
    let output = run_client(&broker.addr, "observer@h", "log", &["--json"]);
    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_text(&output);
    assert!(stdout.lines().next().is_some(), "log shows the message");
    for line in stdout.lines() {
        let value = parse_line(line);
        assert_eq!(value["type"].as_str(), Some("log"));
        assert_eq!(value["untrusted"].as_bool(), Some(true));
    }
}

#[test]
fn errors_go_to_stderr_with_exit_one() {
    // No broker listens on these ports: each was free a moment ago. Retry
    // a few times in case another test's broker recycled one of them.
    for _ in 0..5 {
        let closed = format!("127.0.0.1:{}", free_port());
        let output = run_client(&closed, "a@h", "inbox", &[]);
        if output.status.code() != Some(1) {
            continue;
        }
        assert!(output.stdout.is_empty(), "stdout is empty");
        let stderr = stderr_text(&output);
        assert!(stderr.starts_with("chattrs: "), "stderr names chattrs: {stderr:?}");
        return;
    }
    panic!("every closed port accepted a connection");
}

#[test]
fn watch_stdout_is_json_lines_only() {
    let broker = Broker::start();
    let mut command = Command::new(binary());
    command.arg("watch").arg("--server").arg(&broker.addr);
    command.env("CHATTRS_ADDRESS", "watcher@h");
    clear_env(&mut command);
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::null());
    let mut watch = command.spawn().expect("spawn watch");
    let stdout: ChildStdout = watch.stdout.take().expect("watch stdout");
    let _guard = KillOnDrop(watch);

    // The broker replays unread messages to a new watcher, so the sends
    // land whether or not `watch` has connected yet.
    for body in ["one", "two"] {
        let sent = run_client(&broker.addr, "writer@h", "send", &["--to", "watcher@h", body]);
        assert_eq!(sent.status.code(), Some(0));
    }

    let (sender, receiver) = mpsc::channel();
    let _reader = std::thread::spawn(move || {
        let lines = BufReader::new(stdout).lines();
        for line in lines {
            match line {
                Ok(line) => {
                    if sender.send(line).is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });
    let mut bodies = Vec::new();
    for _ in 0..2 {
        let line = receiver.recv_timeout(Duration::from_secs(15)).expect("watch prints a line");
        let value = parse_line(&line);
        assert_eq!(value["type"].as_str(), Some("message"));
        bodies.push(value["body"].as_str().expect("message carries a body").to_string());
    }
    assert_eq!(bodies, ["one", "two"]);
}
