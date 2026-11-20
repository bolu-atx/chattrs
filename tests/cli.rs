//! Black-box CLI tests: a real broker plus the built binary as a child.
//!
//! Each test starts `chattrs serve` on a loopback port with a fresh temp
//! database, then runs client commands against it and checks stdout,
//! stderr, and the exit code. Nothing here touches `src/`.

mod common;

use common::free_port;
use std::io::{BufRead, BufReader, Write as _};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, ChildStdout, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Path of the binary under test.
fn binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_chattrs"))
}

/// A running broker. Killed on drop so no test leaks a server.
struct Broker {
    child: Child,
    _dir: tempfile::TempDir,
    addr: String,
}

impl Broker {
    fn start() -> Broker {
        Broker::start_with(&[])
    }

    fn start_with(extra: &[&str]) -> Broker {
        let dir = tempfile::TempDir::new().expect("make a temp dir");
        let db = dir.path().join("c.db");
        let mut child = common::command(binary())
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
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Run one client command with `--server` and `CHATTRS_ADDRESS` set.
fn run_client(server: &str, address: &str, subcommand: &str, args: &[&str]) -> Output {
    let mut command = common::command(binary());
    command.arg(subcommand).arg("--server").arg(server).args(args);
    command.env("CHATTRS_ADDRESS", address);
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
    run_client(&broker.addr, "b@h", "inbox", &[]);
    let output = run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "hello"]);
    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_text(&output);
    let id: u64 = stdout.trim().parse().expect("stdout is the id");
    assert_eq!(stdout, format!("{id}\n"));
    assert!(output.stderr.is_empty(), "stderr is empty: {:?}", stderr_text(&output));
}

#[test]
fn send_to_an_unknown_address_warns_on_stderr_and_still_succeeds() {
    let broker = Broker::start();
    let output = run_client(&broker.addr, "a@h", "send", &["--to", "typo@h", "hello"]);
    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_text(&output);
    let id: u64 = stdout.trim().parse().expect("stdout is the id");
    assert_eq!(stdout, format!("{id}\n"));
    assert_eq!(
        stderr_text(&output),
        format!("chattrs: warning: typo@h has never connected; message {id} stored\n")
    );
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
fn ask_to_an_unknown_address_warns_on_stderr_and_keeps_waiting() {
    let broker = Broker::start();
    let output =
        run_client(&broker.addr, "a@h", "ask", &["--to", "typo@h", "--timeout", "1", "are you?"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty(), "stdout is empty");
    let stderr = stderr_text(&output);
    let mut lines = stderr.lines();
    let warning = lines.next().expect("warning line");
    let id = warning
        .strip_prefix("chattrs: warning: typo@h has never connected; message ")
        .and_then(|rest| rest.strip_suffix(" stored"))
        .expect("warning names the address and id");
    assert_eq!(lines.next(), Some(format!("timeout: no reply to {id}").as_str()));
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
fn first_run_creates_the_data_directory() {
    // On a new machine the data directory does not exist yet. Autostart
    // opens its log there before the broker creates the DB, so it must
    // create the directory itself.
    let dir = tempfile::TempDir::new().expect("make a temp dir");
    let db = dir.path().join("new").join("data").join("c.db");
    let closed = format!("127.0.0.1:{}", free_port());
    let mut command = common::command(binary());
    command.arg("inbox").arg("--server").arg(&closed);
    command.env("CHATTRS_ADDRESS", "a@h");
    command.env("CHATTRS_DB", &db);
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let output = command.output().expect("run the client");
    assert_eq!(output.status.code(), Some(0), "first run auto-starts a broker: {output:?}");
    assert!(db.parent().expect("db dir").join("chattrs.log").exists(), "the log is created");

    let mut stop = common::command(binary());
    stop.arg("stop").env("CHATTRS_DB", &db);
    stop.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let stopped = stop.output().expect("stop the broker");
    assert_eq!(stopped.status.code(), Some(0), "stop exits zero: {stopped:?}");
}

#[test]
fn errors_go_to_stderr_with_exit_one() {
    // A loopback server with no broker starts one on demand (CLI-11):
    // the command then runs against it. The DB is a temp path, so the
    // real default database is never touched, and the broker this
    // starts is stopped before the test ends.
    let dir = tempfile::TempDir::new().expect("make a temp dir");
    let db = dir.path().join("c.db");
    let closed = format!("127.0.0.1:{}", free_port());
    let mut command = common::command(binary());
    command.arg("inbox").arg("--server").arg(&closed);
    command.env("CHATTRS_ADDRESS", "a@h");
    command.env("CHATTRS_DB", &db);
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let output = command.output().expect("run the client");
    assert_eq!(output.status.code(), Some(0), "inbox auto-starts a broker: {output:?}");
    assert!(output.stdout.is_empty(), "stdout is empty");
    let stderr = stderr_text(&output);
    assert!(stderr.contains("started a broker"), "stderr names the start: {stderr:?}");

    let mut stop = common::command(binary());
    stop.arg("stop").env("CHATTRS_DB", &db);
    stop.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let stopped = stop.output().expect("stop the broker");
    assert_eq!(stopped.status.code(), Some(0), "stop exits zero: {stopped:?}");
    assert!(stopped.stdout.is_empty(), "stop prints nothing");
    assert!(TcpStream::connect(&closed).is_err(), "the port no longer answers after stop");

    // A remote server is never auto-started: the failure is an error
    // on stderr with exit 1, and it starts nothing.
    let remote = format!("192.0.2.1:{}", free_port());
    let mut command = common::command(binary());
    command.arg("inbox").arg("--server").arg(&remote);
    command.env("CHATTRS_ADDRESS", "a@h");
    command.env("CHATTRS_DB", &db);
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let output = command.output().expect("run the client");
    assert_eq!(output.status.code(), Some(1), "remote failure exits one: {output:?}");
    assert!(output.stdout.is_empty(), "stdout is empty");
    let stderr = stderr_text(&output);
    assert!(stderr.starts_with("chattrs: "), "stderr names chattrs: {stderr:?}");
    assert!(!dir.path().join("chattrs.pid").exists(), "no pidfile for a remote failure");
}

/// Store `count` messages over one raw TCP connection speaking the
/// line-JSON protocol, without spawning a child per message.
fn seed_history(server: &str, count: usize) {
    let mut stream = TcpStream::connect(server).expect("connect to the broker");
    let mut reader = BufReader::new(stream.try_clone().expect("clone the stream"));
    let mut exchange = |line: &str| {
        stream.write_all(line.as_bytes()).expect("write a frame");
        stream.flush().expect("flush a frame");
        let mut reply = String::new();
        reader.read_line(&mut reply).expect("read the reply");
        reply
    };
    let welcome = exchange("{\"type\":\"hello\",\"v\":1,\"address\":\"seed@h\"}\n");
    assert!(welcome.contains("\"type\":\"welcome\""), "broker welcomes: {welcome:?}");
    for i in 0..count {
        let line =
            format!("{{\"type\":\"send\",\"to\":\"rcpt@h\",\"body\":\"seeded message {i}\"}}\n");
        let reply = exchange(&line);
        assert!(reply.contains("\"type\":\"sent\""), "broker acks send {i}: {reply:?}");
    }
}

#[test]
fn log_about_filters_by_sender_or_recipient() {
    let broker = Broker::start();
    for (from, to, body) in
        [("a@h", "b@h", "alpha"), ("b@h", "a@h", "beta"), ("a@h", "c@h", "gamma")]
    {
        let sent = run_client(&broker.addr, from, "send", &["--to", to, body]);
        assert_eq!(sent.status.code(), Some(0));
    }
    let sent = run_client(&broker.addr, "x@h", "send", &["--to", "y@h", "unrelated"]);
    assert_eq!(sent.status.code(), Some(0));

    let output = run_client(&broker.addr, "observer@h", "log", &["--about", "a@h"]);
    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_text(&output);
    for body in ["alpha", "beta", "gamma"] {
        assert!(stdout.contains(body), "log --about a@h keeps {body}: {stdout:?}");
    }
    assert!(!stdout.contains("unrelated"), "log --about a@h drops x@h: {stdout:?}");

    let output = run_client(&broker.addr, "observer@h", "log", &["--about", "not-an-address"]);
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty(), "stdout is empty");
    let stderr = stderr_text(&output);
    assert!(stderr.starts_with("chattrs: "), "stderr names chattrs: {stderr:?}");
}

#[test]
fn reply_json_prints_a_sent_line() {
    let broker = Broker::start();
    let sent = run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "question"]);
    assert_eq!(sent.status.code(), Some(0));
    let id: u64 = stdout_text(&sent).trim().parse().expect("send prints the id");

    let output = run_client(&broker.addr, "b@h", "reply", &["--json", &id.to_string(), "answer"]);
    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_text(&output);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 2, "preamble plus the sent line: {stdout:?}");
    let preamble = parse_line(lines[0]);
    assert_eq!(preamble["type"].as_str(), Some("message"));
    assert_eq!(preamble["body"].as_str(), Some("question"));
    let sent_line = parse_line(lines[1]);
    assert_eq!(sent_line["type"].as_str(), Some("sent"));
    let reply_id = sent_line["id"].as_u64().expect("sent line carries an id");
    assert!(reply_id > id, "the reply id follows the question");
}

#[test]
fn closed_stdout_does_not_panic() {
    let broker = Broker::start();
    seed_history(&broker.addr, 2000);

    let mut command = common::command(binary());
    command.arg("log").arg("--server").arg(&broker.addr);
    command.env("CHATTRS_ADDRESS", "reader@h");
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn log");
    let stdout = child.stdout.take().expect("log stdout");
    let mut stderr = child.stderr.take().expect("log stderr");
    let mut reader = BufReader::new(stdout);
    let mut first = String::new();
    reader.read_line(&mut first).expect("log prints a first line");
    assert!(!first.is_empty(), "log prints before the pipe closes");
    drop(reader);

    let deadline = Instant::now() + Duration::from_secs(15);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll log") {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("log did not exit after its stdout closed");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let mut err_bytes = Vec::new();
    use std::io::Read as _;
    stderr.read_to_end(&mut err_bytes).expect("read log stderr");
    let stderr = String::from_utf8(err_bytes).expect("stderr is utf-8");
    assert_eq!(status.code(), Some(0), "closed stdout exits zero");
    assert!(!stderr.contains("panicked"), "no panic on a closed stdout: {stderr:?}");
}

#[test]
fn watch_stdout_is_json_lines_only() {
    let broker = Broker::start();
    let mut command = common::command(binary());
    command.arg("watch").arg("--server").arg(&broker.addr);
    command.env("CHATTRS_ADDRESS", "watcher@h");
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

/// Spawn `watch` for `address` with stderr piped.
fn spawn_watch(server: &str, address: &str) -> KillOnDrop {
    let mut command = common::command(binary());
    command.arg("watch").arg("--server").arg(server);
    command.env("CHATTRS_ADDRESS", address);
    command.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::piped());
    KillOnDrop(command.spawn().expect("spawn watch"))
}

#[test]
fn second_watcher_warns_on_stderr_and_the_first_stays_silent() {
    let broker = Broker::start();
    let mut first = spawn_watch(&broker.addr, "dup@h");
    let mut online = false;
    for _ in 0..100 {
        let agents = run_client(&broker.addr, "obs@h", "agents", &["--json"]);
        online = stdout_text(&agents)
            .lines()
            .map(parse_line)
            .any(|agent| agent["address"] == "dup@h" && agent["online"] == true);
        if online {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(online, "first watcher never came online");

    let mut second = spawn_watch(&broker.addr, "dup@h");
    let stderr = second.0.stderr.take().expect("stderr");
    let (sender, receiver) = mpsc::channel();
    let _reader = std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let _ = sender.send(line);
        }
    });
    let line = receiver.recv_timeout(std::time::Duration::from_secs(10)).expect("a warning line");
    assert!(line.starts_with("chattrs: warning: dup@h already has a live watch"), "{line}");

    let mut stderr = String::new();
    let mut pipe = first.0.stderr.take().expect("stderr");
    let _ = first.0.kill();
    std::io::Read::read_to_string(&mut pipe, &mut stderr).unwrap();
    assert!(!stderr.contains("warning"), "first watcher warned: {stderr}");
}

/// Run `chattrs hook EVENT` with the given stdin. `address` of `None`
/// leaves `CHATTRS_ADDRESS` unset.
fn run_hook(server: &str, address: Option<&str>, event: &str, stdin: &str) -> Output {
    run_hook_with(server, address, event, stdin, &[])
}

/// `run_hook` with extra flags, such as `--agent codex`.
fn run_hook_with(
    server: &str,
    address: Option<&str>,
    event: &str,
    stdin: &str,
    flags: &[&str],
) -> Output {
    let mut command = common::command(binary());
    command.args(["hook", event]).arg("--server").arg(server).args(flags);
    command.env_remove("CHATTRS_ADDRESS");
    if let Some(address) = address {
        command.env("CHATTRS_ADDRESS", address);
    }
    command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn the hook");
    child.stdin.take().expect("hook stdin").write_all(stdin.as_bytes()).expect("write stdin");
    child.wait_with_output().expect("run the hook")
}

#[test]
fn hook_is_a_no_op_without_an_address() {
    let broker = Broker::start();
    run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "hi"]);
    let output = run_hook(&broker.addr, None, "post-tool-use", "{}");
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());
}

#[test]
fn post_tool_use_hook_returns_wrapped_messages_and_acks_them() {
    let broker = Broker::start();
    run_client(&broker.addr, "b@h", "inbox", &[]);
    run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "hello hook"]);
    let output = run_hook(&broker.addr, Some("b@h"), "post-tool-use", "{}");
    assert_eq!(output.status.code(), Some(0));
    let value = parse_line(stdout_text(&output).trim());
    assert_eq!(value["hookSpecificOutput"]["hookEventName"], "PostToolUse");
    let context = value["hookSpecificOutput"]["additionalContext"].as_str().expect("context");
    assert!(context.starts_with("Agent messages below."), "{context}");
    assert!(context.contains("<agent-message") && context.contains("hello hook"), "{context}");
    let inbox = run_client(&broker.addr, "b@h", "inbox", &[]);
    assert!(inbox.stdout.is_empty(), "the hook acked: {:?}", stdout_text(&inbox));
}

#[test]
fn hook_prints_nothing_when_nothing_is_unread() {
    let broker = Broker::start();
    for event in ["user-prompt-submit", "post-tool-use", "stop"] {
        let output = run_hook(&broker.addr, Some("b@h"), event, "{}");
        assert_eq!(output.status.code(), Some(0));
        assert!(output.stdout.is_empty(), "{event}");
    }
}

#[test]
fn stop_hook_blocks_with_unread_messages_unless_stop_hook_active() {
    let broker = Broker::start();
    run_client(&broker.addr, "b@h", "inbox", &[]);
    run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "stop me"]);
    let active = run_hook(&broker.addr, Some("b@h"), "stop", r#"{"stop_hook_active":true}"#);
    assert!(active.stdout.is_empty());
    assert_eq!(agent_state(&broker.addr, "b@h"), "idle");
    let output = run_hook(&broker.addr, Some("b@h"), "stop", "not json");
    let value = parse_line(stdout_text(&output).trim());
    assert_eq!(value["decision"], "block");
    assert!(value["reason"].as_str().expect("reason").contains("stop me"));
    let again = run_hook(&broker.addr, Some("b@h"), "stop", "{}");
    assert!(again.stdout.is_empty(), "the block acked the message");
}

#[test]
fn concurrent_hooks_deliver_a_message_exactly_once() {
    let broker = Broker::start();
    run_client(&broker.addr, "b@h", "inbox", &[]);
    run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "only once"]);
    let runs: Vec<_> = (0..2)
        .map(|_| {
            let server = broker.addr.clone();
            std::thread::spawn(move || run_hook(&server, Some("b@h"), "post-tool-use", "{}"))
        })
        .collect();
    let outputs: Vec<Output> = runs.into_iter().map(|run| run.join().expect("hook")).collect();
    let emitted = outputs.iter().filter(|output| !output.stdout.is_empty()).count();
    assert_eq!(emitted, 1, "exactly one run emits the message");
}

#[test]
fn hook_emits_nothing_when_the_ack_fails() {
    // A broker that serves welcome and inbox, then drops the connection at
    // the ack.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let server = listener.local_addr().expect("addr").to_string();
    std::thread::spawn(move || {
        while let Ok((stream, _)) = listener.accept() {
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            let mut stream = stream;
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap_or(0) > 0 {
                let reply = if line.contains("\"hello\"") {
                    r#"{"type":"welcome","server":"t","v":1,"address":"b@h"}"#
                } else if line.contains("\"inbox\"") {
                    r#"{"type":"inbox","messages":[{"id":1,"from":"a@h","to":"b@h","body":"lost?","ts":0}]}"#
                } else {
                    break;
                };
                writeln!(stream, "{reply}").expect("reply");
                line.clear();
            }
        }
    });
    for event in ["post-tool-use", "user-prompt-submit", "stop"] {
        let output = run_hook(&server, Some("b@h"), event, "{}");
        assert_eq!(output.status.code(), Some(0));
        assert!(output.stdout.is_empty(), "{event} emitted without a confirmed claim");
    }
}

#[test]
fn hook_debug_file_gets_one_json_line_per_run() {
    let broker = Broker::start();
    run_client(&broker.addr, "b@h", "inbox", &[]);
    run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "trace me"]);
    let dir = tempfile::TempDir::new().expect("temp dir");
    let log = dir.path().join("hook.log");
    for _ in 0..2 {
        let mut command = common::command(binary());
        command.args(["hook", "post-tool-use"]).arg("--server").arg(&broker.addr);
        command.env("CHATTRS_ADDRESS", "b@h").env("CHATTRS_HOOK_DEBUG", &log);
        command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn the hook");
        child.stdin.take().expect("stdin").write_all(br#"{"tool_name":"Bash"}"#).expect("stdin");
        child.wait_with_output().expect("run the hook");
    }
    let text = std::fs::read_to_string(&log).expect("debug file");
    let lines: Vec<serde_json::Value> = text.lines().map(parse_line).collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["event"], "PostToolUse");
    assert_eq!(lines[0]["input"]["tool_name"], "Bash");
    assert!(lines[0]["pid"].as_u64().is_some() && lines[0]["ts_ms"].as_u64().is_some());
    assert_eq!(lines[0]["fetched"].as_array().unwrap().len(), 1);
    assert_eq!(lines[0]["emitted"].as_array().unwrap().len(), 1);
    assert!(lines[1]["fetched"].as_array().unwrap().is_empty());
}

#[test]
fn hook_with_an_unreachable_broker_is_silent_fast_and_exits_zero() {
    let server = format!("127.0.0.1:{}", free_port());
    let started = Instant::now();
    let output = run_hook(&server, Some("b@h"), "post-tool-use", "{}");
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());
    assert!(started.elapsed() < Duration::from_secs(1), "{:?}", started.elapsed());
}

#[test]
fn session_start_hook_differs_for_claude_and_codex() {
    let server = format!("127.0.0.1:{}", free_port());
    let claude = run_hook(&server, Some("b@h"), "session-start", r#"{"source":"startup"}"#);
    let codex = run_hook_with(&server, Some("b@h"), "session-start", "{}", &["--agent", "codex"]);
    let explicit =
        run_hook_with(&server, Some("b@h"), "session-start", "{}", &["--agent", "claude"]);
    assert_eq!(stdout_text(&claude), stdout_text(&explicit));
    let text = |output: &Output| {
        let value = parse_line(stdout_text(output).trim());
        assert_eq!(value["hookSpecificOutput"]["hookEventName"], "SessionStart");
        value["hookSpecificOutput"]["additionalContext"].as_str().expect("context").to_string()
    };
    let (claude, codex) = (text(&claude), text(&codex));
    assert!(claude.contains("b@h") && claude.contains("chattrs skill"));
    assert!(claude.contains("chattrs watch") && claude.contains("Monitor"));
    assert!(codex.contains("chattrs skill") && !codex.contains("Monitor"));
}

/// Run `session-start` in process dir `process_dir`, with the hook input
/// naming `hook_cwd`, and return its context text.
fn session_context_in(process_dir: &std::path::Path, hook_cwd: &std::path::Path) -> String {
    let server = format!("127.0.0.1:{}", free_port());
    let mut command = common::command(binary());
    command.args(["hook", "session-start"]).arg("--server").arg(server);
    command.env("CHATTRS_ADDRESS", "b@h").current_dir(process_dir);
    command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn the hook");
    let input = serde_json::json!({ "cwd": hook_cwd }).to_string();
    child.stdin.take().expect("stdin").write_all(input.as_bytes()).expect("write stdin");
    let output = child.wait_with_output().expect("run the hook");
    assert_eq!(output.status.code(), Some(0));
    let value = parse_line(stdout_text(&output).trim());
    value["hookSpecificOutput"]["additionalContext"].as_str().expect("context").to_string()
}

#[test]
fn hook_reads_the_repo_config_from_the_hook_cwd_not_the_process_cwd() {
    let process_dir = tempfile::tempdir().unwrap();
    let hook_dir = tempfile::tempdir().unwrap();
    std::fs::write(process_dir.path().join(".chattrs.toml"), "project = \"procproj\"\n").unwrap();
    std::fs::write(hook_dir.path().join(".chattrs.toml"), "project = \"hookproj\"\n").unwrap();
    let text = session_context_in(process_dir.path(), hook_dir.path());
    assert!(text.contains("Your chattrs project is hookproj."), "{text}");
}

#[test]
fn hook_takes_the_lead_from_the_hook_cwd_config() {
    let process_dir = tempfile::tempdir().unwrap();
    let hook_dir = tempfile::tempdir().unwrap();
    std::fs::write(
        process_dir.path().join(".chattrs.toml"),
        "project = \"procproj\"\nlead = \"plead@m\"\n",
    )
    .unwrap();
    std::fs::write(
        hook_dir.path().join(".chattrs.toml"),
        "project = \"hookproj\"\nlead = \"hlead@m\"\n",
    )
    .unwrap();
    let text = session_context_in(process_dir.path(), hook_dir.path());
    assert!(text.contains("Your lead is hlead@m."), "{text}");
    assert!(!text.contains("plead@m"), "{text}");
}

#[test]
fn hook_has_no_lead_when_the_hook_cwd_config_sets_none() {
    let process_dir = tempfile::tempdir().unwrap();
    let hook_dir = tempfile::tempdir().unwrap();
    std::fs::write(
        process_dir.path().join(".chattrs.toml"),
        "project = \"procproj\"\nlead = \"plead@m\"\n",
    )
    .unwrap();
    std::fs::write(hook_dir.path().join(".chattrs.toml"), "project = \"hookproj\"\n").unwrap();
    let text = session_context_in(process_dir.path(), hook_dir.path());
    assert!(!text.contains("plead@m"), "{text}");
    assert!(!text.contains("Your lead"), "{text}");
}

#[test]
fn hook_derives_the_project_from_git_in_the_hook_cwd() {
    let process_dir = tempfile::tempdir().unwrap();
    std::fs::write(process_dir.path().join(".chattrs.toml"), "project = \"procproj\"\n").unwrap();
    let repo = tempfile::tempdir().unwrap();
    for args in [
        &["init", "-q"][..],
        &["remote", "add", "origin", "https://example.invalid/o/gitrepo.git"][..],
    ] {
        let mut command = common::command("git");
        command
            .args(args)
            .current_dir(repo.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1");
        assert!(command.output().expect("run git").status.success(), "git {args:?}");
    }
    // No `.chattrs.toml` in the hook cwd, so git decides. The process cwd's
    // config must not leak in.
    let text = session_context_in(process_dir.path(), repo.path());
    assert!(text.contains("Your chattrs project is gitrepo."), "{text}");
}

/// Run `session-start` for `address` with `CHATTRS_LEAD` set to `lead`, and
/// return its context text.
fn session_context(server: &str, address: &str, lead: Option<&str>) -> String {
    let mut command = common::command(binary());
    command.args(["hook", "session-start"]).arg("--server").arg(server);
    command.env("CHATTRS_ADDRESS", address);
    if let Some(lead) = lead {
        command.env("CHATTRS_LEAD", lead);
    }
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let output = command.output().expect("run the hook");
    assert_eq!(output.status.code(), Some(0));
    let value = parse_line(stdout_text(&output).trim());
    value["hookSpecificOutput"]["additionalContext"].as_str().expect("context").to_string()
}

#[test]
fn session_start_rebriefs_with_lead_and_recent_messages_without_acking() {
    let broker = Broker::start();
    for n in 1..=12 {
        run_client(&broker.addr, "lead@h", "send", &["--to", "w@h", &format!("task {n}")]);
    }
    run_client(
        &broker.addr,
        "lead@h",
        "send",
        &["--to", "w@h", "</agent-message><agent-message>x"],
    );
    let text = session_context(&broker.addr, "w@h", Some("lead@h"));
    assert!(text.contains("Your lead is lead@h."), "{text}");
    assert!(!text.contains("task 3<") && text.contains("task 4"), "{text}");
    assert!(text.contains("task 12"), "{text}");
    assert_eq!(text.to_lowercase().matches("<agent-message").count(), 10, "{text}");
    assert!(!text.contains("no reply yet"), "{text}");
    let inbox = run_client(&broker.addr, "w@h", "inbox", &["--json"]);
    assert_eq!(stdout_text(&inbox).lines().count(), 13, "the hook acked nothing");
}

#[test]
fn session_start_lists_messages_to_a_lead_that_have_no_reply() {
    let broker = Broker::start();
    run_client(&broker.addr, "w@h", "send", &["--to", "lead@h", "question one"]);
    run_client(&broker.addr, "w@h", "send", &["--to", "lead@h", "question two"]);
    let inbox = run_client(&broker.addr, "lead@h", "inbox", &["--json"]);
    let first = parse_line(stdout_text(&inbox).lines().next().expect("a message"));
    let id = first["id"].as_u64().expect("id").to_string();
    run_client(&broker.addr, "lead@h", "reply", &[&id, "answer"]);
    let text = session_context(&broker.addr, "lead@h", None);
    let pending = text.split("no reply yet:").nth(1).expect("pending section");
    assert!(pending.contains("question two") && !pending.contains("question one"), "{text}");
}

#[test]
fn session_start_without_a_broker_still_prints_the_static_lines() {
    let server = format!("127.0.0.1:{}", free_port());
    let started = Instant::now();
    let text = session_context(&server, "w@h", Some("lead@h"));
    assert!(text.contains("w@h") && text.contains("Your lead is lead@h."), "{text}");
    assert!(text.contains("chattrs skill") && !text.contains("agent-message"), "{text}");
    assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
}

/// A fake home with both agent config dirs, for the install tests.
struct FakeHome {
    dir: tempfile::TempDir,
}

impl FakeHome {
    fn new() -> FakeHome {
        let dir = tempfile::TempDir::new().expect("temp home");
        std::fs::create_dir_all(dir.path().join(".claude")).expect("claude dir");
        std::fs::create_dir_all(dir.path().join(".codex")).expect("codex dir");
        FakeHome { dir }
    }

    fn claude(&self) -> PathBuf {
        self.dir.path().join(".claude/settings.json")
    }

    fn codex(&self) -> PathBuf {
        self.dir.path().join(".codex/hooks.json")
    }

    /// Run `chattrs hooks ARGS` with HOME in the fake home, from `cwd`.
    fn hooks_in(&self, cwd: &std::path::Path, args: &[&str]) -> Output {
        let mut command = common::command(binary());
        command.arg("hooks").args(args);
        command.env("HOME", self.dir.path()).env_remove("CODEX_HOME");
        command.current_dir(cwd);
        command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
        command.output().expect("run hooks")
    }

    fn hooks(&self, args: &[&str]) -> Output {
        self.hooks_in(self.dir.path(), args)
    }
}

const SETTINGS: &str = "{\n  \"model\": \"x\",\n  \"hooks\": {\n    \"Stop\": [\n      {\n        \
\"hooks\": [\n          {\n            \"type\": \"command\",\n            \"command\": \
\"other.sh\"\n          }\n        ]\n      }\n    ],\n    \"PreToolUse\": []\n  }\n}\n";

#[test]
fn hooks_install_twice_is_byte_identical_and_remove_restores_the_original() {
    let home = FakeHome::new();
    std::fs::write(home.claude(), SETTINGS).unwrap();
    let first = home.hooks(&["install"]);
    assert_eq!(first.status.code(), Some(0), "{}", stderr_text(&first));
    let installed = std::fs::read_to_string(home.claude()).unwrap();
    assert!(installed.contains("chattrs hook post-tool-use") && installed.contains("other.sh"));
    assert!(home.codex().exists());
    home.hooks(&["install"]);
    assert_eq!(std::fs::read_to_string(home.claude()).unwrap(), installed);
    let removed = home.hooks(&["remove"]);
    assert_eq!(removed.status.code(), Some(0));
    assert_eq!(std::fs::read_to_string(home.claude()).unwrap(), SETTINGS);
}

#[test]
fn hooks_install_says_the_next_step_for_each_agent() {
    let home = FakeHome::new();
    let stderr = stderr_text(&home.hooks(&["install"]));
    assert!(stderr.contains("codex: open /hooks in Codex and trust the chattrs hooks"), "{stderr}");
    assert!(stderr.contains("claude: Claude Code may ask you to review hooks"), "{stderr}");
    assert!(stderr.contains("PATH"), "{stderr}");
}

#[test]
fn hooks_install_with_invalid_json_exits_one_and_leaves_the_file() {
    let home = FakeHome::new();
    std::fs::write(home.claude(), "{ not json").unwrap();
    let output = home.hooks(&["install", "--agent", "claude"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(std::fs::read_to_string(home.claude()).unwrap(), "{ not json");
    assert!(!home.dir.path().join(".claude/settings.json.bak").exists());
}

#[test]
fn hooks_install_writes_the_backup_once() {
    let home = FakeHome::new();
    std::fs::write(home.claude(), SETTINGS).unwrap();
    home.hooks(&["install", "--agent", "claude"]);
    let backup = home.dir.path().join(".claude/settings.json.bak");
    assert_eq!(std::fs::read_to_string(&backup).unwrap(), SETTINGS);
    home.hooks(&["remove", "--agent", "claude"]);
    home.hooks(&["install", "--agent", "claude"]);
    assert_eq!(std::fs::read_to_string(&backup).unwrap(), SETTINGS);
}

#[test]
fn hooks_install_all_skips_a_missing_agent_dir_but_explicit_creates_it() {
    let home = FakeHome::new();
    std::fs::remove_dir_all(home.dir.path().join(".codex")).unwrap();
    let output = home.hooks(&["install"]);
    assert!(stderr_text(&output).contains("skipping codex"), "{}", stderr_text(&output));
    assert!(!home.codex().exists());
    home.hooks(&["install", "--agent", "codex"]);
    assert!(home.codex().exists());
}

#[test]
fn hooks_project_edits_the_files_in_the_current_dir() {
    let home = FakeHome::new();
    let project = home.dir.path().join("proj");
    std::fs::create_dir_all(project.join(".claude")).unwrap();
    home.hooks_in(&project, &["install", "--project", "--agent", "claude"]);
    let text = std::fs::read_to_string(project.join(".claude/settings.json")).unwrap();
    assert!(text.contains("chattrs hook stop"));
    assert!(!home.claude().exists());
}

/// The state `agents --json` reports for `address`, or null.
fn agent_state(server: &str, address: &str) -> serde_json::Value {
    let output = run_client(server, "obs@h", "agents", &["--json"]);
    stdout_text(&output)
        .lines()
        .map(parse_line)
        .find(|agent| agent["address"] == address)
        .map_or(serde_json::Value::Null, |agent| agent["state"].clone())
}

#[test]
fn user_prompt_submit_hook_marks_the_agent_working() {
    let broker = Broker::start();
    run_hook(&broker.addr, Some("b@h"), "user-prompt-submit", "{}");
    assert_eq!(agent_state(&broker.addr, "b@h"), "working");
}

#[test]
fn session_start_hook_marks_the_agent_idle() {
    let broker = Broker::start();
    run_hook(&broker.addr, Some("b@h"), "session-start", "{}");
    assert_eq!(agent_state(&broker.addr, "b@h"), "idle");
}

#[test]
fn post_tool_use_hook_leaves_the_state_alone() {
    let broker = Broker::start();
    run_hook(&broker.addr, Some("b@h"), "user-prompt-submit", "{}");
    run_hook(&broker.addr, Some("b@h"), "post-tool-use", "{}");
    assert_eq!(agent_state(&broker.addr, "b@h"), "working");
}

#[test]
fn stop_hook_with_nothing_unread_marks_the_agent_idle() {
    let broker = Broker::start();
    run_hook(&broker.addr, Some("b@h"), "user-prompt-submit", "{}");
    let output = run_hook(&broker.addr, Some("b@h"), "stop", "{}");
    assert!(output.stdout.is_empty());
    assert_eq!(agent_state(&broker.addr, "b@h"), "idle");
}

#[test]
fn stop_hook_that_blocks_does_not_leave_the_agent_idle() {
    let broker = Broker::start();
    run_hook(&broker.addr, Some("b@h"), "session-start", "{}");
    run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "keep going"]);
    let output = run_hook(&broker.addr, Some("b@h"), "stop", "{}");
    assert!(stdout_text(&output).contains("block"));
    assert_eq!(agent_state(&broker.addr, "b@h"), "working");
}

#[test]
fn hooks_install_writes_the_agent_flag_into_each_config() {
    let home = FakeHome::new();
    home.hooks(&["install"]);
    let claude = std::fs::read_to_string(home.claude()).unwrap();
    let codex = std::fs::read_to_string(home.codex()).unwrap();
    assert!(claude.contains("chattrs hook session-start --agent claude"));
    assert!(codex.contains("chattrs hook stop --agent codex"));
    assert!(!claude.contains("--agent codex") && !codex.contains("--agent claude"));
}

#[test]
fn log_tail_prints_only_the_newest_messages() {
    let broker = Broker::start();
    for body in ["one", "two", "three", "four"] {
        let sent = run_client(&broker.addr, "a@h", "send", &["--to", "b@h", body]);
        assert_eq!(sent.status.code(), Some(0));
    }
    let output = run_client(&broker.addr, "observer@h", "log", &["--tail", "2"]);
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("one") && !stdout.contains("two"), "{stdout:?}");
    assert!(stdout.find("three").unwrap() < stdout.find("four").unwrap(), "{stdout:?}");
}

/// Run `chattrs <args>` with a clean env, a temp HOME, and a temp cwd.
fn run_in(
    home: &std::path::Path,
    cwd: &std::path::Path,
    envs: &[(&str, &str)],
    args: &[&str],
) -> Output {
    let mut command = common::command(binary());
    command.args(args).current_dir(cwd);
    command.env("HOME", home).envs(envs.iter().copied());
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    command.output().expect("run chattrs")
}

fn write_private(path: &std::path::Path, text: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn user_config_token_file_reaches_a_token_broker() {
    let broker = Broker::start_with(&["--token", "sekret"]);
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    write_private(&home.join("tok"), "sekret\n");
    write_private(
        &home.join(".config/chattrs/config.toml"),
        &format!("server = \"{}\"\ntoken_file = \"{}\"\n", broker.addr, home.join("tok").display()),
    );
    let envs = [("CHATTRS_ADDRESS", "a@h")];
    let ok = run_in(home, home, &envs, &["send", "--to", "b@h", "hi"]);
    assert_eq!(ok.status.code(), Some(0), "{}", stderr_text(&ok));
    // An explicit token beats the file and is refused.
    let bad = run_in(home, home, &envs, &["send", "--token", "wrong", "--to", "b@h", "hi"]);
    assert_eq!(bad.status.code(), Some(1));
    // The env beats the file as well.
    let bad = run_in(
        home,
        home,
        &[("CHATTRS_TOKEN", "wrong"), ("CHATTRS_ADDRESS", "a@h")],
        &["send", "--to", "b@h", "hi"],
    );
    assert_eq!(bad.status.code(), Some(1));
}

#[test]
fn repo_file_gives_the_project_and_loses_to_env_and_flag() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    std::fs::create_dir(cwd.join(".git")).unwrap();
    std::fs::write(cwd.join(".chattrs.toml"), "project = \"repo\"\nlead = \"l@h\"\n").unwrap();
    let env_out = |envs: &[(&str, &str)], extra: &[&str]| {
        let mut args = vec!["env", "--as", "w@h"];
        args.extend(extra);
        stdout_text(&run_in(cwd, cwd, envs, &args))
    };
    let out = env_out(&[], &[]);
    assert!(out.contains("export CHATTRS_PROJECT='repo'\n"), "{out}");
    assert!(out.contains("export CHATTRS_LEAD='l@h'\n"), "{out}");
    let out = env_out(&[("CHATTRS_PROJECT", "fromenv")], &[]);
    assert!(out.contains("CHATTRS_PROJECT='fromenv'"), "{out}");
    let out = env_out(&[("CHATTRS_PROJECT", "fromenv")], &["--project", "flag"]);
    assert!(out.contains("CHATTRS_PROJECT='flag'"), "{out}");
}

#[test]
fn a_secret_in_the_repo_file_stops_the_command() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join(".git")).unwrap();
    std::fs::write(dir.path().join(".chattrs.toml"), "token = \"x\"\n").unwrap();
    let out = run_in(dir.path(), dir.path(), &[], &["env", "--as", "w@h"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr_text(&out).contains(".chattrs.toml:1"), "{}", stderr_text(&out));
}

#[test]
fn env_output_evals_in_sh_with_a_fresh_session_and_no_token() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    write_private(&cwd.join("tok"), "sekret\n");
    write_private(
        &cwd.join(".config/chattrs/config.toml"),
        &format!("token_file = \"{}\"\n", cwd.join("tok").display()),
    );
    let hostile = [("CHATTRS_TAGS", "it's $(touch pwned) \\\"x")];
    let first = stdout_text(&run_in(cwd, cwd, &hostile, &["env", "--as", "w@h"]));
    let second = stdout_text(&run_in(cwd, cwd, &hostile, &["env", "--as", "w@h"]));
    assert!(!first.contains("sekret") && !first.contains("CHATTRS_TOKEN"), "{first}");
    let session = |text: &str| {
        text.lines().find(|l| l.starts_with("export CHATTRS_SESSION=")).unwrap().to_string()
    };
    assert_ne!(session(&first), session(&second));
    let script = format!("eval '{}'; printf '%s|%s|%s' \"$CHATTRS_ADDRESS\" \"$CHATTRS_TAGS\" \"${{CHATTRS_TOKEN-unset}}\"", first.replace('\'', "'\\''"));
    let sh = common::command("sh").args(["-c", &script]).current_dir(cwd).output().unwrap();
    assert_eq!(stdout_text(&sh), "w@h|it's $(touch pwned) \\\"x|unset");
    assert!(!cwd.join("pwned").exists());
    let with = stdout_text(&run_in(cwd, cwd, &[], &["env", "--as", "w@h", "--with-token"]));
    assert!(with.contains("export CHATTRS_TOKEN='sekret'\n"), "{with}");
}

#[test]
fn first_command_retries_when_a_starting_broker_drops_the_connection() {
    // A broker that is starting can accept a connection and then drop it
    // before it answers `hello`. On a loopback server the first command
    // retries on a fresh connection instead of failing (CLI-11). The
    // stand-in below drops the first connection, then frees the port so
    // the client starts the real broker.
    let dir = tempfile::TempDir::new().expect("make a temp dir");
    let db = dir.path().join("c.db");
    let (listener, addr) = loop {
        let addr = format!("127.0.0.1:{}", free_port());
        if let Ok(listener) = std::net::TcpListener::bind(&addr) {
            break (listener, addr);
        }
    };
    let dropper = std::thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept the client");
        drop(stream);
    });
    let mut command = common::command(binary());
    command.arg("inbox").arg("--server").arg(&addr);
    command.env("CHATTRS_ADDRESS", "a@h");
    command.env("CHATTRS_DB", &db);
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let output = command.output().expect("run the client");
    dropper.join().expect("stand-in thread");
    assert_eq!(output.status.code(), Some(0), "a drop during start is retried: {output:?}");

    let mut stop = common::command(binary());
    stop.arg("stop").env("CHATTRS_DB", &db);
    stop.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let stopped = stop.output().expect("stop the broker");
    assert_eq!(stopped.status.code(), Some(0), "stop exits zero: {stopped:?}");
}

/// Stops the broker that autostart left under a fake home, even when an
/// assert fails first.
struct StopOnDrop {
    home: PathBuf,
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let mut stop = common::command(binary());
        stop.arg("stop");
        clean_home_env(&mut stop, &self.home);
        stop.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
        let _ = stop.status();
    }
}

/// Point every path the client reads at `home` and clear the overrides.
fn clean_home_env(command: &mut Command, home: &std::path::Path) {
    command
        .env("HOME", home)
        .env("CODEX_HOME", home.join(".codex"))
        .env_remove("XDG_DATA_HOME")
        .env_remove("XDG_CONFIG_HOME")
        .env_remove("CHATTRS_ADDRESS")
        .current_dir(home);
}

/// Parse `export NAME='value'` lines, undoing the `'\''` escape.
fn parse_exports(text: &str) -> std::collections::HashMap<String, String> {
    text.lines()
        .filter_map(|line| line.strip_prefix("export ")?.split_once('='))
        .map(|(name, value)| {
            let inner =
                value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')).unwrap_or(value);
            (name.to_string(), inner.replace("'\\''", "'"))
        })
        .collect()
}

#[test]
fn readme_solo_path_works_in_a_clean_home() {
    // The README's solo steps on a brand-new machine: no data directory, no
    // config. The first command that needs a broker must start one.
    let started = Instant::now();
    let dir = tempfile::TempDir::new().expect("make a temp home");
    let home = dir.path().to_path_buf();
    let _stop = StopOnDrop { home: home.clone() };
    let server = format!("127.0.0.1:{}", free_port());
    std::fs::create_dir_all(home.join(".claude")).expect("claude dir");
    std::fs::create_dir_all(home.join(".codex")).expect("codex dir");
    let run = |args: &[&str], envs: &[(&str, &str)]| -> Output {
        let mut command = common::command(binary());
        command.args(args);
        clean_home_env(&mut command, &home);
        command.envs(envs.iter().copied());
        command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
        command.output().expect("run chattrs")
    };

    let setup = run(&["setup"], &[]);
    assert_eq!(setup.status.code(), Some(0), "setup: {setup:?}");
    for file in [".claude/settings.json", ".codex/hooks.json"] {
        let text = std::fs::read_to_string(home.join(file)).expect("config file written");
        assert!(text.contains("chattrs hook"), "{file} has the hook: {text}");
    }
    let skill = run(&["skill"], &[]);
    assert_eq!(skill.status.code(), Some(0), "skill: {skill:?}");
    assert!(!skill.stdout.is_empty(), "the skill text is not empty");
    for file in [".claude/skills/chattrs/SKILL.md", ".codex/skills/chattrs/SKILL.md"] {
        let text = std::fs::read(home.join(file)).expect("skill file written");
        assert_eq!(text, skill.stdout, "{file} holds the bundled skill");
    }
    assert!(home.join(".config/chattrs/config.toml").is_file(), "setup wrote the config");

    let env = run(&["env", "--as", "w1@mac", "--lead", "lead@mac", "--server", &server], &[]);
    assert_eq!(env.status.code(), Some(0), "env: {env:?}");
    let exports = parse_exports(&stdout_text(&env));
    for name in ["CHATTRS_ADDRESS", "CHATTRS_SESSION", "CHATTRS_LEAD"] {
        assert!(exports.contains_key(name), "{name} is exported: {exports:?}");
    }
    let worker: Vec<(&str, &str)> =
        exports.iter().map(|(name, value)| (name.as_str(), value.as_str())).collect();

    let lead = [("CHATTRS_ADDRESS", "lead@mac"), ("CHATTRS_SERVER", server.as_str())];
    let sent = run(&["send", "--to", "w1@mac", "hello"], &lead);
    assert_eq!(sent.status.code(), Some(0), "send autostarts a broker: {sent:?}");
    let inbox = run(&["inbox"], &worker);
    assert_eq!(inbox.status.code(), Some(0), "inbox: {inbox:?}");
    assert!(stdout_text(&inbox).contains("hello"), "the worker sees the message: {inbox:?}");

    assert!(started.elapsed() < Duration::from_secs(10), "too slow: {:?}", started.elapsed());
}

/// The bytes of every file `setup` or a user may own, keyed by path.
fn snapshot(home: &std::path::Path, files: &[&str]) -> Vec<Option<Vec<u8>>> {
    files.iter().map(|file| std::fs::read(home.join(file)).ok()).collect()
}

#[test]
fn setup_is_safe_to_run_again() {
    let dir = tempfile::TempDir::new().expect("make a temp home");
    let home = dir.path().to_path_buf();
    std::fs::create_dir_all(home.join(".claude")).expect("claude dir");
    std::fs::create_dir_all(home.join(".codex")).expect("codex dir");
    let run_setup = || -> Output {
        let mut command = common::command(binary());
        command.arg("setup");
        clean_home_env(&mut command, &home);
        command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
        command.output().expect("run chattrs setup")
    };
    let config = home.join(".config/chattrs/config.toml");
    let skill = home.join(".claude/skills/chattrs/SKILL.md");
    let user_files = [".claude/settings.json", ".codex/hooks.json"];

    let first = run_setup();
    assert_eq!(first.status.code(), Some(0), "first setup: {first:?}");
    let stderr = String::from_utf8_lossy(&first.stderr).into_owned();
    assert!(stderr.contains("/hooks in Codex") && stderr.contains("Claude Code"), "{stderr}");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&config).expect("config exists").permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "the config is private");
    }

    // The user edits the config; an upgrade leaves an old skill text behind.
    std::fs::write(&config, "server = \"10.0.0.5:4646\"\n").expect("edit the config");
    std::fs::write(&skill, "old skill text\n").expect("age the skill");
    let before = snapshot(&home, &user_files);
    let second = run_setup();
    assert_eq!(second.status.code(), Some(0), "second setup: {second:?}");
    assert_eq!(snapshot(&home, &user_files), before, "user settings are untouched");
    assert_eq!(
        std::fs::read_to_string(&config).expect("read the config"),
        "server = \"10.0.0.5:4646\"\n",
        "an existing config is kept"
    );
    let bundled = std::fs::read(home.join(".codex/skills/chattrs/SKILL.md")).expect("codex skill");
    assert_eq!(std::fs::read(&skill).expect("claude skill"), bundled, "the skill is refreshed");
}

#[test]
fn setup_skips_an_agent_that_is_not_installed() {
    let dir = tempfile::TempDir::new().expect("make a temp home");
    let home = dir.path().to_path_buf();
    std::fs::create_dir_all(home.join(".claude")).expect("claude dir");
    let mut command = common::command(binary());
    command.arg("setup");
    clean_home_env(&mut command, &home);
    // An empty PATH hides any real `codex` binary from detection.
    command.env("PATH", "");
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let output = command.output().expect("run chattrs setup");
    assert_eq!(output.status.code(), Some(0), "setup: {output:?}");
    assert!(home.join(".claude/skills/chattrs/SKILL.md").is_file());
    assert!(!home.join(".codex").exists(), "codex dir is not created");
}

#[test]
fn env_with_token_picks_the_token_for_the_address() {
    let dir = tempfile::TempDir::new().expect("make a temp home");
    let home = dir.path().to_path_buf();
    let write_token = |name: &str| -> PathBuf {
        let path = home.join(name);
        std::fs::write(&path, format!("tok-{name}\n")).expect("write a token");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        }
        path
    };
    let (plain, lead, host) = (write_token("plain"), write_token("lead"), write_token("host"));
    let config_dir = home.join(".config/chattrs");
    std::fs::create_dir_all(&config_dir).expect("config dir");
    std::fs::write(
        config_dir.join("config.toml"),
        format!(
            "token_file = \"{}\"\n[tokens]\n\"*@mac\" = \"{}\"\n\"lead@mac\" = \"{}\"\n",
            plain.display(),
            host.display(),
            lead.display()
        ),
    )
    .expect("write the config");
    let token_for = |address: &str, token_env: Option<&str>| -> Option<String> {
        let mut command = common::command(binary());
        command.args(["env", "--as", address, "--with-token"]);
        clean_home_env(&mut command, &home);
        if let Some(token) = token_env {
            command.env("CHATTRS_TOKEN", token);
        }
        command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
        let output = command.output().expect("run chattrs env");
        assert_eq!(output.status.code(), Some(0), "env: {output:?}");
        parse_exports(&stdout_text(&output)).remove("CHATTRS_TOKEN")
    };
    assert_eq!(token_for("lead@mac", None).as_deref(), Some("tok-lead"));
    assert_eq!(token_for("w1@mac", None).as_deref(), Some("tok-host"));
    assert_eq!(token_for("w1@box", None).as_deref(), Some("tok-plain"));
    assert_eq!(token_for("lead@mac", Some("explicit")).as_deref(), Some("explicit"));
}

/// Run chattrs in `home` with `args`, feeding `stdin` text.
fn run_with_stdin(home: &std::path::Path, args: &[&str], stdin: &str) -> Output {
    use std::io::Write as _;
    let mut command = common::command(binary());
    command.args(args);
    clean_home_env(&mut command, home);
    command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn chattrs");
    child.stdin.take().expect("stdin").write_all(stdin.as_bytes()).expect("write stdin");
    child.wait_with_output().expect("run chattrs")
}

#[test]
fn setup_broker_then_join_gives_each_identity_its_token() {
    let broker = tempfile::TempDir::new().expect("broker home");
    let client = tempfile::TempDir::new().expect("client home");
    let first = run_with_stdin(
        broker.path(),
        &["setup", "--broker", "--hosts", "mac,bobox", "--bind", "127.0.0.1"],
        "",
    );
    assert_eq!(first.status.code(), Some(0), "setup --broker: {first:?}");
    let tokens = broker.path().join(".config/chattrs/tokens.toml");
    let unit = if cfg!(target_os = "macos") {
        broker.path().join("Library/LaunchAgents/com.chattrs.broker.plist")
    } else {
        broker.path().join(".config/systemd/user/chattrs.service")
    };
    assert!(tokens.is_file() && unit.is_file(), "tokens and unit are written");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&tokens).expect("tokens").permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "tokens.toml is private");
    }
    let unit_text = std::fs::read_to_string(&unit).expect("unit");
    assert!(unit_text.contains("127.0.0.1:4646") && unit_text.contains("--tokens"), "{unit_text}");
    let stdout = stdout_text(&first);
    assert!(stdout.starts_with("Treat these like passwords."), "{stdout}");
    let joins: Vec<(&str, &str)> =
        stdout.lines().filter_map(|line| line.split_once(": chattrs://")).collect();
    let find = |label: &str| {
        let (_, rest) = joins.iter().find(|(l, _)| *l == label).expect(label);
        format!("chattrs://{rest}")
    };
    assert_eq!(joins.len(), 4, "{stdout}");

    let again = run_with_stdin(
        broker.path(),
        &["setup", "--broker", "--hosts", "mac", "--bind", "127.0.0.1"],
        "",
    );
    assert_ne!(again.status.code(), Some(0), "a second run refuses");
    assert!(String::from_utf8_lossy(&again.stderr).contains("tokens.toml"));
    let wild = run_with_stdin(
        client.path(),
        &["setup", "--broker", "--hosts", "mac", "--bind", "0.0.0.0"],
        "",
    );
    assert_ne!(wild.status.code(), Some(0), "a wildcard bind is refused");

    let input = format!("{}\n{}\n", find("lead lead@mac"), find("agent bobox"));
    let joined = run_with_stdin(client.path(), &["setup", "--join", "-"], &input);
    assert_eq!(joined.status.code(), Some(0), "setup --join: {joined:?}");
    let leaked = String::from_utf8_lossy(&joined.stdout).to_string()
        + &String::from_utf8_lossy(&joined.stderr);
    assert!(!leaked.contains("chattrs://"), "no join string in the output");
    let token_of = |address: &str| {
        let out = run_with_stdin(client.path(), &["env", "--as", address, "--with-token"], "");
        assert_eq!(out.status.code(), Some(0), "env: {out:?}");
        parse_exports(&stdout_text(&out)).remove("CHATTRS_TOKEN")
    };
    let secret = |join: String| join.split_once('@').map(|(head, _)| head[10..].to_string());
    assert_eq!(token_of("lead@mac"), secret(find("lead lead@mac")));
    assert_eq!(token_of("w1@bobox"), secret(find("agent bobox")));
    let config =
        std::fs::read_to_string(client.path().join(".config/chattrs/config.toml")).expect("config");
    assert!(config.contains("server = \"127.0.0.1:4646\""), "{config}");
    assert!(client.path().join(".config/chattrs/any-bobox.token").is_file());

    let repeat = run_with_stdin(client.path(), &["setup", "--join", &find("agent bobox")], "");
    assert_ne!(repeat.status.code(), Some(0), "re-joining a pattern refuses");
    let forced =
        run_with_stdin(client.path(), &["setup", "--join", &find("agent bobox"), "--force"], "");
    assert_eq!(forced.status.code(), Some(0), "--force replaces: {forced:?}");
}

#[test]
fn help_never_prints_the_token() {
    let secret = "help-must-not-show-this-token";
    let help = |args: &[&str]| {
        let output = common::command(binary())
            .args(args)
            .env("CHATTRS_TOKEN", secret)
            .output()
            .expect("run chattrs help");
        format!("{}{}", stdout_text(&output), String::from_utf8_lossy(&output.stderr))
    };
    let top = help(&["--help"]);
    let commands: Vec<String> = top
        .lines()
        .skip_while(|line| !line.starts_with("Commands:"))
        .skip(1)
        .take_while(|line| line.starts_with("  "))
        .filter_map(|line| line.split_whitespace().next().map(str::to_string))
        .filter(|name| name != "help")
        .collect();
    assert!(commands.len() > 5, "no subcommands found in help: {top}");
    assert!(!top.contains(secret));
    for name in &commands {
        let text = help(&[name, "--help"]);
        assert!(!text.contains(secret), "`chattrs {name} --help` printed the token");
    }
}

#[test]
fn env_says_where_the_project_came_from() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    let env_out =
        |envs: &[(&str, &str)]| stdout_text(&run_in(cwd, cwd, envs, &["env", "--as", "w@h"]));
    let out = env_out(&[]);
    assert!(out.contains("# project source: default\n"), "{out}");
    assert!(!out.contains("CHATTRS_PROJECT"), "{out}");
    let out = env_out(&[("CHATTRS_PROJECT", "fromenv")]);
    assert!(out.contains("# project source: env\n"), "{out}");
    std::fs::write(cwd.join(".chattrs.toml"), "project = \"repo\"\n").unwrap();
    let out = env_out(&[]);
    assert!(out.contains("# project source: config\n"), "{out}");
}

#[test]
fn env_derives_the_project_from_a_repo() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path().join("My_Repo");
    std::fs::create_dir(&cwd).unwrap();
    let status = std::process::Command::new("git")
        .args(["init", "-q"])
        .arg(&cwd)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("GIT_DIR")
        .status()
        .unwrap();
    assert!(status.success());
    let out = stdout_text(&run_in(dir.path(), &cwd, &[], &["env", "--as", "w@h"]));
    assert!(out.contains("export CHATTRS_PROJECT='my-repo'\n"), "{out}");
    assert!(out.contains("# project source: git\n"), "{out}");
}

/// Run `chattrs exec ARGS...` in `home`, with extra env pairs.
#[cfg(unix)]
fn run_exec(home: &std::path::Path, args: &[&str], envs: &[(&str, &str)]) -> Output {
    let mut command = common::command(binary());
    command.arg("exec").args(args);
    clean_home_env(&mut command, home);
    command.envs(envs.iter().copied());
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    command.output().expect("run chattrs exec")
}

/// The `CHATTRS_*` lines of `env` output, as a sorted map.
#[cfg(unix)]
fn chattrs_vars(text: &str) -> std::collections::BTreeMap<String, String> {
    text.lines()
        .filter(|line| line.starts_with("CHATTRS_"))
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

#[cfg(unix)]
#[test]
fn exec_child_env_matches_env_without_the_token() {
    let dir = tempfile::TempDir::new().expect("temp home");
    let home = dir.path();
    let token = home.join("tok");
    std::fs::write(&token, "secret\n").expect("write token");
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    }
    let config = home.join(".config/chattrs");
    std::fs::create_dir_all(&config).expect("config dir");
    std::fs::write(
        config.join("config.toml"),
        format!("[tokens]\n\"w1@h\" = \"{}\"\n", token.display()),
    )
    .expect("write config");
    let flags = ["--as", "w1@h", "--lead", "lead@h", "--tags", "gpu"];

    let mut env_cmd = common::command(binary());
    env_cmd.arg("env").args(flags);
    clean_home_env(&mut env_cmd, home);
    let env_out = env_cmd.output().expect("run chattrs env");
    assert_eq!(env_out.status.code(), Some(0), "env: {env_out:?}");
    let mut expected = parse_exports(&stdout_text(&env_out));
    let session_env = expected.remove("CHATTRS_SESSION").expect("env session");
    assert!(!expected.contains_key("CHATTRS_TOKEN"));

    let mut args = flags.to_vec();
    args.extend(["--", "env"]);
    let out = run_exec(home, &args, &[]);
    assert_eq!(out.status.code(), Some(0), "exec: {out:?}");
    let mut actual = chattrs_vars(&stdout_text(&out));
    let session_exec = actual.remove("CHATTRS_SESSION").expect("exec session");
    let expected: std::collections::BTreeMap<_, _> = expected.into_iter().collect();
    assert_eq!(actual, expected);
    assert_eq!(session_exec.len(), 32);
    assert_ne!(session_exec, session_env);
    assert!(!stdout_text(&out).contains("secret"), "the token must not reach the child");
}

#[cfg(unix)]
#[test]
fn exec_passes_an_explicit_token_through() {
    let dir = tempfile::TempDir::new().expect("temp home");
    let out = run_exec(
        dir.path(),
        &["--as", "w1@h", "--", "sh", "-c", "echo $CHATTRS_TOKEN"],
        &[("CHATTRS_TOKEN", "explicit")],
    );
    assert_eq!(stdout_text(&out).trim(), "explicit");
    assert!(!stderr_text(&out).contains("no token"));
}

#[cfg(unix)]
#[test]
fn exec_warns_when_no_token_is_found() {
    let dir = tempfile::TempDir::new().expect("temp home");
    let out = run_exec(dir.path(), &["--as", "w1@h", "sh", "-c", "echo ok"], &[]);
    assert_eq!(stdout_text(&out).trim(), "ok");
    assert!(stderr_text(&out).contains("chattrs: no token for w1@h; the broker may refuse"));
}

#[cfg(unix)]
#[test]
fn exec_default_address_is_the_program_basename_at_the_host() {
    let dir = tempfile::TempDir::new().expect("temp home");
    let out = run_exec(dir.path(), &["/bin/sh", "-c", "echo $CHATTRS_ADDRESS"], &[]);
    let host =
        gethostname::gethostname().to_string_lossy().split('.').next().unwrap().to_lowercase();
    assert_eq!(stdout_text(&out).trim(), format!("sh@{host}"));
}

#[cfg(unix)]
#[test]
fn exec_rejects_a_program_name_that_is_no_address_part() {
    let dir = tempfile::TempDir::new().expect("temp home");
    let out = run_exec(dir.path(), &["bad name!"], &[]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr_text(&out).contains("pass --as"), "{}", stderr_text(&out));
}

#[cfg(unix)]
#[test]
fn exec_passes_hyphen_args_and_the_exit_code_to_the_program() {
    let dir = tempfile::TempDir::new().expect("temp home");
    let out = run_exec(
        dir.path(),
        &["--as", "w1@h", "sh", "-c", "echo \"$1 $2\"; exit 7", "x", "--as", "evil@h"],
        &[],
    );
    assert_eq!(out.status.code(), Some(7));
    assert_eq!(stdout_text(&out).trim(), "--as evil@h");
}

/// A scratch dir with a fake `ssh` (logs its argv, one word per line, to
/// `ssh.log`; with `FAKE_SSH_RUN` set it runs the remote string under
/// `sh -c` with PATH set to `FAKE_REMOTE_PATH`) and a `$SHELL` stand-in
/// that skips the `-l`. Returns the dir.
#[cfg(unix)]
fn fake_ssh_dir(name: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = std::env::temp_dir().join(format!("chattrs-fake-ssh-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("bin")).unwrap();
    let script = |path: std::path::PathBuf, body: &str| {
        std::fs::write(&path, body).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    };
    script(
        dir.join("bin/ssh"),
        &format!(
            "#!/bin/sh\nfor a; do printf '%s\\n' \"$a\" >> '{log}'; done\n\
             [ -n \"$FAKE_SSH_RUN\" ] || exit 0\n\
             for last; do :; done\n\
             PATH=\"$FAKE_REMOTE_PATH\" SHELL='{shell}' exec /bin/sh -c \"$last\"\n",
            log = dir.join("ssh.log").display(),
            shell = dir.join("fakeshell").display(),
        ),
    );
    script(dir.join("fakeshell"), "#!/bin/sh\nexec /bin/sh -c \"$2\"\n");
    dir
}

/// Run `chattrs exec ARGS` with the fake ssh first in PATH.
#[cfg(unix)]
fn run_exec_ssh(dir: &std::path::Path, args: &[&str], envs: &[(&str, &str)]) -> Output {
    let path = format!("{}:/usr/bin:/bin", dir.join("bin").display());
    let mut all = vec![("PATH", path.as_str())];
    all.extend_from_slice(envs);
    run_exec(&std::env::temp_dir(), args, &all)
}

#[cfg(unix)]
#[test]
fn exec_host_runs_ssh_with_one_remote_string_and_no_secrets() {
    let dir = fake_ssh_dir("plain");
    let out = run_exec_ssh(
        &dir,
        &[
            "--host",
            "box",
            "--cwd",
            "/srv/r",
            "--as",
            "w1@box",
            "--lead",
            "lead@mac",
            "--project",
            "p",
            "--tags",
            "t",
            "--",
            "claude",
            "--resume",
            "a b",
        ],
        &[("CHATTRS_TOKEN", "sekrit"), ("CHATTRS_SERVER", "10.9.9.9:1")],
    );
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let log = std::fs::read_to_string(dir.join("ssh.log")).unwrap();
    let argv: Vec<&str> = log.lines().collect();
    assert_eq!(argv.len(), 3, "{log}");
    assert_eq!(&argv[..2], ["-t", "box"]);
    assert_eq!(
        argv[2],
        "exec \"$SHELL\" -lc 'cd '\\''/srv/r'\\'' && exec chattrs exec --as '\\''w1@box'\\'' --lead '\\''lead@mac'\\'' -- '\\''claude'\\'' '\\''--resume'\\'' '\\''a b'\\'''"
    );
    for secret in ["sekrit", "CHATTRS_", "10.9.9.9", "--project", "--tags", "--server"] {
        assert!(!log.contains(secret), "{secret} leaked: {log}");
    }
}

#[cfg(unix)]
#[test]
fn exec_host_flags_need_their_parents() {
    let dir = fake_ssh_dir("clap");
    for args in [
        &["--cwd", "/x", "claude"][..],
        &["--detach", "claude"],
        &["--host", "b", "--session-name", "n", "claude"],
    ] {
        let out = run_exec_ssh(&dir, args, &[]);
        assert_eq!(out.status.code(), Some(2), "{args:?}: {out:?}");
        assert!(String::from_utf8_lossy(&out.stderr).contains("required arguments"), "{out:?}");
    }
}

/// Run a detached start whose fake ssh executes the remote string.
/// `with_tmux`: a fake tmux that logs its args; `exists`: it reports a session.
#[cfg(unix)]
fn run_detached(name: &str, with_tmux: bool, exists: bool) -> (Output, std::path::PathBuf) {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = fake_ssh_dir(name);
    let remote_bin = dir.join("remote-bin");
    std::fs::create_dir_all(&remote_bin).unwrap();
    if with_tmux {
        let tmux = remote_bin.join("tmux");
        let body = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\n[ \"$1\" = has-session ] && exit {}\nexit 0\n",
            dir.join("tmux.log").display(),
            if exists { 0 } else { 1 },
        );
        std::fs::write(&tmux, body).unwrap();
        std::fs::set_permissions(&tmux, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let remote_path = remote_bin.display().to_string();
    let out = run_exec_ssh(
        &dir,
        &["--host", "box", "--cwd", "/", "--detach", "--as", "my.w1@box", "claude", "go now"],
        &[("FAKE_SSH_RUN", "1"), ("FAKE_REMOTE_PATH", &remote_path)],
    );
    (out, dir)
}

#[cfg(unix)]
#[test]
fn exec_detach_without_tmux_exits_1_with_a_hint() {
    let (out, _) = run_detached("notmux", false, false);
    assert_eq!(out.status.code(), Some(1), "{out:?}");
    assert!(String::from_utf8_lossy(&out.stderr).contains("install tmux, or drop --detach"));
    assert!(!String::from_utf8_lossy(&out.stdout).contains("started"));
}

#[cfg(unix)]
#[test]
fn exec_detach_starts_a_tmux_session_and_prints_hints() {
    let (out, dir) = run_detached("ok", true, false);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("started my-w1 on box"), "{text}");
    assert!(text.contains("attach: ssh -t box tmux attach -t =my-w1"), "{text}");
    assert!(text.contains("stop: ssh box tmux kill-session -t =my-w1"), "{text}");
    let log = std::fs::read_to_string(dir.join("tmux.log")).unwrap();
    assert!(log.contains("has-session -t =my-w1"), "{log}");
    assert!(
        log.contains(
            "new-session -d -s my-w1 exec chattrs exec --as 'my.w1@box' -- 'claude' 'go now'"
        ),
        "{log}"
    );
    let ssh = std::fs::read_to_string(dir.join("ssh.log")).unwrap();
    assert_eq!(ssh.lines().next(), Some("box"), "detach uses no -t: {ssh}");
}

#[cfg(unix)]
#[test]
fn exec_detach_refuses_an_existing_session() {
    let (out, dir) = run_detached("exists", true, true);
    assert_eq!(out.status.code(), Some(1), "{out:?}");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("session my-w1 already exists"), "{err}");
    assert!(err.contains("ssh -t box tmux attach -t =my-w1"), "{err}");
    let log = std::fs::read_to_string(dir.join("tmux.log")).unwrap();
    assert!(!log.contains("new-session") && !log.contains("kill"), "{log}");
}
