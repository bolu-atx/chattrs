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
        let mut child = Command::new(binary())
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

/// Clear the `CHATTRS_*` overrides that would let the developer's shell
/// change the result. The address always comes from the caller.
fn clear_env(command: &mut Command) {
    command
        .env_remove("CHATTRS_TOKEN")
        .env_remove("CHATTRS_AGENT")
        .env_remove("CHATTRS_SERVER")
        .env_remove("CHATTRS_DB")
        .env_remove("CHATTRS_LEAD")
        .env_remove("CHATTRS_PROJECT")
        .env_remove("CHATTRS_TAGS")
        .env_remove("CHATTRS_SESSION")
        .env("HOME", "/nonexistent-chattrs-test-home");
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
fn errors_go_to_stderr_with_exit_one() {
    // A loopback server with no broker starts one on demand (CLI-11):
    // the command then runs against it. The DB is a temp path, so the
    // real default database is never touched, and the broker this
    // starts is stopped before the test ends.
    let dir = tempfile::TempDir::new().expect("make a temp dir");
    let db = dir.path().join("c.db");
    let closed = format!("127.0.0.1:{}", free_port());
    let mut command = Command::new(binary());
    command.arg("inbox").arg("--server").arg(&closed);
    command.env("CHATTRS_ADDRESS", "a@h");
    command.env("CHATTRS_DB", &db);
    command.env_remove("CHATTRS_TOKEN").env_remove("CHATTRS_AGENT").env_remove("CHATTRS_SERVER");
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let output = command.output().expect("run the client");
    assert_eq!(output.status.code(), Some(0), "inbox auto-starts a broker: {output:?}");
    assert!(output.stdout.is_empty(), "stdout is empty");
    let stderr = stderr_text(&output);
    assert!(stderr.contains("started a broker"), "stderr names the start: {stderr:?}");

    let mut stop = Command::new(binary());
    stop.arg("stop").env("CHATTRS_DB", &db);
    stop.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let stopped = stop.output().expect("stop the broker");
    assert_eq!(stopped.status.code(), Some(0), "stop exits zero: {stopped:?}");
    assert!(stopped.stdout.is_empty(), "stop prints nothing");
    assert!(TcpStream::connect(&closed).is_err(), "the port no longer answers after stop");

    // A remote server is never auto-started: the failure is an error
    // on stderr with exit 1, and it starts nothing.
    let remote = format!("192.0.2.1:{}", free_port());
    let mut command = Command::new(binary());
    command.arg("inbox").arg("--server").arg(&remote);
    command.env("CHATTRS_ADDRESS", "a@h");
    command.env("CHATTRS_DB", &db);
    command.env_remove("CHATTRS_TOKEN").env_remove("CHATTRS_AGENT").env_remove("CHATTRS_SERVER");
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

    let mut command = Command::new(binary());
    command.arg("log").arg("--server").arg(&broker.addr);
    command.env("CHATTRS_ADDRESS", "reader@h");
    clear_env(&mut command);
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

/// Spawn `watch` for `address` with stderr piped.
fn spawn_watch(server: &str, address: &str) -> KillOnDrop {
    let mut command = Command::new(binary());
    command.arg("watch").arg("--server").arg(server);
    command.env("CHATTRS_ADDRESS", address);
    clear_env(&mut command);
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
    let mut command = Command::new(binary());
    command.args(["hook", event]).arg("--server").arg(server).args(flags);
    command.env_remove("CHATTRS_ADDRESS");
    if let Some(address) = address {
        command.env("CHATTRS_ADDRESS", address);
    }
    clear_env(&mut command);
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
        let mut command = Command::new(binary());
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

/// Run `session-start` for `address` with `CHATTRS_LEAD` set to `lead`, and
/// return its context text.
fn session_context(server: &str, address: &str, lead: Option<&str>) -> String {
    let mut command = Command::new(binary());
    command.args(["hook", "session-start"]).arg("--server").arg(server);
    clear_env(&mut command);
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
        let mut command = Command::new(binary());
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
    let mut command = Command::new(binary());
    command.args(args).current_dir(cwd);
    clear_env(&mut command);
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
    let sh = Command::new("sh").args(["-c", &script]).current_dir(cwd).output().unwrap();
    assert_eq!(stdout_text(&sh), "w@h|it's $(touch pwned) \\\"x|unset");
    assert!(!cwd.join("pwned").exists());
    let with = stdout_text(&run_in(cwd, cwd, &[], &["env", "--as", "w@h", "--with-token"]));
    assert!(with.contains("export CHATTRS_TOKEN='sekret'\n"), "{with}");
}
