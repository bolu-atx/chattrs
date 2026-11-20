//! Black-box CLI tests for messaging: send, ask, reply, inbox, agents, log, watch.

#[path = "common/broker.rs"]
mod broker;
mod common;

use broker::*;
use common::free_port;
use std::io::{BufRead, BufReader, Write as _};
use std::net::TcpStream;
use std::process::{ChildStdout, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

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
