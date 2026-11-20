//! SEC-5: the broker writes one stderr line per rejected hello and never
//! the token. A spawned `chattrs serve` runs with stderr sent to a file.

mod common;

use std::io::{BufRead as _, BufReader, Write as _};
use std::net::TcpStream;
use std::os::unix::fs::PermissionsExt as _;
use std::process::Stdio;
use std::time::{Duration, Instant};

const TOKENS: &str = "[[token]]\ntoken = \"tok-secret-w1\"\nproject = \"alpha\"\n\
                      addr = \"w1@box1\"\nrole = \"agent\"\n";

/// Send one raw hello and read the reply line. The stream stays open so a
/// session keeps its address.
fn hello(addr: &str, address: &str, token: &str, project: &str, session: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).expect("connect");
    let line = format!(
        "{{\"type\":\"hello\",\"v\":1,\"address\":{address:?},\"token\":{token:?},\
         \"project\":{project:?},\"session\":{session:?}}}\n"
    );
    stream.write_all(line.as_bytes()).expect("send hello");
    let mut reply = String::new();
    BufReader::new(stream.try_clone().expect("clone")).read_line(&mut reply).expect("reply");
    assert!(reply.contains("\"type\""), "{reply}");
    stream
}

#[test]
fn each_rejected_hello_logs_one_line_and_never_the_token() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let tokens = dir.path().join("tokens.toml");
    std::fs::write(&tokens, TOKENS).expect("write tokens");
    std::fs::set_permissions(&tokens, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    let addr = format!("127.0.0.1:{}", common::free_port());
    let log_path = dir.path().join("stderr.log");
    let mut child = common::command(env!("CARGO_BIN_EXE_chattrs"))
        .args(["serve", "--listen", &addr, "--db"])
        .arg(dir.path().join("c.db"))
        .arg("--tokens")
        .arg(&tokens)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&log_path).expect("log file"))
        .spawn()
        .expect("spawn the broker");
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(&addr).is_err() {
        assert!(Instant::now() < deadline, "broker never listened");
        std::thread::sleep(Duration::from_millis(50));
    }

    let secret = "tok-secret-w1";
    let wrong = "tok-wrong-xyz";
    let _held = [
        hello(&addr, "w1@box1", wrong, "alpha", "s1"), // bad_token
        hello(&addr, "w1@box1", secret, "alpha", "s1"), // welcome, holds the address
        hello(&addr, "w1@box1", secret, "alpha", "s2"), // address_in_use
        hello(&addr, "other@box1", secret, "alpha", "s1"), // forbidden (address)
        hello(&addr, "a\t\nchattrs: forged@h", wrong, "alpha", "s1"), // escaped
        hello(&addr, "x\" reason=forbidden", wrong, "alpha", "s1"), // quoted
    ];
    std::thread::sleep(Duration::from_millis(300));
    let _ = child.kill();
    let _ = child.wait();

    let log = std::fs::read_to_string(&log_path).expect("read log");
    assert!(!log.contains(secret) && !log.contains(wrong), "token leaked: {log}");
    let lines: Vec<&str> = log.lines().filter(|l| l.contains("rejected hello")).collect();
    assert_eq!(lines.len(), 5, "{log}");
    let expect = [
        "project=\"alpha\" address=\"w1@box1\" reason=bad_token",
        "project=\"alpha\" address=\"w1@box1\" reason=address_in_use",
        "project=\"alpha\" address=\"other@box1\" reason=forbidden",
        "project=\"alpha\" address=\"a\\t\\nchattrs: forged@h\" reason=bad_token",
        "address=\"x\\\" reason=forbidden\" reason=bad_token (",
    ];
    for (line, needle) in lines.iter().zip(expect) {
        assert!(line.starts_with("chattrs: rejected hello from 127.0.0.1:"), "{line}");
        assert!(line.contains(needle), "{line} lacks {needle}");
    }
    assert_eq!(log.lines().filter(|l| l.starts_with("chattrs: forged")).count(), 0, "{log}");
}
