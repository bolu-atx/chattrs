//! Shared helpers for the broker tests: broker spawns, raw protocol
//! connections, and child-process drivers.

use super::super::*;
use crate::client;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt as _;
use tokio::io::BufReader as TokioBufReader;
use tokio::net::TcpStream as TokioTcpStream;

pub(crate) async fn spawn() -> (String, TempDir, tokio::task::JoinHandle<()>) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path().join("chattrs.db")).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let broker = Arc::new(Broker::new(store, None));
    let handle = tokio::spawn(async move {
        let _ = run_listener(listener, broker).await;
    });
    (addr, dir, handle)
}

pub(crate) async fn spawn_at(db: &Path) -> (String, tokio::task::JoinHandle<()>) {
    let store = Store::open(db.to_path_buf()).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let broker = Arc::new(Broker::new(store, None));
    let handle = tokio::spawn(async move {
        let _ = run_listener(listener, broker).await;
    });
    (addr, handle)
}

pub(crate) struct Raw {
    pub(crate) reader: TokioBufReader<tokio::net::tcp::OwnedReadHalf>,
    pub(crate) writer: tokio::net::tcp::OwnedWriteHalf,
}

pub(crate) async fn raw_connect(addr: &str) -> Raw {
    let stream = TokioTcpStream::connect(addr).await.unwrap();
    let (read_half, write_half) = stream.into_split();
    Raw { reader: TokioBufReader::new(read_half), writer: write_half }
}

pub(crate) async fn raw_send(raw: &mut Raw, line: &str) {
    raw.writer.write_all(line.as_bytes()).await.unwrap();
    raw.writer.flush().await.unwrap();
}

pub(crate) async fn raw_recv(raw: &mut Raw) -> Result<Outbound, String> {
    let mut line = String::new();
    let n = raw.reader.read_line(&mut line).await.map_err(|e| e.to_string())?;
    if n == 0 {
        return Err("server closed the connection".to_string());
    }
    protocol::decode(&line)
}

pub(crate) async fn raw_hello(raw: &mut Raw, hello: &str) -> Outbound {
    raw_send(raw, hello).await;
    raw_recv(raw).await.unwrap()
}

pub(crate) fn error_code(frame: &Outbound) -> &str {
    match frame {
        Outbound::Error { code, .. } => code,
        other => panic!("expected an error frame, got {other:?}"),
    }
}

/// One message to `to` with `body` and no reply id or topic: the
/// general chat. Tests that need a reply id or a topic use struct
/// update syntax on the result.
pub(crate) fn outgoing(to: &str, body: &str) -> client::Outgoing {
    client::Outgoing {
        to: Some(to.to_string()),
        body: body.to_string(),
        reply_to: None,
        topic: None,
    }
}

/// The endpoint for `address` on the broker at `server`, with no token.
pub(crate) fn endpoint(server: &str, address: &str) -> client::Endpoint {
    client::Endpoint { server: server.to_string(), address: address.to_string(), token: None }
}

pub(crate) async fn spawn_with_token(
    token: &str,
) -> (String, TempDir, tokio::task::JoinHandle<()>) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path().join("chattrs.db")).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let broker = Arc::new(Broker::new(store, Some(token.to_string())));
    let handle = tokio::spawn(async move {
        let _ = run_listener(listener, broker).await;
    });
    (addr, dir, handle)
}

pub(crate) async fn seen(server: &str, address: &str) -> u64 {
    client::fetch_agents(&endpoint(server, "observer@h"))
        .await
        .unwrap()
        .into_iter()
        .find(|agent| agent.address == address)
        .unwrap()
        .last_seen
}

pub(crate) async fn read_watched_message(raw: &mut Raw) -> Message {
    let frame = tokio::time::timeout(std::time::Duration::from_secs(10), raw_recv(raw))
        .await
        .expect("watch delivers in time")
        .expect("broker answers");
    match frame {
        Outbound::Message(message) => message,
        other => panic!("expected a message frame, got {other:?}"),
    }
}

pub(crate) async fn raw_watch(raw: &mut Raw, address: &str) {
    let hello = format!("{{\"type\":\"hello\",\"v\":1,\"address\":\"{address}\"}}\n");
    raw_hello(raw, &hello).await;
    raw_send(raw, "{\"type\":\"watch\"}\n").await;
}

/// An announcement to `topic`: no recipient, only a room.
pub(crate) fn announce(topic: &str, body: &str) -> client::Outgoing {
    client::Outgoing {
        to: None,
        body: body.to_string(),
        reply_to: None,
        topic: Some(topic.to_string()),
    }
}

pub(crate) async fn raw_topic_watch(raw: &mut Raw, address: &str, topics: &[&str]) {
    let hello = format!("{{\"type\":\"hello\",\"v\":1,\"address\":\"{address}\"}}\n");
    raw_hello(raw, &hello).await;
    let line =
        format!("{{\"type\":\"watch\",\"topics\":{}}}\n", serde_json::to_string(topics).unwrap());
    raw_send(raw, &line).await;
}

/// Fail when any frame arrives within half a second: the quiet
/// connection must stay quiet.
pub(crate) async fn expect_no_message(raw: &mut Raw) {
    let result = tokio::time::timeout(std::time::Duration::from_millis(500), raw_recv(raw)).await;
    assert!(result.is_err(), "no message arrives, got {result:?}");
}

pub(crate) fn test_bin_path() -> std::path::PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_chattrs") {
        let path = std::path::PathBuf::from(path);
        if path.exists() {
            return path;
        }
    }
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(format!("target/debug/chattrs{}", std::env::consts::EXE_SUFFIX))
}

pub(crate) fn ensure_bin() -> std::path::PathBuf {
    static BUILD_ONCE: std::sync::Once = std::sync::Once::new();
    let path = test_bin_path();
    BUILD_ONCE.call_once(|| {
        let status = std::process::Command::new("cargo")
            .arg("build")
            .arg("--bin")
            .arg("chattrs")
            .status()
            .expect("run cargo build");
        assert!(status.success(), "cargo build failed");
    });
    assert!(path.exists(), "chattrs binary missing at {}", path.display());
    path
}

pub(crate) struct ProcChild {
    pub(crate) child: std::process::Child,
}

impl ProcChild {
    pub(crate) fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ProcChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

pub(crate) fn free_addr() -> String {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    probe.local_addr().expect("probe addr").to_string()
}

pub(crate) fn start_server(bin: &std::path::Path, addr: &str, db: &std::path::Path) -> ProcChild {
    let child = std::process::Command::new(bin)
        .arg("serve")
        .arg("--listen")
        .arg(addr)
        .arg("--db")
        .arg(db)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn serve");
    ProcChild { child }
}

pub(crate) fn start_watch(
    bin: &std::path::Path,
    addr: &str,
    who: &str,
    out: &std::path::Path,
) -> ProcChild {
    start_topic_watch(bin, addr, who, &[], out)
}

pub(crate) fn start_topic_watch(
    bin: &std::path::Path,
    addr: &str,
    who: &str,
    topics: &[&str],
    out: &std::path::Path,
) -> ProcChild {
    let file = std::fs::File::create(out).expect("watch output file");
    let mut command = std::process::Command::new(bin);
    command.arg("watch").arg("--server").arg(addr).arg("--address").arg(who);
    for topic in topics {
        command.arg("--topic").arg(topic);
    }
    let child =
        command.stdout(file).stderr(std::process::Stdio::null()).spawn().expect("spawn watch");
    ProcChild { child }
}

pub(crate) async fn wait_for_server(addr: &str) {
    let start = tokio::time::Instant::now();
    loop {
        if client::fetch_inbox(&endpoint(addr, "probe@h")).await.is_ok() {
            return;
        }
        assert!(
            start.elapsed() < std::time::Duration::from_secs(10),
            "server never came up at {addr}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

pub(crate) fn read_out_lines(out: &std::path::Path) -> Vec<String> {
    std::fs::read_to_string(out).unwrap_or_default().lines().map(|line| line.to_string()).collect()
}

pub(crate) async fn wait_for_lines(out: &std::path::Path, count: usize) -> Vec<String> {
    let start = tokio::time::Instant::now();
    loop {
        let text = std::fs::read_to_string(out).unwrap_or_default();
        if text.ends_with('\n') {
            let lines = read_out_lines(out);
            if lines.len() >= count {
                return lines;
            }
        }
        assert!(
            start.elapsed() < std::time::Duration::from_secs(15),
            "watch printed no line in time"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

pub(crate) async fn wait_until_inbox_empty(addr: &str, who: &str) {
    let start = tokio::time::Instant::now();
    loop {
        let messages = client::fetch_inbox(&endpoint(addr, who)).await.expect("fetch inbox");
        if messages.is_empty() {
            return;
        }
        assert!(
            start.elapsed() < std::time::Duration::from_secs(10),
            "inbox never drained: {messages:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

pub(crate) fn start_ask(
    bin: &std::path::Path,
    addr: &str,
    asker: (&str, &str),
    timeout_secs: &str,
    files: (&std::path::Path, &std::path::Path),
    body: &str,
    topic: Option<&str>,
) -> ProcChild {
    let out_file = std::fs::File::create(files.0).expect("ask output file");
    let err_file = std::fs::File::create(files.1).expect("ask error file");
    let mut command = std::process::Command::new(bin);
    command
        .arg("ask")
        .arg("--server")
        .arg(addr)
        .arg("--address")
        .arg(asker.0)
        .arg("--to")
        .arg(asker.1)
        .arg("--timeout")
        .arg(timeout_secs);
    if let Some(topic) = topic {
        command.arg("--topic").arg(topic);
    }
    let child = command.arg(body).stdout(out_file).stderr(err_file).spawn().expect("spawn ask");
    ProcChild { child }
}

pub(crate) async fn wait_for_exit(
    child: &mut std::process::Child,
    secs: u64,
) -> std::process::ExitStatus {
    let start = tokio::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("poll ask child") {
            return status;
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(secs), "ask did not exit in time");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

pub(crate) async fn wait_for_question(addr: &str, who: &str) -> Message {
    let start = tokio::time::Instant::now();
    loop {
        let messages = client::fetch_inbox(&endpoint(addr, who)).await.expect("fetch inbox");
        if let Some(question) = messages.into_iter().next() {
            return question;
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(10), "question never arrived");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

pub(crate) fn run_chattrs(
    bin: &std::path::Path,
    subcommand: &str,
    addr: &str,
    who: &str,
    extra: &[&str],
) -> std::process::Output {
    std::process::Command::new(bin)
        .arg(subcommand)
        .arg("--server")
        .arg(addr)
        .arg("--address")
        .arg(who)
        .args(extra)
        .output()
        .expect("run chattrs")
}

pub(crate) fn stdout_lines(out: &std::process::Output) -> Vec<String> {
    String::from_utf8(out.stdout.clone())
        .expect("stdout is utf8")
        .lines()
        .map(|line| line.to_string())
        .collect()
}
