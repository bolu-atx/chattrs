//! Broker tests: hello state (MON-4), `chattrs status`, and the lagged
//! observer.

use super::super::*;
use super::fixture::*;
use crate::client;
use crate::model::Outbound;
use tempfile::TempDir;

fn hello_state(address: &str, extra: &str) -> String {
    format!("{{\"type\":\"hello\",\"v\":1,\"address\":\"{address}\"{extra}}}\n")
}

async fn state_of(addr: &str, address: &str) -> (Option<String>, Option<u64>) {
    let agents = client::fetch_agents(&endpoint(addr, "viewer@h")).await.unwrap();
    let agent = agents.into_iter().find(|agent| agent.address == address).unwrap();
    (agent.state, agent.state_at)
}

#[tokio::test]
async fn hello_state_is_stored_and_a_hello_without_one_keeps_it() {
    let (addr, _dir, handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, &hello_state("a@h", "")).await;
    assert_eq!(state_of(&addr, "a@h").await, (None, None), "never set");
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, &hello_state("a@h", ",\"state\":\"working\"")).await;
    let (state, at) = state_of(&addr, "a@h").await;
    assert_eq!(state.as_deref(), Some("working"));
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, &hello_state("a@h", "")).await;
    assert_eq!(state_of(&addr, "a@h").await, (state, at), "no-state hello keeps both");
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, &hello_state("a@h", ",\"state\":\"idle\"")).await;
    let (state, newer) = state_of(&addr, "a@h").await;
    assert_eq!(state.as_deref(), Some("idle"));
    assert!(newer > at, "a new state restamps state_at");
    handle.abort();
}

#[tokio::test]
async fn bad_state_and_observer_state_get_error_frames() {
    let (addr, _dir, handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    let frame = raw_hello(&mut raw, &hello_state("a@h", ",\"state\":\"sleeping\"")).await;
    assert_eq!(error_code(&frame), "bad_state");
    let mut raw = raw_connect(&addr).await;
    let observer_state = hello_state("v@h", ",\"observer\":true,\"state\":\"idle\"");
    let frame = raw_hello(&mut raw, &observer_state).await;
    assert_eq!(error_code(&frame), "observer_read_only");
    let mut raw = raw_connect(&addr).await;
    let frame = raw_hello(&mut raw, &hello_state("v@h", ",\"observer\":true")).await;
    assert!(matches!(frame, Outbound::Welcome { observer: true, .. }));
    handle.abort();
}

#[tokio::test]
async fn status_command_reports_state_and_agents_shows_it() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;

    let out = run_chattrs(&bin, "status", &addr, "a@h", &["waiting"], &db);
    assert!(out.status.success());
    assert!(out.stdout.is_empty(), "status prints nothing on stdout");

    let out = run_chattrs(&bin, "agents", &addr, "b@h", &[], &db);
    let text = stdout_lines(&out);
    let line = text.iter().find(|line| line.starts_with("a@h")).expect("a@h listed");
    assert!(line.contains("  waiting  "), "state follows last-seen: {line}");

    let out = run_chattrs(&bin, "agents", &addr, "b@h", &["--json"], &db);
    let json: Vec<serde_json::Value> =
        stdout_lines(&out).iter().map(|line| serde_json::from_str(line).unwrap()).collect();
    let a = json.iter().find(|value| value["address"] == "a@h").unwrap();
    let b = json.iter().find(|value| value["address"] == "b@h").unwrap();
    assert_eq!(a["state"], "waiting");
    assert!(a["state_at"].is_u64());
    assert!(b["state"].is_null() && b["state_at"].is_null());
}

#[tokio::test]
async fn status_exits_zero_with_one_stderr_line_when_the_broker_is_down() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let start = std::time::Instant::now();
    let out = run_chattrs(&bin, "status", &addr, "a@h", &["idle"], &db);
    assert!(out.status.success(), "status never fails the agent");
    assert!(out.stdout.is_empty());
    assert!(start.elapsed() < std::time::Duration::from_secs(6), "bounded: {:?}", start.elapsed());
    let err = String::from_utf8(out.stderr).unwrap();
    assert_eq!(err.lines().count(), 1, "one stderr line: {err}");
    assert!(err.starts_with("chattrs: status not reported:"), "{err}");
}

#[tokio::test]
async fn a_lagging_observer_never_blocks_a_send_and_is_dropped() {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path().join("chattrs.db")).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let broker = Arc::new(Broker::with_capacity(store, None, 4));
    let handle = tokio::spawn(async move {
        let _ = run_listener(listener, broker).await;
    });

    let mut slow = raw_connect(&addr).await;
    raw_hello(&mut slow, &hello_state("slow@h", ",\"observer\":true")).await;
    raw_send(&mut slow, "{\"type\":\"watch\",\"all\":true}\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // The observer never reads. Big bodies fill its socket buffers, so the
    // broker stalls writing to it and the small broadcast overflows.
    let body = "x".repeat(60 * 1024);
    let mut sender = client::Client::connect(&endpoint(&addr, "a@h")).await.unwrap();
    let start = std::time::Instant::now();
    for _ in 0..400 {
        sender.publish(outgoing("b@h", &body)).await.unwrap();
    }
    assert!(
        start.elapsed() < std::time::Duration::from_secs(20),
        "sends finish promptly: {:?}",
        start.elapsed()
    );

    // Draining the dead observer ends in a close, not a hang.
    let drained = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while raw_recv(&mut slow).await.is_ok() {}
    })
    .await;
    assert!(drained.is_ok(), "the lagging observer connection was closed");
    handle.abort();
}
