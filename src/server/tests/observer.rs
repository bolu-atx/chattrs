//! Broker tests: observer hello and `watch` with `all`.

use super::fixture::*;
use crate::client;
use crate::model::Outbound;
use tempfile::TempDir;

const OBSERVER_HELLO: &str =
    "{\"type\":\"hello\",\"v\":1,\"address\":\"tray@h\",\"observer\":true,\"tags\":[\"gpu\"]}\n";

async fn observe(addr: &str) -> Raw {
    let mut raw = raw_connect(addr).await;
    let welcome = raw_hello(&mut raw, OBSERVER_HELLO).await;
    assert!(matches!(welcome, Outbound::Welcome { observer: true, watching: false, .. }));
    raw_send(&mut raw, "{\"type\":\"watch\",\"all\":true}\n").await;
    raw
}

async fn listed(addr: &str) -> Vec<String> {
    let agents = client::fetch_agents(&endpoint(addr, "a@h")).await.unwrap();
    agents.into_iter().map(|agent| agent.address).collect()
}

#[tokio::test]
async fn observer_is_never_listed_or_online() {
    let (addr, _dir, handle) = spawn().await;
    let raw = observe(&addr).await;
    assert_eq!(listed(&addr).await, ["a@h"], "observer is not listed while connected");
    drop(raw);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(listed(&addr).await, ["a@h"], "observer is not listed after close");
    handle.abort();
}

#[tokio::test]
async fn observer_watch_does_not_make_the_address_online() {
    let (addr, _dir, handle) = spawn().await;
    let _raw = observe(&addr).await;
    let mut other = raw_connect(&addr).await;
    let hello = "{\"type\":\"hello\",\"v\":1,\"address\":\"tray@h\"}\n";
    let welcome = raw_hello(&mut other, hello).await;
    assert!(matches!(welcome, Outbound::Welcome { watching: false, .. }));
    raw_send(&mut other, "{\"type\":\"agents\"}\n").await;
    match raw_recv(&mut other).await.unwrap() {
        Outbound::Agents { agents, .. } => {
            let tray = agents.iter().find(|agent| agent.address == "tray@h").unwrap();
            assert!(!tray.online);
        }
        frame => panic!("{frame:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn observer_hello_stores_no_tags() {
    let (addr, _dir, handle) = spawn().await;
    let _raw = observe(&addr).await;
    client::publish(&endpoint(&addr, "tray@h"), outgoing("a@h", "x")).await.unwrap();
    let agents = client::fetch_agents(&endpoint(&addr, "a@h")).await.unwrap();
    let tray = agents.iter().find(|agent| agent.address == "tray@h").unwrap();
    assert!(tray.tags.iter().all(|tag| tag != "gpu"));
    handle.abort();
}

#[tokio::test]
async fn watch_all_streams_direct_and_topic_traffic_without_acking() {
    let (addr, _dir, handle) = spawn().await;
    let mut raw = observe(&addr).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let direct =
        client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "private")).await.unwrap();
    let announced =
        client::publish(&endpoint(&addr, "a@h"), announce("room", "hear ye")).await.unwrap();
    let first = read_watched_message(&mut raw).await;
    let second = read_watched_message(&mut raw).await;
    assert_eq!((first.id, first.body.as_str()), (direct, "private"));
    assert_eq!((second.id, second.body.as_str()), (announced, "hear ye"));
    let unread = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(unread.len(), 1, "the recipient still has it unread");
    handle.abort();
}

#[tokio::test]
async fn watch_all_without_observer_is_an_error() {
    let (addr, _dir, handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
    raw_send(&mut raw, "{\"type\":\"watch\",\"all\":true}\n").await;
    let frame = raw_recv(&mut raw).await.unwrap();
    assert_eq!(error_code(&frame), "not_observer");
    handle.abort();
}

#[tokio::test]
async fn observer_connection_is_read_only() {
    let (addr, _dir, handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, OBSERVER_HELLO).await;
    for line in [
        "{\"type\":\"send\",\"to\":\"b@h\",\"body\":\"x\"}\n",
        "{\"type\":\"ack\",\"ids\":[1]}\n",
        "{\"type\":\"wait\",\"reply_to\":1}\n",
        "{\"type\":\"inbox\"}\n",
    ] {
        raw_send(&mut raw, line).await;
        let frame = raw_recv(&mut raw).await.unwrap();
        assert_eq!(error_code(&frame), "observer_read_only", "{line}");
    }
    raw_send(&mut raw, "{\"type\":\"agents\"}\n").await;
    assert!(matches!(raw_recv(&mut raw).await.unwrap(), Outbound::Agents { .. }));
    raw_send(&mut raw, "{\"type\":\"log\"}\n").await;
    assert!(matches!(raw_recv(&mut raw).await.unwrap(), Outbound::Log { .. }));
    handle.abort();
}

#[tokio::test]
async fn observer_hello_still_needs_the_token() {
    let (addr, _dir, handle) = spawn_with_token("s3cret").await;
    let mut raw = raw_connect(&addr).await;
    let frame = raw_hello(&mut raw, OBSERVER_HELLO).await;
    assert_eq!(error_code(&frame), "bad_token");
    handle.abort();
}

#[tokio::test]
async fn cli_watch_all_prints_streamed_messages() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("watch.out");
    let file = std::fs::File::create(&out).unwrap();
    let _watcher = ProcChild {
        child: super::fixture::ports::command(&bin)
            .args(["watch", "--all", "--server", &addr, "--address", "tray@h"])
            .env("CHATTRS_DB", &db)
            .stdout(file)
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    };
    // The observer has no backlog, so resend until it is subscribed.
    let start = tokio::time::Instant::now();
    let line = loop {
        client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        if let Some(line) = read_out_lines(&out).first() {
            break line.clone();
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(15), "no line printed");
    };
    let value: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(value["untrusted"], true);
    assert_eq!(value["body"], "hi");
    let unread = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert!(!unread.is_empty(), "the observer acked nothing");
}
