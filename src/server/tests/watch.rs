//! Broker tests: watch.

use super::helpers::*;
use crate::client;
use crate::model::Outbound;
use tempfile::TempDir;

#[tokio::test]
async fn watch_delivers_unread_then_live_messages_in_order() {
    let (addr, _dir, handle) = spawn().await;
    let first = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "one")).await.unwrap();
    let second = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "two")).await.unwrap();

    let mut first_watcher = raw_connect(&addr).await;
    raw_watch(&mut first_watcher, "b@h").await;
    let got_first = read_watched_message(&mut first_watcher).await;
    let got_second = read_watched_message(&mut first_watcher).await;
    assert_eq!(got_first.id, first);
    assert_eq!(got_first.body, "one");
    assert_eq!(got_second.id, second);
    assert_eq!(got_second.body, "two");

    let mut second_watcher = raw_connect(&addr).await;
    raw_watch(&mut second_watcher, "b@h").await;
    let again_first = read_watched_message(&mut second_watcher).await;
    let again_second = read_watched_message(&mut second_watcher).await;
    assert_eq!(again_first.id, first);
    assert_eq!(again_second.id, second);

    let third = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "three")).await.unwrap();
    let live_first = read_watched_message(&mut first_watcher).await;
    let live_second = read_watched_message(&mut second_watcher).await;
    assert_eq!(live_first.id, third);
    assert_eq!(live_first.body, "three");
    assert_eq!(live_second.id, third);
    assert!(first < second && second < third, "ids grow in send order");
    handle.abort();
}

#[tokio::test]
async fn watch_acks_what_it_prints() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("watch.out");
    let _watcher = start_watch(&bin, &addr, "b@h", &out, &db);

    let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();
    let lines = wait_for_lines(&out, 1).await;
    let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
    assert_eq!(value["id"].as_u64(), Some(id));

    wait_until_inbox_empty(&addr, "b@h").await;
}

#[tokio::test]
async fn watch_reconnects_after_broker_restart() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let mut server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("watch.out");
    let _watcher = start_watch(&bin, &addr, "b@h", &out, &db);

    let first = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "one")).await.unwrap();
    let lines = wait_for_lines(&out, 1).await;
    let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
    assert_eq!(value["id"].as_u64(), Some(first));
    wait_until_inbox_empty(&addr, "b@h").await;

    server.stop();
    let mut server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let second = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "two")).await.unwrap();
    let lines = wait_for_lines(&out, 2).await;
    let value: serde_json::Value = serde_json::from_str(&lines[1]).expect("JSON line");
    assert_eq!(value["id"].as_u64(), Some(second));
    assert_eq!(value["body"].as_str(), Some("two"));
    wait_until_inbox_empty(&addr, "b@h").await;
    server.stop();
}

#[tokio::test]
async fn watch_stdout_is_json_lines_only() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("watch.out");
    let _watcher = start_watch(&bin, &addr, "b@h", &out, &db);

    let first = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "plain")).await.unwrap();
    let second = client::publish(
        &endpoint(&addr, "a@h"),
        client::Outgoing { reply_to: Some(first), ..outgoing("b@h", "answer") },
    )
    .await
    .unwrap();
    let lines = wait_for_lines(&out, 2).await;
    assert_eq!(lines.len(), 2, "watch prints one line per message: {lines:?}");
    let plain: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
    assert_eq!(plain["type"].as_str(), Some("message"));
    assert_eq!(plain["id"].as_u64(), Some(first));
    assert_eq!(plain["from"].as_str(), Some("a@h"));
    assert_eq!(plain["to"].as_str(), Some("b@h"));
    assert_eq!(plain["body"].as_str(), Some("plain"));
    assert!(plain["ts"].as_u64().is_some(), "message carries ts");
    assert!(plain["reply_to"].is_null(), "plain message has no reply_to");
    let answer: serde_json::Value = serde_json::from_str(&lines[1]).expect("JSON line");
    assert_eq!(answer["type"].as_str(), Some("message"));
    assert_eq!(answer["id"].as_u64(), Some(second));
    assert_eq!(answer["reply_to"].as_u64(), Some(first));

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let lines = read_out_lines(&out);
    assert_eq!(lines.len(), 2, "watch prints nothing else: {lines:?}");
}

#[tokio::test]
async fn watch_has_no_preamble() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "old")).await.unwrap();
    let out = dir.path().join("watch.out");
    let _watcher = start_watch(&bin, &addr, "b@h", &out, &db);

    let lines = wait_for_lines(&out, 1).await;
    let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
    assert_eq!(value["type"].as_str(), Some("message"));
    assert_eq!(value["body"].as_str(), Some("old"));

    client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "new")).await.unwrap();
    let lines = wait_for_lines(&out, 2).await;
    let mut bodies = Vec::new();
    for line in &lines {
        let value: serde_json::Value = serde_json::from_str(line).expect("JSON line");
        assert_eq!(value["type"].as_str(), Some("message"), "watch prints JSON: {line}");
        bodies.push(value["body"].as_str().expect("message carries a body").to_string());
    }
    assert_eq!(bodies, vec!["old", "new"], "each message prints once: {lines:?}");
}

#[tokio::test]
async fn watch_lines_carry_untrusted_true() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("watch.out");
    let _watcher = start_watch(&bin, &addr, "b@h", &out, &db);

    let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();
    let lines = wait_for_lines(&out, 1).await;
    assert_eq!(lines.len(), 1, "watch prints one line per message: {lines:?}");
    let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
    assert_eq!(value["type"].as_str(), Some("message"), "line: {}", lines[0]);
    assert_eq!(value["id"].as_u64(), Some(id));
    assert_eq!(value["untrusted"], serde_json::Value::Bool(true));
}

#[tokio::test]
async fn watch_acks_on_its_own_connection() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("watch.out");
    let _watcher = start_watch(&bin, &addr, "b@h", &out, &db);

    let start = tokio::time::Instant::now();
    let before = loop {
        let agents = client::fetch_agents(&endpoint(&addr, "observer@h")).await.unwrap();
        if let Some(agent) = agents.iter().find(|agent| agent.address == "b@h") {
            break agent.last_seen;
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(10), "watcher never said hello");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };

    let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();
    let lines = wait_for_lines(&out, 1).await;
    let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
    assert_eq!(value["id"].as_u64(), Some(id));

    let start = tokio::time::Instant::now();
    loop {
        let history = client::fetch_log(&endpoint(&addr, "reader@h"), None).await.unwrap();
        if history.iter().any(|row| row.message.id == id && row.acked_at.is_some()) {
            break;
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(10), "watch never acked");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let after = seen(&addr, "b@h").await;
    assert_eq!(after, before, "the ack used the watch connection, not a new one");

    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert!(messages.is_empty(), "the watched message is acked: {messages:?}");
}

#[tokio::test]
async fn ack_works_while_watching() {
    let (addr, _dir, handle) = spawn().await;
    let mut watcher = raw_connect(&addr).await;
    raw_watch(&mut watcher, "b@h").await;

    let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();
    let got = read_watched_message(&mut watcher).await;
    assert_eq!(got.id, id);

    let line = format!("{{\"type\":\"ack\",\"ids\":[{id}]}}\n");
    raw_send(&mut watcher, &line).await;
    match raw_recv(&mut watcher).await.unwrap() {
        Outbound::Acked { ids } => assert_eq!(ids, vec![id]),
        other => panic!("expected an acked frame, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn frames_keep_flowing_while_waiting() {
    let (addr, _dir, handle) = spawn().await;
    let question = client::publish(&endpoint(&addr, "w@h"), outgoing("c@h", "go?")).await.unwrap();

    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"w@h\"}\n").await;
    let wait = format!("{{\"type\":\"wait\",\"reply_to\":{question}}}\n");
    raw_send(&mut raw, &wait).await;
    raw_send(&mut raw, "{\"type\":\"agents\"}\n").await;
    match raw_recv(&mut raw).await.unwrap() {
        Outbound::Agents { agents, .. } => {
            assert!(agents.iter().any(|agent| agent.address == "w@h"), "{agents:?}")
        }
        other => panic!("expected an agents frame, got {other:?}"),
    }

    let reply = client::reply(&endpoint(&addr, "c@h"), question, "yes", None).await.unwrap();
    let got = read_watched_message(&mut raw).await;
    assert_eq!(got.id, reply);
    assert_eq!(got.reply_to, Some(question));
    handle.abort();
}

async fn online_of(addr: &str, address: &str) -> bool {
    let agents = client::fetch_agents(&endpoint(addr, "observer@h")).await.unwrap();
    agents.iter().find(|agent| agent.address == address).unwrap().online
}

#[tokio::test]
async fn agent_is_online_while_it_has_an_inbox_watch() {
    let (addr, _dir, _handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"w@h\"}\n").await;
    assert!(!online_of(&addr, "w@h").await, "connected but not watching");
    raw_send(&mut raw, "{\"type\":\"watch\"}\n").await;
    raw_send(&mut raw, "{\"type\":\"agents\"}\n").await;
    let Outbound::Agents { agents, .. } = raw_recv(&mut raw).await.unwrap() else {
        panic!("expected agents")
    };
    assert!(agents.iter().find(|a| a.address == "w@h").unwrap().online);
    drop(raw);
    for _ in 0..50 {
        if !online_of(&addr, "w@h").await {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("agent stayed online after its watch closed");
}

#[tokio::test]
async fn topic_watch_does_not_make_an_agent_online() {
    let (addr, _dir, _handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"t@h\"}\n").await;
    raw_send(&mut raw, "{\"type\":\"watch\",\"topics\":[\"room\"]}\n").await;
    raw_send(&mut raw, "{\"type\":\"agents\"}\n").await;
    let _ = raw_recv(&mut raw).await.unwrap();
    assert!(!online_of(&addr, "t@h").await);
}

#[tokio::test]
async fn agent_goes_offline_when_a_watch_connection_is_reset() {
    let (addr, _dir, _handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"r@h\"}\n").await;
    raw_send(&mut raw, "{\"type\":\"watch\"}\n").await;
    raw_send(&mut raw, "{\"type\":\"agents\"}\n").await;
    let _ = raw_recv(&mut raw).await.unwrap();
    assert!(online_of(&addr, "r@h").await);
    let stream = raw.reader.into_inner().reunite(raw.writer).unwrap();
    socket2::SockRef::from(&stream).set_linger(Some(std::time::Duration::ZERO)).unwrap();
    drop(stream);
    for _ in 0..50 {
        if !online_of(&addr, "r@h").await {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("agent stayed online after a reset");
}

async fn welcome_watching(raw: &mut Raw, address: &str) -> (Outbound, bool) {
    let hello = format!("{{\"type\":\"hello\",\"v\":1,\"address\":\"{address}\"}}\n");
    let welcome = raw_hello(raw, &hello).await;
    let wire = crate::protocol::encode(&welcome).unwrap();
    (welcome, wire.contains("watching"))
}

#[tokio::test]
async fn second_watcher_welcome_says_watching_and_the_first_does_not() {
    let (addr, _dir, _handle) = spawn().await;
    let mut first = raw_connect(&addr).await;
    let (welcome, on_wire) = welcome_watching(&mut first, "d@h").await;
    assert!(matches!(welcome, Outbound::Welcome { watching: false, .. }));
    assert!(!on_wire, "false is omitted from the wire");
    raw_send(&mut first, "{\"type\":\"watch\"}\n").await;
    raw_send(&mut first, "{\"type\":\"agents\"}\n").await;
    let _ = raw_recv(&mut first).await.unwrap();

    let mut second = raw_connect(&addr).await;
    let (welcome, on_wire) = welcome_watching(&mut second, "d@h").await;
    assert!(matches!(welcome, Outbound::Welcome { watching: true, .. }));
    assert!(on_wire);

    let mut other = raw_connect(&addr).await;
    let (welcome, _) = welcome_watching(&mut other, "e@h").await;
    assert!(matches!(welcome, Outbound::Welcome { watching: false, .. }));
}
