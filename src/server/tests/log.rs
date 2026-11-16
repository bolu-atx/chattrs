//! Broker tests: log.

use super::helpers::*;
use crate::client;
use crate::model::Outbound;
use tempfile::TempDir;

#[tokio::test]
async fn agents_lists_addresses_that_said_hello() {
    let (addr, _dir, handle) = spawn().await;
    client::publish(&endpoint(&addr, "b@h"), outgoing("a@h", "hi")).await.unwrap();
    client::fetch_inbox(&endpoint(&addr, "a@h")).await.unwrap();

    let agents = client::fetch_agents(&endpoint(&addr, "a@h")).await.unwrap();
    let names: Vec<&str> = agents.iter().map(|agent| agent.address.as_str()).collect();
    assert_eq!(names, vec!["a@h", "b@h"]);
    assert!(agents.iter().all(|agent| agent.last_seen > 0), "{agents:?}");
    handle.abort();
}

#[tokio::test]
async fn agents_omits_addresses_that_only_received() {
    let (addr, _dir, handle) = spawn().await;
    client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();

    let agents = client::fetch_agents(&endpoint(&addr, "a@h")).await.unwrap();
    let names: Vec<&str> = agents.iter().map(|agent| agent.address.as_str()).collect();
    assert_eq!(names, vec!["a@h"]);
    handle.abort();
}

#[tokio::test]
async fn last_seen_updates_on_disconnect() {
    let (addr, _dir, handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
    let first = seen(&addr, "a@h").await;
    assert!(first > 0, "hello records a last_seen");

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    drop(raw);

    let start = tokio::time::Instant::now();
    loop {
        let last_seen = seen(&addr, "a@h").await;
        if last_seen > first {
            break;
        }
        assert!(
            start.elapsed() < std::time::Duration::from_secs(10),
            "disconnect never updated last_seen"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    handle.abort();
}

#[tokio::test]
async fn agents_survive_broker_restart() {
    let (addr, dir, handle) = spawn().await;
    let db = dir.path().join("chattrs.db");
    client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();
    handle.abort();

    let (addr, handle) = spawn_at(&db).await;
    let agents = client::fetch_agents(&endpoint(&addr, "a@h")).await.unwrap();
    assert!(agents.iter().any(|agent| agent.address == "a@h"), "agents survive: {agents:?}");
    handle.abort();
}

#[tokio::test]
async fn log_returns_all_messages_oldest_first() {
    let (addr, _dir, handle) = spawn().await;
    let first = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "one")).await.unwrap();
    let second = client::reply(&endpoint(&addr, "b@h"), first, "two", None).await.unwrap();
    let third = client::publish(&endpoint(&addr, "c@h"), outgoing("a@h", "three")).await.unwrap();

    let history = client::fetch_log(&endpoint(&addr, "reader@h"), None).await.unwrap();
    let ids: Vec<u64> = history.iter().map(|message| message.message.id).collect();
    assert_eq!(ids, vec![first, second, third]);
    assert_eq!(history[0].message.from, "a@h");
    assert_eq!(history[0].message.to.as_deref(), Some("b@h"));
    assert_eq!(history[0].message.body, "one");
    assert_eq!(history[0].message.reply_to, None);
    assert_eq!(history[1].message.from, "b@h");
    assert_eq!(history[1].message.to.as_deref(), Some("a@h"));
    assert_eq!(history[1].message.reply_to, Some(first));
    assert_eq!(history[2].message.from, "c@h");
    handle.abort();
}

#[tokio::test]
async fn log_filters_by_sender_or_recipient() {
    let (addr, _dir, handle) = spawn().await;
    let ab = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "a to b")).await.unwrap();
    let ba = client::publish(&endpoint(&addr, "b@h"), outgoing("a@h", "b to a")).await.unwrap();
    let ac = client::publish(&endpoint(&addr, "a@h"), outgoing("c@h", "a to c")).await.unwrap();
    let xy = client::publish(&endpoint(&addr, "x@h"), outgoing("y@h", "x to y")).await.unwrap();

    async fn filtered(addr: &str, who: &str) -> Vec<u64> {
        client::fetch_log(&endpoint(addr, "reader@h"), Some(who.to_string()))
            .await
            .unwrap()
            .into_iter()
            .map(|message| message.message.id)
            .collect()
    }
    assert_eq!(filtered(&addr, "a@h").await, vec![ab, ba, ac]);
    assert_eq!(filtered(&addr, "b@h").await, vec![ab, ba]);
    assert_eq!(filtered(&addr, "y@h").await, vec![xy]);

    let all = client::fetch_log(&endpoint(&addr, "reader@h"), None).await.unwrap();
    assert_eq!(all.len(), 4);
    handle.abort();
}

#[tokio::test]
async fn log_includes_acked_and_unread_messages() {
    let (addr, _dir, handle) = spawn().await;
    let first = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "one")).await.unwrap();
    let second = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "two")).await.unwrap();
    client::ack(&endpoint(&addr, "b@h"), &[first]).await.unwrap();

    let history = client::fetch_log(&endpoint(&addr, "a@h"), None).await.unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].message.id, first);
    assert!(history[0].acked_at.is_some_and(|ts| ts > 0), "acked message carries acked_at");
    assert_eq!(history[1].message.id, second);
    assert_eq!(history[1].acked_at, None, "unread message has null acked_at");
    handle.abort();
}

#[tokio::test]
async fn log_does_not_ack() {
    let (addr, _dir, handle) = spawn().await;
    let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();

    let history = client::fetch_log(&endpoint(&addr, "a@h"), None).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].message.id, id);

    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(messages.len(), 1, "log leaves the message unread: {messages:?}");
    assert_eq!(messages[0].id, id);
    handle.abort();
}

#[tokio::test]
async fn log_pages_past_500_messages() {
    let (addr, _dir, handle) = spawn().await;
    let mut expected = Vec::new();
    for i in 0..1200 {
        let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", &format!("msg {i}")))
            .await
            .unwrap();
        expected.push(id);
    }

    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
    raw_send(&mut raw, "{\"type\":\"log\",\"after\":0,\"limit\":5000}\n").await;
    match raw_recv(&mut raw).await.unwrap() {
        Outbound::Log { messages } => {
            assert_eq!(messages.len(), 500, "broker caps one page at 500");
        }
        other => panic!("expected a log frame, got {other:?}"),
    }
    drop(raw);

    let history = client::fetch_log(&endpoint(&addr, "a@h"), None).await.unwrap();
    let ids: Vec<u64> = history.iter().map(|message| message.message.id).collect();
    assert_eq!(ids, expected, "all 1200 messages return once, in order");
    handle.abort();
}

#[tokio::test]
async fn log_survives_broker_restart() {
    let (addr, dir, handle) = spawn().await;
    let db = dir.path().join("chattrs.db");
    let first = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "one")).await.unwrap();
    let second = client::reply(&endpoint(&addr, "b@h"), first, "two", None).await.unwrap();
    handle.abort();

    let (addr, handle) = spawn_at(&db).await;
    let history = client::fetch_log(&endpoint(&addr, "a@h"), None).await.unwrap();
    let ids: Vec<u64> = history.iter().map(|message| message.message.id).collect();
    assert_eq!(ids, vec![first, second]);
    assert_eq!(history[0].message.body, "one");
    assert_eq!(history[1].message.body, "two");
    assert_eq!(history[1].message.reply_to, Some(first));
    handle.abort();
}

#[tokio::test]
async fn send_prints_unread_messages_before_its_id() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let first = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "one")).await.unwrap();

    let out = run_chattrs(&bin, "send", &addr, "b@h", &["--to", "a@h", "two"], &db);
    assert!(out.status.success(), "send exits zero: {out:?}");
    let lines = stdout_lines(&out);
    assert_eq!(lines.len(), 5, "wrapped preamble plus the new id: {lines:?}");
    let preamble = lines[..4].join("\n");
    assert!(preamble.contains("one"), "preamble is the unread message: {preamble}");
    assert!(preamble.contains("a@h"), "preamble names the sender: {preamble}");
    let second: u64 = lines[4].parse().expect("last line is the new id");
    assert!(second > first, "the id comes after the preamble: {lines:?}");
}

#[tokio::test]
async fn reply_prints_unread_messages_first() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let original = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();
    let id_arg = original.to_string();

    let out = run_chattrs(&bin, "reply", &addr, "b@h", &[&id_arg, "yo"], &db);
    assert!(out.status.success(), "reply exits zero: {out:?}");
    let lines = stdout_lines(&out);
    assert_eq!(lines.len(), 5, "wrapped preamble plus the new id: {lines:?}");
    let preamble = lines[..4].join("\n");
    assert!(preamble.contains("hi"), "preamble is the unread message: {preamble}");
    let second: u64 = lines[4].parse().expect("last line is the new id");
    assert!(second > original, "the reply id comes after the preamble: {lines:?}");

    let messages = client::fetch_inbox(&endpoint(&addr, "a@h")).await.unwrap();
    assert_eq!(messages.len(), 1, "the reply reaches the asker: {messages:?}");
    assert_eq!(messages[0].id, second);
    assert_eq!(messages[0].body, "yo");
}

#[tokio::test]
async fn agents_prints_unread_messages_first() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();

    let out = run_chattrs(&bin, "agents", &addr, "b@h", &[], &db);
    assert!(out.status.success(), "agents exits zero: {out:?}");
    let lines = stdout_lines(&out);
    assert!(lines.len() >= 5, "wrapped preamble plus agent lines: {lines:?}");
    let preamble = lines[..4].join("\n");
    assert!(preamble.contains("hi"), "preamble is the unread message: {preamble}");
    assert!(
        lines[4..].iter().any(|line| line.contains("a@h")),
        "agents still lists a@h: {lines:?}"
    );
    assert!(
        lines[4..].iter().any(|line| line.contains("b@h")),
        "agents still lists b@h: {lines:?}"
    );
}

#[tokio::test]
async fn preamble_acks_what_it_prints() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "one")).await.unwrap();

    let out = run_chattrs(&bin, "send", &addr, "b@h", &["--to", "a@h", "two"], &db);
    assert!(out.status.success(), "send exits zero: {out:?}");
    let lines = stdout_lines(&out);
    assert_eq!(lines.len(), 5, "wrapped preamble plus the new id: {lines:?}");
    let preamble = lines[..4].join("\n");
    assert!(preamble.contains("one"), "preamble is the unread message: {preamble}");

    let again = run_chattrs(&bin, "send", &addr, "b@h", &["--to", "a@h", "three"], &db);
    assert!(again.status.success(), "send exits zero: {again:?}");
    let lines = stdout_lines(&again);
    assert_eq!(lines.len(), 1, "a second command prints only an id: {lines:?}");
    lines[0].parse::<u64>().expect("the only line is the new id");
}

#[tokio::test]
async fn json_preamble_lines_have_type_message() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let first = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "one")).await.unwrap();

    let out = run_chattrs(&bin, "send", &addr, "b@h", &["--to", "a@h", "--json", "two"], &db);
    assert!(out.status.success(), "send exits zero: {out:?}");
    let lines = stdout_lines(&out);
    assert_eq!(lines.len(), 2, "preamble plus the sent line: {lines:?}");
    let preamble: serde_json::Value = serde_json::from_str(&lines[0]).expect("preamble is JSON");
    assert_eq!(preamble["type"].as_str(), Some("message"), "preamble line: {}", lines[0]);
    assert_eq!(preamble["untrusted"], serde_json::Value::Bool(true));
    assert_eq!(preamble["body"].as_str(), Some("one"));
    assert_eq!(preamble["id"].as_u64(), Some(first));
    let sent: serde_json::Value = serde_json::from_str(&lines[1]).expect("sent is JSON");
    assert_eq!(sent["type"].as_str(), Some("sent"), "own line: {}", lines[1]);
    let id = sent["id"].as_u64().expect("sent carries an id");
    assert!(id > first, "the new id follows the preamble");
}

#[tokio::test]
async fn json_message_lines_carry_untrusted_true() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let first = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();

    let out = run_chattrs(&bin, "inbox", &addr, "b@h", &["--json"], &db);
    assert!(out.status.success(), "inbox exits zero: {out:?}");
    let lines = stdout_lines(&out);
    assert_eq!(lines.len(), 1, "one message line: {lines:?}");
    let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
    assert_eq!(value["type"].as_str(), Some("message"), "line: {}", lines[0]);
    assert_eq!(value["id"].as_u64(), Some(first));
    assert_eq!(value["untrusted"], serde_json::Value::Bool(true));
}

#[test]
fn dist_config_covers_all_targets_and_installers() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let config =
        std::fs::read_to_string(root.join("dist-workspace.toml")).expect("dist config exists");
    for target in [
        "aarch64-apple-darwin",
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "x86_64-pc-windows-msvc",
    ] {
        assert!(config.contains(target), "dist builds {target}");
    }
    for installer in ["shell", "powershell", "homebrew", "npm"] {
        assert!(config.contains(installer), "dist makes the {installer} installer");
    }
    assert!(config.contains("checksum"), "dist writes a checksum file");
    let workflow = std::fs::read_to_string(root.join(".github/workflows/release.yml"))
        .expect("the tag-triggered release workflow exists");
    assert!(workflow.contains("autogenerated by dist"), "dist generates the workflow");
    assert!(workflow.contains("bolu-atx/homebrew-tap"), "the workflow publishes the tap formula");
    assert!(workflow.contains("npm publish"), "the workflow publishes the npm package");
}

#[test]
fn binary_reports_crate_version() {
    let bin = ensure_bin();
    let out =
        std::process::Command::new(&bin).arg("--version").output().expect("run chattrs --version");
    assert!(out.status.success(), "version exits zero: {out:?}");
    let text = String::from_utf8(out.stdout).expect("version is utf8");
    assert_eq!(
        text.trim(),
        format!("chattrs {}", env!("CARGO_PKG_VERSION")),
        "the binary names its crate version"
    );
}
