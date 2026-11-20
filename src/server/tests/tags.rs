//! Broker tests: hello tags.

use super::helpers::*;
use crate::client;
use crate::model::Outbound;
use tempfile::TempDir;

fn hello_with(address: &str, tags: &[String]) -> String {
    let frame = serde_json::json!({"type": "hello", "v": 1, "address": address, "tags": tags});
    format!("{frame}\n")
}

async fn tags_of(addr: &str, address: &str) -> Vec<String> {
    let agents = client::fetch_agents(&endpoint(addr, "observer@h")).await.unwrap();
    agents.into_iter().find(|agent| agent.address == address).unwrap().tags
}

#[tokio::test]
async fn hello_tags_are_stored_and_shown_by_agents() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;

    let out = run_chattrs(&bin, "agents", &addr, "a@h", &[], &db);
    let os = format!("os:{}", std::env::consts::OS);
    let arch = format!("arch:{}", std::env::consts::ARCH);
    let text = stdout_lines(&out);
    let line = text.iter().find(|line| line.starts_with("a@h")).expect("a@h listed");
    assert!(line.ends_with(&format!("  {os},{arch}")), "tags follow last-seen: {line}");

    let out = run_chattrs(&bin, "agents", &addr, "a@h", &["--json"], &db);
    let json: Vec<serde_json::Value> = stdout_lines(&out)
        .iter()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|value: &serde_json::Value| value["address"] == "a@h")
        .collect();
    assert_eq!(json[0]["tags"], serde_json::json!([os, arch]));
}

#[tokio::test]
async fn agents_json_tags_is_an_empty_array_when_none() {
    let (addr, _dir, _handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"old@h\"}\n").await;
    let agents = client::fetch_agents(&endpoint(&addr, "observer@h")).await.unwrap();
    let old = agents.iter().find(|agent| agent.address == "old@h").unwrap();
    assert!(old.tags.is_empty());
    let line = crate::output::agent_json(old);
    let value: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(value["tags"], serde_json::json!([]));
}

#[tokio::test]
async fn empty_hello_keeps_earlier_tags() {
    let (addr, _dir, _handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    let tags = vec!["gpu:4090".to_string()];
    raw_hello(&mut raw, &hello_with("a@h", &tags)).await;
    let mut old_client = raw_connect(&addr).await;
    raw_hello(&mut old_client, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
    assert_eq!(tags_of(&addr, "a@h").await, tags);
    let newer = vec!["gpu:5090".to_string()];
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, &hello_with("a@h", &newer)).await;
    assert_eq!(tags_of(&addr, "a@h").await, newer, "a non-empty hello replaces tags");
}

#[tokio::test]
async fn hello_over_the_tag_limits_gets_an_error_frame() {
    let (addr, _dir, _handle) = spawn().await;
    let too_many: Vec<String> = (0..33).map(|i| format!("t{i}")).collect();
    let too_long = vec!["x".repeat(65)];
    for tags in [too_many, too_long] {
        let mut raw = raw_connect(&addr).await;
        let frame = raw_hello(&mut raw, &hello_with("a@h", &tags)).await;
        assert_eq!(error_code(&frame), "bad_tags");
    }
    let at_limit: Vec<String> = (0..32).map(|_| "y".repeat(64)).collect();
    let mut raw = raw_connect(&addr).await;
    let frame = raw_hello(&mut raw, &hello_with("a@h", &at_limit)).await;
    assert!(matches!(frame, Outbound::Welcome { .. }), "limits are inclusive: {frame:?}");
}
