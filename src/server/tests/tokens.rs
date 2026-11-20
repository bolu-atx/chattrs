//! Broker tests: the token table (SEC-4).

use super::helpers::*;
use crate::model::{Message, Outbound};
use crate::output;
use crate::server::serve;
use tempfile::TempDir;

const TABLE: &str = r#"
[[token]]
token = "t-lead"
project = "alpha"
addr = "lead@h"
role = "lead"

[[token]]
token = "t-agent"
project = "alpha"
addr = "*@h"
role = "agent"

[[token]]
token = "t-beta"
project = "beta"
addr = "*@h"
role = "agent"

[[token]]
token = "t-multi"
project = "alpha"
addr = "multi@h"
role = "lead"

[[token]]
token = "t-multi"
project = "beta"
addr = "multi@h"
role = "lead"

[[token]]
token = "t-obs-alpha"
project = "alpha"
addr = "tray@h"
role = "observer"

[[token]]
token = "t-obs-all"
project = "*"
addr = "tray@h"
role = "observer"
"#;

fn hello(token: &str, address: &str, project: &str, observer: bool) -> String {
    let observer = if observer { ",\"observer\":true" } else { "" };
    format!(
        "{{\"type\":\"hello\",\"v\":1,\"address\":\"{address}\",\"token\":\"{token}\",\
         \"project\":\"{project}\"{observer}}}\n"
    )
}

/// Say hello and return the answer frame.
async fn join(addr: &str, line: &str) -> (Raw, Outbound) {
    let mut raw = raw_connect(addr).await;
    let frame = raw_hello(&mut raw, line).await;
    (raw, frame)
}

async fn expect_welcome(addr: &str, line: &str) -> Raw {
    let (raw, frame) = join(addr, line).await;
    assert!(matches!(frame, Outbound::Welcome { .. }), "{frame:?}");
    raw
}

async fn expect_code(addr: &str, line: &str, code: &str) {
    let (_raw, frame) = join(addr, line).await;
    assert_eq!(error_code(&frame), code, "{frame:?}");
}

/// Send one direct message and return its stored form from the inbox.
async fn send_and_fetch(addr: &str, sender: &str, to: &str, body: &str) -> Message {
    let mut raw = expect_welcome(addr, &hello("t-agent", sender, "alpha", false)).await;
    raw_send(&mut raw, &format!("{{\"type\":\"send\",\"to\":\"{to}\",\"body\":\"{body}\"}}\n"))
        .await;
    assert!(matches!(raw_recv(&mut raw).await.unwrap(), Outbound::Sent { .. }));
    let mut inbox = expect_welcome(addr, &hello("t-agent", to, "alpha", false)).await;
    raw_send(&mut inbox, "{\"type\":\"inbox\"}\n").await;
    match raw_recv(&mut inbox).await.unwrap() {
        Outbound::Inbox { mut messages } => messages.remove(0),
        other => panic!("expected inbox, got {other:?}"),
    }
}

#[tokio::test]
async fn unknown_or_missing_token_is_bad_token_and_closes() {
    let (addr, _dir, handle) = spawn_with_tokens(TABLE, None).await;
    let (mut raw, frame) = join(&addr, &hello("nope", "w@h", "alpha", false)).await;
    assert_eq!(error_code(&frame), "bad_token");
    assert!(raw_recv(&mut raw).await.is_err(), "connection closes after bad_token");
    let bare = "{\"type\":\"hello\",\"v\":1,\"address\":\"w@h\"}\n";
    expect_code(&addr, bare, "bad_token").await;
    handle.abort();
}

#[tokio::test]
async fn token_for_another_project_is_forbidden() {
    let (addr, _dir, handle) = spawn_with_tokens(TABLE, None).await;
    expect_code(&addr, &hello("t-agent", "w@h", "beta", false), "forbidden").await;
    let default = "{\"type\":\"hello\",\"v\":1,\"address\":\"w@h\",\"token\":\"t-agent\"}\n";
    expect_code(&addr, default, "forbidden").await;
    expect_welcome(&addr, &hello("t-agent", "w@h", "alpha", false)).await;
    expect_welcome(&addr, &hello("t-beta", "w@h", "beta", false)).await;
    handle.abort();
}

#[tokio::test]
async fn token_for_another_address_is_forbidden() {
    let (addr, _dir, handle) = spawn_with_tokens(TABLE, None).await;
    expect_code(&addr, &hello("t-agent", "w@other", "alpha", false), "forbidden").await;
    expect_code(&addr, &hello("t-lead", "boss@h", "alpha", false), "forbidden").await;
    handle.abort();
}

#[tokio::test]
async fn only_a_lead_token_claims_the_lead_address() {
    let (addr, _dir, handle) = spawn_with_tokens(TABLE, None).await;
    expect_code(&addr, &hello("t-agent", "lead@h", "alpha", false), "forbidden").await;
    expect_welcome(&addr, &hello("t-lead", "lead@h", "alpha", false)).await;
    // The lead address is the lead's in alpha only.
    expect_welcome(&addr, &hello("t-beta", "lead@h", "beta", false)).await;
    handle.abort();
}

#[tokio::test]
async fn observer_token_is_read_only_and_observer_only() {
    let (addr, _dir, handle) = spawn_with_tokens(TABLE, None).await;
    expect_code(&addr, &hello("t-obs-alpha", "tray@h", "alpha", false), "forbidden").await;
    expect_code(&addr, &hello("t-agent", "w@h", "alpha", true), "forbidden").await;
    let mut raw = expect_welcome(&addr, &hello("t-obs-alpha", "tray@h", "alpha", true)).await;
    raw_send(&mut raw, "{\"type\":\"send\",\"to\":\"w@h\",\"body\":\"x\"}\n").await;
    let frame = raw_recv(&mut raw).await.unwrap();
    assert_eq!(error_code(&frame), "observer_read_only");
    handle.abort();
}

#[tokio::test]
async fn star_project_observer_sees_every_project() {
    let (addr, _dir, handle) = spawn_with_tokens(TABLE, None).await;
    let mut obs = expect_welcome(&addr, &hello("t-obs-all", "tray@h", "*", true)).await;
    raw_send(&mut obs, "{\"type\":\"watch\",\"all\":true}\n").await;
    for (token, project) in [("t-agent", "alpha"), ("t-beta", "beta")] {
        let mut sender = expect_welcome(&addr, &hello(token, "w@h", project, false)).await;
        raw_send(&mut sender, "{\"type\":\"send\",\"to\":\"x@h\",\"body\":\"hi\"}\n").await;
        let message = read_watched_message(&mut obs).await;
        assert_eq!(message.project, project);
        assert_eq!(message.from_role.as_deref(), Some("agent"));
    }
    // A per-project observer token cannot see everything.
    expect_code(&addr, &hello("t-obs-alpha", "tray@h", "*", true), "forbidden").await;
    handle.abort();
}

#[tokio::test]
async fn from_role_is_stored_and_shown_everywhere() {
    let (addr, _dir, handle) = spawn_with_tokens(TABLE, None).await;
    let message = send_and_fetch(&addr, "w1@h", "w2@h", "hello").await;
    assert_eq!(message.from_role.as_deref(), Some("agent"));
    let text = output::render_message(&message);
    assert!(text.contains(" to=\"w2@h\" from-role=\"agent\""), "{text}");
    let json = serde_json::to_value(&message).unwrap();
    assert_eq!(json["from_role"], "agent");

    let mut lead = expect_welcome(&addr, &hello("t-lead", "lead@h", "alpha", false)).await;
    raw_send(&mut lead, "{\"type\":\"send\",\"to\":\"w2@h\",\"body\":\"do it\"}\n").await;
    raw_recv(&mut lead).await.unwrap();
    raw_send(&mut lead, "{\"type\":\"log\"}\n").await;
    let Outbound::Log { messages } = raw_recv(&mut lead).await.unwrap() else {
        panic!("expected log");
    };
    let roles: Vec<_> = messages.iter().map(|row| row.message.from_role.as_deref()).collect();
    assert_eq!(roles, [Some("agent"), Some("lead")]);
    assert!(output::render_log_line(&messages[1]).contains("(from lead)"));
    handle.abort();
}

#[tokio::test]
async fn legacy_token_works_as_before_with_no_from_role() {
    let (addr, _dir, handle) = spawn_with_token("s3").await;
    let mut raw = expect_welcome(&addr, &hello("s3", "a@h", "alpha", false)).await;
    raw_send(&mut raw, "{\"type\":\"send\",\"to\":\"b@h\",\"body\":\"hi\"}\n").await;
    assert!(matches!(raw_recv(&mut raw).await.unwrap(), Outbound::Sent { .. }));
    raw_send(&mut raw, "{\"type\":\"log\"}\n").await;
    let Outbound::Log { messages } = raw_recv(&mut raw).await.unwrap() else {
        panic!("expected log");
    };
    assert_eq!(messages[0].message.from_role, None);
    let line = serde_json::to_string(&messages[0].message).unwrap();
    assert!(!line.contains("from_role"), "{line}");
    expect_welcome(&addr, &hello("s3", "lead@h", "beta", true)).await;
    expect_code(&addr, &hello("wrong", "a@h", "alpha", false), "bad_token").await;
    handle.abort();
}

#[tokio::test]
async fn tokens_file_wins_and_the_legacy_token_is_refused() {
    let (addr, _dir, handle) = spawn_with_tokens(TABLE, Some("s3")).await;
    expect_code(&addr, &hello("s3", "w@h", "alpha", false), "bad_token").await;
    expect_welcome(&addr, &hello("t-agent", "w@h", "alpha", false)).await;
    handle.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn serve_refuses_a_tokens_file_that_is_too_open_or_malformed() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let path = dir.path().join("tokens.toml");
    std::fs::write(&path, TABLE).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let err = serve("127.0.0.1:0", &db, None, Some(&path)).await.unwrap_err();
    assert!(err.contains("mode 644"), "{err}");

    std::fs::write(&path, "[[token]]\ntoken = \"a\"\nrole = \"boss\"\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let err = serve("127.0.0.1:0", &db, None, Some(&path)).await.unwrap_err();
    assert!(err.contains("line 1:"), "{err}");
    assert!(!err.contains("\"a\""), "token never appears: {err}");

    let missing = dir.path().join("missing.toml");
    assert!(serve("127.0.0.1:0", &db, None, Some(&missing)).await.is_err());
}

#[tokio::test]
async fn one_lead_token_works_in_each_listed_project_and_not_others() {
    let (addr, _dir, handle) = spawn_with_tokens(TABLE, None).await;
    expect_welcome(&addr, &hello("t-multi", "multi@h", "alpha", false)).await;
    expect_welcome(&addr, &hello("t-multi", "multi@h", "beta", false)).await;
    expect_code(&addr, &hello("t-multi", "multi@h", "gamma", false), "forbidden").await;
    // An agent token repeated across projects works in each.
    expect_welcome(&addr, &hello("t-agent", "w@h", "alpha", false)).await;
    handle.abort();
}
