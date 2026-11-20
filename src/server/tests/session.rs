//! Broker tests: one session per address (ID-1).

use super::helpers::*;
use crate::model::Outbound;

fn hello(address: &str, project: &str, session: Option<&str>, observer: bool) -> String {
    let mut frame = serde_json::json!({"type": "hello", "v": 1, "address": address});
    if project != "default" {
        frame["project"] = project.into();
    }
    if let Some(session) = session {
        frame["session"] = session.into();
    }
    if observer {
        frame["observer"] = true.into();
    }
    format!("{frame}\n")
}

async fn join(addr: &str, address: &str, project: &str, session: Option<&str>) -> (Raw, Outbound) {
    let mut raw = raw_connect(addr).await;
    let frame = raw_hello(&mut raw, &hello(address, project, session, false)).await;
    (raw, frame)
}

fn assert_welcome(frame: &Outbound) {
    assert!(matches!(frame, Outbound::Welcome { .. }), "expected welcome, got {frame:?}");
}

#[tokio::test]
async fn a_second_session_is_refused_with_the_holder_age() {
    let (addr, _dir, _handle) = spawn().await;
    let (_first, frame) = join(&addr, "a@h", "default", Some("s1")).await;
    assert_welcome(&frame);
    let (_second, frame) = join(&addr, "a@h", "default", Some("s2")).await;
    assert_eq!(error_code(&frame), "address_in_use");
    let Outbound::Error { message, .. } = &frame else { unreachable!() };
    assert!(message.contains("last active") && message.contains(" s ago"), "{message}");
}

#[tokio::test]
async fn the_same_session_may_open_more_connections() {
    let (addr, _dir, _handle) = spawn().await;
    let (_first, frame) = join(&addr, "a@h", "default", Some("s1")).await;
    assert_welcome(&frame);
    let (_second, frame) = join(&addr, "a@h", "default", Some("s1")).await;
    assert_welcome(&frame);
}

#[tokio::test]
async fn the_address_is_free_after_the_holder_disconnects() {
    let (addr, _dir, _handle) = spawn().await;
    let (first, frame) = join(&addr, "a@h", "default", Some("s1")).await;
    assert_welcome(&frame);
    let (second, _) = join(&addr, "a@h", "default", Some("s1")).await;
    drop(first);
    // One connection is still live, so the address stays held.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (_other, frame) = join(&addr, "a@h", "default", Some("s2")).await;
    assert_eq!(error_code(&frame), "address_in_use");
    drop(second);
    for _ in 0..50 {
        let (raw, frame) = join(&addr, "a@h", "default", Some("s2")).await;
        if matches!(frame, Outbound::Welcome { .. }) {
            drop(raw);
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("the address was never freed");
}

#[tokio::test]
async fn the_same_address_in_two_projects_is_two_holders() {
    let (addr, _dir, _handle) = spawn().await;
    let (_a, frame) = join(&addr, "a@h", "alpha", Some("s1")).await;
    assert_welcome(&frame);
    let (_b, frame) = join(&addr, "a@h", "beta", Some("s2")).await;
    assert_welcome(&frame);
}

#[tokio::test]
async fn observers_and_old_clients_are_never_refused() {
    let (addr, _dir, _handle) = spawn().await;
    let (_holder, frame) = join(&addr, "a@h", "default", Some("s1")).await;
    assert_welcome(&frame);
    let mut observer = raw_connect(&addr).await;
    let frame = raw_hello(&mut observer, &hello("a@h", "default", Some("s2"), true)).await;
    assert_welcome(&frame);
    let (_old, frame) = join(&addr, "a@h", "default", None).await;
    assert_welcome(&frame);
    // An old client does not block a session holder either.
    let (_old, frame) = join(&addr, "b@h", "default", None).await;
    assert_welcome(&frame);
    let (_new, frame) = join(&addr, "b@h", "default", Some("s1")).await;
    assert_welcome(&frame);
}

#[tokio::test]
async fn a_bad_session_is_rejected() {
    let (addr, _dir, _handle) = spawn().await;
    let long = "x".repeat(129);
    for bad in ["", "has space", "tab\t", "caf\u{e9}", long.as_str()] {
        let (_raw, frame) = join(&addr, "a@h", "default", Some(bad)).await;
        assert_eq!(error_code(&frame), "bad_session", "session {bad:?}");
    }
}
