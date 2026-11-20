//! Broker tests: auth.

use super::helpers::*;
use crate::client;
use crate::server::{check_listen, serve};
use tempfile::TempDir;

#[tokio::test]
async fn non_loopback_bind_without_token_refuses_to_start() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    for listen in ["0.0.0.0:0", "[::]:0"] {
        let err = serve(listen, &db, None, None).await.unwrap_err();
        assert!(err.contains("token"), "got: {err}");
        assert!(!err.contains("s3"), "token never appears: {err}");
    }
    assert!(check_listen("127.0.0.1:0", false).is_ok());
    assert!(check_listen("0.0.0.0:0", true).is_ok());
}

#[tokio::test]
async fn loopback_bind_without_token_starts() {
    assert!(check_listen("127.0.0.1:0", false).is_ok());
    let (addr, _dir, handle) = spawn().await;
    let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();
    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, id);
    handle.abort();
}

#[tokio::test]
async fn hello_with_wrong_token_is_rejected_and_closed() {
    let (addr, _dir, handle) = spawn_with_token("s3").await;
    let mut raw = raw_connect(&addr).await;
    let frame = raw_hello(
        &mut raw,
        "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\",\"token\":\"wrong\"}\n",
    )
    .await;
    assert_eq!(error_code(&frame), "bad_token");
    assert!(raw_recv(&mut raw).await.is_err(), "connection closes after bad_token");
    handle.abort();
}

#[tokio::test]
async fn hello_with_missing_token_is_rejected_when_broker_has_token() {
    let (addr, _dir, handle) = spawn_with_token("s3").await;
    let mut raw = raw_connect(&addr).await;
    let frame = raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
    assert_eq!(error_code(&frame), "bad_token");
    assert!(raw_recv(&mut raw).await.is_err(), "connection closes after bad_token");
    let err = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap_err();
    assert!(err.to_string().contains("bad_token"), "got: {err}");
    assert!(!err.to_string().contains("s3"), "token never appears: {err}");
    handle.abort();
}

#[tokio::test]
async fn hello_with_right_token_is_welcomed() {
    let (addr, _dir, handle) = spawn_with_token("s3").await;
    let token_endpoint = |address: &str| client::Endpoint {
        server: addr.clone(),
        address: address.to_string(),
        token: Some("s3".to_string()),
    };
    let id = client::publish(&token_endpoint("a@h"), outgoing("b@h", "hi")).await.unwrap();
    let messages = client::fetch_inbox(&token_endpoint("b@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, id);
    assert_eq!(messages[0].body, "hi");
    handle.abort();
}
