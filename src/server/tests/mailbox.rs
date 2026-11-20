//! Broker tests: mailbox.

use super::fixture::*;
use tokio::io::AsyncWriteExt as _;

use crate::client;
use crate::model;
use crate::model::Outbound;

#[tokio::test]
async fn message_survives_broker_restart() {
    let (addr, dir, handle) = spawn().await;
    let db = dir.path().join("chattrs.db");
    let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();
    handle.abort();

    let (addr, handle) = spawn_at(&db).await;
    let (sender, recipient, body): (String, String, String) = rusqlite::Connection::open(&db)
        .unwrap()
        .query_row(
            "SELECT sender, recipient, body FROM messages WHERE id = ?1",
            [id as i64],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("row survives the restart");
    assert_eq!(sender, "a@h");
    assert_eq!(recipient, "b@h");
    assert_eq!(body, "hi");

    let next = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "again")).await.unwrap();
    assert_eq!(next, id + 1);
    handle.abort();
}

#[tokio::test]
async fn send_returns_increasing_ids() {
    let (addr, _dir, handle) = spawn().await;
    let mut last = 0;
    for i in 0..5 {
        let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", &format!("msg {i}")))
            .await
            .unwrap();
        assert!(id > last, "ids increase with send order");
        last = id;
    }
    handle.abort();
}

#[tokio::test]
async fn first_frame_must_be_hello() {
    let (addr, _dir, handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    raw_send(&mut raw, "{\"type\":\"send\",\"to\":\"b@h\",\"body\":\"hi\",\"reply_to\":null}\n")
        .await;
    let frame = raw_recv(&mut raw).await.unwrap();
    assert_eq!(error_code(&frame), "hello_required");
    assert!(raw_recv(&mut raw).await.is_err(), "connection closes after hello_required");
    handle.abort();
}

#[tokio::test]
async fn hello_rejects_wrong_version() {
    let (addr, _dir, handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    let frame = raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":2,\"address\":\"a@h\"}\n").await;
    assert_eq!(error_code(&frame), "bad_version");
    handle.abort();
}

#[tokio::test]
async fn hello_rejects_bad_address() {
    let (addr, _dir, handle) = spawn().await;
    for address in ["not-an-address", "a@", "@h", "a@b@c"] {
        let mut raw = raw_connect(&addr).await;
        let hello = format!("{{\"type\":\"hello\",\"v\":1,\"address\":\"{address}\"}}\n");
        let frame = raw_hello(&mut raw, &hello).await;
        assert_eq!(error_code(&frame), "bad_address", "for {address}");
    }
    handle.abort();
}

#[tokio::test]
async fn second_hello_is_rejected() {
    let (addr, _dir, handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
    let frame = raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"other@h\"}\n").await;
    assert_eq!(error_code(&frame), "bad_frame");
    raw_send(&mut raw, "{\"type\":\"send\",\"to\":\"b@h\",\"body\":\"hi\"}\n").await;
    match raw_recv(&mut raw).await.unwrap() {
        Outbound::Sent { .. } => {}
        other => panic!("expected a sent frame, got {other:?}"),
    }
    drop(raw);
    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].from, "a@h");
    handle.abort();
}

#[tokio::test]
async fn sent_reports_whether_the_recipient_ever_connected() {
    let (addr, _dir, handle) = spawn().await;
    client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
    for (to, want) in [("b@h", true), ("typo@h", false)] {
        let line = format!("{{\"type\":\"send\",\"to\":\"{to}\",\"body\":\"hi\"}}\n");
        raw_send(&mut raw, &line).await;
        match raw_recv(&mut raw).await.unwrap() {
            Outbound::Sent { known, .. } => assert_eq!(known, Some(want), "{to}"),
            other => panic!("expected a sent frame, got {other:?}"),
        }
    }
    drop(raw);
    let messages = client::fetch_inbox(&endpoint(&addr, "typo@h")).await.unwrap();
    assert_eq!(messages.len(), 1, "the message is stored for the unknown address");
    handle.abort();
}

#[tokio::test]
async fn pipelined_frames_are_all_answered() {
    let (addr, _dir, handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    let frames = concat!(
        "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n",
        "{\"type\":\"send\",\"to\":\"b@h\",\"body\":\"one\"}\n",
        "{\"type\":\"send\",\"to\":\"b@h\",\"body\":\"two\"}\n",
    );
    raw.writer.write_all(frames.as_bytes()).await.unwrap();
    raw.writer.flush().await.unwrap();
    match raw_recv(&mut raw).await.unwrap() {
        Outbound::Welcome { address, .. } => assert_eq!(address, "a@h"),
        other => panic!("expected a welcome frame, got {other:?}"),
    }
    let mut ids = Vec::new();
    for _ in 0..2 {
        match raw_recv(&mut raw).await.unwrap() {
            Outbound::Sent { id, .. } => ids.push(id),
            other => panic!("expected a sent frame, got {other:?}"),
        }
    }
    assert_ne!(ids[0], ids[1], "pipelined sends get different ids");
    handle.abort();
}

#[tokio::test]
async fn oversize_line_is_rejected_without_buffering() {
    let (addr, _dir, handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
    raw.writer.write_all(&vec![b'x'; 1024 * 1024]).await.unwrap();
    raw.writer.flush().await.unwrap();
    raw.writer.write_all(b"\n").await.unwrap();
    raw.writer.flush().await.unwrap();
    let frame = raw_recv(&mut raw).await.unwrap();
    assert_eq!(error_code(&frame), "too_large");
    raw_send(&mut raw, "{\"type\":\"inbox\"}\n").await;
    match raw_recv(&mut raw).await.unwrap() {
        Outbound::Inbox { messages } => assert!(messages.is_empty()),
        other => panic!("expected an inbox frame, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn send_rejects_empty_body() {
    let (addr, _dir, handle) = spawn().await;
    let err = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "")).await.unwrap_err();
    assert!(err.to_string().contains("empty_body"), "got: {err}");
    handle.abort();
}

#[tokio::test]
async fn send_rejects_oversize_body() {
    let (addr, _dir, handle) = spawn().await;
    let body = "x".repeat(model::MAX_BODY_BYTES + 1);
    let err = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", &body)).await.unwrap_err();
    assert!(err.to_string().contains("too_large"), "got: {err}");
    handle.abort();
}

#[tokio::test]
async fn inbox_returns_only_messages_for_own_address() {
    let (addr, _dir, handle) = spawn().await;
    let to_b = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "for b")).await.unwrap();
    let to_c = client::publish(&endpoint(&addr, "a@h"), outgoing("c@h", "for c")).await.unwrap();

    let b_messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(b_messages.len(), 1);
    assert_eq!(b_messages[0].id, to_b);
    assert_eq!(b_messages[0].from, "a@h");
    assert_eq!(b_messages[0].to.as_deref(), Some("b@h"));
    assert_eq!(b_messages[0].body, "for b");

    let c_messages = client::fetch_inbox(&endpoint(&addr, "c@h")).await.unwrap();
    assert_eq!(c_messages.len(), 1);
    assert_eq!(c_messages[0].id, to_c);
    assert_eq!(c_messages[0].to.as_deref(), Some("c@h"));
    assert_eq!(c_messages[0].body, "for c");
    handle.abort();
}

#[tokio::test]
async fn inbox_returns_messages_in_id_order() {
    let (addr, _dir, handle) = spawn().await;
    let mut expected = Vec::new();
    for i in 0..3 {
        let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", &format!("msg {i}")))
            .await
            .unwrap();
        expected.push(id);
    }
    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    let ids: Vec<u64> = messages.iter().map(|message| message.id).collect();
    assert_eq!(ids, expected);
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]), "ids grow in id order");
    handle.abort();
}

#[tokio::test]
async fn unacked_message_is_redelivered_after_disconnect() {
    let (addr, _dir, handle) = spawn().await;
    let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();

    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"b@h\"}\n").await;
    raw_send(&mut raw, "{\"type\":\"inbox\"}\n").await;
    match raw_recv(&mut raw).await.unwrap() {
        Outbound::Inbox { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, id);
        }
        other => panic!("expected an inbox frame, got {other:?}"),
    }
    drop(raw);

    let again = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].id, id);
    assert_eq!(again[0].body, "hi");
    handle.abort();
}

#[tokio::test]
async fn acked_message_is_not_redelivered() {
    let (addr, _dir, handle) = spawn().await;
    let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();

    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    let acked = client::ack(&endpoint(&addr, "b@h"), &[id]).await.unwrap();
    assert_eq!(acked, vec![id]);

    let again = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert!(again.is_empty(), "acked message stays gone: {again:?}");
    handle.abort();
}

#[tokio::test]
async fn ack_ignores_ids_of_other_addresses() {
    let (addr, _dir, handle) = spawn().await;
    let to_b = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "for b")).await.unwrap();
    let to_c = client::publish(&endpoint(&addr, "a@h"), outgoing("c@h", "for c")).await.unwrap();

    let acked = client::ack(&endpoint(&addr, "b@h"), &[to_b, to_c]).await.unwrap();
    assert_eq!(acked, vec![to_b], "only the own id changes");

    let c_messages = client::fetch_inbox(&endpoint(&addr, "c@h")).await.unwrap();
    assert_eq!(c_messages.len(), 1);
    assert_eq!(c_messages[0].id, to_c);
    handle.abort();
}

#[tokio::test]
async fn reply_goes_to_sender_of_original() {
    let (addr, _dir, handle) = spawn().await;
    let original = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();
    let id = client::reply(&endpoint(&addr, "b@h"), original, "yo", None).await.unwrap();
    assert!(id > original, "reply gets a fresh id");

    let messages = client::fetch_inbox(&endpoint(&addr, "a@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, id);
    assert_eq!(messages[0].from, "b@h");
    assert_eq!(messages[0].to.as_deref(), Some("a@h"));
    assert_eq!(messages[0].body, "yo");
    handle.abort();
}

#[tokio::test]
async fn reply_carries_reply_to_id() {
    let (addr, _dir, handle) = spawn().await;
    let original = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();
    let id = client::reply(&endpoint(&addr, "b@h"), original, "yo", None).await.unwrap();

    let messages = client::fetch_inbox(&endpoint(&addr, "a@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, id);
    assert_eq!(messages[0].reply_to, Some(original));
    handle.abort();
}

#[tokio::test]
async fn reply_to_unknown_id_is_rejected() {
    let (addr, _dir, handle) = spawn().await;
    let err = client::reply(&endpoint(&addr, "b@h"), 9999, "yo", None).await.unwrap_err();
    assert!(err.to_string().contains("unknown_message"), "got: {err}");
    let err = client::publish(
        &endpoint(&addr, "a@h"),
        client::Outgoing { reply_to: Some(9999), ..outgoing("b@h", "hi") },
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("unknown_message"), "got: {err}");

    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
    raw_send(&mut raw, "{\"type\":\"send\",\"body\":\"hi\"}\n").await;
    let frame = raw_recv(&mut raw).await.unwrap();
    assert_eq!(error_code(&frame), "bad_address");
    handle.abort();
}

#[tokio::test]
async fn reply_by_non_recipient_is_rejected() {
    let (addr, _dir, handle) = spawn().await;
    let original = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();
    let err = client::reply(&endpoint(&addr, "c@h"), original, "intrude", None).await.unwrap_err();
    assert!(err.to_string().contains("not_recipient"), "got: {err}");

    let id = client::publish(
        &endpoint(&addr, "c@h"),
        client::Outgoing { reply_to: Some(original), ..outgoing("d@h", "ref") },
    )
    .await
    .unwrap();
    let messages = client::fetch_inbox(&endpoint(&addr, "d@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, id);
    assert_eq!(messages[0].reply_to, Some(original));
    handle.abort();
}

#[tokio::test]
async fn message_to_offline_address_survives_restart_and_arrives_once() {
    let (addr, dir, handle) = spawn().await;
    let db = dir.path().join("chattrs.db");
    let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();
    handle.abort();

    let (addr, handle) = spawn_at(&db).await;
    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, id);
    assert_eq!(messages[0].from, "a@h");
    assert_eq!(messages[0].to.as_deref(), Some("b@h"));
    assert_eq!(messages[0].body, "hi");

    let acked = client::ack(&endpoint(&addr, "b@h"), &[id]).await.unwrap();
    assert_eq!(acked, vec![id]);
    let again = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert!(again.is_empty(), "message arrives once: {again:?}");
    handle.abort();
}
