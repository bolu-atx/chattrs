//! Broker tests: topics.

use super::helpers::*;
use crate::client;
use crate::model::Outbound;
use tempfile::TempDir;

#[tokio::test]
async fn topic_is_stored_and_shown_in_inbox() {
    let (addr, _dir, handle) = spawn().await;
    let id = client::publish(
        &endpoint(&addr, "a@h"),
        client::Outgoing { topic: Some("release-26".into()), ..outgoing("b@h", "hi") },
    )
    .await
    .unwrap();
    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, id);
    assert_eq!(messages[0].topic.as_deref(), Some("release-26"));
    let value = serde_json::to_value(&messages[0]).unwrap();
    assert_eq!(value["topic"].as_str(), Some("release-26"));
    let text = crate::output::render_message(&messages[0]);
    assert!(text.contains("topic=\"release-26\""), "wrapper shows the topic: {text}");
    handle.abort();
}

#[tokio::test]
async fn message_without_topic_is_general_chat() {
    let (addr, _dir, handle) = spawn().await;
    let id = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "hi")).await.unwrap();
    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, id);
    assert_eq!(messages[0].topic, None);
    let value = serde_json::to_value(&messages[0]).unwrap();
    assert!(value["topic"].is_null(), "general chat wires topic null");
    let text = crate::output::render_message(&messages[0]);
    assert!(!text.contains("topic="), "wrapper omits the topic: {text}");
    let history = client::fetch_log(&endpoint(&addr, "reader@h"), None).await.unwrap();
    assert_eq!(history.len(), 1);
    let line = crate::output::render_log_line(&history[0]);
    assert!(!line.contains("(topic"), "log line omits the topic: {line}");
    handle.abort();
}

#[tokio::test]
async fn reply_inherits_topic() {
    let (addr, _dir, handle) = spawn().await;
    let original = client::publish(
        &endpoint(&addr, "a@h"),
        client::Outgoing { topic: Some("release-26".into()), ..outgoing("b@h", "hi") },
    )
    .await
    .unwrap();
    let id = client::reply(&endpoint(&addr, "b@h"), original, "yo", None).await.unwrap();
    let messages = client::fetch_inbox(&endpoint(&addr, "a@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, id);
    assert_eq!(messages[0].reply_to, Some(original));
    assert_eq!(messages[0].topic.as_deref(), Some("release-26"));
    handle.abort();
}

#[tokio::test]
async fn explicit_topic_overrides_inherited() {
    let (addr, _dir, handle) = spawn().await;
    let original = client::publish(
        &endpoint(&addr, "a@h"),
        client::Outgoing { topic: Some("release-26".into()), ..outgoing("b@h", "hi") },
    )
    .await
    .unwrap();
    let id =
        client::reply(&endpoint(&addr, "b@h"), original, "yo", Some("other-topic")).await.unwrap();
    let messages = client::fetch_inbox(&endpoint(&addr, "a@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, id);
    assert_eq!(messages[0].reply_to, Some(original));
    assert_eq!(messages[0].topic.as_deref(), Some("other-topic"));
    handle.abort();
}

#[tokio::test]
async fn ask_reply_shows_the_asks_topic() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("ask.out");
    let err = dir.path().join("ask.err");
    let mut asker = start_ask(
        &bin,
        &addr,
        "w@h",
        "15",
        (&out, &err),
        client::Outgoing { topic: Some("release-26".into()), ..outgoing("c@h", "go?") },
        &db,
    );

    let question = wait_for_question(&addr, "c@h").await;
    assert_eq!(question.body, "go?");
    assert_eq!(question.topic.as_deref(), Some("release-26"));
    client::reply(&endpoint(&addr, "c@h"), question.id, "yes", None).await.unwrap();

    let status = wait_for_exit(&mut asker.child, 15).await;
    assert_eq!(status.code(), Some(0), "ask exits zero on a reply");
    let lines = read_out_lines(&out);
    assert_eq!(lines.len(), 4, "ask prints one wrapped reply: {lines:?}");
    assert!(lines[0].contains("topic=\"release-26\""), "wrapper shows the topic: {}", lines[0]);
    assert!(lines[2].contains("yes"), "reply carries the body: {}", lines[2]);
}

#[tokio::test]
async fn send_rejects_bad_topic() {
    let (addr, _dir, handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
    let long = "a".repeat(65);
    let cases = vec![
        "Release 26".to_string(),
        "-lead".to_string(),
        "trail-".to_string(),
        String::new(),
        long,
    ];
    for topic in &cases {
        let line = format!(
            "{{\"type\":\"send\",\"to\":\"b@h\",\"body\":\"hi\",\"topic\":{}}}\n",
            serde_json::to_string(topic).unwrap()
        );
        raw_send(&mut raw, &line).await;
        let frame = raw_recv(&mut raw).await.unwrap();
        assert_eq!(error_code(&frame), "bad_topic", "topic {topic:?} is rejected");
    }
    let bad = client::Outgoing { topic: Some("Release 26".into()), ..outgoing("b@h", "hi") };
    let err = client::publish(&endpoint(&addr, "a@h"), bad).await.unwrap_err();
    assert!(err.to_string().contains("bad_topic"), "got: {err}");
    raw_send(&mut raw, "{\"type\":\"send\",\"to\":\"b@h\",\"body\":\"hi\"}\n").await;
    match raw_recv(&mut raw).await.unwrap() {
        Outbound::Sent { .. } => {}
        other => panic!("connection stays open after bad_topic, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn topic_survives_broker_restart() {
    let (addr, dir, handle) = spawn().await;
    let db = dir.path().join("chattrs.db");
    let id = client::publish(
        &endpoint(&addr, "a@h"),
        client::Outgoing { topic: Some("release-26".into()), ..outgoing("b@h", "hi") },
    )
    .await
    .unwrap();
    handle.abort();

    let (addr, handle) = spawn_at(&db).await;
    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, id);
    assert_eq!(messages[0].topic.as_deref(), Some("release-26"));
    handle.abort();
}

#[tokio::test]
async fn existing_db_without_topic_column_is_migrated() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE messages (
               id        INTEGER PRIMARY KEY AUTOINCREMENT,
               sender    TEXT NOT NULL,
               recipient TEXT NOT NULL,
               body      TEXT NOT NULL,
               ts        INTEGER NOT NULL,
               reply_to  INTEGER,
               acked_at  INTEGER
             );",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO messages (sender, recipient, body, ts, reply_to)
             VALUES ('a@h', 'b@h', 'old', 1763200000000, NULL)",
            [],
        )
        .unwrap();
    }
    let (addr, handle) = spawn_at(&db).await;
    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body, "old");
    assert_eq!(messages[0].topic, None, "old rows read back with topic null");

    let id = client::publish(
        &endpoint(&addr, "a@h"),
        client::Outgoing { topic: Some("release-26".into()), ..outgoing("b@h", "new") },
    )
    .await
    .unwrap();
    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    let fresh = messages.iter().find(|message| message.id == id).unwrap();
    assert_eq!(fresh.topic.as_deref(), Some("release-26"));
    handle.abort();
}

#[tokio::test]
async fn topic_shows_in_log() {
    let (addr, _dir, handle) = spawn().await;
    let first = client::publish(&endpoint(&addr, "a@h"), outgoing("b@h", "one")).await.unwrap();
    let second = client::publish(
        &endpoint(&addr, "a@h"),
        client::Outgoing {
            topic: Some("release-26".into()),
            reply_to: Some(first),
            ..outgoing("b@h", "two")
        },
    )
    .await
    .unwrap();
    let history = client::fetch_log(&endpoint(&addr, "reader@h"), None).await.unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].message.id, first);
    assert_eq!(history[0].message.topic, None);
    assert_eq!(history[1].message.id, second);
    assert_eq!(history[1].message.topic.as_deref(), Some("release-26"));
    let plain = crate::output::render_log_line(&history[0]);
    assert!(!plain.contains("(topic"), "general chat shows no topic: {plain}");
    let lined = crate::output::render_log_line(&history[1]);
    let wanted = format!("(topic release-26) (re {first})");
    assert!(lined.contains(&wanted), "topic comes before the reply id: {lined}");
    let logged = crate::output::log_json(&history[1]);
    let value: serde_json::Value = serde_json::from_str(&logged).unwrap();
    assert_eq!(value["topic"].as_str(), Some("release-26"));
    let logged = crate::output::log_json(&history[0]);
    let value: serde_json::Value = serde_json::from_str(&logged).unwrap();
    assert!(value["topic"].is_null(), "general chat logs null topic");
    handle.abort();
}

#[tokio::test]
async fn watch_topic_receives_announcement_from_another_agent() {
    let (addr, _dir, handle) = spawn().await;
    let mut watcher = raw_connect(&addr).await;
    raw_topic_watch(&mut watcher, "ops@h", &["release-26"]).await;

    let id = client::publish(&endpoint(&addr, "w@h"), announce("release-26", "build 421 is red"))
        .await
        .unwrap();
    let got = read_watched_message(&mut watcher).await;
    assert_eq!(got.id, id);
    assert_eq!(got.from, "w@h");
    assert_eq!(got.to, None);
    assert_eq!(got.topic.as_deref(), Some("release-26"));
    assert_eq!(got.body, "build 421 is red");
    let value = serde_json::to_value(&got).unwrap();
    assert!(value["to"].is_null(), "the wire message has to null");
    assert_eq!(value["topic"].as_str(), Some("release-26"));
    handle.abort();
}

#[tokio::test]
async fn announcement_needs_no_recipient() {
    let (addr, _dir, handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"w@h\"}\n").await;
    raw_send(&mut raw, "{\"type\":\"send\",\"topic\":\"release-26\",\"body\":\"hi\"}\n").await;
    let id = match raw_recv(&mut raw).await.unwrap() {
        Outbound::Sent { id } => id,
        other => panic!("expected a sent frame, got {other:?}"),
    };

    let history = client::fetch_log(&endpoint(&addr, "reader@h"), None).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].message.id, id);
    assert_eq!(history[0].message.to, None);
    assert_eq!(history[0].message.topic.as_deref(), Some("release-26"));
    let value = serde_json::to_value(&history[0]).unwrap();
    assert!(value["to"].is_null(), "the wire message has to null");
    handle.abort();
}

#[tokio::test]
async fn watcher_only_gets_subscribed_topics() {
    let (addr, _dir, handle) = spawn().await;
    let mut watcher = raw_connect(&addr).await;
    raw_topic_watch(&mut watcher, "ops@h", &["release-26"]).await;

    let other = client::publish(&endpoint(&addr, "w@h"), announce("other-topic", "elsewhere"))
        .await
        .unwrap();
    let id =
        client::publish(&endpoint(&addr, "w@h"), announce("release-26", "here")).await.unwrap();
    assert!(other < id, "ids grow with send order");

    let got = read_watched_message(&mut watcher).await;
    assert_eq!(got.id, id);
    assert_eq!(got.topic.as_deref(), Some("release-26"));
    expect_no_message(&mut watcher).await;
    handle.abort();
}

#[tokio::test]
async fn watch_accepts_multiple_topics() {
    let (addr, _dir, handle) = spawn().await;
    let first = client::publish(&endpoint(&addr, "w@h"), announce("room-a", "one")).await.unwrap();
    let second = client::publish(&endpoint(&addr, "w@h"), announce("room-b", "two")).await.unwrap();
    assert!(first < second, "ids grow with send order");

    let mut watcher = raw_connect(&addr).await;
    raw_topic_watch(&mut watcher, "ops@h", &["room-a", "room-b"]).await;
    let got_first = read_watched_message(&mut watcher).await;
    let got_second = read_watched_message(&mut watcher).await;
    assert_eq!(got_first.id, first);
    assert_eq!(got_first.topic.as_deref(), Some("room-a"));
    assert_eq!(got_second.id, second);
    assert_eq!(got_second.topic.as_deref(), Some("room-b"));
    handle.abort();
}

#[tokio::test]
async fn announcement_appears_in_no_inbox() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;

    client::publish(&endpoint(&addr, "w@h"), announce("release-26", "build 421 is red"))
        .await
        .unwrap();
    for who in ["w@h", "ops@h", "fresh@h"] {
        let messages = client::fetch_inbox(&endpoint(&addr, who)).await.unwrap();
        assert!(messages.is_empty(), "{who} has no announcement: {messages:?}");
    }

    let out = run_chattrs(&bin, "send", &addr, "ops@h", &["--to", "w@h", "hi"], &db);
    assert!(out.status.success(), "send exits zero: {out:?}");
    let lines = stdout_lines(&out);
    assert_eq!(lines.len(), 1, "no preamble before the new id: {lines:?}");
    lines[0].parse::<u64>().expect("the only line is the new id");

    let messages = client::fetch_inbox(&endpoint(&addr, "w@h")).await.unwrap();
    assert_eq!(messages.len(), 1, "only the direct message waits: {messages:?}");
    assert_eq!(messages[0].body, "hi");
}

#[tokio::test]
async fn direct_message_with_topic_is_not_broadcast() {
    let (addr, _dir, handle) = spawn().await;
    let mut watcher = raw_connect(&addr).await;
    raw_topic_watch(&mut watcher, "ops@h", &["release-26"]).await;

    let id = client::publish(
        &endpoint(&addr, "a@h"),
        client::Outgoing { topic: Some("release-26".into()), ..outgoing("b@h", "hi") },
    )
    .await
    .unwrap();
    expect_no_message(&mut watcher).await;

    let messages = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, id);
    assert_eq!(messages[0].to.as_deref(), Some("b@h"));
    assert_eq!(messages[0].topic.as_deref(), Some("release-26"));
    handle.abort();
}

#[tokio::test]
async fn topic_watch_replays_recent_then_live_without_duplicates() {
    let (addr, _dir, handle) = spawn().await;
    let first =
        client::publish(&endpoint(&addr, "w@h"), announce("release-26", "one")).await.unwrap();
    let second =
        client::publish(&endpoint(&addr, "w@h"), announce("release-26", "two")).await.unwrap();

    let mut watcher = raw_connect(&addr).await;
    raw_topic_watch(&mut watcher, "ops@h", &["release-26"]).await;
    let got_first = read_watched_message(&mut watcher).await;
    let got_second = read_watched_message(&mut watcher).await;
    assert_eq!(got_first.id, first);
    assert_eq!(got_second.id, second);

    let third =
        client::publish(&endpoint(&addr, "w@h"), announce("release-26", "three")).await.unwrap();
    let got_third = read_watched_message(&mut watcher).await;
    assert_eq!(got_third.id, third);
    assert_eq!([got_first.id, got_second.id, got_third.id], [first, second, third]);
    expect_no_message(&mut watcher).await;
    handle.abort();
}

#[tokio::test]
async fn announcements_survive_broker_restart() {
    let (addr, dir, handle) = spawn().await;
    let db = dir.path().join("chattrs.db");
    let id = client::publish(&endpoint(&addr, "w@h"), announce("release-26", "build 421 is red"))
        .await
        .unwrap();
    handle.abort();

    let (addr, handle) = spawn_at(&db).await;
    let mut watcher = raw_connect(&addr).await;
    raw_topic_watch(&mut watcher, "ops@h", &["release-26"]).await;
    let got = read_watched_message(&mut watcher).await;
    assert_eq!(got.id, id);
    assert_eq!(got.to, None);
    assert_eq!(got.topic.as_deref(), Some("release-26"));
    assert_eq!(got.body, "build 421 is red");
    handle.abort();
}

#[tokio::test]
async fn announcements_appear_in_log() {
    let (addr, _dir, handle) = spawn().await;
    let id =
        client::publish(&endpoint(&addr, "codex@box1"), announce("release-26", "build 421 is red"))
            .await
            .unwrap();

    let history = client::fetch_log(&endpoint(&addr, "reader@h"), None).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].message.id, id);
    assert_eq!(history[0].message.to, None);
    assert_eq!(history[0].message.topic.as_deref(), Some("release-26"));
    let line = crate::output::render_log_line(&history[0]);
    assert!(line.contains("→ topic:release-26:"), "log names the room: {line}");
    assert!(!line.contains("(topic "), "the room is not repeated as a suffix: {line}");

    let by_sender =
        client::fetch_log(&endpoint(&addr, "reader@h"), Some("codex@box1".into())).await.unwrap();
    assert_eq!(by_sender.len(), 1, "an address filter still matches the sender");
    let by_other =
        client::fetch_log(&endpoint(&addr, "reader@h"), Some("other@h".into())).await.unwrap();
    assert!(by_other.is_empty(), "an unrelated address matches nothing");
    handle.abort();
}

#[tokio::test]
async fn topic_watch_cli_prints_announcement_as_json() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("topic-watch.out");
    let _watcher = start_topic_watch(&bin, &addr, "ops@h", &["release-26"], &out, &db);

    let id = client::publish(&endpoint(&addr, "w@h"), announce("release-26", "build 421 is red"))
        .await
        .unwrap();
    let lines = wait_for_lines(&out, 1).await;
    assert_eq!(lines.len(), 1, "watch prints one JSON line: {lines:?}");
    let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
    assert_eq!(value["type"].as_str(), Some("message"));
    assert_eq!(value["id"].as_u64(), Some(id));
    assert_eq!(value["topic"].as_str(), Some("release-26"));
    assert!(value["to"].is_null(), "the announcement has no recipient");
    assert_eq!(value["untrusted"], serde_json::Value::Bool(true));

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let again = read_out_lines(&out);
    assert_eq!(again.len(), 1, "the announcement prints once: {again:?}");

    let pending = client::fetch_inbox(&endpoint(&addr, "ops@h")).await.unwrap();
    assert!(pending.is_empty(), "the watcher acks nothing: {pending:?}");
}
