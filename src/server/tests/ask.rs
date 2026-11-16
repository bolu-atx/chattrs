//! Broker tests: ask.

use super::helpers::*;
use crate::client;
use tempfile::TempDir;

#[tokio::test]
async fn ask_topic_cli_run_shows_topic_in_reply() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("ask.out");
    let err = dir.path().join("ask.err");
    let mut asker =
        start_ask(&bin, &addr, ("w@h", "c@h"), "15", (&out, &err), "go?", Some("release-26"));

    let question = wait_for_question(&addr, "c@h").await;
    assert_eq!(question.topic.as_deref(), Some("release-26"));
    let id_arg = question.id.to_string();
    let reply_out = run_chattrs(&bin, "reply", &addr, "c@h", &[&id_arg, "yes"]);
    assert!(reply_out.status.success(), "reply exits zero: {reply_out:?}");

    let status = wait_for_exit(&mut asker.child, 15).await;
    assert_eq!(status.code(), Some(0), "ask exits zero on a reply");
    let lines = read_out_lines(&out);
    assert_eq!(lines.len(), 4, "ask prints one wrapped reply: {lines:?}");
    assert!(lines[0].contains("topic=\"release-26\""), "wrapper shows the topic: {}", lines[0]);
}

#[tokio::test]
async fn ask_returns_reply_and_exits_zero() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("ask.out");
    let err = dir.path().join("ask.err");
    let mut asker = start_ask(&bin, &addr, ("w@h", "c@h"), "15", (&out, &err), "go?", None);

    let question = wait_for_question(&addr, "c@h").await;
    assert_eq!(question.from, "w@h");
    assert_eq!(question.body, "go?");
    client::reply(&endpoint(&addr, "c@h"), question.id, "yes", None).await.unwrap();

    let status = wait_for_exit(&mut asker.child, 15).await;
    assert_eq!(status.code(), Some(0), "ask exits zero on a reply");
    let lines = read_out_lines(&out);
    assert_eq!(lines.len(), 4, "ask prints one wrapped reply: {lines:?}");
    assert!(lines[0].contains("c@h"), "reply names the replier: {}", lines[0]);
    assert!(lines[2].contains("yes"), "reply carries the body: {}", lines[2]);
    let wanted = format!("reply-to=\"{}\"", question.id);
    assert!(lines[0].contains(&wanted), "reply cites the question: {}", lines[0]);
    assert_eq!(lines[3], "</agent-message>", "wrapper closes last: {lines:?}");
}

#[tokio::test]
async fn ask_times_out_with_exit_code_two() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("ask.out");
    let err = dir.path().join("ask.err");
    let mut asker = start_ask(&bin, &addr, ("w@h", "c@h"), "1", (&out, &err), "go?", None);

    let status = wait_for_exit(&mut asker.child, 10).await;
    assert_eq!(status.code(), Some(2), "ask exits 2 on a timeout");
    let lines = read_out_lines(&out);
    assert!(lines.is_empty(), "no reply means no stdout: {lines:?}");
    let question = wait_for_question(&addr, "c@h").await;
    let stderr = std::fs::read_to_string(&err).unwrap_or_default();
    let wanted = format!("timeout: no reply to {}", question.id);
    assert!(stderr.contains(&wanted), "stderr names the id, got: {stderr}");
}

#[tokio::test]
async fn ask_acks_its_reply() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("ask.out");
    let err = dir.path().join("ask.err");
    let mut asker = start_ask(&bin, &addr, ("w@h", "c@h"), "15", (&out, &err), "go?", None);

    let question = wait_for_question(&addr, "c@h").await;
    client::reply(&endpoint(&addr, "c@h"), question.id, "yes", None).await.unwrap();
    let status = wait_for_exit(&mut asker.child, 15).await;
    assert_eq!(status.code(), Some(0), "ask exits zero on a reply");

    let pending = client::fetch_inbox(&endpoint(&addr, "w@h")).await.unwrap();
    assert!(pending.is_empty(), "the reply stays acked: {pending:?}");
}

#[tokio::test]
async fn late_reply_stays_unread_after_timeout() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("ask.out");
    let err = dir.path().join("ask.err");
    let mut asker = start_ask(&bin, &addr, ("w@h", "c@h"), "1", (&out, &err), "go?", None);

    let status = wait_for_exit(&mut asker.child, 10).await;
    assert_eq!(status.code(), Some(2), "ask exits 2 on a timeout");
    let question = wait_for_question(&addr, "c@h").await;
    let reply_id = client::reply(&endpoint(&addr, "c@h"), question.id, "late", None).await.unwrap();

    let pending = client::fetch_inbox(&endpoint(&addr, "w@h")).await.unwrap();
    assert_eq!(pending.len(), 1, "the late reply stays unread: {pending:?}");
    assert_eq!(pending[0].id, reply_id);
    assert_eq!(pending[0].reply_to, Some(question.id));
    assert_eq!(pending[0].body, "late");
}

#[tokio::test]
async fn ask_ignores_messages_that_are_not_the_reply() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("ask.out");
    let err = dir.path().join("ask.err");
    let mut asker = start_ask(&bin, &addr, ("w@h", "c@h"), "15", (&out, &err), "go?", None);

    let question = wait_for_question(&addr, "c@h").await;
    let noise = client::publish(&endpoint(&addr, "x@h"), outgoing("w@h", "noise")).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        asker.child.try_wait().expect("poll ask child").is_none(),
        "noise does not end the wait"
    );
    client::reply(&endpoint(&addr, "c@h"), question.id, "yes", None).await.unwrap();

    let status = wait_for_exit(&mut asker.child, 15).await;
    assert_eq!(status.code(), Some(0), "ask exits zero on a reply");
    let lines = read_out_lines(&out);
    assert_eq!(lines.len(), 4, "ask prints only the reply: {lines:?}");
    assert!(lines.join("\n").contains("yes"), "ask prints the reply: {lines:?}");
    let pending = client::fetch_inbox(&endpoint(&addr, "w@h")).await.unwrap();
    assert_eq!(pending.len(), 1, "noise stays unread: {pending:?}");
    assert_eq!(pending[0].id, noise);
    assert_eq!(pending[0].body, "noise");
}

#[tokio::test]
async fn ask_survives_broker_restart() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let mut server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let out = dir.path().join("ask.out");
    let err = dir.path().join("ask.err");
    let mut asker = start_ask(&bin, &addr, ("w@h", "c@h"), "30", (&out, &err), "go?", None);

    let question = wait_for_question(&addr, "c@h").await;
    server.stop();
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let mut server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    client::reply(&endpoint(&addr, "c@h"), question.id, "yes", None).await.unwrap();

    let status = wait_for_exit(&mut asker.child, 30).await;
    assert_eq!(status.code(), Some(0), "ask exits zero after a restart");
    let lines = read_out_lines(&out);
    assert_eq!(lines.len(), 4, "ask prints one wrapped reply: {lines:?}");
    assert!(lines.join("\n").contains("yes"), "ask prints the reply: {lines:?}");
    let count: i64 = rusqlite::Connection::open(&db)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM messages WHERE body = 'go?'", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1, "the question exists once in the DB");
    server.stop();
}

#[tokio::test]
async fn ask_prints_unread_before_sending_and_then_the_reply() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    client::publish(&endpoint(&addr, "x@h"), outgoing("w@h", "noise")).await.unwrap();
    let out = dir.path().join("ask.out");
    let err = dir.path().join("ask.err");
    let mut asker = start_ask(&bin, &addr, ("w@h", "c@h"), "15", (&out, &err), "go?", None);

    let question = wait_for_question(&addr, "c@h").await;
    client::reply(&endpoint(&addr, "c@h"), question.id, "yes", None).await.unwrap();

    let status = wait_for_exit(&mut asker.child, 15).await;
    assert_eq!(status.code(), Some(0), "ask exits zero on a reply");
    let lines = read_out_lines(&out);
    assert_eq!(lines.len(), 8, "wrapped preamble plus the wrapped reply: {lines:?}");
    let preamble = lines[..4].join("\n");
    let reply = lines[4..].join("\n");
    assert!(preamble.contains("noise"), "preamble prints what waited: {preamble}");
    assert!(!preamble.contains("yes"), "preamble never contains the reply: {preamble}");
    assert!(reply.contains("yes"), "ask prints the reply: {reply}");
    let wanted = format!("reply-to=\"{}\"", question.id);
    assert!(reply.contains(&wanted), "reply cites the question: {reply}");
}
