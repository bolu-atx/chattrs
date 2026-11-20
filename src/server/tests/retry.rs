//! Broker tests: `send` and `reply` retry the connection (CLI-12).
//!
//! The client dials `0.0.0.0:<port>` while the broker listens on
//! `127.0.0.1:<port>`. The address is not loopback, so a refused connect
//! never autostarts a broker and the retry path runs.

use super::fixture::*;
use crate::client;
use tempfile::TempDir;

fn dial_addr(addr: &str) -> String {
    addr.replace("127.0.0.1", "0.0.0.0")
}

fn spawn_cli(
    bin: &std::path::Path,
    subcommand: &str,
    addr: &str,
    extra: &[&str],
    files: (&std::path::Path, &std::path::Path),
    db: &std::path::Path,
) -> ProcChild {
    let child = super::fixture::ports::command(bin)
        .arg(subcommand)
        .arg("--server")
        .arg(dial_addr(addr))
        .arg("--address")
        .arg("w@h")
        .args(extra)
        .env("CHATTRS_DB", db)
        .stdout(std::fs::File::create(files.0).unwrap())
        .stderr(std::fs::File::create(files.1).unwrap())
        .spawn()
        .expect("spawn cli");
    ProcChild { child }
}

#[tokio::test]
async fn send_retries_until_the_broker_is_up() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let (out, err) = (dir.path().join("send.out"), dir.path().join("send.err"));
    let mut sender = spawn_cli(&bin, "send", &addr, &["--to", "b@h", "hi"], (&out, &err), &db);

    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    assert!(sender.child.try_wait().unwrap().is_none(), "send keeps retrying");
    let _server = start_server(&bin, &addr, &db);
    let status = wait_for_exit(&mut sender.child, 15).await;

    assert_eq!(status.code(), Some(0), "send exits zero once the broker is up");
    let stored = client::fetch_inbox(&endpoint(&addr, "b@h")).await.unwrap();
    assert_eq!(stored.len(), 1, "the message is stored once");
    assert_eq!(read_out_lines(&out), vec![stored[0].id.to_string()]);
    let retry = std::fs::read_to_string(&err).unwrap();
    assert!(retry.contains("chattrs: send cannot connect:"), "retry line: {retry}");
    assert!(retry.contains("retrying in 1s"), "retry line: {retry}");
}

#[tokio::test]
async fn reply_retries_until_the_broker_is_up() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let mut first = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;
    let question = client::publish(&endpoint(&addr, "c@h"), outgoing("w@h", "go?")).await.unwrap();
    first.stop();
    let (out, err) = (dir.path().join("reply.out"), dir.path().join("reply.err"));
    let id_arg = question.to_string();
    let mut replier = spawn_cli(&bin, "reply", &addr, &[&id_arg, "yes"], (&out, &err), &db);

    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    assert!(replier.child.try_wait().unwrap().is_none(), "reply keeps retrying");
    let _server = start_server(&bin, &addr, &db);
    let status = wait_for_exit(&mut replier.child, 15).await;

    assert_eq!(status.code(), Some(0), "reply exits zero once the broker is up");
    let answers = client::fetch_inbox(&endpoint(&addr, "c@h")).await.unwrap();
    assert_eq!(answers.len(), 1, "the reply is stored once");
    assert_eq!(answers[0].reply_to, Some(question));
    let retry = std::fs::read_to_string(&err).unwrap();
    assert!(retry.contains("chattrs: reply cannot connect:"), "retry line: {retry}");
}

#[tokio::test]
async fn connect_gives_up_at_the_deadline() {
    let addr = dial_addr(&free_addr());
    let start = std::time::Instant::now();
    let result = client::connect_retrying(
        &endpoint(&addr, "w@h"),
        "send",
        std::time::Duration::from_millis(1500),
    )
    .await;
    assert!(matches!(result, Err(client::ClientError::Io(_))), "io error expected");
    let elapsed = start.elapsed();
    assert!(elapsed >= std::time::Duration::from_millis(1500), "waited the window: {elapsed:?}");
    assert!(elapsed < std::time::Duration::from_secs(5), "stopped at the deadline: {elapsed:?}");
}

#[tokio::test]
async fn bad_token_fails_at_once_without_retry() {
    let (addr, _dir, handle) = spawn_with_token("s3").await;
    let start = std::time::Instant::now();
    let result = client::connect_retrying(
        &endpoint(&addr, "w@h"),
        "send",
        std::time::Duration::from_secs(30),
    )
    .await;
    match result {
        Err(client::ClientError::Server { code, .. }) => assert_eq!(code, "bad_token"),
        _ => panic!("expected a bad_token server error"),
    }
    assert!(start.elapsed() < std::time::Duration::from_millis(900), "no backoff pause");
    handle.abort();
}
