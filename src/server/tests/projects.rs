//! Broker tests: project namespaces (NS-1).

use super::super::*;
use super::helpers::*;
use crate::client;
use crate::model::Outbound;
use tempfile::TempDir;

/// A hello for `address` in `project`; `extra` is more JSON fields.
fn hello_in(address: &str, project: Option<&str>, extra: &str) -> String {
    let project = project.map(|p| format!(",\"project\":\"{p}\"")).unwrap_or_default();
    format!("{{\"type\":\"hello\",\"v\":1,\"address\":\"{address}\"{project}{extra}}}\n")
}

async fn join(addr: &str, address: &str, project: &str) -> Raw {
    let mut raw = raw_connect(addr).await;
    let welcome = raw_hello(&mut raw, &hello_in(address, Some(project), "")).await;
    assert!(matches!(welcome, Outbound::Welcome { .. }), "{welcome:?}");
    raw
}

async fn observe_all(addr: &str) -> Raw {
    let mut raw = raw_connect(addr).await;
    let hello = hello_in("tray@h", Some("*"), ",\"observer\":true");
    let welcome = raw_hello(&mut raw, &hello).await;
    assert!(matches!(welcome, Outbound::Welcome { observer: true, .. }), "{welcome:?}");
    raw
}

/// Send `frame` and read the one answer.
async fn ask(raw: &mut Raw, frame: &str) -> Outbound {
    raw_send(raw, &format!("{frame}\n")).await;
    raw_recv(raw).await.unwrap()
}

async fn send_to(raw: &mut Raw, to: &str, body: &str) -> (u64, Option<bool>) {
    let frame = serde_json::json!({"type": "send", "to": to, "body": body});
    match ask(raw, &frame.to_string()).await {
        Outbound::Sent { id, known } => (id, known),
        other => panic!("expected sent, got {other:?}"),
    }
}

async fn inbox_ids(raw: &mut Raw) -> Vec<u64> {
    match ask(raw, "{\"type\":\"inbox\"}").await {
        Outbound::Inbox { messages } => messages.iter().map(|m| m.id).collect(),
        other => panic!("expected inbox, got {other:?}"),
    }
}

async fn agents_of(raw: &mut Raw) -> Vec<(String, String)> {
    match ask(raw, "{\"type\":\"agents\"}").await {
        Outbound::Agents { agents, .. } => {
            agents.into_iter().map(|agent| (agent.project, agent.address)).collect()
        }
        other => panic!("expected agents, got {other:?}"),
    }
}

async fn log_ids(raw: &mut Raw) -> Vec<u64> {
    match ask(raw, "{\"type\":\"log\"}").await {
        Outbound::Log { messages } => messages.iter().map(|m| m.message.id).collect(),
        other => panic!("expected log, got {other:?}"),
    }
}

#[tokio::test]
async fn a_project_cannot_see_another_projects_agents_inbox_or_log() {
    let (addr, _dir, handle) = spawn().await;
    let mut a = join(&addr, "a@h", "alpha").await;
    let mut b = join(&addr, "b@h", "beta").await;
    let mut only_b = join(&addr, "c@h", "beta").await;

    // `c@h` exists only in beta, so the send lands in alpha and is unknown.
    let (id, known) = send_to(&mut a, "c@h", "from alpha").await;
    assert_eq!(known, Some(false), "known looks only inside the sender's project");
    assert!(inbox_ids(&mut only_b).await.is_empty(), "beta's c@h gets nothing");
    let mut alpha_c = join(&addr, "c@h", "alpha").await;
    assert_eq!(inbox_ids(&mut alpha_c).await, [id], "the message waits in alpha");

    let (beta_id, known) = send_to(&mut b, "c@h", "from beta").await;
    assert_eq!(known, Some(true));
    assert_eq!(inbox_ids(&mut only_b).await, [beta_id]);

    assert_eq!(
        agents_of(&mut a).await,
        [("alpha".into(), "a@h".into()), ("alpha".into(), "c@h".into())]
    );
    assert_eq!(log_ids(&mut a).await, [id]);
    assert_eq!(log_ids(&mut b).await, [beta_id]);

    // A cannot ack a beta message, and cannot reply to one.
    match ask(&mut a, &format!("{{\"type\":\"ack\",\"ids\":[{beta_id}]}}")).await {
        Outbound::Acked { ids } => assert!(ids.is_empty()),
        other => panic!("{other:?}"),
    }
    let reply =
        format!("{{\"type\":\"send\",\"to\":\"b@h\",\"body\":\"x\",\"reply_to\":{beta_id}}}");
    assert_eq!(error_code(&ask(&mut a, &reply).await), "unknown_message");
    handle.abort();
}

#[tokio::test]
async fn watch_and_topics_stay_inside_the_project() {
    let (addr, _dir, handle) = spawn().await;
    let mut a = raw_connect(&addr).await;
    raw_hello(&mut a, &hello_in("a@h", Some("alpha"), "")).await;
    raw_send(&mut a, "{\"type\":\"watch\"}\n").await;
    let mut a_topic = raw_connect(&addr).await;
    raw_hello(&mut a_topic, &hello_in("t@h", Some("alpha"), "")).await;
    raw_send(&mut a_topic, "{\"type\":\"watch\",\"topics\":[\"room\"]}\n").await;
    let mut b = join(&addr, "b@h", "beta").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    send_to(&mut b, "a@h", "cross").await;
    let announce = "{\"type\":\"send\",\"body\":\"hear\",\"topic\":\"room\"}";
    ask(&mut b, announce).await;
    expect_no_message(&mut a).await;
    expect_no_message(&mut a_topic).await;

    // The beta announcement is also absent from alpha's topic backlog.
    let mut late = raw_connect(&addr).await;
    raw_hello(&mut late, &hello_in("l@h", Some("alpha"), "")).await;
    raw_send(&mut late, "{\"type\":\"watch\",\"topics\":[\"room\"]}\n").await;
    expect_no_message(&mut late).await;
    let mut beta_late = raw_connect(&addr).await;
    raw_hello(&mut beta_late, &hello_in("m@h", Some("beta"), "")).await;
    raw_send(&mut beta_late, "{\"type\":\"watch\",\"topics\":[\"room\"]}\n").await;
    assert_eq!(read_watched_message(&mut beta_late).await.body, "hear");
    handle.abort();
}

#[tokio::test]
async fn same_address_in_two_projects_is_two_agents() {
    let (addr, _dir, handle) = spawn().await;
    let mut a = join(&addr, "w@h", "alpha").await;
    let mut b = join(&addr, "w@h", "beta").await;
    let mut tray = observe_all(&addr).await;
    let rows = agents_of(&mut tray).await;
    assert_eq!(rows, [("alpha".into(), "w@h".into()), ("beta".into(), "w@h".into())]);
    let (id, _) = send_to(&mut a, "w@h", "self").await;
    assert_eq!(inbox_ids(&mut a).await, [id]);
    assert!(inbox_ids(&mut b).await.is_empty(), "the other w@h has its own inbox");
    handle.abort();
}

#[tokio::test]
async fn star_observer_sees_every_project_and_frames_carry_project() {
    let (addr, _dir, handle) = spawn().await;
    let mut a = join(&addr, "a@h", "alpha").await;
    let mut b = join(&addr, "b@h", "beta").await;
    let mut tray = observe_all(&addr).await;
    raw_send(&mut tray, "{\"type\":\"watch\",\"all\":true}\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    send_to(&mut a, "x@h", "one").await;
    send_to(&mut b, "y@h", "two").await;
    let first = read_watched_message(&mut tray).await;
    let second = read_watched_message(&mut tray).await;
    assert_eq!((first.project.as_str(), first.body.as_str()), ("alpha", "one"));
    assert_eq!((second.project.as_str(), second.body.as_str()), ("beta", "two"));

    let mut viewer = observe_all(&addr).await;
    assert_eq!(log_ids(&mut viewer).await, [first.id, second.id], "log spans projects too");

    // An observer in one project sees only that project.
    let mut scoped = raw_connect(&addr).await;
    raw_hello(&mut scoped, &hello_in("v@h", Some("beta"), ",\"observer\":true")).await;
    raw_send(&mut scoped, "{\"type\":\"watch\",\"all\":true}\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    send_to(&mut a, "x@h", "three").await;
    send_to(&mut b, "y@h", "four").await;
    assert_eq!(read_watched_message(&mut scoped).await.body, "four");
    handle.abort();
}

#[tokio::test]
async fn star_and_invalid_projects_get_bad_project() {
    let (addr, _dir, handle) = spawn().await;
    for (project, extra) in [("*", ""), ("Bad Name", ""), ("-x", ",\"observer\":true"), ("", "")] {
        let mut raw = raw_connect(&addr).await;
        let frame = raw_hello(&mut raw, &hello_in("a@h", Some(project), extra)).await;
        assert_eq!(error_code(&frame), "bad_project", "{project:?}");
        assert!(raw_recv(&mut raw).await.is_err(), "the connection closes");
    }
    handle.abort();
}

#[tokio::test]
async fn welcome_echoes_the_effective_project_and_old_clients_land_in_default() {
    let (addr, _dir, handle) = spawn().await;
    let mut raw = raw_connect(&addr).await;
    let welcome = raw_hello(&mut raw, &hello_in("old@h", None, "")).await;
    let Outbound::Welcome { project, .. } = welcome else { panic!("{welcome:?}") };
    assert_eq!(project.as_deref(), Some("default"));
    assert_eq!(agents_of(&mut raw).await, [("default".into(), "old@h".into())]);
    let mut raw = raw_connect(&addr).await;
    let welcome = raw_hello(&mut raw, &hello_in("n@h", Some("alpha"), "")).await;
    let Outbound::Welcome { project, .. } = welcome else { panic!("{welcome:?}") };
    assert_eq!(project.as_deref(), Some("alpha"));
    let mut tray = observe_all(&addr).await;
    let (_, known) = send_to(&mut raw, "old@h", "x").await;
    assert_eq!(known, Some(false), "default's old@h is not alpha's");
    assert_eq!(agents_of(&mut tray).await.len(), 2);
    handle.abort();
}

#[tokio::test]
async fn watching_counts_per_project() {
    let (addr, _dir, handle) = spawn().await;
    let mut first = raw_connect(&addr).await;
    raw_watch(&mut first, "w@h").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let mut same = raw_connect(&addr).await;
    let welcome = raw_hello(&mut same, &hello_in("w@h", None, "")).await;
    assert!(matches!(welcome, Outbound::Welcome { watching: true, .. }));
    let mut other = raw_connect(&addr).await;
    let welcome = raw_hello(&mut other, &hello_in("w@h", Some("beta"), "")).await;
    assert!(matches!(welcome, Outbound::Welcome { watching: false, .. }), "{welcome:?}");
    let mut tray = observe_all(&addr).await;
    match ask(&mut tray, "{\"type\":\"agents\"}").await {
        Outbound::Agents { agents, .. } => {
            let online: Vec<(&str, bool)> =
                agents.iter().map(|a| (a.project.as_str(), a.online)).collect();
            assert_eq!(online, [("beta", false), ("default", true)]);
        }
        other => panic!("{other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn projects_lists_counts_for_every_project() {
    let (addr, _dir, handle) = spawn().await;
    let mut a = join(&addr, "a@h", "alpha").await;
    join(&addr, "b@h", "alpha").await;
    let mut c = join(&addr, "c@h", "beta").await;
    send_to(&mut a, "b@h", "1").await;
    send_to(&mut a, "b@h", "2").await;
    send_to(&mut c, "a@h", "3").await;
    let projects = client::projects(&endpoint(&addr, "viewer@h")).await.unwrap();
    let rows: Vec<(&str, u64, u64)> =
        projects.iter().map(|p| (p.project.as_str(), p.agents, p.messages)).collect();
    assert_eq!(rows, [("alpha", 2, 2), ("beta", 1, 1)]);
    // A non-`*` connection that asks gets only its own project.
    match ask(&mut a, "{\"type\":\"agents\",\"projects\":true}").await {
        Outbound::Agents { projects: Some(own), .. } => {
            assert_eq!(own.len(), 1);
            assert_eq!(own[0].project, "alpha");
        }
        other => panic!("{other:?}"),
    }
    handle.abort();
}

fn seed_agents(db: &Path) {
    let conn = rusqlite::Connection::open(db).unwrap();
    let now = store::now_ms() as i64;
    let old = now - 8 * 24 * 60 * 60 * 1000;
    for address in ["gone@h", "chatty@h"] {
        conn.execute(
            "INSERT INTO agents (project, address, last_seen) VALUES ('default', ?1, ?2)",
            rusqlite::params![address, old],
        )
        .unwrap();
    }
    conn.execute(
        "INSERT INTO messages (sender, recipient, body, ts, project) VALUES ('chatty@h', 'x@h', 'hi', ?1, 'default')",
        [now],
    )
    .unwrap();
}

#[tokio::test]
async fn agents_hides_long_quiet_offline_agents_unless_all() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let (addr, handle) = spawn_at(&db).await;
    seed_agents(&db);
    let mut conn = client::Client::connect(&endpoint(&addr, "me@h")).await.unwrap();
    let shown: Vec<String> =
        conn.agents(false).await.unwrap().into_iter().map(|agent| agent.address).collect();
    assert_eq!(shown, ["chatty@h", "me@h"], "gone@h is hidden, chatty@h sent lately");
    let all: Vec<String> =
        conn.agents(true).await.unwrap().into_iter().map(|agent| agent.address).collect();
    assert_eq!(all, ["chatty@h", "gone@h", "me@h"]);
    handle.abort();
}

fn chattrs_in(
    bin: &Path,
    project: &str,
    addr: &str,
    who: &str,
    args: &[&str],
) -> std::process::Output {
    std::process::Command::new(bin)
        .args(args)
        .arg("--server")
        .arg(addr)
        .arg("--address")
        .arg(who)
        .env("CHATTRS_PROJECT", project)
        .output()
        .expect("run chattrs")
}

#[tokio::test]
async fn cli_reads_chattrs_project_and_projects_command_lists_them() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("chattrs.db");
    let addr = free_addr();
    let bin = ensure_bin();
    let _server = start_server(&bin, &addr, &db);
    wait_for_server(&addr).await;

    let sent = chattrs_in(&bin, "alpha", &addr, "a@h", &["send", "--to", "b@h", "hello"]);
    assert!(sent.status.success());
    let other = chattrs_in(&bin, "beta", &addr, "b@h", &["inbox"]);
    assert!(stdout_lines(&other).is_empty(), "beta's b@h has nothing");
    let own = chattrs_in(&bin, "alpha", &addr, "b@h", &["inbox"]);
    assert!(stdout_lines(&own).iter().any(|line| line.contains("hello")));

    let text = chattrs_in(&bin, "beta", &addr, "v@h", &["projects"]);
    assert_eq!(stdout_lines(&text), ["alpha  2 agents  1 messages", "beta  1 agents  0 messages"]);
    let json = chattrs_in(&bin, "beta", &addr, "v@h", &["projects", "--json"]);
    let first: serde_json::Value = serde_json::from_str(&stdout_lines(&json)[0]).unwrap();
    assert_eq!(first["type"], "project");
    assert_eq!(first["project"], "alpha");

    let agents = chattrs_in(&bin, "alpha", &addr, "v@h", &["agents", "--json"]);
    let rows: Vec<serde_json::Value> =
        stdout_lines(&agents).iter().map(|line| serde_json::from_str(line).unwrap()).collect();
    assert!(rows.iter().all(|row| row["project"] == "alpha"), "{rows:?}");
}
