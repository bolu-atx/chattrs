//! Black-box CLI tests for `chattrs hook` and `chattrs hooks install`.

#[path = "common/broker.rs"]
mod broker;
mod common;

use broker::*;
use common::free_port;
use std::io::{BufRead, BufReader, Write as _};
use std::path::PathBuf;
use std::process::{Output, Stdio};
use std::time::{Duration, Instant};

/// Run `chattrs hook EVENT` with the given stdin. `address` of `None`
/// leaves `CHATTRS_ADDRESS` unset.
fn run_hook(server: &str, address: Option<&str>, event: &str, stdin: &str) -> Output {
    run_hook_with(server, address, event, stdin, &[])
}

/// `run_hook` with extra flags, such as `--agent codex`.
fn run_hook_with(
    server: &str,
    address: Option<&str>,
    event: &str,
    stdin: &str,
    flags: &[&str],
) -> Output {
    let mut command = common::command(binary());
    command.args(["hook", event]).arg("--server").arg(server).args(flags);
    command.env_remove("CHATTRS_ADDRESS");
    if let Some(address) = address {
        command.env("CHATTRS_ADDRESS", address);
    }
    command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn the hook");
    child.stdin.take().expect("hook stdin").write_all(stdin.as_bytes()).expect("write stdin");
    child.wait_with_output().expect("run the hook")
}

#[test]
fn hook_is_a_no_op_without_an_address() {
    let broker = Broker::start();
    run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "hi"]);
    let output = run_hook(&broker.addr, None, "post-tool-use", "{}");
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());
}

#[test]
fn post_tool_use_hook_returns_wrapped_messages_and_acks_them() {
    let broker = Broker::start();
    run_client(&broker.addr, "b@h", "inbox", &[]);
    run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "hello hook"]);
    let output = run_hook(&broker.addr, Some("b@h"), "post-tool-use", "{}");
    assert_eq!(output.status.code(), Some(0));
    let value = parse_line(stdout_text(&output).trim());
    assert_eq!(value["hookSpecificOutput"]["hookEventName"], "PostToolUse");
    let context = value["hookSpecificOutput"]["additionalContext"].as_str().expect("context");
    assert!(context.starts_with("Agent messages below."), "{context}");
    assert!(context.contains("<agent-message") && context.contains("hello hook"), "{context}");
    let inbox = run_client(&broker.addr, "b@h", "inbox", &[]);
    assert!(inbox.stdout.is_empty(), "the hook acked: {:?}", stdout_text(&inbox));
}

#[test]
fn hook_prints_nothing_when_nothing_is_unread() {
    let broker = Broker::start();
    for event in ["user-prompt-submit", "post-tool-use", "stop"] {
        let output = run_hook(&broker.addr, Some("b@h"), event, "{}");
        assert_eq!(output.status.code(), Some(0));
        assert!(output.stdout.is_empty(), "{event}");
    }
}

#[test]
fn stop_hook_blocks_with_unread_messages_unless_stop_hook_active() {
    let broker = Broker::start();
    run_client(&broker.addr, "b@h", "inbox", &[]);
    run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "stop me"]);
    let active = run_hook(&broker.addr, Some("b@h"), "stop", r#"{"stop_hook_active":true}"#);
    assert!(active.stdout.is_empty());
    assert_eq!(agent_state(&broker.addr, "b@h"), "idle");
    let output = run_hook(&broker.addr, Some("b@h"), "stop", "not json");
    let value = parse_line(stdout_text(&output).trim());
    assert_eq!(value["decision"], "block");
    assert!(value["reason"].as_str().expect("reason").contains("stop me"));
    let again = run_hook(&broker.addr, Some("b@h"), "stop", "{}");
    assert!(again.stdout.is_empty(), "the block acked the message");
}

#[test]
fn concurrent_hooks_deliver_a_message_exactly_once() {
    let broker = Broker::start();
    run_client(&broker.addr, "b@h", "inbox", &[]);
    run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "only once"]);
    let runs: Vec<_> = (0..2)
        .map(|_| {
            let server = broker.addr.clone();
            std::thread::spawn(move || run_hook(&server, Some("b@h"), "post-tool-use", "{}"))
        })
        .collect();
    let outputs: Vec<Output> = runs.into_iter().map(|run| run.join().expect("hook")).collect();
    let emitted = outputs.iter().filter(|output| !output.stdout.is_empty()).count();
    assert_eq!(emitted, 1, "exactly one run emits the message");
}

#[test]
fn hook_emits_nothing_when_the_ack_fails() {
    // A broker that serves welcome and inbox, then drops the connection at
    // the ack.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let server = listener.local_addr().expect("addr").to_string();
    std::thread::spawn(move || {
        while let Ok((stream, _)) = listener.accept() {
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            let mut stream = stream;
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap_or(0) > 0 {
                let reply = if line.contains("\"hello\"") {
                    r#"{"type":"welcome","server":"t","v":1,"address":"b@h"}"#
                } else if line.contains("\"inbox\"") {
                    r#"{"type":"inbox","messages":[{"id":1,"from":"a@h","to":"b@h","body":"lost?","ts":0}]}"#
                } else {
                    break;
                };
                writeln!(stream, "{reply}").expect("reply");
                line.clear();
            }
        }
    });
    for event in ["post-tool-use", "user-prompt-submit", "stop"] {
        let output = run_hook(&server, Some("b@h"), event, "{}");
        assert_eq!(output.status.code(), Some(0));
        assert!(output.stdout.is_empty(), "{event} emitted without a confirmed claim");
    }
}

#[test]
fn hook_debug_file_gets_one_json_line_per_run() {
    let broker = Broker::start();
    run_client(&broker.addr, "b@h", "inbox", &[]);
    run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "trace me"]);
    let dir = tempfile::TempDir::new().expect("temp dir");
    let log = dir.path().join("hook.log");
    for _ in 0..2 {
        let mut command = common::command(binary());
        command.args(["hook", "post-tool-use"]).arg("--server").arg(&broker.addr);
        command.env("CHATTRS_ADDRESS", "b@h").env("CHATTRS_HOOK_DEBUG", &log);
        command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn the hook");
        child.stdin.take().expect("stdin").write_all(br#"{"tool_name":"Bash"}"#).expect("stdin");
        child.wait_with_output().expect("run the hook");
    }
    let text = std::fs::read_to_string(&log).expect("debug file");
    let lines: Vec<serde_json::Value> = text.lines().map(parse_line).collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["event"], "PostToolUse");
    assert_eq!(lines[0]["input"]["tool_name"], "Bash");
    assert!(lines[0]["pid"].as_u64().is_some() && lines[0]["ts_ms"].as_u64().is_some());
    assert_eq!(lines[0]["fetched"].as_array().unwrap().len(), 1);
    assert_eq!(lines[0]["emitted"].as_array().unwrap().len(), 1);
    assert!(lines[1]["fetched"].as_array().unwrap().is_empty());
}

#[test]
fn hook_with_an_unreachable_broker_is_silent_fast_and_exits_zero() {
    let server = format!("127.0.0.1:{}", free_port());
    let started = Instant::now();
    let output = run_hook(&server, Some("b@h"), "post-tool-use", "{}");
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());
    assert!(started.elapsed() < Duration::from_secs(1), "{:?}", started.elapsed());
}

#[test]
fn session_start_hook_differs_for_claude_and_codex() {
    let server = format!("127.0.0.1:{}", free_port());
    let claude = run_hook(&server, Some("b@h"), "session-start", r#"{"source":"startup"}"#);
    let codex = run_hook_with(&server, Some("b@h"), "session-start", "{}", &["--agent", "codex"]);
    let explicit =
        run_hook_with(&server, Some("b@h"), "session-start", "{}", &["--agent", "claude"]);
    assert_eq!(stdout_text(&claude), stdout_text(&explicit));
    let text = |output: &Output| {
        let value = parse_line(stdout_text(output).trim());
        assert_eq!(value["hookSpecificOutput"]["hookEventName"], "SessionStart");
        value["hookSpecificOutput"]["additionalContext"].as_str().expect("context").to_string()
    };
    let (claude, codex) = (text(&claude), text(&codex));
    assert!(claude.contains("b@h") && claude.contains("chattrs skill"));
    assert!(claude.contains("chattrs watch") && claude.contains("Monitor"));
    assert!(codex.contains("chattrs skill") && !codex.contains("Monitor"));
}

/// Run `session-start` in process dir `process_dir`, with the hook input
/// naming `hook_cwd`, and return its context text.
fn session_context_in(process_dir: &std::path::Path, hook_cwd: &std::path::Path) -> String {
    let server = format!("127.0.0.1:{}", free_port());
    let mut command = common::command(binary());
    command.args(["hook", "session-start"]).arg("--server").arg(server);
    command.env("CHATTRS_ADDRESS", "b@h").current_dir(process_dir);
    command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn the hook");
    let input = serde_json::json!({ "cwd": hook_cwd }).to_string();
    child.stdin.take().expect("stdin").write_all(input.as_bytes()).expect("write stdin");
    let output = child.wait_with_output().expect("run the hook");
    assert_eq!(output.status.code(), Some(0));
    let value = parse_line(stdout_text(&output).trim());
    value["hookSpecificOutput"]["additionalContext"].as_str().expect("context").to_string()
}

#[test]
fn hook_reads_the_repo_config_from_the_hook_cwd_not_the_process_cwd() {
    let process_dir = tempfile::tempdir().unwrap();
    let hook_dir = tempfile::tempdir().unwrap();
    std::fs::write(process_dir.path().join(".chattrs.toml"), "project = \"procproj\"\n").unwrap();
    std::fs::write(hook_dir.path().join(".chattrs.toml"), "project = \"hookproj\"\n").unwrap();
    let text = session_context_in(process_dir.path(), hook_dir.path());
    assert!(text.contains("Your chattrs project is hookproj."), "{text}");
}

#[test]
fn hook_takes_the_lead_from_the_hook_cwd_config() {
    let process_dir = tempfile::tempdir().unwrap();
    let hook_dir = tempfile::tempdir().unwrap();
    std::fs::write(
        process_dir.path().join(".chattrs.toml"),
        "project = \"procproj\"\nlead = \"plead@m\"\n",
    )
    .unwrap();
    std::fs::write(
        hook_dir.path().join(".chattrs.toml"),
        "project = \"hookproj\"\nlead = \"hlead@m\"\n",
    )
    .unwrap();
    let text = session_context_in(process_dir.path(), hook_dir.path());
    assert!(text.contains("Your lead is hlead@m."), "{text}");
    assert!(!text.contains("plead@m"), "{text}");
}

#[test]
fn hook_has_no_lead_when_the_hook_cwd_config_sets_none() {
    let process_dir = tempfile::tempdir().unwrap();
    let hook_dir = tempfile::tempdir().unwrap();
    std::fs::write(
        process_dir.path().join(".chattrs.toml"),
        "project = \"procproj\"\nlead = \"plead@m\"\n",
    )
    .unwrap();
    std::fs::write(hook_dir.path().join(".chattrs.toml"), "project = \"hookproj\"\n").unwrap();
    let text = session_context_in(process_dir.path(), hook_dir.path());
    assert!(!text.contains("plead@m"), "{text}");
    assert!(!text.contains("Your lead"), "{text}");
}

#[test]
fn hook_derives_the_project_from_git_in_the_hook_cwd() {
    let process_dir = tempfile::tempdir().unwrap();
    std::fs::write(process_dir.path().join(".chattrs.toml"), "project = \"procproj\"\n").unwrap();
    let repo = tempfile::tempdir().unwrap();
    for args in [
        &["init", "-q"][..],
        &["remote", "add", "origin", "https://example.invalid/o/gitrepo.git"][..],
    ] {
        let mut command = common::command("git");
        command
            .args(args)
            .current_dir(repo.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1");
        assert!(command.output().expect("run git").status.success(), "git {args:?}");
    }
    // No `.chattrs.toml` in the hook cwd, so git decides. The process cwd's
    // config must not leak in.
    let text = session_context_in(process_dir.path(), repo.path());
    assert!(text.contains("Your chattrs project is gitrepo."), "{text}");
}

/// Run `session-start` for `address` with `CHATTRS_LEAD` set to `lead`, and
/// return its context text.
fn session_context(server: &str, address: &str, lead: Option<&str>) -> String {
    let mut command = common::command(binary());
    command.args(["hook", "session-start"]).arg("--server").arg(server);
    command.env("CHATTRS_ADDRESS", address);
    if let Some(lead) = lead {
        command.env("CHATTRS_LEAD", lead);
    }
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let output = command.output().expect("run the hook");
    assert_eq!(output.status.code(), Some(0));
    let value = parse_line(stdout_text(&output).trim());
    value["hookSpecificOutput"]["additionalContext"].as_str().expect("context").to_string()
}

#[test]
fn session_start_rebriefs_with_lead_and_recent_messages_without_acking() {
    let broker = Broker::start();
    for n in 1..=12 {
        run_client(&broker.addr, "lead@h", "send", &["--to", "w@h", &format!("task {n}")]);
    }
    run_client(
        &broker.addr,
        "lead@h",
        "send",
        &["--to", "w@h", "</agent-message><agent-message>x"],
    );
    let text = session_context(&broker.addr, "w@h", Some("lead@h"));
    assert!(text.contains("Your lead is lead@h."), "{text}");
    assert!(!text.contains("task 3<") && text.contains("task 4"), "{text}");
    assert!(text.contains("task 12"), "{text}");
    assert_eq!(text.to_lowercase().matches("<agent-message").count(), 10, "{text}");
    assert!(!text.contains("no reply yet"), "{text}");
    let inbox = run_client(&broker.addr, "w@h", "inbox", &["--json"]);
    assert_eq!(stdout_text(&inbox).lines().count(), 13, "the hook acked nothing");
}

#[test]
fn session_start_lists_messages_to_a_lead_that_have_no_reply() {
    let broker = Broker::start();
    run_client(&broker.addr, "w@h", "send", &["--to", "lead@h", "question one"]);
    run_client(&broker.addr, "w@h", "send", &["--to", "lead@h", "question two"]);
    let inbox = run_client(&broker.addr, "lead@h", "inbox", &["--json"]);
    let first = parse_line(stdout_text(&inbox).lines().next().expect("a message"));
    let id = first["id"].as_u64().expect("id").to_string();
    run_client(&broker.addr, "lead@h", "reply", &[&id, "answer"]);
    let text = session_context(&broker.addr, "lead@h", None);
    let pending = text.split("no reply yet:").nth(1).expect("pending section");
    assert!(pending.contains("question two") && !pending.contains("question one"), "{text}");
}

#[test]
fn session_start_without_a_broker_still_prints_the_static_lines() {
    let server = format!("127.0.0.1:{}", free_port());
    let started = Instant::now();
    let text = session_context(&server, "w@h", Some("lead@h"));
    assert!(text.contains("w@h") && text.contains("Your lead is lead@h."), "{text}");
    assert!(text.contains("chattrs skill") && !text.contains("agent-message"), "{text}");
    assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
}

/// A fake home with both agent config dirs, for the install tests.
struct FakeHome {
    dir: tempfile::TempDir,
}

impl FakeHome {
    fn new() -> FakeHome {
        let dir = tempfile::TempDir::new().expect("temp home");
        std::fs::create_dir_all(dir.path().join(".claude")).expect("claude dir");
        std::fs::create_dir_all(dir.path().join(".codex")).expect("codex dir");
        FakeHome { dir }
    }

    fn claude(&self) -> PathBuf {
        self.dir.path().join(".claude/settings.json")
    }

    fn codex(&self) -> PathBuf {
        self.dir.path().join(".codex/hooks.json")
    }

    /// Run `chattrs hooks ARGS` with HOME in the fake home, from `cwd`.
    fn hooks_in(&self, cwd: &std::path::Path, args: &[&str]) -> Output {
        let mut command = common::command(binary());
        command.arg("hooks").args(args);
        command.env("HOME", self.dir.path()).env_remove("CODEX_HOME");
        command.current_dir(cwd);
        command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
        command.output().expect("run hooks")
    }

    fn hooks(&self, args: &[&str]) -> Output {
        self.hooks_in(self.dir.path(), args)
    }
}

const SETTINGS: &str = "{\n  \"model\": \"x\",\n  \"hooks\": {\n    \"Stop\": [\n      {\n        \
\"hooks\": [\n          {\n            \"type\": \"command\",\n            \"command\": \
\"other.sh\"\n          }\n        ]\n      }\n    ],\n    \"PreToolUse\": []\n  }\n}\n";

#[test]
fn hooks_install_twice_is_byte_identical_and_remove_restores_the_original() {
    let home = FakeHome::new();
    std::fs::write(home.claude(), SETTINGS).unwrap();
    let first = home.hooks(&["install"]);
    assert_eq!(first.status.code(), Some(0), "{}", stderr_text(&first));
    let installed = std::fs::read_to_string(home.claude()).unwrap();
    assert!(installed.contains("chattrs hook post-tool-use") && installed.contains("other.sh"));
    assert!(home.codex().exists());
    home.hooks(&["install"]);
    assert_eq!(std::fs::read_to_string(home.claude()).unwrap(), installed);
    let removed = home.hooks(&["remove"]);
    assert_eq!(removed.status.code(), Some(0));
    assert_eq!(std::fs::read_to_string(home.claude()).unwrap(), SETTINGS);
}

#[test]
fn hooks_install_says_the_next_step_for_each_agent() {
    let home = FakeHome::new();
    let stderr = stderr_text(&home.hooks(&["install"]));
    assert!(stderr.contains("codex: open /hooks in Codex and trust the chattrs hooks"), "{stderr}");
    assert!(stderr.contains("claude: Claude Code may ask you to review hooks"), "{stderr}");
    assert!(stderr.contains("PATH"), "{stderr}");
}

#[test]
fn hooks_install_with_invalid_json_exits_one_and_leaves_the_file() {
    let home = FakeHome::new();
    std::fs::write(home.claude(), "{ not json").unwrap();
    let output = home.hooks(&["install", "--agent", "claude"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(std::fs::read_to_string(home.claude()).unwrap(), "{ not json");
    assert!(!home.dir.path().join(".claude/settings.json.bak").exists());
}

#[test]
fn hooks_install_writes_the_backup_once() {
    let home = FakeHome::new();
    std::fs::write(home.claude(), SETTINGS).unwrap();
    home.hooks(&["install", "--agent", "claude"]);
    let backup = home.dir.path().join(".claude/settings.json.bak");
    assert_eq!(std::fs::read_to_string(&backup).unwrap(), SETTINGS);
    home.hooks(&["remove", "--agent", "claude"]);
    home.hooks(&["install", "--agent", "claude"]);
    assert_eq!(std::fs::read_to_string(&backup).unwrap(), SETTINGS);
}

#[test]
fn hooks_install_all_skips_a_missing_agent_dir_but_explicit_creates_it() {
    let home = FakeHome::new();
    std::fs::remove_dir_all(home.dir.path().join(".codex")).unwrap();
    let output = home.hooks(&["install"]);
    assert!(stderr_text(&output).contains("skipping codex"), "{}", stderr_text(&output));
    assert!(!home.codex().exists());
    home.hooks(&["install", "--agent", "codex"]);
    assert!(home.codex().exists());
}

#[test]
fn hooks_project_edits_the_files_in_the_current_dir() {
    let home = FakeHome::new();
    let project = home.dir.path().join("proj");
    std::fs::create_dir_all(project.join(".claude")).unwrap();
    home.hooks_in(&project, &["install", "--project", "--agent", "claude"]);
    let text = std::fs::read_to_string(project.join(".claude/settings.json")).unwrap();
    assert!(text.contains("chattrs hook stop"));
    assert!(!home.claude().exists());
}

/// The state `agents --json` reports for `address`, or null.
fn agent_state(server: &str, address: &str) -> serde_json::Value {
    let output = run_client(server, "obs@h", "agents", &["--json"]);
    stdout_text(&output)
        .lines()
        .map(parse_line)
        .find(|agent| agent["address"] == address)
        .map_or(serde_json::Value::Null, |agent| agent["state"].clone())
}

#[test]
fn user_prompt_submit_hook_marks_the_agent_working() {
    let broker = Broker::start();
    run_hook(&broker.addr, Some("b@h"), "user-prompt-submit", "{}");
    assert_eq!(agent_state(&broker.addr, "b@h"), "working");
}

#[test]
fn session_start_hook_marks_the_agent_idle() {
    let broker = Broker::start();
    run_hook(&broker.addr, Some("b@h"), "session-start", "{}");
    assert_eq!(agent_state(&broker.addr, "b@h"), "idle");
}

#[test]
fn post_tool_use_hook_leaves_the_state_alone() {
    let broker = Broker::start();
    run_hook(&broker.addr, Some("b@h"), "user-prompt-submit", "{}");
    run_hook(&broker.addr, Some("b@h"), "post-tool-use", "{}");
    assert_eq!(agent_state(&broker.addr, "b@h"), "working");
}

#[test]
fn stop_hook_with_nothing_unread_marks_the_agent_idle() {
    let broker = Broker::start();
    run_hook(&broker.addr, Some("b@h"), "user-prompt-submit", "{}");
    let output = run_hook(&broker.addr, Some("b@h"), "stop", "{}");
    assert!(output.stdout.is_empty());
    assert_eq!(agent_state(&broker.addr, "b@h"), "idle");
}

#[test]
fn stop_hook_that_blocks_does_not_leave_the_agent_idle() {
    let broker = Broker::start();
    run_hook(&broker.addr, Some("b@h"), "session-start", "{}");
    run_client(&broker.addr, "a@h", "send", &["--to", "b@h", "keep going"]);
    let output = run_hook(&broker.addr, Some("b@h"), "stop", "{}");
    assert!(stdout_text(&output).contains("block"));
    assert_eq!(agent_state(&broker.addr, "b@h"), "working");
}

#[test]
fn hooks_install_writes_the_agent_flag_into_each_config() {
    let home = FakeHome::new();
    home.hooks(&["install"]);
    let claude = std::fs::read_to_string(home.claude()).unwrap();
    let codex = std::fs::read_to_string(home.codex()).unwrap();
    assert!(claude.contains("chattrs hook session-start --agent claude"));
    assert!(codex.contains("chattrs hook stop --agent codex"));
    assert!(!claude.contains("--agent codex") && !codex.contains("--agent claude"));
}
