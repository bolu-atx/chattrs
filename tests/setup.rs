//! Black-box CLI tests for setup, join, and the clean-home solo path.

#[path = "common/broker.rs"]
mod broker;
mod common;

use broker::*;
use common::free_port;
use std::path::PathBuf;
use std::process::{Output, Stdio};
use std::time::{Duration, Instant};

/// Stops the broker that autostart left under a fake home, even when an
/// assert fails first.
struct StopOnDrop {
    home: PathBuf,
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let mut stop = common::command(binary());
        stop.arg("stop");
        clean_home_env(&mut stop, &self.home);
        stop.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
        let _ = stop.status();
    }
}

#[test]
fn readme_solo_path_works_in_a_clean_home() {
    // The README's solo steps on a brand-new machine: no data directory, no
    // config. The first command that needs a broker must start one.
    let started = Instant::now();
    let dir = tempfile::TempDir::new().expect("make a temp home");
    let home = dir.path().to_path_buf();
    let _stop = StopOnDrop { home: home.clone() };
    let server = format!("127.0.0.1:{}", free_port());
    std::fs::create_dir_all(home.join(".claude")).expect("claude dir");
    std::fs::create_dir_all(home.join(".codex")).expect("codex dir");
    let run = |args: &[&str], envs: &[(&str, &str)]| -> Output {
        let mut command = common::command(binary());
        command.args(args);
        clean_home_env(&mut command, &home);
        command.envs(envs.iter().copied());
        command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
        command.output().expect("run chattrs")
    };

    let setup = run(&["setup"], &[]);
    assert_eq!(setup.status.code(), Some(0), "setup: {setup:?}");
    for file in [".claude/settings.json", ".codex/hooks.json"] {
        let text = std::fs::read_to_string(home.join(file)).expect("config file written");
        assert!(text.contains("chattrs hook"), "{file} has the hook: {text}");
    }
    let skill = run(&["skill"], &[]);
    assert_eq!(skill.status.code(), Some(0), "skill: {skill:?}");
    assert!(!skill.stdout.is_empty(), "the skill text is not empty");
    for file in [".claude/skills/chattrs/SKILL.md", ".codex/skills/chattrs/SKILL.md"] {
        let text = std::fs::read(home.join(file)).expect("skill file written");
        assert_eq!(text, skill.stdout, "{file} holds the bundled skill");
    }
    assert!(home.join(".config/chattrs/config.toml").is_file(), "setup wrote the config");

    let env = run(&["env", "--as", "w1@mac", "--lead", "lead@mac", "--server", &server], &[]);
    assert_eq!(env.status.code(), Some(0), "env: {env:?}");
    let exports = parse_exports(&stdout_text(&env));
    for name in ["CHATTRS_ADDRESS", "CHATTRS_SESSION", "CHATTRS_LEAD"] {
        assert!(exports.contains_key(name), "{name} is exported: {exports:?}");
    }
    let worker: Vec<(&str, &str)> =
        exports.iter().map(|(name, value)| (name.as_str(), value.as_str())).collect();

    let lead = [("CHATTRS_ADDRESS", "lead@mac"), ("CHATTRS_SERVER", server.as_str())];
    let sent = run(&["send", "--to", "w1@mac", "hello"], &lead);
    assert_eq!(sent.status.code(), Some(0), "send autostarts a broker: {sent:?}");
    let inbox = run(&["inbox"], &worker);
    assert_eq!(inbox.status.code(), Some(0), "inbox: {inbox:?}");
    assert!(stdout_text(&inbox).contains("hello"), "the worker sees the message: {inbox:?}");

    assert!(started.elapsed() < Duration::from_secs(10), "too slow: {:?}", started.elapsed());
}

/// The bytes of every file `setup` or a user may own, keyed by path.
fn snapshot(home: &std::path::Path, files: &[&str]) -> Vec<Option<Vec<u8>>> {
    files.iter().map(|file| std::fs::read(home.join(file)).ok()).collect()
}

#[test]
fn setup_is_safe_to_run_again() {
    let dir = tempfile::TempDir::new().expect("make a temp home");
    let home = dir.path().to_path_buf();
    std::fs::create_dir_all(home.join(".claude")).expect("claude dir");
    std::fs::create_dir_all(home.join(".codex")).expect("codex dir");
    let run_setup = || -> Output {
        let mut command = common::command(binary());
        command.arg("setup");
        clean_home_env(&mut command, &home);
        command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
        command.output().expect("run chattrs setup")
    };
    let config = home.join(".config/chattrs/config.toml");
    let skill = home.join(".claude/skills/chattrs/SKILL.md");
    let user_files = [".claude/settings.json", ".codex/hooks.json"];

    let first = run_setup();
    assert_eq!(first.status.code(), Some(0), "first setup: {first:?}");
    let stderr = String::from_utf8_lossy(&first.stderr).into_owned();
    assert!(stderr.contains("/hooks in Codex") && stderr.contains("Claude Code"), "{stderr}");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&config).expect("config exists").permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "the config is private");
    }

    // The user edits the config; an upgrade leaves an old skill text behind.
    std::fs::write(&config, "server = \"10.0.0.5:4646\"\n").expect("edit the config");
    std::fs::write(&skill, "old skill text\n").expect("age the skill");
    let before = snapshot(&home, &user_files);
    let second = run_setup();
    assert_eq!(second.status.code(), Some(0), "second setup: {second:?}");
    assert_eq!(snapshot(&home, &user_files), before, "user settings are untouched");
    assert_eq!(
        std::fs::read_to_string(&config).expect("read the config"),
        "server = \"10.0.0.5:4646\"\n",
        "an existing config is kept"
    );
    let bundled = std::fs::read(home.join(".codex/skills/chattrs/SKILL.md")).expect("codex skill");
    assert_eq!(std::fs::read(&skill).expect("claude skill"), bundled, "the skill is refreshed");
}

#[test]
fn setup_skips_an_agent_that_is_not_installed() {
    let dir = tempfile::TempDir::new().expect("make a temp home");
    let home = dir.path().to_path_buf();
    std::fs::create_dir_all(home.join(".claude")).expect("claude dir");
    let mut command = common::command(binary());
    command.arg("setup");
    clean_home_env(&mut command, &home);
    // An empty PATH hides any real `codex` binary from detection.
    command.env("PATH", "");
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let output = command.output().expect("run chattrs setup");
    assert_eq!(output.status.code(), Some(0), "setup: {output:?}");
    assert!(home.join(".claude/skills/chattrs/SKILL.md").is_file());
    assert!(!home.join(".codex").exists(), "codex dir is not created");
}
/// Run chattrs in `home` with `args`, feeding `stdin` text.
fn run_with_stdin(home: &std::path::Path, args: &[&str], stdin: &str) -> Output {
    use std::io::Write as _;
    let mut command = common::command(binary());
    command.args(args);
    clean_home_env(&mut command, home);
    command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn chattrs");
    child.stdin.take().expect("stdin").write_all(stdin.as_bytes()).expect("write stdin");
    child.wait_with_output().expect("run chattrs")
}

#[test]
fn setup_broker_then_join_gives_each_identity_its_token() {
    let broker = tempfile::TempDir::new().expect("broker home");
    let client = tempfile::TempDir::new().expect("client home");
    let join_dir = broker.path().join("joins");
    let dir = join_dir.display().to_string();
    let first = run_with_stdin(
        broker.path(),
        &["setup", "--broker", "--hosts", "mac,bobox", "--bind", "127.0.0.1", "--join-dir", &dir],
        "",
    );
    assert_eq!(first.status.code(), Some(0), "setup --broker: {first:?}");
    let tokens = broker.path().join(".config/chattrs/tokens.toml");
    let unit = if cfg!(target_os = "macos") {
        broker.path().join("Library/LaunchAgents/com.chattrs.broker.plist")
    } else {
        broker.path().join(".config/systemd/user/chattrs.service")
    };
    assert!(tokens.is_file() && unit.is_file(), "tokens and unit are written");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&tokens).expect("tokens").permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "tokens.toml is private");
    }
    let unit_text = std::fs::read_to_string(&unit).expect("unit");
    assert!(unit_text.contains("127.0.0.1:4646") && unit_text.contains("--tokens"), "{unit_text}");
    let stdout = stdout_text(&first);
    assert!(!stdout.contains("chattrs://"), "no join string on stdout: {stdout}");
    let find = |label: &str| {
        let (kind, rest) = label.split_once(' ').expect("label");
        let name = if kind == "agent" { rest } else { kind };
        let text = std::fs::read_to_string(join_dir.join(format!("{name}.join"))).expect(label);
        text.trim().to_string()
    };

    let again = run_with_stdin(
        broker.path(),
        &["setup", "--broker", "--hosts", "mac", "--bind", "127.0.0.1", "--join-dir", &dir],
        "",
    );
    assert_ne!(again.status.code(), Some(0), "a second run refuses");
    assert!(String::from_utf8_lossy(&again.stderr).contains("tokens.toml"));
    let wild = run_with_stdin(
        client.path(),
        &["setup", "--broker", "--hosts", "mac", "--bind", "0.0.0.0"],
        "",
    );
    assert_ne!(wild.status.code(), Some(0), "a wildcard bind is refused");

    let input = format!("{}\n{}\n", find("lead lead@mac"), find("agent bobox"));
    let joined = run_with_stdin(client.path(), &["setup", "--join", "-"], &input);
    assert_eq!(joined.status.code(), Some(0), "setup --join: {joined:?}");
    let leaked = String::from_utf8_lossy(&joined.stdout).to_string()
        + &String::from_utf8_lossy(&joined.stderr);
    assert!(!leaked.contains("chattrs://"), "no join string in the output");
    let token_of = |address: &str| {
        let out = run_with_stdin(client.path(), &["env", "--as", address, "--with-token"], "");
        assert_eq!(out.status.code(), Some(0), "env: {out:?}");
        parse_exports(&stdout_text(&out)).remove("CHATTRS_TOKEN")
    };
    let secret = |join: String| join.split_once('@').map(|(head, _)| head[10..].to_string());
    assert_eq!(token_of("lead@mac"), secret(find("lead lead@mac")));
    assert_eq!(token_of("w1@bobox"), secret(find("agent bobox")));
    let config =
        std::fs::read_to_string(client.path().join(".config/chattrs/config.toml")).expect("config");
    assert!(config.contains("server = \"127.0.0.1:4646\""), "{config}");
    assert!(client.path().join(".config/chattrs/any-bobox.token").is_file());

    let repeat = run_with_stdin(client.path(), &["setup", "--join", &find("agent bobox")], "");
    assert_ne!(repeat.status.code(), Some(0), "re-joining a pattern refuses");
    let forced =
        run_with_stdin(client.path(), &["setup", "--join", &find("agent bobox"), "--force"], "");
    assert_eq!(forced.status.code(), Some(0), "--force replaces: {forced:?}");
}

/// Every token in the broker's tokens file.
fn tokens_in(path: &std::path::Path) -> Vec<String> {
    let text = std::fs::read_to_string(path).expect("tokens file");
    let tokens: Vec<String> = text
        .lines()
        .filter_map(|l| l.strip_prefix("token = \""))
        .map(|l| l.trim_end_matches('"').to_string())
        .collect();
    assert_eq!(tokens.len(), 4, "{text}");
    tokens
}

/// Run `setup --broker` for `hosts` with `--join-dir dir`.
fn broker_to_dir(home: &std::path::Path, hosts: &str, dir: &std::path::Path) -> Output {
    let dir = dir.to_str().expect("utf8 path");
    run_with_stdin(
        home,
        &["setup", "--broker", "--hosts", hosts, "--bind", "127.0.0.1", "--join-dir", dir],
        "",
    )
}

#[cfg(unix)]
#[test]
fn join_dir_writes_private_files_and_prints_only_paths() {
    use std::os::unix::fs::PermissionsExt;
    let mode =
        |p: &std::path::Path| std::fs::metadata(p).expect("meta").permissions().mode() & 0o777;
    let broker = tempfile::TempDir::new().expect("broker home");
    let dir = broker.path().join("out/joins");
    let out = broker_to_dir(broker.path(), "mac,bobox", &dir);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    assert_eq!(mode(&dir), 0o700);
    let shown = stdout_text(&out);
    let all = shown.clone() + &String::from_utf8_lossy(&out.stderr);
    assert!(!all.contains("chattrs://"), "{all}");
    for token in tokens_in(&broker.path().join(".config/chattrs/tokens.toml")) {
        assert!(!all.contains(&token), "a token reached the output");
    }
    for name in ["mac", "bobox", "lead", "observer"] {
        let file = dir.join(format!("{name}.join"));
        assert_eq!(mode(&file), 0o600, "{name}");
        assert!(std::fs::read_to_string(&file).expect("join").starts_with("chattrs://"));
        assert!(shown.contains(&file.display().to_string()), "{shown}");
    }
    assert_eq!(shown.lines().count(), 4, "{shown}");
}

#[test]
fn join_dir_refusals_leave_no_error_text_with_tokens() {
    let broker = tempfile::TempDir::new().expect("broker home");
    let dir = broker.path().join("joins");
    std::fs::create_dir_all(&dir).expect("dir");
    std::fs::write(dir.join("mac.join"), "old").expect("file");
    let out = broker_to_dir(broker.path(), "mac", &dir);
    assert_eq!(out.status.code(), Some(1), "{out:?}");
    assert!(String::from_utf8_lossy(&out.stderr).contains("mac.join"), "{out:?}");
    assert!(!broker.path().join(".config/chattrs/tokens.toml").exists(), "no partial state");
    assert_eq!(std::fs::read_to_string(dir.join("mac.join")).expect("kept"), "old");
}

#[test]
fn broker_without_a_terminal_needs_join_dir() {
    let broker = tempfile::TempDir::new().expect("broker home");
    let out = run_with_stdin(
        broker.path(),
        &["setup", "--broker", "--hosts", "mac", "--bind", "127.0.0.1"],
        "",
    );
    assert_eq!(out.status.code(), Some(1), "{out:?}");
    assert!(out.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains(
            "chattrs: stdout is not a terminal; pass --join-dir DIR so join strings go to files"
        ),
        "{out:?}"
    );
    assert!(!broker.path().join(".config/chattrs/tokens.toml").exists(), "no partial state");
}

#[test]
fn join_file_round_trips_through_stdin() {
    let broker = tempfile::TempDir::new().expect("broker home");
    let client = tempfile::TempDir::new().expect("client home");
    let dir = broker.path().join("joins");
    let out = broker_to_dir(broker.path(), "bobox", &dir);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let file = std::fs::read_to_string(dir.join("bobox.join")).expect("join file");
    assert!(file.ends_with('\n'));
    let joined = run_with_stdin(client.path(), &["setup", "--join", "-"], &file);
    assert_eq!(joined.status.code(), Some(0), "{joined:?}");
    assert!(client.path().join(".config/chattrs/any-bobox.token").is_file());
    let config =
        std::fs::read_to_string(client.path().join(".config/chattrs/config.toml")).expect("config");
    assert!(config.contains("any-bobox.token"), "{config}");
}

#[test]
fn help_never_prints_the_token() {
    let secret = "help-must-not-show-this-token";
    let help = |args: &[&str]| {
        let output = common::command(binary())
            .args(args)
            .env("CHATTRS_TOKEN", secret)
            .output()
            .expect("run chattrs help");
        format!("{}{}", stdout_text(&output), String::from_utf8_lossy(&output.stderr))
    };
    let top = help(&["--help"]);
    let commands: Vec<String> = top
        .lines()
        .skip_while(|line| !line.starts_with("Commands:"))
        .skip(1)
        .take_while(|line| line.starts_with("  "))
        .filter_map(|line| line.split_whitespace().next().map(str::to_string))
        .filter(|name| name != "help")
        .collect();
    assert!(commands.len() > 5, "no subcommands found in help: {top}");
    assert!(!top.contains(secret));
    for name in &commands {
        let text = help(&[name, "--help"]);
        assert!(!text.contains(secret), "`chattrs {name} --help` printed the token");
    }
}
