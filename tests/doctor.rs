//! Black-box tests for `chattrs doctor` (SETUP-3): a scratch home after
//! `setup`, a real broker on a free port, and the built binary as a child.

mod common;

use std::collections::BTreeMap;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Output, Stdio};
use std::time::{Duration, Instant, SystemTime};

const CLIENT_TOKEN: &str = "tok-secret-w1";
const OTHER_TOKEN: &str = "tok-secret-other";
const LEAD_TOKEN: &str = "tok-secret-lead";
const TOKENS: &str = r#"
[[token]]
token = "tok-secret-w1"
project = "alpha"
addr = "*@box1"
role = "agent"

[[token]]
token = "tok-secret-lead"
project = "alpha"
addr = "lead@box1"
role = "lead"
"#;

fn binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_chattrs"))
}

/// A broker that dies with the test.
struct Broker {
    child: Child,
    server: String,
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_broker(dir: &Path, extra: &[&str]) -> Broker {
    let server = format!("127.0.0.1:{}", common::free_port());
    let db = dir.join("broker.db");
    let child = common::command(binary())
        .args(["serve", "--listen", &server, "--db", db.to_str().expect("db path")])
        .args(extra)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(dir.join("broker.err")).expect("stderr file"))
        .spawn()
        .expect("spawn the broker");
    let broker = Broker { child, server };
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(&broker.server).is_err() {
        assert!(Instant::now() < deadline, "broker never accepted on {}", broker.server);
        std::thread::sleep(Duration::from_millis(50));
    }
    broker
}

fn write_private(path: &Path, text: &str) {
    std::fs::create_dir_all(path.parent().expect("parent")).expect("make the dir");
    std::fs::write(path, text).expect("write the file");
    set_mode(path, 0o600);
}

fn set_mode(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
}

/// A scratch home after `setup`, with a token map for `w1@box1`, plus the
/// broker it points at.
struct Host {
    dir: tempfile::TempDir,
    broker: Broker,
}

impl Host {
    fn home(&self) -> &Path {
        self.dir.path()
    }

    fn run(&self, args: &[&str], envs: &[(&str, &str)]) -> Output {
        let mut command = common::command(binary());
        command
            .args(args)
            .env("HOME", self.home())
            .env("CODEX_HOME", self.home().join(".codex"))
            .env_remove("XDG_CONFIG_HOME")
            .env("PATH", "")
            .env("CHATTRS_ADDRESS", "w1@box1")
            .env_remove("CHATTRS_SESSION")
            .envs(envs.iter().copied())
            .current_dir(self.home())
            .stdin(Stdio::null());
        command.output().expect("run chattrs")
    }

    /// The `rejected hello` lines the broker has logged so far.
    fn rejections(&self) -> Vec<String> {
        let log = std::fs::read_to_string(self.home().join("broker.err")).expect("broker log");
        log.lines().filter(|line| line.contains("rejected hello")).map(str::to_string).collect()
    }

    /// Run doctor, and check that no secret reached stdout or stderr.
    fn doctor(&self, envs: &[(&str, &str)]) -> (i32, String) {
        let output = self.run(&["doctor"], envs);
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        for secret in [CLIENT_TOKEN, OTHER_TOKEN, LEAD_TOKEN] {
            assert!(!stdout.contains(secret) && !stderr.contains(secret), "{stdout}{stderr}");
        }
        (output.status.code().expect("exit code"), stdout)
    }
}

/// A host that passes every check, against a broker with a token table.
fn healthy_host() -> Host {
    let dir = tempfile::TempDir::new().expect("make a temp home");
    let home = dir.path().to_path_buf();
    std::fs::create_dir_all(home.join(".claude")).expect("claude dir");
    std::fs::create_dir_all(home.join(".codex")).expect("codex dir");
    let tokens = home.join("broker-tokens.toml");
    write_private(&tokens, TOKENS);
    let broker = start_broker(&home, &["--tokens", tokens.to_str().expect("path")]);
    let host = Host { dir, broker };
    let home = host.home();
    let setup = host.run(&["setup"], &[]);
    assert_eq!(setup.status.code(), Some(0), "setup: {setup:?}");
    let own = home.join("tokens/w1");
    let spare = home.join("tokens/spare");
    let lead = home.join("tokens/lead");
    write_private(&lead, LEAD_TOKEN);
    write_private(&own, CLIENT_TOKEN);
    write_private(&spare, OTHER_TOKEN);
    write_private(
        &home.join(".config/chattrs/config.toml"),
        &format!(
            "server = \"{}\"\n[tokens]\n\"w1@box1\" = \"{}\"\n\"*@box2\" = \"{}\"\n\
             \"lead@box1\" = \"{}\"\n",
            host.broker.server,
            own.display(),
            spare.display(),
            lead.display()
        ),
    );
    std::fs::write(home.join(".chattrs.toml"), "project = \"alpha\"\n").expect("repo config");
    host
}

/// Every file under `dir` with its length and mtime.
fn snapshot(dir: &Path) -> BTreeMap<PathBuf, (u64, SystemTime)> {
    let mut files = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).expect("read dir").map(|e| e.expect("entry")) {
            let meta = entry.metadata().expect("metadata");
            if entry.file_name().to_string_lossy().starts_with("broker.") {
                continue; // the broker's own db and log change on a hello
            }
            if meta.is_dir() {
                pending.push(entry.path());
            } else {
                files.insert(entry.path(), (meta.len(), meta.modified().expect("mtime")));
            }
        }
    }
    files
}

fn line_with<'a>(stdout: &'a str, check: &str) -> &'a str {
    let prefix = format!("{check}:");
    stdout.lines().find(|line| line[5..].starts_with(&prefix)).unwrap_or("")
}

#[test]
fn doctor_passes_on_a_healthy_host_and_writes_nothing() {
    let host = healthy_host();
    let before = snapshot(host.home());
    let (code, stdout) = host.doctor(&[]);
    assert_eq!(code, 0, "{stdout}");
    assert!(!stdout.contains("FAIL"), "{stdout}");
    for check in
        ["config", "token-map", "token-pick", "broker", "token", "protocol", "hooks", "skill"]
    {
        assert!(line_with(&stdout, check).starts_with("ok"), "{check}: {stdout}");
    }
    assert!(stdout.contains("w1@box1: project alpha"), "{stdout}");
    assert!(stdout.contains("*@box2: not probed"), "{stdout}");
    assert!(stdout.contains("lead@box1: not probed"), "{stdout}");
    assert!(stdout.contains("Codex hook trust cannot be detected"), "{stdout}");
    assert_eq!(snapshot(host.home()), before, "doctor wrote a file");
}

#[test]
fn doctor_accepts_a_shared_token() {
    let dir = tempfile::TempDir::new().expect("make a temp home");
    let broker = start_broker(dir.path(), &["--token", "tok-secret-w1"]);
    let host = Host { dir, broker };
    let envs = [("CHATTRS_SERVER", host.broker.server.as_str()), ("CHATTRS_TOKEN", CLIENT_TOKEN)];
    let (_, stdout) = host.doctor(&envs);
    assert!(line_with(&stdout, "token").starts_with("ok"), "{stdout}");
    assert!(stdout.contains("CHATTRS_TOKEN, which overrides"), "{stdout}");
}

/// A broker log with no `rejected hello` line: doctor never trips SEC-5.
#[test]
fn doctor_logs_no_rejection_and_shows_the_broker_version() {
    let host = healthy_host();
    let (code, stdout) = host.doctor(&[("CHATTRS_SESSION", "w-sess")]);
    assert_eq!(code, 0, "{stdout}");
    let version = format!("ok   broker: {}", env!("CARGO_PKG_VERSION"));
    assert!(stdout.contains(&version), "{stdout}");
    assert!(!stdout.contains("CHATTRS_SESSION not set"), "{stdout}");
    assert_eq!(host.rejections(), Vec::<String>::new());
}

#[test]
fn doctor_beside_a_live_lead_session_and_a_worker_logs_no_rejection() {
    let host = healthy_host();
    let lead = [("CHATTRS_ADDRESS", "lead@box1"), ("CHATTRS_SESSION", "lead-sess")];
    let mut watch = common::command(binary());
    watch
        .arg("watch")
        .env("HOME", host.home())
        .env("CODEX_HOME", host.home().join(".codex"))
        .env_remove("XDG_CONFIG_HOME")
        .envs(lead)
        .current_dir(host.home())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let _watch = KillOnDrop(watch.spawn().expect("spawn watch"));
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let agents = host.run(&["agents"], &[]);
        if String::from_utf8_lossy(&agents.stdout).contains("lead@box1  online") {
            break;
        }
        assert!(Instant::now() < deadline, "the lead watch never came online: {agents:?}");
        std::thread::sleep(Duration::from_millis(100));
    }
    let (code, stdout) = host.doctor(&lead);
    assert_eq!(code, 0, "lead: {stdout}");
    assert!(stdout.contains("lead@box1: project alpha"), "{stdout}");
    let (code, stdout) = host.doctor(&[("CHATTRS_SESSION", "worker-sess")]);
    assert_eq!(code, 0, "worker: {stdout}");
    assert!(stdout.contains("w1@box1: project alpha"), "{stdout}");
    assert_eq!(host.rejections(), Vec::<String>::new());
}

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn doctor_without_an_address_checks_no_token_and_still_passes() {
    let host = healthy_host();
    let (code, stdout) = host.doctor(&[("CHATTRS_ADDRESS", "")]);
    assert_eq!(code, 0, "{stdout}");
    assert!(stdout.contains("ok   token: no current address: token not checked"), "{stdout}");
    assert!(stdout.contains("eval \"$(chattrs env --as <addr>)\""), "{stdout}");
    assert!(!stdout.contains("not probed"), "{stdout}");
    assert_eq!(host.rejections(), Vec::<String>::new());
}

#[test]
fn doctor_without_a_session_says_hello_anyway_and_adds_a_note() {
    let host = healthy_host();
    let (code, stdout) = host.doctor(&[]);
    assert_eq!(code, 0, "{stdout}");
    assert!(stdout.contains("note: CHATTRS_SESSION not set: use chattrs env"), "{stdout}");
    assert!(line_with(&stdout, "token").starts_with("ok"), "{stdout}");
}

#[test]
fn doctor_says_so_when_the_welcome_has_no_version() {
    let host = healthy_host();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind the fake broker");
    let server = listener.local_addr().expect("fake broker address").to_string();
    std::thread::spawn(move || {
        use std::io::{BufRead as _, BufReader, Write as _};
        for stream in listener.incoming().flatten() {
            let mut line = String::new();
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            if reader.read_line(&mut line).unwrap_or(0) > 0 {
                let welcome = "{\"type\":\"welcome\",\"server\":\"chattrs\",\"v\":1,\
                               \"address\":\"w1@box1\",\"project\":\"alpha\"}\n";
                let _ = (&stream).write_all(welcome.as_bytes());
            }
        }
    });
    let (code, stdout) = host.doctor(&[("CHATTRS_SERVER", &server)]);
    assert_eq!(code, 0, "{stdout}");
    assert!(
        stdout.contains("ok   broker: version not reported (older than 0.6); upgrade recommended"),
        "{stdout}"
    );
}

#[test]
fn doctor_fails_when_the_broker_is_unreachable() {
    let host = healthy_host();
    let dead = format!("127.0.0.1:{}", common::free_port());
    let (code, stdout) = host.doctor(&[("CHATTRS_SERVER", &dead)]);
    assert_eq!(code, 1, "{stdout}");
    assert!(line_with(&stdout, "broker").starts_with("FAIL"), "{stdout}");
    assert!(line_with(&stdout, "token").contains("broker unreachable"), "{stdout}");
}

#[test]
fn doctor_fails_on_a_wrong_token() {
    let host = healthy_host();
    write_private(&host.home().join("tokens/w1"), "tok-secret-wrong");
    let (code, stdout) = host.doctor(&[]);
    assert_eq!(code, 1, "{stdout}");
    assert!(line_with(&stdout, "token").starts_with("FAIL"), "{stdout}");
    assert!(stdout.contains("token is missing or incorrect"), "{stdout}");
}

#[test]
fn doctor_fails_on_an_outdated_skill() {
    let host = healthy_host();
    std::fs::write(host.home().join(".claude/skills/chattrs/SKILL.md"), "old\n").expect("age it");
    let (code, stdout) = host.doctor(&[]);
    assert_eq!(code, 1, "{stdout}");
    let line = line_with(&stdout, "skill");
    assert!(line.contains("FAIL") && line.contains("outdated"), "{stdout}");
    assert!(stdout.contains("ok   skill: codex"), "{stdout}");
}

#[test]
fn doctor_fails_on_a_token_file_wider_than_0600() {
    let host = healthy_host();
    set_mode(&host.home().join("tokens/spare"), 0o644);
    let (code, stdout) = host.doctor(&[]);
    assert_eq!(code, 1, "{stdout}");
    let line = line_with(&stdout, "token-map");
    assert!(line.starts_with("ok"), "own entry still passes: {stdout}");
    assert!(stdout.contains("FAIL token-map: *@box2"), "{stdout}");
    assert!(stdout.contains("mode 644"), "{stdout}");
}

#[test]
fn doctor_fails_when_the_hooks_are_missing() {
    let host = healthy_host();
    std::fs::remove_file(host.home().join(".claude/settings.json")).expect("remove it");
    let (code, stdout) = host.doctor(&[]);
    assert_eq!(code, 1, "{stdout}");
    assert!(stdout.contains("FAIL hooks: claude"), "{stdout}");
    assert!(stdout.contains("ok   hooks: codex"), "{stdout}");
}
