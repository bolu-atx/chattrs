//! Black-box tests for `chattrs setup-guide` (SETUP-5): a scratch home, the
//! built binary as a child, and one isolated broker for the no-hello test.

mod common;

use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

const SECRET: &str = "tok-guide-secret-1";
const SECRET_JOIN: &str = "tok-guide-secret-2";

fn binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_chattrs"))
}

/// A scratch home with both agent directories.
fn home() -> tempfile::TempDir {
    let dir = tempfile::TempDir::new().expect("temp home");
    std::fs::create_dir_all(dir.path().join(".claude")).expect("claude dir");
    std::fs::create_dir_all(dir.path().join(".codex")).expect("codex dir");
    dir
}

fn run(home: &Path, args: &[&str]) -> Output {
    let mut command = common::command(binary());
    command
        .args(args)
        .env("HOME", home)
        .env("CODEX_HOME", home.join(".codex"))
        .env_remove("XDG_CONFIG_HOME")
        .env("PATH", "")
        .current_dir(home)
        .stdin(Stdio::null());
    command.output().expect("run chattrs")
}

/// Run the guide. It must exit 0 and print no secret.
fn guide(home: &Path, args: &[&str]) -> (String, String) {
    let mut all = vec!["setup-guide"];
    all.extend_from_slice(args);
    let output = run(home, &all);
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_eq!(output.status.code(), Some(0), "{stdout}{stderr}");
    for secret in [SECRET, SECRET_JOIN] {
        assert!(!stdout.contains(secret) && !stderr.contains(secret), "{stdout}{stderr}");
    }
    (stdout, stderr)
}

fn json(home: &Path, args: &[&str]) -> Value {
    let mut all = args.to_vec();
    all.push("--json");
    serde_json::from_str(&guide(home, &all).0).expect("valid json")
}

fn steps(doc: &Value) -> Vec<(String, String, String)> {
    doc["steps"]
        .as_array()
        .expect("steps")
        .iter()
        .map(|step| {
            let text = |key: &str| step[key].as_str().expect(key).to_string();
            (text("id"), text("status"), text("who"))
        })
        .collect()
}

fn ids(doc: &Value) -> Vec<String> {
    steps(doc).into_iter().map(|(id, _, _)| id).collect()
}

fn status_of(doc: &Value, id: &str) -> String {
    steps(doc).into_iter().find(|(step, _, _)| step == id).expect("step exists").1
}

fn position(ids: &[String], id: &str) -> usize {
    ids.iter().position(|step| step == id).unwrap_or_else(|| panic!("no step {id}: {ids:?}"))
}

fn write_private(path: &Path, text: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(path.parent().expect("parent")).expect("make dir");
    std::fs::write(path, text).expect("write");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
}

#[test]
fn solo_is_the_guess_and_steps_flip_after_setup() {
    let dir = home();
    let before = json(dir.path(), &[]);
    assert_eq!(before["role"]["name"], "solo");
    assert_eq!(before["role"]["guessed"], true);
    assert_eq!(
        ids(&before),
        ["setup", "claude-review", "codex-trust", "doctor", "launch-claude", "launch-codex"]
    );
    assert_eq!(status_of(&before, "setup"), "todo");
    assert_eq!(status_of(&before, "doctor"), "blocked");
    let setup = run(dir.path(), &["setup"]);
    assert_eq!(setup.status.code(), Some(0), "{setup:?}");
    assert!(String::from_utf8_lossy(&setup.stderr).contains("next: chattrs setup-guide"));
    let after = json(dir.path(), &[]);
    assert_eq!(status_of(&after, "setup"), "done");
    assert_eq!(status_of(&after, "doctor"), "todo");
    assert_eq!(status_of(&after, "codex-trust"), "todo");
}

#[test]
fn solo_guide_says_never_to_run_serve_by_hand() {
    let dir = home();
    let (stdout, _) = guide(dir.path(), &["--role", "solo"]);
    assert!(stdout.contains("never run `chattrs serve` by hand"), "{stdout}");
}

#[test]
fn text_output_has_one_block_per_step_and_the_rerun_line() {
    let dir = home();
    let (stdout, _) = guide(dir.path(), &["--hosts", "mac,box", "--broker-host", "box"]);
    assert!(
        stdout.contains("role: host (guessed: --hosts or --broker-host was given)"),
        "{stdout}"
    );
    assert!(stdout.contains("--role solo|broker|host"), "{stdout}");
    assert!(stdout.contains("run: ssh -o BatchMode=yes mac true"), "{stdout}");
    assert!(
        stdout.contains("run `chattrs setup-guide --hosts mac,box --broker-host box`"),
        "{stdout}"
    );
}

#[test]
fn the_role_guess_follows_the_files_and_the_flag_overrides_it() {
    let dir = home();
    let home = dir.path();
    let how = |args: &[&str]| {
        let doc = json(home, args);
        (doc["role"]["name"].as_str().expect("name").to_string(), doc)
    };
    assert_eq!(how(&[]).0, "solo");
    write_private(&home.join(".config/chattrs/config.toml"), "server = \"100.64.0.9:4646\"\n");
    let (name, doc) = how(&[]);
    assert_eq!(name, "host");
    assert!(doc["role"]["how"].as_str().expect("how").contains("100.64.0.9"), "{doc}");
    write_private(&home.join(".config/chattrs/tokens.toml"), "");
    let (name, doc) = how(&[]);
    assert_eq!(name, "broker");
    assert!(doc["role"]["how"].as_str().expect("how").contains("tokens.toml"), "{doc}");
    let (name, doc) = how(&["--role", "solo"]);
    assert_eq!(name, "solo");
    assert_eq!(doc["role"]["guessed"], false);
    assert_eq!(doc["role"]["how"], "set by --role");
}

#[test]
fn swarm_asks_come_first_and_alone() {
    let dir = home();
    let nothing = json(dir.path(), &["--role", "host"]);
    assert_eq!(ids(&nothing), ["ask-hosts", "ask-broker-host"]);
    assert!(steps(&nothing).iter().all(|(_, status, who)| status == "todo" && who == "ask"));
    let hosts_only = json(dir.path(), &["--role", "host", "--hosts", "mac,box"]);
    assert_eq!(ids(&hosts_only), ["ask-broker-host"]);
    let broker_only = json(dir.path(), &["--role", "host", "--broker-host", "box"]);
    assert_eq!(ids(&broker_only), ["ask-hosts"]);
}

#[test]
fn swarm_lists_the_per_host_steps_in_order() {
    let dir = home();
    let doc = json(dir.path(), &["--hosts", "mac,box", "--broker-host", "box"]);
    let ids = ids(&doc);
    for host in ["mac", "box"] {
        let order = ["ssh", "version", "ask-install", "install", "setup"]
            .map(|prefix| position(&ids, &format!("{prefix}-{host}")));
        assert!(order.windows(2).all(|pair| pair[0] < pair[1]), "{ids:?}");
        assert!(position(&ids, &format!("setup-{host}")) < position(&ids, "broker"));
        assert!(position(&ids, "broker") < position(&ids, &format!("join-{host}")));
        assert!(
            position(&ids, &format!("join-{host}")) < position(&ids, &format!("doctor-{host}"))
        );
        assert!(
            position(&ids, &format!("doctor-{host}")) < position(&ids, &format!("trust-{host}"))
        );
    }
    let command = |id: &str| {
        let step = doc["steps"].as_array().expect("steps").iter().find(|s| s["id"] == id);
        step.expect("step")["command"].as_str().expect("command").to_string()
    };
    assert_eq!(command("ssh-mac"), "ssh -o BatchMode=yes mac true");
    assert_eq!(command("version-box"), "ssh box chattrs --version");
    assert!(command("install-mac").contains("chattrs-installer.sh"), "{}", command("install-mac"));
    assert!(command("install-mac").contains("/v0."), "pinned to this version");
    assert!(!command("install-mac").contains("sudo"));
    assert!(
        command("broker").contains("--hosts mac,box --join-dir ~/.config/chattrs/joins --enable")
    );
    assert!(
        command("join-mac").ends_with("ssh mac chattrs setup --join -"),
        "{}",
        command("join-mac")
    );
    assert!(command("join-mac").contains("joins/lead.join"), "the first host also joins as lead");
    assert_eq!(command("doctor-box"), "ssh box chattrs doctor --as claude@box");
    // Every remote step is todo: the guide cannot see remote state.
    assert!(doc["steps"].as_array().expect("steps").iter().all(|s| s["status"] == "todo"));
}

#[test]
fn a_local_broker_feeds_the_join_over_stdin() {
    let dir = home();
    let here = {
        let output = common::command("hostname").output().expect("hostname");
        String::from_utf8_lossy(&output.stdout).trim().split('.').next().unwrap().to_lowercase()
    };
    let doc = json(dir.path(), &["--hosts", "mac,box", "--broker-host", &here]);
    let step = doc["steps"].as_array().expect("steps").iter().find(|s| s["id"] == "join-box");
    let command = step.expect("step")["command"].as_str().expect("command");
    assert_eq!(command, "ssh box chattrs setup --join - < ~/.config/chattrs/joins/box.join");
    assert_eq!(doc["role"]["name"], "broker");
}

#[test]
fn no_token_or_join_string_reaches_any_output() {
    let dir = home();
    let home = dir.path();
    write_private(&home.join("tokens/me"), SECRET);
    write_private(
        &home.join(".config/chattrs/config.toml"),
        &format!(
            "server = \"100.64.0.9:4646\"\n[tokens]\n\"*@x\" = \"{}\"\n",
            home.join("tokens/me").display()
        ),
    );
    write_private(
        &home.join(".config/chattrs/tokens.toml"),
        &format!("[[token]]\ntoken = \"{SECRET}\"\naddr = \"*@x\"\nrole = \"agent\"\n"),
    );
    write_private(
        &home.join(".config/chattrs/joins/box.join"),
        &format!("chattrs://{SECRET_JOIN}@100.64.0.9:4646?as=%2A%40box\n"),
    );
    for args in [&[][..], &["--json"], &["--hosts", "box", "--broker-host", "box"]] {
        let (stdout, stderr) = guide(home, args);
        assert!(stdout.contains("chattrs"), "{stdout}{stderr}");
    }
}

#[test]
fn json_is_byte_stable_and_sorted() {
    let dir = home();
    let args = ["--hosts", "mac,box", "--broker-host", "box"];
    let first = guide(dir.path(), &[&args[..], &["--json"]].concat()).0;
    let second = guide(dir.path(), &[&args[..], &["--json"]].concat()).0;
    assert_eq!(first, second);
    let doc: Value = serde_json::from_str(&first).expect("json");
    let keys = |value: &Value| -> Vec<String> {
        value.as_object().expect("object").keys().cloned().collect()
    };
    assert_eq!(keys(&doc), ["rerun", "role", "steps", "version"]);
    assert_eq!(keys(&doc["role"]), ["guessed", "how", "name", "override_with"]);
    assert_eq!(keys(&doc["steps"][0]), ["command", "id", "status", "who", "why"]);
    assert_eq!(doc["rerun"], "chattrs setup-guide --hosts mac,box --broker-host box --json");
}

struct Broker {
    child: Child,
    err: PathBuf,
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_broker(dir: &Path, tokens: &Path) -> (Broker, String) {
    let server = format!("127.0.0.1:{}", common::free_port());
    let err = dir.join("broker.err");
    let child = common::command(binary())
        .args(["serve", "--listen", &server, "--db"])
        .arg(dir.join("broker.db"))
        .arg("--tokens")
        .arg(tokens)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&err).expect("stderr file"))
        .spawn()
        .expect("spawn the broker");
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(&server).is_err() {
        assert!(Instant::now() < deadline, "broker never accepted on {server}");
        std::thread::sleep(Duration::from_millis(50));
    }
    (Broker { child, err }, server)
}

#[test]
fn the_guide_never_says_hello_so_the_broker_logs_no_rejection() {
    let dir = home();
    let home = dir.path();
    // The broker does not know the fixture token, so any hello would be rejected.
    let tokens = home.join("broker-tokens.toml");
    write_private(
        &tokens,
        "[[token]]\ntoken = \"some-other-token\"\naddr = \"*@elsewhere\"\nrole = \"agent\"\n",
    );
    let (broker, server) = start_broker(home, &tokens);
    write_private(&home.join("tokens/me"), SECRET);
    write_private(
        &home.join(".config/chattrs/config.toml"),
        &format!("server = \"{server}\"\ntoken_file = \"{}\"\n", home.join("tokens/me").display()),
    );
    let setup = run(home, &["setup"]);
    assert_eq!(setup.status.code(), Some(0));
    let mut command = common::command(binary());
    command
        .args(["setup-guide", "--role", "solo"])
        .env("HOME", home)
        .env("CODEX_HOME", home.join(".codex"))
        .env("PATH", "")
        .env("CHATTRS_ADDRESS", "w1@box1")
        .env("CHATTRS_TOKEN", SECRET)
        .current_dir(home)
        .stdin(Stdio::null());
    let output = command.output().expect("run the guide");
    assert_eq!(output.status.code(), Some(0));
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(!text.contains(SECRET), "{text}");
    std::thread::sleep(Duration::from_millis(300));
    let log = std::fs::read_to_string(&broker.err).expect("broker log");
    assert!(!log.contains("rejected hello"), "{log}");
}
