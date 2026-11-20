//! Black-box CLI tests for config, tokens, and `chattrs env`.

#[path = "common/broker.rs"]
mod broker;
#[allow(dead_code)] // this file needs only `command`, not `free_port`
mod common;

use broker::*;
use std::path::PathBuf;
use std::process::{Output, Stdio};

/// Run `chattrs <args>` with a clean env, a temp HOME, and a temp cwd.
fn run_in(
    home: &std::path::Path,
    cwd: &std::path::Path,
    envs: &[(&str, &str)],
    args: &[&str],
) -> Output {
    let mut command = common::command(binary());
    command.args(args).current_dir(cwd);
    command.env("HOME", home).envs(envs.iter().copied());
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    command.output().expect("run chattrs")
}

fn write_private(path: &std::path::Path, text: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn user_config_token_file_reaches_a_token_broker() {
    let broker = Broker::start_with(&["--token", "sekret"]);
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    write_private(&home.join("tok"), "sekret\n");
    write_private(
        &home.join(".config/chattrs/config.toml"),
        &format!("server = \"{}\"\ntoken_file = \"{}\"\n", broker.addr, home.join("tok").display()),
    );
    let envs = [("CHATTRS_ADDRESS", "a@h")];
    let ok = run_in(home, home, &envs, &["send", "--to", "b@h", "hi"]);
    assert_eq!(ok.status.code(), Some(0), "{}", stderr_text(&ok));
    // An explicit token beats the file and is refused.
    let bad = run_in(home, home, &envs, &["send", "--token", "wrong", "--to", "b@h", "hi"]);
    assert_eq!(bad.status.code(), Some(1));
    // The env beats the file as well.
    let bad = run_in(
        home,
        home,
        &[("CHATTRS_TOKEN", "wrong"), ("CHATTRS_ADDRESS", "a@h")],
        &["send", "--to", "b@h", "hi"],
    );
    assert_eq!(bad.status.code(), Some(1));
}

#[test]
fn repo_file_gives_the_project_and_loses_to_env_and_flag() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    std::fs::create_dir(cwd.join(".git")).unwrap();
    std::fs::write(cwd.join(".chattrs.toml"), "project = \"repo\"\nlead = \"l@h\"\n").unwrap();
    let env_out = |envs: &[(&str, &str)], extra: &[&str]| {
        let mut args = vec!["env", "--as", "w@h"];
        args.extend(extra);
        stdout_text(&run_in(cwd, cwd, envs, &args))
    };
    let out = env_out(&[], &[]);
    assert!(out.contains("export CHATTRS_PROJECT='repo'\n"), "{out}");
    assert!(out.contains("export CHATTRS_LEAD='l@h'\n"), "{out}");
    let out = env_out(&[("CHATTRS_PROJECT", "fromenv")], &[]);
    assert!(out.contains("CHATTRS_PROJECT='fromenv'"), "{out}");
    let out = env_out(&[("CHATTRS_PROJECT", "fromenv")], &["--project", "flag"]);
    assert!(out.contains("CHATTRS_PROJECT='flag'"), "{out}");
}

#[test]
fn a_secret_in_the_repo_file_stops_the_command() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join(".git")).unwrap();
    std::fs::write(dir.path().join(".chattrs.toml"), "token = \"x\"\n").unwrap();
    let out = run_in(dir.path(), dir.path(), &[], &["env", "--as", "w@h"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr_text(&out).contains(".chattrs.toml:1"), "{}", stderr_text(&out));
}

#[test]
fn env_output_evals_in_sh_with_a_fresh_session_and_no_token() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    write_private(&cwd.join("tok"), "sekret\n");
    write_private(
        &cwd.join(".config/chattrs/config.toml"),
        &format!("token_file = \"{}\"\n", cwd.join("tok").display()),
    );
    let hostile = [("CHATTRS_TAGS", "it's $(touch pwned) \\\"x")];
    let first = stdout_text(&run_in(cwd, cwd, &hostile, &["env", "--as", "w@h"]));
    let second = stdout_text(&run_in(cwd, cwd, &hostile, &["env", "--as", "w@h"]));
    assert!(!first.contains("sekret") && !first.contains("CHATTRS_TOKEN"), "{first}");
    let session = |text: &str| {
        text.lines().find(|l| l.starts_with("export CHATTRS_SESSION=")).unwrap().to_string()
    };
    assert_ne!(session(&first), session(&second));
    let script = format!("eval '{}'; printf '%s|%s|%s' \"$CHATTRS_ADDRESS\" \"$CHATTRS_TAGS\" \"${{CHATTRS_TOKEN-unset}}\"", first.replace('\'', "'\\''"));
    let sh = common::command("sh").args(["-c", &script]).current_dir(cwd).output().unwrap();
    assert_eq!(stdout_text(&sh), "w@h|it's $(touch pwned) \\\"x|unset");
    assert!(!cwd.join("pwned").exists());
    let with = stdout_text(&run_in(cwd, cwd, &[], &["env", "--as", "w@h", "--with-token"]));
    assert!(with.contains("export CHATTRS_TOKEN='sekret'\n"), "{with}");
}
#[test]
fn env_with_token_picks_the_token_for_the_address() {
    let dir = tempfile::TempDir::new().expect("make a temp home");
    let home = dir.path().to_path_buf();
    let write_token = |name: &str| -> PathBuf {
        let path = home.join(name);
        std::fs::write(&path, format!("tok-{name}\n")).expect("write a token");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        }
        path
    };
    let (plain, lead, host) = (write_token("plain"), write_token("lead"), write_token("host"));
    let config_dir = home.join(".config/chattrs");
    std::fs::create_dir_all(&config_dir).expect("config dir");
    std::fs::write(
        config_dir.join("config.toml"),
        format!(
            "token_file = \"{}\"\n[tokens]\n\"*@mac\" = \"{}\"\n\"lead@mac\" = \"{}\"\n",
            plain.display(),
            host.display(),
            lead.display()
        ),
    )
    .expect("write the config");
    let token_for = |address: &str, token_env: Option<&str>| -> Option<String> {
        let mut command = common::command(binary());
        command.args(["env", "--as", address, "--with-token"]);
        clean_home_env(&mut command, &home);
        if let Some(token) = token_env {
            command.env("CHATTRS_TOKEN", token);
        }
        command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
        let output = command.output().expect("run chattrs env");
        assert_eq!(output.status.code(), Some(0), "env: {output:?}");
        parse_exports(&stdout_text(&output)).remove("CHATTRS_TOKEN")
    };
    assert_eq!(token_for("lead@mac", None).as_deref(), Some("tok-lead"));
    assert_eq!(token_for("w1@mac", None).as_deref(), Some("tok-host"));
    assert_eq!(token_for("w1@box", None).as_deref(), Some("tok-plain"));
    assert_eq!(token_for("lead@mac", Some("explicit")).as_deref(), Some("explicit"));
}
#[test]
fn env_says_where_the_project_came_from() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    let env_out =
        |envs: &[(&str, &str)]| stdout_text(&run_in(cwd, cwd, envs, &["env", "--as", "w@h"]));
    let out = env_out(&[]);
    assert!(out.contains("# project source: default\n"), "{out}");
    assert!(!out.contains("CHATTRS_PROJECT"), "{out}");
    let out = env_out(&[("CHATTRS_PROJECT", "fromenv")]);
    assert!(out.contains("# project source: env\n"), "{out}");
    std::fs::write(cwd.join(".chattrs.toml"), "project = \"repo\"\n").unwrap();
    let out = env_out(&[]);
    assert!(out.contains("# project source: config\n"), "{out}");
}

#[test]
fn env_derives_the_project_from_a_repo() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path().join("My_Repo");
    std::fs::create_dir(&cwd).unwrap();
    let status = std::process::Command::new("git")
        .args(["init", "-q"])
        .arg(&cwd)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("GIT_DIR")
        .status()
        .unwrap();
    assert!(status.success());
    let out = stdout_text(&run_in(dir.path(), &cwd, &[], &["env", "--as", "w@h"]));
    assert!(out.contains("export CHATTRS_PROJECT='my-repo'\n"), "{out}");
    assert!(out.contains("# project source: git\n"), "{out}");
}

#[test]
fn a_blank_lead_variable_falls_through_to_the_repo_config() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(".chattrs.toml"), "lead = \"cfg@h\"\n").unwrap();
    for blank in ["", " "] {
        let out = common::command(binary())
            .current_dir(dir.path())
            .env("CHATTRS_LEAD", blank)
            .args(["env", "--as", "w1@h"])
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&out.stdout);
        assert!(text.contains("CHATTRS_LEAD='cfg@h'"), "[{blank}]: {text}");
    }
}
