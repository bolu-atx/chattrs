//! Black-box CLI tests for `chattrs exec`.

#[path = "common/broker.rs"]
mod broker;
#[allow(dead_code)] // this file needs only `command`, not `free_port`
mod common;

use broker::*;
use std::process::{Output, Stdio};

/// Run `chattrs exec ARGS...` in `home`, with extra env pairs.
#[cfg(unix)]
fn run_exec(home: &std::path::Path, args: &[&str], envs: &[(&str, &str)]) -> Output {
    let mut command = common::command(binary());
    command.arg("exec").args(args);
    clean_home_env(&mut command, home);
    command.envs(envs.iter().copied());
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    command.output().expect("run chattrs exec")
}

/// The `CHATTRS_*` lines of `env` output, as a sorted map.
#[cfg(unix)]
fn chattrs_vars(text: &str) -> std::collections::BTreeMap<String, String> {
    text.lines()
        .filter(|line| line.starts_with("CHATTRS_"))
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

#[cfg(unix)]
#[test]
fn exec_child_env_matches_env_without_the_token() {
    let dir = tempfile::TempDir::new().expect("temp home");
    let home = dir.path();
    let token = home.join("tok");
    std::fs::write(&token, "secret\n").expect("write token");
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    }
    let config = home.join(".config/chattrs");
    std::fs::create_dir_all(&config).expect("config dir");
    std::fs::write(
        config.join("config.toml"),
        format!("[tokens]\n\"w1@h\" = \"{}\"\n", token.display()),
    )
    .expect("write config");
    let flags = ["--as", "w1@h", "--lead", "lead@h", "--tags", "gpu"];

    let mut env_cmd = common::command(binary());
    env_cmd.arg("env").args(flags);
    clean_home_env(&mut env_cmd, home);
    let env_out = env_cmd.output().expect("run chattrs env");
    assert_eq!(env_out.status.code(), Some(0), "env: {env_out:?}");
    let mut expected = parse_exports(&stdout_text(&env_out));
    let session_env = expected.remove("CHATTRS_SESSION").expect("env session");
    assert!(!expected.contains_key("CHATTRS_TOKEN"));

    let mut args = flags.to_vec();
    args.extend(["--", "env"]);
    let out = run_exec(home, &args, &[]);
    assert_eq!(out.status.code(), Some(0), "exec: {out:?}");
    let mut actual = chattrs_vars(&stdout_text(&out));
    let session_exec = actual.remove("CHATTRS_SESSION").expect("exec session");
    let expected: std::collections::BTreeMap<_, _> = expected.into_iter().collect();
    assert_eq!(actual, expected);
    assert_eq!(session_exec.len(), 32);
    assert_ne!(session_exec, session_env);
    assert!(!stdout_text(&out).contains("secret"), "the token must not reach the child");
}

#[cfg(unix)]
#[test]
fn exec_passes_an_explicit_token_through() {
    let dir = tempfile::TempDir::new().expect("temp home");
    let out = run_exec(
        dir.path(),
        &["--as", "w1@h", "--", "sh", "-c", "echo $CHATTRS_TOKEN"],
        &[("CHATTRS_TOKEN", "explicit")],
    );
    assert_eq!(stdout_text(&out).trim(), "explicit");
    assert!(!stderr_text(&out).contains("no token"));
}

#[cfg(unix)]
#[test]
fn exec_warns_when_no_token_is_found() {
    let dir = tempfile::TempDir::new().expect("temp home");
    let out = run_exec(dir.path(), &["--as", "w1@h", "sh", "-c", "echo ok"], &[]);
    assert_eq!(stdout_text(&out).trim(), "ok");
    assert!(stderr_text(&out).contains("chattrs: no token for w1@h; the broker may refuse"));
}

#[cfg(unix)]
#[test]
fn exec_default_address_is_the_program_basename_at_the_host() {
    let dir = tempfile::TempDir::new().expect("temp home");
    let out = run_exec(dir.path(), &["/bin/sh", "-c", "echo $CHATTRS_ADDRESS"], &[]);
    let host =
        gethostname::gethostname().to_string_lossy().split('.').next().unwrap().to_lowercase();
    assert_eq!(stdout_text(&out).trim(), format!("sh@{host}"));
}

#[cfg(unix)]
#[test]
fn exec_rejects_a_program_name_that_is_no_address_part() {
    let dir = tempfile::TempDir::new().expect("temp home");
    let out = run_exec(dir.path(), &["bad name!"], &[]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr_text(&out).contains("pass --as"), "{}", stderr_text(&out));
}

#[cfg(unix)]
#[test]
fn exec_passes_hyphen_args_and_the_exit_code_to_the_program() {
    let dir = tempfile::TempDir::new().expect("temp home");
    let out = run_exec(
        dir.path(),
        &["--as", "w1@h", "sh", "-c", "echo \"$1 $2\"; exit 7", "x", "--as", "evil@h"],
        &[],
    );
    assert_eq!(out.status.code(), Some(7));
    assert_eq!(stdout_text(&out).trim(), "--as evil@h");
}

/// A scratch dir with a fake `ssh` (logs its argv, one word per line, to
/// `ssh.log`; with `FAKE_SSH_RUN` set it runs the remote string under
/// `sh -c` with PATH set to `FAKE_REMOTE_PATH`) and a `$SHELL` stand-in
/// that skips the `-l`. Returns the dir.
#[cfg(unix)]
fn fake_ssh_dir(name: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = std::env::temp_dir().join(format!("chattrs-fake-ssh-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("bin")).unwrap();
    let script = |path: std::path::PathBuf, body: &str| {
        std::fs::write(&path, body).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    };
    script(
        dir.join("bin/ssh"),
        &format!(
            "#!/bin/sh\nfor a; do printf '%s\\n' \"$a\" >> '{log}'; done\n\
             [ -n \"$FAKE_SSH_RUN\" ] || exit 0\n\
             for last; do :; done\n\
             PATH=\"$FAKE_REMOTE_PATH\" SHELL='{shell}' exec /bin/sh -c \"$last\"\n",
            log = dir.join("ssh.log").display(),
            shell = dir.join("fakeshell").display(),
        ),
    );
    script(dir.join("fakeshell"), "#!/bin/sh\nexec /bin/sh -c \"$2\"\n");
    dir
}

/// Run `chattrs exec ARGS` with the fake ssh first in PATH.
#[cfg(unix)]
fn run_exec_ssh(dir: &std::path::Path, args: &[&str], envs: &[(&str, &str)]) -> Output {
    let path = format!("{}:/usr/bin:/bin", dir.join("bin").display());
    let mut all = vec![("PATH", path.as_str())];
    all.extend_from_slice(envs);
    run_exec(&std::env::temp_dir(), args, &all)
}

#[cfg(unix)]
#[test]
fn exec_host_runs_ssh_with_one_remote_string_and_no_secrets() {
    let dir = fake_ssh_dir("plain");
    let out = run_exec_ssh(
        &dir,
        &[
            "--host",
            "box",
            "--cwd",
            "/srv/r",
            "--as",
            "w1@box",
            "--lead",
            "lead@mac",
            "--project",
            "p",
            "--tags",
            "t",
            "--",
            "claude",
            "--resume",
            "a b",
        ],
        &[("CHATTRS_TOKEN", "sekrit"), ("CHATTRS_SERVER", "10.9.9.9:1")],
    );
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let log = std::fs::read_to_string(dir.join("ssh.log")).unwrap();
    let argv: Vec<&str> = log.lines().collect();
    assert_eq!(argv.len(), 3, "{log}");
    assert_eq!(&argv[..2], ["-t", "box"]);
    assert_eq!(
        argv[2],
        "exec \"$SHELL\" -lc 'cd '\\''/srv/r'\\'' && exec chattrs exec --as '\\''w1@box'\\'' --lead '\\''lead@mac'\\'' -- '\\''claude'\\'' '\\''--resume'\\'' '\\''a b'\\'''"
    );
    for secret in ["sekrit", "CHATTRS_", "10.9.9.9", "--project", "--tags", "--server"] {
        assert!(!log.contains(secret), "{secret} leaked: {log}");
    }
}

#[cfg(unix)]
#[test]
fn exec_host_flags_need_their_parents() {
    let dir = fake_ssh_dir("clap");
    for args in [
        &["--cwd", "/x", "claude"][..],
        &["--detach", "claude"],
        &["--host", "b", "--session-name", "n", "claude"],
    ] {
        let out = run_exec_ssh(&dir, args, &[]);
        assert_eq!(out.status.code(), Some(2), "{args:?}: {out:?}");
        assert!(String::from_utf8_lossy(&out.stderr).contains("required arguments"), "{out:?}");
    }
}

/// Run a detached start whose fake ssh executes the remote string.
/// `with_tmux`: a fake tmux that logs its args; `exists`: it reports a session.
#[cfg(unix)]
fn run_detached(name: &str, with_tmux: bool, exists: bool) -> (Output, std::path::PathBuf) {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = fake_ssh_dir(name);
    let remote_bin = dir.join("remote-bin");
    std::fs::create_dir_all(&remote_bin).unwrap();
    if with_tmux {
        let tmux = remote_bin.join("tmux");
        let body = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\n[ \"$1\" = has-session ] && exit {}\nexit 0\n",
            dir.join("tmux.log").display(),
            if exists { 0 } else { 1 },
        );
        std::fs::write(&tmux, body).unwrap();
        std::fs::set_permissions(&tmux, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let remote_path = remote_bin.display().to_string();
    let out = run_exec_ssh(
        &dir,
        &["--host", "box", "--cwd", "/", "--detach", "--as", "my.w1@box", "claude", "go now"],
        &[("FAKE_SSH_RUN", "1"), ("FAKE_REMOTE_PATH", &remote_path)],
    );
    (out, dir)
}

#[cfg(unix)]
#[test]
fn exec_detach_without_tmux_exits_1_with_a_hint() {
    let (out, _) = run_detached("notmux", false, false);
    assert_eq!(out.status.code(), Some(1), "{out:?}");
    assert!(String::from_utf8_lossy(&out.stderr).contains("install tmux, or drop --detach"));
    assert!(!String::from_utf8_lossy(&out.stdout).contains("started"));
}

#[cfg(unix)]
#[test]
fn exec_detach_starts_a_tmux_session_and_prints_hints() {
    let (out, dir) = run_detached("ok", true, false);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("started my-w1 on box"), "{text}");
    assert!(text.contains("attach: ssh -t box tmux attach -t =my-w1"), "{text}");
    assert!(text.contains("stop: ssh box tmux kill-session -t =my-w1"), "{text}");
    let log = std::fs::read_to_string(dir.join("tmux.log")).unwrap();
    assert!(log.contains("has-session -t =my-w1"), "{log}");
    assert!(
        log.contains(
            "new-session -d -s my-w1 exec chattrs exec --as 'my.w1@box' -- 'claude' 'go now'"
        ),
        "{log}"
    );
    let ssh = std::fs::read_to_string(dir.join("ssh.log")).unwrap();
    assert_eq!(ssh.lines().next(), Some("box"), "detach uses no -t: {ssh}");
}

#[cfg(unix)]
#[test]
fn exec_detach_refuses_an_existing_session() {
    let (out, dir) = run_detached("exists", true, true);
    assert_eq!(out.status.code(), Some(1), "{out:?}");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("session my-w1 already exists"), "{err}");
    assert!(err.contains("ssh -t box tmux attach -t =my-w1"), "{err}");
    let log = std::fs::read_to_string(dir.join("tmux.log")).unwrap();
    assert!(!log.contains("new-session") && !log.contains("kill"), "{log}");
}
