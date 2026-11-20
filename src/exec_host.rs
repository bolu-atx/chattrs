//! `chattrs exec --host`: build the `ssh` command line that starts
//! `chattrs exec` on another host. Pure functions, so tests can check the
//! exact argv without ssh. The token, server, project, tags, and every
//! `CHATTRS_*` variable stay local: the remote host reads its own config.

/// Quote `s` for a POSIX shell: single quotes, each `'` written as `'\''`.
pub(crate) fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// The default tmux session name: the user part of the address, else the
/// program basename, with each char outside `[A-Za-z0-9_-]` turned into `-`.
pub(crate) fn session_name(address: Option<&str>, program: &str) -> String {
    let base = std::path::Path::new(program).file_name().and_then(|n| n.to_str()).unwrap_or("");
    let user = address.map(|a| a.split('@').next().unwrap_or(a)).unwrap_or(base);
    let name: String = user
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '-' })
        .collect();
    if name.is_empty() {
        "chattrs".to_string()
    } else {
        name
    }
}

/// `exec chattrs exec [--as A] [--lead L] -- PROGRAM ARGS...`, quoted.
fn exec_line(address: Option<&str>, lead: Option<&str>, command: &[String]) -> String {
    let mut line = String::from("exec chattrs exec");
    for (flag, value) in [("--as", address), ("--lead", lead)] {
        if let Some(value) = value {
            line.push_str(&format!(" {flag} {}", sh_quote(value)));
        }
    }
    line.push_str(" --");
    for word in command {
        line.push(' ');
        line.push_str(&sh_quote(word));
    }
    line
}

/// The script for a detached start, run by the remote login shell.
fn detach_script(host: &str, cwd: Option<&str>, name: &str, exec: &str) -> String {
    let say = |text: String| format!("echo {} >&2; exit 1", sh_quote(&text));
    let mut script = String::new();
    if let Some(cwd) = cwd {
        script.push_str(&format!("cd {} || exit 1; ", sh_quote(cwd)));
    }
    script.push_str(&format!(
        "command -v tmux >/dev/null 2>&1 || {{ {}; }}; ",
        say(format!("chattrs: tmux not found on {host}; install tmux, or drop --detach"))
    ));
    script.push_str(&format!(
        "tmux has-session -t {} 2>/dev/null && {{ {}; }}; ",
        sh_quote(name),
        say(format!(
            "chattrs: tmux session {name} already exists on {host}; attach with: ssh -t {host} tmux attach -t {name}, or pick another --session-name"
        ))
    ));
    script.push_str(&format!("tmux new-session -d -s {} {}", sh_quote(name), sh_quote(exec)));
    script
}

/// The ssh argv (program name first). The remote part is ONE string for
/// the remote shell: `exec "$SHELL" -lc '<script>'`. Without `detach_name`
/// ssh gets `-t`, so the agent has a terminal. A detached start needs none.
pub(crate) fn remote_command(
    host: &str,
    cwd: Option<&str>,
    address: Option<&str>,
    lead: Option<&str>,
    command: &[String],
    detach_name: Option<&str>,
) -> Vec<String> {
    let exec = exec_line(address, lead, command);
    let script = match detach_name {
        Some(name) => detach_script(host, cwd, name, &exec),
        None => match cwd {
            Some(cwd) => format!("cd {} && {exec}", sh_quote(cwd)),
            None => exec,
        },
    };
    let remote = format!("exec \"$SHELL\" -lc {}", sh_quote(&script));
    let mut argv = vec!["ssh".to_string()];
    if detach_name.is_none() {
        argv.push("-t".to_string());
    }
    argv.push(host.to_string());
    argv.push(remote);
    argv
}

/// The lines printed after a detached start succeeds.
pub(crate) fn detach_hints(host: &str, name: &str) -> String {
    format!(
        "started {name} on {host}\nattach: ssh -t {host} tmux attach -t {name}\nstop: ssh {host} tmux kill-session -t {name}\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn sh_quote_covers_the_shell_specials() {
        assert_eq!(sh_quote(""), "''");
        assert_eq!(sh_quote("a b"), "'a b'");
        assert_eq!(sh_quote("it's"), r"'it'\''s'");
        assert_eq!(sh_quote("say \"hi\""), "'say \"hi\"'");
        assert_eq!(sh_quote("$HOME;`id`"), "'$HOME;`id`'");
        assert_eq!(sh_quote("a\nb"), "'a\nb'");
    }

    #[test]
    fn session_name_is_the_sanitized_user_part() {
        assert_eq!(session_name(Some("w1@box"), "claude"), "w1");
        assert_eq!(session_name(Some("my.agent@box"), "claude"), "my-agent");
        assert_eq!(session_name(None, "/usr/bin/codex"), "codex");
        assert_eq!(session_name(None, "a b"), "a-b");
    }

    #[test]
    fn plain_argv_shape() {
        let argv = remote_command(
            "box",
            Some("/srv/my repo"),
            Some("w1@box"),
            Some("lead@mac"),
            &words(&["claude", "--resume", "it's"]),
            None,
        );
        assert_eq!(argv[..3], ["ssh", "-t", "box"]);
        assert_eq!(argv.len(), 4);
        let inner = "cd '/srv/my repo' && exec chattrs exec --as 'w1@box' --lead 'lead@mac' -- 'claude' '--resume' 'it'\\''s'";
        assert_eq!(argv[3], format!("exec \"$SHELL\" -lc {}", sh_quote(inner)));
    }

    #[test]
    fn plain_argv_without_cwd_or_flags() {
        let argv = remote_command("box", None, None, None, &words(&["codex"]), None);
        assert_eq!(argv[3], "exec \"$SHELL\" -lc 'exec chattrs exec -- '\\''codex'\\'''");
    }

    #[test]
    fn detach_argv_shape() {
        let argv = remote_command(
            "box",
            Some("/w"),
            Some("w1@box"),
            None,
            &words(&["claude"]),
            Some("w1"),
        );
        assert_eq!(argv[..2], ["ssh", "box"]);
        assert_eq!(argv.len(), 3);
        let remote = &argv[2];
        assert!(remote.starts_with("exec \"$SHELL\" -lc "), "{remote}");
        assert!(remote.contains("install tmux, or drop --detach"), "{remote}");
        assert!(remote.contains("tmux new-session -d -s "), "{remote}");
    }

    /// Run `remote` under `sh -c`. `$SHELL` is a script that drops the `-l`
    /// (a login shell may reset PATH) and runs the script with /bin/sh.
    #[cfg(unix)]
    fn run_remote(remote: &str, path: &str) -> std::process::Output {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = std::env::temp_dir().join(format!("chattrs-exec-host-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let shell = dir.join("fakeshell");
        std::fs::write(&shell, "#!/bin/sh\nexec /bin/sh -c \"$2\"\n").unwrap();
        std::fs::set_permissions(&shell, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(remote)
            .env("SHELL", &shell)
            .env("PATH", path)
            .output()
            .unwrap()
    }

    #[cfg(unix)]
    #[test]
    fn remote_string_delivers_args_intact() {
        use std::os::unix::fs::PermissionsExt as _;
        let bin = std::env::temp_dir().join(format!("chattrs-exec-bin-{}", std::process::id()));
        std::fs::create_dir_all(&bin).unwrap();
        let fake = bin.join("chattrs");
        std::fs::write(&fake, "#!/bin/sh\nfor a; do printf '[%s]\\n' \"$a\"; done\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let tricky =
            words(&["prog", "a b", "it's", "say \"hi\"", "$HOME", "x;y", "`id`", "l1\nl2", ""]);
        let argv = remote_command("h", Some("/"), Some("w 1@h"), Some("l'd@h"), &tricky, None);
        let out = run_remote(&argv[3], &format!("{}:/usr/bin:/bin", bin.display()));
        let text = String::from_utf8_lossy(&out.stdout);
        let expected = "[exec]\n[--as]\n[w 1@h]\n[--lead]\n[l'd@h]\n[--]\n[prog]\n[a b]\n[it's]\n[say \"hi\"]\n[$HOME]\n[x;y]\n[`id`]\n[l1\nl2]\n[]\n";
        assert_eq!(text, expected, "{out:?}");
    }
}
