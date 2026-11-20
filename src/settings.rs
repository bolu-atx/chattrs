//! chattrs — the settings of one run: the environment merged with the
//! config files. One pure resolver, no process-environment writes.
//!
//! Precedence, high to low: a flag, the real environment variable, the
//! config file, the built-in default. Clap reads the flags and the real
//! variables itself, so `Settings` holds the part below them (variable,
//! file, default) and `apply` fills only the values that clap left unset.

use std::path::Path;

use clap::parser::ValueSource;
use clap::ArgMatches;

use crate::cli::{Args, ClientArgs, Command, DEFAULT_SERVER};
use crate::config::Config;
use crate::project::{self, ProjectSource};

/// Where the token came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenSource {
    /// `CHATTRS_TOKEN`, set even when empty.
    Env,
    /// A token file: the `[tokens]` entry that fits the address, else `token_file`.
    Config,
    /// No token anywhere.
    Unset,
}

/// The real environment: a variable that is set and valid Unicode.
pub fn process_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// The merged settings below the flags.
#[derive(Debug, PartialEq, Eq)]
pub struct Settings {
    /// `CHATTRS_SERVER`, then the config `server`, then the default.
    pub server: String,
    /// `CHATTRS_TAGS`, then the config `tags`.
    pub tags: Option<String>,
    /// `CHATTRS_TOKEN`, then the config token. The config token is the one
    /// `Config::select_token` chose.
    pub token: Option<String>,
    pub token_source: TokenSource,
    /// `CHATTRS_LEAD`, then the config `lead`. A set variable wins even when blank.
    pub lead: Option<String>,
    project_env: Option<String>,
    project_config: Option<String>,
}

impl Settings {
    /// Merge the variables `env` reports with the config `files`. A variable
    /// that is set, even empty, blocks the config value.
    pub fn resolve(env: &dyn Fn(&str) -> Option<String>, files: &Config) -> Settings {
        let pick = |name: &str, file: &Option<String>| env(name).or_else(|| file.clone());
        let (token, token_source) = match (env("CHATTRS_TOKEN"), &files.token) {
            (Some(token), _) => (Some(token), TokenSource::Env),
            (None, Some(token)) => (Some(token.clone()), TokenSource::Config),
            (None, None) => (None, TokenSource::Unset),
        };
        Settings {
            server: pick("CHATTRS_SERVER", &files.server)
                .unwrap_or_else(|| DEFAULT_SERVER.to_string()),
            tags: pick("CHATTRS_TAGS", &files.tags),
            token,
            token_source,
            lead: pick("CHATTRS_LEAD", &files.lead),
            project_env: env("CHATTRS_PROJECT"),
            project_config: files.project.clone(),
        }
    }

    /// The project and where it came from: `flag` (or the variable), the
    /// config, the repo at `dir`, then the default. `flag` is the value clap
    /// parsed, which already holds the variable. Git runs only here, so only
    /// the commands that need a project pay for it.
    pub fn project(&self, flag: Option<&str>, dir: &Path) -> (String, ProjectSource) {
        let explicit = flag.or(self.project_env.as_deref());
        project::resolve(explicit, self.project_config.as_deref(), dir)
    }

    /// Fill what clap left unset. `server_is_default` says the server value
    /// is clap's built-in default, not a flag or a variable.
    pub fn apply(&self, args: &mut Args, server_is_default: bool) {
        match &mut args.command {
            Command::Serve(c) => fill(&mut c.token, &self.token),
            Command::Send(c) => self.apply_client(&mut c.client, server_is_default),
            Command::Reply(c) => self.apply_client(&mut c.client, server_is_default),
            Command::Watch(c) => self.apply_client(&mut c.client, server_is_default),
            Command::Ask(c) => self.apply_client(&mut c.client, server_is_default),
            Command::Inbox(c) => self.apply_client(&mut c.client, server_is_default),
            Command::Agents(c) => self.apply_client(&mut c.client, server_is_default),
            Command::Projects(c) => self.apply_client(&mut c.client, server_is_default),
            Command::Log(c) => self.apply_client(&mut c.client, server_is_default),
            Command::Status(c) => self.apply_client(&mut c.client, server_is_default),
            Command::Hook(c) => self.apply_client(&mut c.client, server_is_default),
            Command::Env(c) => {
                self.apply_server(&mut c.server, server_is_default);
                fill(&mut c.lead, &self.lead);
                fill(&mut c.tags, &self.tags);
                fill(&mut c.token, &self.token);
            }
            Command::Exec(c) => {
                self.apply_server(&mut c.server, server_is_default);
                fill(&mut c.lead, &self.lead);
                fill(&mut c.tags, &self.tags);
            }
            _ => {}
        }
    }

    fn apply_client(&self, client: &mut ClientArgs, server_is_default: bool) {
        self.apply_server(&mut client.server, server_is_default);
        fill(&mut client.token, &self.token);
    }

    fn apply_server(&self, server: &mut String, server_is_default: bool) {
        if server_is_default {
            server.clone_from(&self.server);
        }
    }
}

/// Set `slot` from `value` only when a flag or variable left it unset.
fn fill(slot: &mut Option<String>, value: &Option<String>) {
    if slot.is_none() {
        slot.clone_from(value);
    }
}

/// Whether the parsed command took `--server` from clap's default value.
/// The subcommand that holds the flag is the innermost one.
pub fn server_is_default(matches: &ArgMatches) -> bool {
    let mut leaf = matches;
    while let Some((_, inner)) = leaf.subcommand() {
        leaf = inner;
    }
    leaf.try_contains_id("server").unwrap_or(false)
        && leaf.value_source("server") == Some(ValueSource::DefaultValue)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::process::{Command as Process, Stdio};

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> =
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |name: &str| map.get(name).cloned()
    }

    fn files(server: Option<&str>, tags: Option<&str>, lead: Option<&str>) -> Config {
        Config {
            server: server.map(Into::into),
            tags: tags.map(Into::into),
            lead: lead.map(Into::into),
            ..Config::default()
        }
    }

    type Row<'a> = (&'a [(&'a str, &'a str)], Config, &'a str, Option<&'a str>, Option<&'a str>);

    #[test]
    fn server_tags_and_lead_follow_env_then_file_then_default() {
        let all_file = || files(Some("f:1"), Some("ft"), Some("fl@h"));
        let table: [Row; 4] = [
            (&[], Config::default(), DEFAULT_SERVER, None, None),
            (&[], all_file(), "f:1", Some("ft"), Some("fl@h")),
            (
                &[("CHATTRS_SERVER", "e:1"), ("CHATTRS_TAGS", "et"), ("CHATTRS_LEAD", "el@h")],
                all_file(),
                "e:1",
                Some("et"),
                Some("el@h"),
            ),
            // A set variable blocks the file, even when it is empty.
            (
                &[("CHATTRS_SERVER", ""), ("CHATTRS_TAGS", ""), ("CHATTRS_LEAD", "")],
                all_file(),
                "",
                Some(""),
                Some(""),
            ),
        ];
        for (env, file, server, tags, lead) in table {
            let s = Settings::resolve(&env_of(env), &file);
            assert_eq!(s.server, server, "{env:?}");
            assert_eq!(s.tags.as_deref(), tags, "{env:?}");
            assert_eq!(s.lead.as_deref(), lead, "{env:?}");
        }
    }

    type TokenRow<'a> = (&'a [(&'a str, &'a str)], &'a Config, Option<&'a str>, TokenSource);

    #[test]
    fn token_is_env_then_file_then_none() {
        let with_file = Config { token: Some("file-tok".into()), ..Config::default() };
        let bare = Config::default();
        let table: [TokenRow; 5] = [
            (&[], &with_file, Some("file-tok"), TokenSource::Config),
            (&[("CHATTRS_TOKEN", "env-tok")], &with_file, Some("env-tok"), TokenSource::Env),
            (&[("CHATTRS_TOKEN", "")], &with_file, Some(""), TokenSource::Env),
            (&[], &bare, None, TokenSource::Unset),
            (&[("CHATTRS_TOKEN", "env-tok")], &bare, Some("env-tok"), TokenSource::Env),
        ];
        for (env, file, token, source) in table {
            let s = Settings::resolve(&env_of(env), file);
            assert_eq!(s.token.as_deref(), token, "{env:?}");
            assert_eq!(s.token_source, source, "{env:?}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn token_files_pick_exact_then_longest_glob_then_plain() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let token_file = |name: &str| {
            let path = dir.path().join(name);
            std::fs::write(&path, format!("tok-{name}\n")).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            path
        };
        let home = dir.path().join("home");
        std::fs::create_dir_all(home.join(".config/chattrs")).unwrap();
        std::fs::write(
            home.join(".config/chattrs/config.toml"),
            format!(
                "token_file = \"{}\"\n[tokens]\n\"*@mac\" = \"{}\"\n\"w-*@mac\" = \"{}\"\n\"lead@mac\" = \"{}\"\n",
                token_file("plain").display(),
                token_file("host").display(),
                token_file("worker").display(),
                token_file("exact").display(),
            ),
        )
        .unwrap();
        let resolve = |env: &[(&str, &str)], address: &str| {
            let mut config = crate::config::load_from(Some(&home), dir.path()).unwrap();
            // The caller skips the pick when the variable is set.
            if env.iter().all(|(name, _)| *name != "CHATTRS_TOKEN") {
                config.select_token(Some(address)).unwrap();
            }
            Settings::resolve(&env_of(env), &config).token
        };
        assert_eq!(resolve(&[], "lead@mac").as_deref(), Some("tok-exact"));
        assert_eq!(resolve(&[], "w-1@mac").as_deref(), Some("tok-worker"));
        assert_eq!(resolve(&[], "x@mac").as_deref(), Some("tok-host"));
        assert_eq!(resolve(&[], "x@box").as_deref(), Some("tok-plain"));
        assert_eq!(resolve(&[("CHATTRS_TOKEN", "e")], "lead@mac").as_deref(), Some("e"));
    }

    fn git_repo(base: &Path, name: &str) -> PathBuf {
        let dir = base.join(name);
        std::fs::create_dir(&dir).unwrap();
        let status = Process::new("git")
            .arg("-C")
            .arg(&dir)
            .args(["init", "-q"])
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_COMMON_DIR")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success());
        dir
    }

    #[test]
    fn project_is_flag_then_env_then_config_then_git_then_default() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = git_repo(tmp.path(), "gitname");
        let cfg = Config { project: Some("conf".into()), ..Config::default() };
        let with_env = Settings::resolve(&env_of(&[("CHATTRS_PROJECT", "fromenv")]), &cfg);
        let with_cfg = Settings::resolve(&env_of(&[]), &cfg);
        let bare = Settings::resolve(&env_of(&[]), &Config::default());
        let env = ProjectSource::Env;
        assert_eq!(with_env.project(Some("flag"), &repo), ("flag".into(), env));
        assert_eq!(with_env.project(None, &repo), ("fromenv".into(), env));
        assert_eq!(with_cfg.project(None, &repo), ("conf".into(), ProjectSource::Config));
        assert_eq!(bare.project(None, &repo), ("gitname".into(), ProjectSource::Git));
        assert_eq!(bare.project(None, tmp.path()), ("default".into(), ProjectSource::Default));
        // A blank explicit value falls through to the config.
        assert_eq!(with_cfg.project(Some(" "), &repo), ("conf".into(), ProjectSource::Config));
    }
}
