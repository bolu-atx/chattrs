//! chattrs — config files (CLI-6). Two optional files feed the same values
//! as the `CHATTRS_*` variables, below the variables and the flags:
//!
//! 1. `~/.config/chattrs/config.toml`: `server`, `token_file`, `tags`, and an
//!    optional `[tokens]` table that maps an address or glob to a token
//!    file (SETUP-4).
//! 2. `.chattrs.toml` in the current directory or the nearest parent, up to
//!    a `.git` boundary: `project`, `lead`. It holds no secrets.
//!
//! `settings::Settings` merges these values with the variables; this module
//! only reads the files. The files use the flat subset of TOML that
//! `tokens::parse_pair` reads.

use std::path::{Path, PathBuf};

use crate::tokens::{glob_matches, parse_pair};

/// Values from the config files. `None` means no file set the value.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Config {
    pub server: Option<String>,
    pub tags: Option<String>,
    pub project: Option<String>,
    pub lead: Option<String>,
    /// The trimmed content of the `token_file`.
    pub token: Option<String>,
    /// The `token_file` path itself, for `doctor`.
    pub token_file: Option<PathBuf>,
    /// The `[tokens]` table: address pattern and token file path, in file
    /// order. The file is read only when the entry is chosen.
    pub token_map: Vec<(String, PathBuf)>,
}

/// Why a config file could not be loaded. `Display` gives the text that
/// `chattrs` prints after `chattrs: `.
#[derive(Debug)]
pub enum ConfigError {
    /// A config file could not be read.
    Read { file: PathBuf, source: std::io::Error },
    /// A line of a config file is wrong. `msg` says how.
    Parse { file: PathBuf, line: usize, msg: String },
    /// A repo file names a token or a token file.
    SecretInRepoFile { file: PathBuf, line: usize },
    /// A repo file has a `[tokens]` table.
    TokensInRepoFile { file: PathBuf },
    /// A token file could not be read.
    TokenFileRead { file: PathBuf, source: std::io::Error },
    /// A token file is wider than 0600.
    TokenFileMode { file: PathBuf, mode: u32 },
    /// A token file holds no token.
    TokenFileEmpty { file: PathBuf },
    /// The current directory is unknown.
    CurrentDir(std::io::Error),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Read { file, source } => write!(f, "{}: {source}", file.display()),
            ConfigError::Parse { file, line, msg } => {
                write!(f, "{}:{line}: {msg}", file.display())
            }
            ConfigError::SecretInRepoFile { file, line } => write!(
                f,
                "{}:{line}: a repo config file must not hold a token; put token_file in \
                 ~/.config/chattrs/config.toml",
                file.display()
            ),
            ConfigError::TokensInRepoFile { file } => {
                write!(f, "{}: a repo config file must not hold [tokens]", file.display())
            }
            ConfigError::TokenFileRead { file, source } => {
                write!(f, "token_file {}: {source}", file.display())
            }
            ConfigError::TokenFileMode { file, mode } => write!(
                f,
                "token_file {} has mode {mode:o}, wider than 600; run chmod 600",
                file.display()
            ),
            ConfigError::TokenFileEmpty { file } => {
                write!(f, "token_file {} is empty", file.display())
            }
            ConfigError::CurrentDir(source) => write!(f, "current directory: {source}"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// What `parse_file` reads: the flat pairs and the `[tokens]` entries.
struct Parsed {
    pairs: Vec<(String, String)>,
    tokens: Vec<(String, String)>,
}

/// Parse a file's `key = "value"` lines and its optional `[tokens]` table.
/// Keys outside `allowed` are errors that name the file and line. In the
/// table, each key is an address pattern, quoted or bare.
fn parse_file(path: &Path, allowed: &[&str]) -> Result<Parsed, ConfigError> {
    let text = std::fs::read_to_string(path)
        .map_err(|source| ConfigError::Read { file: path.to_path_buf(), source })?;
    let mut parsed = Parsed { pairs: Vec::new(), tokens: Vec::new() };
    let mut in_tokens = false;
    for (index, raw) in text.lines().enumerate() {
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let parse_error =
            |msg: String| ConfigError::Parse { file: path.to_path_buf(), line: index + 1, msg };
        if trimmed == "[tokens]" {
            in_tokens = true;
            continue;
        }
        let Some((key, value)) = parse_pair(trimmed) else {
            return Err(parse_error("expected key = \"value\"".to_string()));
        };
        if in_tokens {
            let pattern = key.trim_matches('"');
            if parsed.tokens.iter().any(|(seen, _)| seen == pattern) {
                return Err(parse_error(format!("duplicate address `{pattern}`")));
            }
            parsed.tokens.push((pattern.to_string(), value));
            continue;
        }
        if !allowed.contains(&key) {
            return Err(parse_error(format!("unknown key `{key}`")));
        }
        if parsed.pairs.iter().any(|(seen, _)| seen == key) {
            return Err(parse_error(format!("duplicate key `{key}`")));
        }
        parsed.pairs.push((key.to_string(), value));
    }
    Ok(parsed)
}

/// Read the token from `path`. On unix, a file wider than 0600 is refused.
pub(crate) fn read_token_file(path: &Path) -> Result<String, ConfigError> {
    let file = || path.to_path_buf();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = std::fs::metadata(path)
            .map_err(|source| ConfigError::TokenFileRead { file: file(), source })?;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o177 != 0 {
            return Err(ConfigError::TokenFileMode { file: file(), mode });
        }
    }
    let text = std::fs::read_to_string(path)
        .map_err(|source| ConfigError::TokenFileRead { file: file(), source })?;
    let token = text.trim();
    if token.is_empty() {
        return Err(ConfigError::TokenFileEmpty { file: file() });
    }
    Ok(token.to_string())
}

/// The nearest `.chattrs.toml` from `start` upward, stopping after the
/// first directory that holds `.git`.
pub(crate) fn find_repo_file(start: &Path) -> Option<PathBuf> {
    for dir in start.ancestors() {
        let file = dir.join(".chattrs.toml");
        if file.is_file() {
            return Some(file);
        }
        if dir.join(".git").exists() {
            return None;
        }
    }
    None
}

/// Refuse a repo file that names a token or a token file.
fn refuse_secrets(file: &Path) -> Result<(), ConfigError> {
    let text = std::fs::read_to_string(file)
        .map_err(|source| ConfigError::Read { file: file.to_path_buf(), source })?;
    for (index, raw) in text.lines().enumerate() {
        if parse_pair(raw.trim()).is_some_and(|(key, _)| matches!(key, "token" | "token_file")) {
            return Err(ConfigError::SecretInRepoFile {
                file: file.to_path_buf(),
                line: index + 1,
            });
        }
    }
    Ok(())
}

/// Load both files for a user `home` and a working directory `cwd`.
pub fn load_from(home: Option<&Path>, cwd: &Path) -> Result<Config, ConfigError> {
    let mut config = Config::default();
    let user_file = home.map(|h| h.join(".config/chattrs/config.toml"));
    if let Some(file) = user_file.filter(|f| f.is_file()) {
        let parsed = parse_file(&file, &["server", "token_file", "tags"])?;
        for (key, value) in parsed.pairs {
            match key.as_str() {
                "server" => config.server = Some(value),
                "tags" => config.tags = Some(value),
                _ => {
                    config.token = Some(read_token_file(Path::new(&value))?);
                    config.token_file = Some(PathBuf::from(value));
                }
            }
        }
        config.token_map = parsed
            .tokens
            .into_iter()
            .map(|(pattern, path)| (pattern, PathBuf::from(path)))
            .collect();
    }
    if let Some(file) = find_repo_file(cwd) {
        refuse_secrets(&file)?;
        let parsed = parse_file(&file, &["project", "lead"])?;
        if !parsed.tokens.is_empty() {
            return Err(ConfigError::TokensInRepoFile { file });
        }
        for (key, value) in parsed.pairs {
            match key.as_str() {
                "project" => config.project = Some(value),
                _ => config.lead = Some(value),
            }
        }
    }
    Ok(config)
}

/// Load the config for the real home and the working directory `cwd`.
pub fn load_at(cwd: &Path) -> Result<Config, ConfigError> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    load_from(home.as_deref(), cwd)
}

/// Load the config for the real home and the current directory.
pub fn load() -> Result<Config, ConfigError> {
    let cwd = std::env::current_dir().map_err(ConfigError::CurrentDir)?;
    load_at(&cwd)
}

/// The address a command will use, read before clap runs: a flag in `args`
/// (`--as`, `--address`, `--agent`) beats the environment (`CHATTRS_ADDRESS`,
/// then `CHATTRS_AGENT`). An agent name alone gives `<agent>@<hostname>`.
/// Scanning stops at a bare `--`, so a positional body never picks the
/// address. For `hook`, `--agent` names the agent kind, not an address, so
/// it is ignored.
pub fn address_hint(args: &[String], var: impl Fn(&str) -> Option<String>) -> Option<String> {
    let is_hook = args.first().is_some_and(|arg| arg == "hook");
    if args.first().is_some_and(|arg| arg == "exec") {
        return exec_address_hint(&args[1..]);
    }
    let args = &args[..args.iter().position(|arg| arg == "--").unwrap_or(args.len())];
    let flag = |name: &str| {
        let prefix = format!("{name}=");
        args.iter().enumerate().find_map(|(index, arg)| {
            if arg == name {
                args.get(index + 1).cloned()
            } else {
                arg.strip_prefix(&prefix).map(str::to_string)
            }
        })
    };
    let is_setup = args.first().is_some_and(|arg| arg == "setup");
    let host = |agent: String| format!("{agent}@{}", crate::cli::short_hostname());
    flag("--as")
        .filter(|_| !is_setup)
        .or_else(|| flag("--address"))
        .or_else(|| flag("--agent").filter(|_| !is_hook).map(host))
        .or_else(|| var("CHATTRS_ADDRESS").filter(|value| !value.is_empty()))
        .or_else(|| var("CHATTRS_AGENT").filter(|value| !value.is_empty()).map(host))
}

/// The address `chattrs exec` will use. Only the flags before the program
/// count: the program and its arguments are never read as chattrs flags.
fn exec_address_hint(args: &[String]) -> Option<String> {
    let mut address = None;
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--" => {
                return address
                    .or_else(|| rest.next().map(|p| crate::exec::exec_default_address(p)))
            }
            "--as" => address = rest.next().cloned(),
            "--project" | "--lead" | "--server" | "--tags" | "--host" | "--cwd"
            | "--session-name" => {
                rest.next();
            }
            _ if arg.starts_with("--as=") => {
                address = arg.strip_prefix("--as=").map(str::to_string)
            }
            _ if arg.starts_with('-') => {}
            program => return address.or_else(|| Some(crate::exec::exec_default_address(program))),
        }
    }
    address
}

impl Config {
    /// The `[tokens]` entry for `address`. An exact entry beats a glob. Among
    /// globs (entries with `*`), the longest pattern wins. A tie goes to the
    /// entry listed first in the file.
    pub(crate) fn mapped_token_file(&self, address: &str) -> Option<&Path> {
        // `max_by_key` keeps the last of equal ranks, so walk the file backwards.
        let (_, path) = self
            .token_map
            .iter()
            .rev()
            .filter(|(pattern, _)| glob_matches(pattern, address))
            .max_by_key(|(pattern, _)| (!pattern.contains('*'), pattern.len()))?;
        Some(path.as_path())
    }

    /// Replace the fallback `token` with the token of the `[tokens]` entry
    /// that fits `address`, if one does. The caller skips this when
    /// `CHATTRS_TOKEN` is set, so the variable keeps winning (`Settings`).
    pub fn select_token(&mut self, address: Option<&str>) -> Result<(), ConfigError> {
        let Some(path) = address.and_then(|address| self.mapped_token_file(address)) else {
            return Ok(());
        };
        self.token = Some(read_token_file(path)?);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write(path: &Path, text: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }

    #[cfg(unix)]
    fn chmod(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }

    #[test]
    fn user_and_repo_files_merge() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let tok = dir.path().join("tok");
        write(&tok, " s3cret\n");
        #[cfg(unix)]
        chmod(&tok, 0o600);
        write(
            &home.join(".config/chattrs/config.toml"),
            &format!("# c\nserver = \"b:1\"\ntags = \"gpu\"\ntoken_file = \"{}\"\n", tok.display()),
        );
        let repo = dir.path().join("repo");
        write(&repo.join(".chattrs.toml"), "project = \"alpha\"\nlead = \"o@m\" # x\n");
        fs::create_dir_all(repo.join(".git")).unwrap();
        let sub = repo.join("a/b");
        fs::create_dir_all(&sub).unwrap();
        let config = load_from(Some(&home), &sub).unwrap();
        assert_eq!(config.server.as_deref(), Some("b:1"));
        assert_eq!(config.tags.as_deref(), Some("gpu"));
        assert_eq!(config.project.as_deref(), Some("alpha"));
        assert_eq!(config.lead.as_deref(), Some("o@m"));
        assert_eq!(config.token.as_deref(), Some("s3cret"));
    }

    #[test]
    fn repo_file_does_not_cross_git_boundary() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join(".chattrs.toml"), "project = \"outer\"\n");
        let inner = dir.path().join("inner");
        fs::create_dir_all(inner.join(".git")).unwrap();
        assert_eq!(load_from(None, &inner).unwrap(), Config::default());
    }

    #[test]
    fn repo_file_with_a_secret_is_refused() {
        for key in ["token", "token_file"] {
            let dir = tempfile::tempdir().unwrap();
            write(&dir.path().join(".chattrs.toml"), &format!("{key} = \"x\"\n"));
            fs::create_dir_all(dir.path().join(".git")).unwrap();
            let error = load_from(None, dir.path()).unwrap_err().to_string();
            assert!(
                error.contains(".chattrs.toml:1") && error.contains("must not hold"),
                "{error}"
            );
        }
    }

    #[test]
    fn unknown_key_names_file_and_line() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join(".chattrs.toml"), "project = \"a\"\n\nlead2 = \"b\"\n");
        fs::create_dir_all(dir.path().join(".git")).unwrap();
        let error = load_from(None, dir.path()).unwrap_err().to_string();
        assert!(error.contains(".chattrs.toml:3: unknown key `lead2`"), "{error}");
        let home = dir.path().join("h");
        write(&home.join(".config/chattrs/config.toml"), "project = \"a\"\n");
        let error = load_from(Some(&home), dir.path()).unwrap_err().to_string();
        assert!(error.contains("config.toml:1: unknown key `project`"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn wide_token_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let tok = dir.path().join("tok");
        write(&tok, "abc\n");
        chmod(&tok, 0o640);
        let home = dir.path().join("h");
        write(
            &home.join(".config/chattrs/config.toml"),
            &format!("token_file = \"{}\"\n", tok.display()),
        );
        let error = load_from(Some(&home), dir.path()).unwrap_err().to_string();
        assert!(error.contains("wider than 600"), "{error}");
        chmod(&tok, 0o600);
        assert_eq!(load_from(Some(&home), dir.path()).unwrap().token.as_deref(), Some("abc"));
    }

    /// A home with token files `name` -> contents and a config that maps
    /// patterns to them. Returns the loaded config.
    fn mapped_config(fallback: bool, map: &[(&str, &str)]) -> (tempfile::TempDir, Config) {
        let guard = tempfile::tempdir().unwrap();
        let dir = guard.path().to_path_buf();
        let mut text = String::new();
        let token_path = |name: &str| {
            let path = dir.join(name);
            write(&path, &format!("tok-{name}\n"));
            #[cfg(unix)]
            chmod(&path, 0o600);
            path
        };
        if fallback {
            text.push_str(&format!("token_file = \"{}\"\n", token_path("plain").display()));
        }
        text.push_str("[tokens]\n");
        for (pattern, name) in map {
            text.push_str(&format!("\"{pattern}\" = \"{}\"\n", token_path(name).display()));
        }
        let home = dir.join("home");
        write(&home.join(".config/chattrs/config.toml"), &text);
        let config = load_from(Some(&home), &dir).unwrap();
        (guard, config)
    }

    fn token_for(config: &mut Config, address: &str) -> Option<String> {
        config.select_token(Some(address)).unwrap();
        config.token.clone()
    }

    #[test]
    fn exact_entry_beats_a_longer_glob() {
        let (_dir, mut config) = mapped_config(true, &[("*@mac", "glob"), ("lead@mac", "exact")]);
        assert_eq!(token_for(&mut config, "lead@mac").as_deref(), Some("tok-exact"));
    }

    #[test]
    fn longest_glob_wins_and_ties_go_to_the_first() {
        let map = [("*", "any"), ("*@mac", "host"), ("w-*@mac", "worker"), ("*@m*", "same-len")];
        let (_dir, mut config) = mapped_config(false, &map);
        assert_eq!(token_for(&mut config, "w-1@mac").as_deref(), Some("tok-worker"));
        let (_dir, mut config) = mapped_config(false, &map);
        assert_eq!(token_for(&mut config, "x@mac").as_deref(), Some("tok-host"));
        let (_dir, mut config) = mapped_config(false, &map);
        assert_eq!(token_for(&mut config, "x@box").as_deref(), Some("tok-any"));
    }

    #[test]
    fn no_match_keeps_the_plain_token_file() {
        let (_dir, mut config) = mapped_config(true, &[("lead@mac", "exact")]);
        assert_eq!(token_for(&mut config, "w1@mac").as_deref(), Some("tok-plain"));
        let (_dir, mut config) = mapped_config(true, &[("lead@mac", "exact")]);
        config.select_token(None).unwrap();
        assert_eq!(config.token.as_deref(), Some("tok-plain"));
        let (_dir, mut config) = mapped_config(false, &[("lead@mac", "exact")]);
        assert_eq!(token_for(&mut config, "w1@mac"), None);
    }

    #[test]
    fn repo_file_with_a_tokens_table_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join(".chattrs.toml"), "[tokens]\n\"a@b\" = \"/x\"\n");
        fs::create_dir_all(dir.path().join(".git")).unwrap();
        let error = load_from(None, dir.path()).unwrap_err().to_string();
        assert!(error.contains("must not hold [tokens]"), "{error}");
    }

    #[test]
    fn hook_ignores_the_agent_flag_and_a_bare_dashes_ends_the_scan() {
        let args = |list: &[&str]| list.iter().map(|arg| arg.to_string()).collect::<Vec<_>>();
        let var = |name: &str| (name == "CHATTRS_ADDRESS").then(|| "lead@mac".to_string());
        let hook = args(&["hook", "session-start", "--agent", "claude"]);
        assert_eq!(address_hint(&hook, var).as_deref(), Some("lead@mac"));
        let send = args(&["send", "--", "--as", "evil@mac"]);
        assert_eq!(address_hint(&send, var).as_deref(), Some("lead@mac"));
    }

    #[test]
    fn address_hint_prefers_flags_then_address_then_agent() {
        let args = |list: &[&str]| list.iter().map(|arg| arg.to_string()).collect::<Vec<_>>();
        let var = |name: &str| match name {
            "CHATTRS_ADDRESS" => Some("env@h".to_string()),
            "CHATTRS_AGENT" => Some("bot".to_string()),
            _ => None,
        };
        assert_eq!(address_hint(&args(&["env", "--as", "a@h"]), var).as_deref(), Some("a@h"));
        assert_eq!(address_hint(&args(&["send", "--address=b@h"]), var).as_deref(), Some("b@h"));
        assert_eq!(address_hint(&args(&["send"]), var).as_deref(), Some("env@h"));
        let host = crate::cli::short_hostname();
        assert_eq!(
            address_hint(&args(&["send"]), |name| (name == "CHATTRS_AGENT").then(|| "bot".into()))
                .as_deref(),
            Some(format!("bot@{host}").as_str())
        );
        assert_eq!(address_hint(&args(&["send"]), |_| None), None);
    }
}
