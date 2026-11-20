//! chattrs — config files (CLI-6). Two optional files feed the same values
//! as the `CHATTRS_*` variables, below the variables and the flags:
//!
//! 1. `~/.config/chattrs/config.toml`: `server`, `token_file`, `tags`.
//! 2. `.chattrs.toml` in the current directory or the nearest parent, up to
//!    a `.git` boundary: `project`, `lead`. It holds no secrets.
//!
//! `apply_env` fills a variable only when the environment leaves it unset,
//! so clap and the `env_*` readers see one merged view. The files use the
//! flat subset of TOML that `tokens::parse_pair` reads.

use std::path::{Path, PathBuf};

use crate::tokens::parse_pair;

/// Values from the config files. `None` means no file set the value.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Config {
    pub server: Option<String>,
    pub tags: Option<String>,
    pub project: Option<String>,
    pub lead: Option<String>,
    /// The trimmed content of the `token_file`.
    pub token: Option<String>,
}

/// Parse a file's `key = "value"` lines. Keys outside `allowed` are errors
/// that name the file and line.
fn parse_file(path: &Path, allowed: &[&str]) -> Result<Vec<(String, String)>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut pairs: Vec<(String, String)> = Vec::new();
    for (index, raw) in text.lines().enumerate() {
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let at = format!("{}:{}", path.display(), index + 1);
        let Some((key, value)) = parse_pair(trimmed) else {
            return Err(format!("{at}: expected key = \"value\""));
        };
        if !allowed.contains(&key) {
            return Err(format!("{at}: unknown key `{key}`"));
        }
        if pairs.iter().any(|(seen, _)| seen == key) {
            return Err(format!("{at}: duplicate key `{key}`"));
        }
        pairs.push((key.to_string(), value));
    }
    Ok(pairs)
}

/// Read the token from `path`. On unix, a file wider than 0600 is refused.
fn read_token_file(path: &Path) -> Result<String, String> {
    let shown = path.display();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = std::fs::metadata(path).map_err(|e| format!("token_file {shown}: {e}"))?;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o177 != 0 {
            return Err(format!(
                "token_file {shown} has mode {mode:o}, wider than 600; run chmod 600"
            ));
        }
    }
    let text = std::fs::read_to_string(path).map_err(|e| format!("token_file {shown}: {e}"))?;
    let token = text.trim();
    if token.is_empty() {
        return Err(format!("token_file {shown} is empty"));
    }
    Ok(token.to_string())
}

/// The nearest `.chattrs.toml` from `start` upward, stopping after the
/// first directory that holds `.git`.
fn find_repo_file(start: &Path) -> Option<PathBuf> {
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
fn refuse_secrets(file: &Path) -> Result<(), String> {
    let text = std::fs::read_to_string(file).map_err(|e| format!("{}: {e}", file.display()))?;
    for (index, raw) in text.lines().enumerate() {
        if parse_pair(raw.trim()).is_some_and(|(key, _)| matches!(key, "token" | "token_file")) {
            return Err(format!(
                "{}:{}: a repo config file must not hold a token; put token_file in \
                 ~/.config/chattrs/config.toml",
                file.display(),
                index + 1
            ));
        }
    }
    Ok(())
}

/// Load both files for a user `home` and a working directory `cwd`.
pub fn load_from(home: Option<&Path>, cwd: &Path) -> Result<Config, String> {
    let mut config = Config::default();
    let user_file = home.map(|h| h.join(".config/chattrs/config.toml"));
    if let Some(file) = user_file.filter(|f| f.is_file()) {
        for (key, value) in parse_file(&file, &["server", "token_file", "tags"])? {
            match key.as_str() {
                "server" => config.server = Some(value),
                "tags" => config.tags = Some(value),
                _ => config.token = Some(read_token_file(Path::new(&value))?),
            }
        }
    }
    if let Some(file) = find_repo_file(cwd) {
        refuse_secrets(&file)?;
        for (key, value) in parse_file(&file, &["project", "lead"])? {
            match key.as_str() {
                "project" => config.project = Some(value),
                _ => config.lead = Some(value),
            }
        }
    }
    Ok(config)
}

/// Load the config for the real home and the current directory.
pub fn load() -> Result<Config, String> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let cwd = std::env::current_dir().map_err(|e| format!("current directory: {e}"))?;
    load_from(home.as_deref(), &cwd)
}

impl Config {
    /// The `(variable, value)` pairs to add: only variables `is_set`
    /// reports as unset in the environment.
    pub fn missing_env(&self, is_set: impl Fn(&str) -> bool) -> Vec<(&'static str, &str)> {
        [
            ("CHATTRS_SERVER", &self.server),
            ("CHATTRS_TAGS", &self.tags),
            ("CHATTRS_PROJECT", &self.project),
            ("CHATTRS_LEAD", &self.lead),
            ("CHATTRS_TOKEN", &self.token),
        ]
        .into_iter()
        .filter_map(|(name, value)| value.as_deref().map(|v| (name, v)))
        .filter(|(name, _)| !is_set(name))
        .collect()
    }

    /// Export the config as the variables clap and the readers look at.
    /// Call it once, before parsing arguments.
    pub fn apply_env(&self) {
        for (name, value) in self.missing_env(|name| std::env::var_os(name).is_some()) {
            std::env::set_var(name, value);
        }
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
    fn config_fills_only_what_the_environment_leaves_unset() {
        let config = Config {
            server: Some("file:1".into()),
            project: Some("repo".into()),
            ..Config::default()
        };
        let added = config.missing_env(|name| name == "CHATTRS_SERVER");
        assert_eq!(added, vec![("CHATTRS_PROJECT", "repo")]);
        assert_eq!(config.missing_env(|_| false).len(), 2);
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
            let error = load_from(None, dir.path()).unwrap_err();
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
        let error = load_from(None, dir.path()).unwrap_err();
        assert!(error.contains(".chattrs.toml:3: unknown key `lead2`"), "{error}");
        let home = dir.path().join("h");
        write(&home.join(".config/chattrs/config.toml"), "project = \"a\"\n");
        let error = load_from(Some(&home), dir.path()).unwrap_err();
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
        let error = load_from(Some(&home), dir.path()).unwrap_err();
        assert!(error.contains("wider than 600"), "{error}");
        chmod(&tok, 0o600);
        assert_eq!(load_from(Some(&home), dir.path()).unwrap().token.as_deref(), Some("abc"));
    }
}
