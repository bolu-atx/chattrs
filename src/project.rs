//! The project of a client. One resolver serves every client path: the
//! `hello`, hooks, `doctor`, and `env`.
//!
//! Precedence: `CHATTRS_PROJECT` (or `--project`), then the config `project`,
//! then the name of the repo, then `default`. The repo name is the basename
//! of the `origin` URL. With no `origin` it is the basename of the parent of
//! the common repo dir, so every worktree of a repo gets one name. It is
//! never the basename of the work tree root.

use std::io::Read as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::{config, model};

const GIT_TIMEOUT: Duration = Duration::from_secs(2);

/// Where the resolved project came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectSource {
    /// `CHATTRS_PROJECT` or `--project`.
    Env,
    /// `project` in a config file.
    Config,
    /// The name of the repo.
    Git,
    /// No setting and no repo.
    Default,
}

impl ProjectSource {
    /// The word `doctor` and `env` print.
    pub fn word(self) -> &'static str {
        match self {
            ProjectSource::Env => "env",
            ProjectSource::Config => "config",
            ProjectSource::Git => "git",
            ProjectSource::Default => "default",
        }
    }

    /// How a sentence says the project was chosen.
    pub fn how(self) -> &'static str {
        match self {
            ProjectSource::Env => "set by env",
            ProjectSource::Config => "set in config",
            ProjectSource::Git => "derived from git",
            ProjectSource::Default => "the default",
        }
    }
}

fn non_blank(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// Pick the project. `explicit` is the env or flag value, `config` the
/// config file value, and `dir` is where git looks for the repo.
pub fn resolve(
    explicit: Option<&str>,
    config: Option<&str>,
    dir: &Path,
) -> (String, ProjectSource) {
    if let Some(project) = non_blank(explicit) {
        return (project.to_string(), ProjectSource::Env);
    }
    if let Some(project) = non_blank(config) {
        return (project.to_string(), ProjectSource::Config);
    }
    match derive(dir) {
        Some(project) => (project, ProjectSource::Git),
        None => (model::DEFAULT_PROJECT.to_string(), ProjectSource::Default),
    }
}

/// Split a value from `CHATTRS_PROJECT` into `(explicit, config)`.
/// `config::apply_env` exports the config project as that variable, so the
/// two look alike once clap has run. This tells them apart.
pub fn split_applied(value: Option<String>) -> (Option<String>, Option<String>) {
    let from_config = config::project_from_config()
        && value.is_some()
        && value.as_deref() == std::env::var("CHATTRS_PROJECT").ok().as_deref();
    if from_config {
        (None, value)
    } else {
        (value, None)
    }
}

static CURRENT: OnceLock<(String, ProjectSource)> = OnceLock::new();

/// Fix the project of this process before the first `current()` call. A hook
/// uses it, since its project comes from the session cwd.
pub fn set_current(project: (String, ProjectSource)) {
    let _ = CURRENT.set(project);
}

/// The project of this process, from the environment and the current
/// directory. Resolved once, since a `watch` reconnects often.
pub fn current() -> &'static (String, ProjectSource) {
    CURRENT.get_or_init(|| {
        let (explicit, config) = split_applied(std::env::var("CHATTRS_PROJECT").ok());
        let dir = std::env::current_dir().unwrap_or_default();
        resolve(explicit.as_deref(), config.as_deref(), &dir)
    })
}

/// The repo name for `dir`, or `None` outside a repo, without `git`, or when
/// the name is not a valid project.
pub fn derive(dir: &Path) -> Option<String> {
    let name = match git(dir, &["remote", "get-url", "origin"]) {
        Some(url) => url_basename(&url).to_string(),
        None => {
            let common = git(dir, &["rev-parse", "--path-format=absolute", "--git-common-dir"])?;
            common_dir_name(&common)?
        }
    };
    let name = sanitize(&name);
    model::is_valid_topic(&name).then_some(name)
}

/// The repo name from the common dir: `/x/repo/.git` gives `repo`, and a
/// bare `/x/repo.git` gives `repo`.
fn common_dir_name(common: &str) -> Option<String> {
    let path = Path::new(common.trim());
    let name = path.file_name()?.to_str()?;
    if name == ".git" {
        return Some(path.parent()?.file_name()?.to_str()?.to_string());
    }
    Some(name.trim_end_matches(".git").to_string())
}

/// The last path segment of a remote URL, without `.git`. It handles
/// `https://host/o/repo.git`, `git@host:o/repo.git`, `host:repo`, and a
/// trailing slash.
fn url_basename(url: &str) -> &str {
    let url = url.trim().trim_end_matches('/');
    let last = url.rsplit(['/', ':']).next().unwrap_or(url);
    last.strip_suffix(".git").unwrap_or(last)
}

/// Lowercase, map every char outside `[a-z0-9]` to `-`, collapse repeats,
/// trim `-`, and cut to the project length.
fn sanitize(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars().flat_map(char::to_lowercase) {
        let c = if c.is_ascii_lowercase() || c.is_ascii_digit() { c } else { '-' };
        if c != '-' || !out.ends_with('-') {
            out.push(c);
        }
    }
    out.truncate(model::MAX_TOPIC_BYTES);
    out.trim_matches('-').to_string()
}

/// Run git in `dir` and return its trimmed stdout, or `None` on any failure
/// or after the timeout. The variables that name a repo are cleared, so git
/// looks at `dir` and not at a repo the environment picks.
fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let mut command = Command::new("git");
    command
        .arg("-C")
        .arg(dir)
        .args(args)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    #[cfg(test)]
    command.env("GIT_CONFIG_GLOBAL", "/dev/null").env("GIT_CONFIG_NOSYSTEM", "1");
    let mut child = command.spawn().ok()?;
    let start = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if start.elapsed() < GIT_TIMEOUT => {
                std::thread::sleep(Duration::from_millis(5))
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    };
    if !status.success() {
        return None;
    }
    let mut out = String::new();
    child.stdout.take()?.read_to_string(&mut out).ok()?;
    let out = out.trim();
    (!out.is_empty()).then(|| out.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_basename_handles_common_shapes() {
        for (url, want) in [
            ("https://github.com/o/repo.git", "repo"),
            ("https://github.com/o/repo", "repo"),
            ("https://github.com/o/repo/", "repo"),
            ("git@github.com:o/repo.git", "repo"),
            ("git@host:repo.git", "repo"),
            ("ssh://git@host:22/o/Repo.git/", "Repo"),
            ("/srv/git/repo.git", "repo"),
        ] {
            assert_eq!(url_basename(url), want, "{url}");
        }
    }

    #[test]
    fn sanitize_fits_the_project_charset() {
        assert_eq!(sanitize("My_Repo.v2"), "my-repo-v2");
        assert_eq!(sanitize("--a  b--"), "a-b");
        assert_eq!(sanitize("___"), "");
        assert_eq!(sanitize(&"a".repeat(80)).len(), 64);
        assert_eq!(sanitize(&format!("{}-b", "a".repeat(63))), "a".repeat(63));
    }

    #[test]
    fn common_dir_names() {
        assert_eq!(common_dir_name("/x/repo/.git\n").as_deref(), Some("repo"));
        assert_eq!(common_dir_name("/x/repo.git").as_deref(), Some("repo"));
    }

    /// Run git in `dir` with no global config and no inherited repo.
    fn run(dir: &Path, args: &[&str]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(["-c", "user.name=t", "-c", "user.email=t@t"])
            .args(args)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_COMMON_DIR")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?}");
    }

    fn repo(base: &Path, name: &str) -> std::path::PathBuf {
        let dir = base.join(name);
        std::fs::create_dir(&dir).unwrap();
        run(&dir, &["init", "-q"]);
        dir
    }

    #[test]
    fn origin_remote_names_the_project() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = repo(tmp.path(), "checkout");
        run(&dir, &["remote", "add", "origin", "git@github.com:o/My_Repo.git"]);
        assert_eq!(derive(&dir).as_deref(), Some("my-repo"));
    }

    #[test]
    fn no_remote_uses_the_dir_name() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = repo(tmp.path(), "Solo");
        assert_eq!(derive(&dir).as_deref(), Some("solo"));
        let sub = dir.join("a");
        std::fs::create_dir(&sub).unwrap();
        assert_eq!(derive(&sub).as_deref(), Some("solo"));
    }

    #[test]
    fn a_worktree_has_the_name_of_the_main_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = repo(tmp.path(), "mainrepo");
        run(&dir, &["commit", "-q", "--allow-empty", "-m", "x"]);
        let wt = tmp.path().join("other-name");
        run(&dir, &["worktree", "add", "-q", "-b", "side", wt.to_str().unwrap()]);
        assert_eq!(derive(&wt).as_deref(), Some("mainrepo"));
    }

    #[test]
    fn a_plain_dir_has_no_project() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(derive(tmp.path()), None);
        assert_eq!(
            resolve(None, None, tmp.path()),
            ("default".to_string(), ProjectSource::Default)
        );
    }

    #[test]
    fn precedence_is_env_then_config_then_git_then_default() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = repo(tmp.path(), "gitname");
        let at = |env, cfg| resolve(env, cfg, &dir);
        assert_eq!(at(Some("e"), Some("c")), ("e".into(), ProjectSource::Env));
        assert_eq!(at(None, Some("c")), ("c".into(), ProjectSource::Config));
        assert_eq!(at(Some("  "), None), ("gitname".into(), ProjectSource::Git));
    }
}
