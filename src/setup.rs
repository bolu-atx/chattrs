//! chattrs — setup (SETUP-1): `chattrs setup` prepares one host.
//!
//! For each agent it finds (Claude Code, Codex), it installs the hooks with
//! the `hooks install` code and writes the skill file. It writes the user
//! config only when none exists. The skill file belongs to chattrs, so a
//! re-run refreshes its text. Hook files and the config are never
//! overwritten.

use std::fs;
use std::path::{Path, PathBuf};

use crate::cli::{HooksAction, HooksAgent, HooksTarget, SetupArgs};
use crate::{hook_install, skill};

/// The starter config. Every line is a comment, so it changes nothing until
/// the user edits it.
const CONFIG_TEMPLATE: &str = "\
# chattrs client config. Remove the leading # from a line to use it.
# server = \"127.0.0.1:4646\"
# token_file = \"/home/me/.config/chattrs/token\"   # absolute path; mode 0600
# tags = \"gpu:4090\"
#
# One host, several identities: map an address or a glob to its own token file.
# The exact address wins; among globs, the longest pattern wins.
# [tokens]
# \"lead@mac\" = \"/home/me/.config/chattrs/lead-token\"
# \"*@mac\" = \"/home/me/.config/chattrs/mac-token\"
";

/// True when `program` is a file in a `PATH` directory.
fn on_path(program: &str) -> bool {
    std::env::var_os("PATH")
        .is_some_and(|path| std::env::split_paths(&path).any(|dir| dir.join(program).is_file()))
}

/// Write `text` to `path` when the file is missing or differs. Returns true
/// when it wrote.
fn write_if_changed(path: &Path, text: &str) -> Result<bool, String> {
    if fs::read_to_string(path).is_ok_and(|current| current == text) {
        return Ok(false);
    }
    let dir = path.parent().ok_or_else(|| format!("no parent dir for {}", path.display()))?;
    fs::create_dir_all(dir).map_err(|error| format!("create {}: {error}", dir.display()))?;
    fs::write(path, text).map_err(|error| format!("write {}: {error}", path.display()))?;
    Ok(true)
}

/// Create the config with mode 0600. Returns false when it already exists.
fn write_config_if_missing(path: &Path) -> Result<bool, String> {
    if path.exists() {
        return Ok(false);
    }
    let dir = path.parent().ok_or_else(|| format!("no parent dir for {}", path.display()))?;
    fs::create_dir_all(dir).map_err(|error| format!("create {}: {error}", dir.display()))?;
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let fail = |error: std::io::Error| format!("write {}: {error}", path.display());
    let mut file = options.open(path).map_err(fail)?;
    std::io::Write::write_all(&mut file, CONFIG_TEMPLATE.as_bytes()).map_err(fail)?;
    Ok(true)
}

/// One agent `setup` can serve.
pub(crate) struct Target {
    pub(crate) name: &'static str,
    pub(crate) agent: HooksAgent,
    /// The directory the agent reads skills from.
    pub(crate) skills_dir: PathBuf,
    pub(crate) detected: bool,
}

/// Claude Code and Codex for the user `home`, each marked found or not.
pub(crate) fn targets(home: &Path) -> [Target; 2] {
    let claude_dir = home.join(".claude");
    let codex_dir = hook_install::codex_home(home);
    [
        Target {
            name: "claude",
            agent: HooksAgent::Claude,
            skills_dir: claude_dir.join("skills"),
            detected: claude_dir.is_dir() || on_path("claude"),
        },
        Target {
            name: "codex",
            agent: HooksAgent::Codex,
            skills_dir: codex_dir.join("skills"),
            detected: codex_dir.is_dir() || on_path("codex"),
        },
    ]
}

/// Install the hooks and the skill for one agent.
fn setup_agent(target: &Target) -> Result<(), String> {
    hook_install::run(HooksAction::Install(HooksTarget { agent: target.agent, project: false }))?;
    let path = target.skills_dir.join("chattrs/SKILL.md");
    let verb = if write_if_changed(&path, skill::text())? { "wrote" } else { "kept" };
    eprintln!("chattrs: {verb} the {} skill {}", target.name, path.display());
    Ok(())
}

/// Run `chattrs setup`. `--broker` sets up a broker host and nothing else.
/// `--join` stores the tokens first, then the host setup runs as usual.
pub fn run(args: SetupArgs) -> Result<(), String> {
    if args.broker {
        return crate::setup_broker::run(&args);
    }
    if !args.join.is_empty() {
        crate::setup_join::run(&args)?;
    }
    run_host()
}

/// Hooks, skill, and config for this host.
fn run_host() -> Result<(), String> {
    let home = hook_install::home()?;
    let targets = targets(&home);
    for target in targets.iter().filter(|target| !target.detected) {
        eprintln!("chattrs: {} not found, skipped", target.name);
    }
    let found: Vec<&Target> = targets.iter().filter(|target| target.detected).collect();
    for target in &found {
        setup_agent(target)?;
    }
    if found.is_empty() {
        eprintln!("chattrs: no agent found; install Claude Code or Codex, then run setup again");
    }
    let config = home.join(".config/chattrs/config.toml");
    if write_config_if_missing(&config)? {
        eprintln!("chattrs: wrote {}", config.display());
    } else {
        eprintln!("chattrs: kept {}", config.display());
    }
    if !found.is_empty() {
        eprintln!("chattrs: start an agent on the swarm with: chattrs exec claude");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_template_parses_as_an_empty_config() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let file = home.join(".config/chattrs/config.toml");
        assert!(write_config_if_missing(&file).unwrap());
        assert!(!write_config_if_missing(&file).unwrap());
        let config = crate::config::load_from(Some(&home), dir.path()).unwrap();
        assert_eq!(config, crate::config::Config::default());
    }

    #[test]
    fn write_if_changed_writes_only_on_a_difference() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a/SKILL.md");
        assert!(write_if_changed(&file, "one").unwrap());
        assert!(!write_if_changed(&file, "one").unwrap());
        assert!(write_if_changed(&file, "two").unwrap());
        assert_eq!(fs::read_to_string(&file).unwrap(), "two");
    }
}
