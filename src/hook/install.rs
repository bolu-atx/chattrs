//! chattrs — hook_install: `hooks install` and `hooks remove` edit the
//! hook config of Claude Code and Codex in place.
//!
//! Both agents use a top-level `hooks` object that maps an event name to
//! an array of groups. Only entries whose command starts with `chattrs
//! hook ` belong to chattrs; everything else is left alone. Output is
//! pretty JSON with a 2-space indent and a trailing newline, so a file in
//! that format round-trips byte for byte through install then remove. A
//! file in another format keeps its content and key order, but is
//! reformatted on the first change.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{json, Map, Value};

use crate::cli::{HooksAction, HooksAgent, HooksTarget};

/// The events chattrs hooks, as the agents name them, with the `chattrs
/// hook` argument for each.
const EVENTS: [(&str, &str); 4] = [
    ("SessionStart", "session-start"),
    ("UserPromptSubmit", "user-prompt-submit"),
    ("PostToolUse", "post-tool-use"),
    ("Stop", "stop"),
];

/// Commands that start with this belong to chattrs.
const COMMAND_PREFIX: &str = "chattrs hook ";

/// Seconds the agent lets a hook run before it kills the hook.
const HOOK_TIMEOUT_SECS: u64 = 5;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Agent {
    Claude,
    Codex,
}

impl Agent {
    fn name(self) -> &'static str {
        match self {
            Agent::Claude => "claude",
            Agent::Codex => "codex",
        }
    }

    /// The config path for this agent, user level or in the current dir.
    fn config_path(self, project: bool) -> Result<PathBuf, String> {
        let base = if project {
            std::env::current_dir().map_err(|error| format!("read current dir: {error}"))?
        } else {
            home()?
        };
        Ok(match (self, project) {
            (Agent::Claude, _) => base.join(".claude/settings.json"),
            (Agent::Codex, true) => base.join(".codex/hooks.json"),
            (Agent::Codex, false) => codex_home(&base).join("hooks.json"),
        })
    }

    /// What the user must do after an install for the hooks to run.
    fn next_step(self) -> &'static str {
        match self {
            Agent::Claude => {
                "claude: Claude Code may ask you to review hooks edited outside it; accept \
                 the chattrs hooks"
            }
            Agent::Codex => {
                "codex: open /hooks in Codex and trust the chattrs hooks; they do not run \
                 until trusted"
            }
        }
    }
}

/// The Codex home: `CODEX_HOME` when set, else `.codex` under `home`.
pub(crate) fn codex_home(home: &Path) -> PathBuf {
    match std::env::var_os("CODEX_HOME") {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => home.join(".codex"),
    }
}

pub(crate) fn home() -> Result<PathBuf, String> {
    match std::env::var_os("HOME") {
        Some(home) if !home.is_empty() => Ok(PathBuf::from(home)),
        _ => Err("HOME is not set".to_string()),
    }
}

/// Run `hooks install` or `hooks remove`.
pub fn run(action: HooksAction) -> Result<(), String> {
    let (target, install) = match action {
        HooksAction::Install(target) => (target, true),
        HooksAction::Remove(target) => (target, false),
    };
    let agents = match target.agent {
        HooksAgent::Claude => vec![Agent::Claude],
        HooksAgent::Codex => vec![Agent::Codex],
        HooksAgent::All => vec![Agent::Claude, Agent::Codex],
    };
    let explicit = target.agent != HooksAgent::All;
    let mut done = Vec::new();
    for agent in agents {
        if edit_agent(agent, &target, install, explicit)? {
            done.push(agent);
        }
    }
    if install && !done.is_empty() {
        for agent in done {
            eprintln!("{}", agent.next_step());
        }
        eprintln!("chattrs must be on the agent's PATH for the hooks to run");
    }
    Ok(())
}

/// The events of `agent` that lack a chattrs hook in the user-level config,
/// with the config path. Read-only: a missing file means every event lacks one.
pub(crate) fn missing_hooks(agent: HooksAgent) -> Result<(PathBuf, Vec<&'static str>), String> {
    let agent = match agent {
        HooksAgent::Codex => Agent::Codex,
        HooksAgent::Claude | HooksAgent::All => Agent::Claude,
    };
    let path = agent.config_path(false)?;
    let original = match fs::read_to_string(&path) {
        Ok(text) => Some(text),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(format!("read {}: {error}", path.display())),
    };
    let root = parse_root(original.as_deref(), &path)?;
    let installed = |event: &str| {
        root.get("hooks")
            .and_then(|hooks| hooks.get(event))
            .and_then(Value::as_array)
            .is_some_and(|groups| groups.iter().any(|g| group_hooks(g).iter().any(is_own_command)))
    };
    let missing = EVENTS.iter().map(|(event, _)| *event).filter(|e| !installed(e)).collect();
    Ok((path, missing))
}

/// Edit one agent's config. Returns true when the agent was handled, false
/// when `--agent all` skipped it for a missing config directory.
fn edit_agent(
    agent: Agent,
    target: &HooksTarget,
    install: bool,
    explicit: bool,
) -> Result<bool, String> {
    let path = agent.config_path(target.project)?;
    let dir = path.parent().ok_or_else(|| format!("no parent dir for {}", path.display()))?;
    if !explicit && !dir.is_dir() {
        eprintln!("chattrs: skipping {}: {} does not exist", agent.name(), dir.display());
        return Ok(false);
    }
    let original = match fs::read_to_string(&path) {
        Ok(text) => Some(text),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(format!("read {}: {error}", path.display())),
    };
    if original.is_none() && !install {
        return Ok(true);
    }
    let mut root = parse_root(original.as_deref(), &path)?;
    if install {
        install_hooks(&mut root, agent);
    } else {
        remove_hooks(&mut root);
    }
    let text = render(&root);
    if original.as_deref() != Some(text.as_str()) {
        write_config(&path, original.is_some(), &text)?;
    }
    let verb = if install { "installed hooks in" } else { "removed hooks from" };
    eprintln!("chattrs: {verb} {}", path.display());
    Ok(true)
}

/// Parse a config file. A missing or blank file is an empty object.
fn parse_root(original: Option<&str>, path: &Path) -> Result<Map<String, Value>, String> {
    let Some(text) = original.filter(|text| !text.trim().is_empty()) else {
        return Ok(Map::new());
    };
    match serde_json::from_str(text) {
        Ok(Value::Object(root)) => Ok(root),
        Ok(_) => Err(format!("{} is not a JSON object", path.display())),
        Err(error) => Err(format!("{} is not valid JSON: {error}", path.display())),
    }
}

/// The file text: pretty JSON, 2-space indent, trailing newline.
fn render(root: &Map<String, Value>) -> String {
    let mut text = serde_json::to_string_pretty(root).unwrap_or_default();
    text.push('\n');
    text
}

fn is_own_command(hook: &Value) -> bool {
    hook["command"].as_str().is_some_and(|command| command.starts_with(COMMAND_PREFIX))
}

fn group_hooks(group: &Value) -> &[Value] {
    group["hooks"].as_array().map_or(&[], Vec::as_slice)
}

/// Add one chattrs group to each event array that lacks one.
fn install_hooks(root: &mut Map<String, Value>, agent: Agent) {
    let Some(hooks) = root.entry("hooks").or_insert_with(|| json!({})).as_object_mut() else {
        return;
    };
    for (event, argument) in EVENTS {
        let Some(groups) = hooks.entry(event).or_insert_with(|| json!([])).as_array_mut() else {
            continue;
        };
        if groups.iter().any(|group| group_hooks(group).iter().any(is_own_command)) {
            continue;
        }
        groups.push(json!({"hooks": [{
            "type": "command",
            "command": format!("{COMMAND_PREFIX}{argument} --agent {}", agent.name()),
            "timeout": HOOK_TIMEOUT_SECS,
        }]}));
    }
}

/// Drop chattrs entries. An event array or the `hooks` object goes only
/// when removing our entries emptied it.
fn remove_hooks(root: &mut Map<String, Value>) {
    let Some(hooks) = root.get_mut("hooks").and_then(Value::as_object_mut) else {
        return;
    };
    let had_events = !hooks.is_empty();
    for (event, _) in EVENTS {
        let Some(groups) = hooks.get_mut(event).and_then(Value::as_array_mut) else {
            continue;
        };
        let before = groups.len();
        groups.retain_mut(|group| !strip_own(group));
        if before > 0 && groups.is_empty() {
            hooks.shift_remove(event);
        }
    }
    if had_events && hooks.is_empty() {
        root.shift_remove("hooks");
    }
}

/// Remove own commands from a group. True when that left the group empty
/// of hooks, so the caller drops the group.
fn strip_own(group: &mut Value) -> bool {
    let Some(entries) = group.get_mut("hooks").and_then(Value::as_array_mut) else {
        return false;
    };
    let before = entries.len();
    entries.retain(|hook| !is_own_command(hook));
    before > 0 && entries.is_empty()
}

/// Write `text` to `path` through a temp file in the same directory, then
/// rename it over the target. The first change to an existing file saves
/// a `.bak` copy when none exists.
fn write_config(path: &Path, existed: bool, text: &str) -> Result<(), String> {
    let dir = path.parent().ok_or_else(|| format!("no parent dir for {}", path.display()))?;
    fs::create_dir_all(dir).map_err(|error| format!("create {}: {error}", dir.display()))?;
    if existed {
        let backup = PathBuf::from(format!("{}.bak", path.display()));
        if !backup.exists() {
            fs::copy(path, &backup)
                .map_err(|error| format!("write {}: {error}", backup.display()))?;
        }
    }
    let name = path.file_name().and_then(|name| name.to_str()).unwrap_or("config");
    let temp = dir.join(format!(".{name}.chattrs-{}.tmp", std::process::id()));
    fs::write(&temp, text).map_err(|error| format!("write {}: {error}", temp.display()))?;
    if existed {
        if let Ok(metadata) = fs::metadata(path) {
            let _ = fs::set_permissions(&temp, metadata.permissions());
        }
    }
    fs::rename(&temp, path).map_err(|error| {
        let _ = fs::remove_file(&temp);
        format!("replace {}: {error}", path.display())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(text: &str) -> Map<String, Value> {
        serde_json::from_str(text).unwrap()
    }

    const FOREIGN: &str = r#"{
  "model": "x",
  "hooks": {
    "Stop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "other.sh"
          }
        ]
      }
    ],
    "PreToolUse": []
  },
  "theme": "dark"
}
"#;

    #[test]
    fn install_twice_changes_nothing_the_second_time() {
        let mut once = root(FOREIGN);
        install_hooks(&mut once, Agent::Codex);
        let mut twice = once.clone();
        install_hooks(&mut twice, Agent::Codex);
        assert_eq!(render(&once), render(&twice));
    }

    #[test]
    fn install_then_remove_restores_the_original_text() {
        for original in [FOREIGN, "{}\n"] {
            let mut config = root(original);
            install_hooks(&mut config, Agent::Claude);
            assert_ne!(render(&config), original);
            remove_hooks(&mut config);
            assert_eq!(render(&config), original);
        }
    }

    #[test]
    fn install_keeps_foreign_hooks_and_key_order() {
        let mut config = root(FOREIGN);
        install_hooks(&mut config, Agent::Claude);
        let keys: Vec<_> = config.keys().cloned().collect();
        assert_eq!(keys, ["model", "hooks", "theme"]);
        assert_eq!(config["hooks"]["Stop"].as_array().unwrap().len(), 2);
        assert_eq!(config["hooks"]["Stop"][0]["hooks"][0]["command"], "other.sh");
    }
}
