//! chattrs — setup guide (SETUP-5): `chattrs setup-guide` lists the steps to
//! set up or repair chattrs, for an agent to follow.
//!
//! It is read-only and always exits 0. It runs no ssh and writes no file. It
//! never prints a token, a join string, or the content of a token file. It
//! never says `hello`: the broker check is a TCP connect only, so the guide
//! cannot cause a rejected-hello log line. Token acceptance is `chattrs
//! doctor`'s job, and the guide lists that as a step it cannot mark done.
//! Remote state is unknowable here, so remote steps stay `todo` and say which
//! check command decides whether to act.

use serde::Serialize;

use crate::cli::{DoctorArgs, GuideRole, SetupGuideArgs};
use crate::settings::{process_env, Settings};
use crate::{autostart, config, doctor, hook, setup, RunError};

mod swarm;

/// Where a step stands. Remote steps are always `Todo`.
#[derive(Serialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Status {
    Done,
    Todo,
    Blocked,
}

/// Who acts: the agent runs it, the human does it, or the agent asks the
/// human and passes the answer back as a flag.
#[derive(Serialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Who {
    Agent,
    Human,
    Ask,
}

/// One step. Fields are in alphabetical order so the JSON keys are sorted.
#[derive(Serialize, Debug)]
pub(crate) struct Step {
    pub(crate) command: Option<String>,
    pub(crate) id: String,
    pub(crate) status: Status,
    pub(crate) who: Who,
    pub(crate) why: String,
}

impl Step {
    pub(crate) fn new(
        id: &str,
        status: Status,
        who: Who,
        command: Option<&str>,
        why: &str,
    ) -> Step {
        Step {
            command: command.map(str::to_string),
            id: id.to_string(),
            status,
            who,
            why: why.to_string(),
        }
    }
}

/// The role and how it was chosen.
#[derive(Serialize, Debug)]
struct RoleInfo {
    guessed: bool,
    how: String,
    name: &'static str,
    override_with: &'static str,
}

/// The whole document. Fields are in alphabetical order.
#[derive(Serialize, Debug)]
struct Guide {
    rerun: String,
    role: RoleInfo,
    steps: Vec<Step>,
    version: &'static str,
}

/// What detection shows about this host.
pub(crate) struct Local {
    /// Names of the agents found, `claude` and `codex`.
    pub(crate) agents: Vec<&'static str>,
    /// Hooks and skill are installed and current for every agent found.
    pub(crate) setup_done: bool,
    /// Messages of the failed checks that doctor would also fail.
    pub(crate) problems: Vec<String>,
    server: String,
    broker_file: Option<String>,
}

fn role_name(role: GuideRole) -> &'static str {
    match role {
        GuideRole::Solo => "solo",
        GuideRole::Broker => "broker",
        GuideRole::Host => "host",
    }
}

/// Run `chattrs setup-guide`. It never fails.
pub async fn run(args: SetupGuideArgs) -> Result<(), RunError> {
    let local = detect().await;
    let (role, how, guessed) = choose_role(&args, &local);
    let steps = if role == GuideRole::Solo {
        solo_steps(&local)
    } else {
        swarm::steps(&args, role, &local)
    };
    let guide = Guide {
        rerun: rerun_command(&args),
        role: RoleInfo {
            guessed,
            how,
            name: role_name(role),
            override_with: "chattrs setup-guide --role solo|broker|host",
        },
        steps,
        version: env!("CARGO_PKG_VERSION"),
    };
    let text = if args.json { render_json(&guide) } else { render_text(&guide) };
    // A closed stdout ends the output; the exit code stays 0.
    let _ = crate::output::write_stdout(&text);
    Ok(())
}

/// Collect the local facts with doctor's checks. No `hello` is sent.
async fn detect() -> Local {
    let args = DoctorArgs { server: None, address: None, agent: None, project: None, token: None };
    let report = doctor::collect(&args).await;
    let home = hook::install::home().ok();
    let agents: Vec<&'static str> = home
        .iter()
        .flat_map(|home| setup::targets(home))
        .filter(|target| target.detected)
        .map(|target| target.name)
        .collect();
    let cwd = std::env::current_dir().unwrap_or_default();
    let config = config::load_from(home.as_deref(), &cwd).unwrap_or_default();
    let server = Settings::resolve(&process_env, &config).server;
    let loopback = autostart::is_loopback_server(&server);
    let failed: Vec<&doctor::Check> = report.checks().filter(|check| !check.pass).collect();
    // A loopback broker that is not running starts on demand.
    let problem = |check: &&&doctor::Check| !(loopback && check.name == "broker");
    let problems =
        failed.iter().filter(problem).map(|c| format!("{}: {}", c.name, c.detail)).collect();
    let setup_done = home.is_some()
        && report
            .checks()
            .filter(|check| matches!(check.name, "hooks" | "skill"))
            .all(|check| check.pass);
    let broker_file = home
        .as_deref()
        .and_then(|home| setup::broker::written_files(home).into_iter().find(|path| path.exists()))
        .map(|path| path.display().to_string());
    Local { agents, setup_done, problems, server, broker_file }
}

/// The role, how it was chosen, and whether it was a guess.
fn choose_role(args: &SetupGuideArgs, local: &Local) -> (GuideRole, String, bool) {
    if let Some(role) = args.role {
        return (role, "set by --role".to_string(), false);
    }
    if !args.hosts.is_empty() || args.broker_host.is_some() {
        let broker_here = args.broker_host.as_deref().is_some_and(swarm::is_local);
        let role = if broker_here { GuideRole::Broker } else { GuideRole::Host };
        return (role, "--hosts or --broker-host was given".to_string(), true);
    }
    if let Some(path) = &local.broker_file {
        return (GuideRole::Broker, format!("broker file {path} exists on this host"), true);
    }
    if !autostart::is_loopback_server(&local.server) {
        let how = format!("the server {} is not a loopback address", local.server);
        return (GuideRole::Host, how, true);
    }
    (GuideRole::Solo, "no remote server is configured".to_string(), true)
}

/// The command to run again, with the flags the user gave.
fn rerun_command(args: &SetupGuideArgs) -> String {
    let mut command = String::from("chattrs setup-guide");
    if let Some(role) = args.role {
        command.push_str(&format!(" --role {}", role_name(role)));
    }
    if !args.hosts.is_empty() {
        command.push_str(&format!(" --hosts {}", args.hosts.join(",")));
    }
    if let Some(host) = &args.broker_host {
        command.push_str(&format!(" --broker-host {host}"));
    }
    if args.json {
        command.push_str(" --json");
    }
    command
}

/// The steps for one machine with no swarm.
fn solo_steps(local: &Local) -> Vec<Step> {
    let mut steps = Vec::new();
    let setup_status = if local.setup_done { Status::Done } else { Status::Todo };
    if local.agents.is_empty() {
        steps.push(Step::new(
            "setup",
            Status::Blocked,
            Who::Human,
            None,
            "No agent found. Install Claude Code or Codex, then run `chattrs setup-guide` again.",
        ));
    } else {
        steps.push(Step::new(
            "setup",
            setup_status,
            Who::Agent,
            Some("chattrs setup"),
            "Installs the chattrs hooks and the skill for each agent found. Safe to run again.",
        ));
    }
    let after_setup = if local.setup_done { Status::Todo } else { Status::Blocked };
    if local.agents.contains(&"claude") {
        steps.push(Step::new(
            "claude-review",
            after_setup,
            Who::Human,
            None,
            "Claude Code may ask you to review the new hooks at its next start. Approve them.",
        ));
    }
    if local.agents.contains(&"codex") {
        steps.push(Step::new(
            "codex-trust",
            after_setup,
            Who::Human,
            None,
            "Open Codex and run /hooks to trust the chattrs hooks. chattrs cannot detect this.",
        ));
    }
    steps.push(doctor_step(local, after_setup));
    let launch = if local.agents.is_empty() { vec!["claude"] } else { local.agents.clone() };
    for agent in launch {
        let command = format!("chattrs exec {agent}");
        steps.push(Step::new(
            &format!("launch-{agent}"),
            after_setup,
            Who::Human,
            Some(&command),
            "Starts the agent on the swarm with its address and session. The first command starts a loopback broker.",
        ));
    }
    steps
}

fn doctor_step(local: &Local, status: Status) -> Step {
    let command = format!("chattrs doctor --as claude@{}", crate::cli::short_hostname());
    let mut why = String::from(
        "Checks that the broker accepts the token. The guide never sends a hello, so only \
         doctor can tell, and this step stays todo. Fix every FAIL line, then run doctor again.",
    );
    if !local.problems.is_empty() {
        why.push_str(&format!(" Failing now: {}.", local.problems.join("; ")));
    }
    Step::new("doctor", status, Who::Agent, Some(&command), &why)
}

fn render_json(guide: &Guide) -> String {
    let mut text = serde_json::to_string_pretty(guide).unwrap_or_default();
    text.push('\n');
    text
}

fn render_text(guide: &Guide) -> String {
    let role = &guide.role;
    let kind = if role.guessed { "guessed" } else { "chosen" };
    let mut text = format!(
        "chattrs setup-guide {} (read-only)\nrole: {} ({kind}: {}). Override: {}\n",
        guide.version, role.name, role.how, role.override_with
    );
    let total = guide.steps.len();
    for (index, step) in guide.steps.iter().enumerate() {
        let who = serde_json::to_value(step.who).ok();
        let who = who.as_ref().and_then(|value| value.as_str()).unwrap_or("");
        let status = serde_json::to_value(step.status).ok();
        let status = status.as_ref().and_then(|value| value.as_str()).unwrap_or("");
        text.push_str(&format!(
            "\nstep {} of {total}: {} [{status}] who: {who}\n",
            index + 1,
            step.id
        ));
        if let Some(command) = &step.command {
            text.push_str(&format!("  run: {command}\n"));
        }
        text.push_str(&format!("  why: {}\n", step.why));
    }
    text.push_str(&format!(
        "\nAgent: do each step marked `who: agent` that is `todo`, in order. Tell the user about \
         each `who: human` step. Ask the user for each `who: ask` step. After you run a step, \
         run `{}` again.\n",
        guide.rerun
    ));
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local(setup_done: bool, agents: &[&'static str]) -> Local {
        Local {
            agents: agents.to_vec(),
            setup_done,
            problems: Vec::new(),
            server: "127.0.0.1:4646".to_string(),
            broker_file: None,
        }
    }

    #[test]
    fn solo_steps_follow_setup_state() {
        let todo = solo_steps(&local(false, &["claude", "codex"]));
        let ids: Vec<&str> = todo.iter().map(|step| step.id.as_str()).collect();
        assert_eq!(
            ids,
            ["setup", "claude-review", "codex-trust", "doctor", "launch-claude", "launch-codex"]
        );
        assert_eq!(todo[0].status, Status::Todo);
        assert_eq!(todo[3].status, Status::Blocked);
        let done = solo_steps(&local(true, &["claude", "codex"]));
        assert_eq!(done[0].status, Status::Done);
        assert_eq!(done[3].status, Status::Todo);
    }

    #[test]
    fn no_agent_blocks_setup() {
        let steps = solo_steps(&local(true, &[]));
        assert_eq!(steps[0].status, Status::Blocked);
        assert_eq!(steps[0].who, Who::Human);
    }

    #[test]
    fn rerun_keeps_the_given_flags() {
        let args = SetupGuideArgs {
            hosts: vec!["a".to_string(), "b".to_string()],
            broker_host: Some("b".to_string()),
            json: true,
            ..SetupGuideArgs::default()
        };
        assert_eq!(rerun_command(&args), "chattrs setup-guide --hosts a,b --broker-host b --json");
    }
}
