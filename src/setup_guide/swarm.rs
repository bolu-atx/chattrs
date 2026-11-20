//! The swarm steps of the setup guide: ask first, then per host. The guide
//! prints ssh commands and never runs them.

use crate::cli::{GuideRole, SetupGuideArgs};
use crate::setup_guide::{Local, Status, Step, Who};

/// Where the broker host keeps the join files. The remote shell expands `~`.
const JOIN_DIR: &str = "~/.config/chattrs/joins";

const RELEASES: &str = "https://github.com/bolu-atx/chattrs/releases/download";

/// True when `host` names this machine, so no ssh is needed.
pub(super) fn is_local(host: &str) -> bool {
    host == "localhost" || host == crate::cli::short_hostname()
}

/// A command that runs on `host`: as written when local, else over ssh. A
/// command with a shell character is quoted so the remote shell reads it.
fn on(host: &str, command: &str) -> String {
    if is_local(host) {
        return command.to_string();
    }
    if command.contains(['~', '|', '<', '&']) {
        return format!("ssh {host} '{}'", command.replace('\'', "'\\''"));
    }
    format!("ssh {host} {command}")
}

/// The ask steps for what the flags leave open, or the full plan.
pub(super) fn steps(args: &SetupGuideArgs, role: GuideRole, local: &Local) -> Vec<Step> {
    let local_name = crate::cli::short_hostname();
    let broker = match (&args.broker_host, role) {
        (Some(host), _) => Some(host.clone()),
        (None, GuideRole::Broker) => Some(local_name),
        (None, _) => None,
    };
    let mut asks = Vec::new();
    if args.hosts.is_empty() {
        asks.push(Step::new(
            "ask-hosts",
            Status::Todo,
            Who::Ask,
            None,
            "Ask the user: Which hosts should join (ssh names from ~/.ssh/config)? If none, \
             the answer is solo: run `chattrs setup-guide --role solo`. Otherwise run \
             `chattrs setup-guide --hosts h1,h2` with the answer.",
        ));
    }
    if broker.is_none() {
        asks.push(Step::new(
            "ask-broker-host",
            Status::Todo,
            Who::Ask,
            None,
            "Ask the user: Which always-on host runs the broker? A laptop sleeps and the swarm \
             stops. Run `chattrs setup-guide --broker-host H` with the answer, and keep the \
             other flags.",
        ));
    }
    match broker {
        Some(broker) if asks.is_empty() => plan(&args.hosts, &broker, local),
        _ => asks,
    }
}

/// Everything per host, in order, once the hosts and the broker are known.
fn plan(hosts: &[String], broker: &str, local: &Local) -> Vec<Step> {
    let mut members: Vec<&str> = Vec::new();
    if !hosts.iter().any(|host| host == broker) {
        members.push(broker);
    }
    members.extend(hosts.iter().map(String::as_str));
    let mut steps = Vec::new();
    for host in &members {
        steps.extend(prepare_host(host, local));
    }
    steps.extend(broker_steps(hosts, broker, local));
    for host in hosts {
        steps.push(join_step(hosts, broker, host));
    }
    for host in &members {
        steps.extend(finish_host(host, broker));
    }
    steps
}

/// Reach the host, check chattrs, install if needed, and run `setup`.
fn prepare_host(host: &str, local: &Local) -> Vec<Step> {
    let mut steps = Vec::new();
    if !is_local(host) {
        steps.extend(reach_and_install(host));
    }
    let setup_status = match (is_local(host), local.setup_done) {
        (true, true) => Status::Done,
        _ => Status::Todo,
    };
    steps.push(Step::new(
        &format!("setup-{host}"),
        setup_status,
        Who::Agent,
        Some(&on(host, "chattrs setup")),
        "Installs the chattrs hooks and the skill on this host. Safe to run again. Then run \
         `chattrs setup-guide` again.",
    ));
    steps
}

fn reach_and_install(host: &str) -> Vec<Step> {
    let version = env!("CARGO_PKG_VERSION");
    let sh = format!(
        "curl --proto '=https' --tlsv1.2 -LsSf {RELEASES}/v{version}/chattrs-installer.sh | sh"
    );
    let ps = format!(
        "powershell -ExecutionPolicy ByPass -c \"irm {RELEASES}/v{version}/chattrs-installer.ps1 | iex\""
    );
    let install_why = format!(
        "Run it only after the user said yes and `version-{host}` showed chattrs missing or \
         older than {version}. Never use sudo. Pick the installer from `ssh {host} uname -sm`: \
         Darwin arm64, Linux x86_64, and Linux aarch64 use the sh installer above. Windows uses \
         this one on the host: {ps}. Intel macOS has no release build: use `cargo install --git \
         https://github.com/bolu-atx/chattrs --tag v{version}`. Then run `chattrs setup-guide` \
         again."
    );
    vec![
        Step::new(
            &format!("ssh-{host}"),
            Status::Todo,
            Who::Agent,
            Some(&format!("ssh -o BatchMode=yes {host} true")),
            &format!(
                "Must succeed with no prompt. If it fails, do `fix-ssh-{host}`. Run `chattrs \
                 setup-guide` again afterward."
            ),
        ),
        Step::new(
            &format!("fix-ssh-{host}"),
            Status::Todo,
            Who::Human,
            None,
            &format!(
                "Only if `ssh -o BatchMode=yes {host} true` fails: fix ssh to {host} (key login, \
                 an entry in ~/.ssh/config). Then run `chattrs setup-guide` again."
            ),
        ),
        Step::new(
            &format!("version-{host}"),
            Status::Todo,
            Who::Agent,
            Some(&format!("ssh {host} chattrs --version")),
            &format!(
                "If it prints chattrs {version}, skip the install steps for {host}. If chattrs is \
                 missing or older than {version}, do `ask-install-{host}` then `install-{host}`. \
                 A non-interactive ssh shell may not have the install directory in PATH."
            ),
        ),
        Step::new(
            &format!("ask-install-{host}"),
            Status::Todo,
            Who::Ask,
            None,
            &format!(
                "Only if chattrs is missing or older than {version} on {host}: ask the user: \
                 Install chattrs {version} on {host}? Do not install without a yes."
            ),
        ),
        Step::new(
            &format!("install-{host}"),
            Status::Todo,
            Who::Agent,
            Some(&on(host, &sh)),
            &install_why,
        ),
    ]
}

/// Issue the tokens on the broker host.
fn broker_steps(hosts: &[String], broker: &str, local: &Local) -> Vec<Step> {
    let command = format!(
        "chattrs setup --broker --hosts {} --join-dir {JOIN_DIR} --enable",
        hosts.join(",")
    );
    let done = is_local(broker) && local.broker_file.is_some();
    let status = if done { Status::Done } else { Status::Todo };
    vec![
        Step::new(
            "broker",
            status,
            Who::Agent,
            Some(&on(broker, &command)),
            &format!(
                "Run on {broker}. It writes the tokens file and the service unit, and puts each \
                 join string in a 0600 file under {JOIN_DIR}. It prints paths only. It never \
                 overwrites: if it says the tokens file exists, the broker is already set up and \
                 you skip this step. Then run `chattrs setup-guide` again."
            ),
        ),
        Step::new(
            "approve-broker",
            Status::Todo,
            Who::Human,
            None,
            &format!(
                "Approve any service or port prompt on {broker}, for example a firewall or login \
                 item dialog for the chattrs broker."
            ),
        ),
    ]
}

/// Send one host its join string over stdin, never as an argument.
fn join_step(hosts: &[String], broker: &str, host: &str) -> Step {
    let mut files = vec![format!("{JOIN_DIR}/{host}.join")];
    if hosts.first().map(String::as_str) == Some(host) {
        files.push(format!("{JOIN_DIR}/lead.join"));
    }
    let target = on(host, "chattrs setup --join -");
    let command = match (is_local(broker), files.len()) {
        (true, 1) => format!("{target} < {}", files[0]),
        (true, _) => format!("cat {} | {target}", files.join(" ")),
        (false, _) => format!("{} | {target}", on(broker, &format!("cat {}", files.join(" ")))),
    };
    Step::new(
        &format!("join-{host}"),
        Status::Todo,
        Who::Agent,
        Some(&command),
        &format!(
            "Gives {host} its token. The join string travels on stdin and never appears in an \
             argument or on screen. Do not print the join file. Then run `chattrs setup-guide` \
             again."
        ),
    )
}

/// The doctor run and the human steps for one host.
fn finish_host(host: &str, broker: &str) -> Vec<Step> {
    let address = format!("claude@{host}");
    let doctor = on(host, &format!("chattrs doctor --as {address}"));
    let server =
        if host == broker { "" } else { " Doctor needs the broker reachable from this host." };
    vec![
        Step::new(
            &format!("doctor-{host}"),
            Status::Todo,
            Who::Agent,
            Some(&doctor),
            &format!(
                "Checks config, broker, token, hooks, and skill on {host}. Fix every FAIL line.{server} \
                 Then run `chattrs setup-guide` again."
            ),
        ),
        Step::new(
            &format!("trust-{host}"),
            Status::Todo,
            Who::Human,
            None,
            &format!(
                "On {host}: if Codex is used there, open it and run /hooks to trust the chattrs \
                 hooks. Claude Code may ask you to review the hooks."
            ),
        ),
    ]
}
