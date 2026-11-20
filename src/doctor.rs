//! chattrs — doctor (SETUP-3): `chattrs doctor` checks one host end to end.
//!
//! It prints one `ok` or `FAIL` line per check on stdout and exits 1 when
//! any check fails. It is read-only: it writes no file, never prints a
//! token, and never acks or reads a message. The token check says `hello`
//! as an observer, which the broker never records as an agent. A broker
//! with a token table refuses that hello for an agent or lead token, but
//! only after it checked the token, the project, and the address, so the
//! refusal still proves those three.

use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::cli::{DoctorArgs, DEFAULT_SERVER};
use crate::config::{self, Config};
use crate::model::{self, Inbound, Outbound};
use crate::{hook_install, protocol, setup, skill, RunError};

/// How long the connect and the `welcome` may each take.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

use crate::tokens::OBSERVER_REFUSAL as ROLE_REFUSAL;

/// Printed after the checks: no code path can see Codex's hook trust.
const CODEX_NOTE: &str = "note: Codex hook trust cannot be detected; if Codex hooks do not fire, \
                          run /hooks in Codex and trust them.";

/// What a probe `hello` got back.
enum Probe {
    Welcome { v: u32, project: Option<String>, observer: bool },
    Rejected { code: String, message: String },
}

/// The lines printed so far, as pass or fail.
#[derive(Default)]
struct Report {
    failed: bool,
}

impl Report {
    fn pass(&mut self, check: &str, detail: impl Display) {
        self.line("ok  ", check, detail);
    }

    fn fail(&mut self, check: &str, detail: impl Display) {
        self.failed = true;
        self.line("FAIL", check, detail);
    }

    fn line(&mut self, status: &str, check: &str, detail: impl Display) {
        // A closed stdout is not a check failure, so the write error is dropped.
        let _ = crate::output::write_stdout(&format!("{status} {check}: {detail}\n"));
    }
}

/// Run `chattrs doctor`. Every failure is printed; the error carries only
/// the exit code.
pub async fn run(args: DoctorArgs) -> Result<(), RunError> {
    let mut report = Report::default();
    match hook_install::home() {
        Ok(home) => {
            check_host(&mut report, &args, &home).await;
            check_agents(&mut report, &home);
        }
        Err(error) => report.fail("config", format!("{error}; set HOME")),
    }
    let _ = crate::output::write_stdout(&format!("{CODEX_NOTE}\n"));
    if report.failed {
        return Err(RunError::Reported);
    }
    Ok(())
}

/// Config, token map, reachability, token acceptance, and protocol.
async fn check_host(report: &mut Report, args: &DoctorArgs, home: &Path) {
    let loaded = check_config(report, home);
    let config_ok = loaded.is_some();
    let config = loaded.unwrap_or_default();
    check_token_map(report, &config);
    let server = args.server.clone().or_else(|| config.server.clone());
    let server = server.unwrap_or_else(|| DEFAULT_SERVER.to_string());
    let reachable = check_reachable(report, &server).await;
    let Some(address) = address_of(args) else {
        report.fail("address", "not set; pass --as or set CHATTRS_ADDRESS or CHATTRS_AGENT");
        return not_checked(report, "no address");
    };
    let override_token = args.token.as_deref().filter(|token| !token.is_empty());
    check_pick(report, &config, &address, override_token.is_some());
    let reason = if !config_ok {
        "config failed"
    } else if !reachable {
        "broker unreachable"
    } else {
        return check_probes(report, args, &config, &server, &address).await;
    };
    not_checked(report, reason);
}

fn not_checked(report: &mut Report, reason: &str) {
    report.fail("token", format!("not checked, {reason}"));
    report.fail("protocol", format!("not checked, {reason}"));
}

/// The own address: `--as`, else `<agent>@<hostname>`.
fn address_of(args: &DoctorArgs) -> Option<String> {
    let blank = |value: &&String| !value.is_empty();
    let host = |agent: &String| format!("{agent}@{}", crate::cli::short_hostname());
    args.address
        .as_ref()
        .filter(blank)
        .cloned()
        .or_else(|| args.agent.as_ref().filter(blank).map(host))
}

/// Parse both config files. Returns the config when they parse.
fn check_config(report: &mut Report, home: &Path) -> Option<Config> {
    let cwd = std::env::current_dir().unwrap_or_default();
    match config::load_from(Some(home), &cwd) {
        Ok(config) => {
            let user = home.join(".config/chattrs/config.toml");
            if user.is_file() {
                report.pass("config", format!("{} parses", user.display()));
            } else {
                report.pass("config", format!("{} not found, defaults apply", user.display()));
            }
            match config::find_repo_file(&cwd) {
                Some(repo) => report.pass("config", format!("{} parses", repo.display())),
                None => report.pass("config", "no .chattrs.toml in this repo"),
            }
            Some(config)
        }
        Err(error) => {
            report.fail("config", format!("{error}; fix that line, then run doctor again"));
            None
        }
    }
}

/// One line for the plain `token_file` and one per `[tokens]` entry: the
/// file it names, and whether that file exists with mode 0600.
fn check_token_map(report: &mut Report, config: &Config) {
    let plain = config.token_file.iter().map(|path| ("token_file", path));
    let mapped = config.token_map.iter().map(|(pattern, path)| (pattern.as_str(), path));
    for (identity, path) in plain.chain(mapped) {
        match token_file_problem(path) {
            None => report.pass("token-map", format!("{identity} -> {}", path.display())),
            Some(problem) => {
                report.fail("token-map", format!("{identity} -> {}: {problem}", path.display()))
            }
        }
    }
}

/// Why a token file is unusable, or `None` when it exists and is private.
fn token_file_problem(path: &Path) -> Option<String> {
    let meta = match std::fs::metadata(path) {
        Ok(meta) => meta,
        Err(error) => return Some(format!("{error}; create it or run chattrs setup --join")),
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o177 != 0 {
            return Some(format!("mode {mode:o} is wider than 600; run chmod 600"));
        }
    }
    let _ = meta;
    None
}

/// Where the token for `address` comes from.
fn token_source(config: &Config, address: &str) -> Option<PathBuf> {
    let mapped = config.mapped_token_file(address).map(Path::to_path_buf);
    mapped.or_else(|| config.token_file.clone())
}

fn check_pick(report: &mut Report, config: &Config, address: &str, env_override: bool) {
    if env_override {
        return report.pass(
            "token-pick",
            format!("{address} uses CHATTRS_TOKEN, which overrides the config"),
        );
    }
    match token_source(config, address) {
        Some(path) => report.pass("token-pick", format!("{address} uses {}", path.display())),
        None => report.pass("token-pick", format!("{address} has no token configured")),
    }
}

async fn check_reachable(report: &mut Report, server: &str) -> bool {
    match connect(server).await {
        Ok(_) => {
            report.pass("broker", format!("{server} accepts connections"));
            true
        }
        Err(error) => {
            report.fail("broker", format!("{error}; start the broker or fix `server`"));
            false
        }
    }
}

async fn connect(server: &str) -> Result<TcpStream, String> {
    let secs = PROBE_TIMEOUT.as_secs();
    match tokio::time::timeout(PROBE_TIMEOUT, TcpStream::connect(server)).await {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(error)) => Err(format!("connect {server}: {error}")),
        Err(_) => Err(format!("connect {server}: timed out after {secs}s")),
    }
}

/// Say an observer `hello` and read the answer. Nothing else is sent.
async fn probe(
    server: &str,
    address: &str,
    token: Option<&str>,
    project: Option<&str>,
) -> Result<Probe, String> {
    let (read_half, mut write_half) = connect(server).await?.into_split();
    let hello = Inbound::Hello {
        v: model::PROTOCOL_VERSION,
        address: address.to_string(),
        token: token.map(str::to_string),
        tags: Vec::new(),
        observer: true,
        state: None,
        project: project.map(str::to_string),
        session: None,
    };
    let line = protocol::encode(&hello)?;
    write_half.write_all(line.as_bytes()).await.map_err(|error| format!("send hello: {error}"))?;
    let mut answer = String::new();
    let mut reader = BufReader::new(read_half);
    let read = reader.read_line(&mut answer);
    match tokio::time::timeout(PROBE_TIMEOUT, read).await {
        Err(_) => return Err("the broker sent no welcome in time".to_string()),
        Ok(Err(error)) => return Err(format!("read welcome: {error}")),
        Ok(Ok(0)) => return Err("the broker closed the connection without a welcome".to_string()),
        Ok(Ok(_)) => {}
    }
    match protocol::decode(&answer)? {
        Outbound::Welcome { v, project, observer, .. } => {
            Ok(Probe::Welcome { v, project, observer })
        }
        Outbound::Error { code, message } => Ok(Probe::Rejected { code, message }),
        _ => Err("the broker sent an unexpected frame instead of welcome".to_string()),
    }
}

/// Probe the current address and every concrete `[tokens]` address. A glob
/// cannot be probed, since no address stands for it.
async fn check_probes(
    report: &mut Report,
    args: &DoctorArgs,
    config: &Config,
    server: &str,
    address: &str,
) {
    let project = args.project.clone().filter(|p| !p.trim().is_empty()).or(config.project.clone());
    let own_token = match args.token.clone().filter(|token| !token.is_empty()) {
        Some(token) => Some(token),
        None => token_source(config, address).and_then(|p| config::read_token_file(&p).ok()),
    };
    let first = probe(server, address, own_token.as_deref(), project.as_deref()).await;
    report_protocol(report, &first);
    report_token(report, address, &first, project.as_deref(), config.project.as_deref());
    for (pattern, path) in config.token_map.iter().filter(|(pattern, _)| pattern != address) {
        if pattern.contains('*') {
            report.pass(
                "token",
                format!("{pattern}: skipped, a glob pattern has no address to test"),
            );
            continue;
        }
        let Ok(token) = config::read_token_file(path) else {
            report.fail("token", format!("{pattern}: token file unreadable, see token-map"));
            continue;
        };
        let result = probe(server, pattern, Some(&token), project.as_deref()).await;
        report_token(report, pattern, &result, project.as_deref(), config.project.as_deref());
    }
}

fn report_protocol(report: &mut Report, result: &Result<Probe, String>) {
    let ours = model::PROTOCOL_VERSION;
    match result {
        Ok(Probe::Welcome { v, .. }) if *v == ours => report.pass("protocol", format!("v{ours}")),
        Ok(Probe::Welcome { v, .. }) => report.fail(
            "protocol",
            format!("the broker speaks v{v}, this client speaks v{ours}; upgrade the older side"),
        ),
        Ok(Probe::Rejected { code, message }) if code == "bad_version" => report.fail(
            "protocol",
            format!("{message}; this client speaks v{ours}; upgrade the older side"),
        ),
        // The broker checks the version before the address, project, and role.
        Ok(Probe::Rejected { code, .. }) if code == "forbidden" => {
            report.pass("protocol", format!("v{ours}"))
        }
        Ok(Probe::Rejected { code, .. }) => {
            report.fail("protocol", format!("not checked, the broker refused the hello ({code})"))
        }
        Err(error) => report.fail("protocol", format!("not checked, {error}")),
    }
}

/// Report one probe. `sent` is the project the hello asked for and `repo`
/// the project of the repo `.chattrs.toml`.
fn report_token(
    report: &mut Report,
    who: &str,
    result: &Result<Probe, String>,
    sent: Option<&str>,
    repo: Option<&str>,
) {
    let sent = sent.unwrap_or(model::DEFAULT_PROJECT);
    let (project, how) = match result {
        Err(error) => return report.fail("token", format!("{who}: {error}")),
        Ok(Probe::Welcome { observer: false, .. }) => {
            return report.fail(
                "token",
                format!("{who}: the broker ignored the observer hello; upgrade the broker"),
            )
        }
        Ok(Probe::Welcome { project, .. }) => (
            project.as_deref().unwrap_or(model::DEFAULT_PROJECT),
            "observer hello accepted; the welcome carries no role",
        ),
        Ok(Probe::Rejected { code, message }) if code == "forbidden" && message == ROLE_REFUSAL => {
            (sent, "an agent or lead token; the welcome carries no role")
        }
        Ok(Probe::Rejected { code, message }) if code == "bad_token" => {
            return report.fail(
                "token",
                format!("{who}: {message}; check that the token file matches the broker, or run chattrs setup --join"),
            )
        }
        Ok(Probe::Rejected { code, message }) if code == "forbidden" => {
            return report.fail(
                "token",
                format!("{who}: {message}; check the [tokens] entry, the address, and the project"),
            )
        }
        Ok(Probe::Rejected { code, message }) => {
            return report.fail("token", format!("{who}: {code}: {message}"))
        }
    };
    match repo.filter(|repo| *repo != project) {
        Some(repo) => report.fail(
            "token",
            format!("{who}: the broker put this connection in project {project}, but .chattrs.toml says {repo}"),
        ),
        None => report.pass("token", format!("{who}: project {project}; {how}; no agent was recorded")),
    }
}

/// Hooks and skill for each agent that setup would find.
fn check_agents(report: &mut Report, home: &Path) {
    let targets = setup::targets(home);
    let found: Vec<&setup::Target> = targets.iter().filter(|target| target.detected).collect();
    if found.is_empty() {
        report.pass("hooks", "no agent found, nothing to check");
        report.pass("skill", "no agent found, nothing to check");
    }
    for target in found {
        match hook_install::missing_hooks(target.agent) {
            Ok((path, missing)) if missing.is_empty() => {
                report.pass("hooks", format!("{}: installed in {}", target.name, path.display()))
            }
            Ok((path, missing)) => report.fail(
                "hooks",
                format!(
                    "{}: {} lacks {}; run chattrs setup",
                    target.name,
                    path.display(),
                    missing.join(", ")
                ),
            ),
            Err(error) => report.fail("hooks", format!("{}: {error}", target.name)),
        }
        let path = target.skills_dir.join("chattrs/SKILL.md");
        match std::fs::read_to_string(&path) {
            Ok(text) if text == skill::text() => report
                .pass("skill", format!("{}: {} matches this binary", target.name, path.display())),
            Ok(_) => report.fail(
                "skill",
                format!("{}: {} is outdated; run chattrs setup", target.name, path.display()),
            ),
            Err(_) => report.fail(
                "skill",
                format!("{}: {} not installed; run chattrs setup", target.name, path.display()),
            ),
        }
    }
}
