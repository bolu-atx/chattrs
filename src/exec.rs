//! chattrs — exec: start an agent on the swarm.

pub(crate) mod host;

use crate::cli;
use crate::commands::write;
use crate::model;
use crate::session::{session_pairs, Session};
use crate::settings::{Settings, TokenSource};
use crate::RunError;

/// The default address of `chattrs exec PROGRAM`: `<basename>@<host>`.
pub(crate) fn exec_default_address(program: &str) -> String {
    let base = std::path::Path::new(program).file_name().and_then(|n| n.to_str()).unwrap_or("");
    format!("{base}@{}", cli::short_hostname())
}

/// Run the program with the `env` pairs. The token stays out of the
/// program's environment, so no tool it runs can print it: each chattrs call
/// in the program reads the token from the config, as the hooks do. An
/// explicit `CHATTRS_TOKEN` in the parent env passes through unchanged.
/// On Unix this replaces the chattrs process.
pub(crate) fn cmd_exec(c: cli::ExecArgs, settings: &Settings) -> Result<(), RunError> {
    if c.host.is_some() {
        return cmd_exec_host(c);
    }
    let program = c.command[0].clone();
    // Only a token the user exported passes on; exec never adds one.
    let has_token = settings.token_source == TokenSource::Env
        && settings.token.as_deref().is_some_and(|token| !token.is_empty());
    let address = match c.address {
        Some(address) => address,
        None => {
            let address = exec_default_address(&program);
            if !model::is_valid_address(&address) {
                return Err(RunError::Message(format!(
                    "`{address}` is not a user@host address; pass --as ADDRESS"
                )));
            }
            address
        }
    };
    let cwd = std::env::current_dir().unwrap_or_default();
    let session = Session {
        address,
        project: settings.project(c.project.as_deref(), &cwd),
        lead: c.lead,
        server: c.server,
        tags: c.tags,
        token: None,
    };
    let (pairs, _) = session_pairs(session, false)?;
    let address = pairs.iter().find(|(name, _)| *name == "CHATTRS_ADDRESS").map(|p| p.1.clone());
    let mut command = std::process::Command::new(&program);
    command.args(&c.command[1..]).envs(pairs);
    // A config token never enters our env, so the program never gets it.
    if settings.token_source != TokenSource::Config && !has_token {
        eprintln!("chattrs: no token for {}; the broker may refuse", address.unwrap_or_default());
    }
    run_program(command, &program)
}

/// `exec --host`: run `chattrs exec` on the remote host over the system
/// `ssh`. Only `--as`, `--lead` (when typed), the program, and its args
/// go over. A detached start waits for ssh and prints how to attach and stop.
fn cmd_exec_host(c: cli::ExecArgs) -> Result<(), RunError> {
    let host = c.host.as_deref().unwrap_or_default();
    if host.starts_with('-') {
        return Err(RunError::Message(format!("`{host}` is not a host name")));
    }
    // `--lead` also reads CHATTRS_LEAD; only a typed flag goes to the remote.
    // Only flags before the program count: its own args may say `--lead`.
    let lead_typed = std::env::args()
        .skip(1)
        .take_while(|a| a != "--" && *a != c.command[0])
        .any(|a| a == "--lead" || a.starts_with("--lead="));
    let lead = c.lead.as_deref().filter(|_| lead_typed);
    let name = c.detach.then(|| {
        c.session_name
            .clone()
            .unwrap_or_else(|| host::session_name(c.address.as_deref(), &c.command[0]))
    });
    let argv = host::remote_command(
        host,
        c.cwd.as_deref(),
        c.address.as_deref(),
        lead,
        &c.command,
        name.as_deref(),
    );
    let mut command = std::process::Command::new(&argv[0]);
    command.args(&argv[1..]);
    let Some(name) = name else {
        return run_program(command, "ssh");
    };
    let status =
        command.status().map_err(|error| RunError::Message(format!("cannot run ssh: {error}")))?;
    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
    write(&host::detach_hints(host, &name))
}

#[cfg(unix)]
fn run_program(mut command: std::process::Command, program: &str) -> Result<(), RunError> {
    use std::os::unix::process::CommandExt as _;
    // `exec` returns only when it fails.
    let error = command.exec();
    eprintln!("chattrs: cannot run {program}: {error}");
    std::process::exit(127)
}

#[cfg(windows)]
fn run_program(mut command: std::process::Command, program: &str) -> Result<(), RunError> {
    let status = command
        .status()
        .map_err(|error| RunError::Message(format!("cannot run {program}: {error}")))?;
    std::process::exit(status.code().unwrap_or(1))
}
