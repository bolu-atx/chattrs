//! chattrs — distributed messaging for cross-agent coordination.
//!
//! A thin entry point: parse `cli`, then dispatch to `server` (the broker),
//! `client` (`send`, `reply`, `inbox`, `watch`, `ask`, `skill`), or
//! `autostart` (`stop`). `model` owns the wire types, `protocol` the
//! line-JSON framing, `store` the SQLite storage, and `output` every line
//! printed to stdout.
//!
//! Quick start:
//! ```text
//! chattrs serve --db /tmp/c.db
//! CHATTRS_ADDRESS=a@h chattrs send --to b@h "anyone there?"
//! ```

mod autostart;
mod backup;
mod cli;
mod client;
mod commands;
mod config;
mod doctor;
mod exec;
mod hook;
mod model;
mod output;
mod project;
mod protocol;
mod server;
mod session;
mod setup;
mod skill;
mod store;
mod tokens;

use clap::Parser;
use cli::{Args, Command};
use commands::{
    cmd_agents, cmd_ask, cmd_inbox, cmd_log, cmd_projects, cmd_reply, cmd_send, cmd_serve,
    cmd_skill, cmd_status, cmd_stop, cmd_watch,
};
use exec::cmd_exec;
use session::cmd_env;

/// Why a command stopped without finishing: a failure to report on stderr,
/// a closed stdout pipe, or an `ask` that timed out.
#[derive(Debug)]
pub(crate) enum RunError {
    Message(String),
    /// A command already printed its failures; exit 1 without a message.
    Reported,
    BrokenPipe,
    Timeout,
}

impl From<client::ClientError> for RunError {
    fn from(error: client::ClientError) -> RunError {
        RunError::Message(error.to_string())
    }
}

impl From<String> for RunError {
    fn from(message: String) -> RunError {
        RunError::Message(message)
    }
}

/// Load the config files and export them as variables. A `[tokens]` entry
/// replaces the plain `token_file` when it fits the address; an explicit
/// `CHATTRS_TOKEN` still wins over both.
fn load_config() -> Result<(), String> {
    let mut config = config::load()?;
    if std::env::var_os("CHATTRS_TOKEN").is_none() {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let address = config::address_hint(&args, |name| std::env::var(name).ok());
        config.select_token(address.as_deref())?;
    }
    config.apply_env();
    Ok(())
}

fn main() {
    // Config files fill the variables that the environment leaves unset,
    // before clap reads them. A bad file stops the command, except a hook,
    // which must never break the agent. This runs before the tokio runtime
    // starts, so no other thread can read the environment while it is set.
    // Doctor reads the config files itself, so it can report a bad one and
    // tell a `CHATTRS_TOKEN` override from a token file.
    let first = std::env::args().nth(1);
    if first.as_deref() != Some("doctor") {
        if let Err(error) = load_config() {
            eprintln!("chattrs: {error}");
            if first.as_deref() != Some("hook") {
                std::process::exit(1);
            }
        }
    }
    let args = Args::parse();
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("chattrs: cannot start the runtime: {error}");
            std::process::exit(1);
        }
    };
    let code = match runtime.block_on(run(args)) {
        Ok(()) => 0,
        Err(RunError::BrokenPipe) => 0,
        Err(RunError::Timeout) => 2,
        Err(RunError::Reported) => 1,
        Err(RunError::Message(error)) => {
            eprintln!("chattrs: {error}");
            1
        }
    };
    std::process::exit(code);
}

async fn run(args: Args) -> Result<(), RunError> {
    match args.command {
        Command::Serve(c) => cmd_serve(c).await,
        Command::Stop => cmd_stop(),
        Command::Send(c) => cmd_send(c).await,
        Command::Reply(c) => cmd_reply(c).await,
        Command::Watch(c) => cmd_watch(c).await,
        Command::Ask(c) => cmd_ask(c).await,
        Command::Inbox(c) => cmd_inbox(c).await,
        Command::Skill => cmd_skill(),
        Command::Env(c) => cmd_env(c),
        Command::Exec(c) => cmd_exec(c),
        Command::Setup(c) => setup::run(c).map_err(RunError::Message),
        Command::Backup(c) => backup::run(c),
        Command::Doctor(c) => doctor::run(c).await,
        Command::Log(c) => cmd_log(c).await,
        Command::Agents(c) => cmd_agents(c).await,
        Command::Projects(c) => cmd_projects(c).await,
        Command::Status(c) => cmd_status(c).await,
        Command::Hook(c) => {
            hook::run(c.event, &c.client).await;
            Ok(())
        }
        Command::Hooks(c) => hook::install::run(c.action).map_err(RunError::Message),
    }
}
