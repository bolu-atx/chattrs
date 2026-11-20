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
mod cli;
mod client;
mod config;
mod hook;
mod hook_install;
mod model;
mod output;
mod protocol;
mod server;
mod setup;
mod setup_broker;
mod setup_join;
mod skill;
mod store;
mod tokens;

use clap::Parser;
use cli::{Args, Command};
use store::Store;

/// Why a command stopped without finishing: a failure to report on stderr,
/// a closed stdout pipe, or an `ask` that timed out.
#[derive(Debug)]
pub(crate) enum RunError {
    Message(String),
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
    if let Err(error) = load_config() {
        eprintln!("chattrs: {error}");
        if std::env::args().nth(1).as_deref() != Some("hook") {
            std::process::exit(1);
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
        Err(RunError::Message(error)) => {
            eprintln!("chattrs: {error}");
            1
        }
    };
    std::process::exit(code);
}

/// Write one piece of command output to stdout. A closed pipe ends the
/// command quietly; any other failure is the command's error.
fn write(text: &str) -> Result<(), RunError> {
    output::write_stdout(text).map_err(|error| {
        if error.kind() == std::io::ErrorKind::BrokenPipe {
            RunError::BrokenPipe
        } else {
            RunError::Message(format!("write stdout: {error}"))
        }
    })
}

/// Build the one endpoint for a client command from its flags.
fn endpoint(client: &cli::ClientArgs) -> Result<client::Endpoint, RunError> {
    Ok(client::Endpoint {
        server: client.server.clone(),
        address: client.resolve_address()?,
        token: client.token.clone(),
    })
}

/// Print the unread messages waiting on this connection, flush stdout,
/// then ack what was printed. Every command runs this before its own
/// work, so the preamble never holds its own output; `inbox` is this
/// alone.
async fn deliver_unread(client: &mut client::Client, json: bool) -> Result<(), RunError> {
    let messages = client.unread().await?;
    if messages.is_empty() {
        return Ok(());
    }
    for message in &messages {
        if json {
            write(&output::message_json(message))?;
        } else {
            write(&output::message_text(message))?;
        }
    }
    let ids: Vec<u64> = messages.iter().map(|message| message.id).collect();
    client.ack(&ids).await?;
    Ok(())
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
        Command::Setup(c) => setup::run(c).map_err(RunError::Message),
        Command::Log(c) => cmd_log(c).await,
        Command::Agents(c) => cmd_agents(c).await,
        Command::Projects(c) => cmd_projects(c).await,
        Command::Status(c) => cmd_status(c).await,
        Command::Hook(c) => {
            hook::run(c.event, &c.client).await;
            Ok(())
        }
        Command::Hooks(c) => hook_install::run(c.action).map_err(RunError::Message),
    }
}

fn cmd_stop() -> Result<(), RunError> {
    let db = autostart::client_db_path();
    autostart::stop_auto_started(&db).map_err(RunError::Message)
}

async fn cmd_serve(c: cli::ServeArgs) -> Result<(), RunError> {
    let db = c.db.clone().unwrap_or_else(Store::default_path);
    server::serve(&c.listen, &db, c.token.clone(), c.tokens.as_deref())
        .await
        .map_err(RunError::Message)
}

async fn cmd_send(c: cli::SendArgs) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client)?;
    if c.to.is_none() && c.topic.is_none() {
        return Err(RunError::Message("send needs --to or --topic".to_string()));
    }
    let mut conn = client::connect_for_publish(&endpoint, "send").await?;
    deliver_unread(&mut conn, c.json).await?;
    let (id, known) = conn
        .publish(client::Outgoing {
            to: c.to.clone(),
            body: c.body(),
            reply_to: None,
            topic: c.topic.clone(),
        })
        .await?;
    if known == Some(false) {
        if let Some(to) = c.to.as_deref() {
            eprintln!("chattrs: warning: {to} has never connected; message {id} stored");
        }
    }
    if c.json {
        write(&output::sent_json(id))
    } else {
        write(&output::sent_text(id))
    }
}

async fn cmd_reply(c: cli::ReplyArgs) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client)?;
    let mut conn = client::connect_for_publish(&endpoint, "reply").await?;
    deliver_unread(&mut conn, c.json).await?;
    let (id, _) = conn
        .publish(client::Outgoing {
            to: None,
            body: c.body(),
            reply_to: Some(c.id),
            topic: c.topic.clone(),
        })
        .await?;
    if c.json {
        write(&output::sent_json(id))
    } else {
        write(&output::sent_text(id))
    }
}

async fn cmd_watch(c: cli::WatchArgs) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client)?;
    if let Some(bad) = c.topic.iter().find(|topic| !model::is_valid_topic(topic)) {
        return Err(RunError::Message(format!("topic is invalid: {bad}")));
    }
    let announcing = !c.topic.is_empty() || c.all;
    let mut watcher = if c.all {
        client::Watcher::observer(endpoint)
    } else {
        client::Watcher::new(endpoint, c.topic.clone())
    };
    loop {
        let message = watcher.next_message().await?;
        write(&output::message_json(&message))?;
        if announcing {
            continue;
        }
        match watcher.ack(message.id).await {
            Ok(_) => {}
            Err(error @ client::ClientError::Server { .. }) => return Err(error.into()),
            Err(error @ client::ClientError::Protocol(_)) => return Err(error.into()),
            Err(error @ client::ClientError::Timeout { .. }) => return Err(error.into()),
            Err(error) => {
                eprintln!("chattrs: watch failed: {error}, retrying in 1s");
                watcher.reset();
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn cmd_ask(c: cli::AskArgs) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client)?;
    let mut conn = client::Client::connect(&endpoint).await?;
    deliver_unread(&mut conn, c.json).await?;
    let (id, known) = conn
        .publish(client::Outgoing {
            to: Some(c.to.clone()),
            body: c.body(),
            reply_to: None,
            topic: c.topic.clone(),
        })
        .await?;
    if known == Some(false) {
        eprintln!("chattrs: warning: {} has never connected; message {id} stored", c.to);
    }
    match client::wait_for_reply(&endpoint, conn, id, c.timeout).await {
        Ok((mut conn, reply)) => {
            if c.json {
                write(&output::message_json(&reply))?;
            } else {
                write(&output::message_text(&reply))?;
            }
            conn.ack(&[reply.id]).await?;
            Ok(())
        }
        Err(client::ClientError::Timeout { id }) => {
            if c.json {
                write(&output::timeout_json(id))?;
            }
            eprintln!("timeout: no reply to {id}");
            Err(RunError::Timeout)
        }
        Err(error) => Err(error.into()),
    }
}

async fn cmd_inbox(c: cli::InboxArgs) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client)?;
    let mut conn = client::Client::connect(&endpoint).await?;
    deliver_unread(&mut conn, c.json).await
}

/// A fresh session id: 16 random bytes as 32 hex digits. Each
/// `RandomState` is seeded from the OS, so no crate is needed.
fn new_session_id() -> String {
    use std::hash::{BuildHasher as _, Hasher as _};
    let word = || std::collections::hash_map::RandomState::new().build_hasher().finish();
    format!("{:016x}{:016x}", word(), word())
}

fn cmd_env(c: cli::EnvArgs) -> Result<(), RunError> {
    if !model::is_valid_address(&c.address) {
        return Err(RunError::Message(format!("`{}` is not a user@host address", c.address)));
    }
    let mut pairs = vec![
        ("CHATTRS_SERVER", c.server),
        ("CHATTRS_ADDRESS", c.address),
        ("CHATTRS_SESSION", new_session_id()),
    ];
    let optional = [
        ("CHATTRS_PROJECT", c.project),
        ("CHATTRS_LEAD", c.lead),
        ("CHATTRS_TAGS", c.tags),
        ("CHATTRS_TOKEN", c.token.filter(|_| c.with_token)),
    ];
    pairs.extend(optional.into_iter().filter_map(|(name, value)| Some((name, value?))));
    write(&output::export_lines(&pairs))
}

fn cmd_skill() -> Result<(), RunError> {
    write(skill::text())
}

async fn cmd_log(c: cli::LogArgs) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client)?;
    if let Some(about) = c.about.as_deref() {
        if !model::is_valid_address(about) {
            return Err(RunError::Message(format!("address is invalid: {about}")));
        }
    }
    let mut conn = client::Client::connect(&endpoint).await?;
    deliver_unread(&mut conn, c.json).await?;
    let messages = match c.tail {
        Some(n) => conn.log_tail(c.about.clone(), n).await?,
        None => conn.log(c.about.clone()).await?,
    };
    if c.json {
        for message in &messages {
            write(&output::log_json(message))?;
        }
    } else {
        write(output::PREAMBLE)?;
        for message in &messages {
            write(&output::log_text(message))?;
        }
    }
    Ok(())
}

/// Report a state and always succeed: a failure is one stderr line, never
/// an exit code, so a hook calling `status` cannot break an agent.
async fn cmd_status(c: cli::StatusArgs) -> Result<(), RunError> {
    let result = match endpoint(&c.client) {
        Ok(endpoint) => {
            client::report_status(&endpoint, c.state.as_str()).await.map_err(|e| e.to_string())
        }
        Err(RunError::Message(message)) => Err(message),
        Err(_) => Err("cannot resolve the own address".to_string()),
    };
    if let Err(message) = result {
        eprintln!("chattrs: status not reported: {message}");
    }
    Ok(())
}

async fn cmd_agents(c: cli::AgentsArgs) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client)?;
    let mut conn = client::Client::connect(&endpoint).await?;
    deliver_unread(&mut conn, c.json).await?;
    let agents = conn.agents(c.all).await?;
    for agent in &agents {
        if c.json {
            write(&output::agent_json(agent))?;
        } else {
            write(&output::agent_text(agent))?;
        }
    }
    Ok(())
}

/// List the projects. The `*` observer behind it cannot hold an inbox, so
/// this command prints no unread messages first.
async fn cmd_projects(c: cli::ProjectsArgs) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client)?;
    for project in &client::projects(&endpoint).await? {
        if c.json {
            write(&output::project_json(project))?;
        } else {
            write(&output::project_text(project))?;
        }
    }
    Ok(())
}
