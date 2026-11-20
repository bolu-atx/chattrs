//! CLI surface: argument parsing only. No networking, no state.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Broker address clients connect to and the server listens on by default.
pub const DEFAULT_SERVER: &str = "127.0.0.1:4646";

/// Distributed messaging for cross-agent coordination.
#[derive(Parser, Debug)]
#[command(name = "chattrs", version)]
pub struct Args {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the message broker.
    Serve(ServeArgs),
    /// Stop the auto-started broker named by the pidfile next to the DB.
    Stop,
    /// Post a message to an address and print its id.
    Send(SendArgs),
    /// Answer the message with an id and print the new id.
    Reply(ReplyArgs),
    /// Read the own inbox and ack what was printed.
    Inbox(InboxArgs),
    /// Stream new messages as JSON lines, acking what is printed.
    Watch(WatchArgs),
    /// Post a question and block until its reply arrives.
    Ask(AskArgs),
    /// List known agent addresses and when each was last seen.
    Agents(AgentsArgs),
    /// Show the full message history, oldest first.
    Log(LogArgs),
    /// Report the own state to the broker. Best effort: always exits 0.
    Status(StatusArgs),
    /// Print the bundled agent skill to stdout.
    Skill,
    /// Run as an agent hook: read the hook JSON on stdin, print hook JSON.
    Hook(HookArgs),
    /// Install or remove the chattrs hooks in Claude Code and Codex configs.
    Hooks(HooksArgs),
}

/// The agent events a hook serves, in the kebab-case form of the Claude
/// Code and Codex hook event names.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum HookEvent {
    SessionStart,
    UserPromptSubmit,
    PostToolUse,
    Stop,
}

#[derive(clap::Args, Debug)]
pub struct HookArgs {
    /// The hook event that fired.
    #[arg(value_enum)]
    pub event: HookEvent,

    #[command(flatten)]
    pub client: ClientArgs,
}

#[derive(clap::Args, Debug)]
pub struct HooksArgs {
    #[command(subcommand)]
    pub action: HooksAction,
}

#[derive(Subcommand, Debug)]
pub enum HooksAction {
    /// Add the chattrs hooks to the agent config files.
    Install(HooksTarget),
    /// Remove only the chattrs hooks from the agent config files.
    Remove(HooksTarget),
}

/// Which agents' config files `hooks install` and `hooks remove` edit.
#[derive(clap::Args, Debug)]
pub struct HooksTarget {
    /// Agent to edit.
    #[arg(long, value_enum, default_value = "all")]
    pub agent: HooksAgent,

    /// Edit the config files in the current directory instead of the user's.
    #[arg(long)]
    pub project: bool,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum HooksAgent {
    Claude,
    Codex,
    All,
}

#[derive(clap::Args, Debug)]
pub struct ServeArgs {
    /// Address to listen on.
    #[arg(long, default_value = DEFAULT_SERVER)]
    pub listen: String,

    /// SQLite database path (created with parent directories as needed).
    #[arg(long, env = "CHATTRS_DB")]
    pub db: Option<PathBuf>,

    /// Token for the broker. A bind outside loopback refuses to start
    /// without one.
    #[arg(long, env = "CHATTRS_TOKEN")]
    pub token: Option<String>,
}

/// Connection flags shared by the client commands.
#[derive(clap::Args, Debug)]
pub struct ClientArgs {
    /// Broker address to connect to.
    #[arg(long, default_value = DEFAULT_SERVER, env = "CHATTRS_SERVER")]
    pub server: String,

    /// Own address, as `user@host`. Without it, the address defaults to
    /// `<agent>@<hostname>` from `--agent`.
    #[arg(long, env = "CHATTRS_ADDRESS")]
    pub address: Option<String>,

    /// Agent name for the default address, as in `<agent>@<hostname>`.
    #[arg(long, env = "CHATTRS_AGENT")]
    pub agent: Option<String>,

    /// Token for the broker (checked once multi-host binds need it).
    #[arg(long, env = "CHATTRS_TOKEN")]
    pub token: Option<String>,
}

impl ClientArgs {
    /// Resolve the own address: `--address` wins, else `<agent>@<hostname>`.
    pub fn resolve_address(&self) -> Result<String, String> {
        if let Some(address) = self.address.as_deref() {
            return Ok(address.to_string());
        }
        let Some(agent) = self.agent.as_deref() else {
            return Err("set CHATTRS_ADDRESS or CHATTRS_AGENT".to_string());
        };
        Ok(format!("{agent}@{hostname}", hostname = short_hostname()))
    }
}

/// This machine's hostname up to the first `.`, lowercased.
pub fn short_hostname() -> String {
    let name = gethostname::gethostname().to_string_lossy().into_owned();
    name.split('.').next().unwrap_or("").to_lowercase()
}

#[derive(clap::Args, Debug)]
pub struct SendArgs {
    #[command(flatten)]
    pub client: ClientArgs,

    /// Recipient address, as `user@host`. Without it, the message is an
    /// announcement to `--topic` instead of a direct message.
    #[arg(long)]
    pub to: Option<String>,

    /// Topic slug naming the workstream this message belongs to. With no
    /// `--to`, the message is an announcement to this room: any agent
    /// watching the topic hears it.
    #[arg(long)]
    pub topic: Option<String>,

    /// Message text. With no text, the body is read from stdin.
    #[arg(value_name = "MESSAGE")]
    pub message: Vec<String>,

    /// Print the `sent` frame as JSON instead of the bare id.
    #[arg(long)]
    pub json: bool,
}

/// The body to publish: joined args, or stdin when no args are given.
fn join_body(parts: &[String]) -> String {
    if !parts.is_empty() {
        return parts.join(" ");
    }
    use std::io::Read;
    let mut body = String::new();
    let _ = std::io::stdin().read_to_string(&mut body);
    body.trim_end().to_string()
}

impl SendArgs {
    /// The body to publish: joined args, or stdin when no args are given.
    pub fn body(&self) -> String {
        join_body(&self.message)
    }
}

#[derive(clap::Args, Debug)]
pub struct ReplyArgs {
    #[command(flatten)]
    pub client: ClientArgs,

    /// Id of the message to answer.
    pub id: u64,

    /// Topic slug naming the workstream this reply belongs to. Without
    /// it, the reply inherits the topic of the message it answers.
    #[arg(long)]
    pub topic: Option<String>,

    /// Message text. With no text, the body is read from stdin.
    #[arg(value_name = "MESSAGE")]
    pub message: Vec<String>,

    /// Print the `sent` frame as JSON instead of the bare id.
    #[arg(long)]
    pub json: bool,
}

impl ReplyArgs {
    /// The body to publish: joined args, or stdin when no args are given.
    pub fn body(&self) -> String {
        join_body(&self.message)
    }
}

#[derive(clap::Args, Debug)]
pub struct WatchArgs {
    #[command(flatten)]
    pub client: ClientArgs,

    /// Topic room to watch instead of the own inbox. Repeat to watch
    /// several rooms; the broker replays each room's recent
    /// announcements, then each new one.
    #[arg(long)]
    pub topic: Vec<String>,

    /// Observe all swarm traffic, direct messages and announcements, as an
    /// observer that is not an agent. Never acks.
    #[arg(long, conflicts_with = "topic")]
    pub all: bool,
}

#[derive(clap::Args, Debug)]
pub struct AskArgs {
    #[command(flatten)]
    pub client: ClientArgs,

    /// Recipient address, as `user@host`.
    #[arg(long)]
    pub to: String,

    /// Topic slug naming the workstream this question belongs to.
    #[arg(long)]
    pub topic: Option<String>,

    /// Wait this many seconds for the reply. Zero waits with no limit.
    #[arg(long, default_value_t = 600)]
    pub timeout: u64,

    /// Print the reply as one JSON `message` line instead of a text line.
    #[arg(long)]
    pub json: bool,

    /// Question text. With no text, the body is read from stdin.
    #[arg(value_name = "MESSAGE")]
    pub message: Vec<String>,
}

impl AskArgs {
    /// The question to post: joined args, or stdin when no args are given.
    pub fn body(&self) -> String {
        join_body(&self.message)
    }
}

#[derive(clap::Args, Debug)]
pub struct InboxArgs {
    #[command(flatten)]
    pub client: ClientArgs,

    /// Print one JSON object per message instead of a text line.
    #[arg(long)]
    pub json: bool,
}

#[derive(clap::Args, Debug)]
pub struct AgentsArgs {
    #[command(flatten)]
    pub client: ClientArgs,

    /// Print one JSON object per agent instead of a text line.
    #[arg(long)]
    pub json: bool,
}

/// The states `status` accepts.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Working,
    Idle,
    Waiting,
}

impl State {
    /// The wire value of this state.
    pub fn as_str(self) -> &'static str {
        match self {
            State::Working => "working",
            State::Idle => "idle",
            State::Waiting => "waiting",
        }
    }
}

#[derive(clap::Args, Debug)]
pub struct StatusArgs {
    #[command(flatten)]
    pub client: ClientArgs,

    /// The state to report.
    #[arg(value_enum)]
    pub state: State,
}

#[derive(clap::Args, Debug)]
pub struct LogArgs {
    #[command(flatten)]
    pub client: ClientArgs,

    /// Keep only messages that this address sent or received.
    #[arg(long)]
    pub about: Option<String>,

    /// Print one JSON object per message instead of a text line.
    #[arg(long)]
    pub json: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client_args() -> ClientArgs {
        ClientArgs { server: "x".into(), address: None, agent: None, token: None }
    }

    fn send_args(message: Vec<&str>) -> SendArgs {
        SendArgs {
            client: ClientArgs {
                server: "x".into(),
                address: Some("a@h".into()),
                agent: None,
                token: None,
            },
            to: Some("b@h".into()),
            topic: None,
            message: message.into_iter().map(|m| m.to_string()).collect(),
            json: false,
        }
    }

    #[test]
    fn body_joins_positional_args() {
        assert_eq!(send_args(vec!["hello", "world"]).body(), "hello world");
    }

    #[test]
    fn body_returns_empty_when_no_args_and_empty_stdin() {
        // A non-interactive test process has an empty stdin.
        assert_eq!(send_args(vec![]).body(), "");
    }

    #[test]
    fn address_defaults_to_agent_at_short_hostname() {
        let mut args = client_args();
        args.agent = Some("codex".into());
        assert_eq!(args.resolve_address().unwrap(), format!("codex@{}", short_hostname()));
    }

    #[test]
    fn explicit_address_overrides_agent() {
        let mut args = client_args();
        args.address = Some("custom@other".into());
        args.agent = Some("codex".into());
        assert_eq!(args.resolve_address().unwrap(), "custom@other");
    }

    #[test]
    fn missing_address_and_agent_is_an_error() {
        let err = client_args().resolve_address().unwrap_err();
        assert!(err.contains("CHATTRS_ADDRESS or CHATTRS_AGENT"), "got: {err}");
    }
}
