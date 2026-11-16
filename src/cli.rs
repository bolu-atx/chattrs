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

    /// Recipient address, as `user@host`.
    #[arg(long)]
    pub to: String,

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

    /// Message text. With no text, the body is read from stdin.
    #[arg(value_name = "MESSAGE")]
    pub message: Vec<String>,
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
}

#[derive(clap::Args, Debug)]
pub struct AskArgs {
    #[command(flatten)]
    pub client: ClientArgs,

    /// Recipient address, as `user@host`.
    #[arg(long)]
    pub to: String,

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
            to: "b@h".into(),
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
