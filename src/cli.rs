//! CLI surface: argument parsing only. No networking, no state.

use clap::{Parser, Subcommand};

/// Broker address clients connect to and the server listens on by default.
pub const DEFAULT_SERVER: &str = "127.0.0.1:4646";
pub const DEFAULT_SESSION: &str = "default";

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
    /// Publish a message and print its id.
    Send(SendArgs),
    /// List recent messages in a session.
    List(ListArgs),
    /// Stream new messages as they arrive.
    Watch(WatchArgs),
    /// List active sessions and their connected agents.
    Sessions(SessionsArgs),
}

#[derive(clap::Args, Debug)]
pub struct ServeArgs {
    /// Address to listen on.
    #[arg(long, default_value = DEFAULT_SERVER)]
    pub listen: String,

    /// Messages to retain per session once the ring buffer fills.
    #[arg(long, default_value = "500")]
    pub history: usize,
}

/// Connection flags shared by the per-session client commands.
#[derive(clap::Args, Debug)]
pub struct EndpointArgs {
    /// Broker address to connect to.
    #[arg(long, default_value = DEFAULT_SERVER)]
    pub server: String,

    /// Session name.
    #[arg(long, default_value = DEFAULT_SESSION)]
    pub session: String,

    /// Sender identity (claude, muse, codex, …).
    #[arg(long, default_value = "agent", env = "CHATTRS_AGENT")]
    pub agent: String,
}

#[derive(clap::Args, Debug)]
pub struct SendArgs {
    #[command(flatten)]
    pub endpoint: EndpointArgs,

    /// Reply to a previous message id.
    #[arg(long)]
    pub reply_to: Option<u64>,

    /// Message text. With no text, the body is read from stdin.
    #[arg(value_name = "MESSAGE")]
    pub message: Vec<String>,
}

impl SendArgs {
    /// The body to publish: joined args, or stdin when no args are given.
    pub fn body(&self) -> String {
        if !self.message.is_empty() {
            return self.message.join(" ");
        }
        use std::io::Read;
        let mut body = String::new();
        let _ = std::io::stdin().read_to_string(&mut body);
        body.trim_end().to_string()
    }
}

#[derive(clap::Args, Debug)]
pub struct ListArgs {
    #[command(flatten)]
    pub endpoint: EndpointArgs,

    /// Maximum number of messages to return.
    #[arg(long, default_value = "50")]
    pub limit: usize,

    /// Only return messages after this id.
    #[arg(long)]
    pub after: Option<u64>,

    /// Print as JSON instead of a human-readable listing.
    #[arg(long)]
    pub json: bool,
}

#[derive(clap::Args, Debug)]
pub struct WatchArgs {
    #[command(flatten)]
    pub endpoint: EndpointArgs,
}

#[derive(clap::Args, Debug)]
pub struct SessionsArgs {
    /// Broker address to connect to.
    #[arg(long, default_value = DEFAULT_SERVER)]
    pub server: String,

    /// Print as JSON instead of a human-readable table.
    #[arg(long)]
    pub json: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_joins_positional_args() {
        let args = SendArgs {
            endpoint: EndpointArgs { server: "x".into(), session: "default".into(), agent: "claude".into() },
            reply_to: None,
            message: vec!["hello".into(), "world".into()],
        };
        assert_eq!(args.body(), "hello world");
    }

    #[test]
    fn body_returns_empty_when_no_args_and_empty_stdin() {
        let args = SendArgs {
            endpoint: EndpointArgs { server: "x".into(), session: "default".into(), agent: "claude".into() },
            reply_to: None,
            message: vec![],
        };
        // A non-interactive test process has an empty stdin.
        assert_eq!(args.body(), "");
    }
}
