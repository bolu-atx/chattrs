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
}

#[derive(clap::Args, Debug)]
pub struct ServeArgs {
    /// Address to listen on.
    #[arg(long, default_value = DEFAULT_SERVER)]
    pub listen: String,

    /// SQLite database path (created with parent directories as needed).
    #[arg(long, env = "CHATTRS_DB")]
    pub db: Option<PathBuf>,
}

/// Connection flags shared by the client commands.
#[derive(clap::Args, Debug)]
pub struct ClientArgs {
    /// Broker address to connect to.
    #[arg(long, default_value = DEFAULT_SERVER, env = "CHATTRS_SERVER")]
    pub server: String,

    /// Own address, as `user@host`.
    #[arg(long, env = "CHATTRS_ADDRESS")]
    pub address: String,

    /// Token for the broker (checked once multi-host binds need it).
    #[arg(long, env = "CHATTRS_TOKEN")]
    pub token: Option<String>,
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
pub struct WatchArgs {
    #[command(flatten)]
    pub client: ClientArgs,
}

#[derive(clap::Args, Debug)]
pub struct InboxArgs {
    #[command(flatten)]
    pub client: ClientArgs,

    /// Print one JSON object per message instead of a text line.
    #[arg(long)]
    pub json: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn send_args(message: Vec<&str>) -> SendArgs {
        SendArgs {
            client: ClientArgs {
                server: "x".into(),
                address: "a@h".into(),
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
}
