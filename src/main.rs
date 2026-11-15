//! chatterbox — distributed messaging for cross-agent coordination.
//!
//! A thin entry point: parse `cli`, then dispatch to `server` (the broker) or
//! `client` (send/list/watch/sessions). `model` owns the wire types and
//! `protocol` the line-JSON framing.
//!
//! Quick start:
//! ```text
//! chatterbox serve
//! chatterbox send --agent claude "anyone there?"
//! chatterbox list
//! chatterbox watch --agent muse
//! ```

mod cli;
mod client;
mod model;
mod protocol;
mod server;

use clap::Parser;
use cli::{Args, Command};

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(e) = run(args).await {
        eprintln!("chatterbox: {e}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), String> {
    match args.command {
        Command::Serve(c) => server::serve(&c.listen, c.history).await,
        Command::Send(c) => {
            let id = client::send(
                &c.endpoint.server,
                &c.endpoint.session,
                &c.endpoint.agent,
                &c.body(),
                c.reply_to,
            )
            .await?;
            println!("{id}");
            Ok(())
        }
        Command::List(c) => {
            let messages =
                client::list(&c.endpoint.server, &c.endpoint.session, &c.endpoint.agent, Some(c.limit), c.after)
                    .await?;
            if c.json {
                println!("{}", serde_json::to_string_pretty(&messages).map_err(|e| e.to_string())?);
            } else {
                for message in &messages {
                    print_message(message);
                }
            }
            Ok(())
        }
        Command::Watch(c) => client::watch(&c.endpoint.server, &c.endpoint.session, &c.endpoint.agent, print_message)
            .await,
        Command::Sessions(c) => {
            let sessions = client::sessions(&c.server).await?;
            if c.json {
                println!("{}", serde_json::to_string_pretty(&sessions).map_err(|e| e.to_string())?);
            } else {
                print_sessions(&sessions);
            }
            Ok(())
        }
    }
}

fn print_message(message: &model::Message) {
    let reply = message.reply_to.map(|id| format!(" ↳ #{id}")).unwrap_or_default();
    println!(
        "#{} [{}] {}{}: {}",
        message.id,
        format_ts(message.ts),
        message.agent,
        reply,
        message.body
    );
}

fn print_sessions(sessions: &[model::SessionInfo]) {
    if sessions.is_empty() {
        println!("no sessions");
        return;
    }
    for session in sessions {
        let agents = if session.agents.is_empty() {
            "—".to_string()
        } else {
            session.agents.join(", ")
        };
        println!("{}\t{}\t{}", session.name, agents, session.message_count);
    }
}

fn format_ts(ms: u64) -> String {
    let secs = (ms / 1000) as i64;
    let nanos = ((ms % 1000) * 1_000_000) as u32;
    match chrono::DateTime::from_timestamp(secs, nanos) {
        Some(dt) => dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        None => ms.to_string(),
    }
}
