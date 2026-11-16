//! chattrs — distributed messaging for cross-agent coordination.
//!
//! A thin entry point: parse `cli`, then dispatch to `server` (the broker) or
//! `client` (`send`, `reply`, `inbox`, `watch`, `ask`). `model` owns the
//! wire types, `protocol` the line-JSON framing, and `store` the SQLite
//! storage.
//!
//! Quick start:
//! ```text
//! chattrs serve --db /tmp/c.db
//! CHATTRS_ADDRESS=a@h chattrs send --to b@h "anyone there?"
//! ```

mod cli;
mod client;
mod model;
mod protocol;
mod server;
mod store;

use clap::Parser;
use cli::{Args, Command};
use store::Store;

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(e) = run(args).await {
        eprintln!("chattrs: {e}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), String> {
    match args.command {
        Command::Serve(c) => {
            let db = c.db.clone().unwrap_or_else(Store::default_path);
            server::serve(&c.listen, &db, c.token.clone()).await
        }
        Command::Send(c) => {
            let address = c.client.resolve_address()?;
            let id = client::send(
                &c.client.server,
                &address,
                c.client.token.as_deref(),
                &c.to,
                &c.body(),
                None,
            )
            .await?;
            if c.json {
                let line = protocol::encode(&model::Outbound::Sent { id })?;
                print!("{line}");
            } else {
                println!("{id}");
            }
            Ok(())
        }
        Command::Reply(c) => {
            let address = c.client.resolve_address()?;
            let id = client::reply(
                &c.client.server,
                &address,
                c.client.token.as_deref(),
                c.id,
                &c.body(),
            )
            .await?;
            println!("{id}");
            Ok(())
        }
        Command::Watch(c) => {
            let address = c.client.resolve_address()?;
            client::watch(&c.client.server, &address, c.client.token.as_deref()).await
        }
        Command::Ask(c) => {
            let address = c.client.resolve_address()?;
            let timeout = c.timeout;
            let json = c.json;
            match client::ask(
                &c.client.server,
                &address,
                c.client.token.as_deref(),
                &c.to,
                &c.body(),
                timeout,
            )
            .await
            {
                Ok((_, reply)) => {
                    if json {
                        print!("{}", client::format_watch_line(&reply)?);
                    } else {
                        println!("{}", model::render_message(&reply));
                    }
                    use std::io::Write;
                    std::io::stdout().flush().map_err(|e| format!("flush stdout: {e}"))?;
                    client::ack(&c.client.server, &address, c.client.token.as_deref(), &[reply.id])
                        .await?;
                    Ok(())
                }
                Err(client::WaitError::Timeout { id }) => {
                    if json {
                        println!("{{\"type\":\"timeout\",\"id\":{id}}}");
                        use std::io::Write;
                        std::io::stdout()
                            .flush()
                            .map_err(|e| format!("flush stdout: {e}"))?;
                    }
                    eprintln!("timeout: no reply to {id}");
                    std::process::exit(2);
                }
                Err(client::WaitError::Failed(error)) => Err(error),
            }
        }
        Command::Inbox(c) => {
            let address = c.client.resolve_address()?;
            let messages =
                client::fetch_inbox(&c.client.server, &address, c.client.token.as_deref()).await?;
            if messages.is_empty() {
                return Ok(());
            }
            for message in &messages {
                if c.json {
                    let mut value = serde_json::to_value(message).map_err(|e| e.to_string())?;
                    value["type"] = serde_json::Value::String("message".to_string());
                    println!("{value}");
                } else {
                    println!("{}", model::render_message(message));
                }
            }
            use std::io::Write;
            std::io::stdout().flush().map_err(|e| format!("flush stdout: {e}"))?;
            let ids: Vec<u64> = messages.iter().map(|message| message.id).collect();
            client::ack(&c.client.server, &address, c.client.token.as_deref(), &ids).await?;
            Ok(())
        }
        Command::Agents(c) => {
            let address = c.client.resolve_address()?;
            let agents =
                client::fetch_agents(&c.client.server, &address, c.client.token.as_deref()).await?;
            for agent in &agents {
                if c.json {
                    let mut value = serde_json::to_value(agent).map_err(|e| e.to_string())?;
                    value["type"] = serde_json::Value::String("agent".to_string());
                    println!("{value}");
                } else {
                    println!("{}  {}", agent.address, model::format_ts(agent.last_seen));
                }
            }
            Ok(())
        }
    }
}
