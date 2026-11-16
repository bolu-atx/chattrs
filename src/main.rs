//! chattrs — distributed messaging for cross-agent coordination.
//!
//! A thin entry point: parse `cli`, then dispatch to `server` (the broker) or
//! `client` (`send`, `reply`, `inbox`, `watch`). `model` owns the wire types,
//! `protocol` the line-JSON framing, and `store` the SQLite storage.
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
            server::serve(&c.listen, &db).await
        }
        Command::Send(c) => {
            let id = client::send(
                &c.client.server,
                &c.client.address,
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
            let id = client::reply(
                &c.client.server,
                &c.client.address,
                c.client.token.as_deref(),
                c.id,
                &c.body(),
            )
            .await?;
            println!("{id}");
            Ok(())
        }
        Command::Watch(c) => {
            client::watch(&c.client.server, &c.client.address, c.client.token.as_deref()).await
        }
        Command::Inbox(c) => {
            let messages =
                client::fetch_inbox(&c.client.server, &c.client.address, c.client.token.as_deref())
                    .await?;
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
            client::ack(&c.client.server, &c.client.address, c.client.token.as_deref(), &ids)
                .await?;
            Ok(())
        }
    }
}
