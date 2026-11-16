//! chattrs — distributed messaging for cross-agent coordination.
//!
//! A thin entry point: parse `cli`, then dispatch to `server` (the broker) or
//! `client` (`send`). `model` owns the wire types, `protocol` the line-JSON
//! framing, and `store` the SQLite storage.
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
    }
}
