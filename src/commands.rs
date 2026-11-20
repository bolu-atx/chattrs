//! chattrs — the message verbs and the shared output helpers.

use crate::cli;
use crate::client;
use crate::model;
use crate::output;
use crate::project::ProjectSource;
use crate::settings::Settings;
use crate::skill;
use crate::store::Store;
use crate::{autostart, server, RunError};

/// Write one piece of command output to stdout. A closed pipe ends the
/// command quietly; any other failure is the command's error.
pub(crate) fn write(text: &str) -> Result<(), RunError> {
    output::write_stdout(text).map_err(|error| {
        if error.kind() == std::io::ErrorKind::BrokenPipe {
            RunError::BrokenPipe
        } else {
            RunError::Message(format!("write stdout: {error}"))
        }
    })
}

/// Build the one endpoint for a client command from its flags and the
/// settings. The project comes from the environment, the config, or the repo
/// at the current directory.
pub(crate) fn endpoint(
    client: &cli::ClientArgs,
    settings: &Settings,
) -> Result<client::Endpoint, RunError> {
    let address = client.resolve_address()?;
    let cwd = std::env::current_dir().unwrap_or_default();
    let (project, source) = settings.project(None, &cwd);
    Ok(client::Endpoint {
        server: client.server.clone(),
        address,
        token: client.token.clone(),
        project: (source != ProjectSource::Default).then_some(project),
        tags: settings.tags.clone(),
    })
}

/// Print the unread messages waiting on this connection, flush stdout,
/// then ack what was printed. Every command runs this before its own
/// work, so the preamble never holds its own output; `inbox` is this
/// alone.
pub(crate) async fn deliver_unread(
    client: &mut client::Client,
    json: bool,
) -> Result<(), RunError> {
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
pub(crate) fn cmd_stop() -> Result<(), RunError> {
    let db = autostart::client_db_path();
    autostart::stop_auto_started(&db).map_err(RunError::Message)
}

pub(crate) async fn cmd_serve(c: cli::ServeArgs) -> Result<(), RunError> {
    let db = c.db.clone().unwrap_or_else(Store::default_path);
    server::serve(&c.listen, &db, c.token.clone(), c.tokens.as_deref())
        .await
        .map_err(RunError::Message)
}

pub(crate) async fn cmd_send(c: cli::SendArgs, settings: &Settings) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client, settings)?;
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

pub(crate) async fn cmd_reply(c: cli::ReplyArgs, settings: &Settings) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client, settings)?;
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

pub(crate) async fn cmd_watch(c: cli::WatchArgs, settings: &Settings) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client, settings)?;
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

pub(crate) async fn cmd_ask(c: cli::AskArgs, settings: &Settings) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client, settings)?;
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

pub(crate) async fn cmd_inbox(c: cli::InboxArgs, settings: &Settings) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client, settings)?;
    let mut conn = client::Client::connect(&endpoint).await?;
    deliver_unread(&mut conn, c.json).await
}

pub(crate) fn cmd_skill() -> Result<(), RunError> {
    write(skill::text())
}

pub(crate) async fn cmd_log(c: cli::LogArgs, settings: &Settings) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client, settings)?;
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
pub(crate) async fn cmd_status(c: cli::StatusArgs, settings: &Settings) -> Result<(), RunError> {
    let result = match endpoint(&c.client, settings) {
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

pub(crate) async fn cmd_agents(c: cli::AgentsArgs, settings: &Settings) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client, settings)?;
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
pub(crate) async fn cmd_projects(
    c: cli::ProjectsArgs,
    settings: &Settings,
) -> Result<(), RunError> {
    let endpoint = endpoint(&c.client, settings)?;
    for project in &client::projects(&endpoint).await? {
        if c.json {
            write(&output::project_json(project))?;
        } else {
            write(&output::project_text(project))?;
        }
    }
    Ok(())
}
