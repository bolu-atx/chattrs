//! chatterbox — client: one TCP connection per command.
//!
//! Every command opens a fresh connection, sends a `hello`, then the command
//! frame, and reads the response. `watch` keeps its connection open and
//! streams broadcast messages to the caller.

use crate::model::{self, Inbound, Outbound};
use crate::protocol;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

struct Conn {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

async fn connect(server: &str) -> Result<Conn, String> {
    let stream = TcpStream::connect(server).await.map_err(|e| format!("connect {server}: {e}"))?;
    let (read_half, write_half) = stream.into_split();
    Ok(Conn { reader: BufReader::new(read_half), writer: write_half })
}

async fn send_frame(conn: &mut Conn, frame: &Inbound) -> Result<(), String> {
    let line = protocol::encode(frame)?;
    conn.writer.write_all(line.as_bytes()).await.map_err(|e| e.to_string())?;
    conn.writer.flush().await.map_err(|e| e.to_string())
}

async fn recv_frame(conn: &mut Conn) -> Result<Outbound, String> {
    let mut line = String::new();
    let n = conn.reader.read_line(&mut line).await.map_err(|e| e.to_string())?;
    if n == 0 {
        return Err("server closed the connection".to_string());
    }
    protocol::decode(&line)
}

async fn open(server: &str, agent: &str, session: &str) -> Result<Conn, String> {
    let mut conn = connect(server).await?;
    send_frame(&mut conn, &Inbound::Hello { agent: agent.to_string(), session: session.to_string() })
        .await?;
    match recv_frame(&mut conn).await? {
        Outbound::Welcome { .. } => Ok(conn),
        Outbound::Error { message } => Err(message),
        _ => Err("server sent an unexpected frame instead of welcome".to_string()),
    }
}

/// Publish one message and return its id.
pub async fn send(
    server: &str,
    session: &str,
    agent: &str,
    body: &str,
    reply_to: Option<u64>,
) -> Result<u64, String> {
    let mut conn = open(server, agent, session).await?;
    send_frame(&mut conn, &Inbound::Send { body: body.to_string(), reply_to }).await?;
    match recv_frame(&mut conn).await? {
        Outbound::Ack { id } => Ok(id),
        Outbound::Error { message } => Err(message),
        _ => Err("server sent an unexpected frame instead of an ack".to_string()),
    }
}

/// Fetch recent messages from a session, newest last.
pub async fn list(
    server: &str,
    session: &str,
    agent: &str,
    limit: Option<usize>,
    after: Option<u64>,
) -> Result<Vec<model::Message>, String> {
    let mut conn = open(server, agent, session).await?;
    send_frame(&mut conn, &Inbound::List { limit, after }).await?;
    match recv_frame(&mut conn).await? {
        Outbound::History { messages } => Ok(messages),
        Outbound::Error { message } => Err(message),
        _ => Err("server sent an unexpected frame instead of history".to_string()),
    }
}

/// Stream new messages, calling `on_message` for each.
pub async fn watch(
    server: &str,
    session: &str,
    agent: &str,
    mut on_message: impl FnMut(&model::Message),
) -> Result<(), String> {
    let mut conn = open(server, agent, session).await?;
    send_frame(&mut conn, &Inbound::Watch).await?;
    loop {
        match recv_frame(&mut conn).await? {
            Outbound::Message { id, session, agent, body, ts, reply_to } => {
                on_message(&model::Message { id, session, agent, body, ts, reply_to });
            }
            Outbound::Error { message } => eprintln!("chatterbox: {message}"),
            _ => {}
        }
    }
}

/// List active sessions and their connected agents.
pub async fn sessions(server: &str) -> Result<Vec<model::SessionInfo>, String> {
    let mut conn = connect(server).await?;
    send_frame(&mut conn, &Inbound::Sessions).await?;
    match recv_frame(&mut conn).await? {
        Outbound::Sessions { sessions } => Ok(sessions),
        Outbound::Error { message } => Err(message),
        _ => Err("server sent an unexpected frame instead of sessions".to_string()),
    }
}
