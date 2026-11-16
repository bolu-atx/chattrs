//! chattrs — client: one TCP connection per command.
//!
//! Every command opens a fresh connection, sends a `hello`, then the command
//! frame, and reads the response. `watch` is the one exception: it holds the
//! connection open and streams `message` frames, printing each as JSON.

use crate::model::{self, Inbound, Message, Outbound};
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

/// Say hello and return the open connection once the broker welcomes us.
async fn hello(server: &str, address: &str, token: Option<&str>) -> Result<Conn, String> {
    let mut conn = connect(server).await?;
    send_frame(
        &mut conn,
        &Inbound::Hello {
            v: model::PROTOCOL_VERSION,
            address: address.to_string(),
            token: token.map(|token| token.to_string()),
        },
    )
    .await?;
    match recv_frame(&mut conn).await? {
        Outbound::Welcome { .. } => Ok(conn),
        Outbound::Error { code, message } => Err(format!("{code}: {message}")),
        _ => Err("server sent an unexpected frame instead of welcome".to_string()),
    }
}

/// Publish one message and return its id.
pub async fn send(
    server: &str,
    address: &str,
    token: Option<&str>,
    to: &str,
    body: &str,
    reply_to: Option<u64>,
) -> Result<u64, String> {
    publish(server, address, token, Some(to.to_string()), body, reply_to).await
}

/// Answer message `reply_to` and return the new id. The broker addresses the
/// reply to the sender of the original message.
pub async fn reply(
    server: &str,
    address: &str,
    token: Option<&str>,
    reply_to: u64,
    body: &str,
) -> Result<u64, String> {
    publish(server, address, token, None, body, Some(reply_to)).await
}

async fn publish(
    server: &str,
    address: &str,
    token: Option<&str>,
    to: Option<String>,
    body: &str,
    reply_to: Option<u64>,
) -> Result<u64, String> {
    let mut conn = hello(server, address, token).await?;
    send_frame(&mut conn, &Inbound::Send { to, body: body.to_string(), reply_to }).await?;
    match recv_frame(&mut conn).await? {
        Outbound::Sent { id } => Ok(id),
        Outbound::Error { code, message } => Err(format!("{code}: {message}")),
        _ => Err("server sent an unexpected frame instead of sent".to_string()),
    }
}

/// Fetch every unread message for `address`, in id order. Fetched
/// messages stay unread until the caller acks them.
pub async fn fetch_inbox(
    server: &str,
    address: &str,
    token: Option<&str>,
) -> Result<Vec<Message>, String> {
    let mut conn = hello(server, address, token).await?;
    send_frame(&mut conn, &Inbound::Inbox).await?;
    match recv_frame(&mut conn).await? {
        Outbound::Inbox { messages } => Ok(messages),
        Outbound::Error { code, message } => Err(format!("{code}: {message}")),
        _ => Err("server sent an unexpected frame instead of inbox".to_string()),
    }
}

/// An open `watch` stream: unread messages first, then each new one.
pub struct WatchConn {
    conn: Conn,
}

impl WatchConn {
    /// Say hello and start streaming for `address`.
    pub async fn connect(
        server: &str,
        address: &str,
        token: Option<&str>,
    ) -> Result<WatchConn, String> {
        let mut conn = hello(server, address, token).await?;
        send_frame(&mut conn, &Inbound::Watch).await?;
        Ok(WatchConn { conn })
    }

    /// Wait for the next streamed message.
    pub async fn next_message(&mut self) -> Result<Message, String> {
        loop {
            match recv_frame(&mut self.conn).await? {
                Outbound::Message(message) => return Ok(message),
                Outbound::Error { code, message } => {
                    return Err(format!("{code}: {message}"));
                }
                _ => continue,
            }
        }
    }
}

/// Format one watched message exactly as `watch` prints it: one JSON line.
pub fn format_watch_line(message: &Message) -> Result<String, String> {
    protocol::encode(&Outbound::Message(message.clone()))
}

/// Print one watched message as JSON, flush stdout, then ack it. Printing
/// before acking keeps delivery at least once across a crash in between.
pub async fn print_and_ack(
    server: &str,
    address: &str,
    token: Option<&str>,
    message: &Message,
) -> Result<(), String> {
    let line = format_watch_line(message)?;
    print!("{line}");
    use std::io::Write;
    std::io::stdout().flush().map_err(|e| format!("flush stdout: {e}"))?;
    ack(server, address, token, &[message.id]).await?;
    Ok(())
}

/// Stream messages forever, printing one JSON line each. When the connection
/// drops, reconnect with backoff from 1 s to 30 s. Logs go to stderr only.
pub async fn watch(server: &str, address: &str, token: Option<&str>) -> Result<(), String> {
    let mut backoff_secs: u64 = 1;
    loop {
        match WatchConn::connect(server, address, token).await {
            Ok(mut stream) => {
                backoff_secs = 1;
                loop {
                    match stream.next_message().await {
                        Ok(message) => {
                            backoff_secs = 1;
                            if let Err(e) = print_and_ack(server, address, token, &message).await
                            {
                                eprintln!(
                                    "chattrs: watch failed: {e}, retrying in {backoff_secs}s"
                                );
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "chattrs: watch disconnected: {e}, retrying in {backoff_secs}s"
                            );
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("chattrs: watch cannot connect: {e}, retrying in {backoff_secs}s");
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
        backoff_secs = (backoff_secs * 2).min(30);
    }
}

/// Ack message ids and return the ids that changed to read.
pub async fn ack(
    server: &str,
    address: &str,
    token: Option<&str>,
    ids: &[u64],
) -> Result<Vec<u64>, String> {
    let mut conn = hello(server, address, token).await?;
    send_frame(&mut conn, &Inbound::Ack { ids: ids.to_vec() }).await?;
    match recv_frame(&mut conn).await? {
        Outbound::Acked { ids } => Ok(ids),
        Outbound::Error { code, message } => Err(format!("{code}: {message}")),
        _ => Err("server sent an unexpected frame instead of acked".to_string()),
    }
}
