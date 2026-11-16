//! chattrs — client: one TCP connection per command.
//!
//! Every command opens a fresh connection, sends a `hello`, then the command
//! frame, and reads the response. `watch` holds one open and streams
//! `message` frames, printing each as JSON; `ask` sends its question on one
//! connection, then holds a `wait` open until the reply arrives.

use crate::model::{self, Agent, Inbound, LoggedMessage, Message, Outbound};
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

/// One message to publish: its recipient, body, reply id, and topic.
/// A `None` topic is the general chat. `send` and `ask` post to an
/// address; `reply` leaves `to` empty so the broker answers the sender
/// of the message it answers.
#[derive(Debug, Clone)]
pub struct Outgoing {
    pub to: Option<String>,
    pub body: String,
    pub reply_to: Option<u64>,
    pub topic: Option<String>,
}

/// Publish one message and return its id. Prints unread messages first,
/// as text or as JSON `message` lines when `json` is set.
pub async fn send(
    server: &str,
    address: &str,
    token: Option<&str>,
    outgoing: Outgoing,
    json: bool,
) -> Result<u64, String> {
    publish(server, address, token, outgoing, json).await
}

/// Answer message `reply_to` and return the new id. The broker addresses the
/// reply to the sender of the original message. Without a `topic`, the
/// reply inherits the topic of the message it answers.
pub async fn reply(
    server: &str,
    address: &str,
    token: Option<&str>,
    reply_to: u64,
    body: &str,
    topic: Option<&str>,
    json: bool,
) -> Result<u64, String> {
    publish(
        server,
        address,
        token,
        Outgoing {
            to: None,
            body: body.to_string(),
            reply_to: Some(reply_to),
            topic: topic.map(|topic| topic.to_string()),
        },
        json,
    )
    .await
}

/// Fetch unread messages on this connection, print them, flush stdout,
/// then ack what was printed. Runs right after `hello`, before the
/// command's own frame, so every command delivers what is waiting.
async fn print_unread_preamble(conn: &mut Conn, json: bool) -> Result<(), String> {
    send_frame(conn, &Inbound::Inbox).await?;
    let messages = match recv_frame(conn).await? {
        Outbound::Inbox { messages } => messages,
        Outbound::Error { code, message } => return Err(format!("{code}: {message}")),
        _ => return Err("server sent an unexpected frame instead of inbox".to_string()),
    };
    if messages.is_empty() {
        return Ok(());
    }
    for message in &messages {
        if json {
            let line = format_watch_line(message)?;
            crate::write_stdout(&line).map_err(crate::stdout_error)?;
        } else {
            crate::write_stdout(&format!("{}\n", model::render_message(message)))
                .map_err(crate::stdout_error)?;
        }
    }
    let ids: Vec<u64> = messages.iter().map(|message| message.id).collect();
    send_frame(conn, &Inbound::Ack { ids }).await?;
    match recv_frame(conn).await? {
        Outbound::Acked { .. } => Ok(()),
        Outbound::Error { code, message } => Err(format!("{code}: {message}")),
        _ => Err("server sent an unexpected frame instead of acked".to_string()),
    }
}

async fn publish(
    server: &str,
    address: &str,
    token: Option<&str>,
    outgoing: Outgoing,
    json: bool,
) -> Result<u64, String> {
    let mut conn = hello(server, address, token).await?;
    print_unread_preamble(&mut conn, json).await?;
    send_frame(
        &mut conn,
        &Inbound::Send {
            to: outgoing.to,
            body: outgoing.body,
            reply_to: outgoing.reply_to,
            topic: outgoing.topic,
        },
    )
    .await?;
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

/// Seconds between reconnects, doubling from 1 s to 30 s. Shared by
/// `watch` and the `ask` wait, so both back off the same way.
pub fn next_backoff_secs(current: u64) -> u64 {
    (current * 2).min(30)
}

/// A failure of `wait_for_reply` or `ask`. A timeout carries the id that
/// never got a reply, so the caller can report it and exit 2.
#[derive(Debug, PartialEq, Eq)]
pub enum WaitError {
    Timeout { id: u64 },
    Failed(String),
}

impl From<String> for WaitError {
    fn from(error: String) -> WaitError {
        WaitError::Failed(error)
    }
}

/// True when the error is a broker `error` frame rather than a dropped
/// connection. Server errors fail the wait; anything else reconnects.
fn is_server_error(error: &str) -> bool {
    const CODES: [&str; 11] = [
        "bad_frame",
        "hello_required",
        "bad_version",
        "bad_address",
        "bad_token",
        "bad_topic",
        "empty_body",
        "too_large",
        "unknown_message",
        "not_recipient",
        "internal",
    ];
    match error.split_once(':') {
        Some((code, _)) => CODES.contains(&code),
        None => false,
    }
}

/// An open `wait`: one reply id, then one `message` frame.
pub struct WaitConn {
    conn: Conn,
}

impl WaitConn {
    /// Say hello and park a `wait` for `reply_to` on this connection.
    pub async fn connect(
        server: &str,
        address: &str,
        token: Option<&str>,
        reply_to: u64,
    ) -> Result<WaitConn, String> {
        let mut conn = hello(server, address, token).await?;
        send_frame(&mut conn, &Inbound::Wait { reply_to }).await?;
        Ok(WaitConn { conn })
    }

    /// Block until the broker delivers the reply.
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

/// Wait for the reply to `reply_to`, reconnecting with backoff when the
/// connection drops and resending only the `wait`. The question is sent
/// once by the caller and never here. `timeout_secs` of zero waits with
/// no limit; otherwise the whole wait, reconnects included, is bounded.
pub async fn wait_for_reply(
    server: &str,
    address: &str,
    token: Option<&str>,
    reply_to: u64,
    timeout_secs: u64,
) -> Result<Message, WaitError> {
    let deadline = if timeout_secs == 0 {
        None
    } else {
        Some(tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs))
    };
    let mut backoff_secs: u64 = 1;
    loop {
        match WaitConn::connect(server, address, token, reply_to).await {
            Ok(mut stream) => {
                backoff_secs = 1;
                let wait = stream.next_message();
                match deadline {
                    Some(end) => match tokio::time::timeout_at(end, wait).await {
                        Ok(Ok(message)) => return Ok(message),
                        Ok(Err(error)) => {
                            if is_server_error(&error) {
                                return Err(WaitError::Failed(error));
                            }
                            if tokio::time::Instant::now() >= end {
                                return Err(WaitError::Timeout { id: reply_to });
                            }
                            eprintln!(
                                "chattrs: ask disconnected: {error}, \
                                 retrying in {backoff_secs}s"
                            );
                        }
                        Err(_) => return Err(WaitError::Timeout { id: reply_to }),
                    },
                    None => match wait.await {
                        Ok(message) => return Ok(message),
                        Err(error) => {
                            if is_server_error(&error) {
                                return Err(WaitError::Failed(error));
                            }
                            eprintln!(
                                "chattrs: ask disconnected: {error}, \
                                 retrying in {backoff_secs}s"
                            );
                        }
                    },
                }
            }
            Err(error) => {
                if is_server_error(&error) {
                    return Err(WaitError::Failed(error));
                }
                if let Some(end) = deadline {
                    if tokio::time::Instant::now() >= end {
                        return Err(WaitError::Timeout { id: reply_to });
                    }
                }
                eprintln!("chattrs: ask cannot connect: {error}, retrying in {backoff_secs}s");
            }
        }
        match deadline {
            Some(end) => {
                let now = tokio::time::Instant::now();
                if now >= end {
                    return Err(WaitError::Timeout { id: reply_to });
                }
                let pause = std::time::Duration::from_secs(backoff_secs).min(end - now);
                tokio::time::sleep(pause).await;
                if tokio::time::Instant::now() >= end {
                    return Err(WaitError::Timeout { id: reply_to });
                }
            }
            None => {
                tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
            }
        }
        backoff_secs = next_backoff_secs(backoff_secs);
    }
}

/// Post a question and wait for its reply. The question is sent once;
/// reconnects resend only the `wait`. Returns the question id and reply.
/// Unread messages print before the question is sent, so the preamble
/// never contains the reply to this `ask`.
pub async fn ask(
    server: &str,
    address: &str,
    token: Option<&str>,
    outgoing: Outgoing,
    timeout_secs: u64,
    json: bool,
) -> Result<(u64, Message), WaitError> {
    let id = publish(server, address, token, outgoing, json).await.map_err(WaitError::Failed)?;
    let reply = wait_for_reply(server, address, token, id, timeout_secs).await?;
    Ok((id, reply))
}

/// One `log` page holds at most this many rows. The client pages with
/// `after` until a page holds fewer, so the output is the full history.
pub const LOG_PAGE_SIZE: u64 = model::MAX_LOG_LIMIT;

/// Fetch the full message history, oldest first. With `filter`, only
/// messages it sent or received. Prints unread messages first, as text or
/// as JSON `message` lines, on the same connection after `hello`.
pub async fn fetch_log(
    server: &str,
    address: &str,
    token: Option<&str>,
    filter: Option<String>,
    json: bool,
) -> Result<Vec<LoggedMessage>, String> {
    let mut conn = hello(server, address, token).await?;
    print_unread_preamble(&mut conn, json).await?;
    let mut history = Vec::new();
    let mut after = 0;
    loop {
        send_frame(
            &mut conn,
            &Inbound::Log { address: filter.clone(), after, limit: LOG_PAGE_SIZE },
        )
        .await?;
        let page = match recv_frame(&mut conn).await? {
            Outbound::Log { messages } => messages,
            Outbound::Error { code, message } => return Err(format!("{code}: {message}")),
            _ => return Err("server sent an unexpected frame instead of log".to_string()),
        };
        let count = page.len();
        if let Some(last) = page.last() {
            after = last.id;
        }
        history.extend(page);
        if count < LOG_PAGE_SIZE as usize {
            return Ok(history);
        }
    }
}

/// Format one history row exactly as `log --json` prints it: the logged
/// message with `type` `log` and `untrusted`, so a reader tells log rows
/// from the preamble `message` lines.
pub fn format_log_json(message: &LoggedMessage) -> Result<String, String> {
    let mut value = serde_json::to_value(message).map_err(|e| e.to_string())?;
    value["type"] = serde_json::Value::String("log".to_string());
    value["untrusted"] = serde_json::Value::Bool(true);
    serde_json::to_string(&value).map(|json| format!("{json}\n")).map_err(|e| e.to_string())
}

/// List every address that ever said `hello`, sorted by address.
/// Prints unread messages first, as text or as JSON `message` lines.
pub async fn fetch_agents(
    server: &str,
    address: &str,
    token: Option<&str>,
    json: bool,
) -> Result<Vec<Agent>, String> {
    let mut conn = hello(server, address, token).await?;
    print_unread_preamble(&mut conn, json).await?;
    send_frame(&mut conn, &Inbound::Agents).await?;
    match recv_frame(&mut conn).await? {
        Outbound::Agents { agents } => Ok(agents),
        Outbound::Error { code, message } => Err(format!("{code}: {message}")),
        _ => Err("server sent an unexpected frame instead of agents".to_string()),
    }
}

/// An open `watch` stream: unread messages first, then each new one.
/// With topics, the stream carries announcements to those rooms instead
/// of the inbox watch.
pub struct WatchConn {
    conn: Conn,
}

impl WatchConn {
    /// Say hello and start streaming for `address`. With topics, the
    /// broker replays each room's recent announcements, then each new
    /// one; without topics, it replays the unread inbox.
    pub async fn connect(
        server: &str,
        address: &str,
        token: Option<&str>,
        topics: &[String],
    ) -> Result<WatchConn, String> {
        let mut conn = hello(server, address, token).await?;
        send_frame(&mut conn, &Inbound::Watch { topics: topics.to_vec() }).await?;
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

/// Format one watched message exactly as `watch` prints it: one JSON
/// `message` line marked `untrusted`, since the body is from another
/// agent and carries no user authority. The broker never sends or
/// stores the flag; only the CLI adds it when printing.
pub fn format_watch_line(message: &Message) -> Result<String, String> {
    let mut value = serde_json::to_value(message).map_err(|e| e.to_string())?;
    value["type"] = serde_json::Value::String("message".to_string());
    value["untrusted"] = serde_json::Value::Bool(true);
    serde_json::to_string(&value).map(|json| format!("{json}\n")).map_err(|e| e.to_string())
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
    crate::write_stdout(&line).map_err(crate::stdout_error)?;
    ack(server, address, token, &[message.id]).await?;
    Ok(())
}

/// Print one watched announcement as JSON and flush stdout, without
/// acking: announcements belong to no inbox.
pub fn print_announcement(message: &Message) -> Result<(), String> {
    let line = format_watch_line(message)?;
    crate::write_stdout(&line).map_err(crate::stdout_error)?;
    Ok(())
}

/// Stream messages forever, printing one JSON line each. With topics, the
/// stream carries announcements and never acks. When the connection
/// drops, reconnect with backoff from 1 s to 30 s. Logs go to stderr only.
pub async fn watch(
    server: &str,
    address: &str,
    token: Option<&str>,
    topics: &[String],
) -> Result<(), String> {
    let announcing = !topics.is_empty();
    let mut backoff_secs: u64 = 1;
    loop {
        match WatchConn::connect(server, address, token, topics).await {
            Ok(mut stream) => {
                backoff_secs = 1;
                loop {
                    match stream.next_message().await {
                        Ok(message) => {
                            backoff_secs = 1;
                            let printed = if announcing {
                                print_announcement(&message)
                            } else {
                                print_and_ack(server, address, token, &message).await
                            };
                            if let Err(e) = printed {
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
        backoff_secs = next_backoff_secs(backoff_secs);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_json_line_carries_type_log_and_untrusted() {
        let message = LoggedMessage {
            id: 13,
            from: "codex@box1".into(),
            to: Some("opus@mac".into()),
            body: "hi".into(),
            ts: 1763200000000,
            reply_to: Some(12),
            topic: None,
            acked_at: None,
        };
        let line = format_log_json(&message).unwrap();
        assert!(line.ends_with('\n'));
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(value["type"].as_str(), Some("log"));
        assert_eq!(value["untrusted"].as_bool(), Some(true));
        assert_eq!(value["id"].as_u64(), Some(13));
        assert_eq!(value["reply_to"].as_u64(), Some(12));
        assert!(value["acked_at"].is_null(), "unread row has null acked_at");
    }
}
