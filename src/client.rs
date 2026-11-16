//! chattrs — client: one TCP connection per command.
//!
//! Every command opens a fresh connection, sends a `hello`, then the command
//! frame, and reads the response. `watch` holds one open and streams
//! `message` frames, printing each as JSON; `ask` sends its question on one
//! connection, then holds a `wait` open until the reply arrives.

use std::fmt;

use crate::model::{self, Agent, Inbound, LoggedMessage, Message, Outbound};
use crate::protocol;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

/// One failure on the client path: a broker `error` frame, a connect,
/// read, or write failure, an unexpected frame, or an `ask` timeout.
#[derive(Debug, PartialEq, Eq)]
pub enum ClientError {
    Server { code: String, message: String },
    Io(String),
    Protocol(String),
    Timeout { id: u64 },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Server { code, message } => write!(f, "{code}: {message}"),
            ClientError::Io(message) => write!(f, "{message}"),
            ClientError::Protocol(message) => write!(f, "{message}"),
            ClientError::Timeout { id } => write!(f, "timeout: no reply to {id}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<ClientError> for String {
    fn from(error: ClientError) -> String {
        error.to_string()
    }
}

/// Where to connect, who to say `hello` as, and the shared token.
/// Built once from the connection flags and passed to every command.
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub server: String,
    pub address: String,
    pub token: Option<String>,
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

/// One open connection to the broker, after `hello`.
pub struct Client {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl Client {
    /// Connect to `endpoint` and say `hello`.
    pub async fn connect(endpoint: &Endpoint) -> Result<Client, ClientError> {
        let stream = TcpStream::connect(&endpoint.server)
            .await
            .map_err(|e| ClientError::Io(format!("connect {}: {e}", endpoint.server)))?;
        let (read_half, write_half) = stream.into_split();
        let mut client = Client { reader: BufReader::new(read_half), writer: write_half };
        let frame = client
            .request(&Inbound::Hello {
                v: model::PROTOCOL_VERSION,
                address: endpoint.address.clone(),
                token: endpoint.token.clone(),
            })
            .await?;
        match frame {
            Outbound::Welcome { .. } => Ok(client),
            _ => Err(ClientError::Protocol(
                "server sent an unexpected frame instead of welcome".to_string(),
            )),
        }
    }

    /// Send one frame and read one answer. An `error` frame becomes
    /// `ClientError::Server`.
    pub async fn request(&mut self, frame: &Inbound) -> Result<Outbound, ClientError> {
        self.send_frame(frame).await?;
        self.recv_frame().await
    }

    async fn send_frame(&mut self, frame: &Inbound) -> Result<(), ClientError> {
        let line = protocol::encode(frame).map_err(ClientError::Protocol)?;
        self.writer.write_all(line.as_bytes()).await.map_err(|e| ClientError::Io(e.to_string()))?;
        self.writer.flush().await.map_err(|e| ClientError::Io(e.to_string()))
    }

    async fn recv_frame(&mut self) -> Result<Outbound, ClientError> {
        let mut line = String::new();
        let n =
            self.reader.read_line(&mut line).await.map_err(|e| ClientError::Io(e.to_string()))?;
        if n == 0 {
            return Err(ClientError::Io("server closed the connection".to_string()));
        }
        match protocol::decode(&line).map_err(ClientError::Protocol)? {
            Outbound::Error { code, message } => Err(ClientError::Server { code, message }),
            frame => Ok(frame),
        }
    }

    /// Wait for the next streamed message, skipping other frames.
    pub async fn next_message(&mut self) -> Result<Message, ClientError> {
        loop {
            match self.recv_frame().await? {
                Outbound::Message(message) => return Ok(message),
                _ => continue,
            }
        }
    }

    /// Fetch unread messages on this connection, print them, flush stdout,
    /// then ack what was printed. Runs right after `hello`, before the
    /// command's own frame, so every command delivers what is waiting.
    async fn print_unread_preamble(&mut self, json: bool) -> Result<(), ClientError> {
        let messages = match self.request(&Inbound::Inbox).await? {
            Outbound::Inbox { messages } => messages,
            _ => {
                return Err(ClientError::Protocol(
                    "server sent an unexpected frame instead of inbox".to_string(),
                ))
            }
        };
        if messages.is_empty() {
            return Ok(());
        }
        for message in &messages {
            if json {
                let line = format_watch_line(message)?;
                crate::write_stdout(&line).map_err(|e| ClientError::Io(crate::stdout_error(e)))?;
            } else {
                crate::write_stdout(&format!("{}\n", model::render_message(message)))
                    .map_err(|e| ClientError::Io(crate::stdout_error(e)))?;
            }
        }
        let ids: Vec<u64> = messages.iter().map(|message| message.id).collect();
        match self.request(&Inbound::Ack { ids }).await? {
            Outbound::Acked { .. } => Ok(()),
            _ => Err(ClientError::Protocol(
                "server sent an unexpected frame instead of acked".to_string(),
            )),
        }
    }

    /// Print the unread preamble, then publish one message and return
    /// its id.
    pub async fn publish(&mut self, outgoing: Outgoing, json: bool) -> Result<u64, ClientError> {
        self.print_unread_preamble(json).await?;
        let frame = self
            .request(&Inbound::Send {
                to: outgoing.to,
                body: outgoing.body,
                reply_to: outgoing.reply_to,
                topic: outgoing.topic,
            })
            .await?;
        match frame {
            Outbound::Sent { id } => Ok(id),
            _ => Err(ClientError::Protocol(
                "server sent an unexpected frame instead of sent".to_string(),
            )),
        }
    }
}

/// Publish one message and return its id. Prints unread messages first,
/// as text or as JSON `message` lines when `json` is set.
pub async fn publish(
    endpoint: &Endpoint,
    outgoing: Outgoing,
    json: bool,
) -> Result<u64, ClientError> {
    Client::connect(endpoint).await?.publish(outgoing, json).await
}

/// Answer message `reply_to` and return the new id. The broker addresses the
/// reply to the sender of the original message. Without a `topic`, the
/// reply inherits the topic of the message it answers.
pub async fn reply(
    endpoint: &Endpoint,
    reply_to: u64,
    body: &str,
    topic: Option<&str>,
    json: bool,
) -> Result<u64, ClientError> {
    publish(
        endpoint,
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

/// Fetch every unread message for the endpoint address, in id order.
/// Fetched messages stay unread until the caller acks them.
pub async fn fetch_inbox(endpoint: &Endpoint) -> Result<Vec<Message>, ClientError> {
    let mut client = Client::connect(endpoint).await?;
    match client.request(&Inbound::Inbox).await? {
        Outbound::Inbox { messages } => Ok(messages),
        _ => Err(ClientError::Protocol(
            "server sent an unexpected frame instead of inbox".to_string(),
        )),
    }
}

/// Seconds between reconnects, doubling from 1 s to 30 s. Shared by
/// `watch` and the `ask` wait, so both back off the same way.
pub fn next_backoff_secs(current: u64) -> u64 {
    (current * 2).min(30)
}

/// Wait for the reply to `reply_to`, reconnecting with backoff when the
/// connection drops and resending only the `wait`. The question is sent
/// once by the caller and never here. `timeout_secs` of zero waits with
/// no limit; otherwise the whole wait, reconnects included, is bounded.
pub async fn wait_for_reply(
    endpoint: &Endpoint,
    reply_to: u64,
    timeout_secs: u64,
) -> Result<Message, ClientError> {
    let deadline = if timeout_secs == 0 {
        None
    } else {
        Some(tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs))
    };
    let mut backoff_secs: u64 = 1;
    loop {
        let attempt = async {
            let mut client = match Client::connect(endpoint).await {
                Ok(client) => client,
                Err(error) => return (false, Err(error)),
            };
            if let Err(error) = client.send_frame(&Inbound::Wait { reply_to }).await {
                return (true, Err(error));
            }
            (true, client.next_message().await)
        };
        let (connected, result) = match deadline {
            Some(end) => match tokio::time::timeout_at(end, attempt).await {
                Ok(outcome) => outcome,
                Err(_) => return Err(ClientError::Timeout { id: reply_to }),
            },
            None => attempt.await,
        };
        if connected {
            backoff_secs = 1;
        }
        match result {
            Ok(message) => return Ok(message),
            Err(error @ ClientError::Server { .. }) => return Err(error),
            Err(error) => {
                if let Some(end) = deadline {
                    if tokio::time::Instant::now() >= end {
                        return Err(ClientError::Timeout { id: reply_to });
                    }
                }
                if connected {
                    eprintln!("chattrs: ask disconnected: {error}, retrying in {backoff_secs}s");
                } else {
                    eprintln!("chattrs: ask cannot connect: {error}, retrying in {backoff_secs}s");
                }
            }
        }
        match deadline {
            Some(end) => {
                let now = tokio::time::Instant::now();
                if now >= end {
                    return Err(ClientError::Timeout { id: reply_to });
                }
                let pause = std::time::Duration::from_secs(backoff_secs).min(end - now);
                tokio::time::sleep(pause).await;
                if tokio::time::Instant::now() >= end {
                    return Err(ClientError::Timeout { id: reply_to });
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
    endpoint: &Endpoint,
    outgoing: Outgoing,
    timeout_secs: u64,
    json: bool,
) -> Result<(u64, Message), ClientError> {
    let id = publish(endpoint, outgoing, json).await?;
    let reply = wait_for_reply(endpoint, id, timeout_secs).await?;
    Ok((id, reply))
}

/// One `log` page holds at most this many rows. The client pages with
/// `after` until a page holds fewer, so the output is the full history.
pub const LOG_PAGE_SIZE: u64 = model::MAX_LOG_LIMIT;

/// Fetch the full message history, oldest first. With `filter`, only
/// messages it sent or received. Prints unread messages first, as text or
/// as JSON `message` lines, on the same connection after `hello`.
pub async fn fetch_log(
    endpoint: &Endpoint,
    filter: Option<String>,
    json: bool,
) -> Result<Vec<LoggedMessage>, ClientError> {
    let mut client = Client::connect(endpoint).await?;
    client.print_unread_preamble(json).await?;
    let mut history = Vec::new();
    let mut after = 0;
    loop {
        let page = match client
            .request(&Inbound::Log { address: filter.clone(), after, limit: LOG_PAGE_SIZE })
            .await?
        {
            Outbound::Log { messages } => messages,
            _ => {
                return Err(ClientError::Protocol(
                    "server sent an unexpected frame instead of log".to_string(),
                ))
            }
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
pub fn format_log_json(message: &LoggedMessage) -> Result<String, ClientError> {
    let mut value =
        serde_json::to_value(message).map_err(|e| ClientError::Protocol(e.to_string()))?;
    value["type"] = serde_json::Value::String("log".to_string());
    value["untrusted"] = serde_json::Value::Bool(true);
    serde_json::to_string(&value)
        .map(|json| format!("{json}\n"))
        .map_err(|e| ClientError::Protocol(e.to_string()))
}

/// List every address that ever said `hello`, sorted by address.
/// Prints unread messages first, as text or as JSON `message` lines.
pub async fn fetch_agents(endpoint: &Endpoint, json: bool) -> Result<Vec<Agent>, ClientError> {
    let mut client = Client::connect(endpoint).await?;
    client.print_unread_preamble(json).await?;
    match client.request(&Inbound::Agents).await? {
        Outbound::Agents { agents } => Ok(agents),
        _ => Err(ClientError::Protocol(
            "server sent an unexpected frame instead of agents".to_string(),
        )),
    }
}

/// Format one watched message exactly as `watch` prints it: one JSON
/// `message` line marked `untrusted`, since the body is from another
/// agent and carries no user authority. The broker never sends or
/// stores the flag; only the CLI adds it when printing.
pub fn format_watch_line(message: &Message) -> Result<String, ClientError> {
    let mut value =
        serde_json::to_value(message).map_err(|e| ClientError::Protocol(e.to_string()))?;
    value["type"] = serde_json::Value::String("message".to_string());
    value["untrusted"] = serde_json::Value::Bool(true);
    serde_json::to_string(&value)
        .map(|json| format!("{json}\n"))
        .map_err(|e| ClientError::Protocol(e.to_string()))
}

/// Print one watched message as JSON, flush stdout, then ack it. Printing
/// before acking keeps delivery at least once across a crash in between.
pub async fn print_and_ack(endpoint: &Endpoint, message: &Message) -> Result<(), ClientError> {
    let line = format_watch_line(message)?;
    crate::write_stdout(&line).map_err(|e| ClientError::Io(crate::stdout_error(e)))?;
    ack(endpoint, &[message.id]).await?;
    Ok(())
}

/// Print one watched announcement as JSON and flush stdout, without
/// acking: announcements belong to no inbox.
pub fn print_announcement(message: &Message) -> Result<(), ClientError> {
    let line = format_watch_line(message)?;
    crate::write_stdout(&line).map_err(|e| ClientError::Io(crate::stdout_error(e)))?;
    Ok(())
}

/// Stream messages forever, printing one JSON line each. With topics, the
/// stream carries announcements and never acks. When the connection
/// drops, reconnect with backoff from 1 s to 30 s. A broker `error`
/// frame fails the command. Logs go to stderr only.
pub async fn watch(endpoint: &Endpoint, topics: &[String]) -> Result<(), ClientError> {
    let announcing = !topics.is_empty();
    let mut backoff_secs: u64 = 1;
    loop {
        match Client::connect(endpoint).await {
            Ok(mut client) => {
                backoff_secs = 1;
                if let Err(error) =
                    client.send_frame(&Inbound::Watch { topics: topics.to_vec() }).await
                {
                    if matches!(error, ClientError::Server { .. }) {
                        return Err(error);
                    }
                    eprintln!("chattrs: watch disconnected: {error}, retrying in {backoff_secs}s");
                } else {
                    loop {
                        match client.next_message().await {
                            Ok(message) => {
                                backoff_secs = 1;
                                let printed = if announcing {
                                    print_announcement(&message)
                                } else {
                                    print_and_ack(endpoint, &message).await
                                };
                                match printed {
                                    Ok(()) => {}
                                    Err(error @ ClientError::Server { .. }) => return Err(error),
                                    Err(e) => {
                                        eprintln!(
                                            "chattrs: watch failed: {e}, \
                                             retrying in {backoff_secs}s"
                                        );
                                        break;
                                    }
                                }
                            }
                            Err(error @ ClientError::Server { .. }) => return Err(error),
                            Err(e) => {
                                eprintln!(
                                    "chattrs: watch disconnected: {e}, \
                                     retrying in {backoff_secs}s"
                                );
                                break;
                            }
                        }
                    }
                }
            }
            Err(error @ ClientError::Server { .. }) => return Err(error),
            Err(e) => {
                eprintln!("chattrs: watch cannot connect: {e}, retrying in {backoff_secs}s");
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
        backoff_secs = next_backoff_secs(backoff_secs);
    }
}

/// Ack message ids and return the ids that changed to read.
pub async fn ack(endpoint: &Endpoint, ids: &[u64]) -> Result<Vec<u64>, ClientError> {
    let mut client = Client::connect(endpoint).await?;
    match client.request(&Inbound::Ack { ids: ids.to_vec() }).await? {
        Outbound::Acked { ids } => Ok(ids),
        _ => Err(ClientError::Protocol(
            "server sent an unexpected frame instead of acked".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_error_displays_code_and_message() {
        let error =
            ClientError::Server { code: "bad_token".into(), message: "token is wrong".into() };
        assert_eq!(error.to_string(), "bad_token: token is wrong");
        assert_eq!(String::from(error), "bad_token: token is wrong");
    }

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
