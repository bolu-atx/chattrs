//! chattrs — client: one TCP connection per command.
//!
//! Every command opens a fresh connection, sends a `hello`, then the command
//! frame, and reads the response. Functions here return data and print
//! nothing; `output` owns what the CLI prints. `watch` is the one long-lived
//! read: a `Watcher` reconnects on I/O failure and hands each message to
//! its caller, which prints it and then acks it.

use std::fmt;
use std::io::ErrorKind;

use crate::autostart;
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

/// How long one TCP connect may take before it counts as failed.
/// Dropped packets to an unreachable address would otherwise hang a
/// one-shot command for the full OS TCP timeout.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// A failed connect: the message plus whether the port refused the
/// connection, which is the only failure that starts a broker.
struct ConnectError {
    message: String,
    refused: bool,
}

async fn connect_stream(server: &str) -> Result<TcpStream, ConnectError> {
    match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(server)).await {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(e)) => Err(ConnectError {
            refused: e.kind() == ErrorKind::ConnectionRefused,
            message: format!("connect {server}: {e}"),
        }),
        Err(_) => Err(ConnectError {
            refused: false,
            message: format!("connect {server}: timed out after {}s", CONNECT_TIMEOUT.as_secs()),
        }),
    }
}

impl Client {
    /// Connect to `endpoint` and say `hello`. When the first connect
    /// fails with connection refused on a loopback server, start a
    /// broker on demand and retry once on a fresh connection. Any
    /// other failure returns the original connect error.
    pub async fn connect(endpoint: &Endpoint) -> Result<Client, ClientError> {
        Client::open(endpoint, true).await
    }

    /// Connect again after a drop. A reconnect never starts a broker:
    /// only a command's first connect does (CLI-11). A broker that died
    /// under a waiting `ask` or `watch` is not replaced behind the
    /// operator's back, possibly on another database.
    pub async fn reconnect(endpoint: &Endpoint) -> Result<Client, ClientError> {
        Client::open(endpoint, false).await
    }

    async fn open(endpoint: &Endpoint, may_start: bool) -> Result<Client, ClientError> {
        let stream = match connect_stream(&endpoint.server).await {
            Ok(stream) => stream,
            Err(error)
                if may_start
                    && error.refused
                    && autostart::is_loopback_server(&endpoint.server) =>
            {
                autostart::ensure_started(&endpoint.server, endpoint.token.as_deref())
                    .await
                    .map_err(ClientError::Io)?;
                connect_stream(&endpoint.server)
                    .await
                    .map_err(|error| ClientError::Io(error.message))?
            }
            Err(error) => return Err(ClientError::Io(error.message)),
        };
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

    /// Send a `wait` for `reply_to` on this connection and wait for the
    /// reply message on it.
    pub async fn wait_reply(&mut self, reply_to: u64) -> Result<Message, ClientError> {
        self.send_frame(&Inbound::Wait { reply_to }).await?;
        self.next_message().await
    }

    /// Send an `ack` without waiting for its answer. The next
    /// `next_message` skips the `acked` frame.
    pub async fn ack_sent(&mut self, id: u64) -> Result<(), ClientError> {
        self.send_frame(&Inbound::Ack { ids: vec![id] }).await
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

    /// Fetch every unread message on this connection, in id order.
    /// Fetched messages stay unread until the caller acks them.
    pub async fn unread(&mut self) -> Result<Vec<Message>, ClientError> {
        match self.request(&Inbound::Inbox).await? {
            Outbound::Inbox { messages } => Ok(messages),
            _ => Err(ClientError::Protocol(
                "server sent an unexpected frame instead of inbox".to_string(),
            )),
        }
    }

    /// Mark messages as read on this connection and return the ids that
    /// changed to read.
    pub async fn ack(&mut self, ids: &[u64]) -> Result<Vec<u64>, ClientError> {
        match self.request(&Inbound::Ack { ids: ids.to_vec() }).await? {
            Outbound::Acked { ids } => Ok(ids),
            _ => Err(ClientError::Protocol(
                "server sent an unexpected frame instead of acked".to_string(),
            )),
        }
    }

    /// Publish one message on this connection and return its id with the
    /// broker's `known` flag. The flag is `None` from an older broker or
    /// with no `to`.
    pub async fn publish(
        &mut self,
        outgoing: Outgoing,
    ) -> Result<(u64, Option<bool>), ClientError> {
        let frame = self
            .request(&Inbound::Send {
                to: outgoing.to,
                body: outgoing.body,
                reply_to: outgoing.reply_to,
                topic: outgoing.topic,
            })
            .await?;
        match frame {
            Outbound::Sent { id, known } => Ok((id, known)),
            _ => Err(ClientError::Protocol(
                "server sent an unexpected frame instead of sent".to_string(),
            )),
        }
    }

    /// Fetch the full message history on this connection, oldest first.
    /// With `filter`, only messages it sent or received.
    pub async fn log(&mut self, filter: Option<String>) -> Result<Vec<LoggedMessage>, ClientError> {
        let mut history = Vec::new();
        let mut after = 0;
        loop {
            let page = match self
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
                after = last.message.id;
            }
            history.extend(page);
            if count < LOG_PAGE_SIZE as usize {
                return Ok(history);
            }
        }
    }

    /// List every address that ever said `hello`, sorted by address.
    pub async fn agents(&mut self) -> Result<Vec<Agent>, ClientError> {
        match self.request(&Inbound::Agents).await? {
            Outbound::Agents { agents } => Ok(agents),
            _ => Err(ClientError::Protocol(
                "server sent an unexpected frame instead of agents".to_string(),
            )),
        }
    }
}

/// Publish one message and return its id.
#[cfg(test)]
pub async fn publish(endpoint: &Endpoint, outgoing: Outgoing) -> Result<u64, ClientError> {
    Ok(Client::connect(endpoint).await?.publish(outgoing).await?.0)
}

/// Answer message `reply_to` and return the new id. The broker addresses the
/// reply to the sender of the original message. Without a `topic`, the
/// reply inherits the topic of the message it answers.
#[cfg(test)]
pub async fn reply(
    endpoint: &Endpoint,
    reply_to: u64,
    body: &str,
    topic: Option<&str>,
) -> Result<u64, ClientError> {
    publish(
        endpoint,
        Outgoing {
            to: None,
            body: body.to_string(),
            reply_to: Some(reply_to),
            topic: topic.map(|topic| topic.to_string()),
        },
    )
    .await
}

/// Fetch every unread message for the endpoint address, in id order.
/// Fetched messages stay unread until the caller acks them.
#[cfg(test)]
pub async fn fetch_inbox(endpoint: &Endpoint) -> Result<Vec<Message>, ClientError> {
    Client::connect(endpoint).await?.unread().await
}

/// Seconds between reconnects, doubling from 1 s to 30 s. Shared by
/// `watch` and the `ask` wait, so both back off the same way.
pub fn next_backoff_secs(current: u64) -> u64 {
    (current * 2).min(30)
}

/// Sleep for one backoff step, bounded by `deadline`. A passed deadline
/// is the `ask` timeout for `reply_to`.
async fn pause_backoff(
    deadline: Option<tokio::time::Instant>,
    backoff_secs: u64,
    reply_to: u64,
) -> Result<(), ClientError> {
    match deadline {
        Some(end) => {
            let now = tokio::time::Instant::now();
            if now >= end {
                return Err(ClientError::Timeout { id: reply_to });
            }
            tokio::time::sleep(std::time::Duration::from_secs(backoff_secs).min(end - now)).await;
            if tokio::time::Instant::now() >= end {
                return Err(ClientError::Timeout { id: reply_to });
            }
            Ok(())
        }
        None => {
            tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
            Ok(())
        }
    }
}

/// Wait for the reply to `reply_to` on `client`, reconnecting with
/// backoff when the connection drops and resending only the `wait` on a
/// new connection. Only an I/O failure reconnects; a broker error or a
/// bad frame fails the wait. The question is sent once by the caller and
/// never here. `timeout_secs` of zero waits with no limit; otherwise the
/// whole wait, reconnects included, is bounded. On success the live
/// connection comes back with the reply, so the caller acks on it.
pub async fn wait_for_reply(
    endpoint: &Endpoint,
    client: Client,
    reply_to: u64,
    timeout_secs: u64,
) -> Result<(Client, Message), ClientError> {
    let deadline = if timeout_secs == 0 {
        None
    } else {
        Some(tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs))
    };
    let mut backoff_secs: u64 = 1;
    let mut current = Some(client);
    loop {
        let mut conn = match current.take() {
            Some(conn) => conn,
            None => match Client::reconnect(endpoint).await {
                Ok(conn) => conn,
                Err(error) => {
                    eprintln!("chattrs: ask cannot connect: {error}, retrying in {backoff_secs}s");
                    pause_backoff(deadline, backoff_secs, reply_to).await?;
                    backoff_secs = next_backoff_secs(backoff_secs);
                    continue;
                }
            },
        };
        backoff_secs = 1;
        let waited = match deadline {
            Some(end) => match tokio::time::timeout_at(end, conn.wait_reply(reply_to)).await {
                Ok(result) => result,
                Err(_) => return Err(ClientError::Timeout { id: reply_to }),
            },
            None => conn.wait_reply(reply_to).await,
        };
        match waited {
            Ok(message) => return Ok((conn, message)),
            Err(error @ ClientError::Server { .. }) => return Err(error),
            Err(error @ ClientError::Protocol(_)) => return Err(error),
            Err(error @ ClientError::Timeout { .. }) => return Err(error),
            Err(error) => {
                eprintln!("chattrs: ask disconnected: {error}, retrying in {backoff_secs}s");
                pause_backoff(deadline, backoff_secs, reply_to).await?;
                backoff_secs = next_backoff_secs(backoff_secs);
            }
        }
    }
}

/// One `log` page holds at most this many rows. The client pages with
/// `after` until a page holds fewer, so the output is the full history.
pub const LOG_PAGE_SIZE: u64 = model::MAX_LOG_LIMIT;

/// Fetch the full message history, oldest first. With `filter`, only
/// messages it sent or received.
#[cfg(test)]
pub async fn fetch_log(
    endpoint: &Endpoint,
    filter: Option<String>,
) -> Result<Vec<LoggedMessage>, ClientError> {
    Client::connect(endpoint).await?.log(filter).await
}

/// List every address that ever said `hello`, sorted by address.
#[cfg(test)]
pub async fn fetch_agents(endpoint: &Endpoint) -> Result<Vec<Agent>, ClientError> {
    Client::connect(endpoint).await?.agents().await
}

/// Ack message ids and return the ids that changed to read.
#[cfg(test)]
pub async fn ack(endpoint: &Endpoint, ids: &[u64]) -> Result<Vec<u64>, ClientError> {
    Client::connect(endpoint).await?.ack(ids).await
}

/// A `watch` stream that reconnects on I/O failure and hands each message
/// to its caller. Only an I/O failure reconnects; a broker error or a bad
/// frame ends the stream with that error. With topics, the stream carries
/// announcements; with none, it carries the own inbox. Logs go to stderr
/// only.
pub struct Watcher {
    endpoint: Endpoint,
    topics: Vec<String>,
    client: Option<Client>,
    backoff_secs: u64,
    /// True after the first successful connect. Later connects are
    /// reconnects and never start a broker.
    connected_once: bool,
}

impl Watcher {
    /// A stream for `endpoint` over `topics`; an empty list watches the
    /// own inbox.
    pub fn new(endpoint: Endpoint, topics: Vec<String>) -> Watcher {
        Watcher { endpoint, topics, client: None, backoff_secs: 1, connected_once: false }
    }

    /// Drop the current connection, so the next message reconnects and
    /// the broker replays what is still unread.
    pub fn reset(&mut self) {
        self.client = None;
    }

    /// Ack one printed message on the watch connection itself. The
    /// `acked` answer is skipped by the next `next_message` call.
    pub async fn ack(&mut self, id: u64) -> Result<(), ClientError> {
        match self.client.as_mut() {
            Some(client) => client.ack_sent(id).await,
            None => Err(ClientError::Io("watch is not connected".to_string())),
        }
    }

    /// Connect and send the `watch` frame. An I/O failure is returned for
    /// the caller to retry; any other failure ends the stream.
    async fn connect(&mut self) -> Result<(), ClientError> {
        let mut client = if self.connected_once {
            Client::reconnect(&self.endpoint).await?
        } else {
            Client::connect(&self.endpoint).await?
        };
        self.connected_once = true;
        client.send_frame(&Inbound::Watch { topics: self.topics.clone() }).await?;
        self.client = Some(client);
        self.backoff_secs = 1;
        Ok(())
    }

    /// Wait for the next streamed message, reconnecting with backoff from
    /// 1 s to 30 s when the connection drops.
    pub async fn next_message(&mut self) -> Result<Message, ClientError> {
        loop {
            if self.client.is_none() {
                match self.connect().await {
                    Ok(()) => {}
                    Err(error @ ClientError::Server { .. }) => return Err(error),
                    Err(error @ ClientError::Protocol(_)) => return Err(error),
                    Err(error @ ClientError::Timeout { .. }) => return Err(error),
                    Err(error) => {
                        eprintln!(
                            "chattrs: watch cannot connect: {error}, \
                             retrying in {}s",
                            self.backoff_secs
                        );
                        self.sleep().await;
                        continue;
                    }
                }
            }
            let result = match self.client.as_mut() {
                Some(client) => client.next_message().await,
                None => continue,
            };
            match result {
                Ok(message) => {
                    self.backoff_secs = 1;
                    return Ok(message);
                }
                Err(error @ ClientError::Server { .. }) => return Err(error),
                Err(error @ ClientError::Protocol(_)) => return Err(error),
                Err(error @ ClientError::Timeout { .. }) => return Err(error),
                Err(error) => {
                    eprintln!(
                        "chattrs: watch disconnected: {error}, retrying in {}s",
                        self.backoff_secs
                    );
                    self.client = None;
                    self.sleep().await;
                }
            }
        }
    }

    async fn sleep(&mut self) {
        tokio::time::sleep(std::time::Duration::from_secs(self.backoff_secs)).await;
        self.backoff_secs = next_backoff_secs(self.backoff_secs);
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
}
