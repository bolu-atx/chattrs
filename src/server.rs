//! chattrs — the broker: hello, send, reply, inbox, ack, watch, wait,
//! agents, and log.
//!
//! One `Broker` owns a `Store` and the `watch`/`wait` registries. Each
//! connection runs its own task with a reader loop: the first frame must be
//! `hello`, then the peer may `send`, `inbox`, `ack`, `watch`, or `wait`.
//! An error before `hello` closes the connection; after `hello` it does
//! not. Every accepted send is committed to SQLite before the broker writes
//! `sent`, and each send also reaches the live `watch` connections and the
//! matching `wait` connections of its recipient.

use crate::model::{self, Inbound, Message, Outbound};
use crate::protocol;
use crate::store::Store;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

pub struct Broker {
    store: Store,
    watchers: Mutex<HashMap<String, Vec<mpsc::UnboundedSender<Message>>>>,
    topic_watchers: Mutex<HashMap<String, Vec<mpsc::UnboundedSender<Message>>>>,
    waiters: Mutex<HashMap<String, Vec<Waiter>>>,
    token: Option<String>,
}

/// One parked `wait`: the reply id it blocks on and the channel that
/// wakes its connection when the reply arrives.
struct Waiter {
    reply_to: u64,
    sender: mpsc::UnboundedSender<Message>,
}

impl Broker {
    pub fn new(store: Store, token: Option<String>) -> Broker {
        Broker {
            store,
            watchers: Mutex::new(HashMap::new()),
            topic_watchers: Mutex::new(HashMap::new()),
            waiters: Mutex::new(HashMap::new()),
            token,
        }
    }

    /// Compare a presented token against the expected one without
    /// short-circuiting on the first difference.
    fn token_matches(presented: Option<&str>, expected: &str) -> bool {
        let Some(presented) = presented else {
            return false;
        };
        let got = presented.as_bytes();
        let want = expected.as_bytes();
        let longest = got.len().max(want.len());
        let mut diff = got.len() ^ want.len();
        for i in 0..longest {
            let pair = got.get(i).copied().unwrap_or(0) ^ want.get(i).copied().unwrap_or(0);
            diff |= pair as usize;
        }
        diff == 0
    }

    /// Reject the `hello` token when this broker requires one. Returns true
    /// when the connection may continue.
    async fn check_token(&self, writer: &mut OwnedWriteHalf, presented: Option<&str>) -> bool {
        let Some(expected) = self.token.as_deref() else {
            return true;
        };
        if Self::token_matches(presented, expected) {
            return true;
        }
        write_error(writer, Outbound::error("bad_token", "token is missing or incorrect")).await;
        false
    }

    /// Hand one stored message to every live `watch` connection of its
    /// recipient. Dead connections are pruned; nothing is waited on.
    fn notify(&self, recipient: &str, message: Message) {
        let Ok(mut watchers) = self.watchers.lock() else {
            return;
        };
        let Some(list) = watchers.get_mut(recipient) else {
            return;
        };
        list.retain(|sender| !sender.is_closed());
        for sender in list.iter() {
            let _ = sender.send(message.clone());
        }
    }

    /// Wake every parked `wait` on `recipient` whose `reply_to` matches
    /// this message. One `wait` gets one message, so matching entries are
    /// removed. A message wakes every waiter on its id, not just the first.
    fn notify_waiters(&self, recipient: &str, message: &Message) {
        let Some(wanted) = message.reply_to else {
            return;
        };
        let Ok(mut waiters) = self.waiters.lock() else {
            return;
        };
        let Some(list) = waiters.get_mut(recipient) else {
            return;
        };
        for waiter in list.iter() {
            if waiter.reply_to == wanted {
                let _ = waiter.sender.send(message.clone());
            }
        }
        list.retain(|waiter| waiter.reply_to != wanted);
        if list.is_empty() {
            waiters.remove(recipient);
        }
    }

    /// Drop one parked `wait` for this address and id. Registration pushes
    /// last, so the last match is the caller's own entry.
    fn remove_one_waiter(&self, address: &str, reply_to: u64) {
        let Ok(mut waiters) = self.waiters.lock() else {
            return;
        };
        let remove_list = match waiters.get_mut(address) {
            Some(list) => list,
            None => return,
        };
        if let Some(pos) = remove_list.iter().rposition(|waiter| waiter.reply_to == reply_to) {
            remove_list.remove(pos);
        }
        if remove_list.is_empty() {
            waiters.remove(address);
        }
    }

    /// Hand one announcement to every live topic `watch` connection of
    /// its room. Direct messages never reach topic watchers, and
    /// announcements never reach inbox watchers. Dead connections are
    /// pruned; nothing is waited on.
    fn notify_topic(&self, topic: &str, message: Message) {
        let Ok(mut watchers) = self.topic_watchers.lock() else {
            return;
        };
        let Some(list) = watchers.get_mut(topic) else {
            return;
        };
        list.retain(|sender| !sender.is_closed());
        for sender in list.iter() {
            let _ = sender.send(message.clone());
        }
    }

    /// Forget the closed `watch` senders of the listed topics.
    fn untopic(&self, topics: &[String]) {
        let Ok(mut watchers) = self.topic_watchers.lock() else {
            return;
        };
        for topic in topics {
            let prune = match watchers.get_mut(topic) {
                Some(list) => {
                    list.retain(|sender| !sender.is_closed());
                    list.is_empty()
                }
                None => false,
            };
            if prune {
                watchers.remove(topic);
            }
        }
    }

    /// Forget the closed `watch` senders of one address.
    fn unwatch(&self, address: &str) {
        let Ok(mut watchers) = self.watchers.lock() else {
            return;
        };
        let prune = match watchers.get_mut(address) {
            Some(list) => {
                list.retain(|sender| !sender.is_closed());
                list.is_empty()
            }
            None => false,
        };
        if prune {
            watchers.remove(address);
        }
    }

    // Over the complexity limit (126/40). Issue #34 step 7 splits this
    // function and deletes this allow. Do not add another one.
    #[allow(clippy::cognitive_complexity)]
    async fn handle(self: Arc<Self>, stream: TcpStream) {
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        let mut peer: Option<String> = None;
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            if line.len() > model::MAX_LINE_BYTES {
                let frame = Outbound::error("too_large", "inbound line is too large");
                write_error(&mut write_half, frame).await;
                if peer.is_none() {
                    break;
                }
                continue;
            }
            let frame: Inbound = match protocol::decode(&line) {
                Ok(frame) => frame,
                Err(e) => {
                    let frame = Outbound::error("bad_frame", format!("bad frame: {e}"));
                    write_error(&mut write_half, frame).await;
                    if peer.is_none() {
                        break;
                    }
                    continue;
                }
            };
            match peer.clone() {
                None => match frame {
                    Inbound::Hello { v, address, token } => {
                        if !self.check_token(&mut write_half, token.as_deref()).await {
                            break;
                        }
                        if v != model::PROTOCOL_VERSION {
                            write_error(
                                &mut write_half,
                                Outbound::error("bad_version", "unsupported version, expected 1"),
                            )
                            .await;
                            break;
                        }
                        if !model::is_valid_address(&address) {
                            write_error(
                                &mut write_half,
                                Outbound::error("bad_address", "address is invalid"),
                            )
                            .await;
                            break;
                        }
                        if let Err(e) = self.store.touch_agent(address.clone()).await {
                            write_error(&mut write_half, Outbound::error("internal", e)).await;
                            break;
                        }
                        peer = Some(address.clone());
                        write_frame(
                            &mut write_half,
                            &Outbound::Welcome {
                                server: protocol::SERVER_NAME.to_string(),
                                v: model::PROTOCOL_VERSION,
                                address,
                            },
                        )
                        .await;
                    }
                    Inbound::Send { .. }
                    | Inbound::Inbox
                    | Inbound::Ack { .. }
                    | Inbound::Watch { .. }
                    | Inbound::Wait { .. }
                    | Inbound::Agents
                    | Inbound::Log { .. } => {
                        write_error(
                            &mut write_half,
                            Outbound::error("hello_required", "hello must be the first frame"),
                        )
                        .await;
                        break;
                    }
                },
                Some(from) => match frame {
                    Inbound::Hello { v, address, token } => {
                        if !self.check_token(&mut write_half, token.as_deref()).await {
                            break;
                        }
                        if v != model::PROTOCOL_VERSION {
                            write_error(
                                &mut write_half,
                                Outbound::error("bad_version", "unsupported version, expected 1"),
                            )
                            .await;
                            continue;
                        }
                        if !model::is_valid_address(&address) {
                            write_error(
                                &mut write_half,
                                Outbound::error("bad_address", "address is invalid"),
                            )
                            .await;
                            continue;
                        }
                        if let Err(e) = self.store.touch_agent(address.clone()).await {
                            write_error(&mut write_half, Outbound::error("internal", e)).await;
                            continue;
                        }
                        peer = Some(address.clone());
                        write_frame(
                            &mut write_half,
                            &Outbound::Welcome {
                                server: protocol::SERVER_NAME.to_string(),
                                v: model::PROTOCOL_VERSION,
                                address,
                            },
                        )
                        .await;
                    }
                    Inbound::Send { to, body, reply_to, topic } => {
                        if let Some(topic) = topic.as_deref() {
                            if !model::is_valid_topic(topic) {
                                write_error(
                                    &mut write_half,
                                    Outbound::error("bad_topic", "topic is invalid"),
                                )
                                .await;
                                continue;
                            }
                        }
                        if let Some(to) = to.as_deref() {
                            if !model::is_valid_address(to) {
                                write_error(
                                    &mut write_half,
                                    Outbound::error("bad_address", "address is invalid"),
                                )
                                .await;
                                continue;
                            }
                        }
                        if body.is_empty() {
                            write_error(
                                &mut write_half,
                                Outbound::error("empty_body", "body is empty"),
                            )
                            .await;
                            continue;
                        }
                        if body.len() > model::MAX_BODY_BYTES {
                            write_error(
                                &mut write_half,
                                Outbound::error("too_large", "body is too large"),
                            )
                            .await;
                            continue;
                        }
                        let saved_reply_to = reply_to;
                        let mut parent_topic: Option<String> = None;
                        let recipient: Option<String> = match (to, reply_to) {
                            (Some(to), reply_to) => {
                                if let Some(id) = reply_to {
                                    match self.store.get(id).await {
                                        Ok(Some(original)) => {
                                            parent_topic = original.topic.clone();
                                        }
                                        Ok(None) => {
                                            write_error(
                                                &mut write_half,
                                                Outbound::error(
                                                    "unknown_message",
                                                    "no message has that id",
                                                ),
                                            )
                                            .await;
                                            continue;
                                        }
                                        Err(e) => {
                                            write_error(
                                                &mut write_half,
                                                Outbound::error("internal", e),
                                            )
                                            .await;
                                            continue;
                                        }
                                    }
                                }
                                Some(to)
                            }
                            (None, Some(id)) => match self.store.get(id).await {
                                Ok(Some(original)) => {
                                    parent_topic = original.topic.clone();
                                    if original.to.as_deref() != Some(from.as_str()) {
                                        write_error(
                                            &mut write_half,
                                            Outbound::error(
                                                "not_recipient",
                                                "that message was not sent to you",
                                            ),
                                        )
                                        .await;
                                        continue;
                                    }
                                    Some(original.from)
                                }
                                Ok(None) => {
                                    write_error(
                                        &mut write_half,
                                        Outbound::error(
                                            "unknown_message",
                                            "no message has that id",
                                        ),
                                    )
                                    .await;
                                    continue;
                                }
                                Err(e) => {
                                    write_error(&mut write_half, Outbound::error("internal", e))
                                        .await;
                                    continue;
                                }
                            },
                            (None, None) => {
                                if topic.is_none() {
                                    write_error(
                                        &mut write_half,
                                        Outbound::error("bad_address", "recipient is missing"),
                                    )
                                    .await;
                                    continue;
                                }
                                None
                            }
                        };
                        let stored_topic = topic.or(parent_topic);
                        match self
                            .store
                            .insert(
                                from.clone(),
                                recipient.clone(),
                                body,
                                saved_reply_to,
                                stored_topic,
                            )
                            .await
                        {
                            Ok(id) => {
                                if let Ok(Some(message)) = self.store.get(id).await {
                                    match recipient.as_deref() {
                                        Some(address) => {
                                            self.notify(address, message.clone());
                                            self.notify_waiters(address, &message);
                                        }
                                        None => {
                                            if let Some(room) = message.topic.clone() {
                                                self.notify_topic(&room, message);
                                            }
                                        }
                                    }
                                }
                                write_frame(&mut write_half, &Outbound::Sent { id }).await;
                            }
                            Err(e) => {
                                write_error(&mut write_half, Outbound::error("internal", e)).await;
                                continue;
                            }
                        }
                    }
                    Inbound::Inbox => match self.store.inbox(from.clone()).await {
                        Ok(messages) => {
                            write_frame(&mut write_half, &Outbound::Inbox { messages }).await;
                        }
                        Err(e) => {
                            write_error(&mut write_half, Outbound::error("internal", e)).await;
                            continue;
                        }
                    },
                    Inbound::Ack { ids } => match self.store.ack(from.clone(), ids).await {
                        Ok(ids) => {
                            write_frame(&mut write_half, &Outbound::Acked { ids }).await;
                        }
                        Err(e) => {
                            write_error(&mut write_half, Outbound::error("internal", e)).await;
                            continue;
                        }
                    },
                    Inbound::Agents => match self.store.agents().await {
                        Ok(agents) => {
                            write_frame(&mut write_half, &Outbound::Agents { agents }).await;
                        }
                        Err(e) => {
                            write_error(&mut write_half, Outbound::error("internal", e)).await;
                            continue;
                        }
                    },
                    Inbound::Log { address, after, limit } => {
                        if let Some(wanted) = address.as_deref() {
                            if !model::is_valid_address(wanted) {
                                write_error(
                                    &mut write_half,
                                    Outbound::error("bad_address", "address is invalid"),
                                )
                                .await;
                                continue;
                            }
                        }
                        let limit = limit.min(model::MAX_LOG_LIMIT);
                        match self.store.log(address.clone(), after, limit).await {
                            Ok(messages) => {
                                write_frame(&mut write_half, &Outbound::Log { messages }).await;
                            }
                            Err(e) => {
                                write_error(&mut write_half, Outbound::error("internal", e)).await;
                                continue;
                            }
                        }
                    }
                    Inbound::Wait { reply_to } => {
                        match self.store.find_reply(from.clone(), reply_to).await {
                            Ok(Some(message)) => {
                                write_frame(&mut write_half, &Outbound::Message(message)).await;
                                continue;
                            }
                            Ok(None) => {}
                            Err(e) => {
                                write_error(&mut write_half, Outbound::error("internal", e)).await;
                                continue;
                            }
                        }
                        let (sender, mut live) = mpsc::unbounded_channel();
                        if let Ok(mut waiters) = self.waiters.lock() {
                            waiters
                                .entry(from.clone())
                                .or_default()
                                .push(Waiter { reply_to, sender });
                        }
                        match self.store.find_reply(from.clone(), reply_to).await {
                            Ok(Some(message)) => {
                                self.remove_one_waiter(&from, reply_to);
                                write_frame(&mut write_half, &Outbound::Message(message)).await;
                                continue;
                            }
                            Ok(None) => {}
                            Err(e) => {
                                self.remove_one_waiter(&from, reply_to);
                                write_error(&mut write_half, Outbound::error("internal", e)).await;
                                continue;
                            }
                        }
                        let mut delivered = false;
                        loop {
                            tokio::select! {
                                result = reader.read_line(&mut line) => {
                                    match result {
                                        Ok(0) | Err(_) => break,
                                        Ok(_) => {}
                                    }
                                    line.clear();
                                }
                                received = live.recv() => {
                                    match received {
                                        Some(message) => {
                                            write_frame(
                                                &mut write_half,
                                                &Outbound::Message(message),
                                            )
                                            .await;
                                            delivered = true;
                                            break;
                                        }
                                        None => break,
                                    }
                                }
                            }
                        }
                        if !delivered {
                            self.remove_one_waiter(&from, reply_to);
                        }
                    }
                    Inbound::Watch { topics } => {
                        if topics.iter().any(|topic| !model::is_valid_topic(topic)) {
                            write_error(
                                &mut write_half,
                                Outbound::error("bad_topic", "topic is invalid"),
                            )
                            .await;
                            continue;
                        }
                        let (sender, mut live) = mpsc::unbounded_channel();
                        if topics.is_empty() {
                            if let Ok(mut watchers) = self.watchers.lock() {
                                watchers.entry(from.clone()).or_default().push(sender);
                            }
                        } else {
                            if let Ok(mut watchers) = self.topic_watchers.lock() {
                                for topic in &topics {
                                    watchers.entry(topic.clone()).or_default().push(sender.clone());
                                }
                            }
                        }
                        let mut highest = 0;
                        let backlog = if topics.is_empty() {
                            self.store.inbox(from.clone()).await
                        } else {
                            self.store.topic_backlog(topics.clone()).await
                        };
                        match backlog {
                            Ok(messages) => {
                                for message in messages {
                                    highest = highest.max(message.id);
                                    write_frame(&mut write_half, &Outbound::Message(message)).await;
                                }
                            }
                            Err(e) => {
                                write_error(&mut write_half, Outbound::error("internal", e)).await;
                                continue;
                            }
                        }
                        loop {
                            tokio::select! {
                                result = reader.read_line(&mut line) => {
                                    match result {
                                        Ok(0) | Err(_) => break,
                                        Ok(_) => {}
                                    }
                                    line.clear();
                                }
                                received = live.recv() => {
                                    match received {
                                        Some(message) => {
                                            if message.id > highest {
                                                highest = message.id;
                                                write_frame(
                                                    &mut write_half,
                                                    &Outbound::Message(message),
                                                )
                                                .await;
                                            }
                                        }
                                        None => break,
                                    }
                                }
                            }
                        }
                        if topics.is_empty() {
                            self.unwatch(&from);
                        } else {
                            self.untopic(&topics);
                        }
                        break;
                    }
                },
            }
        }
        if let Some(address) = peer {
            let _ = self.store.touch_agent(address).await;
        }
        let _ = write_half.shutdown().await;
    }
}

async fn write_frame(writer: &mut OwnedWriteHalf, frame: &Outbound) {
    let line = match protocol::encode(frame) {
        Ok(line) => line,
        Err(_) => return,
    };
    let _ = writer.write_all(line.as_bytes()).await;
    let _ = writer.flush().await;
}

async fn write_error(writer: &mut OwnedWriteHalf, frame: Outbound) {
    write_frame(writer, &frame).await;
}

/// True when `listen` binds a loopback address. Anything else, including
/// `0.0.0.0` and `::`, counts as reachable from the network.
pub fn is_loopback_listen(listen: &str) -> bool {
    listen.parse::<std::net::SocketAddr>().map(|addr| addr.ip().is_loopback()).unwrap_or(false)
}

/// Refuse a network-reachable bind that has no token.
pub fn check_listen(listen: &str, token: Option<&str>) -> Result<(), String> {
    if token.is_none() && !is_loopback_listen(listen) {
        return Err(format!("non-loopback bind {listen} requires a token"));
    }
    Ok(())
}

/// Bind and serve until the process exits.
pub async fn serve(listen: &str, db: &Path, token: Option<String>) -> Result<(), String> {
    check_listen(listen, token.as_deref())?;
    let listener = TcpListener::bind(listen).await.map_err(|e| format!("bind {listen}: {e}"))?;
    let store = Store::open(db.to_path_buf()).await?;
    println!("chattrs: serving on {listen}");
    let broker = Arc::new(Broker::new(store, token));
    run_listener(listener, broker).await
}

async fn run_listener(listener: TcpListener, broker: Arc<Broker>) -> Result<(), String> {
    loop {
        let (stream, _peer) = listener.accept().await.map_err(|e| e.to_string())?;
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            broker.handle(stream).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client;
    use tempfile::TempDir;
    use tokio::io::BufReader as TokioBufReader;
    use tokio::net::TcpStream as TokioTcpStream;

    async fn spawn() -> (String, TempDir, tokio::task::JoinHandle<()>) {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path().join("chattrs.db")).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let broker = Arc::new(Broker::new(store, None));
        let handle = tokio::spawn(async move {
            let _ = run_listener(listener, broker).await;
        });
        (addr, dir, handle)
    }

    async fn spawn_at(db: &Path) -> (String, tokio::task::JoinHandle<()>) {
        let store = Store::open(db.to_path_buf()).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let broker = Arc::new(Broker::new(store, None));
        let handle = tokio::spawn(async move {
            let _ = run_listener(listener, broker).await;
        });
        (addr, handle)
    }

    struct Raw {
        reader: TokioBufReader<tokio::net::tcp::OwnedReadHalf>,
        writer: tokio::net::tcp::OwnedWriteHalf,
    }

    async fn raw_connect(addr: &str) -> Raw {
        let stream = TokioTcpStream::connect(addr).await.unwrap();
        let (read_half, write_half) = stream.into_split();
        Raw { reader: TokioBufReader::new(read_half), writer: write_half }
    }

    async fn raw_send(raw: &mut Raw, line: &str) {
        raw.writer.write_all(line.as_bytes()).await.unwrap();
        raw.writer.flush().await.unwrap();
    }

    async fn raw_recv(raw: &mut Raw) -> Result<Outbound, String> {
        let mut line = String::new();
        let n = raw.reader.read_line(&mut line).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("server closed the connection".to_string());
        }
        protocol::decode(&line)
    }

    async fn raw_hello(raw: &mut Raw, hello: &str) -> Outbound {
        raw_send(raw, hello).await;
        raw_recv(raw).await.unwrap()
    }

    fn error_code(frame: &Outbound) -> &str {
        match frame {
            Outbound::Error { code, .. } => code,
            other => panic!("expected an error frame, got {other:?}"),
        }
    }

    /// One message to `to` with `body` and no reply id or topic: the
    /// general chat. Tests that need a reply id or a topic use struct
    /// update syntax on the result.
    fn outgoing(to: &str, body: &str) -> client::Outgoing {
        client::Outgoing {
            to: Some(to.to_string()),
            body: body.to_string(),
            reply_to: None,
            topic: None,
        }
    }

    #[tokio::test]
    async fn message_survives_broker_restart() {
        let (addr, dir, handle) = spawn().await;
        let db = dir.path().join("chattrs.db");
        let id = client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();
        handle.abort();

        let (addr, handle) = spawn_at(&db).await;
        let (sender, recipient, body): (String, String, String) = rusqlite::Connection::open(&db)
            .unwrap()
            .query_row(
                "SELECT sender, recipient, body FROM messages WHERE id = ?1",
                [id as i64],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("row survives the restart");
        assert_eq!(sender, "a@h");
        assert_eq!(recipient, "b@h");
        assert_eq!(body, "hi");

        let next = client::send(&addr, "a@h", None, outgoing("b@h", "again"), false).await.unwrap();
        assert_eq!(next, id + 1);
        handle.abort();
    }

    #[tokio::test]
    async fn send_returns_increasing_ids() {
        let (addr, _dir, handle) = spawn().await;
        let mut last = 0;
        for i in 0..5 {
            let id = client::send(&addr, "a@h", None, outgoing("b@h", &format!("msg {i}")), false)
                .await
                .unwrap();
            assert!(id > last, "ids increase with send order");
            last = id;
        }
        handle.abort();
    }

    #[tokio::test]
    async fn first_frame_must_be_hello() {
        let (addr, _dir, handle) = spawn().await;
        let mut raw = raw_connect(&addr).await;
        raw_send(
            &mut raw,
            "{\"type\":\"send\",\"to\":\"b@h\",\"body\":\"hi\",\"reply_to\":null}\n",
        )
        .await;
        let frame = raw_recv(&mut raw).await.unwrap();
        assert_eq!(error_code(&frame), "hello_required");
        assert!(raw_recv(&mut raw).await.is_err(), "connection closes after hello_required");
        handle.abort();
    }

    #[tokio::test]
    async fn hello_rejects_wrong_version() {
        let (addr, _dir, handle) = spawn().await;
        let mut raw = raw_connect(&addr).await;
        let frame = raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":2,\"address\":\"a@h\"}\n").await;
        assert_eq!(error_code(&frame), "bad_version");
        handle.abort();
    }

    #[tokio::test]
    async fn hello_rejects_bad_address() {
        let (addr, _dir, handle) = spawn().await;
        for address in ["not-an-address", "a@", "@h", "a@b@c"] {
            let mut raw = raw_connect(&addr).await;
            let hello = format!("{{\"type\":\"hello\",\"v\":1,\"address\":\"{address}\"}}\n");
            let frame = raw_hello(&mut raw, &hello).await;
            assert_eq!(error_code(&frame), "bad_address", "for {address}");
        }
        handle.abort();
    }

    async fn spawn_with_token(token: &str) -> (String, TempDir, tokio::task::JoinHandle<()>) {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path().join("chattrs.db")).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let broker = Arc::new(Broker::new(store, Some(token.to_string())));
        let handle = tokio::spawn(async move {
            let _ = run_listener(listener, broker).await;
        });
        (addr, dir, handle)
    }

    #[tokio::test]
    async fn non_loopback_bind_without_token_refuses_to_start() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        for listen in ["0.0.0.0:0", "[::]:0"] {
            let err = serve(listen, &db, None).await.unwrap_err();
            assert!(err.contains("token"), "got: {err}");
            assert!(!err.contains("s3"), "token never appears: {err}");
        }
        assert!(check_listen("127.0.0.1:0", None).is_ok());
        assert!(check_listen("0.0.0.0:0", Some("s3")).is_ok());
    }

    #[tokio::test]
    async fn loopback_bind_without_token_starts() {
        assert!(check_listen("127.0.0.1:0", None).is_ok());
        let (addr, _dir, handle) = spawn().await;
        let id = client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();
        let messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, id);
        handle.abort();
    }

    #[tokio::test]
    async fn hello_with_wrong_token_is_rejected_and_closed() {
        let (addr, _dir, handle) = spawn_with_token("s3").await;
        let mut raw = raw_connect(&addr).await;
        let frame = raw_hello(
            &mut raw,
            "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\",\"token\":\"wrong\"}\n",
        )
        .await;
        assert_eq!(error_code(&frame), "bad_token");
        assert!(raw_recv(&mut raw).await.is_err(), "connection closes after bad_token");
        handle.abort();
    }

    #[tokio::test]
    async fn hello_with_missing_token_is_rejected_when_broker_has_token() {
        let (addr, _dir, handle) = spawn_with_token("s3").await;
        let mut raw = raw_connect(&addr).await;
        let frame = raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
        assert_eq!(error_code(&frame), "bad_token");
        assert!(raw_recv(&mut raw).await.is_err(), "connection closes after bad_token");
        let err = client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap_err();
        assert!(err.contains("bad_token"), "got: {err}");
        assert!(!err.contains("s3"), "token never appears: {err}");
        handle.abort();
    }

    #[tokio::test]
    async fn hello_with_right_token_is_welcomed() {
        let (addr, _dir, handle) = spawn_with_token("s3").await;
        let id =
            client::send(&addr, "a@h", Some("s3"), outgoing("b@h", "hi"), false).await.unwrap();
        let messages = client::fetch_inbox(&addr, "b@h", Some("s3")).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, id);
        assert_eq!(messages[0].body, "hi");
        handle.abort();
    }

    #[tokio::test]
    async fn send_rejects_empty_body() {
        let (addr, _dir, handle) = spawn().await;
        let err = client::send(&addr, "a@h", None, outgoing("b@h", ""), false).await.unwrap_err();
        assert!(err.contains("empty_body"), "got: {err}");
        handle.abort();
    }

    #[tokio::test]
    async fn send_rejects_oversize_body() {
        let (addr, _dir, handle) = spawn().await;
        let body = "x".repeat(model::MAX_BODY_BYTES + 1);
        let err =
            client::send(&addr, "a@h", None, outgoing("b@h", &body), false).await.unwrap_err();
        assert!(err.contains("too_large"), "got: {err}");
        handle.abort();
    }

    #[tokio::test]
    async fn inbox_returns_only_messages_for_own_address() {
        let (addr, _dir, handle) = spawn().await;
        let to_b = client::send(&addr, "a@h", None, outgoing("b@h", "for b"), false).await.unwrap();
        let to_c = client::send(&addr, "a@h", None, outgoing("c@h", "for c"), false).await.unwrap();

        let b_messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(b_messages.len(), 1);
        assert_eq!(b_messages[0].id, to_b);
        assert_eq!(b_messages[0].from, "a@h");
        assert_eq!(b_messages[0].to.as_deref(), Some("b@h"));
        assert_eq!(b_messages[0].body, "for b");

        let c_messages = client::fetch_inbox(&addr, "c@h", None).await.unwrap();
        assert_eq!(c_messages.len(), 1);
        assert_eq!(c_messages[0].id, to_c);
        assert_eq!(c_messages[0].to.as_deref(), Some("c@h"));
        assert_eq!(c_messages[0].body, "for c");
        handle.abort();
    }

    #[tokio::test]
    async fn inbox_returns_messages_in_id_order() {
        let (addr, _dir, handle) = spawn().await;
        let mut expected = Vec::new();
        for i in 0..3 {
            let id = client::send(&addr, "a@h", None, outgoing("b@h", &format!("msg {i}")), false)
                .await
                .unwrap();
            expected.push(id);
        }
        let messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        let ids: Vec<u64> = messages.iter().map(|message| message.id).collect();
        assert_eq!(ids, expected);
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]), "ids grow in id order");
        handle.abort();
    }

    #[tokio::test]
    async fn unacked_message_is_redelivered_after_disconnect() {
        let (addr, _dir, handle) = spawn().await;
        let id = client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();

        let mut raw = raw_connect(&addr).await;
        raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"b@h\"}\n").await;
        raw_send(&mut raw, "{\"type\":\"inbox\"}\n").await;
        match raw_recv(&mut raw).await.unwrap() {
            Outbound::Inbox { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].id, id);
            }
            other => panic!("expected an inbox frame, got {other:?}"),
        }
        drop(raw);

        let again = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].id, id);
        assert_eq!(again[0].body, "hi");
        handle.abort();
    }

    #[tokio::test]
    async fn acked_message_is_not_redelivered() {
        let (addr, _dir, handle) = spawn().await;
        let id = client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();

        let messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        let acked = client::ack(&addr, "b@h", None, &[id]).await.unwrap();
        assert_eq!(acked, vec![id]);

        let again = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert!(again.is_empty(), "acked message stays gone: {again:?}");
        handle.abort();
    }

    #[tokio::test]
    async fn ack_ignores_ids_of_other_addresses() {
        let (addr, _dir, handle) = spawn().await;
        let to_b = client::send(&addr, "a@h", None, outgoing("b@h", "for b"), false).await.unwrap();
        let to_c = client::send(&addr, "a@h", None, outgoing("c@h", "for c"), false).await.unwrap();

        let acked = client::ack(&addr, "b@h", None, &[to_b, to_c]).await.unwrap();
        assert_eq!(acked, vec![to_b], "only the own id changes");

        let c_messages = client::fetch_inbox(&addr, "c@h", None).await.unwrap();
        assert_eq!(c_messages.len(), 1);
        assert_eq!(c_messages[0].id, to_c);
        handle.abort();
    }

    #[tokio::test]
    async fn reply_goes_to_sender_of_original() {
        let (addr, _dir, handle) = spawn().await;
        let original =
            client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();
        let id = client::reply(&addr, "b@h", None, original, "yo", None).await.unwrap();
        assert!(id > original, "reply gets a fresh id");

        let messages = client::fetch_inbox(&addr, "a@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, id);
        assert_eq!(messages[0].from, "b@h");
        assert_eq!(messages[0].to.as_deref(), Some("a@h"));
        assert_eq!(messages[0].body, "yo");
        handle.abort();
    }

    #[tokio::test]
    async fn reply_carries_reply_to_id() {
        let (addr, _dir, handle) = spawn().await;
        let original =
            client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();
        let id = client::reply(&addr, "b@h", None, original, "yo", None).await.unwrap();

        let messages = client::fetch_inbox(&addr, "a@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, id);
        assert_eq!(messages[0].reply_to, Some(original));
        handle.abort();
    }

    #[tokio::test]
    async fn reply_to_unknown_id_is_rejected() {
        let (addr, _dir, handle) = spawn().await;
        let err = client::reply(&addr, "b@h", None, 9999, "yo", None).await.unwrap_err();
        assert!(err.contains("unknown_message"), "got: {err}");
        let err = client::send(
            &addr,
            "a@h",
            None,
            client::Outgoing { reply_to: Some(9999), ..outgoing("b@h", "hi") },
            false,
        )
        .await
        .unwrap_err();
        assert!(err.contains("unknown_message"), "got: {err}");

        let mut raw = raw_connect(&addr).await;
        raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
        raw_send(&mut raw, "{\"type\":\"send\",\"body\":\"hi\"}\n").await;
        let frame = raw_recv(&mut raw).await.unwrap();
        assert_eq!(error_code(&frame), "bad_address");
        handle.abort();
    }

    #[tokio::test]
    async fn reply_by_non_recipient_is_rejected() {
        let (addr, _dir, handle) = spawn().await;
        let original =
            client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();
        let err = client::reply(&addr, "c@h", None, original, "intrude", None).await.unwrap_err();
        assert!(err.contains("not_recipient"), "got: {err}");

        let id = client::send(
            &addr,
            "c@h",
            None,
            client::Outgoing { reply_to: Some(original), ..outgoing("d@h", "ref") },
            false,
        )
        .await
        .unwrap();
        let messages = client::fetch_inbox(&addr, "d@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, id);
        assert_eq!(messages[0].reply_to, Some(original));
        handle.abort();
    }

    #[tokio::test]
    async fn topic_is_stored_and_shown_in_inbox() {
        let (addr, _dir, handle) = spawn().await;
        let id = client::send(
            &addr,
            "a@h",
            None,
            client::Outgoing { topic: Some("release-26".into()), ..outgoing("b@h", "hi") },
            false,
        )
        .await
        .unwrap();
        let messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, id);
        assert_eq!(messages[0].topic.as_deref(), Some("release-26"));
        let value = serde_json::to_value(&messages[0]).unwrap();
        assert_eq!(value["topic"].as_str(), Some("release-26"));
        let text = model::render_message(&messages[0]);
        assert!(text.contains("topic=\"release-26\""), "wrapper shows the topic: {text}");
        handle.abort();
    }

    #[tokio::test]
    async fn message_without_topic_is_general_chat() {
        let (addr, _dir, handle) = spawn().await;
        let id = client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();
        let messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, id);
        assert_eq!(messages[0].topic, None);
        let value = serde_json::to_value(&messages[0]).unwrap();
        assert!(value["topic"].is_null(), "general chat wires topic null");
        let text = model::render_message(&messages[0]);
        assert!(!text.contains("topic="), "wrapper omits the topic: {text}");
        let history = client::fetch_log(&addr, "reader@h", None, None, false).await.unwrap();
        assert_eq!(history.len(), 1);
        let line = model::render_log_line(&history[0]);
        assert!(!line.contains("(topic"), "log line omits the topic: {line}");
        handle.abort();
    }

    #[tokio::test]
    async fn reply_inherits_topic() {
        let (addr, _dir, handle) = spawn().await;
        let original = client::send(
            &addr,
            "a@h",
            None,
            client::Outgoing { topic: Some("release-26".into()), ..outgoing("b@h", "hi") },
            false,
        )
        .await
        .unwrap();
        let id = client::reply(&addr, "b@h", None, original, "yo", None).await.unwrap();
        let messages = client::fetch_inbox(&addr, "a@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, id);
        assert_eq!(messages[0].reply_to, Some(original));
        assert_eq!(messages[0].topic.as_deref(), Some("release-26"));
        handle.abort();
    }

    #[tokio::test]
    async fn explicit_topic_overrides_inherited() {
        let (addr, _dir, handle) = spawn().await;
        let original = client::send(
            &addr,
            "a@h",
            None,
            client::Outgoing { topic: Some("release-26".into()), ..outgoing("b@h", "hi") },
            false,
        )
        .await
        .unwrap();
        let id =
            client::reply(&addr, "b@h", None, original, "yo", Some("other-topic")).await.unwrap();
        let messages = client::fetch_inbox(&addr, "a@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, id);
        assert_eq!(messages[0].reply_to, Some(original));
        assert_eq!(messages[0].topic.as_deref(), Some("other-topic"));
        handle.abort();
    }

    #[tokio::test]
    async fn ask_reply_shows_the_asks_topic() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let out = dir.path().join("ask.out");
        let err = dir.path().join("ask.err");
        let mut asker =
            start_ask(&bin, &addr, ("w@h", "c@h"), "15", (&out, &err), "go?", Some("release-26"));

        let question = wait_for_question(&addr, "c@h").await;
        assert_eq!(question.body, "go?");
        assert_eq!(question.topic.as_deref(), Some("release-26"));
        client::reply(&addr, "c@h", None, question.id, "yes", None).await.unwrap();

        let status = wait_for_exit(&mut asker.child, 15).await;
        assert_eq!(status.code(), Some(0), "ask exits zero on a reply");
        let lines = read_out_lines(&out);
        assert_eq!(lines.len(), 4, "ask prints one wrapped reply: {lines:?}");
        assert!(lines[0].contains("topic=\"release-26\""), "wrapper shows the topic: {}", lines[0]);
        assert!(lines[2].contains("yes"), "reply carries the body: {}", lines[2]);
    }

    #[tokio::test]
    async fn send_rejects_bad_topic() {
        let (addr, _dir, handle) = spawn().await;
        let mut raw = raw_connect(&addr).await;
        raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
        let long = "a".repeat(65);
        let cases = vec![
            "Release 26".to_string(),
            "-lead".to_string(),
            "trail-".to_string(),
            String::new(),
            long,
        ];
        for topic in &cases {
            let line = format!(
                "{{\"type\":\"send\",\"to\":\"b@h\",\"body\":\"hi\",\"topic\":{}}}\n",
                serde_json::to_string(topic).unwrap()
            );
            raw_send(&mut raw, &line).await;
            let frame = raw_recv(&mut raw).await.unwrap();
            assert_eq!(error_code(&frame), "bad_topic", "topic {topic:?} is rejected");
        }
        let bad = client::Outgoing { topic: Some("Release 26".into()), ..outgoing("b@h", "hi") };
        let err = client::send(&addr, "a@h", None, bad, false).await.unwrap_err();
        assert!(err.contains("bad_topic"), "got: {err}");
        raw_send(&mut raw, "{\"type\":\"send\",\"to\":\"b@h\",\"body\":\"hi\"}\n").await;
        match raw_recv(&mut raw).await.unwrap() {
            Outbound::Sent { .. } => {}
            other => panic!("connection stays open after bad_topic, got {other:?}"),
        }
        handle.abort();
    }

    #[tokio::test]
    async fn topic_survives_broker_restart() {
        let (addr, dir, handle) = spawn().await;
        let db = dir.path().join("chattrs.db");
        let id = client::send(
            &addr,
            "a@h",
            None,
            client::Outgoing { topic: Some("release-26".into()), ..outgoing("b@h", "hi") },
            false,
        )
        .await
        .unwrap();
        handle.abort();

        let (addr, handle) = spawn_at(&db).await;
        let messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, id);
        assert_eq!(messages[0].topic.as_deref(), Some("release-26"));
        handle.abort();
    }

    #[tokio::test]
    async fn existing_db_without_topic_column_is_migrated() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.execute_batch(
                "CREATE TABLE messages (
                   id        INTEGER PRIMARY KEY AUTOINCREMENT,
                   sender    TEXT NOT NULL,
                   recipient TEXT NOT NULL,
                   body      TEXT NOT NULL,
                   ts        INTEGER NOT NULL,
                   reply_to  INTEGER,
                   acked_at  INTEGER
                 );",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO messages (sender, recipient, body, ts, reply_to)
                 VALUES ('a@h', 'b@h', 'old', 1763200000000, NULL)",
                [],
            )
            .unwrap();
        }
        let (addr, handle) = spawn_at(&db).await;
        let messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].body, "old");
        assert_eq!(messages[0].topic, None, "old rows read back with topic null");

        let id = client::send(
            &addr,
            "a@h",
            None,
            client::Outgoing { topic: Some("release-26".into()), ..outgoing("b@h", "new") },
            false,
        )
        .await
        .unwrap();
        let messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        let fresh = messages.iter().find(|message| message.id == id).unwrap();
        assert_eq!(fresh.topic.as_deref(), Some("release-26"));
        handle.abort();
    }

    #[tokio::test]
    async fn topic_shows_in_log() {
        let (addr, _dir, handle) = spawn().await;
        let first = client::send(&addr, "a@h", None, outgoing("b@h", "one"), false).await.unwrap();
        let second = client::send(
            &addr,
            "a@h",
            None,
            client::Outgoing {
                topic: Some("release-26".into()),
                reply_to: Some(first),
                ..outgoing("b@h", "two")
            },
            false,
        )
        .await
        .unwrap();
        let history = client::fetch_log(&addr, "reader@h", None, None, false).await.unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].id, first);
        assert_eq!(history[0].topic, None);
        assert_eq!(history[1].id, second);
        assert_eq!(history[1].topic.as_deref(), Some("release-26"));
        let plain = model::render_log_line(&history[0]);
        assert!(!plain.contains("(topic"), "general chat shows no topic: {plain}");
        let lined = model::render_log_line(&history[1]);
        let wanted = format!("(topic release-26) (re {first})");
        assert!(lined.contains(&wanted), "topic comes before the reply id: {lined}");
        let logged = client::format_log_json(&history[1]).unwrap();
        let value: serde_json::Value = serde_json::from_str(&logged).unwrap();
        assert_eq!(value["topic"].as_str(), Some("release-26"));
        let logged = client::format_log_json(&history[0]).unwrap();
        let value: serde_json::Value = serde_json::from_str(&logged).unwrap();
        assert!(value["topic"].is_null(), "general chat logs null topic");
        handle.abort();
    }

    #[tokio::test]
    async fn watch_topic_receives_announcement_from_another_agent() {
        let (addr, _dir, handle) = spawn().await;
        let mut watcher = raw_connect(&addr).await;
        raw_topic_watch(&mut watcher, "ops@h", &["release-26"]).await;

        let id =
            client::send(&addr, "w@h", None, announce("release-26", "build 421 is red"), false)
                .await
                .unwrap();
        let got = read_watched_message(&mut watcher).await;
        assert_eq!(got.id, id);
        assert_eq!(got.from, "w@h");
        assert_eq!(got.to, None);
        assert_eq!(got.topic.as_deref(), Some("release-26"));
        assert_eq!(got.body, "build 421 is red");
        let value = serde_json::to_value(&got).unwrap();
        assert!(value["to"].is_null(), "the wire message has to null");
        assert_eq!(value["topic"].as_str(), Some("release-26"));
        handle.abort();
    }

    #[tokio::test]
    async fn announcement_needs_no_recipient() {
        let (addr, _dir, handle) = spawn().await;
        let mut raw = raw_connect(&addr).await;
        raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"w@h\"}\n").await;
        raw_send(&mut raw, "{\"type\":\"send\",\"topic\":\"release-26\",\"body\":\"hi\"}\n").await;
        let id = match raw_recv(&mut raw).await.unwrap() {
            Outbound::Sent { id } => id,
            other => panic!("expected a sent frame, got {other:?}"),
        };

        let history = client::fetch_log(&addr, "reader@h", None, None, false).await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].id, id);
        assert_eq!(history[0].to, None);
        assert_eq!(history[0].topic.as_deref(), Some("release-26"));
        let value = serde_json::to_value(&history[0]).unwrap();
        assert!(value["to"].is_null(), "the wire message has to null");
        handle.abort();
    }

    #[tokio::test]
    async fn watcher_only_gets_subscribed_topics() {
        let (addr, _dir, handle) = spawn().await;
        let mut watcher = raw_connect(&addr).await;
        raw_topic_watch(&mut watcher, "ops@h", &["release-26"]).await;

        let other = client::send(&addr, "w@h", None, announce("other-topic", "elsewhere"), false)
            .await
            .unwrap();
        let id =
            client::send(&addr, "w@h", None, announce("release-26", "here"), false).await.unwrap();
        assert!(other < id, "ids grow with send order");

        let got = read_watched_message(&mut watcher).await;
        assert_eq!(got.id, id);
        assert_eq!(got.topic.as_deref(), Some("release-26"));
        expect_no_message(&mut watcher).await;
        handle.abort();
    }

    #[tokio::test]
    async fn watch_accepts_multiple_topics() {
        let (addr, _dir, handle) = spawn().await;
        let first =
            client::send(&addr, "w@h", None, announce("room-a", "one"), false).await.unwrap();
        let second =
            client::send(&addr, "w@h", None, announce("room-b", "two"), false).await.unwrap();
        assert!(first < second, "ids grow with send order");

        let mut watcher = raw_connect(&addr).await;
        raw_topic_watch(&mut watcher, "ops@h", &["room-a", "room-b"]).await;
        let got_first = read_watched_message(&mut watcher).await;
        let got_second = read_watched_message(&mut watcher).await;
        assert_eq!(got_first.id, first);
        assert_eq!(got_first.topic.as_deref(), Some("room-a"));
        assert_eq!(got_second.id, second);
        assert_eq!(got_second.topic.as_deref(), Some("room-b"));
        handle.abort();
    }

    #[tokio::test]
    async fn announcement_appears_in_no_inbox() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;

        client::send(&addr, "w@h", None, announce("release-26", "build 421 is red"), false)
            .await
            .unwrap();
        for who in ["w@h", "ops@h", "fresh@h"] {
            let messages = client::fetch_inbox(&addr, who, None).await.unwrap();
            assert!(messages.is_empty(), "{who} has no announcement: {messages:?}");
        }

        let out = run_chattrs(&bin, "send", &addr, "ops@h", &["--to", "w@h", "hi"]);
        assert!(out.status.success(), "send exits zero: {out:?}");
        let lines = stdout_lines(&out);
        assert_eq!(lines.len(), 1, "no preamble before the new id: {lines:?}");
        lines[0].parse::<u64>().expect("the only line is the new id");

        let messages = client::fetch_inbox(&addr, "w@h", None).await.unwrap();
        assert_eq!(messages.len(), 1, "only the direct message waits: {messages:?}");
        assert_eq!(messages[0].body, "hi");
    }

    #[tokio::test]
    async fn direct_message_with_topic_is_not_broadcast() {
        let (addr, _dir, handle) = spawn().await;
        let mut watcher = raw_connect(&addr).await;
        raw_topic_watch(&mut watcher, "ops@h", &["release-26"]).await;

        let id = client::send(
            &addr,
            "a@h",
            None,
            client::Outgoing { topic: Some("release-26".into()), ..outgoing("b@h", "hi") },
            false,
        )
        .await
        .unwrap();
        expect_no_message(&mut watcher).await;

        let messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, id);
        assert_eq!(messages[0].to.as_deref(), Some("b@h"));
        assert_eq!(messages[0].topic.as_deref(), Some("release-26"));
        handle.abort();
    }

    #[tokio::test]
    async fn topic_watch_replays_recent_then_live_without_duplicates() {
        let (addr, _dir, handle) = spawn().await;
        let first =
            client::send(&addr, "w@h", None, announce("release-26", "one"), false).await.unwrap();
        let second =
            client::send(&addr, "w@h", None, announce("release-26", "two"), false).await.unwrap();

        let mut watcher = raw_connect(&addr).await;
        raw_topic_watch(&mut watcher, "ops@h", &["release-26"]).await;
        let got_first = read_watched_message(&mut watcher).await;
        let got_second = read_watched_message(&mut watcher).await;
        assert_eq!(got_first.id, first);
        assert_eq!(got_second.id, second);

        let third =
            client::send(&addr, "w@h", None, announce("release-26", "three"), false).await.unwrap();
        let got_third = read_watched_message(&mut watcher).await;
        assert_eq!(got_third.id, third);
        assert_eq!([got_first.id, got_second.id, got_third.id], [first, second, third]);
        expect_no_message(&mut watcher).await;
        handle.abort();
    }

    #[tokio::test]
    async fn announcements_survive_broker_restart() {
        let (addr, dir, handle) = spawn().await;
        let db = dir.path().join("chattrs.db");
        let id =
            client::send(&addr, "w@h", None, announce("release-26", "build 421 is red"), false)
                .await
                .unwrap();
        handle.abort();

        let (addr, handle) = spawn_at(&db).await;
        let mut watcher = raw_connect(&addr).await;
        raw_topic_watch(&mut watcher, "ops@h", &["release-26"]).await;
        let got = read_watched_message(&mut watcher).await;
        assert_eq!(got.id, id);
        assert_eq!(got.to, None);
        assert_eq!(got.topic.as_deref(), Some("release-26"));
        assert_eq!(got.body, "build 421 is red");
        handle.abort();
    }

    #[tokio::test]
    async fn announcements_appear_in_log() {
        let (addr, _dir, handle) = spawn().await;
        let id = client::send(
            &addr,
            "codex@box1",
            None,
            announce("release-26", "build 421 is red"),
            false,
        )
        .await
        .unwrap();

        let history = client::fetch_log(&addr, "reader@h", None, None, false).await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].id, id);
        assert_eq!(history[0].to, None);
        assert_eq!(history[0].topic.as_deref(), Some("release-26"));
        let line = model::render_log_line(&history[0]);
        assert!(line.contains("→ topic:release-26:"), "log names the room: {line}");
        assert!(!line.contains("(topic "), "the room is not repeated as a suffix: {line}");

        let by_sender =
            client::fetch_log(&addr, "reader@h", None, Some("codex@box1".into()), false)
                .await
                .unwrap();
        assert_eq!(by_sender.len(), 1, "an address filter still matches the sender");
        let by_other = client::fetch_log(&addr, "reader@h", None, Some("other@h".into()), false)
            .await
            .unwrap();
        assert!(by_other.is_empty(), "an unrelated address matches nothing");
        handle.abort();
    }

    #[tokio::test]
    async fn topic_watch_cli_prints_announcement_as_json() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let out = dir.path().join("topic-watch.out");
        let _watcher = start_topic_watch(&bin, &addr, "ops@h", &["release-26"], &out);

        let id =
            client::send(&addr, "w@h", None, announce("release-26", "build 421 is red"), false)
                .await
                .unwrap();
        let lines = wait_for_lines(&out, 1).await;
        assert_eq!(lines.len(), 1, "watch prints one JSON line: {lines:?}");
        let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
        assert_eq!(value["type"].as_str(), Some("message"));
        assert_eq!(value["id"].as_u64(), Some(id));
        assert_eq!(value["topic"].as_str(), Some("release-26"));
        assert!(value["to"].is_null(), "the announcement has no recipient");
        assert_eq!(value["untrusted"], serde_json::Value::Bool(true));

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let again = read_out_lines(&out);
        assert_eq!(again.len(), 1, "the announcement prints once: {again:?}");

        let pending = client::fetch_inbox(&addr, "ops@h", None).await.unwrap();
        assert!(pending.is_empty(), "the watcher acks nothing: {pending:?}");
    }

    #[tokio::test]
    async fn ask_topic_cli_run_shows_topic_in_reply() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let out = dir.path().join("ask.out");
        let err = dir.path().join("ask.err");
        let mut asker =
            start_ask(&bin, &addr, ("w@h", "c@h"), "15", (&out, &err), "go?", Some("release-26"));

        let question = wait_for_question(&addr, "c@h").await;
        assert_eq!(question.topic.as_deref(), Some("release-26"));
        let id_arg = question.id.to_string();
        let reply_out = run_chattrs(&bin, "reply", &addr, "c@h", &[&id_arg, "yes"]);
        assert!(reply_out.status.success(), "reply exits zero: {reply_out:?}");

        let status = wait_for_exit(&mut asker.child, 15).await;
        assert_eq!(status.code(), Some(0), "ask exits zero on a reply");
        let lines = read_out_lines(&out);
        assert_eq!(lines.len(), 4, "ask prints one wrapped reply: {lines:?}");
        assert!(lines[0].contains("topic=\"release-26\""), "wrapper shows the topic: {}", lines[0]);
    }

    #[tokio::test]
    async fn message_to_offline_address_survives_restart_and_arrives_once() {
        let (addr, dir, handle) = spawn().await;
        let db = dir.path().join("chattrs.db");
        let id = client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();
        handle.abort();

        let (addr, handle) = spawn_at(&db).await;
        let messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, id);
        assert_eq!(messages[0].from, "a@h");
        assert_eq!(messages[0].to.as_deref(), Some("b@h"));
        assert_eq!(messages[0].body, "hi");

        let acked = client::ack(&addr, "b@h", None, &[id]).await.unwrap();
        assert_eq!(acked, vec![id]);
        let again = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert!(again.is_empty(), "message arrives once: {again:?}");
        handle.abort();
    }

    #[tokio::test]
    async fn agents_lists_addresses_that_said_hello() {
        let (addr, _dir, handle) = spawn().await;
        client::send(&addr, "b@h", None, outgoing("a@h", "hi"), false).await.unwrap();
        client::fetch_inbox(&addr, "a@h", None).await.unwrap();

        let agents = client::fetch_agents(&addr, "a@h", None, false).await.unwrap();
        let names: Vec<&str> = agents.iter().map(|agent| agent.address.as_str()).collect();
        assert_eq!(names, vec!["a@h", "b@h"]);
        assert!(agents.iter().all(|agent| agent.last_seen > 0), "{agents:?}");
        handle.abort();
    }

    #[tokio::test]
    async fn agents_omits_addresses_that_only_received() {
        let (addr, _dir, handle) = spawn().await;
        client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();

        let agents = client::fetch_agents(&addr, "a@h", None, false).await.unwrap();
        let names: Vec<&str> = agents.iter().map(|agent| agent.address.as_str()).collect();
        assert_eq!(names, vec!["a@h"]);
        handle.abort();
    }

    #[tokio::test]
    async fn last_seen_updates_on_disconnect() {
        let (addr, _dir, handle) = spawn().await;
        let mut raw = raw_connect(&addr).await;
        raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
        let first = seen(&addr, "a@h").await;
        assert!(first > 0, "hello records a last_seen");

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        drop(raw);

        let start = tokio::time::Instant::now();
        loop {
            let last_seen = seen(&addr, "a@h").await;
            if last_seen > first {
                break;
            }
            assert!(
                start.elapsed() < std::time::Duration::from_secs(10),
                "disconnect never updated last_seen"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        handle.abort();
    }

    async fn seen(server: &str, address: &str) -> u64 {
        client::fetch_agents(server, "observer@h", None, false)
            .await
            .unwrap()
            .into_iter()
            .find(|agent| agent.address == address)
            .unwrap()
            .last_seen
    }

    #[tokio::test]
    async fn agents_survive_broker_restart() {
        let (addr, dir, handle) = spawn().await;
        let db = dir.path().join("chattrs.db");
        client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();
        handle.abort();

        let (addr, handle) = spawn_at(&db).await;
        let agents = client::fetch_agents(&addr, "a@h", None, false).await.unwrap();
        assert!(agents.iter().any(|agent| agent.address == "a@h"), "agents survive: {agents:?}");
        handle.abort();
    }

    #[tokio::test]
    async fn log_returns_all_messages_oldest_first() {
        let (addr, _dir, handle) = spawn().await;
        let first = client::send(&addr, "a@h", None, outgoing("b@h", "one"), false).await.unwrap();
        let second = client::reply(&addr, "b@h", None, first, "two", None).await.unwrap();
        let third =
            client::send(&addr, "c@h", None, outgoing("a@h", "three"), false).await.unwrap();

        let history = client::fetch_log(&addr, "reader@h", None, None, false).await.unwrap();
        let ids: Vec<u64> = history.iter().map(|message| message.id).collect();
        assert_eq!(ids, vec![first, second, third]);
        assert_eq!(history[0].from, "a@h");
        assert_eq!(history[0].to.as_deref(), Some("b@h"));
        assert_eq!(history[0].body, "one");
        assert_eq!(history[0].reply_to, None);
        assert_eq!(history[1].from, "b@h");
        assert_eq!(history[1].to.as_deref(), Some("a@h"));
        assert_eq!(history[1].reply_to, Some(first));
        assert_eq!(history[2].from, "c@h");
        handle.abort();
    }

    #[tokio::test]
    async fn log_filters_by_sender_or_recipient() {
        let (addr, _dir, handle) = spawn().await;
        let ab = client::send(&addr, "a@h", None, outgoing("b@h", "a to b"), false).await.unwrap();
        let ba = client::send(&addr, "b@h", None, outgoing("a@h", "b to a"), false).await.unwrap();
        let ac = client::send(&addr, "a@h", None, outgoing("c@h", "a to c"), false).await.unwrap();
        let xy = client::send(&addr, "x@h", None, outgoing("y@h", "x to y"), false).await.unwrap();

        async fn filtered(addr: &str, who: &str) -> Vec<u64> {
            client::fetch_log(addr, "reader@h", None, Some(who.to_string()), false)
                .await
                .unwrap()
                .into_iter()
                .map(|message| message.id)
                .collect()
        }
        assert_eq!(filtered(&addr, "a@h").await, vec![ab, ba, ac]);
        assert_eq!(filtered(&addr, "b@h").await, vec![ab, ba]);
        assert_eq!(filtered(&addr, "y@h").await, vec![xy]);

        let all = client::fetch_log(&addr, "reader@h", None, None, false).await.unwrap();
        assert_eq!(all.len(), 4);
        handle.abort();
    }

    #[tokio::test]
    async fn log_includes_acked_and_unread_messages() {
        let (addr, _dir, handle) = spawn().await;
        let first = client::send(&addr, "a@h", None, outgoing("b@h", "one"), false).await.unwrap();
        let second = client::send(&addr, "a@h", None, outgoing("b@h", "two"), false).await.unwrap();
        client::ack(&addr, "b@h", None, &[first]).await.unwrap();

        let history = client::fetch_log(&addr, "a@h", None, None, false).await.unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].id, first);
        assert!(history[0].acked_at.is_some_and(|ts| ts > 0), "acked message carries acked_at");
        assert_eq!(history[1].id, second);
        assert_eq!(history[1].acked_at, None, "unread message has null acked_at");
        handle.abort();
    }

    #[tokio::test]
    async fn log_does_not_ack() {
        let (addr, _dir, handle) = spawn().await;
        let id = client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();

        let history = client::fetch_log(&addr, "a@h", None, None, false).await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].id, id);

        let messages = client::fetch_inbox(&addr, "b@h", None).await.unwrap();
        assert_eq!(messages.len(), 1, "log leaves the message unread: {messages:?}");
        assert_eq!(messages[0].id, id);
        handle.abort();
    }

    #[tokio::test]
    async fn log_pages_past_500_messages() {
        let (addr, _dir, handle) = spawn().await;
        let mut expected = Vec::new();
        for i in 0..1200 {
            let id = client::send(&addr, "a@h", None, outgoing("b@h", &format!("msg {i}")), false)
                .await
                .unwrap();
            expected.push(id);
        }

        let mut raw = raw_connect(&addr).await;
        raw_hello(&mut raw, "{\"type\":\"hello\",\"v\":1,\"address\":\"a@h\"}\n").await;
        raw_send(&mut raw, "{\"type\":\"log\",\"after\":0,\"limit\":5000}\n").await;
        match raw_recv(&mut raw).await.unwrap() {
            Outbound::Log { messages } => {
                assert_eq!(messages.len(), 500, "broker caps one page at 500");
            }
            other => panic!("expected a log frame, got {other:?}"),
        }
        drop(raw);

        let history = client::fetch_log(&addr, "a@h", None, None, false).await.unwrap();
        let ids: Vec<u64> = history.iter().map(|message| message.id).collect();
        assert_eq!(ids, expected, "all 1200 messages return once, in order");
        handle.abort();
    }

    #[tokio::test]
    async fn log_survives_broker_restart() {
        let (addr, dir, handle) = spawn().await;
        let db = dir.path().join("chattrs.db");
        let first = client::send(&addr, "a@h", None, outgoing("b@h", "one"), false).await.unwrap();
        let second = client::reply(&addr, "b@h", None, first, "two", None).await.unwrap();
        handle.abort();

        let (addr, handle) = spawn_at(&db).await;
        let history = client::fetch_log(&addr, "a@h", None, None, false).await.unwrap();
        let ids: Vec<u64> = history.iter().map(|message| message.id).collect();
        assert_eq!(ids, vec![first, second]);
        assert_eq!(history[0].body, "one");
        assert_eq!(history[1].body, "two");
        assert_eq!(history[1].reply_to, Some(first));
        handle.abort();
    }

    async fn read_watched_message(raw: &mut Raw) -> Message {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(10), raw_recv(raw))
            .await
            .expect("watch delivers in time")
            .expect("broker answers");
        match frame {
            Outbound::Message(message) => message,
            other => panic!("expected a message frame, got {other:?}"),
        }
    }

    async fn raw_watch(raw: &mut Raw, address: &str) {
        let hello = format!("{{\"type\":\"hello\",\"v\":1,\"address\":\"{address}\"}}\n");
        raw_hello(raw, &hello).await;
        raw_send(raw, "{\"type\":\"watch\"}\n").await;
    }

    /// An announcement to `topic`: no recipient, only a room.
    fn announce(topic: &str, body: &str) -> client::Outgoing {
        client::Outgoing {
            to: None,
            body: body.to_string(),
            reply_to: None,
            topic: Some(topic.to_string()),
        }
    }

    async fn raw_topic_watch(raw: &mut Raw, address: &str, topics: &[&str]) {
        let hello = format!("{{\"type\":\"hello\",\"v\":1,\"address\":\"{address}\"}}\n");
        raw_hello(raw, &hello).await;
        let line = format!(
            "{{\"type\":\"watch\",\"topics\":{}}}\n",
            serde_json::to_string(topics).unwrap()
        );
        raw_send(raw, &line).await;
    }

    /// Fail when any frame arrives within half a second: the quiet
    /// connection must stay quiet.
    async fn expect_no_message(raw: &mut Raw) {
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(500), raw_recv(raw)).await;
        assert!(result.is_err(), "no message arrives, got {result:?}");
    }

    #[tokio::test]
    async fn watch_delivers_unread_then_live_messages_in_order() {
        let (addr, _dir, handle) = spawn().await;
        let first = client::send(&addr, "a@h", None, outgoing("b@h", "one"), false).await.unwrap();
        let second = client::send(&addr, "a@h", None, outgoing("b@h", "two"), false).await.unwrap();

        let mut first_watcher = raw_connect(&addr).await;
        raw_watch(&mut first_watcher, "b@h").await;
        let got_first = read_watched_message(&mut first_watcher).await;
        let got_second = read_watched_message(&mut first_watcher).await;
        assert_eq!(got_first.id, first);
        assert_eq!(got_first.body, "one");
        assert_eq!(got_second.id, second);
        assert_eq!(got_second.body, "two");

        let mut second_watcher = raw_connect(&addr).await;
        raw_watch(&mut second_watcher, "b@h").await;
        let again_first = read_watched_message(&mut second_watcher).await;
        let again_second = read_watched_message(&mut second_watcher).await;
        assert_eq!(again_first.id, first);
        assert_eq!(again_second.id, second);

        let third =
            client::send(&addr, "a@h", None, outgoing("b@h", "three"), false).await.unwrap();
        let live_first = read_watched_message(&mut first_watcher).await;
        let live_second = read_watched_message(&mut second_watcher).await;
        assert_eq!(live_first.id, third);
        assert_eq!(live_first.body, "three");
        assert_eq!(live_second.id, third);
        assert!(first < second && second < third, "ids grow in send order");
        handle.abort();
    }

    fn test_bin_path() -> std::path::PathBuf {
        if let Some(path) = option_env!("CARGO_BIN_EXE_chattrs") {
            let path = std::path::PathBuf::from(path);
            if path.exists() {
                return path;
            }
        }
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("target/debug/chattrs{}", std::env::consts::EXE_SUFFIX))
    }

    fn ensure_bin() -> std::path::PathBuf {
        static BUILD_ONCE: std::sync::Once = std::sync::Once::new();
        let path = test_bin_path();
        BUILD_ONCE.call_once(|| {
            let status = std::process::Command::new("cargo")
                .arg("build")
                .arg("--bin")
                .arg("chattrs")
                .status()
                .expect("run cargo build");
            assert!(status.success(), "cargo build failed");
        });
        assert!(path.exists(), "chattrs binary missing at {}", path.display());
        path
    }

    struct ProcChild {
        child: std::process::Child,
    }

    impl ProcChild {
        fn stop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    impl Drop for ProcChild {
        fn drop(&mut self) {
            let _ = self.child.kill();
        }
    }

    fn free_addr() -> String {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
        probe.local_addr().expect("probe addr").to_string()
    }

    fn start_server(bin: &std::path::Path, addr: &str, db: &std::path::Path) -> ProcChild {
        let child = std::process::Command::new(bin)
            .arg("serve")
            .arg("--listen")
            .arg(addr)
            .arg("--db")
            .arg(db)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn serve");
        ProcChild { child }
    }

    fn start_watch(
        bin: &std::path::Path,
        addr: &str,
        who: &str,
        out: &std::path::Path,
    ) -> ProcChild {
        start_topic_watch(bin, addr, who, &[], out)
    }

    fn start_topic_watch(
        bin: &std::path::Path,
        addr: &str,
        who: &str,
        topics: &[&str],
        out: &std::path::Path,
    ) -> ProcChild {
        let file = std::fs::File::create(out).expect("watch output file");
        let mut command = std::process::Command::new(bin);
        command.arg("watch").arg("--server").arg(addr).arg("--address").arg(who);
        for topic in topics {
            command.arg("--topic").arg(topic);
        }
        let child =
            command.stdout(file).stderr(std::process::Stdio::null()).spawn().expect("spawn watch");
        ProcChild { child }
    }

    async fn wait_for_server(addr: &str) {
        let start = tokio::time::Instant::now();
        loop {
            if client::fetch_inbox(addr, "probe@h", None).await.is_ok() {
                return;
            }
            assert!(
                start.elapsed() < std::time::Duration::from_secs(10),
                "server never came up at {addr}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    fn read_out_lines(out: &std::path::Path) -> Vec<String> {
        std::fs::read_to_string(out)
            .unwrap_or_default()
            .lines()
            .map(|line| line.to_string())
            .collect()
    }

    async fn wait_for_lines(out: &std::path::Path, count: usize) -> Vec<String> {
        let start = tokio::time::Instant::now();
        loop {
            let text = std::fs::read_to_string(out).unwrap_or_default();
            if text.ends_with('\n') {
                let lines = read_out_lines(out);
                if lines.len() >= count {
                    return lines;
                }
            }
            assert!(
                start.elapsed() < std::time::Duration::from_secs(15),
                "watch printed no line in time"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    async fn wait_until_inbox_empty(addr: &str, who: &str) {
        let start = tokio::time::Instant::now();
        loop {
            let messages = client::fetch_inbox(addr, who, None).await.expect("fetch inbox");
            if messages.is_empty() {
                return;
            }
            assert!(
                start.elapsed() < std::time::Duration::from_secs(10),
                "inbox never drained: {messages:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn watch_acks_what_it_prints() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let out = dir.path().join("watch.out");
        let _watcher = start_watch(&bin, &addr, "b@h", &out);

        let id = client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();
        let lines = wait_for_lines(&out, 1).await;
        let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
        assert_eq!(value["id"].as_u64(), Some(id));

        wait_until_inbox_empty(&addr, "b@h").await;
    }

    #[tokio::test]
    async fn watch_reconnects_after_broker_restart() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let mut server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let out = dir.path().join("watch.out");
        let _watcher = start_watch(&bin, &addr, "b@h", &out);

        let first = client::send(&addr, "a@h", None, outgoing("b@h", "one"), false).await.unwrap();
        let lines = wait_for_lines(&out, 1).await;
        let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
        assert_eq!(value["id"].as_u64(), Some(first));
        wait_until_inbox_empty(&addr, "b@h").await;

        server.stop();
        let mut server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let second = client::send(&addr, "a@h", None, outgoing("b@h", "two"), false).await.unwrap();
        let lines = wait_for_lines(&out, 2).await;
        let value: serde_json::Value = serde_json::from_str(&lines[1]).expect("JSON line");
        assert_eq!(value["id"].as_u64(), Some(second));
        assert_eq!(value["body"].as_str(), Some("two"));
        wait_until_inbox_empty(&addr, "b@h").await;
        server.stop();
    }

    #[tokio::test]
    async fn watch_stdout_is_json_lines_only() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let out = dir.path().join("watch.out");
        let _watcher = start_watch(&bin, &addr, "b@h", &out);

        let first =
            client::send(&addr, "a@h", None, outgoing("b@h", "plain"), false).await.unwrap();
        let second = client::send(
            &addr,
            "a@h",
            None,
            client::Outgoing { reply_to: Some(first), ..outgoing("b@h", "answer") },
            false,
        )
        .await
        .unwrap();
        let lines = wait_for_lines(&out, 2).await;
        assert_eq!(lines.len(), 2, "watch prints one line per message: {lines:?}");
        let plain: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
        assert_eq!(plain["type"].as_str(), Some("message"));
        assert_eq!(plain["id"].as_u64(), Some(first));
        assert_eq!(plain["from"].as_str(), Some("a@h"));
        assert_eq!(plain["to"].as_str(), Some("b@h"));
        assert_eq!(plain["body"].as_str(), Some("plain"));
        assert!(plain["ts"].as_u64().is_some(), "message carries ts");
        assert!(plain["reply_to"].is_null(), "plain message has no reply_to");
        let answer: serde_json::Value = serde_json::from_str(&lines[1]).expect("JSON line");
        assert_eq!(answer["type"].as_str(), Some("message"));
        assert_eq!(answer["id"].as_u64(), Some(second));
        assert_eq!(answer["reply_to"].as_u64(), Some(first));

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let lines = read_out_lines(&out);
        assert_eq!(lines.len(), 2, "watch prints nothing else: {lines:?}");
    }

    fn start_ask(
        bin: &std::path::Path,
        addr: &str,
        asker: (&str, &str),
        timeout_secs: &str,
        files: (&std::path::Path, &std::path::Path),
        body: &str,
        topic: Option<&str>,
    ) -> ProcChild {
        let out_file = std::fs::File::create(files.0).expect("ask output file");
        let err_file = std::fs::File::create(files.1).expect("ask error file");
        let mut command = std::process::Command::new(bin);
        command
            .arg("ask")
            .arg("--server")
            .arg(addr)
            .arg("--address")
            .arg(asker.0)
            .arg("--to")
            .arg(asker.1)
            .arg("--timeout")
            .arg(timeout_secs);
        if let Some(topic) = topic {
            command.arg("--topic").arg(topic);
        }
        let child = command.arg(body).stdout(out_file).stderr(err_file).spawn().expect("spawn ask");
        ProcChild { child }
    }

    async fn wait_for_exit(child: &mut std::process::Child, secs: u64) -> std::process::ExitStatus {
        let start = tokio::time::Instant::now();
        loop {
            if let Some(status) = child.try_wait().expect("poll ask child") {
                return status;
            }
            assert!(
                start.elapsed() < std::time::Duration::from_secs(secs),
                "ask did not exit in time"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    async fn wait_for_question(addr: &str, who: &str) -> Message {
        let start = tokio::time::Instant::now();
        loop {
            let messages = client::fetch_inbox(addr, who, None).await.expect("fetch inbox");
            if let Some(question) = messages.into_iter().next() {
                return question;
            }
            assert!(start.elapsed() < std::time::Duration::from_secs(10), "question never arrived");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn ask_returns_reply_and_exits_zero() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let out = dir.path().join("ask.out");
        let err = dir.path().join("ask.err");
        let mut asker = start_ask(&bin, &addr, ("w@h", "c@h"), "15", (&out, &err), "go?", None);

        let question = wait_for_question(&addr, "c@h").await;
        assert_eq!(question.from, "w@h");
        assert_eq!(question.body, "go?");
        client::reply(&addr, "c@h", None, question.id, "yes", None).await.unwrap();

        let status = wait_for_exit(&mut asker.child, 15).await;
        assert_eq!(status.code(), Some(0), "ask exits zero on a reply");
        let lines = read_out_lines(&out);
        assert_eq!(lines.len(), 4, "ask prints one wrapped reply: {lines:?}");
        assert!(lines[0].contains("c@h"), "reply names the replier: {}", lines[0]);
        assert!(lines[2].contains("yes"), "reply carries the body: {}", lines[2]);
        let wanted = format!("reply-to=\"{}\"", question.id);
        assert!(lines[0].contains(&wanted), "reply cites the question: {}", lines[0]);
        assert_eq!(lines[3], "</agent-message>", "wrapper closes last: {lines:?}");
    }

    #[tokio::test]
    async fn ask_times_out_with_exit_code_two() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let out = dir.path().join("ask.out");
        let err = dir.path().join("ask.err");
        let mut asker = start_ask(&bin, &addr, ("w@h", "c@h"), "1", (&out, &err), "go?", None);

        let status = wait_for_exit(&mut asker.child, 10).await;
        assert_eq!(status.code(), Some(2), "ask exits 2 on a timeout");
        let lines = read_out_lines(&out);
        assert!(lines.is_empty(), "no reply means no stdout: {lines:?}");
        let question = wait_for_question(&addr, "c@h").await;
        let stderr = std::fs::read_to_string(&err).unwrap_or_default();
        let wanted = format!("timeout: no reply to {}", question.id);
        assert!(stderr.contains(&wanted), "stderr names the id, got: {stderr}");
    }

    #[tokio::test]
    async fn ask_acks_its_reply() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let out = dir.path().join("ask.out");
        let err = dir.path().join("ask.err");
        let mut asker = start_ask(&bin, &addr, ("w@h", "c@h"), "15", (&out, &err), "go?", None);

        let question = wait_for_question(&addr, "c@h").await;
        client::reply(&addr, "c@h", None, question.id, "yes", None).await.unwrap();
        let status = wait_for_exit(&mut asker.child, 15).await;
        assert_eq!(status.code(), Some(0), "ask exits zero on a reply");

        let pending = client::fetch_inbox(&addr, "w@h", None).await.unwrap();
        assert!(pending.is_empty(), "the reply stays acked: {pending:?}");
    }

    #[tokio::test]
    async fn late_reply_stays_unread_after_timeout() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let out = dir.path().join("ask.out");
        let err = dir.path().join("ask.err");
        let mut asker = start_ask(&bin, &addr, ("w@h", "c@h"), "1", (&out, &err), "go?", None);

        let status = wait_for_exit(&mut asker.child, 10).await;
        assert_eq!(status.code(), Some(2), "ask exits 2 on a timeout");
        let question = wait_for_question(&addr, "c@h").await;
        let reply_id = client::reply(&addr, "c@h", None, question.id, "late", None).await.unwrap();

        let pending = client::fetch_inbox(&addr, "w@h", None).await.unwrap();
        assert_eq!(pending.len(), 1, "the late reply stays unread: {pending:?}");
        assert_eq!(pending[0].id, reply_id);
        assert_eq!(pending[0].reply_to, Some(question.id));
        assert_eq!(pending[0].body, "late");
    }

    #[tokio::test]
    async fn ask_ignores_messages_that_are_not_the_reply() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let out = dir.path().join("ask.out");
        let err = dir.path().join("ask.err");
        let mut asker = start_ask(&bin, &addr, ("w@h", "c@h"), "15", (&out, &err), "go?", None);

        let question = wait_for_question(&addr, "c@h").await;
        let noise =
            client::send(&addr, "x@h", None, outgoing("w@h", "noise"), false).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert!(
            asker.child.try_wait().expect("poll ask child").is_none(),
            "noise does not end the wait"
        );
        client::reply(&addr, "c@h", None, question.id, "yes", None).await.unwrap();

        let status = wait_for_exit(&mut asker.child, 15).await;
        assert_eq!(status.code(), Some(0), "ask exits zero on a reply");
        let lines = read_out_lines(&out);
        assert_eq!(lines.len(), 4, "ask prints only the reply: {lines:?}");
        assert!(lines.join("\n").contains("yes"), "ask prints the reply: {lines:?}");
        let pending = client::fetch_inbox(&addr, "w@h", None).await.unwrap();
        assert_eq!(pending.len(), 1, "noise stays unread: {pending:?}");
        assert_eq!(pending[0].id, noise);
        assert_eq!(pending[0].body, "noise");
    }

    #[tokio::test]
    async fn ask_survives_broker_restart() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let mut server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let out = dir.path().join("ask.out");
        let err = dir.path().join("ask.err");
        let mut asker = start_ask(&bin, &addr, ("w@h", "c@h"), "30", (&out, &err), "go?", None);

        let question = wait_for_question(&addr, "c@h").await;
        server.stop();
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        let mut server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        client::reply(&addr, "c@h", None, question.id, "yes", None).await.unwrap();

        let status = wait_for_exit(&mut asker.child, 30).await;
        assert_eq!(status.code(), Some(0), "ask exits zero after a restart");
        let lines = read_out_lines(&out);
        assert_eq!(lines.len(), 4, "ask prints one wrapped reply: {lines:?}");
        assert!(lines.join("\n").contains("yes"), "ask prints the reply: {lines:?}");
        let count: i64 = rusqlite::Connection::open(&db)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM messages WHERE body = 'go?'", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1, "the question exists once in the DB");
        server.stop();
    }

    fn run_chattrs(
        bin: &std::path::Path,
        subcommand: &str,
        addr: &str,
        who: &str,
        extra: &[&str],
    ) -> std::process::Output {
        std::process::Command::new(bin)
            .arg(subcommand)
            .arg("--server")
            .arg(addr)
            .arg("--address")
            .arg(who)
            .args(extra)
            .output()
            .expect("run chattrs")
    }

    fn stdout_lines(out: &std::process::Output) -> Vec<String> {
        String::from_utf8(out.stdout.clone())
            .expect("stdout is utf8")
            .lines()
            .map(|line| line.to_string())
            .collect()
    }

    #[tokio::test]
    async fn send_prints_unread_messages_before_its_id() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let first = client::send(&addr, "a@h", None, outgoing("b@h", "one"), false).await.unwrap();

        let out = run_chattrs(&bin, "send", &addr, "b@h", &["--to", "a@h", "two"]);
        assert!(out.status.success(), "send exits zero: {out:?}");
        let lines = stdout_lines(&out);
        assert_eq!(lines.len(), 5, "wrapped preamble plus the new id: {lines:?}");
        let preamble = lines[..4].join("\n");
        assert!(preamble.contains("one"), "preamble is the unread message: {preamble}");
        assert!(preamble.contains("a@h"), "preamble names the sender: {preamble}");
        let second: u64 = lines[4].parse().expect("last line is the new id");
        assert!(second > first, "the id comes after the preamble: {lines:?}");
    }

    #[tokio::test]
    async fn reply_prints_unread_messages_first() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let original =
            client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();
        let id_arg = original.to_string();

        let out = run_chattrs(&bin, "reply", &addr, "b@h", &[&id_arg, "yo"]);
        assert!(out.status.success(), "reply exits zero: {out:?}");
        let lines = stdout_lines(&out);
        assert_eq!(lines.len(), 5, "wrapped preamble plus the new id: {lines:?}");
        let preamble = lines[..4].join("\n");
        assert!(preamble.contains("hi"), "preamble is the unread message: {preamble}");
        let second: u64 = lines[4].parse().expect("last line is the new id");
        assert!(second > original, "the reply id comes after the preamble: {lines:?}");

        let messages = client::fetch_inbox(&addr, "a@h", None).await.unwrap();
        assert_eq!(messages.len(), 1, "the reply reaches the asker: {messages:?}");
        assert_eq!(messages[0].id, second);
        assert_eq!(messages[0].body, "yo");
    }

    #[tokio::test]
    async fn agents_prints_unread_messages_first() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();

        let out = run_chattrs(&bin, "agents", &addr, "b@h", &[]);
        assert!(out.status.success(), "agents exits zero: {out:?}");
        let lines = stdout_lines(&out);
        assert!(lines.len() >= 5, "wrapped preamble plus agent lines: {lines:?}");
        let preamble = lines[..4].join("\n");
        assert!(preamble.contains("hi"), "preamble is the unread message: {preamble}");
        assert!(
            lines[4..].iter().any(|line| line.contains("a@h")),
            "agents still lists a@h: {lines:?}"
        );
        assert!(
            lines[4..].iter().any(|line| line.contains("b@h")),
            "agents still lists b@h: {lines:?}"
        );
    }

    #[tokio::test]
    async fn ask_prints_unread_before_sending_and_then_the_reply() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        client::send(&addr, "x@h", None, outgoing("w@h", "noise"), false).await.unwrap();
        let out = dir.path().join("ask.out");
        let err = dir.path().join("ask.err");
        let mut asker = start_ask(&bin, &addr, ("w@h", "c@h"), "15", (&out, &err), "go?", None);

        let question = wait_for_question(&addr, "c@h").await;
        client::reply(&addr, "c@h", None, question.id, "yes", None).await.unwrap();

        let status = wait_for_exit(&mut asker.child, 15).await;
        assert_eq!(status.code(), Some(0), "ask exits zero on a reply");
        let lines = read_out_lines(&out);
        assert_eq!(lines.len(), 8, "wrapped preamble plus the wrapped reply: {lines:?}");
        let preamble = lines[..4].join("\n");
        let reply = lines[4..].join("\n");
        assert!(preamble.contains("noise"), "preamble prints what waited: {preamble}");
        assert!(!preamble.contains("yes"), "preamble never contains the reply: {preamble}");
        assert!(reply.contains("yes"), "ask prints the reply: {reply}");
        let wanted = format!("reply-to=\"{}\"", question.id);
        assert!(reply.contains(&wanted), "reply cites the question: {reply}");
    }

    #[tokio::test]
    async fn preamble_acks_what_it_prints() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        client::send(&addr, "a@h", None, outgoing("b@h", "one"), false).await.unwrap();

        let out = run_chattrs(&bin, "send", &addr, "b@h", &["--to", "a@h", "two"]);
        assert!(out.status.success(), "send exits zero: {out:?}");
        let lines = stdout_lines(&out);
        assert_eq!(lines.len(), 5, "wrapped preamble plus the new id: {lines:?}");
        let preamble = lines[..4].join("\n");
        assert!(preamble.contains("one"), "preamble is the unread message: {preamble}");

        let again = run_chattrs(&bin, "send", &addr, "b@h", &["--to", "a@h", "three"]);
        assert!(again.status.success(), "send exits zero: {again:?}");
        let lines = stdout_lines(&again);
        assert_eq!(lines.len(), 1, "a second command prints only an id: {lines:?}");
        lines[0].parse::<u64>().expect("the only line is the new id");
    }

    #[tokio::test]
    async fn watch_has_no_preamble() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        client::send(&addr, "a@h", None, outgoing("b@h", "old"), false).await.unwrap();
        let out = dir.path().join("watch.out");
        let _watcher = start_watch(&bin, &addr, "b@h", &out);

        let lines = wait_for_lines(&out, 1).await;
        let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
        assert_eq!(value["type"].as_str(), Some("message"));
        assert_eq!(value["body"].as_str(), Some("old"));

        client::send(&addr, "a@h", None, outgoing("b@h", "new"), false).await.unwrap();
        let lines = wait_for_lines(&out, 2).await;
        let mut bodies = Vec::new();
        for line in &lines {
            let value: serde_json::Value = serde_json::from_str(line).expect("JSON line");
            assert_eq!(value["type"].as_str(), Some("message"), "watch prints JSON: {line}");
            bodies.push(value["body"].as_str().expect("message carries a body").to_string());
        }
        assert_eq!(bodies, vec!["old", "new"], "each message prints once: {lines:?}");
    }

    #[tokio::test]
    async fn json_preamble_lines_have_type_message() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let first = client::send(&addr, "a@h", None, outgoing("b@h", "one"), false).await.unwrap();

        let out = run_chattrs(&bin, "send", &addr, "b@h", &["--to", "a@h", "--json", "two"]);
        assert!(out.status.success(), "send exits zero: {out:?}");
        let lines = stdout_lines(&out);
        assert_eq!(lines.len(), 2, "preamble plus the sent line: {lines:?}");
        let preamble: serde_json::Value =
            serde_json::from_str(&lines[0]).expect("preamble is JSON");
        assert_eq!(preamble["type"].as_str(), Some("message"), "preamble line: {}", lines[0]);
        assert_eq!(preamble["untrusted"], serde_json::Value::Bool(true));
        assert_eq!(preamble["body"].as_str(), Some("one"));
        assert_eq!(preamble["id"].as_u64(), Some(first));
        let sent: serde_json::Value = serde_json::from_str(&lines[1]).expect("sent is JSON");
        assert_eq!(sent["type"].as_str(), Some("sent"), "own line: {}", lines[1]);
        let id = sent["id"].as_u64().expect("sent carries an id");
        assert!(id > first, "the new id follows the preamble");
    }

    #[tokio::test]
    async fn json_message_lines_carry_untrusted_true() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let first = client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();

        let out = run_chattrs(&bin, "inbox", &addr, "b@h", &["--json"]);
        assert!(out.status.success(), "inbox exits zero: {out:?}");
        let lines = stdout_lines(&out);
        assert_eq!(lines.len(), 1, "one message line: {lines:?}");
        let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
        assert_eq!(value["type"].as_str(), Some("message"), "line: {}", lines[0]);
        assert_eq!(value["id"].as_u64(), Some(first));
        assert_eq!(value["untrusted"], serde_json::Value::Bool(true));
    }

    #[tokio::test]
    async fn watch_lines_carry_untrusted_true() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("chattrs.db");
        let addr = free_addr();
        let bin = ensure_bin();
        let _server = start_server(&bin, &addr, &db);
        wait_for_server(&addr).await;
        let out = dir.path().join("watch.out");
        let _watcher = start_watch(&bin, &addr, "b@h", &out);

        let id = client::send(&addr, "a@h", None, outgoing("b@h", "hi"), false).await.unwrap();
        let lines = wait_for_lines(&out, 1).await;
        assert_eq!(lines.len(), 1, "watch prints one line per message: {lines:?}");
        let value: serde_json::Value = serde_json::from_str(&lines[0]).expect("JSON line");
        assert_eq!(value["type"].as_str(), Some("message"), "line: {}", lines[0]);
        assert_eq!(value["id"].as_u64(), Some(id));
        assert_eq!(value["untrusted"], serde_json::Value::Bool(true));
    }

    #[test]
    fn dist_config_covers_all_targets_and_installers() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let config =
            std::fs::read_to_string(root.join("dist-workspace.toml")).expect("dist config exists");
        for target in [
            "aarch64-apple-darwin",
            "x86_64-unknown-linux-gnu",
            "aarch64-unknown-linux-gnu",
            "x86_64-pc-windows-msvc",
        ] {
            assert!(config.contains(target), "dist builds {target}");
        }
        for installer in ["shell", "powershell", "homebrew", "npm"] {
            assert!(config.contains(installer), "dist makes the {installer} installer");
        }
        assert!(config.contains("checksum"), "dist writes a checksum file");
        let workflow = std::fs::read_to_string(root.join(".github/workflows/release.yml"))
            .expect("the tag-triggered release workflow exists");
        assert!(workflow.contains("autogenerated by dist"), "dist generates the workflow");
        assert!(
            workflow.contains("bolu-atx/homebrew-tap"),
            "the workflow publishes the tap formula"
        );
        assert!(workflow.contains("npm publish"), "the workflow publishes the npm package");
    }

    #[test]
    fn binary_reports_crate_version() {
        let bin = ensure_bin();
        let out = std::process::Command::new(&bin)
            .arg("--version")
            .output()
            .expect("run chattrs --version");
        assert!(out.status.success(), "version exits zero: {out:?}");
        let text = String::from_utf8(out.stdout).expect("version is utf8");
        assert_eq!(
            text.trim(),
            format!("chattrs {}", env!("CARGO_PKG_VERSION")),
            "the binary names its crate version"
        );
    }
}
