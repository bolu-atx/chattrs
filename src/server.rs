//! chattrs — the broker: hello, send, reply, inbox, ack, watch, wait,
//! agents, and log.
//!
//! One `Broker` owns a `Store` and one broadcast channel. Each connection
//! runs its own task: a reader task forwards lines over a channel, and the
//! connection loop selects between the next line and the next pushed
//! message. `watch` and `wait` are state on the connection, so a watching
//! or waiting connection still answers frames such as `ack` and `agents`.
//! An error before `hello` closes the connection; after `hello` it does
//! not, except `bad_token`, which always closes it. Every accepted send is
//! committed to SQLite before the broker writes `sent`, and each stored
//! message is broadcast once for connections to filter by their own state.

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};

use crate::autostart;
use crate::model::{self, Inbound, Message, Outbound};
use crate::protocol;
use crate::store::{self, Store};

/// One broker error: a machine-readable code and a human message.
#[derive(Debug)]
pub struct WireError {
    pub code: String,
    pub message: String,
}

impl WireError {
    fn new(code: &str, message: impl Into<String>) -> WireError {
        WireError { code: code.to_string(), message: message.into() }
    }
}

impl From<String> for WireError {
    fn from(message: String) -> WireError {
        WireError::new("internal", message)
    }
}

pub struct Broker {
    store: Store,
    pushed: broadcast::Sender<Arc<Message>>,
    token: Option<String>,
    /// Live inbox `watch` connections per (project, address). In memory
    /// only.
    watchers: Arc<Mutex<Watchers>>,
}

/// The live inbox watch count for each (project, address).
type Watchers = HashMap<(String, String), usize>;

/// Agents offline longer than this, with no messages in the same time,
/// are hidden from `agents` unless the request sets `all`.
const AGENT_STALE_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// One live inbox watch in the broker's count. Dropping it removes the
/// watch, so EOF, errors, panics, and shutdown all decrement.
struct WatchGuard {
    watchers: Arc<Mutex<Watchers>>,
    key: (String, String),
}

impl Drop for WatchGuard {
    fn drop(&mut self) {
        let mut watchers = self.watchers.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = watchers.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                watchers.remove(&self.key);
            }
        }
    }
}

/// Probe an idle connection after 30 s, every 10 s, so a peer that
/// vanished without a FIN (sleep, lost Wi-Fi) is noticed and closed.
pub(crate) fn set_keepalive(stream: &TcpStream) {
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(30))
        .with_interval(std::time::Duration::from_secs(10));
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd"
    ))]
    let keepalive = keepalive.with_retries(3);
    let _ = socket2::SockRef::from(stream).set_tcp_keepalive(&keepalive);
}

/// State held by one connection: its address after `hello`, what it
/// watches, the highest id sent on it, and the reply ids it waits for.
#[derive(Default)]
struct ConnState {
    address: Option<String>,
    /// The effective project after `hello`: a name, or `*` for an
    /// observer that sees every project.
    project: String,
    /// True after an observer `hello`: never recorded, never online.
    observer: bool,
    /// True after an observer `watch` with `all`: every new message.
    watch_all: bool,
    watch_topics: Option<Vec<String>>,
    /// Held while this connection counts in `Broker::watchers`. Its drop
    /// decrements, on every exit path.
    watch_guard: Option<WatchGuard>,
    highest: u64,
    waits: HashSet<u64>,
    push_rx: Option<broadcast::Receiver<Arc<Message>>>,
}

impl ConnState {
    /// Subscribe to pushed messages when this connection first needs them.
    fn subscribe(&mut self, pushed: &broadcast::Sender<Arc<Message>>) {
        if self.push_rx.is_none() {
            self.push_rx = Some(pushed.subscribe());
        }
    }

    /// Decide whether a pushed message is for this connection, updating
    /// the highest id sent and dropping a satisfied `wait` id.
    fn pushed(&mut self, message: &Message) -> Option<Outbound> {
        let address = self.address.as_deref()?;
        if self.project != model::ALL_PROJECTS && message.project != self.project {
            return None;
        }
        if self.watch_all {
            return Some(Outbound::Message(message.clone()));
        }
        let mut deliver = false;
        if let Some(topics) = &self.watch_topics {
            let matches = if topics.is_empty() {
                message.to.as_deref() == Some(address)
            } else {
                message.to.is_none()
                    && message.topic.as_ref().is_some_and(|topic| topics.contains(topic))
            };
            if matches && message.id > self.highest {
                self.highest = message.id;
                deliver = true;
            }
        }
        if message.to.as_deref() == Some(address) {
            if let Some(reply_to) = message.reply_to {
                if self.waits.remove(&reply_to) {
                    deliver = true;
                }
            }
        }
        deliver.then(|| Outbound::Message(message.clone()))
    }
}

/// The outcome of one bounded line read: a closed peer, a line within the
/// limit, or a line over the limit whose rest was discarded.
enum ReadOutcome {
    Closed,
    Line,
    TooLarge,
}

/// One line forwarded by a connection reader task.
enum LineRead {
    Closed,
    Line(String),
    TooLarge,
}

/// Read one `\n`-ended line without buffering more than
/// `MAX_LINE_BYTES` plus one byte. A line over the limit discards the
/// rest of that line without storing it and reports `TooLarge`.
async fn read_line_bounded(
    reader: &mut BufReader<OwnedReadHalf>,
    line: &mut String,
) -> std::io::Result<ReadOutcome> {
    use tokio::io::AsyncBufReadExt as _;
    line.clear();
    let mut kept: Vec<u8> = Vec::new();
    let mut total: usize = 0;
    loop {
        let (chunk, ended) = {
            let buf = reader.fill_buf().await?;
            if buf.is_empty() {
                if total == 0 {
                    return Ok(ReadOutcome::Closed);
                }
                break;
            }
            match buf.iter().position(|byte| *byte == b'\n') {
                Some(pos) => (buf[..pos + 1].to_vec(), true),
                None => (buf.to_vec(), false),
            }
        };
        reader.consume(chunk.len());
        total += chunk.len();
        let room = (model::MAX_LINE_BYTES + 1).saturating_sub(kept.len());
        kept.extend_from_slice(&chunk[..chunk.len().min(room)]);
        if ended {
            break;
        }
    }
    if total > model::MAX_LINE_BYTES {
        line.clear();
        return Ok(ReadOutcome::TooLarge);
    }
    String::from_utf8(kept)
        .map(|text| {
            line.push_str(&text);
            ReadOutcome::Line
        })
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Forward lines from `read_half` to `sender`, one at a time. This task
/// owns the non-cancel-safe bounded read, so the connection loop never
/// drops it in the middle of a line.
fn spawn_reader(read_half: OwnedReadHalf, sender: mpsc::Sender<LineRead>) {
    tokio::spawn(async move {
        let mut reader = BufReader::new(read_half);
        let mut line = String::new();
        loop {
            let item = match read_line_bounded(&mut reader, &mut line).await {
                Ok(ReadOutcome::Line) => LineRead::Line(line.clone()),
                Ok(ReadOutcome::TooLarge) => LineRead::TooLarge,
                Ok(ReadOutcome::Closed) | Err(_) => LineRead::Closed,
            };
            let closed = matches!(item, LineRead::Closed);
            if sender.send(item).await.is_err() || closed {
                break;
            }
        }
    });
}

/// The next pushed message for a connection, or a lag or close report.
enum Push {
    Message(Arc<Message>),
    Lagged,
    Closed,
}

/// Wait for the next pushed message. With no subscription this never
/// returns, so the connection loop only waits on its reader.
async fn next_push(conn: &mut ConnState) -> Push {
    match &mut conn.push_rx {
        Some(receiver) => match receiver.recv().await {
            Ok(message) => Push::Message(message),
            Err(broadcast::error::RecvError::Lagged(_)) => Push::Lagged,
            Err(broadcast::error::RecvError::Closed) => Push::Closed,
        },
        None => std::future::pending().await,
    }
}

/// The fields of a `hello` frame, handed to `on_hello` as one value.
struct Hello {
    v: u32,
    address: String,
    token: Option<String>,
    tags: Vec<String>,
    observer: bool,
    state: Option<String>,
    project: Option<String>,
}

/// One event for the connection loop: a line from the peer or a push.
enum Event {
    Line(Option<LineRead>),
    Push(Push),
}

impl Broker {
    pub fn new(store: Store, token: Option<String>) -> Broker {
        Broker::with_capacity(store, token, 1024)
    }

    /// A broker whose broadcast holds `capacity` messages; a connection
    /// that falls further behind is dropped.
    fn with_capacity(store: Store, token: Option<String>, capacity: usize) -> Broker {
        let (pushed, _) = broadcast::channel(capacity);
        Broker { store, pushed, token, watchers: Arc::default() }
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

    /// True when a presented token passes this broker's check.
    fn token_ok(&self, presented: Option<&str>) -> bool {
        match self.token.as_deref() {
            None => true,
            Some(expected) => Self::token_matches(presented, expected),
        }
    }

    async fn handle(self: Arc<Self>, stream: TcpStream) {
        let (read_half, mut writer) = stream.into_split();
        let (line_sender, mut lines) = mpsc::channel(1);
        spawn_reader(read_half, line_sender);
        let mut conn = ConnState::default();
        loop {
            let event = tokio::select! {
                item = lines.recv() => Event::Line(item),
                push = next_push(&mut conn) => Event::Push(push),
            };
            let result = match event {
                Event::Line(Some(LineRead::Line(line))) => match protocol::decode(&line) {
                    Ok(frame) => self.dispatch(&mut conn, frame).await,
                    Err(e) => Err(WireError::new("bad_frame", format!("bad frame: {e}"))),
                },
                Event::Line(Some(LineRead::TooLarge)) => {
                    Err(WireError::new("too_large", "inbound line is too large"))
                }
                Event::Line(_) => break,
                Event::Push(Push::Message(message)) => {
                    Ok(conn.pushed(&message).into_iter().collect())
                }
                // A slow observer lags the bounded broadcast and is dropped here, so a
                // send never waits on it. It reconnects and catches up from `log`.
                Event::Push(Push::Lagged) => break,
                Event::Push(Push::Closed) => {
                    conn.push_rx = None;
                    continue;
                }
            };
            let close = match &result {
                Err(error) => conn.address.is_none() || error.code == "bad_token",
                Ok(_) => false,
            };
            write_result(&mut writer, result).await;
            if close {
                break;
            }
        }
        self.count_watch(&mut conn, false);
        if let Some(address) = conn.address.clone().filter(|_| !conn.observer) {
            let _ = self.store.touch_agent(conn.project.clone(), address, Vec::new(), None).await;
        }
        let _ = writer.shutdown().await;
    }

    /// Run one frame and return the frames to write back.
    async fn dispatch(
        &self,
        conn: &mut ConnState,
        frame: Inbound,
    ) -> Result<Vec<Outbound>, WireError> {
        if conn.address.is_none() {
            return match frame {
                Inbound::Hello { v, address, token, tags, observer, state, project } => {
                    let hello = Hello { v, address, token, tags, observer, state, project };
                    self.on_hello(conn, hello).await
                }
                _ => Err(WireError::new("hello_required", "hello must be the first frame")),
            };
        }
        if conn.observer
            && matches!(
                frame,
                Inbound::Send { .. } | Inbound::Ack { .. } | Inbound::Wait { .. } | Inbound::Inbox
            )
        {
            return Err(WireError::new(
                "observer_read_only",
                "an observer connection is read-only",
            ));
        }
        match frame {
            Inbound::Hello { .. } => Err(WireError::new("bad_frame", "hello is already set")),
            Inbound::Send { to, body, reply_to, topic } => {
                self.on_send(conn, to, body, reply_to, topic).await
            }
            Inbound::Inbox => self.on_inbox(conn).await,
            Inbound::Ack { ids } => self.on_ack(conn, ids).await,
            Inbound::Watch { topics, all } => self.on_watch(conn, topics, all).await,
            Inbound::Wait { reply_to } => self.on_wait(conn, reply_to).await,
            Inbound::Agents { all, projects } => self.on_agents(conn, all, projects).await,
            Inbound::Log { address, after, limit } => {
                self.on_log(conn, address, after, limit).await
            }
        }
    }

    async fn on_hello(
        &self,
        conn: &mut ConnState,
        hello: Hello,
    ) -> Result<Vec<Outbound>, WireError> {
        let Hello { v, address, token, tags, observer, state, project } = hello;
        if !self.token_ok(token.as_deref()) {
            return Err(WireError::new("bad_token", "token is missing or incorrect"));
        }
        if v != model::PROTOCOL_VERSION {
            return Err(WireError::new("bad_version", "unsupported version, expected 1"));
        }
        if !model::is_valid_address(&address) {
            return Err(WireError::new("bad_address", "address is invalid"));
        }
        let project = project.unwrap_or_else(|| model::DEFAULT_PROJECT.to_string());
        let project_ok =
            if project == model::ALL_PROJECTS { observer } else { model::is_valid_topic(&project) };
        if !project_ok {
            return Err(WireError::new(
                "bad_project",
                "project is invalid, or `*` without an observer hello",
            ));
        }
        if tags.len() > model::MAX_TAGS || tags.iter().any(|tag| tag.len() > model::MAX_TAG_BYTES) {
            return Err(WireError::new("bad_tags", "too many tags or a tag is too long"));
        }
        if state.as_deref().is_some_and(|state| !model::STATES.contains(&state)) {
            return Err(WireError::new("bad_state", "state must be working, idle, or waiting"));
        }
        if observer && state.is_some() {
            return Err(WireError::new("observer_read_only", "an observer cannot set a state"));
        }
        if !observer {
            self.store.touch_agent(project.clone(), address.clone(), tags, state).await?;
        }
        let watching = !observer && self.is_watched(&project, &address);
        conn.observer = observer;
        conn.address = Some(address.clone());
        conn.project = project.clone();
        Ok(vec![Outbound::Welcome {
            server: protocol::SERVER_NAME.to_string(),
            v: model::PROTOCOL_VERSION,
            address,
            watching,
            observer,
            project: Some(project),
        }])
    }

    async fn on_send(
        &self,
        conn: &mut ConnState,
        to: Option<String>,
        body: String,
        reply_to: Option<u64>,
        topic: Option<String>,
    ) -> Result<Vec<Outbound>, WireError> {
        if let Some(topic) = topic.as_deref() {
            if !model::is_valid_topic(topic) {
                return Err(WireError::new("bad_topic", "topic is invalid"));
            }
        }
        if let Some(to) = to.as_deref() {
            if !model::is_valid_address(to) {
                return Err(WireError::new("bad_address", "address is invalid"));
            }
        }
        if body.is_empty() {
            return Err(WireError::new("empty_body", "body is empty"));
        }
        if body.len() > model::MAX_BODY_BYTES {
            return Err(WireError::new("too_large", "body is too large"));
        }
        let from = conn.address.clone().unwrap_or_default();
        let known = match to.clone() {
            Some(to) => Some(self.store.is_known_agent(conn.project.clone(), to).await?),
            None => None,
        };
        let (recipient, parent_topic) = self.resolve(conn, to, reply_to, &topic).await?;
        let stored_topic = topic.or(parent_topic);
        let message = self
            .store
            .insert(conn.project.clone(), from, recipient, body, reply_to, stored_topic)
            .await?;
        let id = message.id;
        let _ = self.pushed.send(Arc::new(message));
        Ok(vec![Outbound::Sent { id, known }])
    }

    /// Work out who a send goes to and the topic it inherits, if any.
    async fn resolve(
        &self,
        conn: &ConnState,
        to: Option<String>,
        reply_to: Option<u64>,
        topic: &Option<String>,
    ) -> Result<(Option<String>, Option<String>), WireError> {
        let from = conn.address.as_deref().unwrap_or_default();
        let project = &conn.project;
        match (to, reply_to) {
            (Some(to), Some(id)) => {
                let original =
                    self.store.get(project.clone(), id).await?.ok_or_else(|| {
                        WireError::new("unknown_message", "no message has that id")
                    })?;
                Ok((Some(to), original.topic))
            }
            (Some(to), None) => Ok((Some(to), None)),
            (None, Some(id)) => {
                let original =
                    self.store.get(project.clone(), id).await?.ok_or_else(|| {
                        WireError::new("unknown_message", "no message has that id")
                    })?;
                if original.to.as_deref() != Some(from) {
                    return Err(WireError::new(
                        "not_recipient",
                        "that message was not sent to you",
                    ));
                }
                Ok((Some(original.from), original.topic))
            }
            (None, None) => {
                if topic.is_none() {
                    return Err(WireError::new("bad_address", "recipient is missing"));
                }
                Ok((None, None))
            }
        }
    }

    async fn on_inbox(&self, conn: &ConnState) -> Result<Vec<Outbound>, WireError> {
        let from = conn.address.clone().unwrap_or_default();
        let messages = self.store.inbox(conn.project.clone(), from).await?;
        Ok(vec![Outbound::Inbox { messages }])
    }

    async fn on_ack(&self, conn: &ConnState, ids: Vec<u64>) -> Result<Vec<Outbound>, WireError> {
        let from = conn.address.clone().unwrap_or_default();
        let ids = self.store.ack(conn.project.clone(), from, ids).await?;
        Ok(vec![Outbound::Acked { ids }])
    }

    async fn on_watch(
        &self,
        conn: &mut ConnState,
        topics: Vec<String>,
        all: bool,
    ) -> Result<Vec<Outbound>, WireError> {
        if all && !conn.observer {
            return Err(WireError::new("not_observer", "watch all needs an observer hello"));
        }
        if topics.iter().any(|topic| !model::is_valid_topic(topic)) {
            return Err(WireError::new("bad_topic", "topic is invalid"));
        }
        conn.subscribe(&self.pushed);
        if all {
            conn.watch_all = true;
            return Ok(Vec::new());
        }
        conn.watch_topics = Some(topics.clone());
        self.count_watch(conn, topics.is_empty());
        let from = conn.address.clone().unwrap_or_default();
        let backlog = if topics.is_empty() {
            self.store.inbox(conn.project.clone(), from).await?
        } else {
            self.store.topic_backlog(conn.project.clone(), topics).await?
        };
        conn.highest = backlog.iter().map(|message| message.id).max().unwrap_or(0);
        Ok(backlog.into_iter().map(Outbound::Message).collect())
    }

    async fn on_wait(
        &self,
        conn: &mut ConnState,
        reply_to: u64,
    ) -> Result<Vec<Outbound>, WireError> {
        conn.subscribe(&self.pushed);
        let from = conn.address.clone().unwrap_or_default();
        if let Some(message) = self.store.find_reply(conn.project.clone(), from, reply_to).await? {
            return Ok(vec![Outbound::Message(message)]);
        }
        conn.waits.insert(reply_to);
        Ok(Vec::new())
    }

    /// Make this connection count as a live inbox watch, or not. Safe to
    /// call again: the connection counts at most once.
    fn count_watch(&self, conn: &mut ConnState, inbox_watch: bool) {
        let Some(address) = conn.address.clone().filter(|_| !conn.observer) else {
            return;
        };
        if conn.watch_guard.is_some() == inbox_watch {
            return;
        }
        if inbox_watch {
            let key = (conn.project.clone(), address);
            let mut watchers = self.watchers.lock().unwrap_or_else(|e| e.into_inner());
            *watchers.entry(key.clone()).or_insert(0) += 1;
            conn.watch_guard = Some(WatchGuard { watchers: Arc::clone(&self.watchers), key });
        } else {
            conn.watch_guard = None;
        }
    }

    /// True when the address has at least one live inbox watch in the
    /// project.
    fn is_watched(&self, project: &str, address: &str) -> bool {
        let key = (project.to_string(), address.to_string());
        self.watchers.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&key)
    }

    /// The agents of the connection's project (every project for `*`).
    /// Unless `all`, agents that are offline, last seen over 7 days ago,
    /// and silent for 7 days are left out; nothing is deleted. With
    /// `projects`, the answer also carries the counts per project.
    async fn on_agents(
        &self,
        conn: &ConnState,
        all: bool,
        projects: bool,
    ) -> Result<Vec<Outbound>, WireError> {
        let mut agents = self.store.agents(conn.project.clone()).await?;
        for agent in &mut agents {
            agent.online = self.is_watched(&agent.project, &agent.address);
        }
        if !all {
            let cutoff = store::now_ms().saturating_sub(AGENT_STALE_MS);
            let active = self.store.active_since(conn.project.clone(), cutoff).await?;
            agents.retain(|agent| {
                agent.online
                    || agent.last_seen >= cutoff
                    || active.contains(&(agent.project.clone(), agent.address.clone()))
            });
        }
        let projects =
            if projects { Some(self.store.projects(conn.project.clone()).await?) } else { None };
        Ok(vec![Outbound::Agents { agents, projects }])
    }

    async fn on_log(
        &self,
        conn: &ConnState,
        address: Option<String>,
        after: u64,
        limit: u64,
    ) -> Result<Vec<Outbound>, WireError> {
        if let Some(wanted) = address.as_deref() {
            if !model::is_valid_address(wanted) {
                return Err(WireError::new("bad_address", "address is invalid"));
            }
        }
        let limit = limit.min(model::MAX_LOG_LIMIT);
        let messages = self.store.log(conn.project.clone(), address, after, limit).await?;
        Ok(vec![Outbound::Log { messages }])
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

/// Write the frames of a dispatch result, or its error frame.
async fn write_result(writer: &mut OwnedWriteHalf, result: Result<Vec<Outbound>, WireError>) {
    match result {
        Ok(frames) => {
            for frame in &frames {
                write_frame(writer, frame).await;
            }
        }
        Err(error) => {
            write_frame(writer, &Outbound::error(&error.code, error.message)).await;
        }
    }
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

/// Bind and serve until the process exits. A broker started on demand
/// records its pid next to the DB and removes it when it exits.
pub async fn serve(listen: &str, db: &Path, token: Option<String>) -> Result<(), String> {
    check_listen(listen, token.as_deref())?;
    let listener = TcpListener::bind(listen).await.map_err(|e| format!("bind {listen}: {e}"))?;
    let bound = listener.local_addr().map_err(|e| e.to_string())?;
    let store = Store::open(db.to_path_buf()).await?;
    eprintln!("chattrs: serving on {bound}");
    let owned = autostart::is_autostart_child();
    if owned {
        autostart::write_pidfile(db)?;
    }
    let broker = Arc::new(Broker::new(store, token));
    let result = run_listener(listener, broker).await;
    if owned {
        autostart::remove_pidfile(db);
    }
    result
}

async fn run_listener(listener: TcpListener, broker: Arc<Broker>) -> Result<(), String> {
    loop {
        let (stream, _peer) = listener.accept().await.map_err(|e| e.to_string())?;
        set_keepalive(&stream);
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            broker.handle(stream).await;
        });
    }
}
