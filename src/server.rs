//! chattrs — the broker: sessions, history, presence, and broadcast.
//!
//! One `Broker` owns every session behind a mutex. Each connection runs its
//! own task: a reader loop parses inbound frames while a writer task flushes
//! outbound frames (including live broadcasts) down the same socket.

use crate::model::{self, Inbound, Outbound};
use crate::protocol;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

struct Session {
    messages: VecDeque<model::Message>,
    subscribers: Vec<mpsc::UnboundedSender<Outbound>>,
    agents: HashMap<String, usize>,
}

impl Session {
    fn new() -> Session {
        Session { messages: VecDeque::new(), subscribers: Vec::new(), agents: HashMap::new() }
    }

    fn is_empty(&self) -> bool {
        self.messages.is_empty() && self.subscribers.is_empty() && self.agents.is_empty()
    }
}

pub struct Broker {
    sessions: Mutex<HashMap<String, Session>>,
    next_id: AtomicU64,
    history_cap: usize,
}

impl Broker {
    pub fn new(history_cap: usize) -> Broker {
        Broker { sessions: Mutex::new(HashMap::new()), next_id: AtomicU64::new(1), history_cap }
    }

    async fn handle(self: Arc<Self>, stream: TcpStream) {
        let (read_half, write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Outbound>();

        let writer = tokio::spawn(async move {
            let mut write_half = write_half;
            while let Some(frame) = out_rx.recv().await {
                let line = match protocol::encode(&frame) {
                    Ok(line) => line,
                    Err(_) => break,
                };
                if write_half.write_all(line.as_bytes()).await.is_err()
                    || write_half.flush().await.is_err()
                {
                    break;
                }
            }
            let _ = write_half.shutdown().await;
        });

        let mut joined: Option<(String, String)> = None;
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            let frame: Inbound = match protocol::decode(&line) {
                Ok(frame) => frame,
                Err(e) => {
                    let _ = out_tx.send(Outbound::Error { message: format!("bad frame: {e}") });
                    continue;
                }
            };
            match frame {
                Inbound::Hello { agent, session } => {
                    if agent.trim().is_empty() || session.trim().is_empty() {
                        let _ = out_tx.send(Outbound::Error {
                            message: "agent and session must be non-empty".to_string(),
                        });
                        continue;
                    }
                    joined = Some((agent.clone(), session.clone()));
                    self.join(&session, &agent);
                    let _ = out_tx.send(Outbound::Welcome {
                        server: protocol::SERVER_NAME.to_string(),
                        session,
                    });
                }
                Inbound::Send { body, reply_to } => {
                    match self.publish(joined.as_ref(), body, reply_to) {
                        Ok(id) => {
                            let _ = out_tx.send(Outbound::Ack { id });
                        }
                        Err(e) => {
                            let _ = out_tx.send(Outbound::Error { message: e });
                        }
                    }
                }
                Inbound::List { limit, after } => {
                    match self.history(joined.as_ref(), limit, after) {
                        Ok(messages) => {
                            let _ = out_tx.send(Outbound::History { messages });
                        }
                        Err(e) => {
                            let _ = out_tx.send(Outbound::Error { message: e });
                        }
                    }
                }
                Inbound::Watch => match &joined {
                    Some((_, session)) => self.subscribe(session, out_tx.clone()),
                    None => {
                        let _ = out_tx.send(Outbound::Error {
                            message: "send hello before watch".to_string(),
                        });
                    }
                },
                Inbound::Sessions => {
                    let sessions = self.sessions_snapshot();
                    let _ = out_tx.send(Outbound::Sessions { sessions });
                }
            }
        }

        if let Some((agent, session)) = joined {
            self.leave(&session, &agent, &out_tx);
        }
        drop(out_tx);
        let _ = writer.await;
    }

    fn join(&self, session: &str, agent: &str) {
        let mut sessions = self.sessions.lock().unwrap();
        let entry = sessions.entry(session.to_string()).or_insert_with(Session::new);
        *entry.agents.entry(agent.to_string()).or_insert(0) += 1;
    }

    fn leave(&self, session: &str, agent: &str, sender: &mpsc::UnboundedSender<Outbound>) {
        let mut sessions = self.sessions.lock().unwrap();
        let mut remove_session = false;
        if let Some(entry) = sessions.get_mut(session) {
            entry.subscribers.retain(|s| !s.same_channel(sender));
            if let Some(count) = entry.agents.get_mut(agent) {
                *count -= 1;
                if *count == 0 {
                    entry.agents.remove(agent);
                }
            }
            remove_session = entry.is_empty();
        }
        if remove_session {
            sessions.remove(session);
        }
    }

    fn subscribe(&self, session: &str, sender: mpsc::UnboundedSender<Outbound>) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.entry(session.to_string()).or_insert_with(Session::new).subscribers.push(sender);
    }

    fn publish(
        &self,
        joined: Option<&(String, String)>,
        body: String,
        reply_to: Option<u64>,
    ) -> Result<u64, String> {
        let (agent, session) = joined.ok_or("send hello before publishing")?;
        if body.trim().is_empty() {
            return Err("message body is empty".to_string());
        }
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let message = model::Message {
            id,
            session: session.clone(),
            agent: agent.clone(),
            body,
            ts: now_ms(),
            reply_to,
        };
        let subscribers = {
            let mut sessions = self.sessions.lock().unwrap();
            let entry = sessions.entry(session.clone()).or_insert_with(Session::new);
            entry.messages.push_back(message.clone());
            while entry.messages.len() > self.history_cap {
                entry.messages.pop_front();
            }
            entry.subscribers.clone()
        };
        for subscriber in &subscribers {
            let _ = subscriber.send(Outbound::from(message.clone()));
        }
        Ok(id)
    }

    fn history(
        &self,
        joined: Option<&(String, String)>,
        limit: Option<usize>,
        after: Option<u64>,
    ) -> Result<Vec<model::Message>, String> {
        let (_, session) = joined.ok_or("send hello before listing")?;
        let sessions = self.sessions.lock().unwrap();
        let messages: Vec<model::Message> = sessions
            .get(session)
            .map(|entry| {
                entry
                    .messages
                    .iter()
                    .filter(|m| after.is_none_or(|after| m.id > after))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        Ok(match limit {
            Some(limit) => {
                let start = messages.len().saturating_sub(limit);
                messages[start..].to_vec()
            }
            None => messages,
        })
    }

    fn sessions_snapshot(&self) -> Vec<model::SessionInfo> {
        let sessions = self.sessions.lock().unwrap();
        let mut infos: Vec<model::SessionInfo> = sessions
            .iter()
            .map(|(name, entry)| {
                let mut agents: Vec<String> = entry.agents.keys().cloned().collect();
                agents.sort();
                model::SessionInfo {
                    name: name.clone(),
                    agents,
                    message_count: entry.messages.len(),
                }
            })
            .collect();
        infos.sort_by(|a, b| a.name.cmp(&b.name));
        infos
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Bind and serve until the process exits.
pub async fn serve(listen: &str, history_cap: usize) -> Result<(), String> {
    let listener = TcpListener::bind(listen).await.map_err(|e| format!("bind {listen}: {e}"))?;
    println!("chattrs: serving on {listen}");
    let broker = Arc::new(Broker::new(history_cap));
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
    use std::time::Duration;

    async fn spawn() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let broker = Arc::new(Broker::new(500));
        let handle = tokio::spawn(async move {
            let _ = run_listener(listener, broker).await;
        });
        (addr, handle)
    }

    async fn wait_for_agent(addr: &str, session: &str, agent: &str) {
        for _ in 0..100 {
            let sessions = client::sessions(addr).await.unwrap();
            if let Some(info) = sessions.iter().find(|s| s.name == session) {
                if info.agents.iter().any(|a| a == agent) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("agent {agent} never appeared in session {session}");
    }

    #[tokio::test]
    async fn send_acks_and_lists_in_order() {
        let (addr, handle) = spawn().await;
        let first = client::send(&addr, "default", "claude", "hello", None).await.unwrap();
        let second = client::send(&addr, "default", "muse", "hi back", Some(first)).await.unwrap();
        assert_eq!(second, first + 1);
        let messages = client::list(&addr, "default", "claude", None, None).await.unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].agent, "claude");
        assert_eq!(messages[1].reply_to, Some(first));
        handle.abort();
    }

    #[tokio::test]
    async fn list_respects_after_and_limit() {
        let (addr, handle) = spawn().await;
        for i in 0..5 {
            client::send(&addr, "default", "claude", &format!("msg {i}"), None).await.unwrap();
        }
        let all = client::list(&addr, "default", "claude", None, None).await.unwrap();
        assert_eq!(all.len(), 5);
        let last_two = client::list(&addr, "default", "claude", Some(2), None).await.unwrap();
        assert_eq!(last_two.len(), 2);
        assert_eq!(last_two[0].body, "msg 3");
        let after = client::list(&addr, "default", "claude", None, Some(all[1].id)).await.unwrap();
        assert_eq!(after.len(), 3);
        handle.abort();
    }

    #[tokio::test]
    async fn sessions_keeps_sessions_with_history() {
        let (addr, handle) = spawn().await;
        client::send(&addr, "default", "claude", "hello", None).await.unwrap();
        client::send(&addr, "other", "muse", "hi", None).await.unwrap();
        let sessions = client::sessions(&addr).await.unwrap();
        let names: Vec<&str> = sessions.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"default"));
        assert!(names.contains(&"other"));
        handle.abort();
    }

    #[tokio::test]
    async fn watch_registers_presence_until_disconnect() {
        let (addr, handle) = spawn().await;
        let addr_for_watch = addr.clone();
        let watch = tokio::spawn(async move {
            let _ = client::watch(&addr_for_watch, "default", "claude", |_| {}).await;
        });
        wait_for_agent(&addr, "default", "claude").await;
        watch.abort();
        handle.abort();
    }

    #[tokio::test]
    async fn empty_body_is_rejected() {
        let (addr, handle) = spawn().await;
        let err = client::send(&addr, "default", "claude", "   ", None).await.unwrap_err();
        assert!(err.contains("empty"));
        handle.abort();
    }
}
