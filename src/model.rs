//! chatterbox — data model for messages, sessions, and wire frames.
//!
//! This module is pure: types and their serde shapes only. Framing lives in
//! `protocol`, the broker in `server`, and the client in `client`.

use serde::{Deserialize, Serialize};

/// One message in a session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Message {
    pub id: u64,
    pub session: String,
    pub agent: String,
    pub body: String,
    /// Milliseconds since the Unix epoch, as stamped by the broker.
    pub ts: u64,
    pub reply_to: Option<u64>,
}

/// A session summary returned by `sessions`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionInfo {
    pub name: String,
    /// Connected agents, sorted.
    pub agents: Vec<String>,
    pub message_count: usize,
}

/// Frames a client sends to the broker.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Inbound {
    /// Register an agent on a session; must precede the other frames.
    Hello { agent: String, session: String },
    Send { body: String, reply_to: Option<u64> },
    List { limit: Option<usize>, after: Option<u64> },
    Watch,
    Sessions,
}

/// Frames the broker sends to a client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Outbound {
    Welcome { server: String, session: String },
    /// Acknowledges a published message with its id.
    Ack { id: u64 },
    Message {
        id: u64,
        session: String,
        agent: String,
        body: String,
        ts: u64,
        reply_to: Option<u64>,
    },
    History { messages: Vec<Message> },
    Sessions { sessions: Vec<SessionInfo> },
    Error { message: String },
}

impl From<Message> for Outbound {
    fn from(message: Message) -> Outbound {
        Outbound::Message {
            id: message.id,
            session: message.session,
            agent: message.agent,
            body: message.body,
            ts: message.ts,
            reply_to: message.reply_to,
        }
    }
}
