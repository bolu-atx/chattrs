//! chattrs — data model: limits, addresses, and wire frames.
//!
//! This module is pure: types and their serde shapes only. Framing lives in
//! `protocol`, storage in `store`, the broker in `server`, and commands in
//! `client`.

use serde::{Deserialize, Serialize};

/// Protocol version spoken by this broker and client.
pub const PROTOCOL_VERSION: u32 = 1;

/// Largest accepted message body, in bytes.
pub const MAX_BODY_BYTES: usize = 64 * 1024;

/// Largest accepted inbound line, in bytes.
pub const MAX_LINE_BYTES: usize = 128 * 1024;

/// Largest accepted address, in bytes.
pub const MAX_ADDRESS_BYTES: usize = 128;

/// An address is valid when it is `user@host` with only letters, digits,
/// `.`, `_`, `-` on each side, and at most 128 bytes long.
pub fn is_valid_address(address: &str) -> bool {
    if address.is_empty() || address.len() > MAX_ADDRESS_BYTES {
        return false;
    }
    let Some((user, host)) = address.split_once('@') else {
        return false;
    };
    if user.is_empty() || host.is_empty() || host.contains('@') {
        return false;
    }
    address
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'@'))
}

/// One message on the wire. This is the single shape for a message
/// everywhere: the `inbox` response, `--json` output, and later stories.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Message {
    pub id: u64,
    pub from: String,
    pub to: String,
    pub body: String,
    pub ts: u64,
    pub reply_to: Option<u64>,
}

/// Render one message as a single text line: `[12] codex@box1: body`,
/// or `[12] codex@box1 (re 7): body` for a reply.
pub fn render_message(message: &Message) -> String {
    match message.reply_to {
        Some(parent) => {
            format!("[{}] {} (re {}): {}", message.id, message.from, parent, message.body)
        }
        None => format!("[{}] {}: {}", message.id, message.from, message.body),
    }
}

/// Frames a client sends to the broker.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Inbound {
    /// Identify as an address; must be the first frame on a connection.
    Hello {
        v: u32,
        address: String,
        #[serde(default)]
        token: Option<String>,
    },
    /// Store a message for the `to` address. The broker fills in `from`.
    /// With no `to`, the broker answers `reply_to` to its sender instead.
    Send {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        to: Option<String>,
        body: String,
        #[serde(default)]
        reply_to: Option<u64>,
    },
    /// Ask for every unread message addressed to the `hello` address.
    Inbox,
    /// Stream every unread message, then each new one, as `message` frames.
    Watch,
    /// Mark messages as read. Only unread messages owned by the `hello`
    /// address change; other ids are ignored, not an error.
    Ack { ids: Vec<u64> },
}

/// Frames the broker sends to a client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Outbound {
    Welcome { server: String, v: u32, address: String },
    /// Confirms a stored message with its id.
    Sent { id: u64 },
    /// Every unread message for the `hello` address, in id order.
    Inbox { messages: Vec<Message> },
    /// One streamed message for a `watch` connection.
    Message(Message),
    /// The ids from `ack` that actually changed to read.
    Acked { ids: Vec<u64> },
    Error { code: String, message: String },
}

impl Outbound {
    /// Build an `error` frame with a machine-readable code.
    pub fn error(code: &str, message: impl Into<String>) -> Outbound {
        Outbound::Error { code: code.to_string(), message: message.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_addresses_pass() {
        for address in ["a@h", "codex@box1", "opus.mac@tail-scale_01", "A.9-x_y@Z.8-w_v"] {
            assert!(is_valid_address(address), "{address}");
        }
    }

    #[test]
    fn render_message_prints_plain_and_reply_lines() {
        let plain = Message {
            id: 12,
            from: "codex@box1".into(),
            to: "opus@mac".into(),
            body: "hi".into(),
            ts: 1763200000000,
            reply_to: None,
        };
        assert_eq!(render_message(&plain), "[12] codex@box1: hi");
        let reply = Message { id: 13, reply_to: Some(7), ..plain.clone() };
        assert_eq!(render_message(&reply), "[13] codex@box1 (re 7): hi");
    }

    #[test]
    fn invalid_addresses_fail() {
        let long = format!("{}@h", "a".repeat(127));
        let cases = vec![
            "".to_string(),
            "no-at-sign".to_string(),
            "@h".to_string(),
            "a@".to_string(),
            "a@b@c".to_string(),
            "a b@c".to_string(),
            "a@h!".to_string(),
            long,
        ];
        for address in cases {
            assert!(!is_valid_address(&address), "{address}");
        }
    }
}
