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

/// Largest history page the broker returns for one `log` frame.
pub const MAX_LOG_LIMIT: u64 = 500;

/// Largest backlog the broker replays per topic to a new topic watcher.
pub const MAX_TOPIC_BACKLOG: u64 = 500;

/// Largest accepted topic slug, in bytes.
pub const MAX_TOPIC_BYTES: usize = 64;

/// Most tags the broker accepts in one `hello`.
pub const MAX_TAGS: usize = 32;

/// Largest accepted tag, in bytes.
pub const MAX_TAG_BYTES: usize = 64;

/// The `log` page size when the frame omits `limit`.
fn default_log_limit() -> u64 {
    MAX_LOG_LIMIT
}

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
    address.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'@'))
}

/// A topic is valid when it is 1-64 bytes of lowercase ASCII letters,
/// digits, and hyphens, not starting or ending with a hyphen, with no
/// empty segment: `^[a-z0-9]+(-[a-z0-9]+)*$`.
pub fn is_valid_topic(topic: &str) -> bool {
    if topic.is_empty() || topic.len() > MAX_TOPIC_BYTES {
        return false;
    }
    if topic.starts_with('-') || topic.ends_with('-') {
        return false;
    }
    let mut last_was_dash = false;
    for byte in topic.bytes() {
        if byte == b'-' {
            if last_was_dash {
                return false;
            }
            last_was_dash = true;
        } else {
            if !byte.is_ascii_lowercase() && !byte.is_ascii_digit() {
                return false;
            }
            last_was_dash = false;
        }
    }
    true
}

/// One message on the wire. This is the single shape for a message
/// everywhere: the `inbox` response, `--json` output, and later stories.
/// A `None` topic is the general chat. A `None` recipient is a topic
/// announcement: it belongs to no inbox and reaches only topic watchers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Message {
    pub id: u64,
    pub from: String,
    pub to: Option<String>,
    pub body: String,
    pub ts: u64,
    pub reply_to: Option<u64>,
    pub topic: Option<String>,
}

/// One known agent: an address that completed a `hello`, and when the
/// broker last saw it, in ms since the epoch on the broker clock.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Agent {
    pub address: String,
    pub last_seen: u64,
    /// Tags from the agent's latest `hello` that carried any; empty when
    /// it never sent tags.
    #[serde(default)]
    pub tags: Vec<String>,
}

/// One message in `log` history: the message plus when it was read, if
/// ever. `acked_at` is `None` for an unread message. The wire shape stays
/// flat: the message fields sit next to `acked_at`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoggedMessage {
    #[serde(flatten)]
    pub message: Message,
    pub acked_at: Option<u64>,
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
        /// Hardware and custom tags, like `os:linux` or `gpu`. Absent
        /// when empty.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tags: Vec<String>,
    },
    /// Store a message for the `to` address. The broker fills in `from`.
    /// With no `to`, the broker answers `reply_to` to its sender instead.
    /// With a `reply_to` and no `topic`, the broker stores the topic of
    /// the message it answers; an explicit `topic` always wins.
    Send {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        to: Option<String>,
        body: String,
        #[serde(default)]
        reply_to: Option<u64>,
        #[serde(default)]
        topic: Option<String>,
    },
    /// Ask for every unread message addressed to the `hello` address.
    Inbox,
    /// Ask for every address that ever said `hello`, sorted by address.
    Agents,
    /// List message history with id above `after`, in id order, at most
    /// `limit` rows. With `address`, only messages it sent or received.
    Log {
        #[serde(default)]
        address: Option<String>,
        #[serde(default)]
        after: u64,
        #[serde(default = "default_log_limit")]
        limit: u64,
    },
    /// Stream every unread message, then each new one, as `message` frames.
    /// With topics, stream announcements to those rooms instead of the
    /// inbox watch. An empty list is the inbox watch.
    Watch {
        #[serde(default)]
        topics: Vec<String>,
    },
    /// Wait for the lowest-id message to the `hello` address with this
    /// `reply_to`, acked or not. The broker answers at once when one
    /// exists, else when one arrives. One `wait` gets one message.
    Wait { reply_to: u64 },
    /// Mark messages as read. Only unread messages owned by the `hello`
    /// address change; other ids are ignored, not an error.
    Ack { ids: Vec<u64> },
}

/// Frames the broker sends to a client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Outbound {
    Welcome {
        server: String,
        v: u32,
        address: String,
    },
    /// Confirms a stored message with its id. `known` is `false` when the
    /// `to` address never said `hello`. It is absent when the send has no
    /// `to`, and from an older broker.
    Sent {
        id: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        known: Option<bool>,
    },
    /// Every unread message for the `hello` address, in id order.
    Inbox {
        messages: Vec<Message>,
    },
    /// Every address that ever said `hello`, sorted by address.
    Agents {
        agents: Vec<Agent>,
    },
    /// Message history in id order. Each row carries `acked_at`, which is
    /// `None` for an unread message. Reading history never acks.
    Log {
        messages: Vec<LoggedMessage>,
    },
    /// One streamed message for a `watch` connection.
    Message(Message),
    /// The ids from `ack` that actually changed to read.
    Acked {
        ids: Vec<u64>,
    },
    Error {
        code: String,
        message: String,
    },
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
