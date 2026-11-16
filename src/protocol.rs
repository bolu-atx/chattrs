//! chattrs — line-delimited JSON framing.
//!
//! Every frame is one JSON object on its own line, terminated by `\n`. The
//! framing is plain enough that agents can speak it from any language with a
//! socket and a JSON library; `model` owns the shapes, this module owns the
//! byte boundary.

use serde::de::DeserializeOwned;
use serde::Serialize;

/// Name reported in the `welcome` frame.
pub const SERVER_NAME: &str = "chattrs";

/// Serialize a frame into one JSON line terminated by `\n`.
pub fn encode<T: Serialize>(frame: &T) -> Result<String, String> {
    serde_json::to_string(frame).map(|json| format!("{json}\n")).map_err(|e| e.to_string())
}

/// Parse one frame from a line, ignoring its trailing newline.
pub fn decode<T: DeserializeOwned>(line: &str) -> Result<T, String> {
    serde_json::from_str(line.trim_end_matches(['\r', '\n'])).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Inbound, Outbound};

    #[test]
    fn inbound_frames_round_trip() {
        let frames = vec![
            Inbound::Hello { v: 1, address: "codex@box1".into(), token: None },
            Inbound::Hello { v: 1, address: "codex@box1".into(), token: Some("s3cret".into()) },
            Inbound::Send {
                to: Some("opus@mac".into()),
                body: "hi".into(),
                reply_to: None,
                topic: None,
            },
            Inbound::Send {
                to: Some("opus@mac".into()),
                body: "hi".into(),
                reply_to: Some(3),
                topic: None,
            },
            Inbound::Send { to: None, body: "hi".into(), reply_to: Some(3), topic: None },
            Inbound::Inbox,
            Inbound::Ack { ids: vec![12, 13] },
            Inbound::Watch { topics: vec![] },
            Inbound::Watch { topics: vec!["release-26".into()] },
            Inbound::Wait { reply_to: 12 },
            Inbound::Agents,
            Inbound::Log { address: None, after: 0, limit: 500 },
            Inbound::Log { address: Some("codex@box1".into()), after: 12, limit: 500 },
        ];
        for frame in frames {
            let line = encode(&frame).unwrap();
            assert!(line.ends_with('\n'));
            assert_eq!(decode::<Inbound>(&line).unwrap(), frame);
        }
    }

    #[test]
    fn outbound_frames_round_trip() {
        let logged = crate::model::LoggedMessage {
            id: 12,
            from: "codex@box1".into(),
            to: Some("opus@mac".into()),
            body: "hi".into(),
            ts: 1763200000000,
            reply_to: None,
            topic: None,
            acked_at: None,
        };
        let message = crate::model::Message {
            id: 12,
            from: "codex@box1".into(),
            to: Some("opus@mac".into()),
            body: "hi".into(),
            ts: 1763200000000,
            reply_to: None,
            topic: None,
        };
        let frames = vec![
            Outbound::Welcome { server: "chattrs".into(), v: 1, address: "codex@box1".into() },
            Outbound::Sent { id: 12 },
            Outbound::Inbox { messages: vec![message.clone()] },
            Outbound::Message(message),
            Outbound::Message(crate::model::Message {
                id: 13,
                from: "codex@box1".into(),
                to: None,
                body: "build 421 is red".into(),
                ts: 1763200000000,
                reply_to: None,
                topic: Some("release-26".into()),
            }),
            Outbound::Agents { agents: vec![] },
            Outbound::Log { messages: vec![logged] },
            Outbound::Acked { ids: vec![12, 13] },
            Outbound::Error { code: "empty_body".into(), message: "body is empty".into() },
        ];
        for frame in frames {
            let line = encode(&frame).unwrap();
            assert!(line.ends_with('\n'));
            assert_eq!(decode::<Outbound>(&line).unwrap(), frame);
        }
    }

    #[test]
    fn every_line_carries_a_type_tag() {
        let frames = vec![
            serde_json::to_value(Inbound::Hello { v: 1, address: "a@h".into(), token: None })
                .unwrap(),
            serde_json::to_value(Inbound::Send {
                to: Some("b@h".into()),
                body: "hi".into(),
                reply_to: None,
                topic: None,
            })
            .unwrap(),
            serde_json::to_value(Inbound::Watch { topics: vec![] }).unwrap(),
            serde_json::to_value(Inbound::Wait { reply_to: 12 }).unwrap(),
            serde_json::to_value(Outbound::Welcome {
                server: "chattrs".into(),
                v: 1,
                address: "a@h".into(),
            })
            .unwrap(),
            serde_json::to_value(Outbound::Sent { id: 1 }).unwrap(),
            serde_json::to_value(Inbound::Inbox).unwrap(),
            serde_json::to_value(Inbound::Ack { ids: vec![1] }).unwrap(),
            serde_json::to_value(Outbound::Inbox { messages: vec![] }).unwrap(),
            serde_json::to_value(Outbound::Agents { agents: vec![] }).unwrap(),
            serde_json::to_value(Inbound::Agents).unwrap(),
            serde_json::to_value(Inbound::Log { address: None, after: 0, limit: 500 }).unwrap(),
            serde_json::to_value(Outbound::Log { messages: vec![] }).unwrap(),
            serde_json::to_value(Outbound::Acked { ids: vec![1] }).unwrap(),
            serde_json::to_value(Outbound::error("bad_frame", "nope")).unwrap(),
        ];
        for frame in frames {
            assert!(frame.get("type").is_some(), "{frame}");
        }
    }

    #[test]
    fn watch_without_topics_is_the_inbox_watch() {
        let frame: Inbound = decode("{\"type\":\"watch\"}\n").unwrap();
        assert_eq!(frame, Inbound::Watch { topics: vec![] });
    }

    #[test]
    fn decode_rejects_bad_json() {
        assert!(decode::<Inbound>("not json\n").is_err());
    }

    #[test]
    fn decode_ignores_trailing_newline() {
        let frame = Outbound::Sent { id: 7 };
        let line = encode(&frame).unwrap();
        assert_eq!(decode::<Outbound>(line.trim_end()).unwrap(), frame);
    }
}
