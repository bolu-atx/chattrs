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
    serde_json::to_string(frame)
        .map(|json| format!("{json}\n"))
        .map_err(|e| e.to_string())
}

/// Parse one frame from a line, ignoring its trailing newline.
pub fn decode<T: DeserializeOwned>(line: &str) -> Result<T, String> {
    serde_json::from_str(line.trim_end_matches(['\r', '\n'])).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{self, Inbound, Outbound};

    #[test]
    fn inbound_frames_round_trip() {
        let frames = vec![
            Inbound::Hello { agent: "claude".into(), session: "default".into() },
            Inbound::Send { body: "hi".into(), reply_to: Some(3) },
            Inbound::Send { body: "hi".into(), reply_to: None },
            Inbound::List { limit: Some(10), after: Some(2) },
            Inbound::List { limit: None, after: None },
            Inbound::Watch,
            Inbound::Sessions,
        ];
        for frame in frames {
            let line = encode(&frame).unwrap();
            assert!(line.ends_with('\n'));
            assert_eq!(decode::<Inbound>(&line).unwrap(), frame);
        }
    }

    #[test]
    fn outbound_frames_round_trip() {
        let message = model::Message {
            id: 1,
            session: "default".into(),
            agent: "claude".into(),
            body: "hi".into(),
            ts: 1_234_567,
            reply_to: None,
        };
        let frames = vec![
            Outbound::Welcome { server: "chattrs".into(), session: "default".into() },
            Outbound::Ack { id: 1 },
            Outbound::Message {
                id: 1,
                session: "default".into(),
                agent: "claude".into(),
                body: "hi".into(),
                ts: 1_234_567,
                reply_to: None,
            },
            Outbound::History { messages: vec![message] },
            Outbound::Sessions {
                sessions: vec![model::SessionInfo {
                    name: "default".into(),
                    agents: vec!["claude".into()],
                    message_count: 1,
                }],
            },
            Outbound::Error { message: "bad".into() },
        ];
        for frame in frames {
            let line = encode(&frame).unwrap();
            assert_eq!(decode::<Outbound>(&line).unwrap(), frame);
        }
    }

    #[test]
    fn decode_rejects_bad_json() {
        assert!(decode::<Inbound>("not json\n").is_err());
    }

    #[test]
    fn decode_ignores_trailing_newline() {
        let frame = Inbound::Watch;
        let line = encode(&frame).unwrap();
        assert_eq!(decode::<Inbound>(line.trim_end()).unwrap(), frame);
    }
}
