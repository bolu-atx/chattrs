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
            Inbound::Hello {
                v: 1,
                address: "codex@box1".into(),
                token: None,
                tags: vec![],
                observer: false,
                state: None,
                project: None,
            },
            Inbound::Hello {
                v: 1,
                address: "codex@box1".into(),
                token: Some("s3cret".into()),
                tags: vec!["os:linux".into(), "gpu".into()],
                observer: false,
                state: None,
                project: None,
            },
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
            Inbound::Watch { topics: vec![], all: false },
            Inbound::Watch { topics: vec!["release-26".into()], all: false },
            Inbound::Wait { reply_to: 12 },
            Inbound::Agents { all: false, projects: false },
            Inbound::Log { address: None, after: 0, limit: 500, tail: None },
            Inbound::Log {
                address: Some("codex@box1".into()),
                after: 12,
                limit: 500,
                tail: Some(10),
            },
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
            message: crate::model::Message {
                id: 12,
                from: "codex@box1".into(),
                to: Some("opus@mac".into()),
                body: "hi".into(),
                ts: 1763200000000,
                reply_to: None,
                topic: None,
                project: "default".into(),
                from_role: None,
            },
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
            project: "default".into(),
            from_role: None,
        };
        let frames = vec![
            Outbound::Welcome {
                server: "chattrs".into(),
                v: 1,
                address: "codex@box1".into(),
                watching: false,
                observer: false,
                project: None,
            },
            Outbound::Sent { id: 12, known: None },
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
                project: "default".into(),
                from_role: None,
            }),
            Outbound::Agents { agents: vec![], projects: None },
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
    fn hello_tags_are_optional_on_the_wire() {
        let old: Inbound =
            serde_json::from_str(r#"{"type":"hello","v":1,"address":"a@h"}"#).unwrap();
        assert_eq!(
            old,
            Inbound::Hello {
                v: 1,
                address: "a@h".into(),
                token: None,
                tags: vec![],
                observer: false,
                state: None,
                project: None,
            }
        );
        let line = encode(&Inbound::Hello {
            v: 1,
            address: "a@h".into(),
            token: None,
            tags: vec![],
            observer: false,
            state: None,
            project: None,
        })
        .unwrap();
        assert!(!line.contains("tags"), "empty tags are omitted: {line}");
    }

    #[test]
    fn hello_state_is_optional_on_the_wire() {
        let hello = |state: Option<&str>| Inbound::Hello {
            v: 1,
            address: "a@h".into(),
            token: None,
            tags: vec![],
            observer: false,
            state: state.map(str::to_string),
            project: None,
        };
        let line = encode(&hello(None)).unwrap();
        assert!(!line.contains("state"), "no state is omitted: {line}");
        let line = encode(&hello(Some("idle"))).unwrap();
        assert!(line.contains(r#""state":"idle""#), "{line}");
        assert_eq!(decode::<Inbound>(&line).unwrap(), hello(Some("idle")));
    }

    #[test]
    fn every_line_carries_a_type_tag() {
        let frames = vec![
            serde_json::to_value(Inbound::Hello {
                v: 1,
                address: "a@h".into(),
                token: None,
                tags: vec![],
                observer: false,
                state: None,
                project: None,
            })
            .unwrap(),
            serde_json::to_value(Inbound::Send {
                to: Some("b@h".into()),
                body: "hi".into(),
                reply_to: None,
                topic: None,
            })
            .unwrap(),
            serde_json::to_value(Inbound::Watch { topics: vec![], all: false }).unwrap(),
            serde_json::to_value(Inbound::Wait { reply_to: 12 }).unwrap(),
            serde_json::to_value(Outbound::Welcome {
                server: "chattrs".into(),
                v: 1,
                address: "a@h".into(),
                watching: false,
                observer: false,
                project: None,
            })
            .unwrap(),
            serde_json::to_value(Outbound::Sent { id: 1, known: None }).unwrap(),
            serde_json::to_value(Inbound::Inbox).unwrap(),
            serde_json::to_value(Inbound::Ack { ids: vec![1] }).unwrap(),
            serde_json::to_value(Outbound::Inbox { messages: vec![] }).unwrap(),
            serde_json::to_value(Outbound::Agents { agents: vec![], projects: None }).unwrap(),
            serde_json::to_value(Inbound::Agents { all: false, projects: false }).unwrap(),
            serde_json::to_value(Inbound::Log { address: None, after: 0, limit: 500, tail: None })
                .unwrap(),
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
        assert_eq!(frame, Inbound::Watch { topics: vec![], all: false });
    }

    #[test]
    fn decode_rejects_bad_json() {
        assert!(decode::<Inbound>("not json\n").is_err());
    }

    #[test]
    fn decode_ignores_trailing_newline() {
        let frame = Outbound::Sent { id: 7, known: None };
        let line = encode(&frame).unwrap();
        assert_eq!(decode::<Outbound>(line.trim_end()).unwrap(), frame);
    }

    #[test]
    fn log_tail_is_optional_on_the_wire() {
        let frame: Inbound = decode("{\"type\":\"log\"}\n").unwrap();
        assert_eq!(frame, Inbound::Log { address: None, after: 0, limit: 500, tail: None });
        let line = encode(&frame).unwrap();
        assert_eq!(
            line.trim_end(),
            "{\"type\":\"log\",\"address\":null,\"after\":0,\"limit\":500}"
        );
        let tailed: Inbound = decode("{\"type\":\"log\",\"tail\":10}\n").unwrap();
        assert_eq!(tailed, Inbound::Log { address: None, after: 0, limit: 500, tail: Some(10) });
    }

    #[test]
    fn sent_without_known_decodes_to_none() {
        let frame: Outbound = decode("{\"type\":\"sent\",\"id\":3}\n").unwrap();
        assert_eq!(frame, Outbound::Sent { id: 3, known: None });
        let line = encode(&frame).unwrap();
        assert_eq!(line.trim_end(), "{\"type\":\"sent\",\"id\":3}");
    }

    #[test]
    fn welcome_without_watching_decodes_to_false() {
        let line = r#"{"type":"welcome","server":"chattrs","v":1,"address":"a@h"}"#;
        match decode::<Outbound>(line) {
            Ok(Outbound::Welcome { watching, .. }) => assert!(!watching),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn observer_fields_are_optional_on_the_wire() {
        let hello: Inbound =
            serde_json::from_str(r#"{"type":"hello","v":1,"address":"a@h"}"#).unwrap();
        assert!(matches!(hello, Inbound::Hello { observer: false, .. }));
        let watch: Inbound = serde_json::from_str(r#"{"type":"watch"}"#).unwrap();
        assert_eq!(watch, Inbound::Watch { topics: vec![], all: false });
        let welcome = r#"{"type":"welcome","server":"chattrs","v":1,"address":"a@h"}"#;
        assert!(matches!(
            decode::<Outbound>(welcome),
            Ok(Outbound::Welcome { observer: false, .. })
        ));
        let hello_line = encode(&Inbound::Hello {
            v: 1,
            address: "a@h".into(),
            token: None,
            tags: vec![],
            observer: false,
            state: None,
            project: None,
        })
        .unwrap();
        let watch_line = encode(&Inbound::Watch { topics: vec![], all: false }).unwrap();
        let welcome_line = encode(&Outbound::Welcome {
            server: "chattrs".into(),
            v: 1,
            address: "a@h".into(),
            watching: false,
            observer: false,
            project: None,
        })
        .unwrap();
        for line in [hello_line, watch_line, welcome_line] {
            assert!(!line.contains("observer") && !line.contains("\"all\""), "{line}");
        }
        let on = encode(&Inbound::Watch { topics: vec![], all: true }).unwrap();
        assert!(on.contains("\"all\":true"), "{on}");
    }
}
