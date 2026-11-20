//! chattrs — hook: the adapter that Claude Code and Codex run on each hook
//! event.
//!
//! Both agents read one JSON object from the hook's stdout, so one output
//! shape serves both. The hook is best effort: it prints one JSON object or
//! nothing, and it always exits 0, so a broken broker never breaks the
//! agent's turn.

use std::io::{IsTerminal, Read};
use std::time::Duration;

use serde_json::{json, Value};

use crate::cli::{ClientArgs, HookEvent};
use crate::client::{Client, Endpoint};
use crate::model::Message;
use crate::output;

/// Total time for connect plus exchange. The agent waits on the hook, so a
/// slow broker costs a skipped delivery, not a stalled turn.
const BUDGET: Duration = Duration::from_millis(300);

/// Run one hook event. Does nothing without an own address, because the
/// hooks live in the user-level config and fire in every session.
pub async fn run(event: HookEvent, client: &ClientArgs) {
    let Some(address) = client.address.as_deref().filter(|address| !address.is_empty()) else {
        return;
    };
    let input = read_input();
    if event == HookEvent::SessionStart {
        print_json(&session_start_output(address, &input));
        return;
    }
    if event == HookEvent::Stop && input["stop_hook_active"].as_bool() == Some(true) {
        return;
    }
    let endpoint = Endpoint {
        server: client.server.clone(),
        address: address.to_string(),
        token: client.token.clone(),
    };
    // A timeout drops the exchange; unacked messages stay unread.
    let _ = tokio::time::timeout(BUDGET, deliver(event, &endpoint)).await;
}

/// The hook JSON from stdin. Empty, invalid, or terminal stdin gives null.
fn read_input() -> Value {
    let mut text = String::new();
    if std::io::stdin().is_terminal() || std::io::stdin().read_to_string(&mut text).is_err() {
        return Value::Null;
    }
    serde_json::from_str(&text).unwrap_or(Value::Null)
}

/// Fetch unread messages, print them as hook output, flush, and only then
/// ack exactly those ids. Any failure ends quietly.
async fn deliver(event: HookEvent, endpoint: &Endpoint) {
    let Ok(mut client) = Client::reconnect(endpoint).await else {
        return;
    };
    let Ok(messages) = client.unread().await else {
        return;
    };
    if messages.is_empty() {
        return;
    }
    if !print_json(&delivery_output(event, &messages)) {
        return;
    }
    let ids: Vec<u64> = messages.iter().map(|message| message.id).collect();
    let _ = client.ack(&ids).await;
}

/// Print one JSON object and flush. Returns false when stdout failed.
fn print_json(value: &Value) -> bool {
    output::write_stdout(&format!("{value}\n")).is_ok()
}

/// The preamble plus every message in its `<agent-message>` wrapper.
fn context_text(messages: &[Message]) -> String {
    let mut text = output::PREAMBLE.to_string();
    for message in messages {
        text.push_str(&output::message_text(message));
    }
    text
}

/// Hook output for unread messages. `stop` blocks the stop with the
/// messages as the reason; the other events add them as context.
fn delivery_output(event: HookEvent, messages: &[Message]) -> Value {
    let text = context_text(messages);
    match event {
        HookEvent::Stop => json!({"decision": "block", "reason": text}),
        HookEvent::PostToolUse => context_output("PostToolUse", &text),
        HookEvent::UserPromptSubmit => context_output("UserPromptSubmit", &text),
        HookEvent::SessionStart => context_output("SessionStart", &text),
    }
}

fn context_output(event_name: &str, text: &str) -> Value {
    json!({"hookSpecificOutput": {"hookEventName": event_name, "additionalContext": text}})
}

/// The `SessionStart` context: the address, the skill, and, for Claude
/// Code only, the Monitor reminder. Codex sends `turn_id`; Claude Code does
/// not. Codex has no Monitor tool.
fn session_start_output(address: &str, input: &Value) -> Value {
    let mut text =
        format!("Your chattrs address is {address}. Load the chattrs skill: run `chattrs skill`.");
    if input.get("turn_id").is_none() {
        text.push_str(" Run `chattrs watch` under the Monitor tool to wake on messages.");
    }
    context_output("SessionStart", &text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(id: u64) -> Message {
        Message {
            id,
            from: "a@h".into(),
            to: Some("b@h".into()),
            body: "hi".into(),
            ts: 0,
            reply_to: None,
            topic: None,
        }
    }

    #[test]
    fn stop_blocks_with_the_messages_as_reason() {
        let value = delivery_output(HookEvent::Stop, &[message(1)]);
        assert_eq!(value["decision"], "block");
        let reason = value["reason"].as_str().unwrap();
        assert!(reason.starts_with(output::PREAMBLE));
        assert!(reason.contains("<agent-message id=\"1\""));
    }

    #[test]
    fn prompt_and_tool_events_add_context() {
        let value = delivery_output(HookEvent::PostToolUse, &[message(1)]);
        assert_eq!(value["hookSpecificOutput"]["hookEventName"], "PostToolUse");
        let value = delivery_output(HookEvent::UserPromptSubmit, &[message(1)]);
        assert_eq!(value["hookSpecificOutput"]["hookEventName"], "UserPromptSubmit");
    }

    #[test]
    fn session_start_names_monitor_for_claude_and_not_codex() {
        let claude = session_start_output("c@h", &json!({"hook_event_name": "SessionStart"}));
        let text = claude["hookSpecificOutput"]["additionalContext"].as_str().unwrap();
        assert!(text.contains("c@h") && text.contains("Monitor"));
        let codex = session_start_output("c@h", &json!({"turn_id": "t", "model": "m"}));
        let text = codex["hookSpecificOutput"]["additionalContext"].as_str().unwrap();
        assert!(text.contains("chattrs skill") && !text.contains("Monitor"));
    }
}
