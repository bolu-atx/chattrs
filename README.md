# chatterbox

A small, distributed client/server messaging system for cross-agent
coordination. One broker, any number of agents — `claude`, `muse`, `codex`,
a shell script, a Python worker — joining named sessions, exchanging
messages, and replying to each other.

It is a plain Rust CLI. The wire protocol is line-delimited JSON over TCP,
so any language with a socket and a JSON library can participate.

## Install

```bash
cargo build --release
# binary at target/release/chatterbox
```

The toolchain is pinned in `rust-toolchain.toml`; `rustup` installs it on
first use.

## Quick start

```bash
# terminal 1 — run the broker
chatterbox serve

# terminal 2 — claude sends a message
export CHATTERBOX_AGENT=claude
chatterbox send "anyone know the coverage numbers?"

# terminal 3 — muse listens and replies
export CHATTERBOX_AGENT=muse
chatterbox watch                          # streams new messages as they land
chatterbox send --reply-to 1 "coverage is 87%"

# anywhere — read the history
chatterbox list
chatterbox list --json                    # machine-readable
chatterbox sessions                       # who is connected, and where
```

`send` prints the assigned message id; pass it to another agent's
`send --reply-to <id>` to thread the conversation.

## Commands

| Command | What it does |
| ------- | ------------ |
| `serve` | Run the broker on `--listen` (default `127.0.0.1:4646`). |
| `send`  | Publish a message (positional text, or stdin when no text is given) and print its id. |
| `list`  | Print recent messages in a session, newest last. `--json` for machines. |
| `watch` | Stream new messages live. |
| `sessions` | List active sessions, connected agents, and message counts. |

Common flags:

- `--server ADDR` — broker address (default `127.0.0.1:4646`).
- `--session NAME` — session name (default `default`).
- `--agent NAME` — sender identity, overridable with the `CHATTERBOX_AGENT`
  environment variable (default `agent`).
- `send --reply-to ID` — reply to a previous message.
- `list --limit N --after ID` — window the history.
- `serve --history N` — messages retained per session (default `500`).

A message body that starts with `-` needs `--`:
`chatterbox send -- "- this starts with a dash"`.

## Agents talking to each other

Each agent runs the same binary and just passes `--agent`. The broker does
not care which agent is which:

```bash
chatterbox send --agent claude --session planning "proposal ready"
chatterbox send --agent muse  --session planning --reply-to 1 "reviewing now"
chatterbox send --agent codex --session planning --reply-to 2 "lgtm, ship it"
```

Because the protocol is line-JSON over TCP, agents can also talk to the
broker directly (see [Protocol](#protocol)) — useful for SDK-less clients
or quick Python/Node integrations.

## Protocol

One JSON object per line, `\n`-terminated. The `type` field disambiguates.

Client → server:

```json
{"type":"hello",    "agent":"claude", "session":"default"}
{"type":"send",     "body":"hi", "reply_to":3}
{"type":"list",     "limit":50, "after":3}
{"type":"watch"}
{"type":"sessions"}
```

Server → client:

```json
{"type":"welcome",  "server":"chatterbox", "session":"default"}
{"type":"ack",      "id":12}
{"type":"message",  "id":12, "session":"default", "agent":"claude", "body":"hi", "ts":1794763538737, "reply_to":3}
{"type":"history",  "messages":[ ... ]}
{"type":"sessions", "sessions":[{"name":"default","agents":["claude"],"message_count":2}]}
{"type":"error",    "message":"message body is empty"}
```

Timestamps are milliseconds since the Unix epoch, stamped by the broker.
`reply_to` is omitted when `null`. `send` is acknowledged with `ack` and the
message id; `list` with `history`; `sessions` with `sessions`; `watch` keeps
the connection open and streams `message` frames.

## Design

- **One broker, many sessions.** A session is a named message log plus the
  set of currently connected agents. Sessions with no history and no agents
  are dropped.
- **Presence, not accounts.** There is no authentication; an agent is
  whoever its `hello` says it is. Run the broker on a trusted network or
  loopback. Auth is tracked as future work in `docs/planning.md`.
- **Broadcast for live, history for catch-up.** `watch` subscribes to new
  messages; `list` reads the retained ring buffer.
- **Small and boring.** No database, no framework. State lives in memory.

## Repository

- `src/main.rs` — thin entry, dispatch.
- `src/cli.rs` — argument parsing only.
- `src/model.rs` — wire types and their serde shapes.
- `src/protocol.rs` — line-JSON framing.
- `src/server.rs` — the broker.
- `src/client.rs` — one connection per command.

See `docs/code-style.md` for conventions and `docs/planning.md` for the
roadmap.

## License

MIT. See `LICENSE`.
