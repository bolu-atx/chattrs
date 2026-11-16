# chattrs

A small, distributed client/server messaging system for cross-agent
coordination. One broker, any number of agents — `claude`, `muse`, `codex`,
a shell script, a Python worker — each with an `agent@host` address, sending
messages to each other through the broker.

It is a plain Rust CLI. The wire protocol is line-delimited JSON over TCP,
so any language with a socket and a JSON library can participate.

## Install

```bash
cargo build --release
# binary at target/release/chattrs
```

The toolchain is pinned in `rust-toolchain.toml`; `rustup` installs it on
first use.

## Quick start

```bash
# terminal 1 — run the broker
chattrs serve --db /tmp/c.db

# terminal 2 — claude sends a message
export CHATTRS_ADDRESS=claude@box1
chattrs send --to muse@mac "anyone know the coverage numbers?"
```

`send` prints the assigned message id. `send --json` prints the `sent`
frame instead: `{"type":"sent","id":12}`.

`inbox` prints one line per unread message — `[12] codex@box1: hi`, or
`[13] codex@box1 (re 7): hi` for a reply — then acks what it printed, so a
second run prints nothing. `inbox --json` prints one message object per
line with `"type":"message"` added.

## Commands

| Command | What it does |
| ------- | ------------ |
| `serve` | Run the broker on `--listen` (default `127.0.0.1:4646`), storing to `--db`. |
| `send`  | Post to `--to ADDR` and print the id (text args, else stdin). |
| `inbox` | Print every unread message for `--address`, then ack what was printed. |

Common flags:

- `--server ADDR` — broker address (default `127.0.0.1:4646`,
  `CHATTRS_SERVER`).
- `--address ADDR` — own address, required until a default lands
  (`CHATTRS_ADDRESS`).
- `--token TOKEN` — token for the broker (`CHATTRS_TOKEN`).
- `--json` — print JSON lines; every line carries a `type`.
- `serve --db PATH` — SQLite path (`CHATTRS_DB`, default
  `$XDG_DATA_HOME/chattrs/chattrs.db`).

A message body that starts with `-` needs `--`:
`chattrs send --to b@h -- "- this starts with a dash"`.

## Agents talking to each other

Each agent runs the same binary and just passes `--address`. The broker
does not care which agent is which:

```bash
CHATTRS_ADDRESS=claude@box1 chattrs send --to muse@mac "proposal ready"
CHATTRS_ADDRESS=muse@mac chattrs send --to codex@box2 "reviewing now"
```

Because the protocol is line-JSON over TCP, agents can also talk to the
broker directly (see [Protocol](#protocol)) — useful for SDK-less clients
or quick Python/Node integrations.

## Protocol

One JSON object per line, `\n`-terminated. The `type` field disambiguates.

Client → broker:

```json
{"type":"hello", "v":1, "address":"codex@box1", "token":"…"}
{"type":"send",  "to":"opus@mac", "body":"hi", "reply_to":null}
{"type":"inbox"}
{"type":"ack",   "ids":[12, 13]}
```

Broker → client:

```json
{"type":"welcome", "server":"chattrs", "v":1, "address":"codex@box1"}
{"type":"sent",    "id":12}
{"type":"inbox",   "messages":[
  {"id":12,"from":"codex@box1","to":"opus@mac",
   "body":"hi","ts":1763200000000,"reply_to":null}]}
{"type":"acked",   "ids":[12, 13]}
{"type":"error",   "code":"empty_body", "message":"body is empty"}
```

`hello` (the token is optional) must be the first frame; anything else first
gets `hello_required` and a closed connection. The broker sets the sender
from the `hello` address; `send` carries no sender. A send to an unknown
address succeeds — the inbox exists by name.

`inbox` returns every message for the `hello` address with no ack yet, in
id order. `ack` marks the listed ids read when they belong to that address
and are unread, and `acked` lists the ids that changed; other ids are
ignored. The broker never pushes messages: the client asks. The `inbox`
command prints each message, flushes stdout, then acks, so a crash between
print and ack redelivers the message on the next run — delivery is at least
once, and the id tells duplicates apart.

Error codes: `bad_frame`, `hello_required`, `bad_version` (v ≠ 1),
`bad_address`, `empty_body`, `too_large`. After `hello`, an error does not
close the connection.

Limits: an address is `user@host` (`[A-Za-z0-9._-]` each side, 128 bytes
max). A body is 1 byte to 64 KiB. An inbound line over 128 KiB is rejected
with `too_large`.

## Design

- **Durable before anything else.** The broker commits each message to
  SQLite before it confirms the send, so a restart loses nothing. The broker
  never deletes a message.
- **Addresses, not accounts.** An agent is whoever its `hello` says it is.
  Run the broker on a trusted network or loopback. Token checks land in a
  later story.
- **Small and boring.** One SQLite file, line-JSON over TCP, no framework.

## Repository

- `src/main.rs` — thin entry, dispatch.
- `src/cli.rs` — argument parsing only.
- `src/model.rs` — wire types and their serde shapes.
- `src/protocol.rs` — line-JSON framing.
- `src/store.rs` — SQLite storage.
- `src/server.rs` — the broker.
- `src/client.rs` — one connection per command.

See `docs/code-style.md` for conventions and `docs/planning.md` for the
roadmap.

## License

MIT. See `LICENSE`.
