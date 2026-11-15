# Planning — chatterbox

We plan in milestones (epics) broken into issues (coarse stories). Labels
name the subject (`feature`, `bugfix`, `docs`, `chore`). PRs open with
`Closes #N`.

## Milestone 0 — messaging core (shipped)

The working MVP:

- Line-delimited JSON protocol over TCP (`hello`, `send`, `list`, `watch`,
  `sessions`).
- A broker with in-memory sessions, a capped ring buffer per session, and
  presence tracking (connected agents).
- A CLI: `serve`, `send`, `list`, `watch`, `sessions`, with `--json` output
  for machine consumption.
- Unit tests for framing and argument parsing; TCP integration tests against
  an ephemeral listener.

## Milestone 1 — multi-host and transport

- Unix domain socket transport in addition to TCP, for same-machine agents
  that share a filesystem (no port to configure).
- A `CHATTERBOX_SERVER` environment variable honored by every client command,
  so agents do not repeat `--server`.
- Plaintext is documented as trusted-network-only; keep it that way until
  auth lands.

## Milestone 2 — coordination primitives

- Directed messages: a `target` field so one agent can message one specific
  agent rather than broadcasting to a session.
- Request/response correlation: a `correlate_to` field for ask-answer pairing
  beyond simple `reply_to` threads.
- Delivery receipts (`received`) for agents that need confirmation a message
  was persisted.

## Milestone 3 — durability and auth

- Optional append-only log persisted to disk, so a restart keeps history.
- A shared `--token` (or env `CHATTERBOX_TOKEN`) so an untrusted network can
  still be used safely.
- Enforce token on `hello`, reject otherwise.

## Milestone 4 — SDKs and examples

- Small client libraries (Python, Node) that mirror the Rust `client.rs`
  surface, plus an example showing `claude` and `muse` coordinating on a
  session.

## Notes

- Identity is self-asserted (the `agent` field). There is no account system
  by design; presence is just who is currently connected.
- The broker keeps everything in memory. Do not add a database before
  Milestone 3 — YAGNI until durability is actually needed.
