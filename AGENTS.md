# AGENTS.md — chatterbox contributor guide

`chatterbox` is a distributed client/server messaging system for cross-agent
coordination, written in Rust. One broker, many agents (`claude`, `muse`,
`codex`, scripts) joining named sessions over a line-JSON TCP protocol.
Crate and binary are both called `chatterbox`.

Start with [README.md](README.md) for what it is and the wire protocol, then
this file for commands and architecture.

## Commands

```bash
cargo build   # refreshes target/debug/chatterbox
cargo test    # unit + TCP integration tests, must be green with zero warnings
cargo clippy --all-targets -- -D warnings   # lint gate, same as CI
# Toolchain is pinned in rust-toolchain.toml; rustup installs it on first use.
```

Run a broker and exercise the client against an ephemeral port:

```bash
target/debug/chatterbox serve --listen 127.0.0.1:47654
target/debug/chatterbox send --server 127.0.0.1:47654 --agent claude "hi"
target/debug/chatterbox list --server 127.0.0.1:47654 --json
```

## Architecture

| File | Owns |
| ---- | ---- |
| `src/main.rs` | Thin entry: parse `cli`, dispatch to `server`/`client` |
| `src/cli.rs` | Clap `Args` + subcommand structs, `--server`/`--session`/`--agent` defaults |
| `src/model.rs` | Wire types (`Message`, `SessionInfo`, `Inbound`, `Outbound`) and their serde shapes |
| `src/protocol.rs` | Line-JSON framing: `encode`/`decode` |
| `src/server.rs` | The broker: sessions, history ring buffer, presence, broadcast |
| `src/client.rs` | One TCP connection per command (`send`/`list`/`watch`/`sessions`) |

Key patterns:

- **Data first.** Types live in `model.rs` and carry the wire shape; behavior
  follows. `model`/`protocol` stay pure — no sockets, no IO.
- **Never block the broker.** Each connection runs its own tokio task with a
  reader loop and a writer task, so a slow subscriber cannot stall the rest.
- **Presence is counts, not accounts.** An agent joins a session on `hello`
  and leaves when its connection closes; sessions with no history and no
  agents are dropped.
- **Broadcast for live, ring buffer for catch-up.** `watch` subscribes to
  new messages; `list` reads retained history. The buffer is capped by
  `serve --history`.

## Code style

Read [docs/code-style.md](docs/code-style.md) before writing code. It covers
module layout, naming, comments, state, and the networking conventions.

## Verification

- End of work, before every push: `cargo clippy --all-targets -- -D warnings`
  and `cargo test`, both clean. CI runs the same two commands.
- `cargo test` after every behavior change. Integration tests in
  `src/server.rs` bind an ephemeral `127.0.0.1:0` and drive the real client
  over TCP — no mocks.

## Planning

We plan in milestones and issues — see [docs/planning.md](docs/planning.md).
Milestone = epic, issue = coarse story. PRs open with `Closes #N`.
Open is not done: a PR is done only when mergeable (checks green, no conflicts).

## Commits and releases

- Title style: `feat: …`, `fix: …`, `docs: …`, `refactor: …`.
