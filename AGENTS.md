# AGENTS.md — chattrs contributor guide

`chattrs` is a distributed client/server messaging system for cross-agent
coordination, written in Rust. One broker, many agents (`claude`, `muse`,
`codex`, scripts) with `agent@host` addresses, sending messages through
protocol v1 over a line-JSON TCP protocol. Crate and binary are both called
`chattrs`.

Start with [README.md](README.md) for what it is and the wire protocol, then
this file for commands and architecture.

## Commands

```bash
cargo build   # refreshes target/debug/chattrs
make fmt      # format the source with rustfmt
make check    # lint gate: rustfmt check, clippy with -D warnings, complexity ≤ 40
make test     # unit + TCP integration tests, must be green with zero warnings
make ci       # check + test, the same commands CI runs
# Toolchain is pinned in rust-toolchain.toml; rustup installs it on first use.
```

Run a broker and exercise the client against an ephemeral port:

```bash
target/debug/chattrs serve --listen 127.0.0.1:47654 --db /tmp/c.db
CHATTRS_ADDRESS=claude@h target/debug/chattrs send --server 127.0.0.1:47654 --to muse@h "hi"
```

## Architecture

| File | Owns |
| ---- | ---- |
| `src/main.rs` | Thin entry: parse `cli`, dispatch to `server`/`client` |
| `src/cli.rs` | Clap `Args` + subcommand structs, connection flags |
| `src/model.rs` | Wire types (`Inbound`, `Outbound`), limits, address validation, serde shapes |
| `src/protocol.rs` | Line-JSON framing: `encode`/`decode` |
| `src/store.rs` | SQLite storage: one `Connection` in `Arc<Mutex<_>>`, blocking calls |
| `src/server.rs` | The broker: `hello`, `send`/`reply`, `inbox`/`ack`, `watch` fan-out, `wait`, `agents`, `log` |
| `src/client.rs` | One connection per command; `watch` holds one open and streams, `ask` holds a `wait` open, `log` pages history |
| `src/skill.rs` | Embedded agent skill text printed by `chattrs skill` |
| `skills/chattrs/SKILL.md` | Agent skill: `send`, `ask`, `reply`, no polling |

Key patterns:

- **Data first.** Types live in `model.rs` and carry the wire shape; behavior
  follows. `model`/`protocol` stay pure — no sockets, no IO.
- **Never block the broker.** Each connection runs its own tokio task with a
  reader loop; storage calls run in `spawn_blocking`.
- **Commit before confirm.** A send is written to SQLite before the broker
  replies `sent`. The broker never deletes a message.

## Code style

Read [docs/code-style.md](docs/code-style.md) before writing code. It covers
module layout, naming, comments, state, and the networking conventions.

## Verification

- End of work, before every push: `make ci`, clean. CI runs the same
  commands.
- `cargo test` after every behavior change. Integration tests in
  `src/server.rs` bind an ephemeral `127.0.0.1:0` and drive the real client
  over TCP — no mocks.

## Planning

Goals: [mission](docs/mission.md) → [requirements](docs/requirements.md) → [phases](docs/planning.md).
Build only the current phase. Cite requirement IDs in every PR (`Meets: MSG-1`).
A PR is done when mergeable (checks green, no conflicts).

## Commits and releases

- Title style: `feat: …`, `fix: …`, `docs: …`, `refactor: …`.
