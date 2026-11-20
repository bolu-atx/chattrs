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
target/debug/chattrs doctor   # check this host: config, broker, token, hooks, skill
target/debug/chattrs backup <file> [--db <db>] [--force]   # online copy of the broker database
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
| `src/main.rs` | Thin entry: parse `cli`, dispatch to one small function per command, map the result to an exit code |
| `src/cli.rs` | Clap `Args` + subcommand structs, connection flags |
| `src/model.rs` | Wire types (`Inbound`, `Outbound`), limits, address validation, serde shapes |
| `src/exec.rs` | `exec` module root (the command itself still lives in `main.rs`) |
| `src/exec/host.rs` | `exec --host`: `sh_quote` and the pure builder of the ssh argv (plain and `--detach` tmux script) |
| `src/output.rs` | Every line the CLI writes to stdout: text rendering, JSON lines, timestamps, the stdout writer |
| `src/protocol.rs` | Line-JSON framing: `encode`/`decode` |
| `src/store.rs` | SQLite storage: one `Connection` in `Arc<Mutex<_>>`, blocking calls |
| `src/server.rs` | The broker: one dispatch per frame, one broadcast push, `watch`/`wait` as connection state; tests in `src/server/tests/` |
| `src/client.rs` | One connection per command, returns data and prints nothing; `watch` streams and acks through a `Watcher` on its own connection, `ask` sends, waits, and acks on one connection, `log` pages history |
| `src/autostart.rs` | On-demand broker: loopback check, detached spawn, pidfile, `stop` |
| `src/setup.rs` | `chattrs setup`: detect agents, install hooks (via `hook/install.rs`) and the skill, write a missing config |
| `src/setup/broker.rs` | `chattrs setup --broker`: write the broker `tokens.toml` (0600) and a systemd or launchd unit, print join strings |
| `src/setup/join.rs` | `chattrs setup --join`: parse join strings, write token files, edit `[tokens]` in `config.toml` |
| `src/hook/install.rs` | `chattrs hooks install` and `hooks remove`: edit the agent hook config files (`hook.rs` holds the hook runtime) |
| `src/backup.rs` | `chattrs backup`: online copy of the database with the SQLite backup API, temp file then rename, mode 0600 |
| `src/doctor.rs` | `chattrs doctor`: read-only host check, one `ok`/`FAIL` line per check; probes the broker with an observer `hello` so no agent is recorded |
| `src/project.rs` | The client project resolver: env, config, git repo name, `default`; runs `git` with a 2 s timeout |
| `src/skill.rs` | Embedded agent skill text printed by `chattrs skill` |
| `skills/chattrs/SKILL.md` | Agent skill: `send`, `ask`, `reply`, no polling |

Key patterns:

- **Data first.** Types live in `model.rs` and carry the wire shape; behavior
  follows. `model`/`protocol` stay pure — no sockets, no IO.
- **Never block the broker.** Each connection runs its own tokio task. A
  reader task owns the bounded line read and forwards lines over a channel;
  the connection loop selects between those lines and pushed messages, and
  storage calls run in `spawn_blocking`.
- **One push path.** The broker broadcasts each stored message once. Each
  connection filters pushes by its own watch and wait state.
- **Commit before confirm.** A send is written to SQLite before the broker
  replies `sent`. The broker never deletes a message.

## Code style

Read [docs/code-style.md](docs/code-style.md) before writing code. It covers
module layout, naming, comments, state, and the networking conventions.

## Verification

- End of work, before every push: `make ci`, clean. CI runs the same
  commands.
- `cargo test` after every behavior change. Integration tests in
  `src/server/tests/` bind an ephemeral `127.0.0.1:0` and drive the real
  client over TCP — no mocks.

## Planning

Goals: [mission](docs/mission.md) → [requirements](docs/requirements.md) → [phases](docs/planning.md).
Build only the current phase. Cite requirement IDs in every PR (`Meets: MSG-1`).
A PR is done when mergeable (checks green, no conflicts).

## Commits and releases

- Title style: `feat: …`, `fix: …`, `docs: …`, `refactor: …`.
