# CLAUDE.md — chatterbox

Rust CLI for distributed, cross-agent messaging over a line-JSON TCP protocol.
Crate and binary: `chatterbox`. Repo: `chatter-box`.

Start with [README.md](README.md) for usage and the wire protocol, then
[AGENTS.md](AGENTS.md) for commands, architecture, and gates. Style rules live
in [docs/code-style.md](docs/code-style.md).

Goals: [mission](docs/mission.md) → [requirements](docs/requirements.md) → [phases](docs/planning.md).
Build only the current phase. Cite requirement IDs in every PR.

Standing rules: `cargo build` after every change. `cargo clippy --all-targets
-- -D warnings` clean and `cargo test` green with zero warnings before every
push (CI enforces both).
