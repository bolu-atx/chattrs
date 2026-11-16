# CLAUDE.md — chattrs

Rust CLI for distributed, cross-agent messaging over a line-JSON TCP protocol.
Crate and binary: `chattrs`. Repo: `chattrs`.

Start with [README.md](README.md) for usage and the wire protocol, then
[AGENTS.md](AGENTS.md) for commands, architecture, and gates. Style rules live
in [docs/code-style.md](docs/code-style.md).

Goals: [mission](docs/mission.md) → [requirements](docs/requirements.md) → [phases](docs/planning.md).
Build only the current phase. Cite requirement IDs in every PR.

Standing rules: `cargo build` after every change. `make fmt`, then `make ci`
clean with zero warnings before every push (CI runs the same commands).
