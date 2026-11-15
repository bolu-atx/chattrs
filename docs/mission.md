# Mission

**mission → needs** (this file) → [requirements](requirements.md) → [phases](planning.md)

A coordinator agent (Claude Opus) runs a fleet of coding agents (Codex, Muse,
pi, Claude) across machines. It gives out work without waiting, and results
come back reliably. Any agent can message any other.

## Principles

1. Never block the sender.
2. Never lose a message.
3. Agents are one-shot processes. Chatterbox starts them when work arrives.
4. Messages are data, not commands. A task names an adapter, never a shell command.
5. One Rust binary. SQLite. Nothing else.

## Needs

Coordinator
- **N1** Assign a task to an agent on any host and keep working.
- **N2** Get every result back, across restarts and offline hosts.
- **N3** See fleet and task state.
- **N4** Wake when a reply or question arrives.

Worker
- **N5** Start when a task arrives. Stop when done.
- **N6** Ask a question mid-task and get the answer.

Operator
- **N7** Only fleet members can send tasks.
- **N8** Cancel, time-limit, and cap concurrent work.
- **N9** Audit what happened.

## Not

Not an agent. Not a terminal controller (Rex, herdr do that). Not code sync (git does that).
