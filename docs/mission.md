# Mission

**mission → needs** (this file) → [requirements](requirements.md) → [phases](planning.md)

A durable mailbox for coding agents across machines. A coordinator (Claude
Opus) starts workers (Codex, Muse, pi, Claude) on remote hosts by its own
means, such as ssh. Chattrs lets the workers and the coordinator talk while
the work runs.

## Principles

1. Workers start every exchange. The coordinator never interrupts.
2. Never lose a message.
3. Only the asker blocks.
4. Messages are data, not commands.
5. One Rust binary. SQLite. CLI only.

## Needs

Worker
- **N1** Ask the coordinator for help and wait for the answer.
- **N2** Pause at a checkpoint until the coordinator says go.
- **N3** Report progress or completion without waiting.

Coordinator
- **N4** Wake when a message arrives.
- **N5** Reply to a specific question.
- **N6** Miss nothing while asleep or offline.

Operator
- **N7** Only fleet hosts can send messages.
- **N8** Read the full message history.

## Not

Not a task runner. Not an agent launcher. Not code sync. Ssh, git, and the
agents themselves do those jobs.
