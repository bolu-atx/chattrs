# Mission

**mission → needs** (this file) → [requirements](requirements.md) → [phases](planning.md)

A durable mailbox for coding agents across machines. A coordinator (Claude
Opus) starts workers (Codex, Muse, pi, Claude) on remote hosts by its own
means, such as ssh. Chattrs lets any of them talk while the work runs, like
Claude Code's agent-to-agent messages, but for every agent.

## Principles

1. Any agent can message any agent. Delivery happens when the recipient next checks.
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

Any agent
- **N9** Find which agents exist and can be reached.
- **N10** Receive messages mid-task without asking for them.
- **N11** Never mistake an agent message for a user instruction.

A one-shot worker that has exited cannot be woken. Its messages wait until
it runs again.

## Not

Not a task runner. Not an agent launcher. Not code sync. Ssh, git, and the
agents themselves do those jobs.
