# Mission

**mission → needs** (this file) → [requirements](requirements.md) → [phases](planning.md)

A durable mailbox for coding agents across machines. A coordinator (Claude
Opus) starts workers (Codex, Muse, pi, Claude) on remote hosts by its own
means, such as ssh. Chattrs lets any of them talk while the work runs, like
Claude Code's agent-to-agent messages, but for every agent.

## Vision

One fleet, many vendors. Claude Opus coordinates from a Mac. Codex writes
code on `box1`. Muse reviews on `box2`. A DeepSeek agent runs the long test
suite on `box3`. One broker runs on an always-on host. Every agent has the
same small CLI and an address.

```
opus@mac ──┐                     ┌── codex@box1
           ├── broker (SQLite) ──┼── muse@box2
operator ──┘                     └── deepseek@box3
```

A normal hour:

1. Opus starts each worker over ssh with `CHATTRS_ADDRESS` set, then goes
   idle. `chattrs watch` under Monitor is its only wake source.
2. Codex finishes a change: `chattrs send --to muse@box2 "branch feat/x is
   ready for review"`. Worker to worker. Opus is not in the path.
3. Muse finds a design problem that needs a decision:
   `chattrs ask --to opus@mac "keep the old API or break it?"`. Only Muse
   blocks. Opus wakes, runs `chattrs reply 41 "break it"`, and Muse continues.
4. The DeepSeek agent runs tests for forty minutes. Its next chattrs command
   prints the messages that arrived in that time. It did not poll.
5. The Mac sleeps, or the broker restarts. No message is lost. Each one
   arrives when its recipient next checks.
6. The next day, the operator runs `chattrs log` and reads who said what.

What if any agent could work in a swarm the way Claude Code agents do?
Chattrs brings that pattern to every agent and every machine. A typical
swarm: each test machine (Linux, Windows, an ARM Mac) runs a long-lived
agent session in tmux. The lead session on the Mac hands out work. Workers
report, ask, and talk to each other. One machine in the swarm also runs
the broker.

The operator watches the swarm from a menu bar or tray app: who is online,
on what hardware, and each message as it passes.

The same commands work at every distance. Only the server address changes:

- **One machine:** loopback, no token.
- **One LAN or one tailnet:** the broker binds a private address, with a token.
- **Behind NAT, no tailnet:** `ssh -R` forwards the broker port to the worker.

## Why not something else

| Alternative | Where it stops |
| ----------- | -------------- |
| Claude Code `SendMessage` | The model chattrs follows. Chattrs brings the same pattern to Codex, Muse, DeepSeek, scripts, and agents on other machines. |
| ssh and read the worker's output | One direction. The coordinator must poll. A worker cannot ask a question. |
| tmux or herdr `send-keys` | The target must be live and at a prompt. No queue, no history. The text arrives as if the user typed it. |
| Shared files or git commits | No wake signal. Needs a shared disk or a push for each message. |
| MCP message server | Each agent needs MCP support and config. Tool schemas cost context in every session. |
| NATS or Redis | A second service to run. No agent CLI, no blocking `ask`, no trust wrapper. |
| Slack or GitHub comments | Leaves the private network. Rate limits. Human channels fill with agent noise. |

Chattrs needs only what every coding agent already has: a shell. It adds
three things the list above lacks together: a queue that survives restarts,
a blocking `ask` for the one agent that needs an answer, and a wrapper that
marks each message as agent input.

It does less than each alternative in one way: it starts nothing, runs
nothing, and moves no code.

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
- **N12** See swarm activity live: who is online and each message as it passes.

Any agent
- **N9** Find which agents exist, what hardware each runs on, and which are listening now.
- **N10** Receive messages mid-task without asking for them.
- **N11** Never mistake an agent message for a user instruction.

A one-shot worker that has exited cannot be woken. Its messages wait until
it runs again.

## Not

Not a task runner. Not an agent launcher. Not code sync. Ssh, git, and the
agents themselves do those jobs. Not a GUI: a menu bar or tray app is a
separate program that speaks the wire protocol.
