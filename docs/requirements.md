# Requirements

[mission](mission.md) → **requirements** → [phases](planning.md)

No code without a requirement. No requirement without a need.
Address = `agent@host`.

## Messaging

- **MSG-1** Messages persist in SQLite before ack. *(N2)* — survives broker restart.
- **MSG-2** Messages go to an address inbox. *(N1, N6)* — only the target receives it.
- **MSG-3** Recipient acks; unacked messages are redelivered. *(N2, N5)* — disconnect before ack, get it again.
- **MSG-4** `correlation_id` pairs a question with its answer. *(N6)*
- **MSG-5** `watch` streams one JSON message per line. *(N4)* — works under Claude Code's Monitor tool.

## Tasks

- **TSK-1** Task = target, adapter, prompt, repo + ref, time limit. *(N1)*
- **TSK-2** States: `queued → running → done | failed | cancelled | timed_out`. *(N3, N9)*
- **TSK-3** Result = state, final text, branch, exit code. *(N2)*
- **TSK-4** Time limit and cancel kill the process. *(N8)*

## Runner

- **RUN-1** A per-host runner starts the adapter for each task. *(N5)* — task sent before runner starts still runs.
- **RUN-2** Adapters live in local config. Unknown adapter → fail, run nothing. *(N7)*
- **RUN-3** Adapters for `claude`, `codex`, `muse`, `pi`. *(N5)*
- **RUN-4** One git worktree per task. *(N8)*
- **RUN-5** Concurrency cap per host. *(N8)*

## Interface

- **API-1** CLI with `--json` for every command. *(N1, N3)*
- **API-2** `chatterbox mcp`: `send`, `inbox`, `ask`, `reply`, `task`. *(N1, N6)*
- **API-3** `ask` blocks only the caller, until answer or timeout. *(N6)*

## Security

- **SEC-1** Loopback by default. Other binds require a token. *(N7)*
- **SEC-2** Token per host, limited to its addresses. *(N7)*
- **SEC-3** Message bodies reach agents marked as untrusted input. *(N7)*
- **SEC-4** Multi-host runs over a private network (Tailscale/WireGuard). No TLS. *(N7)*

## Ops

- **OPS-1** `chatterbox fleet` shows hosts, runners, tasks by state. *(N3)*
- **OPS-2** `chatterbox log` shows full history. *(N9)*

Limits: ≤10 hosts, macOS + Linux.
