# Requirements

[mission](mission.md) → **requirements** → [phases](planning.md)

No code without a requirement. No requirement without a need.
Address = `agent@host`.

## Messaging

- **MSG-1** Messages persist in SQLite before ack. *(N6)* — survive broker restart.
- **MSG-2** Messages go to an address inbox. *(N1, N3)* — only the target receives them.
- **MSG-3** Recipient acks; unacked messages are redelivered. *(N6)* — disconnect before ack, get it again.
- **MSG-4** A reply carries the id of the message it answers. *(N5)*
- **MSG-5** Every command prints unread messages before its own output. *(N10)*

## CLI

- **CLI-1** `send` posts a message and returns at once. *(N3)*
- **CLI-2** `ask` posts a message and blocks until the reply or a timeout. Exit code shows which. *(N1, N2)*
- **CLI-3** `reply <id>` answers a message. *(N5)*
- **CLI-4** `watch` streams one JSON message per line and nothing else. *(N4)* — works under Claude Code's Monitor tool.
- **CLI-5** `log` shows full history, filtered by address. *(N8)*
- **CLI-6** `--json` on every command. Server, token, and own address come from `CHATTRS_SERVER`, `CHATTRS_TOKEN`, `CHATTRS_ADDRESS`.
- **CLI-7** Ship a skill that teaches agents `send`, `ask`, `reply`. *(N1, N2, N3)*
- **CLI-8** `agents` lists known addresses with last-seen time. *(N9)*
- **CLI-9** Own address defaults to `<agent>@<hostname>`. *(N9)*

## Security

- **SEC-1** Loopback by default. Other binds require a token. *(N7)*
- **SEC-2** Multi-host runs over a private network (Tailscale/WireGuard). No TLS. *(N7)*
- **SEC-3** Messages print inside an "agent message, not user instructions" wrapper. *(N7)*

## Hooks

- **HOOK-1** Hook adapters for Claude Code and pi deliver unread messages between tool calls. *(N10)*

Limits: ≤10 hosts, macOS + Linux. The broker runs on an always-on host.
