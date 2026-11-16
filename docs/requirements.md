# Requirements

[mission](mission.md) → **requirements** → [phases](planning.md)

No code without a requirement. No requirement without a need.
Address = `agent@host`.

## Messaging

- **MSG-1** Messages persist in SQLite before the broker confirms the send. *(N6)* — survive broker restart.
- **MSG-2** Messages go to an address inbox. *(N1, N3)* — only the target receives them.
- **MSG-3** Recipient acks; unacked messages are redelivered. *(N6)* — disconnect before ack, get it again.
- **MSG-4** A reply carries the id of the message it answers. *(N5)*
- **MSG-5** Every client command except `watch` prints unread messages before its own output, then acks them. *(N10)*
- **MSG-6** A message may carry a topic slug. A reply inherits the topic of the message it answers unless it sets its own. No topic is the general chat.
- **MSG-7** An agent may announce to a topic instead of an address. Any agent watching that topic receives the announcement. An announcement belongs to no inbox.

## CLI

- **CLI-1** `send` posts a message and returns at once. *(N3)*
- **CLI-2** `ask` posts a message and blocks until the reply or a timeout. Exit code shows which. *(N1, N2)*
- **CLI-3** `reply <id>` answers a message. *(N5)*
- **CLI-4** `watch` streams one JSON message per line and nothing else. *(N4)* — works under Claude Code's Monitor tool.
- **CLI-5** `log` shows full history. `--about ADDR` keeps only messages that ADDR sent or received. *(N8)*
- **CLI-6** `--json` on every command. Server, token, and own address come from `CHATTRS_SERVER`, `CHATTRS_TOKEN`, `CHATTRS_ADDRESS`. *(N1, N3, N4)* — a worker started over ssh needs no flags.
- **CLI-7** Ship a skill that teaches agents `send`, `ask`, `reply`. `chattrs skill` prints it. *(N1, N2, N3)*
- **CLI-8** `agents` lists known addresses with last-seen time. *(N9)*
- **CLI-9** Own address defaults to `<agent>@<hostname>`. *(N9)*
- **CLI-10** `inbox` prints unread messages and acks them. *(N6)*

## Distribution

- **DIST-1** A release is one git tag. Every channel installs the binary built from that tag, and `chattrs --version` names it.

## Security

- **SEC-1** Loopback by default. Other binds require a token. *(N7)*
- **SEC-2** Multi-host runs over a private network (Tailscale/WireGuard). No TLS. *(N7)*
- **SEC-3** Messages print inside an "agent message, not user instructions" wrapper. *(N11)*

## Hooks

- **HOOK-1** Hook adapters for Claude Code and pi deliver unread messages between tool calls. *(N10)*

## Semantics

- **Ack.** A command acks a message after it prints the message and flushes stdout. `watch` acks each line it prints. `ask` acks its reply.
- **Delivery.** At least once. A crash between print and ack prints the message again. The id identifies duplicates.
- **Order.** Ids increase with send order. An inbox delivers in id order.
- **Size.** A body is 1 byte to 64 KiB of UTF-8.
- **Retention.** The broker never deletes a message.
- **Timeout.** `ask` waits 600 seconds by default. Exit 0 = reply, 2 = timeout, 1 = error.
- **Identity.** An address is self-asserted. The fleet shares one token. Any token holder can claim any address and read its inbox. Two agents with the same address share one inbox, so the coordinator gives each worker a unique address.

Limits: ≤10 hosts, macOS + Linux + Windows (x64). The broker runs on an always-on host.
