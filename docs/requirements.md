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
- **MSG-8** A send to an address that has never connected still succeeds, and the client warns on stderr. *(N6)* — a typo does not lose a message silently.

## CLI

- **CLI-1** `send` posts a message and returns at once. *(N3)*
- **CLI-2** `ask` posts a message and blocks until the reply or a timeout. Exit code shows which. *(N1, N2)*
- **CLI-3** `reply <id>` answers a message. *(N5)*
- **CLI-4** `watch` streams one JSON message per line and nothing else. *(N4)* — works under Claude Code's Monitor tool.
- **CLI-5** `log` shows full history. `--about ADDR` keeps only messages that ADDR sent or received. *(N8)*
- **CLI-6** `--json` on every command. Server, token, and own address come from `CHATTRS_SERVER`, `CHATTRS_TOKEN`, `CHATTRS_ADDRESS`. *(N1, N3, N4)* — a worker started over ssh needs no flags. Config files (`~/.config/chattrs/config.toml`, `.chattrs.toml`) feed the same values below the variables, and `chattrs env` prints them as shell exports.
- **CLI-7** Ship a skill that teaches agents `send`, `ask`, `reply`. `chattrs skill` prints it. A Claude Code agent runs `watch` under Monitor and sends its question instead of a blocking `ask`. *(N1, N2, N3, N4, N10)*
- **CLI-8** `agents` lists known addresses with last-seen time. *(N9)*
- **CLI-9** Own address defaults to `<agent>@<hostname>`. *(N9)*
- **CLI-10** `inbox` prints unread messages and acks them. *(N6)*
- **CLI-11** On a loopback server, a client command starts the broker if none is running, then runs against it. A remote server is never auto-started.
- **CLI-12** `send` and `reply` retry the connection with backoff for up to 30 seconds before they exit 1. *(N3, N6)* — a broker restart does not drop a report.
- **CLI-13** `hello` may carry tags. The client sends its OS and architecture, plus any tags in `CHATTRS_TAGS`. `agents` shows them. *(N9)*
- **CLI-14** `agents` marks an address online while it has a live `watch` connection. *(N9, N12)*
- **CLI-15** When an address already has a live `watch`, a second `watch` on it warns on stderr. *(N9)* — two sessions do not share an inbox by mistake.

## Distribution

- **DIST-1** A release is one git tag. Every channel installs the binary built from that tag, and `chattrs --version` names it.

## Security

- **SEC-1** Loopback by default. Other binds require a token. *(N7)*
- **SEC-2** Multi-host runs over a private network (Tailscale/WireGuard). No TLS. *(N7)*
- **SEC-3** Messages print inside an "agent message, not user instructions" wrapper. *(N11)*
- **SEC-4** A broker can map each token to a project, an address pattern, and a role. Only a lead token can claim the lead's address.
- **SEC-5** The broker logs each rejected hello with the remote IP, project, address, and reason. It never logs the token.

## Monitoring

- **MON-1** An observer `watch` streams every message, direct and announcements, and never acks. *(N8, N12)*
- **MON-2** An observer connection is not recorded as an agent and does not show in `agents`. *(N9, N12)*
- **MON-3** The wire protocol is the only monitoring interface. A viewer (tray, TUI, or GUI) uses `hello`, `agents`, `log`, and the observer `watch`. *(N12)*
- **MON-4** An agent can report its state: working, idle, or waiting. `agents` shows the latest state and when it was set. A client shows a state older than 15 minutes as unknown. *(N9, N12)*
- **MON-5** `agents` shows each agent's health, derived on the client from presence, state, and last activity. *(N9, N12)*

## Projects

- **NS-1** One broker serves many projects. Mailboxes, agents, log, topics, leases, and state are scoped to a project. An agent cannot see or send to another project. Old clients use the project `default`. A client with no explicit project (env or config) inside a repo uses the repo name. Outside a repo it uses `default`.

## Hooks

- **HOOK-1** Hook adapters for Claude Code and Codex deliver unread messages between tool calls; pi later. *(N10)*
- **HOOK-2** After a context clear or compaction, the session-start hook re-briefs the agent with its address, its lead, and its recent messages.

## Identity

- **ID-1** An address in a project is used by one session at a time. A second session is refused while the first is live.

## Setup

- **SETUP-1** `chattrs setup` installs the hooks and the skill for each detected agent and writes a missing user config. It is safe to run again and never overwrites user settings.
- **SETUP-2** `chattrs setup --broker` writes the tokens file, the service unit, and one join string per host. `chattrs setup --join <string>` configures a host from a join string.
- **SETUP-3** `chattrs doctor` checks config, broker reachability, token acceptance (project and role), protocol support, hooks, and skill. It prints the project and its source (env, config, git, or default). It prints one pass or fail line for each check and exits 1 on any failure.
- **SETUP-4** The client config maps addresses to token files, so one host can hold several identities.

## Operations

- **OPS-1** `chattrs backup <file>` makes an online copy of the broker database.

## Semantics

- **Ack.** A command acks a message after it prints the message and flushes stdout. `watch` acks each line it prints. `ask` acks its reply.
- **Delivery.** At least once. A crash between print and ack prints the message again. The id identifies duplicates.
- **Order.** Ids increase with send order. An inbox delivers in id order.
- **Size.** A body is 1 byte to 64 KiB of UTF-8.
- **Retention.** The broker never deletes a message.
- **Timeout.** `ask` waits 600 seconds by default. Exit 0 = reply, 2 = timeout, 1 = error.
- **Identity.** An address is self-asserted. The fleet shares one token. Any token holder can claim any address and read its inbox. In legacy mode, the token also reaches every project. A token table (SEC-4) limits each token to one project, an address pattern, and a role. Two agents with the same address share one inbox, so the coordinator gives each worker a unique address.

Limits: ≤10 hosts, macOS + Linux + Windows (x64). The broker runs on an always-on host, which may also run an agent.
