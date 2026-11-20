---
name: chattrs-fleet
description: Run a chattrs fleet across hosts: install, broker, client env, verify.
---

# chattrs: run a fleet

To set up or repair chattrs, run `chattrs setup-guide` and follow it.

One broker on an always-on host, reachable over Tailscale. Clients
connect with the fleet token.

Install the published binary on every host, never a source build:

`npm install -g chattrs` (any host with Node)

`brew install bolu-atx/tap/chattrs` (macOS arm64)

`chattrs --version` must match on every host.

Multiple hosts need a tailnet. Use a plain LAN only on a trusted home
network. One broker serves every project; a second swarm sets a new
`CHATTRS_PROJECT`, not a second broker.

On the broker host, find its tailnet IP (`tailscale ip -4`). Write a
tokens file at mode 0600, one `[[token]]` per host (`openssl rand -hex 32`;
fields `token`, `addr`, `role`, and an optional `project`). An entry with no
`project` works in every project. A lead address is reserved across the
broker: only the lead token can claim it. Put the lead token only on the
lead's host. See the README "Token table".

Run a `systemd --user` unit with `ExecStart` at the installed binary and
`serve --listen <tailscale-ip>:4646 --db <path> --tokens <tokens-file>`:

```bash
systemctl --user daemon-reload
systemctl --user enable --now chattrs
loginctl enable-linger $USER  # survive logout
```

On every client host, set `server` and `token_file` in
`~/.config/chattrs/config.toml`. The project is the git repo name by
default. Set `project` in the repo's `.chattrs.toml` only to override it,
and set the lead there. Never put `CHATTRS_ADDRESS`, `CHATTRS_SESSION`,
`CHATTRS_LEAD`, or `CHATTRS_PROJECT` in a shell rc file.

Verify both directions: `send` from A, `inbox` on B, `reply`
from B, `inbox` on A. `chattrs agents` lists who connected.

Launch each worker with its own address and its lead:

```bash
chattrs exec --host box --cwd ~/repo --detach --as worker1@box --lead opus@mac claude "Your address is worker1@box. Your lead is opus@mac. Run chattrs watch under the Monitor tool now."
```

`--detach` starts the worker in a tmux session on `box` and prints how to
attach and stop it. Without `--detach`, the worker lives only as long as
the ssh connection. chattrs never watches, kills, or restarts a worker.

If the host's logind kills user processes at logout
(`KillUserProcesses=yes`), a detached tmux session dies with the ssh
connection. On such a host, start tmux in its own scope instead:
`ssh box systemd-run --user --scope tmux new -d -s w1 'chattrs exec --as worker1@box --lead opus@mac claude'`.

The prompt comes from the user or the lead, so the worker trusts it. Put
`$(chattrs skill)` in the same prompt. One address has one live session.

One host needs nothing: the first loopback command starts a broker
on demand, and `chattrs stop` ends it. A remote server is never
auto-started.

Limits: no TLS inside the tailnet, so bind the Tailscale IP only.
Without a token table, one shared token covers the fleet and addresses
are self-asserted.
