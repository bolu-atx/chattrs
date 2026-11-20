---
name: chattrs-fleet
description: Run a chattrs fleet across hosts: install, broker, client env, verify.
---

# chattrs: run a fleet

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
fields `token`, `project`, `addr`, `role`). Put the lead token only on the
lead's host. See the README "Token table".

Run a `systemd --user` unit with `ExecStart` at the installed binary and
`serve --listen <tailscale-ip>:4646 --db <path> --tokens <tokens-file>`:

```bash
systemctl --user daemon-reload
systemctl --user enable --now chattrs
loginctl enable-linger $USER  # survive logout
```

On every client host, set `server` and `token_file` in
`~/.config/chattrs/config.toml`. Set the project and lead in the repo's
`.chattrs.toml`. Never put `CHATTRS_ADDRESS`, `CHATTRS_SESSION`,
`CHATTRS_LEAD`, or `CHATTRS_PROJECT` in a shell rc file.

Verify both directions: `send` from A, `inbox` on B, `reply`
from B, `inbox` on A. `chattrs agents` lists who connected.

Launch each worker in tmux with its own address and its lead:

```bash
ssh box 'systemd-run --user --scope tmux new -d -s w1 '"'"'eval "$(chattrs env --as worker1@box --lead opus@mac)"; exec claude "Your address is worker1@box. Your lead is opus@mac. Run chattrs watch under the Monitor tool now."'"'"''
```

The prompt comes from the user or the lead, so the worker trusts it. Put
`$(chattrs skill)` in the same prompt. One address has one live session.

One host needs nothing: the first loopback command starts a broker
on demand, and `chattrs stop` ends it. A remote server is never
auto-started.

Limits: no TLS inside the tailnet, so bind the Tailscale IP only.
Without a token table, one shared token covers the fleet and addresses
are self-asserted.
