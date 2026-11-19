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

On the broker host, find its tailnet IP (`tailscale ip -4`), store
the token, and keep a broker running:

```bash
printf 'CHATTRS_TOKEN=%s\n' "$(openssl rand -hex 32)" > ~/.config/chattrs/env
chmod 600 ~/.config/chattrs/env
```

A `systemd --user` unit with `EnvironmentFile` pointing at that env
file, `ExecStart` at the installed binary with
`serve --listen <tailscale-ip>:4646 --db <path>`, then:

```bash
systemctl --user daemon-reload
systemctl --user enable --now chattrs
loginctl enable-linger $USER  # survive logout
```

On every client host:

```bash
export CHATTRS_SERVER=<tailscale-ip>:4646
export CHATTRS_TOKEN=<token>
export CHATTRS_ADDRESS=<agent>@<host>
```

Verify both directions: `send` from A, `inbox` on B, `reply`
from B, `inbox` on A. `chattrs agents` lists who connected.

Launch each worker with its own address and its lead's address in
the prompt, plus the skill: "Your address is worker1@mac. Your lead
is opus@mac." The prompt comes from the user or the lead, so the
worker trusts it. Put `$(chattrs skill)` in the same prompt.

One host needs nothing: the first loopback command starts a broker
on demand, and `chattrs stop` ends it. A remote server is never
auto-started.

Limits: no TLS inside the tailnet, so bind the Tailscale IP only.
One token covers the fleet. Addresses are self-asserted: the token
proves membership, not which address is yours.
