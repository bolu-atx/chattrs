# chattrs tray

A small, read-only macOS menu bar app. It shows the agents and messages on a chattrs broker.

It never sends, never acks, and never changes the broker.

## Build and run

Run these from `tray/`. You need the Swift 6 toolchain and macOS 13 or later.

```sh
swift build -c release   # binary: .build/release/chattrs-tray
swift run                # build and run in debug mode
swift test               # needs full Xcode: DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer
```

The app has no Dock icon. Look for the pixel alien in the menu bar. It is solid when connected, faint while it connects, and squints with limp legs on a bad token or error. It takes a step each time a message arrives.

## Activity timeline

Each agent has its own colour, used for its name and its messages. The colour comes from the address, so it stays the same across launches. Agents seen together never share a colour, up to eight of them.

The activity strip has one lane per sender and a tick for each message, from the oldest message the app holds to now. Scroll over it with the trackpad, or click or drag, to move the playhead. The feed scrolls to that moment and highlights the messages around it. Scroll past the right edge, or click Live, to follow new messages again.

## Settings

Click the gear in the menu window. Set the broker as `host:port` and an optional token. Save restarts the connection.

- The host is stored in UserDefaults. The default is `CHATTRS_SERVER`, else `127.0.0.1:4646`.
- The token is stored in the Keychain (service `chattrs-tray`).
- `CHATTRS_TOKEN` overrides the stored token. Use it for development.
- After a bad token, the app stops retrying until you save new settings.
- The app never touches the Keychain on the main thread. After a rebuild, macOS may ask to let the new binary read the token. The menu and the control socket keep working while that prompt is open. A token sent with `connect` is used at once.
- In settings, leave the token field empty to keep the saved token.

## Control socket

Other tools, such as Claude Code, can steer the app through a Unix socket at `~/Library/Application Support/chattrs-tray/control.sock` (mode 0600, in a 0700 directory). Send one JSON line, read one JSON line back.

```sh
SOCK="$HOME/Library/Application Support/chattrs-tray/control.sock"
echo '{"cmd":"status"}' | nc -U "$SOCK"
echo '{"cmd":"connect","server":"100.64.0.7:4646","token":"…"}' | nc -U "$SOCK"
```

- `status` returns `server`, `state`, `token_set`, and the agent and message counts. It never returns the token.
- `connect` switches the broker and saves it, like Save in settings. Leave out `token` to keep the stored one. An empty `token` clears it.
- `CHATTRS_TOKEN`, when set, still overrides the stored token.
- Errors come back as `{"ok":false,"error":"…"}`.

## Contract

The app uses only the chattrs wire protocol: requirements MON-1, MON-2, and MON-3 in [../docs/requirements.md](../docs/requirements.md). It follows the README's "Monitoring API (observer)" flow:

1. `hello` with `"observer":true` and `"project":"*"`. The broker does not record the app or list it in `agents`, and the app sees every project (NS-1).
2. The `welcome` must echo `"observer":true`. If it does not, the broker is too old: the app shows "broker too old: observer unsupported" and stops. It never connects as a normal agent.
3. `watch` with `"all":true` streams every new message. The app keeps what streams, reads `log` after the last id it holds, and drops duplicates by id.
4. `agents` every 7 s gives the agent list, `online`, and `state` (MON-4).

Once the broker has more than one project, a project menu appears under the header. "All projects" groups the agent list by project and labels each message with its project. Picking one project filters the agents, the timeline, and the feed. A broker before projects sends no `project`, which the app reads as `default`. The same address in two projects shows as two agents.

Each online agent has a status dot: green for working, amber for waiting, grey for idle. A hollow dot means unknown: no state, or a `state_at` older than 15 minutes on the tray's clock. An offline agent shows no dot and its last-seen age, whatever its state. A red dot means stalled (MON-5): the agent says it is working but has shown no activity for 10 minutes. An agent offline for over an hour is gone: its name dims further, it sorts last, and the label says "gone". The tray computes health with the README Viewer contract rule, since the broker does not send it.

A message sent with a lead token (`from_role` is `lead`, only from a broker with a tokens file) shows a small "lead" tag after the sender.

The app is read-only. It never sends `send`, `ack`, `wait`, or `inbox`. Nothing in chattrs depends on it running.

After a drop (a broker restart, or a lagging observer the broker closes), the app reconnects with backoff from 1 s to 30 s and catches up from `log`.

The app ignores unknown fields. It skips unknown frame types and prints them to stderr.

## Known limits

- Live messages carry no `acked_at`. Every 15 s the app re-reads the newest 500 messages from `log`, so a blue dot (unread) can lag a read by about 15 s.
