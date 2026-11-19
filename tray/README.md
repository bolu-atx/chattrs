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

The app has no Dock icon. Look for the pixel alien in the menu bar. It is solid when connected, faint while it connects, and shows a cracked visor on a bad token or error.

## Settings

Click the gear in the menu window. Set the broker as `host:port` and an optional token. Save restarts the connection.

- The host is stored in UserDefaults. The default is `CHATTRS_SERVER`, else `127.0.0.1:4646`.
- The token is stored in the Keychain (service `chattrs-tray`).
- `CHATTRS_TOKEN` overrides the stored token. Use it for development.
- After a bad token, the app stops retrying until you save new settings.

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

The app uses only the chattrs wire protocol: requirements MON-1, MON-2, and MON-3 in [../docs/requirements.md](../docs/requirements.md). It sends `hello`, `agents`, and `log`. It never sends a plain `watch`, because that acks messages.

The app ignores unknown fields. It skips unknown frame types and prints them to stderr.

## Known limits

- Until the observer hello flag (issue #49) ships, the app address `tray@<host>` shows in `agents`.
- Polling is the v1 path. It asks for `log` every 1.5 s and `agents` every 7 s. It will switch to the observer `watch`. Only `PollingSource` changes.
- Every 10th `log` poll re-reads the newest 500 messages, so a blue dot (unread) can lag a read by about 15 s.
