#!/bin/sh
# Build chattrs-tray, wrap it in an app bundle, sign it, and install it to
# ~/Applications. A stable signing identity keeps the Keychain from asking
# again after every rebuild. Set CHATTRS_TRAY_SIGN_ID to pick the identity.
set -eu
cd "$(dirname "$0")"

ID="${CHATTRS_TRAY_SIGN_ID:-chattrs-tray local}"
APP="$HOME/Applications/chattrs-tray.app"

if ! security find-identity -p codesigning | grep -qF "\"$ID\""; then
    echo "install.sh: no signing identity \"$ID\". See README \"Install\"." >&2
    exit 1
fi

swift build -c release
pkill -x chattrs-tray 2>/dev/null || true

rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS"
cp .build/release/chattrs-tray "$APP/Contents/MacOS/chattrs-tray"
# The bundle id matches the bare binary's defaults domain, so saved settings carry over.
cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleIdentifier</key><string>chattrs-tray</string>
    <key>CFBundleName</key><string>chattrs-tray</string>
    <key>CFBundleExecutable</key><string>chattrs-tray</string>
    <key>CFBundlePackageType</key><string>APPL</string>
    <key>CFBundleShortVersionString</key><string>1.0</string>
    <key>LSMinimumSystemVersion</key><string>13.0</string>
    <key>LSUIElement</key><true/>
</dict>
</plist>
PLIST

codesign --force --options runtime --sign "$ID" "$APP"
open "$APP"
echo "Installed $APP"
