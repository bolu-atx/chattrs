import AppKit
import TrayCore

/// Pixel alien for the menu bar. Solid when connected, faint while connecting,
/// cracked visor and limp tentacles on a bad token or error.
enum Icon {
    private static let alien = [
        "....X....",
        "...XXX...",
        ".XXXXXXX.",
        "XX.....XX",
        "XXXXXXXXX",
        ".XXXXXXX.",
        ".X.X.X.X.",
        "X..X.X..X",
    ]
    private static let broken = [
        "....X....",
        "...XXX...",
        ".XXXXXXX.",
        "XX.X.X.XX",
        "XXXXXXXXX",
        ".XXXXXXX.",
        ".X.X.X.X.",
        ".X.X.X.X.",
    ]

    static func image(for state: ConnectionState) -> NSImage {
        switch state {
        case .connected: return draw(alien, alpha: 1)
        case .connecting, .reconnecting: return draw(alien, alpha: 0.4)
        case .badToken, .error: return draw(broken, alpha: 1)
        }
    }

    private static func draw(_ grid: [String], alpha: CGFloat) -> NSImage {
        let px: CGFloat = 2
        let rows = grid.count
        let size = NSSize(width: 18, height: 18)
        let img = NSImage(size: size, flipped: true) { _ in
            NSColor.black.withAlphaComponent(alpha).setFill()
            let top = (size.height - CGFloat(rows) * px) / 2
            for (y, row) in grid.enumerated() {
                for (x, c) in row.enumerated() where c == "X" {
                    NSRect(x: CGFloat(x) * px, y: top + CGFloat(y) * px, width: px, height: px).fill()
                }
            }
            return true
        }
        img.isTemplate = true
        return img
    }
}
