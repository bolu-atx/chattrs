import AppKit
import TrayCore

/// An original 11x8 pixel alien: domed head, wide eyes, splayed legs.
/// Two leg frames let it take a step when a message arrives.
enum Icon {
    private static let head = [
        "....XXX....",
        "..XXXXXXX..",
        ".XXXXXXXXX.",
    ]
    private static let face = "XX..XXX..XX"
    private static let squint = "XX.X.X.X.XX"
    private static let body = [
        "XXXXXXXXXXX",
        "..XX.X.XX..",
    ]
    private static let legsOut = [".XX.....XX.", "XX.......XX"]
    private static let legsIn = ["..X.....X..", "...X...X..."]
    private static let legsLimp = [".X.X...X.X.", ".X.X...X.X."]

    static func grid(for state: ConnectionState, step: Bool) -> [String] {
        switch state {
        case .badToken, .error: return head + [squint] + body + legsLimp
        default: return head + [face] + body + (step ? legsIn : legsOut)
        }
    }

    /// Template image for the menu bar: faint while connecting.
    static func menuBar(for state: ConnectionState, step: Bool) -> NSImage {
        let faint = state == .connecting || state == .reconnecting
        let img = draw(grid(for: state, step: step), px: 2, color: .black.withAlphaComponent(faint ? 0.4 : 1))
        img.isTemplate = true
        return img
    }

    static func draw(_ grid: [String], px: CGFloat, color: NSColor) -> NSImage {
        let cols = grid.first?.count ?? 0
        let size = NSSize(width: CGFloat(cols) * px, height: CGFloat(grid.count) * px)
        return NSImage(size: size, flipped: true) { _ in
            color.setFill()
            for (y, row) in grid.enumerated() {
                for (x, c) in row.enumerated() where c == "X" {
                    NSRect(x: CGFloat(x) * px, y: CGFloat(y) * px, width: px, height: px).fill()
                }
            }
            return true
        }
    }
}
