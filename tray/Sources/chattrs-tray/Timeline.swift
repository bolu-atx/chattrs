import AppKit
import SwiftUI
import TrayCore

/// Agent identity colours. Chosen to read on light and dark, and to stay
/// clear of the status colours (green, amber, grey).
@MainActor
enum AgentPalette {
    static let colors: [Color] = [
        Color(red: 0.357, green: 0.553, blue: 0.937),  // #5B8DEF blue
        Color(red: 0.635, green: 0.455, blue: 0.910),  // #A274E8 violet
        Color(red: 0.878, green: 0.416, blue: 0.651),  // #E06AA6 pink
        Color(red: 0.184, green: 0.710, blue: 0.659),  // #2FB5A8 teal
        Color(red: 0.878, green: 0.471, blue: 0.290),  // #E0784A coral
        Color(red: 0.310, green: 0.639, blue: 0.851),  // #4FA3D9 sky
        Color(red: 0.761, green: 0.604, blue: 0.118),  // #C29A1E gold
        Color(red: 0.498, green: 0.651, blue: 0.314),  // #7FA650 olive
    ]

    private static var slots: [String: Int] = [:]

    /// Call when the known addresses change, so current agents never share a colour.
    static func assign(_ addresses: Set<String>) { slots = colorSlots(for: addresses, slots: colors.count) }

    static func color(_ address: String) -> Color {
        colors[slots[address] ?? colorSlot(for: address, slots: colors.count)]
    }
}

/// One lane per sender with a tick per message. Scroll, click, or drag to
/// move the playhead; the feed follows. `playhead` nil means live.
struct ActivityStrip: View {
    let messages: [Message]
    @Binding var playhead: Int64?

    private static let clock: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "HH:mm"
        return f
    }()

    var body: some View {
        TimelineView(.periodic(from: .now, by: 10)) { ctx in
            let range = activityRange(messages, now: ctx.date)
            let lanes = activityLanes(messages)
            VStack(alignment: .leading, spacing: 4) {
                HStack {
                    Text("Activity").font(.subheadline.weight(.semibold)).foregroundStyle(.secondary)
                    Spacer()
                    if let p = playhead {
                        Text(Self.clock.string(from: date(p))).font(.caption.monospacedDigit()).foregroundStyle(Theme.amber)
                        Button("Live") { playhead = nil }.controlSize(.small)
                    } else {
                        Text("Live").font(.caption).foregroundStyle(.secondary)
                    }
                }
                GeometryReader { geo in
                    ZStack {
                        Canvas { gc, size in draw(gc, size, range: range, lanes: lanes) }
                        ScrubCatcher(
                            onScrub: { x in playhead = time(atX: x, width: geo.size.width, range: range) },
                            onScroll: { dx in scroll(by: dx, width: geo.size.width, range: range) }
                        )
                    }
                }
                .frame(height: 44)
                .background(.quaternary.opacity(0.35), in: Rectangle())
                HStack {
                    Text(Self.clock.string(from: date(range.lowerBound)))
                    Spacer()
                    Text("now")
                }
                .font(.caption2.monospacedDigit()).foregroundStyle(.tertiary)
            }
            .padding(.horizontal, Theme.pad).padding(.vertical, 10)
            .accessibilityElement(children: .ignore)
            .accessibilityLabel("Activity timeline")
            .accessibilityValue(playhead.map { Self.clock.string(from: date($0)) } ?? "live")
            .accessibilityAdjustableAction { dir in
                let stepMs = (range.upperBound - range.lowerBound) / 20
                let p = playhead ?? range.upperBound
                set(dir == .increment ? p + stepMs : p - stepMs, range: range)
            }
        }
    }

    private func draw(_ gc: GraphicsContext, _ size: CGSize, range: ClosedRange<Int64>, lanes: [String]) {
        let span = Double(max(range.upperBound - range.lowerBound, 1))
        let laneH = size.height / CGFloat(max(lanes.count, 1))
        let tickH = max(4, min(laneH - 3, 12))
        let index = Dictionary(uniqueKeysWithValues: lanes.enumerated().map { ($1, $0) })
        for m in messages {
            guard let lane = index[m.from] else { continue }
            let x = CGFloat(Double(m.ts - range.lowerBound) / span) * size.width
            let y = CGFloat(lane) * laneH + (laneH - tickH) / 2
            gc.fill(Path(CGRect(x: min(x, size.width - 3), y: y, width: 3, height: tickH)),
                    with: .color(AgentPalette.color(m.from)))
        }
        if let p = playhead {
            let x = CGFloat(Double(p - range.lowerBound) / span) * size.width
            gc.fill(Path(CGRect(x: x - 1, y: 0, width: 2, height: size.height)), with: .color(Theme.amber))
        }
    }

    private func time(atX x: CGFloat, width: CGFloat, range: ClosedRange<Int64>) -> Int64? {
        let f = min(max(Double(x / max(width, 1)), 0), 1)
        if f > 0.985 { return nil }  // the right edge is live
        return range.lowerBound + Int64(f * Double(range.upperBound - range.lowerBound))
    }

    /// Scrolling left goes back in time; past the right edge returns to live.
    private func scroll(by dx: CGFloat, width: CGFloat, range: ClosedRange<Int64>) {
        let msPerPt = Double(range.upperBound - range.lowerBound) / Double(max(width, 1))
        let p = playhead ?? range.upperBound
        set(p + Int64(Double(dx) * msPerPt), range: range)
    }

    private func set(_ t: Int64, range: ClosedRange<Int64>) {
        playhead = t >= range.upperBound ? nil : max(range.lowerBound, t)
    }

    private func date(_ ms: Int64) -> Date { Date(timeIntervalSince1970: Double(ms) / 1000) }
}

/// Catches trackpad scrolls, clicks, and drags over the strip.
private struct ScrubCatcher: NSViewRepresentable {
    let onScrub: (CGFloat) -> Void
    let onScroll: (CGFloat) -> Void

    func makeNSView(context: Context) -> CatcherView { CatcherView() }

    func updateNSView(_ view: CatcherView, context: Context) {
        view.onScrub = onScrub
        view.onScroll = onScroll
    }

    final class CatcherView: NSView {
        var onScrub: ((CGFloat) -> Void)?
        var onScroll: ((CGFloat) -> Void)?

        override func scrollWheel(with e: NSEvent) {
            // Horizontal swipes move along time; a vertical wheel works too.
            let d = abs(e.scrollingDeltaX) >= abs(e.scrollingDeltaY) ? -e.scrollingDeltaX : e.scrollingDeltaY
            onScroll?(d)
        }

        override func mouseDown(with e: NSEvent) { scrub(e) }
        override func mouseDragged(with e: NSEvent) { scrub(e) }

        private func scrub(_ e: NSEvent) { onScrub?(convert(e.locationInWindow, from: nil).x) }
    }
}
