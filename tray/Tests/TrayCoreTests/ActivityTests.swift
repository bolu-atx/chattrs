import Foundation
import Testing
@testable import TrayCore

struct ActivityTests {
    func msg(_ id: Int64, _ from: String, _ ts: Int64) -> Message {
        let j = #"{"id":\#(id),"from":"\#(from)","to":"x@h","body":"b","ts":\#(ts)}"#
        return try! JSONDecoder().decode(Message.self, from: Data(j.utf8))
    }

    @Test func colorSlotIsStable() {
        #expect(colorSlot(for: "codex@box1", slots: 8) == colorSlot(for: "codex@box1", slots: 8))
        #expect((0..<8).contains(colorSlot(for: "lead@mac", slots: 8)))
    }

    @Test func colorSlotsNeverCollideUpToPaletteSize() {
        let addrs = Set((1...8).map { "agent\($0)@host" })
        #expect(Set(colorSlots(for: addrs, slots: 8).values).count == 8)
    }

    @Test func rangeSpansAtLeastFiveMinutes() {
        let now = Date(timeIntervalSince1970: 10_000)
        #expect(activityRange([], now: now) == 9_700_000...10_000_000)
        #expect(activityRange([msg(1, "a@h", 1_000_000)], now: now).lowerBound == 1_000_000)
    }

    @Test func lanesAreMostRecentSendersFirst() {
        let ms = [msg(3, "b@h", 300), msg(2, "a@h", 200), msg(1, "b@h", 100)]
        #expect(activityLanes(ms) == ["b@h", "a@h"])
        #expect(activityLanes(ms, max: 1) == ["b@h"])
    }

    @Test func messageAtPicksNewestAtOrBefore() {
        let ms = [msg(3, "a@h", 300), msg(2, "a@h", 200), msg(1, "a@h", 100)]  // newest first
        #expect(messageAt(250, in: ms)?.id == 2)
        #expect(messageAt(50, in: ms)?.id == 1)
    }
}
