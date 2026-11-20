import Foundation

/// Stable palette slot for an address (FNV-1a), so an agent keeps its colour across launches.
public func colorSlot(for address: String, slots: Int) -> Int {
    var h: UInt32 = 2_166_136_261
    for b in address.utf8 { h = (h ^ UInt32(b)) &* 16_777_619 }
    return Int(h % UInt32(max(slots, 1)))
}

/// Palette slots for a set of addresses: each keeps its hashed slot unless
/// another address already took it, then it moves to the next free one.
/// Up to `slots` addresses never share a colour.
public func colorSlots(for addresses: Set<String>, slots: Int) -> [String: Int] {
    var taken = Set<Int>(), out: [String: Int] = [:]
    for a in addresses.sorted(by: { (colorSlot(for: $0, slots: slots), $0) < (colorSlot(for: $1, slots: slots), $1) }) {
        var s = colorSlot(for: a, slots: slots)
        if taken.count < slots { while taken.contains(s) { s = (s + 1) % slots } }
        taken.insert(s)
        out[a] = s
    }
    return out
}

/// Every project seen in agents and messages, sorted.
public func projectNames(agents: [Agent], messages: [Message]) -> [String] {
    Set(agents.map(\.projectName) + messages.map(\.projectName)).sorted()
}

/// The timeline's span: oldest held message to now, at least five minutes.
public func activityRange(_ messages: [Message], now: Date) -> ClosedRange<Int64> {
    let end = Int64(now.timeIntervalSince1970 * 1000)
    let oldest = messages.map(\.ts).min() ?? end
    return min(oldest, end - 5 * 60_000)...end
}

/// One lane per sender, most recently active first, at most `max`.
public func activityLanes(_ messages: [Message], max: Int = 8) -> [String] {
    var latest: [String: Int64] = [:]
    for m in messages { latest[m.from] = Swift.max(latest[m.from] ?? 0, m.ts) }
    return latest.sorted { $0.value != $1.value ? $0.value > $1.value : $0.key < $1.key }
        .prefix(max).map(\.key)
}

/// The newest message at or before `time`, for the feed to scroll to. Messages are newest first.
public func messageAt(_ time: Int64, in messages: [Message]) -> Message? {
    messages.first { $0.ts <= time } ?? messages.last
}
