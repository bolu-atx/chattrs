import Foundation
import Testing
@testable import TrayCore

/// MON-5 rule from the README Viewer contract.
struct HealthTests {
    let now = Date(timeIntervalSince1970: 100_000)
    let ms: Int64 = 100_000_000
    let min: Int64 = 60_000

    @Test func goneIsOfflineOverAnHourAndCheckedFirst() {
        #expect(Agent(address: "a@h", lastSeen: ms - 61 * min, online: false, state: "working", stateAt: ms - 61 * min).health(now: now) == .gone)
        #expect(Agent(address: "a@h", lastSeen: ms - 59 * min, online: false).health(now: now) == .none)
    }

    @Test func stalledIsWorkingWithTenMinutesQuiet() {
        #expect(Agent(address: "a@h", lastSeen: ms - 10 * min, online: true, state: "working", stateAt: ms - 20 * min).health(now: now) == .stalled)
        #expect(Agent(address: "a@h", lastSeen: ms - 9 * min, online: true, state: "working", stateAt: ms - 20 * min).health(now: now) == .healthy)
        #expect(Agent(address: "a@h", lastSeen: ms - 20 * min, online: false, state: "working", stateAt: ms - 20 * min).health(now: now) == .stalled)
    }

    @Test func activityIsTheLaterOfLastSeenAndStateAt() {
        #expect(Agent(address: "a@h", lastSeen: ms - 30 * min, online: true, state: "working", stateAt: ms - 1 * min).health(now: now) == .healthy)
    }

    @Test func quietIdleOnlineAgentHasNoLabel() {
        #expect(Agent(address: "a@h", lastSeen: ms - 30 * min, online: true, state: "idle", stateAt: ms - 30 * min).health(now: now) == .none)
    }

    @Test func decodesFromRole() throws {
        let f = try Wire.decode(Data(#"{"type":"message","id":1,"from":"lead@mac","to":"a@h","body":"x","ts":1,"from_role":"lead"}"#.utf8))
        guard case .message(let m) = f else { Issue.record("not message"); return }
        #expect(m.fromRole == "lead")
    }
}
