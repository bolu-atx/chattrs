import Foundation
import Testing
@testable import TrayCore

struct StatusTests {
    let now = Date(timeIntervalSince1970: 1_000_000)
    var nowMs: Int64 { 1_000_000_000 }

    @Test func freshStatesMapToColors() {
        for s in ["working", "waiting", "idle"] {
            let a = Agent(address: "a@h", online: true, state: s, stateAt: nowMs - 60_000)
            #expect(a.status(now: now).rawValue == s)
        }
    }

    @Test func offlineWinsOverState() {
        #expect(Agent(address: "a@h", online: false, state: "working", stateAt: nowMs).status(now: now) == .offline)
        #expect(Agent(address: "a@h", online: nil, state: "working", stateAt: nowMs).status(now: now) == .offline)
    }

    @Test func nullStaleOrStrangeStateIsUnknown() {
        #expect(Agent(address: "a@h", online: true).status(now: now) == .unknown)
        #expect(Agent(address: "a@h", online: true, state: "idle", stateAt: nil).status(now: now) == .unknown)
        #expect(Agent(address: "a@h", online: true, state: "idle", stateAt: nowMs - 15 * 60_000 - 1).status(now: now) == .unknown)
        #expect(Agent(address: "a@h", online: true, state: "idle", stateAt: nowMs - 15 * 60_000).status(now: now) == .idle)
        #expect(Agent(address: "a@h", online: true, state: "dreaming", stateAt: nowMs).status(now: now) == .unknown)
    }

    @Test func decodesStateFields() throws {
        let f = try Wire.decode(Data(#"{"type":"agents","agents":[{"address":"a@h","last_seen":1,"online":true,"tags":[],"state":"waiting","state_at":5},{"address":"b@h","last_seen":2,"online":false,"tags":[],"state":null,"state_at":null}]}"#.utf8))
        guard case .agents(let a) = f else { Issue.record("not agents"); return }
        #expect(a[0].state == "waiting" && a[0].stateAt == 5 && a[1].state == nil && a[1].stateAt == nil)
    }
}
