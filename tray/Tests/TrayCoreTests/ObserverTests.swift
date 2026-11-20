import Foundation
import Testing
@testable import TrayCore

struct ObserverTests {
    @Test func welcomeEchoesObserverOrNot() throws {
        #expect(try Wire.decode(Data(#"{"type":"welcome","server":"chattrs","v":1,"address":"tray@m","observer":true}"#.utf8))
            == .welcome(observer: true))
        #expect(try Wire.decode(Data(#"{"type":"welcome","server":"chattrs","v":1,"address":"tray@m"}"#.utf8))
            == .welcome(observer: false))
    }

    @Test func liveMessageHasNoAckedAt() throws {
        let f = try Wire.decode(Data(#"{"type":"message","id":7,"from":"a@h","to":null,"body":"x","ts":1,"reply_to":null,"topic":"r"}"#.utf8))
        guard case .message(let m) = f else { Issue.record("not a message"); return }
        #expect(m.id == 7 && m.to == nil && m.topic == "r" && m.ackedAt == nil)
    }

    @Test func agentOnlineIsOptional() throws {
        let f = try Wire.decode(Data(#"{"type":"agents","agents":[{"address":"a@h","last_seen":1,"online":true,"tags":["gpu"]},{"address":"b@h","last_seen":2}]}"#.utf8))
        guard case .agents(let a) = f else { Issue.record("not agents"); return }
        #expect(a[0].online == true && a[1].online == nil)
    }

    @Test func helloAsksForObserver() throws {
        let o = try JSONSerialization.jsonObject(with: Wire.hello(address: "tray@m", token: nil)) as? [String: Any]
        #expect(o?["observer"] as? Bool == true)
        let w = try JSONSerialization.jsonObject(with: Wire.watchAllRequest()) as? [String: Any]
        #expect(w?["type"] as? String == "watch" && w?["all"] as? Bool == true)
    }
}
