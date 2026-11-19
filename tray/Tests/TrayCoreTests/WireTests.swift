import Foundation
import Testing
@testable import TrayCore

struct WireTests {
    private func d(_ s: String) -> Data { Data(s.utf8) }

    @Test func testLogWithNullsAndUnknownFields() throws {
        let line = #"{"type":"log","extra":1,"messages":[{"id":1,"from":"a@h","to":null,"body":"x","ts":5,"reply_to":null,"topic":null,"acked_at":null,"future":{"k":1}},{"id":2,"from":"a@h","to":"b@h","body":"y","ts":6}]}"#
        guard case .log(let m) = try Wire.decode(d(line)) else { Issue.record("not log"); return }
        #expect(m.count == 2)
        #expect(m[0].to == nil); #expect(m[0].ackedAt == nil); #expect(m[0].topic == nil)
        #expect(m[1].replyTo == nil)
        #expect(m[1].route == "a@h → b@h")
    }

    @Test func testAnnouncementRoute() throws {
        let line = #"{"type":"log","messages":[{"id":1,"from":"a@h","to":null,"body":"x","ts":5,"topic":"r1"}]}"#
        guard case .log(let m) = try Wire.decode(d(line)) else { Issue.record("wrong frame"); return }
        #expect(m[0].route == "a@h → #r1")
    }

    @Test func testAgentsMissingLastSeen() throws {
        guard case .agents(let a) = try Wire.decode(d(#"{"type":"agents","agents":[{"address":"a@h"}]}"#)) else { Issue.record("wrong frame"); return }
        #expect(a[0].lastSeen == nil)
    }

    @Test func testUnknownTypeAndError() throws {
        #expect(try Wire.decode(d(#"{"type":"zzz","a":1}"#)) == .unknown("zzz"))
        #expect(try Wire.decode(d(#"{"type":"error","code":"bad_token","message":"no"}"#)) == .error(code: "bad_token", message: "no"))
        #expect(throws: (any Error).self) { try Wire.decode(d("{}")) }
    }

    @Test func testHelloOmitsEmptyToken() {
        #expect(!(String(decoding: Wire.hello(address: "tray@h", token: nil), as: UTF8.self).contains("token")))
        #expect(String(decoding: Wire.hello(address: "tray@h", token: "t"), as: UTF8.self).contains(#""token":"t""#))
    }

    @Test func testLineBufferSplits() {
        var b = LineBuffer()
        #expect(b.append(d("{\"a\":1}\n{\"b\"")).lines.count == 1)
        #expect(b.append(d(":2}\n")).lines.count == 1)
    }
}
