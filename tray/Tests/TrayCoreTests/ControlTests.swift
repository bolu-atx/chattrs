import Foundation
import Testing
@testable import TrayCore

struct ControlTests {
    @Test func parsesConnectWithAndWithoutToken() throws {
        #expect(try Control.parse(Data(#"{"cmd":"connect","server":"10.0.0.5:4646","token":"t"}"#.utf8)).get()
            == .connect(server: "10.0.0.5:4646", token: "t"))
        #expect(try Control.parse(Data(#"{"cmd":"connect","server":"h:1","extra":1}"#.utf8)).get()
            == .connect(server: "h:1", token: nil))
    }

    @Test func parsesStatusAndRejectsBadInput() throws {
        #expect(try Control.parse(Data(#"{"cmd":"status"}"#.utf8)).get() == .status)
        #expect((try? Control.parse(Data(#"{"cmd":"connect"}"#.utf8)).get()) == nil)
        #expect((try? Control.parse(Data(#"{"cmd":"nope"}"#.utf8)).get()) == nil)
        #expect((try? Control.parse(Data("junk".utf8)).get()) == nil)
    }
}
