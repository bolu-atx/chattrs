import Foundation
import Testing
@testable import TrayCore

struct ProjectTests {
    @Test func missingProjectIsDefault() throws {
        let f = try Wire.decode(Data(#"{"type":"log","messages":[{"id":1,"from":"a@h","to":"b@h","body":"x","ts":1},{"id":2,"from":"a@h","to":"b@h","body":"y","ts":2,"project":"alpha"}]}"#.utf8))
        guard case .log(let ms) = f else { Issue.record("not log"); return }
        #expect(ms.map(\.projectName) == ["default", "alpha"])
    }

    @Test func sameAddressInTwoProjectsIsTwoAgents() {
        let a = Agent(address: "codex@box1", project: "alpha"), b = Agent(address: "codex@box1", project: nil)
        #expect(a.id != b.id)
        #expect(b.id == "default/codex@box1")
    }

    @Test func helloAsksForEveryProject() throws {
        let o = try JSONSerialization.jsonObject(with: Wire.hello(address: "tray@m", token: nil)) as? [String: Any]
        #expect(o?["project"] as? String == "*")
    }

    @Test func projectNamesAreSortedAndUnique() {
        let agents = [Agent(address: "a@h", project: "beta"), Agent(address: "b@h")]
        #expect(projectNames(agents: agents, messages: []) == ["beta", "default"])
    }
}
