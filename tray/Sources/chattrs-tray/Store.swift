import Foundation
import TrayCore

@MainActor
final class AppStore: ObservableObject {
    @Published var state: ConnectionState = .connecting
    @Published var agents: [Agent] = []
    @Published var messages: [Message] = []  // newest first, capped
    @Published var server = Settings.server

    private var source: (any FeedSource)?
    private var epoch = 0
    private let cap = 500

    /// (Re)start the source from current settings.
    func start() {
        source?.stop()
        epoch += 1
        // Ids are per broker: never mix one broker's feed into another's.
        agents = []
        messages = []
        server = Settings.server
        guard let config = Settings.config() else {
            state = .error("bad host:port")
            return
        }
        state = .connecting
        let mine = epoch
        let src = PollingSource(config: config)
        source = src
        src.start { [weak self] event in
            DispatchQueue.main.async {
                MainActor.assumeIsolated {
                    guard let self, self.epoch == mine else { return }
                    self.apply(event)
                }
            }
        }
    }

    /// Handles one control-socket request. See README "Control socket".
    func handleControl(_ line: Data) -> Data {
        switch Control.parse(line) {
        case .failure(let e):
            return Control.reply(["ok": false, "error": e.message])
        case .success(.status):
            return Control.reply(["ok": true, "server": server, "state": state.text,
                                  "agents": agents.count, "messages": messages.count,
                                  "token_set": Settings.config()?.token.map { !$0.isEmpty } ?? false])
        case .success(.connect(let s, let token)):
            guard Settings.parse(s) != nil else { return Control.reply(["ok": false, "error": "use host:port"]) }
            Settings.save(server: s.trimmingCharacters(in: .whitespaces), token: token ?? Settings.storedToken)
            start()
            return Control.reply(["ok": true, "server": server])
        }
    }

    private func apply(_ event: FeedEvent) {
        switch event {
        case .state(let s): state = s
        case .agents(let a): agents = a
        case .messages(let new):
            var byID = Dictionary(uniqueKeysWithValues: messages.map { ($0.id, $0) })
            for m in new { byID[m.id] = m }
            messages = Array(byID.values.sorted { $0.id > $1.id }.prefix(cap))
        }
    }
}
