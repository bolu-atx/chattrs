import Foundation
import TrayCore

@MainActor
final class AppStore: ObservableObject {
    @Published var state: ConnectionState = .connecting
    @Published var agents: [Agent] = []
    @Published var messages: [Message] = []  // newest first, capped
    @Published var server = Settings.server
    /// Flips on each new message so the alien takes a step.
    @Published var step = false
    /// Whether the last config read found a token. Cached so nothing reads the Keychain on main.
    @Published var tokenSet = false

    private var source: (any FeedSource)?
    private var epoch = 0
    private let cap = 500

    /// (Re)start the source from current settings. A given `token` is used as is,
    /// so a new token connects at once even while a Keychain prompt is open.
    func start(token: String? = nil) {
        source?.stop()
        epoch += 1
        // Ids are per broker: never mix one broker's feed into another's.
        agents = []
        messages = []
        server = Settings.server
        guard Settings.parse(server) != nil else {
            state = .error("bad host:port")
            return
        }
        state = .connecting
        let mine = epoch
        if let token, !token.isEmpty, !Settings.envTokenSet, let hp = Settings.parse(server) {
            connect(BrokerConfig(host: hp.host, port: hp.port, token: token, address: Settings.address()), epoch: mine)
            return
        }
        Settings.keychainQueue.async {
            let config = Settings.config()
            DispatchQueue.main.async {
                MainActor.assumeIsolated {
                    guard self.epoch == mine, let config else { return }
                    self.connect(config, epoch: mine)
                }
            }
        }
    }

    /// Saves settings and reconnects. `token` nil keeps the stored token; "" clears it.
    func save(server: String, token: String?) {
        Settings.saveServer(server.trimmingCharacters(in: .whitespaces))
        if let token { Settings.keychainQueue.async { Keychain.set(token) } }
        start(token: token)
    }

    private func connect(_ config: BrokerConfig, epoch mine: Int) {
        tokenSet = !(config.token ?? "").isEmpty
        let src = BrokerSource(config: config)
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

    private func assignColors() {
        var all = Set(agents.map(\.address))
        for m in messages { all.insert(m.from); if let to = m.to { all.insert(to) } }
        AgentPalette.assign(all)
    }

    /// Handles one control-socket request. See README "Control socket".
    func handleControl(_ line: Data) -> Data {
        switch Control.parse(line) {
        case .failure(let e):
            return Control.reply(["ok": false, "error": e.message])
        case .success(.status):
            return Control.reply(["ok": true, "server": server, "state": state.text,
                                  "agents": agents.count, "messages": messages.count,
                                  "token_set": tokenSet])
        case .success(.connect(let s, let token)):
            guard Settings.parse(s) != nil else { return Control.reply(["ok": false, "error": "use host:port"]) }
            save(server: s, token: token)
            return Control.reply(["ok": true, "server": server])
        }
    }

    private func apply(_ event: FeedEvent) {
        defer { assignColors() }
        switch event {
        case .state(let s): state = s
        case .agents(let a): agents = a
        case .messages(let new):
            if let top = new.map(\.id).max(), top > (messages.first?.id ?? 0) { step.toggle() }
            var byID = Dictionary(uniqueKeysWithValues: messages.map { ($0.id, $0) })
            for m in new { byID[m.id] = m }
            messages = Array(byID.values.sorted { $0.id > $1.id }.prefix(cap))
        }
    }
}
