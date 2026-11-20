import AppKit
import SwiftUI
import TrayCore

enum Theme {
    static let amber = Color(red: 0.969, green: 0.663, blue: 0.294)  // #F7A94B
    static let green = Color(red: 0.298, green: 0.765, blue: 0.541)  // #4CC38A
    static let grey = Color(red: 0.541, green: 0.561, blue: 0.596)   // #8A8F98
    static let red = Color(red: 0.898, green: 0.282, blue: 0.302)    // #E5484D
    static let pad: CGFloat = 14
}

/// A square "pixel", the motif shared with the alien icon.
struct Pixel: View {
    var color: Color?
    var size: CGFloat = 7

    var body: some View {
        Group {
            if let color { Rectangle().fill(color) } else { Rectangle().strokeBorder(Theme.grey, lineWidth: 1.2) }
        }
        .frame(width: size, height: size)
    }
}

struct ContentView: View {
    @EnvironmentObject var store: AppStore
    @State private var showSettings = false
    @State private var playhead: Int64?

    var body: some View {
        VStack(spacing: 0) {
            Header(showSettings: $showSettings)
            Divider()
            if showSettings {
                SettingsView(done: { showSettings = false })
            } else {
                AgentsSection(agents: store.agents)
                Divider()
                ActivityStrip(messages: store.messages, playhead: $playhead)
                Divider()
                FeedList(messages: store.messages, playhead: playhead)
            }
            Divider()
            footer
        }
        .frame(width: 400, height: 640)
    }

    private var footer: some View {
        HStack {
            Button(showSettings ? "Close settings" : "Settings") { showSettings.toggle() }
            Spacer()
            Button("Quit") { NSApplication.shared.terminate(nil) }
        }
        .buttonStyle(.borderless)
        .font(.callout)
        .foregroundStyle(.secondary)
        .padding(.horizontal, Theme.pad).padding(.vertical, 10)
    }
}

struct Header: View {
    @EnvironmentObject var store: AppStore
    @Binding var showSettings: Bool

    private var tint: Color {
        switch store.state {
        case .connected: return Theme.amber
        case .connecting, .reconnecting: return Theme.amber.opacity(0.45)
        case .badToken, .error: return Theme.red
        }
    }

    private var stateText: String {
        switch store.state {
        case .connected:
            let online = store.agents.filter { $0.online == true }.count
            return "Connected, \(online) of \(store.agents.count) agents online"
        case .connecting: return "Connecting…"
        case .reconnecting: return "Reconnecting…"
        case .badToken: return "The broker rejected the token"
        case .error(let m): return m
        }
    }

    var body: some View {
        HStack(alignment: .center, spacing: 12) {
            Image(nsImage: Icon.draw(Icon.grid(for: store.state, step: store.step), px: 3, color: .white))
                .renderingMode(.template)
                .foregroundStyle(tint)
                .accessibilityHidden(true)
            VStack(alignment: .leading, spacing: 2) {
                Text(store.server).font(.system(.headline, design: .monospaced))
                Text(stateText)
                    .font(.callout)
                    .foregroundStyle(isProblem ? Theme.red : .secondary)
                    .lineLimit(2)
            }
            Spacer(minLength: 0)
            if isProblem && !showSettings {
                Button("Fix") { showSettings = true }.controlSize(.small)
            }
        }
        .padding(Theme.pad)
    }

    private var isProblem: Bool {
        switch store.state {
        case .badToken, .error: return true
        default: return false
        }
    }
}

struct AgentsSection: View {
    let agents: [Agent]

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Agents").font(.subheadline.weight(.semibold)).foregroundStyle(.secondary)
            if agents.isEmpty {
                Text("Agents show here once they connect to the broker.")
                    .font(.callout).foregroundStyle(.secondary)
            } else {
                ScrollView {
                    TimelineView(.periodic(from: .now, by: 5)) { ctx in
                        VStack(alignment: .leading, spacing: 6) {
                            ForEach(sorted(agents, now: ctx.date)) { a in
                                AgentRow(agent: a, now: ctx.date)
                            }
                        }
                    }
                }
                .frame(maxHeight: 110)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(Theme.pad)
    }

    /// Online agents first, then by address.
    private func sorted(_ list: [Agent], now: Date) -> [Agent] {
        list.sorted { a, b in
            let ao = a.status(now: now) != .offline, bo = b.status(now: now) != .offline
            return ao != bo ? ao : a.address < b.address
        }
    }
}

struct AgentRow: View {
    let agent: Agent
    let now: Date

    var body: some View {
        let st = agent.status(now: now)
        HStack(spacing: 10) {
            StatusPixel(status: st)
            Text(agent.address)
                .font(.system(.callout, design: .monospaced))
                .foregroundStyle(AgentPalette.color(agent.address).opacity(st == .offline ? 0.55 : 1))
                .lineLimit(1).truncationMode(.middle)
            Spacer()
            Text(label(st)).font(.callout).foregroundStyle(.secondary)
        }
        .accessibilityElement(children: .combine)
    }

    private func label(_ st: AgentStatus) -> String {
        switch st {
        case .offline: return agent.lastSeen.map { "seen " + ageText(lastSeenMs: $0, now: now) } ?? "offline"
        case .unknown: return "online"
        default: return st.rawValue
        }
    }
}

/// MON-4: green working, amber waiting, grey idle, hollow unknown, nothing when offline.
struct StatusPixel: View {
    let status: AgentStatus

    var body: some View {
        Group {
            switch status {
            case .working: Pixel(color: Theme.green)
            case .waiting: Pixel(color: Theme.amber)
            case .idle: Pixel(color: Theme.grey)
            case .unknown: Pixel(color: nil)
            case .offline: Color.clear.frame(width: 7, height: 7)
            }
        }
        .help(status.rawValue)
    }
}

struct FeedList: View {
    let messages: [Message]
    let playhead: Int64?
    @State private var expanded: Set<Int64> = []

    /// Messages within this many ms of the playhead are highlighted.
    private var window: Int64 {
        guard let first = messages.last?.ts, let last = messages.first?.ts else { return 30_000 }
        return max(30_000, (last - first) / 40)
    }

    private static let clock: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "HH:mm:ss"
        return f
    }()

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            Text("Messages").font(.subheadline.weight(.semibold)).foregroundStyle(.secondary)
                .padding(.horizontal, Theme.pad).padding(.top, Theme.pad).padding(.bottom, 4)
            if messages.isEmpty {
                Text("New messages show here as agents send them.")
                    .font(.callout).foregroundStyle(.secondary)
                    .padding(.horizontal, Theme.pad)
                Spacer()
            } else {
                ScrollViewReader { proxy in
                    ScrollView {
                        LazyVStack(alignment: .leading, spacing: 0) {
                            ForEach(messages) { row($0).id($0.id) }
                        }
                    }
                    .onChange(of: playhead) { p in
                        let target = p.flatMap { messageAt($0, in: messages) } ?? messages.first
                        guard let target else { return }
                        withAnimation(.easeOut(duration: 0.2)) { proxy.scrollTo(target.id, anchor: .top) }
                    }
                }
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private func row(_ m: Message) -> some View {
        let open = expanded.contains(m.id)
        return VStack(alignment: .leading, spacing: 4) {
            HStack(spacing: 6) {
                route(m).font(.callout).lineLimit(1).truncationMode(.middle)
                Spacer(minLength: 8)
                Text(Self.clock.string(from: Date(timeIntervalSince1970: Double(m.ts) / 1000)))
                    .font(.caption.monospacedDigit()).foregroundStyle(.secondary)
                Pixel(color: m.ackedAt == nil ? Theme.amber : .clear, size: 6)
                    .accessibilityLabel(m.ackedAt == nil ? "unread" : "read")
            }
            if open {
                Text(m.body)
                    .font(.callout)
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(8)
                    .background(.quaternary.opacity(0.5), in: RoundedRectangle(cornerRadius: 6))
                if let r = m.replyTo {
                    Text("Reply to message \(r)").font(.caption).foregroundStyle(.secondary)
                }
            } else {
                Text(m.body.replacingOccurrences(of: "\n", with: " "))
                    .font(.callout).foregroundStyle(.secondary).lineLimit(1)
            }
        }
        .padding(.horizontal, Theme.pad).padding(.vertical, 8)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(inWindow(m) ? Theme.amber.opacity(0.12) : .clear)
        .overlay(alignment: .leading) {
            if inWindow(m) { Rectangle().fill(Theme.amber).frame(width: 2) }
        }
        .contentShape(Rectangle())
        .onTapGesture { if open { expanded.remove(m.id) } else { expanded.insert(m.id) } }
    }

    private func inWindow(_ m: Message) -> Bool {
        guard let p = playhead else { return false }
        return abs(m.ts - p) <= window
    }

    /// Sender and recipient in their agent colours; a topic in amber.
    private func route(_ m: Message) -> Text {
        let to = m.to.map { Text($0).foregroundColor(AgentPalette.color($0)) }
            ?? Text("#\(m.topic ?? "?")").foregroundColor(Theme.amber)
        return Text(m.from).fontWeight(.semibold).foregroundColor(AgentPalette.color(m.from))
            + Text("  →  ").foregroundColor(.secondary) + to
    }
}

struct SettingsView: View {
    @EnvironmentObject var store: AppStore
    let done: () -> Void
    @State private var host = Settings.server
    @State private var token = Settings.storedToken
    @State private var invalid = false

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            VStack(alignment: .leading, spacing: 4) {
                Text("Broker address").font(.subheadline.weight(.semibold))
                TextField("100.64.0.7:4646", text: $host)
                    .textFieldStyle(.roundedBorder)
                    .font(.system(.body, design: .monospaced))
                Text(invalid ? "Enter the address as host:port." : "The swarm machine's LAN or tailnet address.")
                    .font(.caption).foregroundStyle(invalid ? Theme.red : .secondary)
            }
            VStack(alignment: .leading, spacing: 4) {
                Text("Token").font(.subheadline.weight(.semibold))
                SecureField("No token", text: $token).textFieldStyle(.roundedBorder)
                Text(Settings.envTokenSet
                     ? "CHATTRS_TOKEN is set and overrides this token."
                     : "The fleet token. It is stored in the Keychain.")
                    .font(.caption).foregroundStyle(.secondary)
            }
            HStack {
                Spacer()
                Button("Cancel", action: done).keyboardShortcut(.cancelAction)
                Button("Save and connect") {
                    guard Settings.parse(host) != nil else { invalid = true; return }
                    Settings.save(server: host.trimmingCharacters(in: .whitespaces), token: token)
                    store.start()
                    done()
                }
                .keyboardShortcut(.defaultAction)
                .tint(Theme.amber)
            }
            Spacer()
        }
        .padding(Theme.pad)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }
}
