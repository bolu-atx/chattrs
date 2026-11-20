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
    /// Empty shows every project.
    @State private var selected: Set<String> = []

    var body: some View {
        let projects = projectNames(agents: store.agents, messages: store.messages)
        let shown = { (p: String) in selected.isEmpty || selected.contains(p) }
        let agents = store.agents.filter { shown($0.projectName) }
        let messages = store.messages.filter { shown($0.projectName) }
        let showProjects = projects.count > 1 && selected.count != 1
        VStack(spacing: 0) {
            Header(showSettings: $showSettings)
            if projects.count > 1 && !showSettings {
                ProjectPills(projects: projects, selection: $selected)
            }
            Divider()
            if showSettings {
                SettingsView(done: { showSettings = false })
            } else {
                AgentsSection(agents: agents, grouped: showProjects)
                Divider()
                ActivityStrip(messages: messages, playhead: $playhead)
                Divider()
                FeedList(messages: messages, playhead: playhead, showProject: showProjects)
            }
            Divider()
            footer
        }
        .frame(width: 400, height: 690)
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

/// Shown once the broker has more than one project. Native toggle buttons
/// (HIG: a multi-select filter). No selection means every project.
struct ProjectPills: View {
    let projects: [String]
    @Binding var selection: Set<String>

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 6) {
                pill("All projects", isOn: Binding(
                    get: { selection.isEmpty },
                    set: { if $0 { selection.removeAll() } }))
                ForEach(projects, id: \.self) { name in
                    pill(name, isOn: Binding(
                        get: { selection.contains(name) },
                        set: { on in if on { selection.insert(name) } else { selection.remove(name) } }))
                }
            }
            .padding(.horizontal, Theme.pad)
        }
        .padding(.bottom, 10)
        .onChange(of: projects) { list in selection.formIntersection(list) }
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Projects")
    }

    @ViewBuilder private func pill(_ title: String, isOn: Binding<Bool>) -> some View {
        let t = Toggle(title, isOn: isOn).toggleStyle(.button).controlSize(.small)
        if #available(macOS 14, *) { t.buttonBorderShape(.capsule) } else { t }
    }
}

struct AgentsSection: View {
    let agents: [Agent]
    var grouped = false
    /// Remembered per viewer. On by default: gone agents are offline for over an hour.
    @AppStorage("hideGone") private var hideGone = true

    var body: some View {
        let goneCount = agents.filter { $0.health() == .gone }.count
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text("Agents").font(.subheadline.weight(.semibold)).foregroundStyle(.secondary)
                Spacer()
                if goneCount > 0 {
                    Toggle("Hide \(goneCount) gone", isOn: $hideGone)
                        .toggleStyle(.checkbox)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .help("Agents offline for over an hour")
                }
            }
            if agents.isEmpty {
                Text("Agents show here once they connect to the broker.")
                    .font(.callout).foregroundStyle(.secondary)
            } else {
                ScrollView {
                    TimelineView(.periodic(from: .now, by: 5)) { ctx in
                        let agents = hideGone ? agents.filter { $0.health(now: ctx.date) != .gone } : agents
                        VStack(alignment: .leading, spacing: 6) {
                            if agents.isEmpty {
                                Text("Every agent is gone. Clear Hide gone to see them.")
                                    .font(.callout).foregroundStyle(.secondary)
                            }
                            if grouped {
                                ForEach(Dictionary(grouping: agents, by: \.projectName).sorted { $0.key < $1.key }, id: \.key) { name, list in
                                    Text(name).font(.caption.weight(.semibold)).foregroundStyle(.tertiary)
                                        .padding(.top, 2)
                                    ForEach(sorted(list, now: ctx.date)) { AgentRow(agent: $0, now: ctx.date) }
                                }
                            } else {
                                ForEach(sorted(agents, now: ctx.date)) { AgentRow(agent: $0, now: ctx.date) }
                            }
                        }
                    }
                }
                .frame(maxHeight: 160)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(Theme.pad)
    }

    /// Online agents first, gone agents last, then by address.
    private func sorted(_ list: [Agent], now: Date) -> [Agent] {
        func rank(_ a: Agent) -> Int {
            a.status(now: now) != .offline ? 0 : (a.health(now: now) == .gone ? 2 : 1)
        }
        return list.sorted { rank($0) != rank($1) ? rank($0) < rank($1) : $0.address < $1.address }
    }
}

struct AgentRow: View {
    let agent: Agent
    let now: Date

    var body: some View {
        let st = agent.status(now: now)
        let health = agent.health(now: now)
        HStack(spacing: 10) {
            StatusPixel(status: st, stalled: health == .stalled)
            Text(agent.address)
                .font(.system(.callout, design: .monospaced))
                .foregroundStyle(AgentPalette.color(agent.address)
                    .opacity(health == .gone ? 0.3 : st == .offline ? 0.55 : 1))
                .lineLimit(1).truncationMode(.middle)
            Spacer()
            Text(label(st, health)).font(.callout)
                .foregroundStyle(health == .stalled && st != .offline ? Theme.red : .secondary)
        }
        .accessibilityElement(children: .combine)
    }

    private func label(_ st: AgentStatus, _ health: AgentHealth) -> String {
        let seen = agent.lastSeen.map { "seen " + ageText(lastSeenMs: $0, now: now) }
        if health == .gone { return seen.map { "gone, " + $0 } ?? "gone" }
        if health == .stalled && st != .offline { return "stalled" }
        switch st {
        case .offline: return seen ?? "offline"
        case .unknown: return "online"
        default: return st.rawValue
        }
    }
}

/// MON-4: green working, amber waiting, grey idle, hollow unknown, nothing when offline.
/// MON-5: red when an online agent is stalled.
struct StatusPixel: View {
    let status: AgentStatus
    var stalled = false

    var body: some View {
        Group {
            if stalled && status != .offline {
                Pixel(color: Theme.red)
            } else {
                stateDot
            }
        }
        .help(stalled && status != .offline ? "stalled" : status.rawValue)
    }

    @ViewBuilder private var stateDot: some View {
            switch status {
            case .working: Pixel(color: Theme.green)
            case .waiting: Pixel(color: Theme.amber)
            case .idle: Pixel(color: Theme.grey)
            case .unknown: Pixel(color: nil)
            case .offline: Color.clear.frame(width: 7, height: 7)
            }
    }
}

/// The sender's token is a lead token (`from_role`).
struct LeadBadge: View {
    var body: some View {
        Text("lead")
            .font(.caption2.weight(.semibold))
            .foregroundStyle(Theme.amber)
            .padding(.horizontal, 4).padding(.vertical, 1)
            .overlay(RoundedRectangle(cornerRadius: 3).strokeBorder(Theme.amber.opacity(0.7), lineWidth: 1))
            .help("Sent with a lead token")
    }
}

struct FeedList: View {
    let messages: [Message]
    let playhead: Int64?
    var showProject = false
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
                HStack(spacing: 5) {
                    Text(m.from).fontWeight(.semibold).foregroundColor(AgentPalette.color(m.from))
                        .layoutPriority(1)
                    if m.fromRole == "lead" { LeadBadge() }
                    Text("→").foregroundColor(.secondary)
                    recipient(m)
                }
                .font(.callout).lineLimit(1).truncationMode(.middle)
                Spacer(minLength: 8)
                if showProject {
                    Text(m.projectName).font(.caption).foregroundStyle(.tertiary).lineLimit(1)
                }
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

    /// The recipient in its agent colour; a topic in amber.
    private func recipient(_ m: Message) -> Text {
        m.to.map { Text($0).foregroundColor(AgentPalette.color($0)) }
            ?? Text("#\(m.topic ?? "?")").foregroundColor(Theme.amber)
    }
}

struct SettingsView: View {
    @EnvironmentObject var store: AppStore
    let done: () -> Void
    @State private var host = Settings.server
    @State private var token = ""
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
                SecureField(store.tokenSet ? "Saved token (leave empty to keep it)" : "No token", text: $token)
                    .textFieldStyle(.roundedBorder)
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
                    store.save(server: host, token: token.isEmpty ? nil : token)
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
