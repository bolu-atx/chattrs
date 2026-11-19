import AppKit
import SwiftUI
import TrayCore

struct ContentView: View {
    @EnvironmentObject var store: AppStore
    @State private var showSettings = false

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            header
            Divider()
            if showSettings {
                SettingsView(done: { showSettings = false })
            } else {
                AgentsSection(agents: store.agents)
                Divider()
                FeedList(messages: store.messages)
            }
            Divider()
            footer
        }
        .frame(width: 380, height: 520)
    }

    private var header: some View {
        HStack {
            Text(store.server).font(.headline)
            Spacer()
            Text(store.state.text).font(.caption).foregroundStyle(.secondary).lineLimit(1)
        }
        .padding(10)
    }

    private var footer: some View {
        HStack {
            Button { showSettings.toggle() } label: { Image(systemName: "gearshape") }
            Spacer()
            Button("Quit") { NSApplication.shared.terminate(nil) }
        }
        .buttonStyle(.borderless)
        .padding(10)
    }
}

struct AgentsSection: View {
    let agents: [Agent]

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text("Agents").font(.caption).foregroundStyle(.secondary)
            if agents.isEmpty {
                Text("None yet").font(.caption).foregroundStyle(.secondary)
            } else {
                ScrollView {
                    TimelineView(.periodic(from: .now, by: 5)) { ctx in
                        VStack(alignment: .leading, spacing: 2) {
                            ForEach(agents) { a in
                                HStack {
                                    Text(a.address).font(.system(.caption, design: .monospaced))
                                    Spacer()
                                    Text(a.lastSeen.map { ageText(lastSeenMs: $0, now: ctx.date) } ?? "?")
                                        .font(.caption).foregroundStyle(.secondary)
                                }
                            }
                        }
                    }
                }
                .frame(maxHeight: 90)
            }
        }
        .padding(10)
    }
}

struct FeedList: View {
    let messages: [Message]
    @State private var expanded: Set<Int64> = []

    private static let clock: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "HH:mm:ss"
        return f
    }()

    var body: some View {
        if messages.isEmpty {
            Text("No messages").foregroundStyle(.secondary).frame(maxWidth: .infinity, maxHeight: .infinity)
        } else {
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 0) {
                    ForEach(messages) { m in
                        row(m)
                        Divider()
                    }
                }
            }
        }
    }

    private func row(_ m: Message) -> some View {
        let open = expanded.contains(m.id)
        return VStack(alignment: .leading, spacing: 3) {
            HStack(spacing: 6) {
                Text(Self.clock.string(from: Date(timeIntervalSince1970: Double(m.ts) / 1000)))
                    .font(.system(.caption, design: .monospaced)).foregroundStyle(.secondary)
                Text(m.route).font(.caption).bold().lineLimit(1)
                Spacer()
                if m.ackedAt == nil { Circle().fill(.blue).frame(width: 8, height: 8) }
            }
            if open {
                Text(m.body).font(.callout).textSelection(.enabled)
                if let r = m.replyTo {
                    Text("reply to #\(r)").font(.caption).foregroundStyle(.secondary)
                }
            } else {
                Text(m.body.replacingOccurrences(of: "\n", with: " ")).font(.callout).lineLimit(1)
            }
        }
        .padding(.horizontal, 10).padding(.vertical, 6)
        .frame(maxWidth: .infinity, alignment: .leading)
        .contentShape(Rectangle())
        .onTapGesture { if open { expanded.remove(m.id) } else { expanded.insert(m.id) } }
    }
}

struct SettingsView: View {
    @EnvironmentObject var store: AppStore
    let done: () -> Void
    @State private var host = Settings.server
    @State private var token = Settings.storedToken
    @State private var invalid = false

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Broker (host:port)").font(.caption)
            TextField("127.0.0.1:4646", text: $host).textFieldStyle(.roundedBorder)
            Text("Token").font(.caption)
            SecureField("none", text: $token).textFieldStyle(.roundedBorder)
            if Settings.envTokenSet {
                Text("CHATTRS_TOKEN is set and overrides this token.").font(.caption).foregroundStyle(.secondary)
            }
            if invalid { Text("Use host:port.").font(.caption).foregroundStyle(.red) }
            Button("Save") {
                guard Settings.parse(host) != nil else { invalid = true; return }
                Settings.save(server: host.trimmingCharacters(in: .whitespaces), token: token)
                store.start()
                done()
            }
            Spacer()
        }
        .padding(10)
    }
}
