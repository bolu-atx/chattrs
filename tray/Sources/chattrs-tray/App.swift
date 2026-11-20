import AppKit
import SwiftUI
import TrayCore

@main
struct TrayApp: App {
    @StateObject private var store: AppStore

    init() {
        NSApplication.shared.setActivationPolicy(.accessory)
        let s = AppStore()
        do {
            try ControlServer.start(path: Settings.controlPath) { line in
                DispatchQueue.main.sync { MainActor.assumeIsolated { s.handleControl(line) } }
            }
        } catch {
            FileHandle.standardError.write(Data("chattrs-tray: control socket: \(error)\n".utf8))
        }
        s.start()
        _store = StateObject(wrappedValue: s)
    }

    var body: some Scene {
        MenuBarExtra {
            ContentView().environmentObject(store)
        } label: {
            Image(nsImage: Icon.menuBar(for: store.state, step: store.step))
        }
        .menuBarExtraStyle(.window)
    }
}
