import Foundation
import Security

public enum Keychain {
    private static let service = "chattrs-tray"
    private static let account = "token"

    private static var base: [String: Any] {
        [kSecClass as String: kSecClassGenericPassword,
         kSecAttrService as String: service,
         kSecAttrAccount as String: account]
    }

    public static func get() -> String? {
        var q = base
        q[kSecReturnData as String] = true
        q[kSecMatchLimit as String] = kSecMatchLimitOne
        var out: CFTypeRef?
        guard SecItemCopyMatching(q as CFDictionary, &out) == errSecSuccess, let d = out as? Data else { return nil }
        return String(data: d, encoding: .utf8)
    }

    /// An empty value deletes the item.
    public static func set(_ value: String) {
        SecItemDelete(base as CFDictionary)
        guard !value.isEmpty else { return }
        var q = base
        q[kSecValueData as String] = Data(value.utf8)
        SecItemAdd(q as CFDictionary, nil)
    }
}

public enum Settings {
    private static let key = "server"
    private static let env = ProcessInfo.processInfo.environment

    public static var server: String {
        UserDefaults.standard.string(forKey: key) ?? env["CHATTRS_SERVER"] ?? "127.0.0.1:4646"
    }

    public static var envTokenSet: Bool { !(env["CHATTRS_TOKEN"] ?? "").isEmpty }

    /// Every Keychain read and write runs here, never on the main thread: a
    /// Keychain prompt (after a rebuild changes the code signature) must not
    /// block the UI or the control socket. Serial, so a read sees the last write.
    public static let keychainQueue = DispatchQueue(label: "chattrs.tray.keychain")

    public static func saveServer(_ server: String) { UserDefaults.standard.set(server, forKey: key) }

    public static func parse(_ hostPort: String) -> (host: String, port: UInt16)? {
        let s = hostPort.trimmingCharacters(in: .whitespaces)
        guard let i = s.lastIndex(of: ":"), let port = UInt16(s[s.index(after: i)...]), port > 0 else { return nil }
        let host = String(s[..<i])
        return host.isEmpty ? nil : (host, port)
    }

    /// "tray@" + short host name, limited to the characters the broker allows.
    public static func address() -> String {
        let short = ProcessInfo.processInfo.hostName.split(separator: ".").first.map(String.init) ?? ""
        let ok = short.filter { $0.isASCII && ($0.isLetter || $0.isNumber || "._-".contains($0)) }
        return "tray@" + (ok.isEmpty ? "mac" : ok)
    }

    /// Unix socket other tools (such as Claude Code) use to steer the app.
    public static var controlPath: String {
        let dir = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        return dir.appendingPathComponent("chattrs-tray/control.sock").path
    }

    /// Reads the Keychain: call on `keychainQueue`.
    public static func config() -> BrokerConfig? {
        guard let hp = parse(server) else { return nil }
        let envToken = env["CHATTRS_TOKEN"] ?? ""
        let token = envToken.isEmpty ? Keychain.get() : envToken
        return BrokerConfig(host: hp.host, port: hp.port, token: token, address: address())
    }
}
