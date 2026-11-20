import Foundation

public enum ConnectionState: Sendable, Equatable {
    case connecting, connected, reconnecting, badToken
    case error(String)

    public var text: String {
        switch self {
        case .connecting: return "Connecting"
        case .connected: return "Connected"
        case .reconnecting: return "Reconnecting"
        case .badToken: return "Bad token"
        case .error(let m): return "Error: \(m)"
        }
    }

}

public enum FeedEvent: Sendable {
    case state(ConnectionState)
    case agents([Agent])
    case messages([Message])
}

/// The one seam between the UI and the broker.
public protocol FeedSource: Sendable {
    func start(onEvent: @escaping @Sendable (FeedEvent) -> Void)
    func stop()
}

public func ageText(lastSeenMs: Int64, now: Date = Date()) -> String {
    let s = max(0, Int(now.timeIntervalSince1970) - Int(lastSeenMs / 1000))
    switch s {
    case ..<60: return "\(s)s ago"
    case ..<3600: return "\(s / 60)m ago"
    case ..<86400: return "\(s / 3600)h ago"
    default: return "\(s / 86400)d ago"
    }
}
