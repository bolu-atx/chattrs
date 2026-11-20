import Foundation

/// Max line length the broker accepts (128 KiB). We use it as a read cap too.
public let maxLineBytes = 128 * 1024

public struct Agent: Sendable, Equatable, Identifiable, Decodable {
    public let address: String
    public let lastSeen: Int64?
    /// Live inbox watch on the broker. nil from a broker that predates it.
    public let online: Bool?
    public var id: String { address }

    enum CodingKeys: String, CodingKey {
        case address, online
        case lastSeen = "last_seen"
    }
}

public struct Message: Sendable, Equatable, Identifiable, Decodable {
    public let id: Int64
    public let from: String
    public let to: String?
    public let body: String
    public let ts: Int64
    public let replyTo: Int64?
    public let topic: String?
    public let ackedAt: Int64?

    enum CodingKeys: String, CodingKey {
        case id, from, to, body, ts, topic
        case replyTo = "reply_to"
        case ackedAt = "acked_at"
    }

    /// "from → to" for direct messages, "from → #topic" for announcements.
    public var route: String {
        if let to { return "\(from) → \(to)" }
        return "\(from) → #\(topic ?? "?")"
    }
}

public enum Frame: Sendable, Equatable {
    case welcome(observer: Bool)
    case message(Message)
    case agents([Agent])
    case log([Message])
    case error(code: String, message: String)
    case unknown(String)
}

public enum WireError: Error { case notAnObject, missingType }

private struct AgentsBody: Decodable { let agents: [Agent] }
private struct LogBody: Decodable { let messages: [Message] }
private struct WelcomeBody: Decodable { let observer: Bool? }
private struct ErrorBody: Decodable { let code: String; let message: String? }

public enum Wire {
    /// Decode one line. Unknown fields are ignored; unknown types give `.unknown`.
    public static func decode(_ line: Data) throws -> Frame {
        guard let obj = try JSONSerialization.jsonObject(with: line) as? [String: Any] else {
            throw WireError.notAnObject
        }
        guard let type = obj["type"] as? String else { throw WireError.missingType }
        let dec = JSONDecoder()
        switch type {
        case "welcome": return .welcome(observer: try dec.decode(WelcomeBody.self, from: line).observer == true)
        case "message": return .message(try dec.decode(Message.self, from: line))
        case "agents": return .agents(try dec.decode(AgentsBody.self, from: line).agents)
        case "log": return .log(try dec.decode(LogBody.self, from: line).messages)
        case "error":
            let e = try dec.decode(ErrorBody.self, from: line)
            return .error(code: e.code, message: e.message ?? "")
        default: return .unknown(type)
        }
    }

    public static func hello(address: String, token: String?) -> Data {
        // observer: an older broker ignores the field and records the address.
        var f: [String: Any] = ["type": "hello", "v": 1, "address": address, "observer": true]
        if let token, !token.isEmpty { f["token"] = token }
        return encode(f)
    }

    /// Every new message, direct and announcements. Observer connections only.
    public static func watchAllRequest() -> Data { encode(["type": "watch", "all": true]) }

    public static func agentsRequest() -> Data { encode(["type": "agents"]) }

    /// `log` with no address: covers all addresses. Never acks.
    public static func logRequest(after: Int64, limit: Int = 500) -> Data {
        encode(["type": "log", "after": after, "limit": limit])
    }

    private static func encode(_ f: [String: Any]) -> Data {
        // Keys and values are plain strings and numbers, so this cannot fail.
        var d = (try? JSONSerialization.data(withJSONObject: f, options: [.sortedKeys])) ?? Data()
        d.append(0x0A)
        return d
    }
}

/// Accumulates stream bytes and yields complete lines.
public struct LineBuffer: Sendable {
    private var data = Data()
    public init() {}

    /// Returns complete lines without the `\n`. Sets `overflow` when a partial line exceeds the cap.
    public mutating func append(_ chunk: Data) -> (lines: [Data], overflow: Bool) {
        data.append(chunk)
        var lines: [Data] = []
        while let nl = data.firstIndex(of: 0x0A) {
            let line = data[data.startIndex..<nl]
            if !line.isEmpty { lines.append(Data(line)) }
            data = Data(data[data.index(after: nl)...])
        }
        return (lines, data.count > maxLineBytes)
    }
}
