import Foundation

/// A local command the app accepts on its control socket.
public enum ControlCommand: Equatable, Sendable {
    case status
    /// `token` nil keeps the stored token; "" clears it.
    case connect(server: String, token: String?)
}

public enum Control {
    private struct Request: Decodable { let cmd: String; let server: String?; let token: String? }

    public static func parse(_ line: Data) -> Result<ControlCommand, ControlError> {
        guard let r = try? JSONDecoder().decode(Request.self, from: line) else {
            return .failure(ControlError("expected one JSON object with \"cmd\""))
        }
        switch r.cmd {
        case "status": return .success(.status)
        case "connect":
            guard let s = r.server else { return .failure(ControlError("connect needs \"server\"")) }
            return .success(.connect(server: s, token: r.token))
        default: return .failure(ControlError("unknown cmd \(r.cmd)"))
        }
    }

    public static func reply(_ fields: [String: Any]) -> Data {
        (try? JSONSerialization.data(withJSONObject: fields, options: [.sortedKeys])) ?? Data("{}".utf8)
    }
}

public struct ControlError: Error, Equatable { public let message: String; public init(_ m: String) { message = m } }

/// Listens on a Unix socket on a background thread. One request line per
/// connection, one reply line, then close.
public enum ControlServer {
    public static func start(path: String, handle: @escaping @Sendable (Data) -> Data) throws {
        let dir = (path as NSString).deletingLastPathComponent
        try FileManager.default.createDirectory(atPath: dir, withIntermediateDirectories: true)
        chmod(dir, 0o700)
        unlink(path)
        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else { throw ControlError("socket failed") }
        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        let bytes = Array(path.utf8)
        guard bytes.count < MemoryLayout.size(ofValue: addr.sun_path) else {
            close(fd)
            throw ControlError("socket path too long")
        }
        withUnsafeMutableBytes(of: &addr.sun_path) { $0.copyBytes(from: bytes) }
        let len = socklen_t(MemoryLayout<sockaddr_un>.size)
        let bound = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(fd, $0, len) }
        }
        guard bound == 0, chmod(path, 0o600) == 0, listen(fd, 8) == 0 else {
            close(fd)
            throw ControlError("bind/listen on \(path) failed: errno \(errno)")
        }
        Thread.detachNewThread { serve(fd, handle) }
    }

    private static func serve(_ fd: Int32, _ handle: @Sendable (Data) -> Data) {
        while true {
            let c = accept(fd, nil, nil)
            if c < 0 { if errno == EINTR { continue } else { return } }
            var one: Int32 = 1
            setsockopt(c, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))
            var tv = timeval(tv_sec: 5, tv_usec: 0)
            setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &tv, socklen_t(MemoryLayout<timeval>.size))
            var data = Data()
            var buf = [UInt8](repeating: 0, count: 4096)
            while data.count <= 65536, !data.contains(0x0A) {
                let n = read(c, &buf, buf.count)
                if n <= 0 { break }
                data.append(buf, count: n)
            }
            let line = data.prefix { $0 != 0x0A }
            var out = handle(Data(line))
            out.append(0x0A)
            out.withUnsafeBytes { _ = write(c, $0.baseAddress, $0.count) }
            close(c)
        }
    }
}
