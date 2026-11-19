import Foundation
import Network

public struct BrokerConfig: Sendable, Equatable {
    public var host: String
    public var port: UInt16
    public var token: String?
    public var address: String

    public init(host: String, port: UInt16, token: String?, address: String) {
        self.host = host; self.port = port; self.token = token; self.address = address
    }
}

/// Connects, says hello, then polls `log` (1.5 s) and `agents` (7 s) on one
/// connection. Requests go out one at a time; replies match by frame type.
// @unchecked Sendable: every mutable property is read and written only on `queue`,
// which is also the queue NWConnection calls back on.
public final class PollingSource: FeedSource, @unchecked Sendable {
    private enum Kind { case log, agents }

    private let config: BrokerConfig
    private let queue = DispatchQueue(label: "chattrs.tray.source")

    private var onEvent: (@Sendable (FeedEvent) -> Void)?
    private var conn: NWConnection?
    private var generation = 0
    private var stopped = false
    private var welcomed = false
    private var attempt = 0
    private var buffer = LineBuffer()
    private var lastID: Int64 = 0
    private var logPolls = 0
    private var pending: [Kind] = []
    private var inFlight: Kind?
    private var sentAt = Date()
    private var timers: [DispatchSourceTimer] = []

    public init(config: BrokerConfig) { self.config = config }

    public func start(onEvent: @escaping @Sendable (FeedEvent) -> Void) {
        queue.async {
            self.onEvent = onEvent
            self.stopped = false
            self.connect()
        }
    }

    public func stop() {
        queue.async {
            self.stopped = true
            self.closeConnection()
        }
    }

    // MARK: connection

    private func connect() {
        guard !stopped, let port = NWEndpoint.Port(rawValue: config.port) else { return }
        generation += 1
        let gen = generation
        onEvent?(.state(attempt == 0 ? .connecting : .reconnecting))
        let c = NWConnection(host: NWEndpoint.Host(config.host), port: port, using: .tcp)
        conn = c
        c.stateUpdateHandler = { [weak self] st in
            guard let self, gen == self.generation else { return }
            switch st {
            case .ready:
                self.send(Wire.hello(address: self.config.address, token: self.config.token))
                self.receive(gen)
            case .failed(let e), .waiting(let e): self.drop("\(e)")
            default: break
            }
        }
        c.start(queue: queue)
    }

    private func closeConnection() {
        generation += 1
        timers.forEach { $0.cancel() }
        timers = []
        conn?.cancel()
        conn = nil
        buffer = LineBuffer()
        pending = []
        inFlight = nil
        welcomed = false
    }

    private func drop(_ reason: String) {
        FileHandle.standardError.write(Data("chattrs-tray: connection dropped: \(reason)\n".utf8))
        closeConnection()
        guard !stopped else { return }
        attempt += 1
        let delay = min(30.0, pow(2.0, Double(attempt - 1)))
        onEvent?(.state(.reconnecting))
        queue.asyncAfter(deadline: .now() + delay) { self.connect() }
    }

    private func receive(_ gen: Int) {
        conn?.receive(minimumIncompleteLength: 1, maximumLength: 65536) { [weak self] data, _, done, err in
            guard let self, gen == self.generation else { return }
            if let data {
                let (lines, overflow) = self.buffer.append(data)
                for line in lines {
                    guard gen == self.generation else { return }
                    self.handle(line)
                }
                if overflow, gen == self.generation { self.drop("line too long"); return }
            }
            guard gen == self.generation else { return }
            if let err { self.drop("\(err)"); return }
            if done { self.drop("closed by broker"); return }
            self.receive(gen)
        }
    }

    private func send(_ data: Data) {
        let gen = generation
        conn?.send(content: data, completion: .contentProcessed { [weak self] err in
            guard let self, gen == self.generation, let err else { return }
            self.drop("\(err)")
        })
    }

    // MARK: frames

    private func handle(_ line: Data) {
        let frame: Frame
        do { frame = try Wire.decode(line) } catch {
            FileHandle.standardError.write(Data("chattrs-tray: bad frame: \(error)\n".utf8))
            return
        }
        switch frame {
        case .welcome:
            guard !welcomed else { return }
            welcomed = true
            attempt = 0
            onEvent?(.state(.connected))
            startTimers()
            enqueue(.log)
            enqueue(.agents)
        case .agents(let list):
            guard inFlight == .agents else { return }
            inFlight = nil
            onEvent?(.agents(list))
            pump()
        case .log(let msgs):
            guard inFlight == .log else { return }
            inFlight = nil
            if let top = msgs.map(\.id).max() { lastID = max(lastID, top) }
            if !msgs.isEmpty { onEvent?(.messages(msgs)) }
            if msgs.count >= 500 { pending.insert(.log, at: 0) }
            pump()
        case .error(let code, let message):
            if code == "bad_token" {
                stopped = true
                closeConnection()
                onEvent?(.state(.badToken))
            } else if !welcomed {
                drop("\(code): \(message)")
                onEvent?(.state(.error(message.isEmpty ? code : message)))
            } else {
                FileHandle.standardError.write(Data("chattrs-tray: broker error \(code): \(message)\n".utf8))
                inFlight = nil
                pump()
            }
        case .unknown(let type):
            FileHandle.standardError.write(Data("chattrs-tray: skipping unknown frame type \(type)\n".utf8))
        }
    }

    // MARK: polling

    private func startTimers() {
        func make(_ every: Double, _ kind: Kind) -> DispatchSourceTimer {
            let t = DispatchSource.makeTimerSource(queue: queue)
            t.schedule(deadline: .now() + every, repeating: every)
            t.setEventHandler { [weak self] in self?.tick(kind) }
            t.resume()
            return t
        }
        timers = [make(1.5, .log), make(7, .agents)]
    }

    private func tick(_ kind: Kind) {
        if inFlight != nil, Date().timeIntervalSince(sentAt) > 10 { drop("request timed out"); return }
        enqueue(kind)
    }

    private func enqueue(_ kind: Kind) {
        if inFlight != kind, !pending.contains(kind) { pending.append(kind) }
        pump()
    }

    private func pump() {
        guard welcomed, inFlight == nil, !pending.isEmpty else { return }
        let kind = pending.removeFirst()
        inFlight = kind
        sentAt = Date()
        guard kind == .log else { send(Wire.agentsRequest()); return }
        // Every 10th poll re-reads the newest 500 so acked_at (the unread dot) refreshes.
        logPolls += 1
        let after = logPolls % 10 == 0 ? max(0, lastID - 500) : lastID
        send(Wire.logRequest(after: after))
    }
}
