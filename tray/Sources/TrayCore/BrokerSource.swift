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

/// One observer connection to the broker. It starts `watch` all, catches up
/// from `log`, asks for `agents` every 7 s, and re-reads the newest 500
/// messages every 15 s so `acked_at` (the unread dot) refreshes. It never
/// sends a write. A broker that does not echo `observer` is too old: the
/// source stops rather than connect as a normal agent.
// @unchecked Sendable: every mutable property is read and written only on `queue`,
// which is also the queue NWConnection calls back on.
public final class BrokerSource: FeedSource, @unchecked Sendable {
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
    /// Every message up to this id is held. Advances from forward `log` reads,
    /// and from live messages once the catch-up is done.
    private var lastID: Int64 = 0
    private var liveTop: Int64 = 0
    private var catchingUp = false
    private var refreshing = false
    private var paging = false
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
        catchingUp = false
        paging = false
    }

    /// The broker also drops a lagging observer. Reconnect, then catch up from `log`.
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
        case .welcome(let observer):
            guard !welcomed else { return }
            guard observer else {
                stopped = true
                closeConnection()
                onEvent?(.state(.error("broker too old: observer unsupported")))
                return
            }
            welcomed = true
            attempt = 0
            onEvent?(.state(.connected))
            // Catch-up order: start watch and keep what streams, then read log after lastID.
            send(Wire.watchAllRequest())
            catchingUp = true
            liveTop = lastID
            startTimers()
            enqueue(.log)
            enqueue(.agents)
        case .message(let m):
            guard welcomed else { return }
            liveTop = max(liveTop, m.id)
            if !catchingUp { lastID = max(lastID, m.id) }
            onEvent?(.messages([m]))
        case .agents(let list):
            guard inFlight == .agents else { return }
            inFlight = nil
            onEvent?(.agents(list))
            pump()
        case .log(let msgs):
            guard inFlight == .log else { return }
            inFlight = nil
            if !msgs.isEmpty { onEvent?(.messages(msgs)) }
            if !refreshing {
                if let top = msgs.map(\.id).max() { lastID = max(lastID, top) }
                if msgs.count >= 500 {
                    pending.insert(.log, at: 0)
                    paging = true
                } else if catchingUp {
                    catchingUp = false
                    lastID = max(lastID, liveTop)
                }
            }
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

    // MARK: requests

    private func startTimers() {
        func make(_ every: Double, _ kind: Kind) -> DispatchSourceTimer {
            let t = DispatchSource.makeTimerSource(queue: queue)
            t.schedule(deadline: .now() + every, repeating: every)
            t.setEventHandler { [weak self] in self?.tick(kind) }
            t.resume()
            return t
        }
        // The watch brings new messages; log is only the unread refresh.
        timers = [make(15, .log), make(7, .agents)]
    }

    private func tick(_ kind: Kind) {
        if inFlight != nil, Date().timeIntervalSince(sentAt) > 10 { drop("request timed out"); return }
        if kind == .log, catchingUp { return }
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
        // Catch-up and paging read forward; a timer tick re-reads the newest 500.
        refreshing = !catchingUp && !paging
        paging = false
        send(Wire.logRequest(after: refreshing ? max(0, lastID - 500) : lastID))
    }
}
