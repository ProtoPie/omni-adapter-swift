import Foundation

/// Options for `MemoryStage.join(_:)` — the declaration options of `ConnectOptions` without credentials and networking.
public struct MemoryJoinOptions: Sendable {
    public var displayName: String?
    public var nodeType: String?
    public var providesView: String?
    public var receives: [String]?
    public var sends: [String]?
    public var strictRouting: Bool
    public var exclusiveName: Bool?
    public var osHostname: String?
    public var queueLimit: Int
    public var onLog: (@Sendable (LogEntry) -> Void)?

    public init(
        displayName: String? = nil,
        nodeType: String? = nil,
        providesView: String? = nil,
        receives: [String]? = nil,
        sends: [String]? = nil,
        strictRouting: Bool = false,
        exclusiveName: Bool? = nil,
        osHostname: String? = nil,
        queueLimit: Int = ConnectClient.defaultQueueLimit,
        onLog: (@Sendable (LogEntry) -> Void)? = nil
    ) {
        self.displayName = displayName
        self.nodeType = nodeType
        self.providesView = providesView
        self.receives = receives
        self.sends = sends
        self.strictRouting = strictRouting
        self.exclusiveName = exclusiveName
        self.osHostname = osHostname
        self.queueLimit = queueLimit
        self.onLog = onLog
    }
}

/// One publish the stage accepted — a record of who sent what, for verification.
public struct MemoryPublishRecord: Sendable, Equatable {
    public let from: String
    public let input: PublishInput
    public let ack: Bool
    public let at: Int64
}

/// An in-memory stage — a test harness that attaches several clients inside one process, without a server.
///
/// Reproduces, **in unit tests**, a topology where several programs trigger each other on one Stage: does a value A
/// sends reach B's handler, does strict routing filter it out, does a late-joining C receive the history, which of
/// two instances with the same name is active, does the queue flow after a disconnect and return, does everyone
/// receive `ended` when the session ends. It imitates the server's rules; it is not a server (no credentials, HTTP or doctor).
///
/// Server rules it imitates: no echo to the sender · `messageId = channel` · a `strictRouting` participant gets only
/// the `receives` channels it declared exactly, plus messages targeted at it · a history ring ≤ `historyLimit`
/// (default 1000) as the snapshot on join and on every reconnect · with `exclusiveName`, only the instance **that
/// joined last** per display name is active (the judgment is on the roster only, not in `joined.me`) · roster
/// joins and leaves are notified only to **other** participants.
public final class MemoryStage: @unchecked Sendable {
    public let roomId: String
    /// A reentrant lock — so `rejectPublish`/`now` callbacks that call back into `members()` and the like do not deadlock.
    private let lock = NSRecursiveLock()
    private let historyLimit: Int
    private let rejectPublish: (@Sendable (PublishInput, Member) -> String?)?
    private let now: @Sendable () -> Int64
    private var seats: [String: Seat] = [:]
    private var order: [String] = []
    private var ring: [StageMessage] = []
    private var publishedLog: [MemoryPublishRecord] = []
    private var seq = 0
    private var ended = false
    private let stageId: Int

    /// The in-process stage serial number — mixed into participant ids so two stages in one test never share an id.
    private static var stageSeq = 0
    private static let stageSeqLock = NSLock()

    private static let channelMax = 512
    private static let declarationMax = 32

    /// - Parameters:
    ///   - historyLimit: Size of the history ring — the server default is 1000.
    ///   - rejectPublish: Failure injection — return a code and that publish is rejected with it (queue retry rules
    ///     stay the client's own). nil lets it through.
    ///   - now: The server stamp time — so tests can control the clock.
    public init(
        roomId: String = "memory-stage",
        historyLimit: Int = 1000,
        rejectPublish: (@Sendable (PublishInput, Member) -> String?)? = nil,
        now: (@Sendable () -> Int64)? = nil
    ) {
        self.roomId = roomId
        self.historyLimit = max(0, historyLimit)
        Self.stageSeqLock.lock()
        Self.stageSeq += 1
        self.stageId = Self.stageSeq
        Self.stageSeqLock.unlock()
        self.rejectPublish = rejectPublish
        self.now = now ?? { Int64(Date().timeIntervalSince1970 * 1000) }
    }

    /// Every publish the stage accepted (none that were rejected) — in order.
    public var published: [MemoryPublishRecord] { sync { publishedLog } }

    /// Joins as one participant and returns an **already open** client — the same shape as `connect(_:)`.
    public func join(_ options: MemoryJoinOptions = MemoryJoinOptions()) async throws -> ConnectClient {
        try ConnectClient.validateQueueLimit(options.queueLimit)
        let transport = try self.transport(options)
        let declared = transport.member.receives
        let client = ConnectClient(
            transport: transport,
            queueLimit: options.queueLimit,
            strictReceives: options.strictRouting ? declared : nil,
            exclusiveName: options.exclusiveName == true,
            onLog: options.onLog
        )
        try await client.open()
        return client
    }

    /// The lower layer — for when you want to assemble the client yourself (`ConnectClient(transport:)`).
    /// Options are validated by the same rules as `connect(_:)` (display name 1..64; declared channels 1..512
    /// characters, ≤32, deduplicated; strictRouting requires receives without wildcards).
    public func transport(_ options: MemoryJoinOptions = MemoryJoinOptions()) throws -> MemoryTransport {
        try sync { if ended { throw AdapterError("SessionEnded", "memory stage has ended") } }
        let displayName = options.displayName ?? "Omni adapter"
        guard (1...64).contains(displayName.count) else {
            throw AdapterError("ConfigError", "displayName must be 1..64 chars")
        }
        let receives = try Self.normalizeChannelList("receives", options.receives)
        let sends = try Self.normalizeChannelList("sends", options.sends)
        if options.strictRouting {
            guard let declared = receives else {
                throw AdapterError("ConfigError", "strictRouting requires a receives declaration")
            }
            if let wildcard = declared.first(where: { $0.contains("*") }) {
                throw AdapterError(
                    "ConfigError",
                    "strictRouting matches channels exactly — wildcard patterns are not routable (got \"\(wildcard)\"). List the concrete channel names in receives, or disable strictRouting."
                )
            }
        }
        return try sync {
            if ended { throw AdapterError("SessionEnded", "memory stage has ended") }
            seq += 1
            let id = "mem-\(stageId)-\(seq)"
            let host = options.osHostname?.trimmingCharacters(in: .whitespacesAndNewlines)
            let member = Member(
                participantId: id,
                displayName: displayName,
                providesView: options.providesView,
                standby: nil,
                nodeType: options.nodeType,
                clientType: "adapter",
                receives: receives,
                sends: sends,
                strictRouting: options.strictRouting,
                exclusiveName: options.exclusiveName,
                osHostname: (host?.isEmpty ?? true) ? nil : host
            )
            let seat = Seat(id: id, seq: seq, member: member)
            seats[id] = seat
            order.append(id)
            return MemoryTransport(stage: self, seat: seat)
        }
    }

    /// The roster as the server sees it — participants that left are removed, and the `exclusiveName` judgment (standby) is attached.
    public func members() -> [Member] { sync { membersLocked() } }

    /// The current contents of the history ring (oldest first).
    public func history() -> [StageMessage] { sync { ring } }

    /// Layers created through ``MemoryTransport/ensureStreamLayer()`` (in creation order) — for tests to check.
    public func streamLayers() -> [(layerId: String, name: String)] {
        sync { streamLayerNames.map { (layerId: "memory-layer:\($0)", name: $0) } }
    }

    /// Injects a value as if a non-participant (a prototype, another tool) had sent it — delivers it to everyone
    /// connected by the strict rules and keeps it in the history.
    @discardableResult
    public func inject(_ channel: String, _ value: Primitive, from: String = "prototype", target: String? = nil) throws -> StageMessage {
        try Self.assertChannel(channel)
        let (msg, deliver) = try sync { () throws -> (StageMessage, [(StageMessage) -> Void]) in
            if ended { throw AdapterError("SessionEnded", "memory stage has ended") }
            let m = stampLocked(channel, value, from: from)
            // 살아 있는 참여자의 id 면 그 참여자에게는 에코하지 않는다(서버 무에코와 같다).
            return (m, fanOutLocked(m, from: seats[from], target: target))
        }
        deliver.forEach { $0(msg) }
        return msg
    }

    /// Simulates a network drop — only that participant goes `reconnecting`, and its publishes meanwhile stay in the queue.
    public func disconnect(_ client: ConnectClient) async throws {
        let id = await client.me?.participantId
        let onState: (@Sendable (TransportState) -> Void)? = try sync {
            let seat = try seatOfLocked(id)
            guard seat.connected else { return nil }
            seat.connected = false
            return seat.handlers?.onState
        }
        onState?(.reconnecting)
    }

    /// Simulates recovery — hands the history snapshot again, then returns to `connected` (the same order as a reconnect).
    public func reconnect(_ client: ConnectClient) async throws {
        let id = await client.me?.participantId
        var handlers: TransportHandlers?
        let resumed: Bool = try sync { () throws -> Bool in
            let seat = try seatOfLocked(id)
            if ended { throw AdapterError("SessionEnded", "memory stage has ended") }
            guard !seat.connected, !seat.left else { return false }
            seat.connected = true
            // 스냅샷은 락 안에서 넘긴다 — 자리가 라우팅 가능해지는 순간과 원자적이어야 다른 스레드의 라이브 배달이
            // 스냅샷을 앞지르지 못한다(앞지르면 클라이언트 재시드가 그 라이브 값을 지운다). 실제 서버는 단일
            // 이벤트 루프라 이 경쟁이 없다. 클라이언트 핸들러는 액터로 넘기기만 하므로 락 안에서 불러도 안전하다.
            seat.handlers?.onHistory?(ring)
            handlers = seat.handlers
            return true
        }
        guard resumed else { return }
        handlers?.onState?(.connected)
    }

    /// Ends the session — everyone connected receives `ended`, and later publishes get `SessionEnded`.
    public func end(_ reason: String = "stage session ended") {
        let notify: [TransportHandlers] = sync {
            if ended { return [] }
            ended = true
            return order.compactMap { id -> TransportHandlers? in
                guard let seat = seats[id], !seat.left else { return nil }
                seat.connected = false
                seat.left = true
                return seat.handlers
            }
        }
        for h in notify {
            h.onState?(.ended)
            h.onEnded?(reason)
        }
    }

    /// Broadcasts a history clear — everyone connected receives an empty snapshot (the server's clearHistory).
    public func clearHistory() {
        let notify: [@Sendable ([StageMessage]) -> Void] = sync {
            ring.removeAll()
            return order.compactMap { id in
                guard let seat = seats[id], seat.connected else { return nil }
                return seat.handlers?.onHistory
            }
        }
        notify.forEach { $0([]) }
    }

    // ── 트랜스포트가 부르는 내부 ────────────────────────────────────────

    func open(_ seat: Seat, handlers: TransportHandlers) throws -> Joined {
        let prepared = try sync { () throws -> [@Sendable (Member) -> Void]? in
            if ended { throw AdapterError("SessionEnded", "memory stage has ended") }
            if seat.left { throw AdapterError("NotConnected", "this participant has left the stage") }
            // 같은 트랜스포트로 두 번 열어도 합류는 한 번이다 — 중복 입장 통지·히스토리 이중 시드 방지.
            if seat.connected { return nil }
            seat.handlers = handlers
            seat.connected = true
            let joins = order.compactMap { id -> (@Sendable (Member) -> Void)? in
                guard let other = seats[id], other !== seat, other.connected else { return nil }
                return other.handlers?.onMemberJoin
            }
            // 스냅샷은 락 안에서 — 자리가 라우팅 가능해지는 순간과 원자적이어야 라이브 배달이 앞지르지 않는다(reconnect 참고).
            handlers.onHistory?(ring)
            return joins
        }
        guard let notifyJoin = prepared else { return Joined(roomId: roomId, me: seat.member) }
        notifyJoin.forEach { $0(seat.member) }
        // 서버와 같이 `me` 에는 standby 가 없다 — 판정은 로스터에만.
        return Joined(roomId: roomId, me: seat.member)
    }

    func publish(_ seat: Seat, _ input: PublishInput, ack: Bool) -> PublishResult {
        let outcome: PublishOutcome = sync {
            if ended || seat.left { return .rejected(PublishResult(ok: false, code: "SessionEnded", message: "stage session ended")) }
            if !seat.connected { return .rejected(PublishResult(ok: false, code: "NotConnected", message: "transport is disconnected")) }
            if input.channel.isEmpty || input.channel.count > Self.channelMax {
                return .rejected(PublishResult(ok: false, code: "ValidationError", message: "channel must be 1..\(Self.channelMax) chars"))
            }
            if let rejected = rejectPublish?(input, seat.member), !rejected.isEmpty {
                return .rejected(PublishResult(ok: false, code: rejected, message: "rejected by memory stage (\(rejected))"))
            }
            let m = stampLocked(input.channel, input.value, from: seat.id)
            publishedLog.append(MemoryPublishRecord(from: seat.id, input: input, ack: ack, at: m.timestamp))
            return .accepted(m, fanOutLocked(m, from: seat, target: input.targetParticipantId))
        }
        switch outcome {
        case .rejected(let result):
            return result
        case .accepted(let msg, let deliver):
            deliver.forEach { $0(msg) }
            return PublishResult(ok: true, messageId: msg.messageId)
        }
    }

    /// One memory layer per name (in creation order) — created when the name is ensured for the first time. Changed under `lock`.
    private var streamLayerNames: [String] = []

    /// The server's sentence for rejecting an undeclared providesView — the memory double rejects with the same sentence.
    static let streamUndeclaredMessage =
        "This adapter did not declare providesView: 'stream' when it joined — an Adapter Stream layer is prepared only for adapters that broadcast a screen"

    func ensureStreamLayer(_ seat: Seat) throws -> EnsuredStreamLayer {
        try sync {
            if ended || seat.left { throw AdapterError("SessionEnded", "memory stage has ended") }
            // 서버처럼 화면을 신고한(providesView "stream") 참여자에게만 — 서버와 같은 문장 · 같은 힌트.
            if seat.member.providesView != "stream" {
                throw AdapterError(
                    "ValidationError", MemoryStage.streamUndeclaredMessage, hint: SocketTransport.streamUndeclaredHint)
            }
            // 서버처럼 앞뒤 공백을 걷은 이름으로 찾는다. 레이어 id 는 이름마다 고정이다(`memory-layer:<이름>` — 여섯 언어 같은 모양).
            let name = seat.member.displayName.trimmingCharacters(in: .whitespacesAndNewlines)
            let created = !streamLayerNames.contains(name)
            if created { streamLayerNames.append(name) }
            return EnsuredStreamLayer(layerId: "memory-layer:\(name)", name: name, created: created)
        }
    }

    func listMembers(_ seat: Seat) throws -> [Member] {
        try sync {
            if !seat.connected { throw AdapterError("NotConnected", "transport is disconnected") }
            return membersLocked()
        }
    }

    func close(_ seat: Seat) {
        let notify: [@Sendable (String) -> Void] = sync {
            if seat.left { return [] }
            seat.left = true
            seat.connected = false
            seat.handlers = nil
            return order.compactMap { id in
                guard let other = seats[id], other !== seat, other.connected else { return nil }
                return other.handlers?.onMemberLeave
            }
        }
        notify.forEach { $0(seat.id) }
    }

    private func membersLocked() -> [Member] {
        let live = order.compactMap { seats[$0] }.filter { !$0.left }
        var winners: [String: Seat] = [:]
        for s in live where s.member.exclusiveName == true {
            let key = s.member.displayName.trimmingCharacters(in: .whitespacesAndNewlines)
            if let cur = winners[key], cur.seq >= s.seq { continue }
            winners[key] = s
        }
        return live.map { s in
            guard s.member.exclusiveName == true else { return s.member }
            let key = s.member.displayName.trimmingCharacters(in: .whitespacesAndNewlines)
            return Self.withStandby(s.member, winners[key] !== s)
        }
    }

    private func stampLocked(_ channel: String, _ value: Primitive, from: String) -> StageMessage {
        let msg = StageMessage(channel: channel, value: value, sourceParticipantId: from, timestamp: now(), messageId: channel)
        ring.append(msg)
        if ring.count > historyLimit { ring.removeFirst(ring.count - historyLimit) }
        return msg
    }

    private func fanOutLocked(_ msg: StageMessage, from: Seat?, target: String?) -> [(StageMessage) -> Void] {
        order.compactMap { id -> ((StageMessage) -> Void)? in
            guard let seat = seats[id], seat !== from, seat.connected else { return nil } // 에코 없음 · 끊긴 쪽은 스냅샷으로
            if let target = target {
                if seat.id != target { return nil }
            } else if seat.member.strictRouting == true, !(seat.member.receives ?? []).contains(msg.channel) {
                return nil // 선언 기반 선별 배달 — 정확한 이름만
            }
            return seat.handlers?.onMessage
        }
    }

    private func seatOfLocked(_ id: String?) throws -> Seat {
        guard let id = id, let seat = seats[id] else {
            throw AdapterError("ConfigError", "that client did not join this memory stage")
        }
        return seat
    }

    private func sync<T>(_ body: () throws -> T) rethrows -> T {
        lock.lock()
        defer { lock.unlock() }
        return try body()
    }

    private static func withStandby(_ m: Member, _ standby: Bool) -> Member {
        Member(
            participantId: m.participantId, displayName: m.displayName, providesView: m.providesView,
            standby: standby, nodeType: m.nodeType, clientType: m.clientType, receives: m.receives,
            sends: m.sends, strictRouting: m.strictRouting, exclusiveName: m.exclusiveName, osHostname: m.osHostname
        )
    }

    private static func assertChannel(_ channel: String) throws {
        if channel.isEmpty || channel.count > channelMax {
            throw AdapterError("ValidationError", "channel must be 1..\(channelMax) chars")
        }
    }

    private static func normalizeChannelList(_ field: String, _ input: [String]?) throws -> [String]? {
        guard let input = input else { return nil }
        var seen: [String] = []
        for raw in input {
            let name = raw.trimmingCharacters(in: .whitespacesAndNewlines)
            if name.isEmpty || name.count > channelMax {
                throw AdapterError("ConfigError", "\(field) entries must be 1..\(channelMax) chars")
            }
            if !seen.contains(name) { seen.append(name) }
        }
        if seen.count > declarationMax {
            throw AdapterError("ConfigError", "\(field) allows at most \(declarationMax) channels")
        }
        return seen.isEmpty ? nil : seen
    }

    private enum PublishOutcome {
        case rejected(PublishResult)
        case accepted(StageMessage, [(StageMessage) -> Void])
    }

    final class Seat: @unchecked Sendable {
        let id: String
        let seq: Int
        let member: Member
        var handlers: TransportHandlers?
        var connected = false
        var left = false
        init(id: String, seq: Int, member: Member) {
            self.id = id
            self.seq = seq
            self.member = member
        }
    }
}

/// A transport on top of `MemoryStage` — usually created by `MemoryStage.join(_:)`.
public final class MemoryTransport: Transport, @unchecked Sendable {
    // (E6)
    /// The test double has no server publish limit — it skips the token bucket.
    public var bypassRateLimit: Bool { true }

    private let stage: MemoryStage
    private let seat: MemoryStage.Seat

    init(stage: MemoryStage, seat: MemoryStage.Seat) {
        self.stage = stage
        self.seat = seat
    }

    /// The roster entry exactly as this seat declared it (no judgment).
    public var member: Member { seat.member }

    public func open(handlers: TransportHandlers) async throws -> Joined { try stage.open(seat, handlers: handlers) }
    public func publish(_ input: PublishInput, ack: Bool) async -> PublishResult { stage.publish(seat, input, ack: ack) }
    public func listMembers() async throws -> [Member] { try stage.listMembers(seat) }
    /// Ensures a memory layer — one fixed fake layer per name (`memory-layer:<name>`). created the first time; calling
    /// again returns the same layer (idempotent). This double never renames, so renamed is always false.
    public func ensureStreamLayer() async throws -> EnsuredStreamLayer { try stage.ensureStreamLayer(seat) }
    public func close() async { stage.close(seat) }
}

extension MemoryTransport: DeclaresProvidesView {
    /// The providesView from the join options — the value it asked to declare.
    var declaredProvidesView: String? { seat.member.providesView }
}
