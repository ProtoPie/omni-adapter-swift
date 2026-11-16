import Foundation

/// Lifecycle events — delivered on the ``ConnectClient/events`` stream.
public enum ClientEvent: Sendable {
    case stateChange(TransportState)
    case failure(AdapterError)
    case memberJoin(Member)
    case memberLeave(String)
    case ended(String?)

    /// The offline queue was full and its oldest item was dropped.
    case queueOverflow(PublishInput)

    /// The history snapshot was (re)seeded — happens on every reconnect.
    case historyReplaced([StageMessage])
}

// 핸들을 버리는 사용법이 다른 언어 어댑터에서 정상이다.
/// Handle that cancels a channel subscription.
///
/// ⚠️ It is **not** cancelled automatically on deinit — discarding the handle, as in `client.on("x") { … }`,
/// is normal usage, and if ARC cancelled it right away the subscription would silently disappear.
/// Always cancel explicitly with ``cancel()``.
public final class Subscription: @unchecked Sendable {
    private let onCancel: @Sendable () -> Void
    private var cancelled = false
    private let lock = NSLock()

    init(onCancel: @escaping @Sendable () -> Void) { self.onCancel = onCancel }

    public func cancel() {
        lock.lock()
        defer { lock.unlock() }
        guard !cancelled else { return }
        cancelled = true
        onCancel()
    }
}

// ICE 서버 없이 만든 피어는 host 후보만 모아 같은 NAT 안에서만 화면이 붙는다(2026-08-21 실기).
// urls 정규화: TS 는 union 타입이 있어 원형 유지, Swift 는 통일.
/// A WebRTC ICE server entry — the same shape as an element of the server's `/api/about` `iceServers`,
/// and maps directly onto the platform WebRTC framework's `RTCIceServer`.
///
/// A peer created without ICE servers gathers host candidates only, so the picture connects **only within
/// the same NAT** — always put this list into the configuration when you create a screen-sending or
/// receiving peer.
///
/// `urls` is normalized to an array — the JSON side also allows a single string, but exposing both shapes
/// would make every consumer branch.
public struct IceServer: Sendable, Equatable {
    public var urls: [String]
    public var username: String?
    public var credential: String?

    public init(urls: [String], username: String? = nil, credential: String? = nil) {
        self.urls = urls
        self.username = username
        self.credential = credential
    }
}

// TS `client.ts` / Python `client.py` / .NET `ConnectClient.cs` / Kotlin `ConnectClient.kt` 미러.
/// The core client.
///
/// What it layers on top of the transport: an offline outbound queue (1000, oldest dropped) ·
/// a `TooManyRequests` backoff (1 s → ×2 → 60 s) · a synthesized `latest` retain (the 1000 most recently
/// updated channels) · the history snapshot · wildcard subscriptions · throttle. The server does not echo
/// a sender's own messages, so `latest` is also folded locally when a send succeeds.
public actor ConnectClient {
    public static let defaultQueueLimit = 1000
    // Kotlin 의 256 drop-oldest 버퍼와 같은 폭. 기본값(`.unbounded`)이면 쥐고만 있고 읽지 않는 스트림이
    // 재조인마다 ≤1000건짜리 스냅샷(`.historyReplaced`)까지 끝없이 쌓았다(실측: 스냅샷 100회 +11MB).
    /// The most events one ``events()`` stream buffers — beyond it the oldest are dropped first.
    static let eventBufferLimit = 256
    // 종전에는 채널마다 항목이 늘고 history 스냅샷만 비웠다 — 채널 이름이 동적인 스테이지(`cursor.<id>`)에
    // 오래 붙어 있는 어댑터는 끝없이 자랐다(실측: 고유 채널 5만 개 → 5만 항목).
    /// How many channels ``latest(_:)`` remembers — beyond it the channel **least recently updated** is
    /// forgotten first (``LatestCache``). The same value as the server's history ring and the offline queue,
    /// and not configurable.
    static let latestLimit = 1000
    // 종전의 메시지마다 Task 가 바로 그 버퍼였고 상한이 없었다.
    // (TS 처럼 콜백이 수신 루프를 막아 TCP 로 미는 길이 액터에는 없다.)
    /// The most **undelivered messages** the inbox holds while callbacks fall behind — beyond it the callback
    /// of the oldest undelivered message is skipped and `queue-overflow` is recorded in the diagnostics ring
    /// (`inbound: …` — two lines per backlog: skipping started · caught up, with the skipped count).
    /// Skipped messages are still folded into `latest()` in their own order (``TransportEventInbox``) — what
    /// is lost is that message's one callback, and an app that reads state channels with `latest()` (as the
    /// README and llms.txt recommend) sees the exact value.
    ///
    /// Why a limit: callbacks run one at a time on the actor, in arrival order. While callbacks are slower than
    /// arrivals, messages have to pile up somewhere. Back-pressure by blocking the socket queue is not possible —
    /// publishes · acks · disconnect handling share that queue, and a cooperative thread waiting on it with
    /// `sync` could starve the drain and deadlock.
    ///
    /// Why drop the oldest: the same principle as the offline queue (1000) · `events()` (latest 256) · the
    /// server history ring (1000) — the newest state is worth more, and a trigger that arrives long late
    /// (`door.open`) is harmful. A backlog of 1000 means callbacks cannot keep up with arrivals, and the
    /// messages still left are already 1000 × the callback time late — the same situation as a client that was
    /// disconnected longer than the ring holds (triggers are at-most-once, state comes from `latest()`).
    ///
    /// Memory bound: 1000 undelivered messages + the folds of skipped messages (one per channel, up to
    /// ``latestLimit``). State changes · snapshots · errors · session ends are never dropped — their number
    /// follows connection transitions (reconnects · rejoins), not the message rate, and as long as callbacks are
    /// only slow (not stuck) the drain reaches those items within 1000 messages.
    static let inboundLimit = 1000
    /// The number of inbox items one drain turn handles — when it is reached, the rest moves to the next turn
    /// (a new Task, at the back of the actor queue). Running to the end in one turn would keep the inbox from
    /// emptying while arrivals outpace processing (including callbacks that publish themselves), and other calls
    /// such as send() · close() could not get the actor.
    static let drainBatch = 64
    // 여섯 언어 동률.
    /// Interval before re-arming the flush after an ack timeout — the state stays connected, so no event would wake it.
    private static let flushRearm: TimeInterval = 0.2
    private static let backoffInitial: TimeInterval = 1
    private static let backoffMax: TimeInterval = 60
    private static let channelMax = 512

    private let transport: Transport
    private let queueLimit: Int
    private let strictReceives: Set<String>?
    private let exclusiveName: Bool

    // ── 대기(standby) ───────────────────────────────────────────────────────
    //
    // 같은 이름의 다른 인스턴스가 활성이면 이 어댑터는 **받기는 계속하고 보내기만
    // 멈춘다**. 활성이 떠나면 자동 승격된다.
    //
    // ⚠️ 판정은 **서버가** 한다 — roster 의 ``Member/standby`` 를 읽을 뿐 클라이언트가
    // 다시 계산하지 않는다. 규칙을 양쪽에 두면 어긋나고, 어긋난 쪽이 값을 이중으로 흘린다.
    private var standbyFlag = false
    /// Latch so entering standby is announced only once — the promotion record keys off it too.
    private var standbyNoticed = false
    // 종전에는 이 캐시가 프로세스 수명 내내 유지돼, 장수 어댑터가 만료된 값을 영원히 재사용하고 나중에 붙는 뷰어에서
    // 조용히 실패했다.
    /// Cache of a successful ``fetchIceServers()`` result — failures are not cached (they can be retried).
    /// ⚠️ TURN credentials **expire** — they are fetched again at `iceRefreshDeadline` (``iceRefreshAt(expiresAt:fetchedAt:)``
    /// — half the remaining lifetime; nil = STUN only, no expiry).
    private var iceServersCache: [IceServer]?
    private var iceRefreshDeadline: Int64?

    private var stateValue: TransportState = .idle
    private var historyList: [StageMessage] = []
    private var latestCache = LatestCache(limit: ConnectClient.latestLimit)
    private var channelSubs: [Int: (pattern: String, callback: @Sendable (StageMessage) -> Void)] = [:]
    /// One continuation per ``events()`` stream — all are `finish()`ed when the session ends so `for await` exits.
    private var eventSinks: [Int: AsyncStream<ClientEvent>.Continuation] = [:]
    private var nextSubscriptionId = 0

    private var queue: [QueueItem] = []
    private var paused = false

    /// One-shot latch for `ended` handling. Guarding on the state would make this handler a no-op as soon as the
    /// transport fires `onState(.ended)` first — the queue would not be emptied (pending ack sends hang forever)
    /// and the `.ended` event would be lost.
    private var endedHandled = false
    /// Why the session ended (the same value as the `ended` event) — carried when open() reports "ended while connecting".
    private var endReason: String?
    private var lastBackoff: TimeInterval = 0
    private var backoffTask: Task<Void, Never>?
    private var flushing = false
    /// The judgment window — the standby judgment right after a connect (first or reconnect) has not arrived yet.
    /// While it is open, sends do not go direct (they go to `deferred`) and the queue is not flushed.
    private var judgmentPending = false
    // (D6) 새 판정으로 갈아타며 창을 붙들던 종전의 세대 방식은 프레즌스가 잦으면 창이 닫히지 않았다.
    /// Only one roster judgment runs at a time. Presence arriving while one runs only marks that another is needed
    /// (`rejudge`), and it runs once more after the current one lands — a late answer from an overlapping judgment
    /// cannot overwrite the latest one (there is only one answer), and frequent presence does not keep the window open.
    private var judgingInFlight = false
    private var rejudge = false
    /// A number that goes up on every connect — raised when the window opens. A late answer from a judgment started
    /// before a disconnect (another connection's roster) is neither applied nor allowed to close the window.
    private var connectEpoch = 0
    // Connect 는 `stage:message` 를 소켓마다 초당 60개로(Connect Omni 서버의 스테이지 소켓 핸들러) 막는다.
    // 큐를 한꺼번에 내보내던 판은 그래서 밀린 값 가운데 가장 새 것들을 잃었다(150건 → 서버 60건, 마지막 값 150 은 끝내
    // 닿지 않음). 여섯 언어 동률.
    /// The server's publish limit — the server limits `stage:message` per socket (60 per second, burst 60, refilled
    /// continuously); over it, an acked publish gets `TooManyRequests` and a fire-and-forget one is **silently
    /// dropped**. So every publish (direct or queued) takes one token, a publish with no token goes to the queue, and
    /// the queue flushes only as many as there are tokens — an app that sends more than 60 per second sees its queue
    /// grow and the oldest values dropped first with `queue-overflow`, while the newest values arrive.
    static let messageRate = 60
    // 한도를 흉내 낸 서버에 대한 TS 실측: 끊긴 동안 150건, 8번 가운데 7번 fire-and-forget 1건이 조용히 버려졌다.
    // 30 이면 같은 실측 8번 모두 0건. 여섯 언어 동률.
    /// The most that can go out at once — half the server's burst (60). The server counts tokens by arrival time, not
    /// send time, so when network or processing delay bunches publishes together, a flow that was within the limit here
    /// overflows on the server. A burst equal to the server's (60) would absorb no bunching at all; 30 absorbs bunching
    /// of up to 0.5 seconds.
    static let messageBurst = 30
    // (D2')
    /// Publish tokens — refilled to full on every connect (the server's limit starts fresh for each socket).
    private var tokens = Double(ConnectClient.messageBurst)
    private var tokensAt = DispatchTime.now().uptimeNanoseconds
    private var tokenTimerArmed = false
    // (D4)
    /// The queue item the flush has sent and is awaiting an answer for — overflow never drops it (it has already gone out).
    private var inFlight: QueueItem?
    // (E6)
    /// A transport with no server publish limit (the MemoryStage test double) skips the token bucket
    /// (``Transport/bypassRateLimit``). With the bucket, 100 publishes would stretch out to 60 per second after the
    /// first 30, and an app tested on that double would see delays the real server does not have.
    private let bypassRateLimit: Bool
    // (E4)
    /// Whether `queue-overflow` was already recorded in this overflow episode — re-armed when the queue drains to half
    /// its limit. Recording it per item would flood the diagnostics ring (100 entries) whenever an app sends slightly
    /// over 60 per second.
    private var overflowNoted = false
    /// The last assigned publish sequence number (``QueueItem/seq``).
    private var sendSeq = 0
    // (TS · Dart 의 throttle) 벽시계(`Date()`)로 재면 시각이 뒤로 뛸 때 그만큼 다음 발행이 멈췄다.
    /// Slot for swapping in the monotonic clock (ns) — for tests. When nil it is `DispatchTime` uptime. It measures the
    /// token bucket and ``throttle(_:hz:)`` intervals — with a wall clock (`Date()`), the next publish would stall for as
    /// long as the clock jumped backwards.
    var monotonicNanosForTest: (@Sendable () -> UInt64)?
    private var monotonicNow: @Sendable () -> UInt64 {
        monotonicNanosForTest ?? { DispatchTime.now().uptimeNanoseconds }
    }
    /// Test only — swaps in the monotonic clock (``throttle(_:hz:)`` uses the clock it was created with).
    func setMonotonicClockForTest(_ clock: @escaping @Sendable () -> UInt64) { monotonicNanosForTest = clock }

    /// Monotonic time (ns) — playback (replay · simulate) measures time without progress with it (follows a clock swapped in by tests).
    func monotonicNanos() -> UInt64 { monotonicNow() }
    /// Monotonic time (ns) when the last backoff ended (sending resumed).
    private var backoffEndedAt: UInt64?
    /// Test only — the backoff ladder's current value (seconds).
    var backoffForTest: TimeInterval { lastBackoff }
    /// Test only — whether a backoff timer is armed.
    var backoffPendingForTest: Bool { backoffTask != nil || paused }
    /// Test only — whether the judgment window is open or a roster judgment is still running (tests wait for it to
    /// clear instead of sleeping a fixed time after a connect).
    var judgmentPendingForTest: Bool { judgmentPending || judgingInFlight }
    // (D1) 순서 큐에 넣던 종전에는 창 안에서 한 번 보낸 대기 인스턴스가 큐 모드에 갇혀 넘침이 진단 링을 덮었고 승격
    // 때 낡은 값이 쏟아졌다(TS · Python · Dart 와 같은 결정).
    /// Sends made inside the judgment window — collected apart from the ordered queue and settled when the judgment
    /// lands: active (including a failed judgment) → appended to the end of the queue and flushed; standby → the direct
    /// path's standby rule (fire-and-forget dropped · acked rejected with `Standby`). Shares `queueLimit` with the queue.
    private var deferred: [QueueItem] = []
    private var rearming = false

    // 처음 합류의 것을 붙잡아 두던 종전에는 roster 에서 나를 못 찾아 대기 판정이 "부재 = 활성" 으로 흘렀고(서버가 대기로
    // 판정한 인스턴스가 계속 발신했다) ``listeners(_:)`` 가 나를 다른 참여자로 셌다.
    /// This adapter's own member — filled after ``open()``, and always from the **most recent** join. A rejoin can
    /// receive a different participantId (the guest JWT lives 6 hours · a lost cookie).
    public private(set) var me: Member?

    /// The providesView this adapter asked to declare — the transport's connect option when the transport knows it,
    /// otherwise (a user transport) the joined member's. `known == false` when nothing is known yet.
    func requestedProvidesView() -> (known: Bool, value: String?) {
        if let declares = transport as? DeclaresProvidesView { return (true, declares.declaredProvidesView) }
        guard let me else { return (false, nil) }
        return (true, me.providesView)
    }
    /// Channels already noted as `no-listener` — recorded once per channel (re-armed when a declarer appears).
    private var noListenerNoted: Set<String> = []

    // 다른 언어는 만들 때 `ConfigError` 를 던지지만 이 이니셜라이저는 `throws` 가 아니라서(바꾸면 만드는 자리마다 `try` 가
    // 필요해진다 — ``throttle(_:hz:)`` 와 같은 이유).
    /// - Precondition: `queueLimit >= 1` — violating it stops the program (`precondition`). This initializer does not
    ///   throw, so ``connect(_:)`` · ``MemoryStage/join(_:)`` reject a bad value with `ConfigError` before they create
    ///   the client. Check the value when you create one directly.
    public init(
        transport: Transport,
        queueLimit: Int = ConnectClient.defaultQueueLimit,
        strictReceives: [String]? = nil,
        /// Whether `exclusiveName` was declared — the standby rule applies only to adapters that declare it.
        /// Without it, ``requestJudgment()`` returns immediately and there is no roster lookup cost either
        /// (duplicate names are normal in some setups, such as 20 visitor phones at a booth).
        exclusiveName: Bool = false,
        onLog: (@Sendable (LogEntry) -> Void)? = nil
    ) {
        precondition(queueLimit >= 1, "queueLimit must be at least 1 (got \(queueLimit))")
        self.transport = transport
        self.queueLimit = queueLimit
        self.strictReceives = strictReceives.map(Set.init)
        self.exclusiveName = exclusiveName
        // 싱크가 없어도 링버퍼는 채운다 — 사후 진단은 앱이 미리 준비하지 않아도
        // 가능해야 한다(실패한 뒤에 붙어서 읽는 게 대부분이다).
        self.diag = Diagnostics(sink: onLog)
        self.registrationWrittenByTransport = transport is SocketTransport
        self.bypassRateLimit = transport.bypassRateLimit
        // 소켓 트랜스포트면 등록 id 는 그 트랜스포트가 ack 를 받은 자리에서 쓴다(``registrationMirror`` 주석).
        (transport as? SocketTransport)?.attachRegistrationMirror(registrationMirror)
    }

    // Diagnostics 는 내부 락으로 스레드-세이프하다 — actor 격리 밖에서 부를 수 있게
    // nonisolated 로 둔다(트랜스포트 콜백은 비동기 컨텍스트가 아니다).
    private nonisolated let diag: Diagnostics

    /// Recent diagnostics entries (oldest first) — dump them right after a failure to see the cause.
    ///
    /// In environments with no file log this is the only after-the-fact clue. It never contains credentials.
    public nonisolated func diagnostics() -> [LogEntry] { diag.entries() }

    // 이 통로가 없으면 링 밖에 남아 사후 덤프에서 그 구간이 비었다(실사고 2026-09-03).
    /// Records a diagnostics entry — it goes to the ring buffer and the sink at once.
    ///
    /// Why it is public: the ring exists so that a trace remains without the app's help, and helpers outside the
    /// library (the screen broadcast's remote-input wiring) and the app's own markers need this path to land in the
    /// ring — otherwise that stretch is missing from the after-the-fact dump.
    /// ⚠️ Never put a credential in it — it is the ring's invariant (tests enforce it).
    public nonisolated func note(_ code: String, _ message: String, level: String = "info") {
        diag.note(code, message, level: level)
    }

    /// Puts a transport event into the ordered inbox — called from the socket thread. Starts a drain if the inbox was empty.
    private nonisolated func enqueueEvent(_ event: TransportEvent) {
        if eventInbox.push(event) { Task { await drainTransportEvents(batch: ConnectClient.drainBatch) } }
    }

    // ── 생명주기 ────────────────────────────────────────────────────────────

    public func open() async throws {
        setState(.connecting)
        var handlers = TransportHandlers()
        handlers.onMessage = { [weak self] message in
            // 메시지도 상태 전이와 **같은 FIFO** 로 — 메시지마다 Task 를 띄우면 액터 도착 순서가 생성 순서와
            // 무관해 콜백이 뒤집혀 받았다(실측: 2만 건에 인접 역전 680~1585). 한 줄에 서므로 스냅샷 · 종료와의
            // 순서도 지킨다: 스냅샷 뒤에 온 라이브 값이 재시드에 지워지지 않고, 종료 전에 온 메시지는 콜백을
            // 놓기 전에 배달된다. 밀린 양의 상한과 버리는 규칙은 ``inboundLimit``.
            guard let self else { return }
            let pushed = self.eventInbox.push(message: message)
            if let first = pushed.firstDropped {
                self.note(
                    "queue-overflow",
                    "inbound: callbacks are \(self.eventInbox.messageLimit) messages behind — "
                        + "skipping the oldest undelivered (\(first.channel)); latest() still folds them",
                    level: "warn")
            }
            if pushed.drain { Task { await self.drainTransportEvents(batch: ConnectClient.drainBatch) } }
        }
        handlers.onHistory = { [weak self] messages in
            // 상태 전이와 같은 FIFO 로 — 트랜스포트가 "history 다음 connected" 로 보낸 순서가 액터 도착에서
            // 뒤집히면, connected 에서 latest() 를 읽는 앱이 빈 값을 본다(아래 onState 주석).
            guard let self else { return }
            self.enqueueEvent(.history(messages))
        }
        handlers.onMemberJoin = { [weak self] member in
            // 프레즌스도 같은 FIFO 로 — 이벤트마다 Task 를 띄우면 같은 참여자의 join 뒤 leave 가 `events()` 에서
            // 뒤집혀 나갈 수 있었다(떠난 참여자가 "방금 들어왔다" 로 남는다). 대기 판정은 드레인이 이어서 띄운다.
            self?.enqueueEvent(.memberJoin(member))
        }
        handlers.onMemberLeave = { [weak self] participantId in
            self?.enqueueEvent(.memberLeave(participantId))
        }
        handlers.onJoined = { [weak self] member in
            // 이 합류의 나 — 같은 FIFO 로, 그 합류의 connected 보다 먼저 적재된다(대기 판정 · listeners 가 지금의 나를 본다).
            self?.enqueueEvent(.joined(member))
        }
        handlers.onState = { [weak self] state in
            // ⚠️ 전이마다 `Task` 를 띄우면 **액터 도착 순서가 보장되지 않는다.** 빠른
            // 재접속(connected → reconnecting → connected)에서 두 Task 가 뒤집히면
            // ① 늦게 처리된 `connected` 는 이미 connected 라 dedup 에 삼켜져
            //    `.stateChange` 가 아예 발화하지 않고(→ 화면 송출이 재알림을 못 한다),
            // ② 뒤늦은 `reconnecting` 이 상태를 되돌려 **재접속 flush 까지 막힌다**
            //    (`resumeSending()` 은 connected 전이에서만 돈다).
            // 그래서 소켓 스레드에서 **순서를 확정해 적재**하고, 액터에서는 그 FIFO 를
            // 그대로 소비한다 — 어느 Task 가 먼저 깨어나든 결과가 같다.
            // (lease-holder 통지가 같은 함정을 잠금 박스로 푼 것과 같은 자세다.)
            self?.enqueueEvent(.state(state))
        }
        handlers.onError = { [weak self] error in
            // 앱이 events() 를 구독하지 않아도 흔적은 남는다 — 무인 어댑터의 사후 진단.
            guard let self else { return }
            self.note("error", "\(error.code): \(error.message)", level: "warn")
            // 오류와 종료도 상태 전이와 **같은 FIFO** 로 — 따로 Task 를 띄우면 종료(스트림을 끝낸다)가 먼저
            // 도착해 원인 오류가 아무 스트림에도 닿지 않았다(실측 3000회 중 169회).
            self.enqueueEvent(.failure(error))
        }
        handlers.onEnded = { [weak self] reason in
            self?.enqueueEvent(.ended(reason))
        }
        handlers.onSignaling = { [weak self] payload in
            // 시그널링도 같은 FIFO 로, 버리지 않는다 — 프레임마다 Task 를 띄우면 리스너가 뒤집힌 순서로 받았다(실측:
            // 2만 건에 인접 역전 350~396 — sdp 보다 ice 가, 재협상의 새 offer 보다 옛 offer 가 늦게 닿는다). 양은
            // 프로토콜이 묶는다: 협상 한 번에 offer · answer 하나와 ICE 후보 몇 개, 접속마다 ready/peers 한 쌍이다.
            self?.enqueueEvent(.signaling(payload))
        }
        handlers.onAdapterLeaseHolder = { [weak self] payload in
            // ⚠️ per-통지 Task 는 액터 도착 순서를 보장하지 않는다 — 이 통지는 보안
            // 게이트라(null 뒤에 stale 통지가 늦게 도착하면 회수된 입력이 다시 열린다)
            // 소켓 스레드에서 **동기·순서대로** 배달한다(잠금 박스, 액터 미경유).
            self?.leaseHolderBox.deliver(payload)
        }
        handlers.onIceChanged = { [weak self] _ in
            // 같은 FIFO 로 — 액터의 캐시를 그 자리에서 버린다(내용은 보지 않는다).
            self?.enqueueEvent(.iceChanged)
        }

        let joined = try await transport.open(handlers: handlers)
        // onJoined 를 부르는 트랜스포트면 인박스가 이미 지금의 나를 적었을 수 있다(이 작업이 깨어나기 전에 재합류했을 수도 있다) —
        // 덮지 않는다.
        if me == nil { me = joined.me }
        stageId = joined.roomId
        // 합류 스냅샷(onHistory)이 인박스에 있으면 connected 전에 적용한다 — 문서의 계약은
        // "connected 를 보면 latest() 가 이미 시드돼 있다" 이다.
        drainTransportEvents()
        // 기다리는 사이 끝났다(close() · 트랜스포트 종료) — connected 로 되살리지 않는다.
        if endedHandled || stateValue == .ended {
            // 이유를 함께 — "연결하는 사이 끝났다" 만으로는 stage:ended · 서버 끊김 · close() 를 가릴 수 없다.
            throw AdapterError(
                "SessionEnded",
                "the stage session ended while connecting (\(endReason ?? "reason unknown"))")
        }
        setState(.connected)
        resumeSending()
    }

    // 종전에는 `connected` 즉시 flush 했고 대기 판정은 프레즌스 이벤트가 올 때만 갱신됐다 — 그래서 flush 시점의 판정이
    // 낡아 있었고, "끊긴 동안 쌓인 값이 재접속 직후 그대로 나가 이중으로 흐른다" 는, 큐 게이트가 막겠다고 적어 둔 바로
    // 그 시나리오가 실제로는 열려 있었다. 판정 창은 TS · Python · Dart 와 같은 창이다.
    /// On every connect (first or reconnect), **runs the standby judgment first and only then flushes the queue.**
    ///
    /// The order is the contract: a flush on a stale judgment would send the values queued while disconnected right
    /// after the reconnect and duplicate them.
    ///
    /// A second hole closes with it: the server sends the `stage:joined` member from its runtime map, so **it carries no
    /// standby** (the judgment is attached only on the path that reads the roster). And my own rejoin raises no presence
    /// **for me** — an adapter that quietly rejoined into a standby seat would never find out unless told, and would
    /// keep sending.
    ///
    /// Whether the judgment succeeds or fails, the flush goes ahead — a lookup failure blocking sends forever is worse.
    ///
    /// The judgment window closes when the current generation's judgment lands (success or failure) — while it is open,
    /// whatever wakes the flush (the backoff timer · the ack-timeout re-arm · a promotion) does not flush the queue, and
    /// sends do not go direct but are collected in `deferred`. The standby flag inside the window is the value from
    /// before the disconnect and is stale: anything sent then would be a duplicate publish from an instance the server
    /// has already judged standby. The window opens within the same actor turn that announces connected (the first
    /// `open()` too), so nothing can slip in between. For a judgment caught by a disconnect, the transport ends the
    /// pending ack with nil (`AckTimeout`) right away, so the window is not held open until the ack timeout (15 s).
    private func resumeSending() {
        // 창을 열 때 접속 번호를 올린다 — 끊기기 전에 시작한 판정의 늦은 답이 이 창을 닫지 못하게.
        connectEpoch += 1
        // 서버의 발행 한도는 소켓마다 가득 찬 채로 시작한다 — 이쪽도 맞춘다(D2').
        tokens = Double(ConnectClient.messageBurst)
        tokensAt = monotonicNow()
        guard exclusiveName else {
            scheduleFlush()
            return
        }
        judgmentPending = true
        // 이 연결의 판정이 닿으면 창을 닫고 flush 한다(runJudgment).
        requestJudgment()
    }

    // (D6)
    /// Requests a judgment — if one is running, only marks that it should run once more after it lands (it does not switch over).
    private func requestJudgment() {
        guard exclusiveName, !endedHandled else { return }
        if judgingInFlight {
            rejudge = true
            return
        }
        judgingInFlight = true
        Task { await runJudgment() }
    }

    // (D6)
    /// Checks against the server roster whether this instance is standby — one at a time. If another judgment was
    /// marked by the time it lands, it runs once more.
    ///
    /// **The client never re-derives the judgment.** The server decides active/standby and puts it on the roster as
    /// ``Member/standby`` — this only finds its own row and reads that value.
    private func runJudgment() async {
        while true {
            let epoch = connectEpoch
            let members = try? await transport.listMembers()
            // 그 사이 다시 접속했으면 이 답은 지난 연결의 것이다 — 반영하지 않는다(새 연결의 판정이 뒤따른다).
            if !endedHandled && epoch == connectEpoch {
                let pending = judgmentPending
                if let members {
                    // D3': 대기가 되는 판정(접속 판정이 대기 · 프레즌스 판정이 활성→대기)이면 남은 큐를 그 자리에서 버린다 — 다른
                    // 인스턴스가 활성 발신자다(접속 판정이면 끊긴 동안 내내 그랬다). 나중의 승격(몇 분 뒤일 수 있다) 때 이 낡은
                    // 값이 나가면 더 새 상태를 되돌린다(직행 경로가 대기 중에 버리는 것과 같은 이유). 개수는 standby 기록 한 줄에
                    // 싣는다 — 그래서 applyStandby 보다 먼저.
                    let dropped = standbyIn(members) && (pending || !standbyFlag) ? dropQueueForStandby() : 0
                    applyStandby(members, dropped: dropped)
                } else if pending {
                    // 접속 판정이 실패했다 — 활성으로 보고 흘린다(D5). 대기였던 인스턴스가 판정 실패로 영영 막혀 있었다. 접속
                    // 판정이 아닌 프레즌스 판정의 실패는 상태를 뒤집지 않는다 — 이 연결의 판정이 이미 있다.
                    markActive("judgment failed — treated as active")
                }
                if pending { closeJudgmentWindow(judgedStandby: standbyFlag) }
            }
            guard rejudge, !endedHandled else {
                rejudge = false
                judgingInFlight = false
                return
            }
            rejudge = false  // 한 번 더 — 도는 판정은 하나뿐이다
        }
    }

    /// Switches to active — if it was standby, records a promotion and flushes the queue.
    private func markActive(_ reason: String) {
        guard standbyFlag else { return }
        standbyFlag = false
        standbyNoticed = false
        note("active", reason, level: "warn")
        scheduleFlush()
    }

    /// Whether the roster judged this instance standby. An older server sends no standby at all — then it is as if there
    /// were no rule. **Absent ≠ standby**: folding nil into standby would make adapters silently stop sending on an older server.
    private func standbyIn(_ members: [Member]) -> Bool {
        guard let myId = me?.participantId else { return false }
        return members.first { $0.participantId == myId }?.standby == true
    }

    // (D3')
    /// Applies the server's roster judgment — records only when it changed, and flushes the queue on promotion.
    /// `dropped` is the number of publishes from the disconnected period this judgment dropped — carried on the one
    /// standby line (that line is recorded even if it was already standby, when something was dropped).
    private func applyStandby(_ members: [Member], dropped: Int = 0) {
        let next = standbyIn(members)
        guard next != standbyFlag else {
            if next && dropped > 0 { noteStandby(members, dropped: dropped) }
            return
        }
        standbyFlag = next
        if next {
            noteStandby(members, dropped: dropped)
            standbyNoticed = true
        } else if standbyNoticed {
            standbyNoticed = false
            note("active", "promoted — sending resumed", level: "warn")
            // 대기가 될 때 큐는 버렸다(D3'). 그래도 대기 동안 큐에 돌아온 항목(대기 직전에 내보낸 발행이 TooManyRequests ·
            // NotConnected 로 돌아온 것)이 있을 수 있다 — 승격이 그것을 흘려보낼 시점이다.
            scheduleFlush()
        }
    }

    /// The standby record line — who is active, plus the number of dropped sends if any.
    private func noteStandby(_ members: [Member], dropped: Int) {
        let myId = me?.participantId
        let myName = me?.displayName.trimmingCharacters(in: .whitespaces) ?? ""
        let holder = members.first {
            $0.participantId != myId
                && $0.standby == false
                && $0.displayName.trimmingCharacters(in: .whitespaces) == myName
        }?.displayName ?? (myName.isEmpty ? "?" : myName)
        let tail = dropped > 0 ? " — dropped \(dropped) queued send\(dropped == 1 ? "" : "s")" : ""
        note("standby", holder + tail, level: "warn")
    }

    // D3'. 내보내고 답을 기다리는 항목은 D4 와 같은 규칙.
    /// A judgment that makes this instance standby — drops the remaining queue (what piled up while disconnected · for
    /// tokens · in backoff). Fire-and-forget items just finish; acked publishes are rejected with `Standby` (they do not
    /// hang). The item already sent and awaiting its answer is left to its result (it has already gone out). Returns the
    /// number dropped.
    private func dropQueueForStandby() -> Int {
        let dropped = queue.filter { $0 !== inFlight }
        guard !dropped.isEmpty else { return 0 }
        queue.removeAll { $0 !== inFlight }
        for item in dropped {
            item.finish(item.wantAck ? .failure(standbyRefusal()) : .success(()))
        }
        return dropped.count
    }

    // (D1)
    /// Closes the judgment window — only when this connection's judgment has landed. Sends made inside the window:
    /// active (including a failed judgment) → appended to the end of the queue and flushed (after what piled up while
    /// disconnected); standby → the direct path's standby rule.
    private func closeJudgmentWindow(judgedStandby: Bool) {
        guard judgmentPending else { return }
        judgmentPending = false
        let moved = deferred
        deferred.removeAll()
        if judgedStandby {
            for item in moved {
                item.finish(item.wantAck ? .failure(standbyRefusal()) : .success(()))
            }
        } else {
            queue.append(contentsOf: moved)
        }
        scheduleFlush()
    }

    /// Disconnected inside the judgment window — the collected sends are the same as sends made while disconnected
    /// (to the queue). Moved before the disconnect is announced, so they stand ahead of later sends made while
    /// disconnected (ordering).
    private func parkDeferred() {
        guard !deferred.isEmpty else { return }
        queue.append(contentsOf: deferred)
        deferred.removeAll()
    }

    /// Not sent because of standby — the rejection for an acked publish (never a false success).
    private func standbyRefusal() -> AdapterError {
        AdapterError("Standby", "not sent: another instance named \"\(me?.displayName ?? "?")\" is active")
    }

    public var state: TransportState { stateValue }

    /// The number of publishes currently waiting in the offline queue.
    ///
    /// The most common cause of "it says connected but nothing happens" is **reconnecting + a queue backlog** (publishes
    /// pile up in the queue while `autoRejoin` silently retries). The state alone cannot tell them apart, so show this
    /// value as well on diagnostics screens and health endpoints.
    public var queuedCount: Int { queue.count + deferred.count }  // 판정 창 안의 발행도 아직 안 나갔다

    /// Whether this instance is currently standby — while standby, sending is suppressed (receiving continues).
    ///
    /// Always `false` when `exclusiveName` was not declared (the rule does not apply).
    public var standby: Bool { standbyFlag }

    /// The server's replay snapshot (chronological, ≤ 1000) — fully re-seeded on every reconnect.
    /// Not a persistent log (meant for "recent conversation"). With `pattern`, only that channel (wildcards allowed).
    public func history(_ pattern: String? = nil) -> [StageMessage] {
        guard let pattern else { return historyList }
        return historyList.filter { Wildcard.matchChannel(pattern, $0.channel) }
    }

    /// Leaves the stage and ends the session (the seat is released immediately).
    public func close() async {
        // 곧바로 끝낸다(TS · Python · Dart · C# 와 같다) — 상태 ended, 큐의 ack 발행은 SessionEnded, `.ended` 1회. 기다리는 것은
        // 소켓 정리(leave 확인, 최대 1초)뿐이다. 종전에는 그 대기가 끝난 뒤에야 끝나, close() 직후의 발행이 leave 뒤에 서버에 닿았다.
        handleEnded("client-closed")
        await transport.close()
    }

    // ── 시그널링 패스스루 (실험적) ──────────────────────────────────────────
    //
    // 어댑터는 페이로드를 **해석하지 않는다**. 이 통로의 의미(ready / sdp / ice)는
    // 화면을 송출하는 어댑터와 Stage 캔버스 사이의 계약이고 어댑터는 배달만 한다.
    // 값 채널(send/on)과 섞지 않는 이유도 그것이다 — 이건 메시지가 아니다.

    private var signalingListeners: [Int: @Sendable ([String: Any]) -> Void] = [:]
    private var nextSignalingToken = 0

    /// Experimental: this API may change in a minor release before 1.0.
    ///
    /// Subscribes to inbound signaling. Cancel with the returned handle's ``SignalingSubscription/cancel()``.
    public func onSignaling(
        _ listener: @escaping @Sendable ([String: Any]) -> Void
    ) -> SignalingSubscription {
        // 끝난 세션에는 붙이지 않는다 — 배달될 프레임이 없고, 리스너가 클라이언트를 잡으면 순환만 남는다.
        if endedHandled { return SignalingSubscription {} }
        nextSignalingToken += 1
        let token = nextSignalingToken
        signalingListeners[token] = listener
        // `Task` 안에서 `self?.` 로 약한 캡처를 읽으면 Swift 5.10 이 "captured var in
        // concurrently-executing code" 로 거부한다(6.x 는 허용). 밖에서 강한 `let` 으로 풀어 넘긴다.
        return SignalingSubscription { [weak self] in
            guard let self else { return }
            Task { await self.removeSignalingListener(token) }
        }
    }

    /// Experimental: this API may change in a minor release before 1.0.
    ///
    /// Outbound signaling — best-effort (dropped before connecting · after the session ended).
    public func sendSignaling(_ payload: [String: Any]) async {
        // 끝난 뒤에는 트랜스포트에 넘기지도 않는다(TS 와 같다) — close() 는 먼저 끝내고 퇴장 확인을 기다리는 동안(최대 1초) 소켓을
        // 붙여 둔다. 그 사이 ended 를 본 쪽의 시그널링이 서버에 닿아 다른 참여자에게 중계됐다(검수 실측: 닫기 90/90).
        if endedHandled || stateValue == .ended { return }
        await transport.sendSignaling(payload)
    }

    private func removeSignalingListener(_ token: Int) {
        signalingListeners.removeValue(forKey: token)
    }

    /// State transition listener — internal (the screen broadcast helper's ready re-announcement · disconnect snapshot ·
    /// cleanup on end).
    ///
    /// Why not `events()`: that stream keeps only the latest 256, so a burst of presence · snapshot events loses the state
    /// transitions in between — the helper hangs work on every single transition (ready on every reconnect), and losing
    /// even one leaves viewers unable to find the adapter again. Listeners are called synchronously on the actor in
    /// transition order and never dropped. The last transition they receive is `.ended`. Not attached to a client that
    /// has already ended (or is in the ended state) — returns nil.
    internal func onStateTransition(
        _ listener: @escaping @Sendable (TransportState) -> Void
    ) -> SignalingSubscription? {
        if endedHandled || stateValue == .ended { return nil }
        nextSignalingToken += 1
        let token = nextSignalingToken
        stateListeners[token] = listener
        return SignalingSubscription { [weak self] in
            guard let self else { return }
            Task { await self.removeStateListener(token) }
        }
    }

    private var stateListeners: [Int: @Sendable (TransportState) -> Void] = [:]

    private func removeStateListener(_ token: Int) {
        stateListeners.removeValue(forKey: token)
    }

    // ── 원격 조작 (§6-5·§6-2) ───────────────────────────────────────────────

    /// The stageId (= roomId) this adapter is attached to. nil before joining.
    public private(set) var stageId: String?

    private let leaseHolderBox = LeaseHolderListenerBox()

    /// The state transition FIFO — filled on the socket thread, consumed in order on the actor.
    private let eventInbox = TransportEventInbox()

    // (§6-5, N4)
    /// Experimental: this API may change in a minor release before 1.0.
    ///
    /// Subscribes to remote-input holder notices (`{registrationId, holderParticipantId, ttlMs}`). The adapter accepts
    /// input only from the holder named by these notices — the channel wiring and the holder gate are done by the
    /// ``startScreenBroadcast(client:stream:createSession:)`` helper.
    ///
    /// ⚠️ The listener is **called synchronously on the socket receive thread** (to preserve notice order — it is a
    /// security gate, so no reordering). Hop to the UI yourself inside the listener if you need to touch it.
    public nonisolated func onRemoteInputHolder(
        _ listener: @escaping @Sendable ([String: Any]) -> Void
    ) -> SignalingSubscription {
        let token = leaseHolderBox.add(listener)
        return SignalingSubscription { [leaseHolderBox] in
            leaseHolderBox.remove(token)
        }
    }

    // (§6-5)
    /// The last successful registration id — used to tell whether a holder notice belongs to this registration.
    public var lastRegistrationId: String? { registrationMirror.get() }

    // 둘이 함께 쓰면 등록 두 개가 겹치고 액터가 바쁠 때 값이 `r0 → r1 → r0 → r1` 로 되돌았고, 그 창에서 r1 통지가
    // 걸러졌다(3차 검수).
    /// The only store of the registration id — guarded by a lock so the screen broadcast helper does not wait on this
    /// actor when it judges a holder notice (waiting would delay a revocation for as long as channel callbacks are slow,
    /// letting the old holder's input through).
    ///
    /// There is **one** writer (``registrationWrittenByTransport``). With the socket transport, that transport writes it
    /// in the socket queue turn that received the registration ack, before waking the waiter — writing it on this actor
    /// would filter a new registration's notice arriving right after the ack against the old value. Two writers could
    /// revert the value while registrations overlap. Other transports write it on this actor after it receives the result.
    private nonisolated let registrationMirror = LockedValue<String?>(nil)
    private nonisolated let registrationWrittenByTransport: Bool
    internal nonisolated var currentRegistrationId: String? { registrationMirror.get() }

    /// Records a registration result — leaves it alone when the socket transport has already written it in ack order.
    private func recordRegistration(_ registrationId: String) {
        if !registrationWrittenByTransport { registrationMirror.set(registrationId) }
    }

    // (§6-2)
    /// Remote-input registration — submits the code a person typed on the device to the server. On success returns the
    /// registrationId + a deviceSecret for taking the registration over on reconnect. The deviceSecret is a **secret**:
    /// store it somewhere safe and never log it.
    public func submitRegistrationCode(_ code: String) async throws -> AdapterRegistrationResult {
        let result: AdapterRegistrationResult
        do {
            result = try await transport.submitRegistrationCode(code)
        } catch let error as AdapterError {
            throw withStateHint(error)
        }
        recordRegistration(result.registrationId)
        return result
    }

    // (§6-2)
    /// Takes a registration over with its deviceSecret on reconnect. When `stageId` is omitted, this session's is used.
    public func resumeRegistration(
        deviceSecret: String,
        stageId: String? = nil
    ) async throws -> AdapterRegistrationResult {
        guard let sid = stageId ?? self.stageId else {
            throw AdapterError(
                "NotConnected", "no stage joined yet — cannot resume",
                hint: notConnectedHintForState() ?? ConnectClient.notJoinedHint)
        }
        let result: AdapterRegistrationResult
        do {
            result = try await transport.resumeRegistration(stageId: sid, deviceSecret: deviceSecret)
        } catch let error as AdapterError {
            throw withStateHint(error)
        }
        recordRegistration(result.registrationId)
        return result
    }

    private func deliverSignaling(_ payload: [String: Any]) {
        for listener in signalingListeners.values {
            listener(payload)
        }
    }

    // ── ICE 배포 설정 ───────────────────────────────────────────────────────

    // (2026-08-21 실기) 진단 링에는 남기지 않는다 — 고정 지점 목록은 6개 언어 공통 계약이라 여기서 코드를 늘리지 않는다
    // (TS 도 console 로그뿐이다).
    /// The WebRTC ICE server list the server hands out as deployment configuration (the transport's authenticated path,
    /// `iceServers`) — put it as-is into the platform WebRTC's `RTCConfiguration.iceServers` when creating a screen peer
    /// over the signaling passthrough. A peer created without ICE servers gathers host candidates only, so the picture
    /// connects **only within the same NAT**.
    ///
    /// Never throws (fail-open) — returns the still-cached list, else the public STUN list, else an empty array. Within
    /// the same network it works without ICE, so a lookup failure must not block broadcasting. **Only successful results
    /// are cached** (failures · the public STUN received because the authenticated path failed are not — the next call
    /// retries). Nothing is recorded in the diagnostics ring.
    public func fetchIceServers() async -> [IceServer] {
        await fetchIceServersOutcome().servers
    }

    /// The body of ``fetchIceServers()`` — also returns whether the lookup succeeded (on failure the screen broadcast
    /// helper uses the last list it received and notes `ice-servers`). Failures (the transport's failure flag) are not
    /// cached — the next call asks again.
    func fetchIceServersOutcome() async -> (servers: [IceServer], ok: Bool) {
        let cached = iceServersCache
        if let cached, iceRefreshDeadline.map({ Self.nowMs() < $0 }) ?? true { return (cached, true) }

        // ① 인증 경로 — 트랜스포트가 JWT 를 쥐고 있다. TURN 단기 자격증명은 여기로만 온다.
        let result = await transport.fetchIceServers()
        // 실패는 캐시하지 않는다 — 만료 임박 캐시가 있으면 그것을, 없으면 트랜스포트가 그래도 준 임시 목록(인증 경로가 실패해 받은
        // 공개 STUN · 빈 목록)을 돌려준다.
        if result.failed { return (cached ?? result.iceServers, false) }
        if !result.iceServers.isEmpty {
            iceServersCache = result.iceServers
            iceRefreshDeadline = Self.iceRefreshAt(expiresAt: result.expiresAt, fetchedAt: Self.nowMs())
            return (result.iceServers, true)
        }
        // 빈 결과 = REST 표면 없는 트랜스포트(Engine·fake) — 만료 임박 캐시가 있으면 그걸 쓰고, 없으면 빈 배열(서버 직접
        // 접근은 트랜스포트만 한다).
        if let cached, !cached.isEmpty { return (cached, true) }
        return ([], true)
    }

    /// TURN credentials are fetched again at the latest this long before they expire (ms).
    static let iceRefreshMarginMs: Int64 = 60_000

    /// When a fetched ICE list is fetched again (epoch ms): once **half** of the lifetime it had when fetched has passed,
    /// or ``iceRefreshMarginMs`` before expiry — whichever comes first. Without an expiry (STUN only) it is never fetched
    /// again (nil). Why half: a screen-broadcast connection opens its relay with the credential held when it answers, and
    /// the relay drops when that credential expires — fetching again at half the remaining lifetime leaves new connections
    /// at least a quarter of the lifetime, usually more than half (the Connect server hands senders a new credential
    /// shortly before half-life). Same answer as the other five languages (`docs/conformance.json`, suite `iceRefreshAt`).
    static func iceRefreshAt(expiresAt: Int64?, fetchedAt: Int64) -> Int64? {
        guard let expiresAt else { return nil }
        let remaining = max(0, expiresAt - fetchedAt)
        return min(fetchedAt + remaining / 2, expiresAt - iceRefreshMarginMs)
    }

    static func nowMs() -> Int64 { Int64(Date().timeIntervalSince1970 * 1000) }

    // 서버가 zod 로 검증해 내려준다. TS `parseIceServers` 미러.
    /// Defensively filters `iceServers` from the `/api/about` response — the server validates it, but this may be talking
    /// to an older or different server implementation, so only well-shaped entries pass and the rest are silently
    /// dropped (the whole list does not fail).
    static func parseIceServers(_ raw: Any?) -> [IceServer] {
        guard let items = raw as? [Any] else { return [] }
        var out: [IceServer] = []
        for item in items {
            guard let object = item as? [String: Any] else { continue }
            let urls: [String]
            if let single = object["urls"] as? String {
                urls = [single]
            } else if let list = object["urls"] as? [String] {
                // 문자열 아닌 원소가 섞인 배열은 캐스트가 통째로 실패한다 — 항목 드롭.
                urls = list
            } else {
                continue
            }
            guard !urls.isEmpty,
                  urls.allSatisfy({ url in
                      url.hasPrefix("stun:") || url.hasPrefix("turn:") || url.hasPrefix("turns:")
                  })
            else { continue }
            out.append(IceServer(
                urls: urls,
                username: object["username"] as? String,
                credential: object["credential"] as? String
            ))
        }
        return out
    }

    // ── Adapter Stream 레이어 ───────────────────────────────────────────────

    // 오류는 여섯 언어 같은 계약(`SocketTransport.mapEnsureError`)이다.
    /// Ensures the Adapter Stream layer (optional) — the server creates and binds the layer of an adapter that joined
    /// with `providesView = "stream"` on its own at join time, so you do not need to call this. Calling it points at that
    /// layer (creating it if missing · idempotent). The name is not sent in the body; **the server derives it from this
    /// member's displayName** — you cannot create someone else's slot. The layer remains after the adapter disconnects.
    ///
    /// ``EnsuredStreamLayer/renamed``: the server gave a different name than requested (``me``'s displayName — e.g.
    /// `Bridge (2)`). After the client has ended it throws `SessionEnded` without a request; cancellation by the caller is `CancellationError`.
    public func ensureStreamLayer() async throws -> EnsuredStreamLayer {
        // 끝났으면 서버에 묻지 않는다 — 닫는 중(퇴장 대기)에도 REST 는 닿는다.
        if endedHandled || stateValue == .ended {
            throw AdapterError("SessionEnded", "this client has ended — cannot ensure a stream layer")
        }
        return try await transport.ensureStreamLayer()
    }

    // ── 송신 ────────────────────────────────────────────────────────────────

    /// - Parameter ack: When `true`, waits for the server ack (failures are ``AdapterError``).
    public func send(_ channel: String, _ value: Primitive, ack: Bool = false) async throws {
        try assertSendable(channel: channel)
        try ConnectClient.validateValue(value)
        try await dispatch(PublishInput(channel: channel, value: value), wantAck: ack)
    }

    /// Delivers to a single participant only (server unicast guard).
    public func sendTo(
        _ participantId: String,
        _ channel: String,
        _ value: Primitive,
        ack: Bool = false
    ) async throws {
        try assertSendable(channel: channel)
        try ConnectClient.validateValue(value)
        try await dispatch(
            PublishInput(channel: channel, value: value, targetParticipantId: participantId),
            wantAck: ack
        )
    }

    /// The JSON-string convention — the prototype parses the string to read it.
    ///
    /// - Parameter value: Only `[String: Any]` · `[Any]` · `String` · finite numbers · `Bool` · `NSNull`, and nestings of them.
    /// - Throws: ``AdapterError`` `ValidationError` — a value outside that (a struct · ``Primitive`` · a non-string
    ///   dictionary key · NaN/Infinity, at any depth). The message names the type.
    public func sendJSON(_ channel: String, _ value: Any, ack: Bool = false) async throws {
        // ⚠️ 먼저 거른다 — `JSONSerialization.data` 는 못 바꾸는 값을 Swift 오류가 아니라 **Objective-C 예외**로 던져
        // 프로세스를 죽인다(구조체 · NaN · 정수 키 사전 — 실측 exit 134, `try` 로 못 잡는다). 배열로 감싸 최상위
        // 조각(문자열 · 숫자)도 같은 규칙으로 본다 — `isValidJSONObject` 는 최상위에 배열 · 사전만 받는다.
        guard JSONSerialization.isValidJSONObject([value]) else {
            throw AdapterError(
                "ValidationError",
                "sendJSON: a \(type(of: value)) cannot be encoded as JSON — not a JSON type, a non-string "
                    + "dictionary key, or NaN/Infinity somewhere inside",
                hint: "Pass plain JSON-compatible values — [String: Any], [Any], String, numbers, Bool or NSNull "
                    + "(for an Encodable struct, encode it with JSONEncoder and send the string with send(_:_:))."
            )
        }
        let data = try JSONSerialization.data(withJSONObject: value, options: [.fragmentsAllowed])
        guard let encoded = String(data: data, encoding: .utf8) else {
            throw AdapterError("ValidationError", "value is not JSON-encodable")
        }
        try await send(channel, .string(encoded), ack: ack)
    }

    // (actor 로 넘기면 Task 스케줄링 순서가 흔들려 마지막 값이 뒤집힌다 — 테스트가 잡아냈다.) 다른 다섯 언어와 같은 계약.
    // 종전에는 클로저가 던지지 않고 발행을 `try?` 로 삼켜, 틀린 채널의 값이 아무 흔적 없이 사라졌다.
    // 다른 언어는 `ConfigError` 를 던지지만 이 함수는 `throws` 가 아니라서(바꾸면 호출 자리마다 `try` 가 필요해진다).
    /// High-frequency coalescing — beyond `hz` per second only the last value is sent, as a trailing flush.
    /// The returned closure is **synchronous**, so call order is preserved exactly (handing values to the actor would let
    /// Task scheduling reorder them and flip the last value).
    ///
    /// The returned closure performs the same validation as ``send(_:_:ack:)`` **at the call site** and throws
    /// `ValidationError` when it fails. The value is a ``Primitive``, so a wrong type does not compile; what is left is
    /// the channel (length 1…512). Once validation passes, a publish failure is fire-and-forget (a call after the session
    /// ended does not throw either).
    ///
    /// - Precondition: `hz > 0` — violating it stops the program (`precondition`). This function does not throw, so check
    ///   the value before calling.
    public func throttle(_ channel: String, hz: Double) -> @Sendable (Primitive) throws -> Void {
        precondition(hz > 0, "throttle hz must be > 0")
        let sender = ThrottledSender(client: self, channel: channel, interval: 1 / hz, now: monotonicNow)
        return { value in
            try ConnectClient.validateChannel(channel)
            try ConnectClient.validateValue(value)
            sender.push(value)
        }
    }

    // ── 수신 ────────────────────────────────────────────────────────────────

    /// Subscribes to a channel (exact or wildcard). Cancel the returned ``Subscription`` to unsubscribe.
    @discardableResult
    public func on(
        _ channelPattern: String,
        _ callback: @escaping @Sendable (StageMessage) -> Void
    ) -> Subscription {
        if let strictReceives,
           channelPattern != "*",
           !channelPattern.contains("*"),
           !strictReceives.contains(channelPattern) {
            // strictRouting 선언 밖 채널은 서버가 배달하지 않는다(타깃 메시지 제외).
            emit(.failure(AdapterError(
                "StrictRoutingWarning",
                "on(\"\(channelPattern)\"): channel is not in the strictRouting receives "
                + "declaration — the server will not deliver it"
            )))
        }

        // 끝난 세션에는 붙이지 않는다 — 더 올 메시지가 없고, 콜백이 클라이언트를 잡으면(핸들을
        // 버리는 것이 정상 사용법이다) 순환이 영영 풀리지 않는다.
        if endedHandled { return Subscription {} }

        let id = nextSubscriptionId
        nextSubscriptionId += 1
        channelSubs[id] = (channelPattern, callback)
        return Subscription { [weak self] in
            guard let self else { return }
            Task { await self.removeSubscription(id) }
        }
    }

    // (TS 와 같다)
    /// JSON-convention receive sugar — values that cannot be parsed are skipped. JSON `null` arrives as `NSNull` (it is not skipped).
    @discardableResult
    public func onJSON(
        _ channelPattern: String,
        _ callback: @escaping @Sendable (Any, StageMessage) -> Void
    ) -> Subscription {
        on(channelPattern) { message in
            guard let parsed = ConnectClient.parseJSONValue(message.value) else { return }
            callback(parsed, message)
        }
    }

    /// The lifecycle event stream — `for await event in await client.events()`.
    ///
    /// When the session ends (right after `.ended` is emitted) the stream **finishes** — `for await` exits on its own.
    /// Called on an ended client, it returns an already finished stream. Each stream keeps at most the latest 256 unread
    /// events (beyond it the oldest are dropped first).
    public func events() -> AsyncStream<ClientEvent> {
        AsyncStream(bufferingPolicy: .bufferingNewest(ConnectClient.eventBufferLimit)) { continuation in
            // 종료 뒤 구독 — 영원히 매달리는 대신 곧바로 끝낸다.
            if endedHandled {
                continuation.finish()
                return
            }
            let id = nextSubscriptionId
            nextSubscriptionId += 1
            eventSinks[id] = continuation
            continuation.onTermination = { [weak self] _ in
                guard let self else { return }
                Task { await self.removeEventSink(id) }
            }
        }
    }

    /// Synthesized retain — kept from the history seed + live messages + your own publishes. Only the 1000 most recently
    /// updated channels are remembered (``latestLimit``) — beyond it the channel least recently updated is forgotten.
    public func latest(_ channel: String) -> Primitive? { latestCache[channel] }

    /// The JSON-convention version of ``latest(_:)`` — nil when there is no value or it fails to parse; JSON `null` is `NSNull`.
    public func latestJSON(_ channel: String) -> Any? {
        guard let value = latestCache[channel] else { return nil }
        return ConnectClient.parseJSONValue(value)
    }

    /// Test hook — the number of channels ``latest(_:)`` remembers.
    internal var latestCount: Int { latestCache.count }

    /// Test hook — the number of undelivered messages backed up in the inbox (readable even while the actor is held by a callback).
    internal nonisolated var inboxMessageCount: Int { eventInbox.messageCount }

    /// Only one channel's JSON values from the history snapshot, chronological — values that fail to parse are skipped.
    public func historyJSON(_ channel: String) -> [Any] {
        historyList
            .filter { $0.channel == channel }
            .compactMap { ConnectClient.parseJSONValue($0.value) }
    }

    public func members() async throws -> [Member] {
        do {
            return try await transport.listMembers()
        } catch let error as AdapterError {
            throw withStateHint(error)
        }
    }

    // ── 세션 중 NotConnected 의 힌트 ─────────────────────────────────────
    //
    // NotConnected 의 기본 힌트("토큰의 서버 주소를 확인하라 — LAN IP 가 바뀐다")는 **접속 실패**용이다. 합류 전 · 재접속 중 ·
    // 끝난 뒤에 members · listeners · ping · 등록을 부르면 같은 코드가 나는데, 그 힌트는 엉뚱한 곳을 가리켰다(현장 시험). 그
    // 자리의 클라이언트 상태로 힌트를 고른다 — 여섯 언어 같은 문장. 접속 실패의 기본 힌트는 그대로다.

    static let notJoinedHint =
        "This client has not joined the stage yet — wait for connect to finish, then call again."
    static let reconnectingHint =
        "The connection dropped and the adapter is reconnecting on its own — call again once the state is connected, "
        + "and re-read the member list then (joins and leaves during the drop are not replayed)."
    static let endedHint =
        "This client has ended (it was closed, or the session ended) — create a new client to join again."

    /// The NotConnected hint for the current state — nil while connected (the default hint stays).
    private func notConnectedHintForState() -> String? {
        switch stateValue {
        case .idle, .connecting: return ConnectClient.notJoinedHint
        case .reconnecting: return ConnectClient.reconnectingHint
        case .ended: return ConnectClient.endedHint
        default: return nil
        }
    }

    /// Rebuilds a NotConnected thrown by the transport with the same message and a hint for the current state. Only those
    /// carrying the default hint — a hint the transport set itself knows its situation better.
    private func withStateHint(_ error: AdapterError) -> AdapterError {
        guard error.code == "NotConnected", error.hint == AdapterError.defaultHints["NotConnected"],
            let hint = notConnectedHintForState()
        else { return error }
        return AdapterError("NotConnected", error.message, hint: hint)
    }

    /// The participants that will **receive** this channel — reads the roster once (a `members()` round trip), leaves this
    /// adapter out, then splits it: those that declared it (`declared`) and those that receive it without declaring
    /// (`broadcast`). A participant that declared other channels and turned strictRouting on does not receive it, so it is
    /// in neither.
    ///
    /// When `declared` is empty, `no-listener` is recorded in the diagnostics ring — warn if `broadcast` is empty too
    /// (really nobody receives it), info otherwise. The publish path is untouched — call it once before attaching, or from
    /// a diagnostics screen. It catches the most common silent failure when several programs trigger each other on one
    /// stage (a typo in a channel name · a counterpart that is not up yet).
    ///
    /// The argument is a concrete channel name — patterns are not expanded (`'robot.*'` finds only participants that declared that exact string). The Engine profile has no presence round trip, so it returns empty lists and records nothing. The entry is recorded once per channel and re-armed when a declarer reappears — a diagnostics screen calling it periodically does not fill the ring.
    public func listeners(_ channel: String) async throws -> Listeners {
        let roster = try await members()
        var declared: [Member] = []
        var broadcast: [Member] = []
        // 빈 로스터 = 프레즌스 왕복이 없는 프로파일(Engine 은 빈 목록을 즉답한다; External 은 항상 나를 포함한다). 판정 불가이지 "아무도 없다" 가 아니므로 기록하지 않는다.
        if roster.isEmpty { return Listeners(channel: channel, declared: declared, broadcast: broadcast) }
        let mine = me?.participantId
        for m in roster {
            if let mine = mine, m.participantId == mine { continue }
            if m.declaresChannel(channel) {
                declared.append(m)
            } else if m.receives == nil || m.strictRouting != true {
                broadcast.append(m)
            }
        }
        if declared.isEmpty {
            // 채널당 한 번 — 선언자가 다시 나타나면 해제되어, 이후 다시 사라질 때 한 번 더 남는다.
            if noListenerNoted.insert(channel).inserted {
                note(
                    "no-listener",
                    "\(channel): no member declared it (\(broadcast.count) broadcast receiver(s))",
                    level: broadcast.isEmpty ? "warn" : "info"
                )
            }
        } else {
            noListenerNoted.remove(channel)
        }
        return Listeners(channel: channel, declared: declared, broadcast: broadcast)
    }

    /// Measures one round-trip latency to the server (ms).
    ///
    /// **No side effects** — it times a `stage:list-members` round trip, so it leaves no message on the stage and does not
    /// wake a prototype's Receive trigger. This surface was chosen because measuring with a publish would pile noise up
    /// in the Console.
    ///
    /// It measures the **adapter ↔ server** leg, not the end-to-end latency to another participant — the server does not
    /// echo a sender's own messages, so a single participant cannot measure that in principle.
    ///
    /// - Throws: `AdapterError` `NotConnected` — when not connected.
    public func ping() async throws -> Double {
        guard stateValue == .connected else {
            throw AdapterError(
                "NotConnected", "ping() requires a live connection (state: \(stateValue))",
                hint: notConnectedHintForState())
        }
        // 단조 시계 — 벽시계로 재면 시각 보정이 음수 RTT 를 만든다.
        let started = DispatchTime.now().uptimeNanoseconds
        _ = try await members()
        return Double(DispatchTime.now().uptimeNanoseconds - started) / 1_000_000.0
    }

    // ── 내부 ────────────────────────────────────────────────────────────────

    /// The JSON-string convention parser — the same strictness as `JSON.parse`. JSON `null` comes back as `NSNull` (nil means no value · not JSON).
    static func parseJSONValue(_ value: Primitive) -> Any? {
        guard case .string(let raw) = value, let data = raw.data(using: .utf8) else { return nil }
        // JSON `null` 은 값이다 — `NSNull` 로 돌려준다(TS 의 `null` 과 같다). 종전에는 nil 로 접어 `onJSON` 이 그 메시지를
        // 통째로 건너뛰었고 `historyJSON` 에서도 빠졌다. nil 은 "값 없음 · JSON 아님" 만 뜻한다.
        return try? JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed])
    }

    private func removeSubscription(_ id: Int) { channelSubs.removeValue(forKey: id) }
    private func removeEventSink(_ id: Int) { eventSinks.removeValue(forKey: id) }

    private func handleMessage(_ message: StageMessage) {
        foldLatest(message.channel, message.value, message.timestamp)
        for (_, sub) in channelSubs where Wildcard.matchChannel(sub.pattern, message.channel) {
            sub.callback(message)
        }
    }

    private func handleHistory(_ messages: [StageMessage]) {
        // 스냅샷은 권위 — 빈 배열 = clearHistory → 캐시 클리어.
        latestCache.removeAll()
        historyList = messages
        for message in messages {
            foldLatest(message.channel, message.value, message.timestamp)
        }
        emit(.historyReplaced(messages))
    }

    private func foldLatest(_ channel: String, _ value: Primitive, _ timestamp: Int64) {
        latestCache.fold(channel, value, timestamp)
    }

    /// Consumes the inbox in the order the socket thread fixed — see the `onState` · `onMessage` comments above.
    ///
    /// Processing is synchronous (no await) — taking an item and handling it stay together in one actor turn, so the order
    /// is the same however many drains are running. After `batch` items the rest moves to the next turn (``drainBatch``).
    /// `open()` empties it to the end, because it must apply the join snapshot before connected.
    private func drainTransportEvents(batch: Int = .max) {
        var handled = 0
        while let next = eventInbox.pop() {
            switch next {
            case .state(let state): handleTransportState(state)
            case .joined(let member) where !endedHandled: me = member
            case .joined: break
            // 종료 뒤 도착한 스냅샷은 버린다 — 끝난 세션의 latest()/history() 를 되살리지 않는다.
            case .history(let messages) where stateValue != .ended: handleHistory(messages)
            case .history: break
            case .failure(let error): emit(.failure(error))
            case .ended(let reason): handleEnded(reason)
            case .message(let message): handleMessage(message)
            case .memberJoin(let member):
                emit(.memberJoin(member))
                // 같은 이름이 새로 붙었을 수 있다 — 그러면 내가 대기로 내려간다.
                requestJudgment()
            case .memberLeave(let participantId):
                emit(.memberLeave(participantId))
                // 활성이 떠났을 수 있다 — 그러면 내가 이어받는다(대기가 곧 예비).
                requestJudgment()
            case .signaling(let payload): deliverSignaling(payload)
            case .iceChanged:
                // 서버가 이 참가자의 TURN 자격을 거뒀거나 발급을 멈췄다 · 다시 준다 — 다음 answer 가 새 목록을 받는다(붙은 연결은
                // 그대로다).
                iceServersCache = nil
                iceRefreshDeadline = nil
            case .skippedFolds(let folds): latestCache.absorb(folds)
            case .caughtUp(let skipped):
                note(
                    "queue-overflow", "inbound: caught up — \(skipped) message callback(s) skipped", level: "warn")
            }
            handled += 1
            if handled >= batch {
                // 인박스의 드레인 예약은 그대로다 — 이 턴이 이어 갈 책임을 진다(비어 있으면 다음 턴이 풀고 끝난다).
                Task { [weak self] in await self?.drainTransportEvents(batch: batch) }
                return
            }
        }
    }

    private func handleTransportState(_ state: TransportState) {
        guard stateValue != .ended else { return }
        // 알리기 전에 — 그 전이를 본 쪽의 끊김 중 발행이 창 안의 발행보다 뒤에 서게.
        if state != .connected { parkDeferred() }
        setState(state)
        // 재접속도 최초 접속과 같은 순서를 탄다 — 대기 판정 먼저, 그 다음 flush.
        if state == .connected { resumeSending() }
    }

    private func handleEnded(_ reason: String?) {
        guard !endedHandled else { return }
        endedHandled = true
        endReason = reason
        backoffTask?.cancel()
        backoffTask = nil
        let pending = queue + deferred
        queue.removeAll()
        deferred.removeAll()
        setState(.ended)
        for item in pending {
            item.finish(.failure(AdapterError("SessionEnded", "stage session ended")))
        }
        note("ended", reason ?? "unknown", level: "warn")
        emit(.ended(reason))
        releaseListeners()
    }

    // GC 언어에서는 남겨 둬도 그만이다. 순환이 close() 뒤에도 남았다(실측: 500/500 미해제).
    /// Releases every subscription the ended session was holding.
    ///
    /// Swift uses ARC: discarding a subscription handle is normal usage (`Subscription` is not cancelled on deinit), so a
    /// callback that captures the client would leave a client → subscription map → callback → client cycle after close().
    /// The `events()` streams are finished here — otherwise `for await` consumers would hang forever, and the client
    /// captured by that task would stay alive too.
    private func releaseListeners() {
        channelSubs.removeAll()
        signalingListeners.removeAll()
        stateListeners.removeAll()
        leaseHolderBox.close()
        let sinks = eventSinks
        eventSinks.removeAll()
        for continuation in sinks.values { continuation.finish() }
    }

    private func setState(_ state: TransportState) {
        guard stateValue != state else { return }
        stateValue = state
        note("state", state.rawValue)
        emit(.stateChange(state))
        for listener in stateListeners.values { listener(state) }
    }

    private func emit(_ event: ClientEvent) {
        for (_, sink) in eventSinks { sink.yield(event) }
    }

    private func assertSendable(channel: String) throws {
        if stateValue == .ended {
            throw AdapterError("SessionEnded", "stage session ended — cannot send")
        }
        try ConnectClient.validateChannel(channel)
    }

    /// Channel validation (`ValidationError`) — shared by ``send(_:_:ack:)`` and the throttle closure (it does not look at the session state).
    nonisolated static func validateChannel(_ channel: String) throws {
        if channel.isEmpty || channel.count > ConnectClient.channelMax {
            throw AdapterError("ValidationError", "channel must be 1..\(ConnectClient.channelMax) chars")
        }
    }

    // (여섯 언어 같은 규칙)
    /// Queue limit check — at least 1. With 0 or a negative value, both the publishes made while disconnected and those
    /// made when no token is left would all be dropped. Otherwise `ConfigError`. ``connect(_:)`` · ``MemoryStage/join(_:)``
    /// call it before creating the client.
    nonisolated static func validateQueueLimit(_ limit: Int) throws {
        if limit < 1 { throw AdapterError("ConfigError", "queueLimit must be at least 1 (got \(limit))") }
    }

    // (실측 exit 134)
    /// Value validation (`ValidationError`) — shared by ``send(_:_:ack:)`` · ``sendTo(_:_:_:ack:)`` · the throttle closure.
    /// JSON has no NaN · Infinity — sent as-is, the socket library's `JSONSerialization` throws an Objective-C exception and
    /// **kills the process** (`try?` cannot catch it). `Float` · `CGFloat` come in converted to `.double`, so they are
    /// caught here too.
    nonisolated static func validateValue(_ value: Primitive) throws {
        if case .double(let number) = value, !number.isFinite {
            throw AdapterError("ValidationError", "value must be a finite number (got \(ConnectClient.describeNonFinite(number)))")
        }
    }

    // TS `String(value)` 와 같은 표기.
    /// Formats a non-finite number as `NaN` · `Infinity` · `-Infinity` — Swift's `"\(Double.infinity)"` is `inf`. The
    /// time options' `ConfigError` uses it too.
    nonisolated static func describeNonFinite(_ number: Double) -> String {
        if number.isNaN { return "NaN" }
        return number < 0 ? "-Infinity" : "Infinity"
    }

    private func dispatch(_ input: PublishInput, wantAck: Bool) async throws {
        // 자리를 정하는 것과 넣는 것은 한 액터 차례 안이다(사이에 await 가 없다) — 창이 닫히거나 재접속 flush 가 끼어들 수 없다.
        // 끝난 뒤의 새 발행은 ack 든 아니든 그 자리에서 SessionEnded 다.
        if endedHandled || stateValue == .ended {
            throw AdapterError("SessionEnded", "stage session ended")
        }
        sendSeq += 1
        let seq = sendSeq
        if stateValue == .connected && judgmentPending {
            // 접속 중인 판정 창 안 → 따로 모았다가 판정이 닿으면 정한다(D1). 창 안에서 직행하면 끊기기 전의 낡은 대기 플래그로
            // 나간다 — 판정이 대기면 이중 발행이다.
            try await enqueue(input, wantAck: wantAck, seq: seq, deferredItem: true)
            return
        }
        if stateValue == .connected && standbyFlag {
            // 접속 중 + 대기 → 큐가 남아 있어도 직행 경로의 대기 규칙(D3). 큐로 보내던 종전에는 끊김을 거친 대기 인스턴스가 큐
            // 모드에 갇혔다(넘침이 링을 덮고, ack 는 Standby 대신 QueueOverflow, 승격 때 낡은 값). 남은 큐는 대기가 될 때
            // 버렸다(D3').
            //
            // ⚠️ `ack: true` 는 **거짓 성공을 주지 않는다.** fire-and-forget 은 조용히 버린다 — 진단 링에도 남기지 않는다(TS ·
            // Python 과 같다): 대기 진입은 이미 standby 로 한 번 남았고, 발행마다 남기면 100칸 링이 몇 초 만에 이것으로 찬다.
            if wantAck { throw standbyRefusal() }
            return
        }
        // 접속 중 + 백오프 아님 + 큐가 빔 + 토큰 있음 → 직행. 그 외(끊김 · 백오프 · 큐가 남음 · 토큰 없음) 전부 큐(D2').
        if stateValue == .connected && !paused && queue.isEmpty && takeToken() {
            let result = await transport.publish(input, ack: wantAck)
            // 직행 성공은 백오프 사다리를 리셋하지 않는다 — 서버가 답한 ack 의 flush 성공만 센다(TS · C# 와 같다). 직행
            // fire-and-forget 의 ok 는 내보냈다는 뜻뿐이라, 리셋하면 거절이 이어져도 사다리가 1초 위로 자라지 못했다.
            afterPublish(input, result, resetBackoff: false)
            if standbyFlag && !result.ok && (result.code == "TooManyRequests" || result.code == "NotConnected") {
                // 답을 기다리는 사이 대기가 됐다 — 큐에 되돌리면 ack 발행은 대기 내내 매달리고, 승격 때 낡은 값이 다시 나갔다.
                // 대기 규칙으로 끝낸다(D3' · E5 와 같은 이유).
                if wantAck { throw standbyRefusal() }
                return
            }
            if !result.ok && result.code == "NotConnected" {
                try await enqueue(input, wantAck: wantAck, seq: seq, requeue: true)
                return
            }
            if !result.ok && result.code == "TooManyRequests" {
                // flood 게이트 — 1급 신호. 큐에 되돌리고 백오프 후 재시도. 보낸 순서대로 되돌린다 — 동시에 거절된 직행 발행들을
                // 답이 온 순서대로 하나씩 맨 앞에 꽂으면 뒤집혀 다시 나갔다(실소켓: [.., 8, 7, 6, 5, 4]).
                beginBackoff()
                try await enqueue(input, wantAck: wantAck, seq: seq, requeue: true)
                return
            }
            if !result.ok {
                // ack 를 요구했든 아니든 남긴다 — 호출자가 예외를 삼켜도 흔적은 있어야 한다.
                note("send-dropped", result.code ?? "unknown", level: "warn")
                if wantAck { throw AdapterError(result.code ?? "ValidationError", result.message) }
            }
            return
        }
        try await enqueue(input, wantAck: wantAck, seq: seq)
    }

    private func afterPublish(_ input: PublishInput, _ result: PublishResult, resetBackoff: Bool = true) {
        guard result.ok else { return }
        // 서버 무에코 — 자기 발행 latest 는 로컬에서 접는다.
        foldLatest(input.channel, input.value, Int64(Date().timeIntervalSince1970 * 1000))
        if resetBackoff { lastBackoff = 0 }
    }

    // (D1)
    /// - Parameters:
    ///   - deferredItem: true for a send made inside the judgment window — it goes to `deferred`, not the ordered queue.
    ///   - seq: The publish sequence number (``QueueItem/seq``).
    ///   - requeue: A publish sent directly that came back rejected (TooManyRequests · NotConnected) — it finds its place by publish sequence number.
    private func enqueue(
        _ input: PublishInput, wantAck: Bool, seq: Int, deferredItem: Bool = false, requeue: Bool = false
    ) async throws {
        // publish 대기 중에 세션이 끝났을 수 있다 — 이미 비워진 큐에 넣으면 그 항목의
        // continuation 을 아무도 재개하지 않는다(영구 hang).
        if endedHandled || stateValue == .ended {
            // 트랜스포트 왕복 중에 끝났다 — ack 발행은 종료 처리와 같은 답을 곧바로 받고, fire-and-forget 은 그냥 끝난다(보내던
            // 중이었다 — 호출자에게 던질 일이 아니다). 끝난 뒤의 새 발행은 dispatch 가 먼저 거절한다.
            guard wantAck else { return }
            throw AdapterError("SessionEnded", "stage session ended")
        }
        let item = QueueItem(input: input, wantAck: wantAck, seq: seq)
        if deferredItem {
            deferred.append(item)
        } else if requeue {
            // 직행으로 보냈다가 거절돼 돌아온 발행 — 맨 뒤가 아니라 발행 순번대로: 그 뒤에 큐에 든 것보다 앞, 먼저 보냈던 것보다
            // 뒤 — 내보내고 답을 기다리는 항목보다 앞이어도 그렇다(TS 와 같다). 그 항목은 자리가 아니라 정체로 뺀다(D4). 내보낸
            // 항목 뒤로 미루던 종전에는 둘 다 거절되면 [D, S, S, D] 로 낡은 D 가 마지막에 나갔다 — 순번대로면 [D, S, D, S].
            var at = 0
            while at < queue.count && queue[at].seq < seq { at += 1 }
            queue.insert(item, at: at)
        } else {
            queue.append(item)
        }
        trimToLimit()
        // 넣은 뒤에 깨운다 — 지금 흐를 수 있으면(접속 중) flush 가 집어 간다.
        if !deferredItem { scheduleFlush() }
        guard wantAck else { return }
        try await withCheckedThrowingContinuation { continuation in
            item.attach(continuation)
        }
    }

    // (D4: 그것을 버리면 서버가 받은 ack 발행이 QueueOverflow 로 거절되고, flush 가 엉뚱한 항목을 뺐다)
    /// When the queue + window sends exceed queueLimit, drops the oldest first — from the queue (what piled up before the
    /// window) first. The item already sent and awaiting its answer (`inFlight`) is never dropped — it has already gone out.
    private func trimToLimit() {
        // 넘침 구간이 끝났는가 — 이번 항목을 넣기 전의 길이가 한도의 절반 이하로 빠져 있었으면 다시 남긴다(E4). 한도 바로
        // 아래에서 다시 남기면 한도를 조금 넘겨 보내는 앱은 떨굴 때마다 한 줄씩 남긴다.
        if queue.count + deferred.count - 1 <= queueLimit / 2 { overflowNoted = false }
        while queue.count + deferred.count > queueLimit {
            let skip = (queue.first.map { $0 === inFlight } ?? false) ? 1 : 0
            let dropped: QueueItem
            if queue.count > skip {
                dropped = queue.remove(at: skip)
            } else if !deferred.isEmpty {
                dropped = deferred.removeFirst()
            } else {
                return
            }
            dropped.finish(.failure(AdapterError("QueueOverflow", "offline queue overflow")))
            // 진단은 넘침 구간마다 한 줄(E4) — 이벤트와 항목의 거절은 종전처럼 항목마다다.
            if !overflowNoted {
                overflowNoted = true
                note(
                    "queue-overflow",
                    "\(dropped.input.channel) — queue full, dropping the oldest (noted once until it drains to half)",
                    level: "warn")
            }
            emit(.queueOverflow(dropped.input))
        }
    }

    // (D4)
    /// Removes that item from the queue — by identity, not by position (the head). Overflow can change the head while it waits.
    private func removeQueued(_ item: QueueItem) {
        if let index = queue.firstIndex(where: { $0 === item }) { queue.remove(at: index) }
    }

    // (D2')
    /// Takes one publish token — refilled continuously like the server's bucket. false when none is left.
    private func takeToken() -> Bool {
        if bypassRateLimit { return true }
        // 단조 시계(uptime)로 잰다 — 벽시계가 뒤로 뛰면 보충이 멈춘다(E2). 그래도 음수 경과는 0 으로(`&-` 로 감싸 돌면 한꺼번에
        // 가득 찼다), 토큰은 [0, burst] 로 묶는다.
        let now = monotonicNow()
        let rate = Double(ConnectClient.messageRate)
        let elapsed = now >= tokensAt ? Double(now - tokensAt) / 1e9 : 0
        tokens = min(Double(ConnectClient.messageBurst), max(0, tokens + elapsed * rate))
        tokensAt = now
        guard tokens >= 1 else { return false }
        tokens -= 1
        return true
    }

    /// Wakes the flush when the next token is available (only one is armed).
    private func scheduleTokenFlush() {
        guard !tokenTimerArmed else { return }
        tokenTimerArmed = true
        let waitSeconds = max(0.001, (1 - tokens) / Double(ConnectClient.messageRate))
        Task { [weak self] in
            try? await Task.sleep(nanoseconds: UInt64(waitSeconds * 1_000_000_000))
            guard let self else { return }
            await self.tokenTimerFired()
        }
    }

    private func tokenTimerFired() async {
        tokenTimerArmed = false
        await flush()
    }

    private func scheduleFlush() {
        Task { await flush() }
    }

    private func flush() async {
        guard !flushing else { return }
        flushing = true
        defer { flushing = false }

        while true {
            // 대기 중에는 큐도 흘리지 않는다. dispatch 만 막으면 **큐 경유 경로가 규칙을
            // 우회한다**: 끊겼을 때 쌓인 값이 재접속 직후(그 사이 다른 인스턴스가 활성을
            // 가져간 상태로) 그대로 나가 값이 이중으로 흐른다. 흘리는 중에 대기로 내려갈
            // 수 있으므로 루프 안에서 매번 다시 본다(프레즌스는 비동기다).
            // 접속 직후 대기 판정이 끝나기 전에도 흘리지 않는다 — 판정이 닿으면 closeJudgmentWindow 가 다시 부른다.
            guard !queue.isEmpty, stateValue == .connected, !paused, !standbyFlag, !judgmentPending else { return }
            let item = queue[0]
            // D2': 토큰만큼만 내보낸다 — 서버의 발행 한도(초당 60 · burst 60)를 넘긴 fire-and-forget 은 서버가 조용히 버린다.
            guard takeToken() else {
                scheduleTokenFlush()
                return
            }
            // fire-and-forget 은 ack 없이 내보내고 곧바로 다음 것으로 간다(순서는 소켓 하나가 지킨다). 전부 ack 를 기다리면 처리량이
            // 왕복 1회당 1건이라, 그보다 빨리 보내는 발신자는 큐를 영영 비우지 못했다. ack 발행은 그 ack 를 기다린다 — 결과 ·
            // 재시도는 종전대로(그래서 ack 발행은 왕복 1회당 1건이다 — 고빈도 흐름에 ack 를 쓰지 말 것). 결과: 큐 경유
            // fire-and-forget 도 직행처럼 최대 한 번이다(내보낸 직후의 끊김을 보지도, 다시 보내지도 않는다). TooManyRequests
            // 백오프는 ack 발행만 일으킨다.
            inFlight = item
            let result = await transport.publish(item.input, ack: item.wantAck)
            inFlight = nil
            if standbyFlag && !result.ok && (result.code == "TooManyRequests" || result.code == "AckTimeout") {
                // 답을 기다리는 사이 대기가 됐다(E5) — 다시 보낼 차례는 승격 뒤인데, 그때 이 값은 낡았다(D3' 와 같은 이유). 큐에
                // 남기면 대기 내내 매달리다 승격 때 다시 나갔다. 대기 규칙으로 끝낸다.
                removeQueued(item)
                item.finish(item.wantAck ? .failure(standbyRefusal()) : .success(()))
                return
            }
            if !result.ok && result.code == "TooManyRequests" {
                beginBackoff()
                return
            }
            // 항목을 큐에 남긴다 — 재접속/재시도 flush 를 기다린다. AckTimeout(소켓은 연결 중,
            // 15s ack 초과)에 빼면 정렬된 큐가 그 항목을 건너뛰어 조용히 유실된다 — 다른 다섯
            // 언어와 같이 남긴다. 그리고 AckTimeout 은 상태 이벤트가 오지 않으므로(소켓은 연결된
            // 채다) 남기기만 하면 아무도 flush 를 다시 깨우지 않는다 — 짧게 재무장한다.
            if !result.ok && (result.code == "NotConnected" || result.code == "AckTimeout") {
                if result.code == "AckTimeout" { rearmFlush() }
                return
            }

            removeQueued(item)
            // 백오프 사다리는 서버가 답한 것(ack)으로만 리셋한다 — fire-and-forget 의 ok 는 내보냈다는 뜻뿐이다.
            afterPublish(item.input, result, resetBackoff: item.wantAck)
            if result.ok || !item.wantAck {
                if !result.ok {
                    // 직행 경로와 같은 지점으로 남긴다 — 큐 경유라고 흔적이 달라질 이유가 없다.
                    note("send-dropped", "\(result.code ?? "unknown") (queued)", level: "warn")
                }
                item.finish(.success(()))
            } else {
                item.finish(.failure(AdapterError(result.code ?? "ValidationError", result.message)))
            }
        }
    }

    // 여섯 언어 동률.
    /// Re-arms the flush after an ack timeout, once `flushRearm` has passed — the state stays connected, so no event
    /// would wake it.
    private func rearmFlush() {
        guard !rearming else { return }
        rearming = true
        Task { [weak self] in
            try? await Task.sleep(nanoseconds: UInt64(ConnectClient.flushRearm * 1_000_000_000))
            guard let self else { return }
            await self.clearRearming()
            await self.flush()
        }
    }

    private func clearRearming() { rearming = false }

    /// The server flood gate — pauses 1 s → ×2 → at most 60 s, then retries the flush.
    private func beginBackoff() {
        // 이미 쉬는 중이면 다시 늘리지 않는다 — 한 번의 거절 구간에 한 번. 끝난 뒤에는 타이머를 만들지 않는다(세션 종료 · 닫기와
        // 엇갈려 늦게 온 TooManyRequests 가 끝난 클라이언트에 타이머를 남겼다).
        guard !endedHandled else { return }
        // 아직 쉬는 중이면 다시 늘리지 않는다 — 한 번의 거절 구간에 한 번(쉬는 중에 온 답은 사다리를 건드리지 않는다).
        guard !paused else { return }
        // 지난 백오프가 끝난(보내기를 다시 시작한) 때보다 지금 백오프의 두 배 넘게 뒤에 온 거절은 새 구간이다 — 사다리를 첫 칸으로.
        // 사다리는 flush 의 ack 성공에서만 내려와, 서로 무관한 단발 거절이 1→2→4→8초로 올라갔다. 기준은 지난 거절 답이 아니라
        // 백오프가 끝난 때다 — 답 왕복이 1.1초면 지난 답 기준으로는 늘 "새 구간" 이라 사다리가 오르지 못했다([1,1,1,…]). 단조 시계.
        let now = monotonicNow()
        if let ended = backoffEndedAt, now >= ended, Double(now - ended) / 1e9 > 2 * lastBackoff { lastBackoff = 0 }
        paused = true
        lastBackoff = lastBackoff > 0
            ? min(ConnectClient.backoffMax, lastBackoff * 2)
            : ConnectClient.backoffInitial
        let delay = lastBackoff
        backoffTask = Task { [weak self] in
            try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
            guard !Task.isCancelled else { return }
            await self?.resumeAfterBackoff()
        }
    }

    private func resumeAfterBackoff() async {
        paused = false
        backoffEndedAt = monotonicNow()  // 다음 거절의 구간 기준
        backoffTask = nil
        await flush()
    }

    private final class QueueItem {
        let input: PublishInput
        let wantAck: Bool
        /// Publish sequence number — the order in which `send` took its place (assigned within an actor turn). A direct
        /// publish that comes back to the queue rejected finds its place in this order.
        let seq: Int
        private var continuation: CheckedContinuation<Void, Error>?
        private var pendingResult: Result<Void, Error>?

        init(input: PublishInput, wantAck: Bool, seq: Int) {
            self.input = input
            self.wantAck = wantAck
            self.seq = seq
        }

        func attach(_ continuation: CheckedContinuation<Void, Error>) {
            if let pendingResult {
                continuation.resume(with: pendingResult)
                self.pendingResult = nil
                return
            }
            self.continuation = continuation
        }

        func finish(_ result: Result<Void, Error>) {
            guard wantAck else { return }
            if let continuation {
                continuation.resume(with: result)
                self.continuation = nil
                return
            }
            pendingResult = result
        }
    }
}

/// throttle's state — guarded by a lock outside the actor to preserve call order.
private final class ThrottledSender: @unchecked Sendable {
    private let lock = NSLock()
    private weak var client: ConnectClient?
    private let channel: String
    private let interval: TimeInterval

    private var pending: Primitive?
    /// Last publish time — monotonic clock (ns). With a wall clock (`Date()`), the next publish would stall for as long as the clock jumped backwards.
    private var lastSentAt: UInt64?
    private var timer: Task<Void, Never>?
    private let now: @Sendable () -> UInt64

    init(client: ConnectClient, channel: String, interval: TimeInterval, now: @escaping @Sendable () -> UInt64) {
        self.client = client
        self.channel = channel
        self.interval = interval
        self.now = now
    }

    func push(_ value: Primitive) {
        lock.lock()
        pending = value
        let current = now()
        let sinceLast = lastSentAt.map { current >= $0 ? Double(current - $0) / 1e9 : 0 } ?? .infinity
        if timer == nil && sinceLast >= interval {
            flushLocked()
            lock.unlock()
            return
        }
        if timer == nil {
            let wait = sinceLast.isInfinite ? 0 : max(0, interval - sinceLast)
            timer = Task { [weak self] in
                try? await Task.sleep(nanoseconds: UInt64(wait * 1_000_000_000))
                guard let self else { return }
                // 비동기 문맥에서는 lock()/unlock() 을 직접 부를 수 없다(Swift 6 오류) — 범위 잠금으로 잡는다.
                self.lock.withLock {
                    self.timer = nil
                    self.flushLocked()
                }
            }
        }
        lock.unlock()
    }

    /// - Important: Call only while holding `lock`.
    private func flushLocked() {
        guard let value = pending else { return }
        pending = nil
        lastSentAt = now()
        let client = self.client
        let channel = self.channel
        Task { try? await client?.send(channel, value) }
    }
}

/// Handle returned by ``ConnectClient/onSignaling(_:)``.
///
/// The subscription is not cancelled on deinit — the same stance as a channel subscription (``Subscription``).
public final class SignalingSubscription: Sendable {
    private let onCancel: @Sendable () -> Void

    init(_ onCancel: @escaping @Sendable () -> Void) {
        self.onCancel = onCancel
    }

    public func cancel() {
        onCancel()
    }
}

/// Events the transport enqueues in a fixed order — puts state transitions · history snapshots · errors · session ends ·
/// presence · signaling · messages in **one line**. Dispatched as separate Tasks, both "history before connected" and the
/// order among messages could flip on the actor.
enum TransportEvent {
    case state(TransportState)
    /// This join's own member — arrives before that join's `connected`.
    case joined(Member)
    case history([StageMessage])
    /// A transport error — if it arrived before the session end, it is delivered before the session end.
    case failure(AdapterError)
    case ended(String?)
    case memberJoin(Member)
    case memberLeave(String)
    /// A raw `stage:signaling` frame — never dropped (its volume is bounded by the number of negotiations).
    case signaling([String: Any])
    /// `stage:ice-changed` — drop the cached ICE servers.
    case iceChanged
    case message(StageMessage)
    /// The latest folds of messages whose callbacks were skipped — merged into `latest()` at the position those messages held (an item the inbox produces).
    case skippedFolds(LatestCache)
    /// The inbox emptied — the number of messages whose callbacks were skipped because of overflow until then (a report from the inbox, not the transport).
    case caughtUp(skipped: Int)
}

/// A FIFO whose order the socket thread fixes and the actor consumes as-is.
///
/// Why it lives outside the actor (behind a lock): the order has to be fixed **at enqueue time**. Left to actor arrival
/// order, two `Task`s can flip, swallowing or reverting transitions and calling message callbacks out of order.
///
/// It holds two lines — only messages are bounded (``ConnectClient/inboundLimit``); the rest are never dropped. Arrival
/// order across the two is kept by enqueue numbers (take whichever head came in first). The drain is announced only once,
/// when the first item arrives while it is empty — no Task per message while backed up.
///
/// An overflowing message skips only its callback; its latest fold is kept (``skipped``). Where the folds are merged is
/// the whole ordering question — only messages and snapshots touch latest, and a skipped message arrived before every
/// remaining message. So only snapshots not yet applied matter: folds of messages that came before such a snapshot are
/// dropped because the snapshot overwrites them, and folds of messages that came after it are merged **after** that
/// snapshot is taken (``skippedAfter``).
final class TransportEventInbox: @unchecked Sendable {
    let messageLimit = ConnectClient.inboundLimit
    private let lock = NSLock()
    private var nextSeq: UInt64 = 0
    /// Enqueue number of the last item taken — taking goes in number order across both lines, so any larger number is still queued.
    private var poppedSeq: UInt64 = 0
    private var events = FIFO<(seq: UInt64, event: TransportEvent)>()
    private var messages = FIFO<(seq: UInt64, message: StageMessage)>()
    /// A drain is scheduled or running — cleared by a take that finds the inbox empty.
    private var drainScheduled = false
    /// Messages whose callbacks were skipped because of overflow since the inbox last emptied — reported as ``TransportEvent/caughtUp(skipped:)`` when it empties again.
    private var skippedSinceEmpty = 0
    /// Enqueue number of the most recently enqueued history snapshot (0 = none).
    private var lastHistorySeq: UInt64 = 0
    /// Latest folds of skipped messages not merged yet. Its limit equals ``ConnectClient/latestLimit``, so a channel
    /// forgotten here is one that, after merging, would also be pushed out of latest by the 1000 more recent channels. The
    /// only divergence from folding one at a time is when a value with a backwards timestamp coincides with forgetting
    /// (``LatestCache/absorb(_:)``).
    private var skipped = LatestCache(limit: ConnectClient.latestLimit)
    /// ``skipped`` is merged after the snapshot with this number is taken (0 = right at the next take).
    private var skippedAfter: UInt64 = 0

    var messageCount: Int { lock.withLock { messages.count } }

    /// Enqueue — true when a new drain must be started.
    func push(_ event: TransportEvent) -> Bool {
        lock.withLock {
            nextSeq &+= 1
            events.append((nextSeq, event))
            if case .history = event { lastHistorySeq = nextSeq }
            return scheduleDrainLocked()
        }
    }

    /// Enqueues a message — beyond the limit, skips the callback of the oldest undelivered message (keeping its fold).
    /// `firstDropped` is the first message skipped in this backlog (so the diagnostics ring gets one line), and `drain`
    /// says whether a new drain must be started.
    func push(message: StageMessage) -> (drain: Bool, firstDropped: StageMessage?) {
        lock.withLock {
            nextSeq &+= 1
            messages.append((nextSeq, message))
            var firstDropped: StageMessage?
            if messages.count > messageLimit, let oldest = messages.popFirst() {
                if skippedSinceEmpty == 0 { firstDropped = oldest.message }
                skippedSinceEmpty += 1
                keepFoldLocked(oldest.seq, oldest.message)
            }
            return (scheduleDrainLocked(), firstDropped)
        }
    }

    /// The next item — the folds of skipped messages first (when it is their turn), then whichever of the two heads came in
    /// first. When empty (after the report, if anything was skipped) returns nil and clears the drain flag — the next
    /// enqueue starts a new drain.
    func pop() -> TransportEvent? {
        lock.withLock {
            if !skipped.isEmpty, skippedAfter <= poppedSeq {
                let folds = skipped
                // 새 캐시로 바꾼다 — 넘긴 쪽과 버퍼를 나눠 쥐지 않고, 밀림이 끝나면 칸도 놓는다.
                skipped = LatestCache(limit: ConnectClient.latestLimit)
                skippedAfter = 0
                return .skippedFolds(folds)
            }
            let fromEvents: Bool
            switch (events.first?.seq, messages.first?.seq) {
            case (nil, nil):
                if skippedSinceEmpty > 0 {
                    defer { skippedSinceEmpty = 0 }
                    return .caughtUp(skipped: skippedSinceEmpty)
                }
                drainScheduled = false
                return nil
            case (.some(let eventSeq), .some(let messageSeq)): fromEvents = eventSeq < messageSeq
            case (.some, nil): fromEvents = true
            case (nil, .some): fromEvents = false
            }
            if fromEvents, let next = events.popFirst() {
                poppedSeq = next.seq
                return next.event
            }
            if let next = messages.popFirst() {
                poppedSeq = next.seq
                return .message(next.message)
            }
            drainScheduled = false // 닿지 않는다 — 위 switch 가 비지 않은 줄을 골랐다
            return nil
        }
    }

    /// Folds a skipped message for latest — the last snapshot not yet taken decides where it is merged.
    private func keepFoldLocked(_ seq: UInt64, _ message: StageMessage) {
        if lastHistorySeq > poppedSeq {
            // 그 스냅샷보다 먼저 온 메시지 — 스냅샷이 latest 를 통째로 덮으므로 접을 것이 없다.
            if lastHistorySeq > seq { return }
            // 모아 둔 접기는 전부 그 스냅샷보다 먼저 온 것이다(뒤에 온 것이 있었다면 이미 이 스냅샷에 묶였다).
            if skippedAfter < lastHistorySeq {
                skipped.removeAll()
                skippedAfter = lastHistorySeq
            }
        }
        skipped.fold(message.channel, message.value, message.timestamp)
    }

    private func scheduleDrainLocked() -> Bool {
        if drainScheduled { return false }
        drainScheduled = true
        return true
    }
}

/// A FIFO taken by a head index — `Array.removeFirst()` is O(n), so with 20,000 items backed up, taking alone would be
/// O(n²). Taken slots are cleared (releasing the value), and once the head passes the halfway point the front is cut
/// off — amortized O(1) per take, with slots a little over twice the remaining items.
struct FIFO<Element> {
    private var slots: [Element?] = []
    private var head = 0

    var count: Int { slots.count - head }
    var first: Element? { head < slots.count ? slots[head] : nil }

    mutating func append(_ element: Element) { slots.append(element) }

    /// Removes matching items and keeps the rest in order — O(n), used only on rare paths such as overflow handling.
    mutating func removeAll(where shouldRemove: (Element) -> Bool) {
        var rest: [Element?] = []
        rest.reserveCapacity(count)
        for slot in slots[head...] {
            if let element = slot, !shouldRemove(element) { rest.append(element) }
        }
        slots = rest
        head = 0
    }

    mutating func popFirst() -> Element? {
        guard head < slots.count else { return nil }
        let element = slots[head]
        slots[head] = nil
        head += 1
        if head == slots.count {
            slots.removeAll(keepingCapacity: true)
            head = 0
        } else if head >= 64, head * 2 >= slots.count {
            slots.removeFirst(head)
            head = 0
        }
        return element
    }
}

// (TS `Map` · Kotlin `LinkedHashMap` 의 자리)
/// Storage for ``ConnectClient/latest(_:)`` — holds the last value per channel for **only the `limit` most recently
/// updated** channels.
///
/// Every accepted fold (live · history seed · own publish) makes its channel the most recent, and when a **new** channel
/// exceeds the limit, the least recently updated channel is forgotten (without notice). A fold rejected for a stale
/// timestamp does not change the order either.
///
/// The order is an index-based doubly linked list over a slot array — updates and evictions are all O(1), so no 1000-slot
/// scan per message, and a new channel reuses a forgotten channel's slot, so slots never exceed the limit. Swift has no
/// standard insertion-ordered dictionary, and nodes as classes would add an allocation and ARC per channel.
struct LatestCache {
    private struct Slot {
        var channel: String
        var value: Primitive
        var timestamp: Int64
        /// One slot more recent · one slot older (-1 = none).
        var newer: Int
        var older: Int
    }

    let limit: Int
    private var slots: [Slot] = []
    private var slotOf: [String: Int] = [:]
    /// The most recently updated slot · the least recently updated slot (the next to forget). -1 = empty.
    private var newest = -1
    private var oldest = -1

    init(limit: Int) {
        precondition(limit > 0, "latest limit must be > 0")
        self.limit = limit
    }

    var count: Int { slotOf.count }
    var isEmpty: Bool { slotOf.isEmpty }

    subscript(channel: String) -> Primitive? {
        slotOf[channel].map { slots[$0].value }
    }

    /// Channel names — least recently updated first (used by tests and ``absorb(_:)``).
    var channelsOldestFirst: [String] {
        var out: [String] = []
        out.reserveCapacity(count)
        var i = oldest
        while i >= 0 {
            out.append(slots[i].channel)
            i = slots[i].newer
        }
        return out
    }

    /// Folds another cache's entries in the order that cache updated them (oldest first) — used when the inbox merges, in
    /// place, the folds of messages whose callbacks were skipped. Even though only the last accepted value per channel
    /// remains, the result equals folding the messages one by one: the stale-value check is decided by the single latest
    /// timestamp, and recency by the last accepted fold. The only exception is when a value with a backwards timestamp (a
    /// fold that would be rejected as stale) and forgetting fall in the same window — folding one by one puts the stale
    /// value into a forgotten channel anew, while merging rejects it against the newer value still present.
    mutating func absorb(_ other: LatestCache) {
        var i = other.oldest
        while i >= 0 {
            let slot = other.slots[i]
            fold(slot.channel, slot.value, slot.timestamp)
            i = slot.newer
        }
    }

    mutating func fold(_ channel: String, _ value: Primitive, _ timestamp: Int64) {
        if let i = slotOf[channel] {
            // 낡은 값 — 버리고 순서도 그대로 둔다(최근 갱신으로 치지 않는다).
            if slots[i].timestamp > timestamp { return }
            slots[i].value = value
            slots[i].timestamp = timestamp
            unlink(i)
            linkAsNewest(i)
            return
        }
        let i: Int
        if slotOf.count >= limit {
            // 새 채널이 상한을 넘긴다 — 가장 오래 갱신되지 않은 채널을 잊고 그 칸을 다시 쓴다.
            i = oldest
            unlink(i)
            slotOf.removeValue(forKey: slots[i].channel)
            slots[i] = Slot(channel: channel, value: value, timestamp: timestamp, newer: -1, older: -1)
        } else {
            i = slots.count
            slots.append(Slot(channel: channel, value: value, timestamp: timestamp, newer: -1, older: -1))
        }
        slotOf[channel] = i
        linkAsNewest(i)
    }

    mutating func removeAll() {
        slots.removeAll(keepingCapacity: true)
        slotOf.removeAll(keepingCapacity: true)
        newest = -1
        oldest = -1
    }

    private mutating func unlink(_ i: Int) {
        let newer = slots[i].newer
        let older = slots[i].older
        if newer >= 0 { slots[newer].older = older } else { newest = older }
        if older >= 0 { slots[older].newer = newer } else { oldest = newer }
        slots[i].newer = -1
        slots[i].older = -1
    }

    private mutating func linkAsNewest(_ i: Int) {
        slots[i].older = newest
        slots[i].newer = -1
        if newest >= 0 { slots[newest].newer = i }
        newest = i
        if oldest < 0 { oldest = i }
    }
}

/// A single lock-guarded value — for copies that must be read outside the actor without waiting.
final class LockedValue<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Value

    init(_ value: Value) { self.value = value }

    func get() -> Value { lock.withLock { value } }
    func set(_ newValue: Value) { lock.withLock { value = newValue } }
}

// (§6-5)
/// Listener store for holder notices — outside the actor, a synchronous delivery path guarded by a lock.
///
/// Why it does not go through the actor: creating a `Task { await … }` per notice makes actor arrival order unrelated to
/// creation order. This notice is a security gate (a revoke → re-grant reordering = a revoked input reopens), so it must
/// be delivered in order on the socket thread.
final class LeaseHolderListenerBox: @unchecked Sendable {
    private let lock = NSLock()
    private var listeners: [Int: @Sendable ([String: Any]) -> Void] = [:]
    private var nextToken = 0
    private var closed = false

    func add(_ listener: @escaping @Sendable ([String: Any]) -> Void) -> Int {
        lock.withLock {
            // 끝난 클라이언트 — 붙이지 않는다(토큰 0 은 발급되지 않으므로 remove(0) 은 no-op).
            if closed { return 0 }
            nextToken += 1
            listeners[nextToken] = listener
            return nextToken
        }
    }

    func remove(_ token: Int) {
        lock.withLock { _ = listeners.removeValue(forKey: token) }
    }

    /// Session end — releases the listeners and accepts no further add (breaks the cycle of subscription callbacks capturing the client).
    func close() {
        lock.withLock {
            closed = true
            listeners.removeAll()
        }
    }

    func deliver(_ payload: [String: Any]) {
        let snapshot = lock.withLock { Array(listeners.values) }
        for listener in snapshot { listener(payload) }
    }
}
