import Foundation

/// One viewer's connection — the app implements it on top of its own WebRTC library.
/// **The adapter never touches the encoder.**
public protocol BroadcastSession: Sendable {
    /// Applies the viewer's offer and returns the answer SDP.
    func answer(offerSdp: String) async throws -> String

    /// Applies one remote ICE candidate (the JSON as received).
    func addIceCandidate(_ candidate: [String: Any]) async throws

    /// Releases the connection.
    func close() async
}

// 원문: 뷰어가 offer 에 실어 연 `coc.input` 데이터채널의 최소 표면(§7)
/// The minimal surface of the `coc.input` data channel a viewer opens in its offer — a session wraps its own WebRTC
/// channel in this shape and passes it to `wireInput`.
public protocol RemoteInputChannel: Sendable {
    /// The channel label — anything other than `coc.input` is ignored.
    var label: String { get }

    /// Sends an adapter→viewer frame (hello and so on). Throws when the channel is closed — that is how the helper
    /// records a hello send failure in the diagnostics ring and stops re-announcing.
    func send(_ data: String) throws

    /// Subscribes to viewer→adapter text frames.
    func onMessage(_ listener: @escaping @Sendable (String) -> Void)
}

// 원문: 뷰어가 보낸 포인터(§7). 좌표는 0~1 정규화 — **주입은 앱이** 한다(§4-3).
/// A pointer sent by a viewer. Coordinates are normalized to 0–1 — **the app does the injection**.
public struct RemoteInputPointer: Sendable, Equatable {
    /// down | move | up | cancel
    public let t: String
    public let p: Int
    public let x: Double
    public let y: Double

    public init(t: String, p: Int, x: Double, y: Double) {
        self.t = t
        self.p = p
        self.x = x
        self.y = y
    }
}

// 원문의 설계 번호: 보유자 게이트(N4)·방향 화이트리스트(N2)·clamp(N6)·cancel 합성(N7)
/// Opens one viewer's session. Return `nil` when there is no screen to send yet — the viewer offers again.
///
/// - `sendIceCandidate` — publishes a local ICE candidate. A library that bundles candidates into the SDP can simply
///   not call it.
/// - `iceServers` — the server's ICE deployment config: the client's internal lookup result; when it fails or takes
///   over 5 s, the last list received; failing that, the list the client could still return (a still-cached list ·
///   the public STUN list); failing that, an empty list. Put it into your WebRTC configuration as is — without it the
///   peer gathers only host candidates and connects **only within the same NAT**.
/// - `wireInput` — pass it the data channel the viewer opened and the helper takes care of sending hello, the holder
///   gate, the direction whitelist, coordinate clamping and synthesizing cancel.
public typealias BroadcastSessionFactory = @Sendable (
    _ viewerParticipantId: String,
    _ sendIceCandidate: @escaping @Sendable ([String: Any]) -> Void,
    _ iceServers: [IceServer],
    _ wireInput: @escaping @Sendable (RemoteInputChannel) -> Void
) async throws -> BroadcastSession?

/// Screen broadcast — the signalling choreography an adapter needs to put **its own rendered screen** on a stage's
/// Adapter Stream layer.
///
/// It owns the protocol, not the media, and **adds no dependency**: pass a factory that opens one session per viewer,
/// and the helper announces the adapter (`ready`), answers each offer, relays ICE and cleans up when a viewer leaves.
///
/// The screen is **view-only** — one machine renders it and every viewer watches the same frames.
///
/// ## Why it lives in the adapter
///
/// This procedure has traps an app author has no reason to know about, and they fail **silently**:
///
/// 1. **If the adapter does not announce itself, viewers never find it.** The server builds its peer list from the
///    sockets in the room, while an adapter's member is created first over REST and its socket joins later — a viewer
///    that asks in that gap misses us. The notice about a late peer fires only when that peer sends `ready`, so there
///    is no other way to recover. The symptom is "it works after a refresh".
/// 2. **The track must be attached to a stream** (`addTrack(track, stream)`). Attached without one, the answer carries
///    no msid and the viewer's `ontrack` fires with empty streams — negotiation reports success and only the picture
///    is missing. This is the session implementation's (the app's) job, but it is noted here.
///
/// Announcing presence and initiating negotiation are different things — the adapter **only ever answers**.
public actor ScreenBroadcast {
    private let client: ConnectClient
    private let createSession: BroadcastSessionFactory
    private let onError: (@Sendable (Error, String) -> Void)?
    private let onRemoteInput: (@Sendable (RemoteInputPointer, String) -> Void)?

    /// Viewer sessions — keyed by (participantId, receiverId) (``sessionKey(_:_:)``).
    private var sessions: [String: BroadcastSession] = [:]
    /// The generation that created each session (key) — when a session reported through ``sessionClosed(_:)`` is older
    /// than an attempt still in progress, that attempt (generation) is kept. Written and cleared together with ``sessions``.
    private var sessionGenerations: [String: Int] = [:]
    /// The generation that sent its answer, per session (key) — when the receiver cap picks a session to evict, sessions
    /// whose current generation has not sent its answer yet (a negotiation not known to be live) go first.
    private var answeredGenerations: [String: Int] = [:]
    // 같은 뷰어의 offer 가 겹치면 actor 재진입으로 앞선 시도의 세션이 맵에서 덮여 닫히지 않은 채 남았다(실측: offer 100×2 → 열린 세션 200, 맵 1).
    /// The **current negotiation attempt's generation** per session (key). Each offer gets a new value, and an attempt
    /// checks after every await whether its generation is still current — otherwise, when offers from the same viewer
    /// overlap, actor reentrancy lets an earlier attempt's session be overwritten in the map and left open. Values come
    /// from a global monotonic counter, so a viewer that left (its entry removed) and came back never collides with an
    /// old attempt's generation.
    private var generations: [String: Int] = [:]
    private var nextGeneration = 0
    /// The `session key → generation` at the moment of disconnect — compared with the server's `peers` reply after reconnecting (used once, then discarded).
    private var awaySnapshot: [String: Int]?
    /// Whether we are outside `connected` — reconnecting → connecting → reconnecting is **one** disconnect.
    private var away = false
    // 원문: 세션(키)별 열린 포인터 — 보유자 이탈/채널 close 시 cancel 합성의 재료(N7).
    /// Open pointers per session (key) — what cancel is synthesized from when the holder leaves or the channel closes.
    private var openPointers: [String: [Int: (x: Double, y: Double)]] = [:]
    /// Pointers per session (key) for which cancel was synthesized and no new down has arrived yet — move · up · cancel
    /// frames in between are dropped (``handleInputFrame(_:_:_:)``).
    private var cancelledPointers: [String: Set<Int>] = [:]
    // 실기에서 정확히 이 증상이었다. / 한 번이라도 통지가 오면 … 그 뒤로는 통지된 보유자만 받는다(원래 N4).
    /// Whether a holder notice has **ever** arrived from the server — the only signal that tells whether this deployment
    /// uses the control lease.
    ///
    /// Why it is needed: a holder gate that always fails closed makes **remote control impossible on deployments that do
    /// not use leases**. The server sends holder notices only from the registration/lease flow (claim · renew · release),
    /// and the current deployment policy is "Editors are bidirectional immediately" (no registration code, no lease), so
    /// that notice never arrives. The viewer would open the control UI and send pointers, and the adapter would silently
    /// drop them all.
    ///
    /// Before any notice, pointers from a viewer that opened the channel are accepted (a lease-less deployment = the
    /// server and the web already gate by membership and by the control surface); once a notice has arrived, the
    /// deployment uses leases, so from then on only the notified holder is accepted. A holder=null notice still counts as
    /// "a notice arrived".
    private var holderNoticeSeen = false

    /// input-dropped is recorded once per (session, reason) — so a malicious viewer cannot flush the ring. When a session
    /// is cleaned up its keys are removed too (viewer ids change per connection; without removal it grows with the number
    /// of viewers).
    private var droppedNoted: [String: Set<String>] = [:]

    // 원격 입력의 세 분기(채널·hello·폐기)는 이 흔적 없이는 사후에 가릴 수 없다
    // (실사고 2026-09-03).
    private func noteDropped(_ key: String, _ reason: String) {
        if !droppedNoted[key, default: []].insert(reason).inserted { return }
        client.note("input-dropped", "\(reason) viewer=\(ScreenBroadcast.describe(key))", level: "warn")
    }

    /// The key of a viewer session — (participantId, receiverId). The web keeps one receiver per (viewer page × adapter
    /// participant) (two receivers on one page replace each other) — two tabs of one participant have different receiverIds
    /// and therefore separate sessions (separate encoders too). Without a receiverId (older web) the key is the
    /// participantId alone, as before — a new offer from the same participant replaces that session. Sessions,
    /// generations, open pointers, hello re-announcements and input-dropped keys are per session; the holder gate,
    /// peer-left, the peers reconciliation and the viewer passed to app callbacks are per participant. The separator is a
    /// control character that cannot appear in a participantId.
    nonisolated static func sessionKey(_ viewer: String, _ receiverId: String?) -> String {
        guard let receiverId, !receiverId.isEmpty else { return viewer }
        return viewer + keySeparator + receiverId
    }

    private nonisolated static let keySeparator = "\u{1F}"

    /// The participant of a session key.
    nonisolated static func viewerOf(_ key: String) -> String {
        guard let range = key.range(of: keySeparator) else { return key }
        return String(key[..<range.lowerBound])
    }

    /// The receiverId of a session key — nil when absent.
    nonisolated static func receiverOf(_ key: String) -> String? {
        guard let range = key.range(of: keySeparator) else { return nil }
        return String(key[range.upperBound...])
    }

    /// The viewer as written in diagnostic lines — includes the receiverId when there is one.
    nonisolated static func describe(_ key: String) -> String {
        guard let receiver = receiverOf(key) else { return key }
        return "\(viewerOf(key)) receiver=\(receiver)"
    }

    /// The frame's receiverId — absent when empty or not a string (older web).
    nonisolated static func receiverId(_ frame: [String: Any]) -> String? {
        guard let raw = frame["receiverId"] as? String, !raw.isEmpty else { return nil }
        return raw
    }
    private var holderParticipantId: String?
    // 원문: 보유자 통지의 잔여 ttlMs 만료 태스크(N11·N22)
    /// The task that expires the holder notice's remaining ttlMs — if renew notices stop, the helper clears the holder itself.
    private var holderTask: Task<Void, Never>?
    /// The notice generation — the only signal that tells, once the expiry task wakes, **whether an update came in
    /// meanwhile**. Cancellation alone is not enough: a task already queued to enter the actor does not see it.
    private var holderGeneration = 0
    /// The holder notice FIFO — the socket thread (where listeners are called in notice order) pushes, and the single
    /// drain below applies them in order. Handing each notice to its own Task makes arrival at the actor unrelated to
    /// creation order: a grant followed by a revoke could be applied in reverse, leaving the old holder until its ttlMs
    /// ran out.
    private let holderNotices = HolderNoticeInbox()
    /// The remote-input pointer frame FIFO — the data channel thread pushes and ``drainPointerFrames()`` hands the frames on in arrival order.
    private let pointerFrames = PointerFrameInbox()
    /// The FIFO of what comes from the client (signalling frames · state transitions) — the client listener pushes and
    /// ``drainInputs()`` dispatches in arrival order. Nothing is dropped: the protocol bounds the volume (one offer and a
    /// few ICE candidates per negotiation, a few transitions and one peers reply per connection, one peer-left per viewer).
    private let inputs = OrderedInbox<BroadcastInput>()
    private var stopped = false
    /// Whether `stream-undeclared` has already been recorded — once per helper.
    private var undeclaredNoted = false
    // 원문: (… 뷰어는 어댑터를 participantId 로 부른다 — 검수 실측: 재합류 5번 → 죽은 연결 5개)
    /// My participantId as of the last time we became `connected` — when we reconnect with a different one (a new session
    /// through auto-rejoin · the revalidation of a long-lived session), every viewer session from before is orphaned
    /// (viewers address the adapter by participantId).
    private var selfId: String?
    // 원문: (검수 실측: 한 참여자의 receiverId 200개 → 세션 200개)
    /// The maximum number of receivers (sessions) per participant — a fifth receiverId evicts one of that participant's
    /// sessions: first one the helper knows is not live (no stored session yet, or its attempt has not sent its answer),
    /// otherwise the one with the smallest generation. When a PIN or share-link guest refreshes or closes a tab, the old
    /// receiverId's session stays behind, because peer-left comes only from that participant's last socket (team members
    /// get a new participantId per tab, so peer-left does arrive for them).
    static let maxReceiversPerViewer = 4
    // 원문: (여섯 언어 같은 값)
    /// How long (ms) viewer ICE that arrived before an offer is held — the server forwards a guest's offer only after its
    /// database checks but forwards ICE immediately, so candidates can arrive before the offer. They are handed to an
    /// offer that arrives in the meantime when its negotiation id matches.
    static let earlyIceHoldMs: UInt64 = 10_000
    // 원문: (여섯 언어 같은 값)
    /// The maximum number of early ICE candidates queued per session key — on overflow the oldest are dropped first.
    static let earlyIceMaxPerViewer = 200
    // 시한이 없던 판은 첫 조회가 20초 멎으면 그동안 아무 뷰어도 답을 받지 못했다(여섯 언어 같은 값).
    /// The ICE-server lookup timeout (ms) — past it, the answer goes on with the last server list received, else what
    /// the client could still give (a remaining cache or the public STUN list), else an empty list (host candidates only).
    static let iceLookupTimeoutMs: UInt64 = 5_000
    /// The ICE-server lookup timeout (ms), shortened by a test hook.
    internal private(set) var iceLookupTimeoutMsValue: UInt64 = ScreenBroadcast.iceLookupTimeoutMs
    // 원문: (여섯 언어 같은 값) / 상한이 없으면 offer 없이 보낸 후보가 10.5초 뒤에도 키 5051개로 남았다(검수 실측 — 시한도 없었다).
    /// The overall maximum number of keys held without an offer — on overflow the oldest hold is evicted first. The
    /// per-participant limit equals the receiver cap ``maxReceiversPerViewer``. Keys are receiverIds chosen by the viewer,
    /// so without a cap the candidates sent without an offer would pile up without bound.
    static let earlyIceMaxHeldKeys = 64
    /// The pre-offer hold time (ms), shortened by a test hook.
    internal private(set) var earlyIceHoldMsValue: UInt64 = ScreenBroadcast.earlyIceHoldMs
    // 원문: (… 여섯 언어 같은 값)
    /// The window (ms) that coalesces ``sourceChanged()`` — at most one ready per window (one at the start plus one at the end).
    static let sourceChangedWindowMs: UInt64 = 1_000

    /// The comparable shape of a negotiation id — string · number (a number is compared by its textual form).
    enum NegotiationTag: Equatable, Sendable {
        case string(String)
        case number(Double, String)

        // 원문: (TS negotiationSeq 와 같은 판별)
        /// The sequence number — a number's value, or a string's trailing digits. nil when unknown.
        var seq: Double? {
            switch self {
            case .number(let value, _): return value.isFinite ? value : nil
            case .string(let text):
                guard let range = text.range(of: #"\d+$"#, options: .regularExpression) else { return nil }
                return Double(text[range])
            }
        }

        static func == (lhs: NegotiationTag, rhs: NegotiationTag) -> Bool {
            switch (lhs, rhs) {
            case (.string(let a), .string(let b)): return a == b
            case (.number(_, let a), .number(_, let b)): return a == b
            default: return false
            }
        }
    }

    nonisolated static func negotiationTag(_ raw: Any?) -> NegotiationTag? {
        if let text = raw as? String { return .string(text) }
        if let number = raw as? NSNumber, CFGetTypeID(number) != CFBooleanGetTypeID() {
            return .number(number.doubleValue, number.stringValue)
        }
        return nil
    }

    /// A held viewer ICE candidate — kept in arrival order. `seq` is the arrival sequence number (it picks out the oldest even among holds that arrived in the same millisecond).
    struct QueuedIce {
        let candidate: UncheckedSendable<[String: Any]>
        let negotiation: NegotiationTag?
        let at: UInt64
        let seq: UInt64
    }
    /// The candidate arrival sequence number.
    private var iceSeq: UInt64 = 0
    /// The timer that sweeps expired pre-offer holds — runs only while holds exist.
    private var iceHoldTimer: Task<Void, Never>?
    /// Records an episode of hitting the pre-offer key cap only once.
    private var iceHoldCapNoted = false
    /// Session key → the generation of the drainer currently handing over candidates (``drainIce(_:_:_:)``, one per key · generation).
    private var iceDrainers: [String: Int] = [:]

    // 종전에는 세션이 없으면 버려 실브라우저에서 세션마다 후보 100여 개 중 36–50개가 사라졌다.
    /// Early viewer ICE — per session key, candidates that cannot be handed over yet are held in arrival order: the
    /// current attempt's candidates that arrive before the session exists (during the ICE-server lookup · the factory) or
    /// before the session has applied the offer (before ``BroadcastSession/answer(offerSdp:)`` returns), and candidates
    /// that arrive before the offer (the server reordered them). Once the answer is done, the current attempt's
    /// candidates are handed over.
    private var iceQueues: [String: [QueuedIce]] = [:]
    /// Session key → the current attempt's negotiation id (what the offer carried; a nil value when absent).
    private var attemptNegotiation: [String: NegotiationTag?] = [:]
    /// Session key → the generation whose offer the session has applied (answer done) — candidates are handed over only while this generation is current.
    private var remoteReady: [String: Int] = [:]
    /// Queue overflow · candidate-apply failures are recorded only once per (session, episode).
    private var iceOverflowNoted: Set<String> = []
    private var iceFailureNoted: Set<String> = []
    /// The last ICE server list received — used to answer when a lookup exceeds its timeout.
    private var lastIceServers: [IceServer]?
    private var iceLookupNoted = false
    /// The monotonic time (ms) we left `connected` — when we reconnect with a new id, early ICE held before then is dropped.
    private var awaySinceMs: UInt64 = 0

    /// The coalescing window (ms) of ``sourceChanged()`` — at most one ready per window (one at the start + one at the end). Shortened by a test hook.
    internal private(set) var sourceChangedWindowMsValue: UInt64 = ScreenBroadcast.sourceChangedWindowMs
    /// The monotonic time (ns) at which ``sourceChanged()`` last sent ready.
    private var sourceReadyAt: UInt64?
    /// The task for the trailing ready, when called again within the window.
    private var sourceTrailing: Task<Void, Never>?

    private var signalingSub: SignalingSubscription?
    private var holderSub: SignalingSubscription?
    private var stateSub: SignalingSubscription?
    // 종전 배열은 끝난 태스크도 stop() 까지 들고 있었다(실측: 뷰어 20k 입퇴장 → 20000개).
    /// Hello re-announcement tasks per viewer (token → task) — collected on viewer cleanup and stop(); a loop that ends
    /// on its own (send failure = channel closed) removes its own entry.
    private var helloTasks: [String: [Int: Task<Void, Never>]] = [:]
    private var nextHelloToken = 0
    /// Test hook — the re-announcement period (ms). The constant is a let, so tests cannot shorten it.
    internal var helloReannounceMs: Int = Protocol.RemoteInput.helloReannounceMs

    /// Test hook — the only way to shorten the period from outside the actor.
    internal func setHelloReannounceMs(_ ms: Int) { helloReannounceMs = ms }
    /// Test hook — the ICE-server lookup timeout.
    internal func setIceLookupTimeoutMs(_ ms: UInt64) { iceLookupTimeoutMsValue = ms }
    /// Test hook — the coalescing window of ``sourceChanged()``.
    internal func setSourceChangedWindowMs(_ ms: UInt64) { sourceChangedWindowMsValue = ms }
    /// Test hook — the pre-offer hold time.
    internal func setEarlyIceHoldMs(_ ms: UInt64) { earlyIceHoldMsValue = ms }
    /// Test hook — the number of keys holding early ICE and of candidate-apply failure marks, and whether the sweep timer is running.
    internal var iceQueueKeyCount: Int { iceQueues.count }
    internal var iceFailureNotedCount: Int { iceFailureNoted.count }
    internal var iceHoldTimerRunning: Bool { iceHoldTimer != nil }

    /// Test hook — counts whether the per-viewer containers are cleaned up.
    internal var helloTaskCount: Int { helloTasks.values.reduce(0) { $0 + $1.count } }
    internal var droppedNotedCount: Int { droppedNoted.values.reduce(0) { $0 + $1.count } }
    internal var generationCount: Int { generations.count }
    /// Test hook — the holder the gate currently accepts (nil = none).
    internal var currentHolder: String? { holderParticipantId }
    /// Test hook — the number of holder notices applied (excluding those filtered out by the registration check). Lets a test wait for "all applied" regardless of the delivery method.
    internal var appliedHolderNoticeCount: Int { holderGeneration }
    /// Test hook — every received holder notice has been processed (applying is synchronous, so nothing pending means done).
    internal var holderNoticesSettled: Bool { holderNotices.count == 0 }
    /// Test hook — the number of holder notices waiting to be applied (readable even while the actor is busy).
    internal nonisolated var pendingHolderNoticeCount: Int { holderNotices.count }
    /// Test hook — the number of pointer frames waiting to be handed to the app (readable even while the actor is busy).
    internal nonisolated var pendingPointerFrameCount: Int { pointerFrames.count }

    init(
        client: ConnectClient,
        createSession: @escaping BroadcastSessionFactory,
        onError: (@Sendable (Error, String) -> Void)?,
        onRemoteInput: (@Sendable (RemoteInputPointer, String) -> Void)?
    ) {
        self.client = client
        self.createSession = createSession
        self.onError = onError
        self.onRemoteInput = onRemoteInput
    }

    /// The number of viewer sessions currently receiving the screen — one per receiving tab (receiverId), so a person with two tabs open counts as two.
    public var viewerCount: Int { sessions.count }

    func start() async {
        signalingSub = await client.onSignaling { [weak self] frame in
            // 리스너는 클라이언트가 도착 순서대로 부른다 — 그 순서를 여기서 줄로 고정하고, 디스패치는 드레인 하나가 한다.
            // 프레임마다 Task 를 띄우면 같은 뷰어의 offer 두 개가 뒤집혀 **옛 offer 가 이기거나**(낡은 answer),
            // offer 뒤의 peer-left 가 먼저 처리돼 떠난 뷰어의 세션이 남았다.
            self?.enqueueInput(.signaling(frame))
        }
        holderSub = client.onRemoteInputHolder { [weak self] frame in
            // 리스너는 소켓 스레드에서 통지 순서대로 불린다 — 그 순서를 여기서 줄로 고정하고, 적용은 드레인 하나가 한다.
            guard let self else { return }
            // 다른 등록의 통지는 줄에 세우지 않는다 — 적용할 때도 걸러지지만, 세워 두면 넘칠 때 내 등록의 통지를 밀어낸다.
            guard ScreenBroadcast.noticeIsForMe(frame, mine: self.client.currentRegistrationId) else { return }
            let pushed = self.holderNotices.push(frame)
            if pushed.overflowStarted {
                self.client.note(
                    "queue-overflow",
                    "holder notices: \(HolderNoticeInbox.limit) pending — dropping the oldest, the newest notice "
                        + "decides the holder",
                    level: "warn")
            }
            if pushed.drain { Task { await self.drainHolderNotices() } }
        }
        // 재접속마다 다시 알린다 — 1번 함정.
        //
        // ⚠️ 상태 **전이**를 하나도 잃지 않는 내부 리스너를 쓴다. 현재 상태 값을 보는 표면(스냅샷/합쳐지는 흐름)을
        // 구독하면 connected → reconnecting → connected 가 한 창에서 일어날 때 마지막 값만 보이고 그것이 직전 값과
        // 같아 재알림이 발화하지 않는다. `events()` 도 안 된다 — 최근 256개만 쥐어 프레즌스 이벤트가 몰리면 그 사이의
        // 전이를 버린다. 전이는 시그널링과 **같은 줄**에 세워 클라이언트가 알린 순서대로 처리한다.
        stateSub = await client.onStateTransition { [weak self] state in
            self?.enqueueInput(.state(state))
        }
        // 이미 끝난 클라이언트 — 붙을 리스너도, 알릴 곳도 없다.
        if stateSub == nil {
            await shutdown()
            return
        }
        if await client.state == .connected { await noteSelf(snapshot: nil) }
        await announce()
    }

    /// The screen to send appeared or changed after start — do not call it per frame. Re-announces `ready` so that viewers
    /// that got no answer (the session factory returned nil — there was nothing to send yet) offer again. When not
    /// connected, the next `connected` announces anyway. Coalesced: at most one ready per 1-second window — the first right
    /// away, and one more at the end of the window if it was called again within it.
    public func sourceChanged() async {
        if stopped { return }
        guard await client.state == .connected, !stopped else { return }
        // 뒤에 보낼 것이 이미 예약돼 있다 — 이 호출은 그것에 묶인다.
        if sourceTrailing != nil { return }
        let now = DispatchTime.now().uptimeNanoseconds
        let windowNs = sourceChangedWindowMsValue * 1_000_000
        if let last = sourceReadyAt, now - last < windowNs {
            let wait = windowNs - (now - last)
            sourceTrailing = Task { [weak self] in
                try? await Task.sleep(nanoseconds: wait)
                await self?.fireTrailingReady()
            }
            return
        }
        sourceReadyAt = now
        await announce()
    }

    /// The trailing ready at the end of the window — sends the calls made within the window as one.
    private func fireTrailingReady() async {
        sourceTrailing = nil
        if stopped { return }
        sourceReadyAt = DispatchTime.now().uptimeNanoseconds
        guard await client.state == .connected, !stopped else { return }
        await announce()
    }

    /// Tell the helper that this viewer session's connection closed or failed (for example your peer connection
    /// reached `failed` or `closed`). The helper forgets that session — its remote-input state and keepalive go with it —
    /// and calls ``BroadcastSession/close()``, so make that idempotent. Optional: without it a dead session stays until
    /// the viewer offers again, leaves, or is evicted by the per-viewer receiver cap. Sessions are matched by identity, so
    /// the parameter takes a class instance (a value-type session does not compile); unknown or already replaced
    /// sessions are ignored, and a newer offer from the same viewer tab that is still being negotiated is left alone.
    public func sessionClosed(_ session: AnyObject & BroadcastSession) {
        guard let key = sessions.first(where: { ($0.value as AnyObject) === session })?.key else { return }
        // 그 세션만 잊는다 — 같은 키의 더 새 offer 가 진행 중이면(ICE 조회 · 세션 생성을 기다린다) 그 세대는 남긴다(검수 실측:
        // 옛 세션의 종료 알림이 세대를 지워 새 시도가 answer 없이 물러났다).
        if generations[key] != sessionGenerations[key] {
            if let closed = detach(key) { Task { await closed.close() } }
            return
        }
        retireNow(key)
    }

    /// Records my current participantId and, if it differs from the last `connected` one (we came back as a new
    /// participant), closes the viewer sessions **that existed at the moment of disconnect** — viewers addressed the old
    /// participantId, so those sessions cannot be reused. Viewers see the new ready and offer again to the new id.
    ///
    /// The reference is the snapshot taken on leaving `connected` (`snapshot` — ``awaySnapshot`` at the moment `connected`
    /// was handled), and sessions whose generation changed after it are left alone — an offer that arrived after the
    /// disconnect (one that arrived after the server put the socket in the room, before `stage:joined`) and offers after
    /// `connected` are negotiations with the new id, so they survive. My id has to be asked from the client actor, so this
    /// check runs after `connected` is handled.
    private func noteSelf(snapshot: [String: Int]?) async {
        guard let current = await client.me?.participantId, !current.isEmpty, !stopped else { return }
        let previous = selfId
        selfId = current
        guard let previous, previous != current else { return }
        // 옛 id 에 쥐어 둔 이른 ICE(offer 가 아직 없는 키) 가운데 끊기기 전에 온 것도 버린다.
        for key in iceQueues.keys where generations[key] == nil {
            let kept = (iceQueues[key] ?? []).filter { $0.at >= awaySinceMs }
            if kept.isEmpty { dropIceState(key) } else { iceQueues[key] = kept }
        }
        guard let snapshot else { return }
        // 옛 참여자로서의 끊김 스냅샷은 여기서 쓴다 — peers 대조에 다시 쓰지 않는다(그 사이 새로 찍혔으면 그대로 둔다).
        if awaySnapshot == snapshot { awaySnapshot = nil }
        for (key, generation) in snapshot where generations[key] == generation { retireNow(key) }
    }

    /// Stops broadcasting and releases every viewer connection.
    public func stop() async {
        await shutdown()
    }

    /// The body of stop() — when the client ends (the `.ended` transition) the helper stops itself through the same path.
    private func shutdown() async {
        if stopped { return }
        stopped = true
        signalingSub?.cancel()
        holderSub?.cancel()
        stateSub?.cancel()
        holderTask?.cancel()
        holderTask = nil
        sourceTrailing?.cancel()
        sourceTrailing = nil
        for tasks in helloTasks.values {
            for task in tasks.values { task.cancel() }
        }
        helloTasks.removeAll()
        droppedNoted.removeAll()
        generations.removeAll()
        awaySnapshot = nil
        away = false
        let toClose = Array(sessions.values)
        sessions.removeAll()
        sessionGenerations.removeAll()
        iceQueues.removeAll()
        attemptNegotiation.removeAll()
        remoteReady.removeAll()
        iceOverflowNoted.removeAll()
        iceFailureNoted.removeAll()
        iceDrainers.removeAll()
        iceHoldTimer?.cancel()
        iceHoldTimer = nil
        answeredGenerations.removeAll()
        openPointers.removeAll()
        cancelledPointers.removeAll()
        for session in toClose { await session.close() }
    }

    // 원문: 처음 것을 붙들면 `peers` 답 하나가 사라졌을 때(서버의 ready 처리가 Redis 타임아웃 · DB 오류로 실패한다 — 바로 끊김이
    // 잦은 때다) 그 재접속 뒤에 온 뷰어가 영영 대조되지 않았다.
    /// Disconnected — remembers the viewers of this moment (those with a session or a negotiation in progress) and compares
    /// them with the `peers` reply after reconnecting.
    ///
    /// A fresh snapshot is taken on **every transition** out of `connected` (an unused snapshot is overwritten). Holding on
    /// to the first one would mean that when one `peers` reply is lost, viewers that came after that reconnect are never
    /// reconciled. Overwriting is safe — while disconnected the generation map only shrinks (offers arrive over the
    /// socket), and the entries that went away are filtered out by the generation check.
    private func rememberViewers() {
        if stopped || away { return }
        away = true
        awaySnapshot = generations.isEmpty ? nil : generations
        awaySinceMs = ScreenBroadcast.nowMs()
    }


    // 그래서 죽은 세션이 stop() 까지 쌓였다(실측: 재접속 5회 × 뷰어 20 → 열린 세션 100).
    // PIN 게스트는 10초 유예(서버 PPC_GUEST_GRACE_SEC 기본 10 — 2026-10-19 전 서버는 8) 동안 로스터에 남는다.
    /// After a reconnect, compares with the server's `peers` reply (its answer to our ready — the list of **sockets**
    /// currently in the room) and closes **the sessions of viewers that left**.
    ///
    /// The server sends `peer-left` **only once, at the moment** a viewer's socket drops — if this adapter was reconnecting
    /// or rejoining then, it never receives it, and when the stage restarts, viewers come back with a new participantId.
    /// Without this, dead sessions would pile up until stop(). The roster must not be used for this — the stage owner's
    /// member row is not marked left when it drops, and PIN guests stay on the roster through a 10-second grace period.
    ///
    /// Without a snapshot (the reply to the first connect · a snapshot already used) it does nothing. When the reply is
    /// not an array (an older server) no judgment is made. An empty array means "everyone left". Viewers that negotiated
    /// again after the snapshot (their generation changed) and viewers that arrived after the reconnect (not in the
    /// snapshot) are left alone. The remaining viewers' P2P survives an adapter-side blip.
    private func reconcileViewers(_ peerList: Any?) {
        guard let peers = peerList as? [Any] else { return }
        let present = Set(peers.compactMap { ($0 as? [String: Any])?["participantId"] as? String })
        // offer 없이 쥐어 둔 떠난 참여자의 이른 ICE 는 끊김 스냅샷이 없어도 버린다.
        for key in iceQueues.keys where generations[key] == nil && !present.contains(ScreenBroadcast.viewerOf(key)) {
            dropIceState(key)
        }
        guard let snapshot = awaySnapshot else { return }
        awaySnapshot = nil
        // 세션 키의 참여자로 대조한다 — 떠난 참여자의 세션은 receiverId 와 상관없이 모두 닫는다.
        for (key, generation) in snapshot where !present.contains(ScreenBroadcast.viewerOf(key)) {
            guard generations[key] == generation else { continue }
            retireNow(key)
        }
    }

    // ── 시그널링 ────────────────────────────────────────────────────────────

    /// Announces presence — on every connect (reconnects included). This is not the same as initiating negotiation: we
    /// still only answer, and the viewer makes the offer.
    private func announce() async {
        await noteIfUndeclared()
        await send(["kind": "ready"])
    }

    // 원문: 서버가 돌려준 나는 구 서버 · 폴백 합류에서 그 칸이 비어 거짓 경고가 났다(검수).
    /// An adapter that did not declare a screen (`providesView` is not `"stream"`) started broadcasting — the server
    /// neither creates nor binds a screen layer for such an adapter, so this is recorded once in the diagnostics ring
    /// (`stream-undeclared`, once per helper). The judgment uses the value this adapter **requested** in its connect
    /// options, not the `me` the server returned (an older server or a fallback join leaves that field empty).
    private func noteIfUndeclared() async {
        if undeclaredNoted { return }
        let requested = await client.requestedProvidesView()
        guard requested.known, requested.value != "stream" else { return }
        let name = await client.me?.displayName ?? "adapter"
        // 기다리는 사이 다른 알림이 먼저 남겼을 수 있다(액터 재진입).
        if undeclaredNoted { return }
        undeclaredNoted = true
        client.note(
            "stream-undeclared",
            "\(name): the screen broadcast started, but this adapter did not declare providesView 'stream' — "
                + SocketTransport.streamUndeclaredHint,
            level: "warn")
    }

    /// Called by the client listener (on the client actor, in arrival order) — enqueues, and starts a drain if the queue was empty.
    private nonisolated func enqueueInput(_ input: BroadcastInput) {
        if inputs.push(input) { Task { await self.drainInputs() } }
    }

    // 원문: TS `onFrame` 과 같은 꼴
    /// Dispatches pending inputs in arrival order — dispatch is synchronous (no await), so taking an item and updating the
    /// bookkeeping happen in one actor turn, and only the negotiation body (the app's `createSession` · `answer` · `close`)
    /// and sends run separately. It empties the queue in one turn — the protocol bounds the number of inputs (``inputs``)
    /// and one dispatch is a few lines of bookkeeping.
    private func drainInputs() {
        while let input = inputs.pop() {
            switch input {
            case .signaling(let frame): dispatchSignaling(frame)
            case .state(let state): dispatchState(state)
            }
        }
    }

    /// One state transition — a disconnect takes a viewer snapshot, a reconnect sends ready, an end cleans up by itself.
    private func dispatchState(_ state: TransportState) {
        if stopped { return }
        switch state {
        case .connected:
            // 다시 붙었다 — 존재를 알린다. 서버는 이 ready 에 지금 방에 있는 소켓 목록(`peers`)으로 답하고, 떠난 뷰어
            // 정리는 그 답이 왔을 때 한다(``reconcileViewers(_:)``).
            away = false
            // 다른 participantId 로 다시 붙었으면 끊긴 순간의 세션은 고아다 — 알리기 전에 닫는다(스냅샷은 지금 잡는다).
            let snapshot = awaySnapshot
            Task { [weak self] in
                await self?.noteSelf(snapshot: snapshot)
                await self?.announce()
            }
        case .ended:
            // 클라이언트 세션이 끝났다 — 앱이 stop() 을 잊어도 재알림 태스크 · 뷰어 세션이 남아 클라이언트를 붙들지
            // 않게 같은 정리를 돈다.
            Task { [weak self] in await self?.shutdown() }
        default:
            rememberViewers()
        }
    }

    /// The synchronous part of one frame — updates the generation and viewer bookkeeping in arrival order. Does not wait for app code.
    private func dispatchSignaling(_ frame: [String: Any]) {
        if stopped { return }
        switch frame["kind"] as? String {
        case "sdp":
            guard let viewer = frame["fromParticipantId"] as? String, !viewer.isEmpty,
                let description = frame["description"] as? [String: Any],
                // 우리는 항상 answer 하는 쪽이다 — answer 를 받으면 무시한다.
                (description["type"] as? String) == "offer",
                let sdp = description["sdp"] as? String, !sdp.isEmpty
            else { return }
            // 세대는 offer 를 받은 **이 순간** 매긴다 — 뒤에 온 offer 가 항상 이긴다. 협상 본문 안에서 매기면 본문
            // 태스크가 액터에 닿는 순서로 정해져 같은 뷰어의 두 offer 중 옛 것이 이길 수 있었다.
            // 뷰어 페이지의 수신기 id — 한 참여자의 탭마다 다르다. answer · ICE 에 되돌려 주고, 세션을 이것까지로 가른다.
            let key = ScreenBroadcast.sessionKey(viewer, ScreenBroadcast.receiverId(frame))
            // 새 수신기인데 그 참여자의 세션이 이미 상한이면 가장 오래전에 offer 한 세션을 밀어낸다(새로고침 · 닫은 탭의 몫).
            if generations[key] == nil {
                let siblings = generations.filter { ScreenBroadcast.viewerOf($0.key) == viewer }
                // 살아 있다고 알 수 없는 세션(올라간 세션이 아직 없거나 — 첫 협상 중 — 올라간 세션을 만든 세대가 answer 를 아직
                // 보내지 않았다)이 먼저, 그다음 가장 작은 세대. answer 를 보낸 세션은 그 수신기의 재협상이 진행 중이어도
                // 살아 있다.
                let rank: (Dictionary<String, Int>.Element) -> (Int, Int) = { entry in
                    (self.isLive(entry.key) ? 1 : 0, entry.value)
                }
                if siblings.count >= ScreenBroadcast.maxReceiversPerViewer,
                   let victim = siblings.min(by: { rank($0) < rank($1) })?.key {
                    retireNow(victim)
                }
            }
            let superseding = generations[key] != nil || sessions[key] != nil
            // offer 전 쥠의 시한은 이 키가 시도를 얻기 전에 본다(그 뒤로는 지금 시도의 큐라 나이로 버리지 않는다).
            sweepIceHolds(ScreenBroadcast.nowMs())
            nextGeneration &+= 1
            let generation = nextGeneration
            generations[key] = generation
            // 뷰어가 협상 번호를 실었으면 answer 와 그 협상의 ICE 에 그대로 돌려준다 — 뷰어는 다른 협상의 답을 버린다.
            let negotiationId = ScreenBroadcast.negotiationId(frame["negotiationId"])
            // 이른 ICE — 이 시도의 협상 id 를 적고, 쥐어 둔 후보 가운데 이 시도에 넘길 것만 남긴다.
            let tag = ScreenBroadcast.negotiationTag(frame["negotiationId"])
            attemptNegotiation[key] = .some(tag)
            remoteReady.removeValue(forKey: key)
            iceFailureNoted.remove(key)
            retainIceForOffer(key, tag, superseding: superseding)
            scheduleIceHoldSweep() // 이 offer 보다 더 새 협상의 후보는 계속 쥔다(시한)
            Task { [weak self] in
                await self?.answerViewer(key, offerSdp: sdp, generation: generation, negotiationId: negotiationId)
            }
        case "ice":
            // 뷰어 ICE 도 receiverId 를 싣는다 — 그 수신기의 세션에만 붙인다. 세션 · offer 적용이 서기 전이면 쥐었다가 넘긴다
            // 붙이기는 기다리지 않는다: 앱 구현이 매달려도 다음 프레임을 막지 않게.
            guard let viewer = frame["fromParticipantId"] as? String, !viewer.isEmpty,
                let candidate = frame["candidate"] as? [String: Any]
            else { return }
            let key = ScreenBroadcast.sessionKey(viewer, ScreenBroadcast.receiverId(frame))
            let tag = ScreenBroadcast.negotiationTag(frame["negotiationId"])
            // 받은 JSON 그대로 넘긴다 — 이 뒤로 아무도 고치지 않는 값이라 태스크로 넘겨도 경합이 없다.
            let now = ScreenBroadcast.nowMs()
            sweepIceHolds(now)
            iceSeq &+= 1
            let entry = QueuedIce(candidate: UncheckedSendable(candidate), negotiation: tag, at: now, seq: iceSeq)
            if let generation = generations[key] {
                if belongsToAttempt(key, tag) {
                    // 지금 시도의 후보는 모두 큐에 선다(시한 없이, 적용 · 대체 · 잊기까지) — 세션 · offer 적용이 서기 전이면 선
                    // 뒤에, 섰으면 드레이너가 도착 순서대로 넘긴다(드레이너가 없으면 띄운다).
                    enqueueIce(key, entry) // 세션 · offer 적용이 서기 전이다 — 선 뒤에 넘긴다
                    if remoteReady[key] == generation, sessionGenerations[key] == generation, let session = sessions[key],
                       iceDrainers[key] != generation {
                        iceDrainers[key] = generation
                        Task { [weak self] in await self?.drainIce(key, generation, session) }
                    }
                    return
                }
                if let seq = tag?.seq, let current = (attemptNegotiation[key] ?? nil)?.seq, seq < current {
                    return // 더 낡은 협상의 후보 — 버린다
                }
                // 더 새 협상의 것 — 그 offer 를 기다리며 쥔다(offer 전 쥠과 같은 시한).
                enqueueIce(key, entry)
                scheduleIceHoldSweep()
                return
            }
            // 아직 offer 가 없다(서버가 순서를 바꿨다) — 다음 offer 를 기다리며 잠깐 쥔다(시한 · 키 상한).
            holdIce(key, entry)
        case "peer-left":
            // 참여자 단위 — 그 참여자의 세션(탭) 전부를 닫는다.
            if let gone = frame["participantId"] as? String, !gone.isEmpty { retireParticipant(gone) }
        case "peers":
            // 우리 ready 에 대한 답 — 협상을 걸지는 않고(뷰어가 건다) 떠난 뷰어 정리에만 쓴다.
            reconcileViewers(frame["peers"])
        default:
            // peer-joined 는 무시한다 — 협상은 뷰어가 시작한다. 송출자가 먼저
            // 걸면 아직 준비되지 않은 뷰어에게 헛된 협상을 건다.
            break
        }
    }

    /// "Live" for the receiver cap — a stored session exists and the generation that created it has sent its answer.
    private func isLive(_ key: String) -> Bool {
        guard let made = sessionGenerations[key] else { return false }
        return answeredGenerations[key] == made
    }

    /// Whether that attempt is still this viewer's current attempt — not when stopped, when a new offer from the same
    /// viewer has arrived, or when the viewer was cleaned up (entry removed).
    private func isCurrent(_ key: String, _ generation: Int) -> Bool {
        !stopped && generations[key] == generation
    }

    /// The offer's negotiation number — only when it is a string or a number (a boolean or anything else counts as absent, like an older viewer).
    static func negotiationId(_ raw: Any?) -> UncheckedSendable<Any>? {
        if let text = raw as? String { return UncheckedSendable(text) }
        if let number = raw as? NSNumber, CFGetTypeID(number) != CFBooleanGetTypeID() { return UncheckedSendable(number) }
        return nil
    }

    /// The negotiation body of one offer — the dispatch has already assigned the generation (``dispatchSignaling(_:)``).
    private func answerViewer(
        _ key: String, offerSdp: String, generation: Int, negotiationId: UncheckedSendable<Any>? = nil
    ) async {
        // 세션(키)과 참여자 — 세대 · 장부는 세션, 앱 팩토리 · onError · 프레임의 대상은 참여자다.
        let viewer = ScreenBroadcast.viewerOf(key)
        let receiverId = ScreenBroadcast.receiverOf(key)
        // 태스크가 닿기 전에 더 새 offer · peer-left · stop() 이 왔다 — 아무것도 하지 않는다.
        guard isCurrent(key, generation) else { return }
        // 만들었지만 맵에 넣지 못한 세션 — 낡은 시도가 되면 스스로 닫는다. 맵에 넣은 세션은 새 시도의
        // forget · 뷰어 정리 · stop() 이 닫는다(두 번 닫지 않게 구분한다).
        var created: BroadcastSession?
        var stored = false
        do {
            // ICE 조회를 **맨 앞에서** 기다린다(사실상 첫 offer 만 실제 대기) — forget
            // 뒤에 기다리면 같은 뷰어의 재협상 두 건이 겹칠 때 경합 창이 넓어진다.
            let iceServers = await readIceServers()
            guard isCurrent(key, generation) else { return }
            await forget(key) // 재협상 — 이 세션의 이전 연결을 남기지 않는다(같은 참여자의 다른 수신기는 그대로).
            guard isCurrent(key, generation) else { return }

            // `Task` 안의 `self?.` 는 Swift 5.10 이 거부한다(ConnectClient 의 같은 주석 참조).
            let sendIce: @Sendable ([String: Any]) -> Void = { [weak self] candidate in
                guard let self else { return }
                Task { await self.relayIce(key, generation, candidate, negotiationId) }
            }
            let wireInput: @Sendable (RemoteInputChannel) -> Void = { [weak self] channel in
                guard let self else { return }
                Task { await self.wireInputChannel(key, generation, channel) }
            }

            // 보낼 화면이 아직 없으면 답하지 않는다 — 빈 연결을 만들어 두면 뷰어가
            // "연결됨, 화면 없음" 에 갇힌다. 답하지 않으면 뷰어가 다시 건다.
            guard let session = try await createSession(viewer, sendIce, iceServers, wireInput)
            else {
                // 세션 없는 뷰어의 세대 항목을 남기지 않는다(뷰어 수만큼 자라지 않게).
                if isCurrent(key, generation) { await retire(key) }
                return
            }
            created = session

            // 만드는 사이 새 offer · peer-left · stop() 이 왔다 — 이 세션은 이제 아무도 닫지 않는다.
            guard isCurrent(key, generation) else {
                await session.close()
                return
            }
            sessions[key] = session
            sessionGenerations[key] = generation
            stored = true

            let answerSdp = try await session.answer(offerSdp: offerSdp)
            guard isCurrent(key, generation) else { return }
            // 세션이 offer 를 적용했다 — 쥐어 둔 이 시도의 ICE 를 도착 순서대로 넘긴다. 넘기는 동안 · 그 뒤에 오는 후보도 같은
            // 줄(``drainIce(_:_:_:)``)에 선다.
            remoteReady[key] = generation
            iceDrainers[key] = generation
            Task { [weak self] in await self?.drainIce(key, generation, session) }
            answeredGenerations[key] = generation
            var answer: [String: Any] = [
                "kind": "sdp",
                "targetParticipantId": viewer,
                "description": ["type": "answer", "sdp": answerSdp],
            ]
            if let negotiationId { answer["negotiationId"] = negotiationId.value }
            if let receiverId { answer["receiverId"] = receiverId }
            await send(answer)
        } catch {
            // 낡은 시도의 실패는 **새 세션을 건드리지 않는다** — 맵의 세션은 이미 새 시도의 것이다.
            guard isCurrent(key, generation) else {
                if let created, !stored { await created.close() }
                return
            }
            // 한 뷰어의 실패가 다른 뷰어의 화면을 끊지 않는다.
            await retire(key)
            onError?(error, viewer)
        }
    }

    /// Relays a local ICE candidate to the viewer — candidates of a replaced or cleaned-up session are not sent (that session is already closed).
    private func relayIce(
        _ key: String, _ generation: Int, _ candidate: [String: Any], _ negotiationId: UncheckedSendable<Any>? = nil
    ) async {
        guard isCurrent(key, generation) else { return }
        var ice: [String: Any] = [
            "kind": "ice", "targetParticipantId": ScreenBroadcast.viewerOf(key), "candidate": candidate,
        ]
        if let negotiationId { ice["negotiationId"] = negotiationId.value }
        if let receiverId = ScreenBroadcast.receiverOf(key) { ice["receiverId"] = receiverId }
        await send(ice)
    }

    /// The synchronous part of cleaning up one viewer session (key) — removes the session from the map, synthesizes cancel
    /// for its open pointers, stops the hello re-announcement and removes its input-dropped keys. Returns the session to
    /// close (close is app code, so the caller awaits it or spawns it).
    private func detach(_ key: String) -> BroadcastSession? {
        let session = sessions.removeValue(forKey: key)
        remoteReady.removeValue(forKey: key)
        sessionGenerations.removeValue(forKey: key)
        answeredGenerations.removeValue(forKey: key)
        cancelOpenPointers(key)
        cancelledPointers.removeValue(forKey: key)
        droppedNoted.removeValue(forKey: key)
        if let tasks = helloTasks.removeValue(forKey: key) {
            for task in tasks.values { task.cancel() }
        }
        return session
    }

    /// Cleans up one viewer session — renegotiation (a new offer for the same session) also releases the previous connection this way.
    private func forget(_ key: String) async {
        if let session = detach(key) { await session.close() }
    }

    /// The session ended (negotiation failed · no session) — invalidates even the attempt in progress and cleans up.
    /// Because it removes the generation entry, a session being created closes itself as soon as it is done, and a late
    /// channel is ignored.
    private func retire(_ key: String) async {
        generations.removeValue(forKey: key)
        dropIceState(key)
        await forget(key)
    }

    /// ``retire(_:)`` for dispatch — the `peers` reconciliation · peer-left. Updates the bookkeeping now and spawns the
    /// session close (awaiting app code would delay later frames by that much).
    private func retireNow(_ key: String) {
        generations.removeValue(forKey: key)
        dropIceState(key)
        if let session = detach(key) { Task { await session.close() } }
    }

    // ── 이른 뷰어 ICE ────────────────────────────────────────

    nonisolated static func nowMs() -> UInt64 { DispatchTime.now().uptimeNanoseconds / 1_000_000 }

    /// Whether this is a candidate of the current attempt — the negotiation ids match or either is absent.
    private func belongsToAttempt(_ key: String, _ tag: NegotiationTag?) -> Bool {
        guard let tag else { return true }
        guard let current = attemptNegotiation[key] ?? nil else { return true }
        return tag == current
    }

    /// Queues the candidate at the end of that key's queue — per key, the current attempt's candidates and the holds
    /// waiting for an offer are counted separately, and when either exceeds ``earlyIceMaxPerViewer`` its oldest is dropped
    /// first. Age is not checked here (holds waiting for an offer are swept by ``sweepIceHolds(_:)``; the current attempt's
    /// candidates live until applied, superseded or forgotten).
    private func enqueueIce(_ key: String, _ entry: QueuedIce) {
        var queue = iceQueues[key] ?? []
        queue.append(entry)
        // 상한은 지금 시도의 후보와 offer 를 기다리는 쥠을 따로 센다(TS 의 두 큐와 같다) — 다음 협상의 후보가 지금 시도의 것을
        // 밀어내지 않게.
        let hold = isHold(key, entry)
        if queue.filter({ isHold(key, $0) == hold }).count > ScreenBroadcast.earlyIceMaxPerViewer,
           let oldest = queue.firstIndex(where: { isHold(key, $0) == hold }) {
            queue.remove(at: oldest)
            if iceOverflowNoted.insert(key).inserted {
                client.note(
                    "ice-queue",
                    "more than \(ScreenBroadcast.earlyIceMaxPerViewer) early ICE candidates — dropped the oldest viewer="
                        + ScreenBroadcast.describe(key),
                    level: "warn")
            }
        }
        iceQueues[key] = queue
    }

    /// Takes the held candidates of the current attempt (in arrival order) — those of a newer negotiation stay held.
    private func takeIceForAttempt(_ key: String) -> [QueuedIce] {
        guard let queue = iceQueues[key] else { return [] }
        var taken: [QueuedIce] = []
        var rest: [QueuedIce] = []
        for entry in queue {
            if belongsToAttempt(key, entry.negotiation) { taken.append(entry) } else { rest.append(entry) }
        }
        if rest.isEmpty {
            iceQueues.removeValue(forKey: key)
            iceOverflowNoted.remove(key)
        } else {
            iceQueues[key] = rest
        }
        return taken
    }

    /// A new offer arrived — of the held candidates, keeps only those for this attempt: the same negotiation id, no
    /// negotiation id (only when there was no earlier attempt — otherwise they belong to that attempt), and those of a
    /// negotiation newer than this offer (they wait for that offer). Outdated ones are dropped. Age is not checked here —
    /// the caller has already swept expired holds with ``sweepIceHolds(_:)``.
    private func retainIceForOffer(_ key: String, _ tag: NegotiationTag?, superseding: Bool) {
        guard let queue = iceQueues[key] else { return }
        let offerSeq = tag?.seq
        let kept = queue.filter { entry in
            guard let entryTag = entry.negotiation else { return !superseding }
            if tag == nil || entryTag == tag { return true }
            guard let seq = entryTag.seq, let offerSeq else { return false }
            return seq > offerSeq
        }
        if kept.isEmpty {
            iceQueues.removeValue(forKey: key)
            iceOverflowNoted.remove(key)
        } else {
            iceQueues[key] = kept
        }
    }

    /// Drops all early ICE state of that key.
    private func dropIceState(_ key: String) {
        iceQueues.removeValue(forKey: key)
        attemptNegotiation.removeValue(forKey: key)
        remoteReady.removeValue(forKey: key)
        iceOverflowNoted.remove(key)
        iceFailureNoted.remove(key)
        iceDrainers.removeValue(forKey: key)
    }

    /// Holds a candidate without an offer — for a new key, evicts the oldest hold to keep the per-participant
    /// ``maxReceiversPerViewer`` and overall ``earlyIceMaxHeldKeys`` caps (recorded once per episode), and arms the sweep
    /// timer.
    private func holdIce(_ key: String, _ entry: QueuedIce) {
        if iceQueues[key] == nil {
            let viewer = ScreenBroadcast.viewerOf(key)
            let holds = iceQueues.filter { generations[$0.key] == nil }
            let mine = holds.filter { ScreenBroadcast.viewerOf($0.key) == viewer }
            var evicted: String?
            if mine.count >= ScreenBroadcast.maxReceiversPerViewer {
                evicted = mine.min(by: { ($0.value.first?.seq ?? 0) < ($1.value.first?.seq ?? 0) })?.key
            } else if holds.count >= ScreenBroadcast.earlyIceMaxHeldKeys {
                evicted = holds.min(by: { ($0.value.first?.seq ?? 0) < ($1.value.first?.seq ?? 0) })?.key
            }
            if let evicted {
                dropIceState(evicted)
                // 두 상한이 한 문장 · 한 표시를 나눈다(TS 와 같은 문장) — 쥠이 모두 비면 다시 기록할 수 있다.
                if !iceHoldCapNoted {
                    iceHoldCapNoted = true
                    client.note(
                        "ice-queue",
                        "too many receivers waiting for an offer with early ICE (\(ScreenBroadcast.maxReceiversPerViewer) per viewer, "
                            + "\(ScreenBroadcast.earlyIceMaxHeldKeys) in all) — dropped the oldest viewer=\(ScreenBroadcast.describe(evicted))",
                        level: "warn")
                }
            }
        }
        enqueueIce(key, entry)
        scheduleIceHoldSweep()
    }

    /// Whether this is a hold waiting for an offer — a candidate of a key with no attempt, or a candidate of a negotiation
    /// newer than the current attempt (it waits for that offer). Only these expire — the current attempt's candidates live
    /// until applied, superseded or forgotten.
    private func isHold(_ key: String, _ entry: QueuedIce) -> Bool {
        generations[key] == nil || !belongsToAttempt(key, entry.negotiation)
    }

    /// Sweeps the holds waiting for an offer whose ``earlyIceHoldMs`` has passed (the current attempt's candidates are left
    /// alone). When every hold is gone, the key-cap episode ends too (the next cap hit is recorded again).
    private func sweepIceHolds(_ now: UInt64) {
        let hold = earlyIceHoldMsValue
        var anyHold = false
        for key in iceQueues.keys {
            guard var queue = iceQueues[key] else { continue }
            if generations[key] == nil {
                // 쥠은 도착 순서라 앞에서부터 시한이 지난다 — 지난 앞부분만 걷는다.
                let expired = queue.prefix { now &- $0.at >= hold }.count
                if expired == queue.count {
                    dropIceState(key)
                    continue
                }
                if expired > 0 {
                    queue.removeFirst(expired)
                    iceQueues[key] = queue
                }
                anyHold = true
                continue
            }
            // 시도가 있는 키 — 더 새 협상을 기다리는 후보만 시한으로 걷는다.
            queue.removeAll { isHold(key, $0) && now &- $0.at >= hold }
            if queue.isEmpty {
                iceQueues.removeValue(forKey: key)
                iceOverflowNoted.remove(key)
                continue
            }
            iceQueues[key] = queue
            if queue.contains(where: { isHold(key, $0) }) { anyHold = true }
        }
        if !anyHold { iceHoldCapNoted = false }
    }

    /// If anything is held, arms one sweep timer for the earliest expiry (an existing one is kept).
    private func scheduleIceHoldSweep() {
        if stopped || iceHoldTimer != nil { return }
        // 키마다 도착 순서라 첫 쥠이 그 키의 가장 오래된 쥠이다.
        guard let oldest = iceQueues.compactMap({ key, queue in queue.first(where: { isHold(key, $0) })?.at }).min()
        else { return }
        let dueAt = oldest &+ earlyIceHoldMsValue &+ 1
        let now = ScreenBroadcast.nowMs()
        let wait = dueAt > now ? dueAt - now : 1
        iceHoldTimer = Task { [weak self] in
            try? await Task.sleep(nanoseconds: wait * 1_000_000)
            await self?.sweepIceHoldsByTimer()
        }
    }

    private func sweepIceHoldsByTimer() {
        iceHoldTimer = nil
        if stopped { return }
        sweepIceHolds(ScreenBroadcast.nowMs())
        scheduleIceHoldSweep()
    }

    /// Hands the key's current-attempt candidates over one at a time, in arrival order — one drainer per key · generation
    /// (``iceDrainers``). It retires in the same turn it sees the queue empty, so candidates arriving after that start a
    /// new drainer. Candidates arriving while it hands over queue up and do not overtake (tasks spawned per candidate have
    /// no ordering guarantee). It stops when this generation is retired.
    private func drainIce(_ key: String, _ generation: Int, _ session: BroadcastSession) async {
        while true {
            let batch = isCurrent(key, generation) && sessionGenerations[key] == generation ? takeIceForAttempt(key) : []
            if batch.isEmpty {
                if iceDrainers[key] == generation { iceDrainers.removeValue(forKey: key) }
                return
            }
            for entry in batch { await addIceQuietly(key, generation, session, entry.candidate) }
        }
    }

    // 원문: (검수 실측: 떠난 뷰어 100명의 늦은 실패가 진단 링 100칸을 채웠다)
    /// Hands one candidate to the session — a failure is recorded once per (session, generation) and is not fatal
    /// (another path can still connect). Late failures from a session already replaced or forgotten are discarded — so
    /// they neither take the mark nor revive a forgotten key.
    private func addIceQuietly(
        _ key: String, _ generation: Int, _ session: BroadcastSession, _ candidate: UncheckedSendable<[String: Any]>
    ) async {
        do {
            try await session.addIceCandidate(candidate.value)
        } catch {
            guard isCurrent(key, generation), sessionGenerations[key] == generation else { return }
            if iceFailureNoted.insert(key).inserted {
                client.note(
                    "ice-candidate",
                    "adding a viewer ICE candidate failed viewer=\(ScreenBroadcast.describe(key)): \(error)",
                    level: "warn")
            }
        }
    }

    /// The ICE deployment config — asks the client again for every viewer (caching is the client's job). When the lookup
    /// times out or fails, the answer goes on with the last list received (else the list the client still gave, else an
    /// empty list), and `ice-servers` is recorded in the diagnostics ring once per episode (never credentials or TURN
    /// secrets).
    private func readIceServers() async -> [IceServer] {
        let timeoutMs = iceLookupTimeoutMsValue
        let client = self.client
        // 조회와 시한 중 먼저 끝난 쪽 — 구조적 태스크 그룹은 끝나지 않는 조회를 기다리므로 따로 띄운 둘을 한 번만 받는다.
        let outcome: (servers: [IceServer], ok: Bool)? = await withCheckedContinuation { continuation in
            let once = IceLookupOnce()
            Task {
                let outcome = await client.fetchIceServersOutcome()
                if once.claim() { continuation.resume(returning: outcome) }
            }
            Task {
                try? await Task.sleep(nanoseconds: timeoutMs * 1_000_000)
                if once.claim() { continuation.resume(returning: nil) }
            }
        }
        if let outcome, outcome.ok {
            lastIceServers = outcome.servers
            iceLookupNoted = false
            return outcome.servers
        }
        // 마지막으로 받은 목록 → 클라이언트가 실패에도 그래도 준 목록(만료 임박 캐시 · 인증 경로가 실패해 받은 공개 STUN) → 빈
        // 목록(TS 와 같은 차례 · 같은 문장).
        let servers = lastIceServers ?? outcome?.servers ?? []
        if !iceLookupNoted {
            iceLookupNoted = true
            let why = outcome != nil
                ? "the lookup failed"
                : "no answer within \(SocketTransport.seconds(Double(timeoutMs) / 1000)) s"
            let fallback: String
            if let last = lastIceServers {
                fallback = "the last known ICE servers (\(last.count))"
            } else if !servers.isEmpty {
                fallback = "the ICE servers still available (\(servers.count))"
            } else {
                fallback = "no ICE servers (host candidates only)"
            }
            client.note("ice-servers", "ICE server lookup: \(why) — answering with \(fallback)", level: "warn")
        }
        return servers
    }

    /// Calls ``retireNow(_:)`` for every session (tab) of one participant — peer-left is per participant.
    private func retireParticipant(_ viewer: String) {
        var keys = Set(sessions.keys).union(generations.keys).union(openPointers.keys)
        keys.formUnion(helloTasks.keys)
        keys.formUnion(droppedNoted.keys)
        keys.formUnion(cancelledPointers.keys)
        keys.formUnion(iceQueues.keys)
        for key in keys where ScreenBroadcast.viewerOf(key) == viewer { retireNow(key) }
    }

    private func send(_ payload: [String: Any]) async {
        if stopped { return }
        // 시그널링은 best-effort — 협상은 뷰어가 다시 걸어온다.
        await client.sendSignaling(payload)
    }

    // ── 원격 조작 (§6-5·§7) ─────────────────────────────────────────────────

    /// Applies pending holder notices in arrival order — applying is synchronous (no await), so taking and applying happen in one actor turn.
    private func drainHolderNotices() {
        while let notice = holderNotices.pop() { handleHolder(notice) }
    }

    /// Whether the notice is for my registration — only notices for my latest registration are accepted, so that notices
    /// of a **previous registration** left over after re-registering (e.g. the old registration's release=null) do not
    /// disturb the new session's gate. Accepted when we have never registered or the notice carries no registration id.
    nonisolated static func noticeIsForMe(_ frame: [String: Any], mine: String?) -> Bool {
        guard let mine, let noticeReg = frame["registrationId"] as? String, !noticeReg.isEmpty else { return true }
        return noticeReg == mine
    }

    // 원문의 설계 번호: 입력의 유일한 게이트(N4) · cancel 합성(N7)
    /// A control-holder notice — this value is the only gate for input.
    ///
    /// When the holder becomes empty, **cancel is synthesized for all of that viewer's open pointers** — otherwise,
    /// losing control while a finger is down would leave the app pressed forever.
    private func handleHolder(_ frame: [String: Any]) {
        // 등록 id 는 클라이언트 액터를 기다리지 않고 읽는다 — 기다리면 클라이언트가 바쁜 동안(느린 채널 콜백) 회수가
        // 그만큼 늦게 적용돼, 그 사이 도착한 옛 보유자의 포인터가 앱에 닿았다(검수 실측: 회수 뒤 400건 중 144~168).
        // 적재할 때 걸렀어도 다시 본다 — 그 사이 재등록했을 수 있다.
        guard ScreenBroadcast.noticeIsForMe(frame, mine: client.currentRegistrationId) else { return }
        let raw = frame["holderParticipantId"] as? String
        let next = (raw?.isEmpty ?? true) ? nil : raw
        // 통지가 왔다 = 이 배포는 리스를 쓴다. 이 뒤로 게이트는 엄격해진다.
        holderNoticeSeen = true
        let previous = holderParticipantId
        holderTask?.cancel()
        holderTask = nil
        // 통지마다 세대를 올린다 — holder=null 통지도 그 사이 갱신으로 친다.
        holderGeneration &+= 1
        let generation = holderGeneration
        holderParticipantId = next
        if let previous, previous != next { cancelParticipantPointers(previous) }
        if let next {
            // 잔여 ttlMs 를 단조 타이머로 만료한다(N11·N22) — renew 통지가 끊기면
            // (뷰어 증발·pod 크래시) 어댑터 스스로 보유자를 내린다.
            //
            // ⚠️ 상한을 씌운다: Swift 의 `*` 는 오버플로에서 **트랩**(프로세스 abort)이라
            // 와이어 값 하나가 호스트 앱을 죽일 수 있다. 다른 언어도 같은 폭으로 자른다.
            // ttlMs 가 **있을 때만** 만료를 건다 — 부재는 "리스 시간을 모른다" 이지 "이미 끝났다" 가 아니다
            // (서버는 항상 실어 보낸다: 보유 시 잔여 ms, 해제 시 0). 0 으로 접으면 통지 직후 보유자가 내려간다.
            // NSNumber 는 Bool 도 감싼다 — `ttlMs: true` 가 1ms 만료로 읽히지 않게 CFBoolean 은 뺀다(다른 다섯 언어는 무시한다).
            if let num = frame["ttlMs"] as? NSNumber, CFGetTypeID(num) != CFBooleanGetTypeID(),
               case let raw = num.doubleValue, raw.isFinite {
                let ttl = min(max(0, raw), Double(Int32.max))
                holderTask = Task { [weak self] in
                    try? await Task.sleep(nanoseconds: UInt64(ttl) * 1_000_000)
                    if Task.isCancelled { return }
                    await self?.expireHolder(next, generation: generation)
                }
            }
        }
    }

    private func expireHolder(_ expected: String, generation: Int) {
        // 세대가 다르면 그 사이 renew 통지가 왔다는 뜻 — 갱신된 보유자를 내리면 안 된다.
        guard !stopped, generation == holderGeneration, holderParticipantId == expected else {
            return
        }
        holderParticipantId = nil
        cancelParticipantPointers(expected)
    }

    // 원문의 설계 번호: (N7)
    /// Synthesizes cancel for the open pointers of every session of that participant — the holder is per participant.
    private func cancelParticipantPointers(_ viewer: String) {
        for key in openPointers.keys where ScreenBroadcast.viewerOf(key) == viewer { cancelOpenPointers(key) }
    }

    // 원문의 설계 번호: (N7)
    /// Synthesizes cancel for all open pointers of one session (key).
    private func cancelOpenPointers(_ key: String) {
        guard let pointers = openPointers.removeValue(forKey: key) else { return }
        cancelledPointers[key, default: []].formUnion(pointers.keys)
        guard let callback = onRemoteInput else { return }
        let viewer = ScreenBroadcast.viewerOf(key)
        for (p, xy) in pointers {
            callback(RemoteInputPointer(t: "cancel", p: p, x: xy.x, y: xy.y), viewer)
        }
    }

    // 원문의 설계 번호: (§4-3·§6-6)
    /// Takes the coc.input channel a viewer opened and hands its pointers to the app.
    private func wireInputChannel(
        _ key: String, _ generation: Int, _ channel: RemoteInputChannel
    ) async {
        guard onRemoteInput != nil else { return }
        guard channel.label == Protocol.RemoteInput.channel else { return }
        // 낡은 시도의 채널(새 offer 가 왔거나 세션이 이미 정리됐다) — 핸들러도 hello 도 붙이지 않는다.
        guard isCurrent(key, generation) else { return }
        let described = ScreenBroadcast.describe(key)
        client.note("input-channel", "opened viewer=\(described)")

        // 핸들러 부착이 먼저 — hello 송신이 한 번 실패해도(순간적으로 닫힌 채널)
        // 수신까지 죽으면 안 된다. 뷰어는 hello 없이 조작 UI 를 안 열 뿐이다.
        // 프레임은 받은 순서대로 넘긴다 — 프레임마다 Task 를 띄우면 액터 도착 순서가 뒤섞여 앱이 down 보다 up 을,
        // 놓은 뒤의 move 를 받았다(실측: 2만 건에 인접 역전 1813~2945). 쌓이는 양의 상한은 ``PointerFrameInbox``.
        channel.onMessage { [weak self] data in
            guard let self else { return }
            // 인박스의 "뷰어" 는 세션 키다 — 프레임 상한 · 리셋도 세션 단위.
            let pushed = self.pointerFrames.push(viewer: key, generation: generation, data: data)
            if pushed.overflowStarted {
                self.client.note(
                    "queue-overflow",
                    "pointer frames: \(PointerFrameInbox.limit) pending — dropping the oldest move frames "
                        + "(down/up/cancel are kept)",
                    level: "warn")
            }
            if pushed.viewerResetStarted {
                self.client.note(
                    "queue-overflow",
                    "pointer frames: viewer=\(described) has \(PointerFrameInbox.keptLimitPerViewer) down/up/cancel "
                        + "pending — dropped its backlog and cancelled its open pointers",
                    level: "warn")
            }
            if pushed.drain { Task { await self.drainPointerFrames() } }
        }

        let hello: [String: Any] = [
            "t": "hello", "v": Protocol.RemoteInput.version, "accepts": true,
        ]
        guard let json = try? JSONSerialization.data(withJSONObject: hello),
            let text = String(data: json, encoding: .utf8)
        else { return }
        do {
            try channel.send(text)
            // 첫 송신만 기록 — 킵얼라이브를 다 담으면 링이 hello 로 채워진다.
            client.note("hello", "sent viewer=\(described)")
        } catch {
            // 첫 송신 실패로 **포기하지 않는다**: 뷰어는 채널이 열리기 전(announce 시점)에
            // onDataChannel 을 넘기므로 첫 hello 가 닫힌 채널에 걸리는 일이 흔하고, 그때
            // 포기하면 조작 UI 가 영영 안 열린다. 아래 재알림 루프가 복구한다(TS·Dart 동일).
            client.note("hello", "send failed viewer=\(described)", level: "warn")
        }

        // hello 는 한 번이 아니라 **채널이 살아 있는 동안 주기적으로** 다시 알린다.
        // 뷰어의 조작 가능 판정(helloUsable)은 탭이 백그라운드에 다녀오는 것만으로
        // 꺼진 채 남을 수 있고(실측 — P2P 는 멀쩡한데 조작만 잠김) 프로토콜에 재요청
        // 경로가 없다 — 어댑터의 재알림이 유일한 복구 수단이다. 판정은 멱등이라
        // 부작용이 없고, 비용은 10초에 ~40바이트다. 송신이 실패하면(채널 닫힘) 멎는다 —
        // 뷰어가 다시 협상하면 새 채널이 새 태스크를 받는다.
        let period = helloReannounceMs
        nextHelloToken &+= 1
        let token = nextHelloToken
        // 항목 등록은 태스크 생성과 같은 actor 턴 안이다 — 루프가 아무리 빨리 끝나도 endHello 가
        // 등록보다 먼저 돌 수 없다.
        helloTasks[key, default: [:]][token] = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: UInt64(period) * 1_000_000)
                if Task.isCancelled { break }
                if await self?.stopped ?? true { break }
                do { try channel.send(text) } catch { break }
            }
            await self?.endHello(key, token)
        }
    }

    /// Removes the entry of a finished re-announcement loop — if it ended by cancellation, the cleanup side already removed it (no-op).
    private func endHello(_ key: String, _ token: Int) {
        helloTasks[key]?.removeValue(forKey: token)
        if helloTasks[key]?.isEmpty == true { helloTasks.removeValue(forKey: key) }
    }

    /// Hands pending pointer frames over in arrival order — processing is synchronous (no await), so taking and processing
    /// happen in one actor turn. It yields the actor every ``ConnectClient/drainBatch`` frames — so signalling does not
    /// wait for the whole backlog while the app's `onRemoteInput` is slow.
    private func drainPointerFrames() {
        var handled = 0
        while let frame = pointerFrames.pop() {
            // 프레임마다 먼저 밀린 보유자 통지를 적용한다(적용은 동기다) — 회수는 소켓 스레드에 닿은 그 순간부터 게이트에
            // 걸려야 한다. 드레인 묶음이 끝날 때까지 미루면 콜백이 느린 동안 회수 뒤에 도착한 옛 보유자의 프레임이 앱에
            // 닿았다(2차 검수 실측: 콜백 안에서 회수가 도착한 뒤 온 30장 전부, 꾸준한 부하에서 회수 뒤 50장 중 8~41장).
            drainHolderNotices()
            switch frame.item {
            case .pointer(let pointer): handleInputFrame(frame.viewer, frame.generation, pointer)
            case .reset:
                // 그 뷰어의 밀린 프레임을 버렸다 — 이미 앱에 닿은 누름이 눌린 채 남지 않게 닫는다(정리와 같다). 세대는 보지
                // 않는다: 버린 것은 그 뷰어의 **모든** 채널의 프레임(현재 채널의 up 포함)이라 홍수가 낡은 채널에서 왔어도
                // 현재 채널의 열린 포인터를 닫아야 한다. 열린 포인터는 현재 세대의 프레임으로만 쌓인다(낡은 세대의 프레임은
                // 적용 전에 버린다).
                if !stopped { cancelOpenPointers(frame.viewer) }
            }
            handled += 1
            if handled >= ConnectClient.drainBatch {
                // 인박스의 드레인 예약은 그대로다 — 이 턴이 이어 갈 책임을 진다.
                Task { [weak self] in await self?.drainPointerFrames() }
                return
            }
        }
    }

    // 원문의 설계 번호: 형식 검증(N2 · N6 · N7)
    /// One frame — format validation and clamping were done when it was queued (``PointerFrameInbox/parse(_:)``). Here
    /// only the gates at this moment (generation · holder) are checked.
    private func handleInputFrame(_ key: String, _ generation: Int, _ pointer: RemoteInputPointer) {
        if stopped { return }
        // 정리된(또는 재협상으로 대체된) 채널에서 늦게 온 프레임 — 열린 포인터·dropped 키를 되살리지 않는다.
        guard generations[key] == generation else { return }
        guard let callback = onRemoteInput else { return }
        // 보유자 게이트와 앱 콜백은 참여자 단위다.
        let viewer = ScreenBroadcast.viewerOf(key)
        // N4 — 보유자 통지를 받는 배포에서는 통지된 보유자의 입력만 받는다.
        // 통지를 한 번도 받지 않았다면 리스 없는 배포이므로 채널을 연 뷰어를 받는다
        // (근거는 holderNoticeSeen 의 주석).
        if holderNoticeSeen {
            guard let holder = holderParticipantId, viewer == holder else {
                noteDropped(key, "not-holder")
                return
            }
        }

        // cancel 을 합성한 포인터는 다음 down 전까지 끝난 것이다 — 그 사이의 move · up · cancel 은 버린다(up · cancel 은
        // 그 제스처의 끝이라 표시를 푼다). 홍수 리셋은 세대와 상관없이 뷰어의 열린 포인터를 닫으므로, 아무것도 버려지지 않은
        // 현재 채널의 드래그도 끊길 수 있다 — 그 뒤 move · up 이 down 없이 앱에 닿으면 앱의 포인터 상태가 어긋났다
        // (3차 검수: down5 · cancel5 · move5 · move5 · up5). 리셋 표시에 실제로 버린 포인터만 싣는 길은 택하지 않았다:
        // 버린 down 뒤의 move · up 도 같은 고아가 되고, cancel 을 합성한 까닭(보유자 교대 · 만료 · 홍수)과 상관없이 "합성한
        // cancel 뒤에는 새 down 부터" 라는 한 규칙이 앱에 일관된 포인터 열을 준다.
        if var cancelled = cancelledPointers[key], cancelled.contains(pointer.p) {
            let gestureEnds = pointer.t != "move"
            if gestureEnds {
                cancelled.remove(pointer.p)
                cancelledPointers[key] = cancelled.isEmpty ? nil : cancelled
            }
            if pointer.t != "down" { return }
        }

        // 열린 포인터 추적 — 보유자 이탈/채널 close 시 cancel 합성의 재료(N7).
        if pointer.t == "down" || pointer.t == "move" {
            openPointers[key, default: [:]][pointer.p] = (x: pointer.x, y: pointer.y)
        } else {
            openPointers[key]?.removeValue(forKey: pointer.p)
        }
        callback(pointer, viewer)
    }
}

// 원문: 리스 흐름(claim · renew · release — 웹 뷰어의 `operation-lease.ts` 는 ttl 의 절반마다 renew, 실패 재시도는 2초 간격)
/// Holder notice FIFO — the socket thread pushes in order and ``ScreenBroadcast``'s drain takes them out in order.
///
/// Capped at ``limit`` — bounds what piles up while applying is stalled (the helper actor is held by an app callback).
/// Notices come only from the lease flow (claim · renew · release), so normally there are no more than a few. On
/// overflow the **oldest** notice is dropped. Only notices for my registration are queued (those of other
/// registrations are filtered out before queuing — ``ScreenBroadcast/noticeIsForMe(_:mine:)``), so every remaining
/// notice is applied and the last of them decides the holder — only intermediate states are lost. Diagnostics: one
/// line per backlog (until it empties and overflows again).
final class HolderNoticeInbox: @unchecked Sendable {
    static let limit = 256
    private let lock = NSLock()
    private var notices = FIFO<[String: Any]>()
    private var drainScheduled = false
    /// Has overflowed since it was last empty — only one line per backlog goes to the diagnostics ring. Cleared only when the queue empties.
    private var overflowing = false

    var count: Int { lock.withLock { notices.count } }

    /// Enqueue — `drain`: whether a new drain must be started; `overflowStarted`: whether this push started an overflow.
    func push(_ notice: [String: Any]) -> (drain: Bool, overflowStarted: Bool) {
        lock.withLock {
            notices.append(notice)
            var started = false
            if notices.count > HolderNoticeInbox.limit {
                _ = notices.popFirst()
                started = !overflowing
                overflowing = true
            }
            let drain = !drainScheduled
            drainScheduled = true
            return (drain, started)
        }
    }

    /// The next notice — when empty, returns nil and clears the drain schedule and the overflow mark.
    func pop() -> [String: Any]? {
        lock.withLock {
            guard let notice = notices.popFirst() else {
                drainScheduled = false
                overflowing = false
                return nil
            }
            return notice
        }
    }
}

// 원문: 검증을 꺼낼 때 하면 콜백이 느린 동안 엉터리 프레임이 상한 없이 쌓였다(검수 실측: `p:-1` 인 down 10만 건이 그대로 대기).
// 원문의 설계 번호: N2 방향 화이트리스트 · N6 좌표 · N7 포인터 id
/// Remote-input pointer frame FIFO — the data channel thread pushes in arrival order and ``ScreenBroadcast``'s drain
/// takes them out in that order (one queue across channels, so the order within each channel is kept too).
///
/// Format validation happens **on enqueue** (``parse(_:)`` — direction whitelist · coordinates · pointer id). Frames
/// that would be dropped anyway never queue up. The generation and holder gates are checked on dequeue, against the
/// state at the moment of applying.
///
/// Two caps — they bound what piles up while the app's `onRemoteInput` is slow.
/// - Overall ``limit``: on overflow the **oldest move** is dropped first. A move is a continuous value the next move replaces.
/// - Per viewer ``keptLimitPerViewer``: down · up · cancel are never dropped by the overall cap — losing even one leaves
///   the app pressed (a lost release) or delivers a move to a press that does not exist. Instead, when one viewer has
///   more of those three pending than this (at human tap speed the callback would have to stall for a long time to
///   reach it — beyond it is a flood), that viewer's pending frames are dropped **all at once** across all its
///   channels, and a mark that closes its open pointers takes their place (the same cancel synthesis as cleanup —
///   regardless of generation: the current channel's release may have been dropped too). It counts per viewer because
///   the channels all belong to the same participant, and a cap that started over with every renegotiation would bound
///   nothing. So what piles up never exceeds 1000 moves + 256 per viewer.
///
/// Held in two queues (kept frames · droppable moves) — arrival order is kept by an enqueue sequence number. The drain
/// schedule is signalled only once, on the first push into an empty inbox.
final class PointerFrameInbox: @unchecked Sendable {
    static let limit = 1000
    static let keptLimitPerViewer = 256
    /// Frames the overall cap never drops — press · release · cancel.
    static let kept: Set<String> = ["down", "up", "cancel"]

    enum Item {
        case pointer(RemoteInputPointer)
        /// That viewer's pending frames were dropped — close its open pointers with cancel.
        case reset
    }

    struct Frame {
        let viewer: String
        let generation: Int
        let item: Item
    }

    struct Pushed {
        var drain = false
        var overflowStarted = false
        var viewerResetStarted = false
    }

    private let lock = NSLock()
    private var nextSeq: UInt64 = 0
    private var keptFrames = FIFO<(seq: UInt64, frame: Frame)>()
    private var droppableFrames = FIFO<(seq: UInt64, frame: Frame)>()
    /// Pending down · up · cancel count per viewer.
    private var keptPerViewer: [String: Int] = [:]
    private var drainScheduled = false
    /// Has overflowed since it was last empty · viewers whose pending frames were dropped — only one line per backlog goes to the diagnostics ring.
    private var overflowing = false
    private var resetViewers: Set<String> = []

    var count: Int { lock.withLock { keptFrames.count + droppableFrames.count } }

    /// Format validation of one frame — the clamped pointer when it passes, otherwise nil (dropped).
    static func parse(_ data: String) -> RemoteInputPointer? {
        guard let raw = data.data(using: .utf8),
            let msg = (try? JSONSerialization.jsonObject(with: raw)) as? [String: Any]
        else { return nil }
        // N2 — 방향 화이트리스트(어댑터는 포인터만 수신).
        guard let t = msg["t"] as? String, Protocol.RemoteInput.adapterAccepts.contains(t) else { return nil }
        // N6 — 좌표 검증/clamp. 비수치·NaN·Infinity 는 폐기.
        guard let xNum = msg["x"] as? NSNumber, let yNum = msg["y"] as? NSNumber else { return nil }
        let x = xNum.doubleValue
        let y = yNum.doubleValue
        guard x.isFinite, y.isFinite else { return nil }
        // N7 — 포인터 id 는 정수·[0,65535] 만. 보유자는 원격의 사람이다 — 악의적
        // p(음수·거대수·비정수)가 예외로 콜백을 죽이지 못하게 거른다.
        guard let pNum = msg["p"] as? NSNumber else { return nil }
        let pRaw = pNum.doubleValue
        guard pRaw.isFinite, pRaw == pRaw.rounded(.down), pRaw >= 0, pRaw <= 65535 else { return nil }
        return RemoteInputPointer(t: t, p: Int(pRaw), x: min(max(x, 0), 1), y: min(max(y, 0), 1))
    }

    /// Enqueue — parsing and validation happen once, here (on the data channel thread).
    func push(viewer: String, generation: Int, data: String) -> Pushed {
        guard let pointer = PointerFrameInbox.parse(data) else { return Pushed() }
        let frame = Frame(viewer: viewer, generation: generation, item: .pointer(pointer))
        return lock.withLock {
            var pushed = Pushed()
            nextSeq &+= 1
            if PointerFrameInbox.kept.contains(pointer.t) {
                let pending = (keptPerViewer[viewer] ?? 0) + 1
                if pending > PointerFrameInbox.keptLimitPerViewer {
                    // 홍수 — 이 프레임까지 그 뷰어의 밀린 것을 버리고, 열린 포인터를 닫는 표시를 세운다(앞선 표시도 이것이
                    // 대신한다).
                    keptFrames.removeAll { $0.frame.viewer == viewer }
                    droppableFrames.removeAll { $0.frame.viewer == viewer }
                    keptPerViewer[viewer] = nil
                    keptFrames.append((nextSeq, Frame(viewer: viewer, generation: generation, item: .reset)))
                    pushed.viewerResetStarted = resetViewers.insert(viewer).inserted
                } else {
                    keptPerViewer[viewer] = pending
                    keptFrames.append((nextSeq, frame))
                }
            } else {
                droppableFrames.append((nextSeq, frame))
            }
            if keptFrames.count + droppableFrames.count > PointerFrameInbox.limit, droppableFrames.popFirst() != nil {
                pushed.overflowStarted = !overflowing
                overflowing = true
            }
            pushed.drain = !drainScheduled
            drainScheduled = true
            return pushed
        }
    }

    /// The next frame — whichever head of the two queues came in first. When empty, returns nil and clears the drain schedule and the overflow marks.
    func pop() -> Frame? {
        lock.withLock {
            let fromKept: Bool
            switch (keptFrames.first?.seq, droppableFrames.first?.seq) {
            case (nil, nil):
                drainScheduled = false
                overflowing = false
                resetViewers.removeAll()
                return nil
            case (.some(let keptSeq), .some(let droppableSeq)): fromKept = keptSeq < droppableSeq
            case (.some, nil): fromKept = true
            case (nil, .some): fromKept = false
            }
            guard fromKept else { return droppableFrames.popFirst()?.frame }
            guard let next = keptFrames.popFirst()?.frame else { return nil }
            if case .pointer = next.item, let pending = keptPerViewer[next.viewer] {
                keptPerViewer[next.viewer] = pending > 1 ? pending - 1 : nil
            }
            return next
        }
    }
}

/// A box for passing, across task boundaries, a value that is built once and never changed — only for values such as
/// received JSON (`[String: Any]`) whose type is not Sendable but which have no shared mutation.
struct UncheckedSendable<Value>: @unchecked Sendable {
    let value: Value
    init(_ value: Value) { self.value = value }
}

/// What ``ScreenBroadcast`` receives from the client — signalling frames and state transitions, in one queue.
enum BroadcastInput {
    case signaling([String: Any])
    case state(TransportState)
}

/// An unbounded ordered FIFO — for flows that need not drop anything (the protocol bounds the volume). Pushers fix the
/// order under the lock, and the drain schedule is signalled only once, on the first push into an empty inbox.
final class OrderedInbox<Element>: @unchecked Sendable {
    private let lock = NSLock()
    private var items = FIFO<Element>()
    private var drainScheduled = false

    var count: Int { lock.withLock { items.count } }

    /// Enqueue — true when a new drain must be started.
    func push(_ item: Element) -> Bool {
        lock.withLock {
            items.append(item)
            defer { drainScheduled = true }
            return !drainScheduled
        }
    }

    /// The next item — when empty, returns nil and clears the drain schedule.
    func pop() -> Element? {
        lock.withLock {
            guard let item = items.popFirst() else {
                drainScheduled = false
                return nil
            }
            return item
        }
    }
}

// 원문의 설계 번호: onRemoteInput … **주입은 앱 몫**(§4-3)
/// Experimental: this API may change in a minor release before 1.0.
///
/// Starts a screen broadcast — see ``ScreenBroadcast``.
///
/// - Parameters:
///   - createSession: A factory that opens one session per viewer (the app injects the media).
///   - onError: Per-viewer negotiation failures (with the viewer id). One viewer's failure does not cut off the others.
///   - onRemoteInput: Remote-control pointers — **injecting them is the app's job**. Omitted = remote control is not accepted.
public func startScreenBroadcast(
    client: ConnectClient,
    createSession: @escaping BroadcastSessionFactory,
    onError: (@Sendable (Error, String) -> Void)? = nil,
    onRemoteInput: (@Sendable (RemoteInputPointer, String) -> Void)? = nil
) async -> ScreenBroadcast {
    let broadcast = ScreenBroadcast(
        client: client, createSession: createSession,
        onError: onError, onRemoteInput: onRemoteInput)
    await broadcast.start()
    return broadcast
}

/// A flag that wins only once — only whichever of the ICE-server lookup and the timeout finishes first goes on.
final class IceLookupOnce: @unchecked Sendable {
    private let lock = NSLock()
    private var used = false

    func claim() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        if used { return false }
        used = true
        return true
    }
}
