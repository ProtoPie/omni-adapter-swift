import Foundation
// socket.io-client-swift 는 Sendable 표기가 없다 — SocketIOClient 는 자체 handleQueue 에서만 만지므로 경고만 끈다.
@preconcurrency import SocketIO

/// Transport settings — ``connect(_:)`` fills them in from the credential.
public struct SocketTransportOptions: Sendable {
    public var url: String
    public var stageId: String
    public var secretKey: String
    public var displayName: String = "Omni adapter"
    public var osHostname: String?
    public var receives: [String]?
    public var sends: [String]?
    public var providesView: String?
    // 이름 하나당 활성 하나 opt-in — 서버가 활성/대기를 판정한다(T4).
    /// Opt in to one active instance per name — the server judges which instance is active and which are standby.
    public var exclusiveName: Bool?
    public var strictRouting = false
    public var socketPath: String?
    public var reconnection = true
    /// Reconnect delay — initial / maximum (seconds, default 0.5 / 30, growing exponentially). This, `joinTimeout`, `rejoinDelay`
    /// and `rejoinDelayMax` follow the same rule as `ackTimeout` — anything else is a `ConfigError` (``SocketTransport/open(handlers:)``).
    public var reconnectDelay: TimeInterval = 0.5
    public var reconnectDelayMax: TimeInterval = 30
    /// Whether to **wait for the session to reopen and join it automatically** when it closes (default true).
    ///
    /// This is a different layer from `reconnection`: reconnection handles "the server is alive, only the socket dropped",
    /// this handles "the host closed the stage" (the session itself is gone). The latter needs a new guest JWT, so it
    /// starts over from the REST validate. While waiting, the state is `.reconnecting` and publishes pile up in the
    /// offline queue. If waiting cannot fix the failure — a revoked adapter key (`AuthRevoked`), a protocol version the
    /// server does not accept (`ProtocolVersionMismatch`, `ClientAheadOfServer`), or a rejected configuration or
    /// request (`ConfigError`, `ValidationError`) — it does not wait and ends with `.ended` right away.
    public var autoRejoin = true
    /// Session-resume check interval — initial / maximum (default 2 s / 60 s, growing exponentially).
    public var rejoinDelay: TimeInterval = 2
    public var rejoinDelayMax: TimeInterval = 60
    public var joinTimeout: TimeInterval = 15
    /// How long to wait for an ack (seconds, default 15) — finite, greater than 0 and at most 2^31−1 ms (2147483.647 s). Anything else is a `ConfigError` (``SocketTransport/open(handlers:)``).
    public var ackTimeout: TimeInterval = 15

    /// Forces polling on networks where WebSocket is blocked.
    public var usePolling = false

    public init(url: String, stageId: String, secretKey: String) {
        self.url = url
        self.stageId = stageId
        self.secretKey = secretKey
    }
}

// 다른 언어 어댑터의 `socket-transport` 미러. (재접속 직접 관리는) 다른 어댑터와 같은 구조.
/// External profile transport — connects directly to Socket.IO `/stage`.
///
/// It takes the proven path (the same as the native Player, web guests and other adapters):
///   ① REST `POST /api/stages/:id/token/validate` → guest JWT
///   ② Socket.IO `/stage` handshake (auth: token/stageId/protocolVersion/clientType)
///   ③ The server joins automatically from the JWT claims → the `stage:joined` push is the single authority on joining
///   ④ `stage:message` in both directions + per-message ack
///
/// ⚠️ SocketIO-Client-Swift is **not thread-safe** — every socket access goes through a dedicated
/// serial queue (`handleQueue`), and adapter state is touched only on that queue.
/// ⚠️ Reconnection is also **managed here** (the library's automatic reconnection is off), because every attempt
/// must revalidate over REST and carry the new JWT in the handshake.
public final class SocketTransport: Transport, @unchecked Sendable {
    private static let namespace = "/stage"
    private static let joinedFallback: TimeInterval = 4

    private let options: SocketTransportOptions
    private let queue = DispatchQueue(label: "com.studioxid.omniadapter.socket")
    private let session: URLSession

    private var manager: SocketManager?
    private var socket: SocketIOClient?
    private var handlers = TransportHandlers()

    /// Test-only — installs handlers to verify only the session-end branches without a socket.
    /// On the real path ``open(handlers:)`` fills them in.
    internal func setHandlersForTest(_ h: TransportHandlers) {
        queue.sync { self.handlers = h }
    }
    private var auth: [String: String] = [:]
    private var participantId = ""
    private var everJoined = false
    private var ended = false
    private var opened = false
    private var terminalError: AdapterError?
    private var joinedContinuation: CheckedContinuation<Joined, Error>?
    private var fallbackWork: DispatchWorkItem?
    private var reconnectTask: Task<Void, Never>?
    /// Waiting for the session to resume — the interval and give-up rules differ from reconnecting a dropped socket.
    private var rejoining = false
    /// An adapter with autoRejoin off got an Unauthorized handshake after joining and is revalidating **once** —
    /// if that revalidation fails, it ends without waiting (`reauth-failed`).
    private var reauthOnly = false
    /// That one revalidation succeeded and it is connecting with the **new token** — if even this token is rejected as
    /// Unauthorized, it ends without revalidating again (cleared once it joins).
    private var reauthRefreshed = false
    /// The last `stage:error` — when the server disconnects before the join, its reason (the key gate's `adapter key revoked`)
    /// goes into the open() error message.
    private var lastServerError: AdapterError?
    // (늦게 온 성공이 살아 있는 JWT 를 덮어써, 다음 재접속이 다른 참여자로 인증됐다.)
    /// Credential generation — goes up every time a revalidation result is committed. If another revalidation committed
    /// first after this one started, or the transport has ended, that result is discarded.
    private var credentialEpoch = 0
    /// Join attempt numbers — the last attempt started · the attempt registered in the queue · the attempt now waiting for the join (0 = none).
    private var joinAttempt = 0
    private var registeredAttempt = 0
    private var waitingAttempt = 0
    /// close() is waiting for `stage:leave` to be processed — a server disconnect or session end in between is taken as the answer to the leave.
    private var closing = false
    /// Own member from the last join (only on `queue`) — used for the ensure's renamed judgment.
    private var joinedMe: Member?
    /// The close in progress (or finished) — concurrent close() calls all wait for it.
    private var closeTask: Task<Void, Never>?
    /// The client's registration id store (handed over by ``ConnectClient``) — only this transport writes it, in the queue turn that received the registration ack.
    private var registrationMirror: LockedValue<String?>?

    /// The client hands over its copy of the registration id — ``registerAck(event:payload:wantSecret:)`` updates it where the ack is received.
    internal func attachRegistrationMirror(_ mirror: LockedValue<String?>) {
        queue.sync { registrationMirror = mirror }
    }
    /// Wakes the leave wait (when the server disconnected without an ack).
    private var leaveWaiter: (@Sendable () -> Void)?
    /// An attempt that timed out or was cancelled before it was registered — it fails with this error as soon as it registers.
    private var abandonedAttempt: (attempt: Int, error: AdapterError)?

    // 종전에는 `URLSession.shared` 의 쿠키 저장소가 프로세스 전체에 하나라, 한 프로세스의 두 클라이언트가 같은 쿠키를 보내
    // 서버에서 **같은 참여자**가 됐다(실측: 두 클라이언트의 `me` 가 같은 id).
    /// The guest cookie the server set in the validate response (`pp_stage_guest_<stageId>=…`) — sent back on the next validate (rejoin).
    ///
    /// When this cookie arrives, the server readmits with the **same participantId** (without it, a new id). Each transport
    /// carries its own cookie and does not use the session's cookie store — its own revalidations come back with the same id,
    /// and it never mixes with another client (even when the app passes in a session). Only on `queue`.
    private var guestCookie: String?
    static let guestCookiePrefix = "pp_stage_guest_"

    public init(options: SocketTransportOptions, session: URLSession = .shared) {
        self.options = options
        self.session = session
    }

    // ── ① REST — adapter key → guest JWT ────────────────────────────────────

    /// - Parameter serverErrorAsNotConnected: Turned on only for the ensure's 401 recovery — a 5xx on revalidation is something to retry (NotConnected).
    ///   The connect path's mapping (`mapValidateError`) stays as is.
    private func validate(serverErrorAsNotConnected: Bool = false) async throws {
        let epoch = queue.sync { credentialEpoch }
        let base = trimTrailingSlash(options.url)
        guard let endpoint = URL(
            string: "\(base)/api/stages/\(options.stageId.addingPercentEncoding(withAllowedCharacters: .alphanumerics) ?? options.stageId)/token/validate"
        ) else {
            throw AdapterError("ConfigError", "could not build the validate endpoint from url \(base)")
        }

        var body: [String: Any] = [
            "token": options.secretKey,
            "displayName": options.displayName,
            "clientType": Protocol.adapterClientType, // Backstage connect-client 노드 판별자
            // 상시 전송(boolean 확정) — preserve-on-omit 이 옛 true 를 되살리는 footgun 방지.
            "strictRouting": options.strictRouting,
        ]
        // 공백뿐인 값은 빼야 한다 — 서버 zod 가 trim().min(1) 이라 ""/" " 는 400
        // (ValidationError = 자동 재조인 포기 코드 → 어댑터가 영영 못 붙는다).
        if let osHostname = options.osHostname,
            !osHostname.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        {
            // 서버 상한은 JS 문자열 길이(UTF-16 코드 유닛) 기준 — grapheme 로 자르면
            // 이모지/결합문자에서 서버 400 이 난다.
            body["osHostname"] = String(decoding: osHostname.utf16.prefix(128), as: UTF16.self)
        }
        if let receives = options.receives, !receives.isEmpty { body["receives"] = receives }
        if let sends = options.sends, !sends.isEmpty { body["sends"] = sends }
        if let providesView = options.providesView { body["providesView"] = providesView }
        // false 도 실어 보낸다 — 규칙을 **끄는** 재신고가 여기서 걸리면 서버에 남은
        // 이전 true 때문에 어댑터가 영원히 대기로 판정된다.
        if let exclusiveName = options.exclusiveName { body["exclusiveName"] = exclusiveName }
        // 어댑터 신원(ADR-0016 · D4) — 언어 · 라이브러리 판 · 런타임 종류와 판. 옵션이 아니라 언제나 싣는다(알아내지
        // 못한 필드만 빠진다). 서버는 키 입장에서만 사용 장부에 남기고, 구 서버는 unknown key strip.
        for (key, value) in AdapterIdentity.fields() { body[key] = value }

        var request = URLRequest(url: endpoint)
        request.httpMethod = "POST"
        request.timeoutInterval = options.joinTimeout
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONSerialization.data(withJSONObject: body)
        // 게스트 쿠키는 이 트랜스포트가 직접 들고 다닌다(``guestCookie``) — 세션의 쿠키 저장소를 쓰지 않는다.
        request.httpShouldHandleCookies = false
        if let cookie = queue.sync(execute: { guestCookie }) {
            request.setValue(cookie, forHTTPHeaderField: "Cookie")
        }

        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await session.data(for: request)
        } catch {
            // 부른 쪽의 취소(ensure 의 시한 · 취소 아래서 부를 때)는 취소로 전한다 — 끊김으로 바꾸지 않는다(검수 실측: 401 회복 중의
            // 취소가 NotConnected "취소됨" 으로 바뀌었다).
            if Task.isCancelled || error is CancellationError { throw CancellationError() }
            throw AdapterError(
                "NotConnected",
                "token/validate request failed — is the server reachable at \(base)? "
                + "(\(error.localizedDescription))"
            )
        }

        let status = (response as? HTTPURLResponse)?.statusCode ?? 0
        let payload = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
        if status < 400, let http = response as? HTTPURLResponse {
            let fields = http.allHeaderFields.reduce(into: [String: String]()) { out, pair in
                if let key = pair.key as? String, let value = pair.value as? String { out[key] = value }
            }
            if let cookie = HTTPCookie.cookies(withResponseHeaderFields: fields, for: endpoint)
                .first(where: { $0.name.hasPrefix(SocketTransport.guestCookiePrefix) })
            {
                queue.sync { guestCookie = "\(cookie.name)=\(cookie.value)" }
            }
        }

        if status >= 500 && serverErrorAsNotConnected {
            throw AdapterError(
                "NotConnected",
                "token/validate failed on the server (\(status))"
                    + ((payload?["message"] as? String).map { ": \($0)" } ?? ""))
        }
        if status >= 400 {
            // 서버 봉투(error-handler.ts)는 코드를 `error` 에 싣는다 — `code` 는 구버전·
            // 테스트 호환 폴백. 놓치면 409 StageFull 이 'Unauthorized' 로 둔갑한다.
            let mapped = SocketTransport.mapValidateError(
                status: status,
                code: (payload?["error"] as? String) ?? (payload?["code"] as? String),
                base: base,
                details: payload?["details"],
                message: payload?["message"] as? String
            )
            // 길이가 46 이 아닌 `ck_` 의 401 — 서버는 그 토큰을 share 토큰으로 보고 거절했다(길이로 가른다):
            // "회수됨" 이 아니라 "잘림" 이 맞는 진단이다(adapterKeyLengthRejection — doctor 도 같은 판정). 코드는 그대로 AuthRevoked.
            throw SocketTransport.adapterKeyLengthRejection(mapped, status: status, keyLength: options.secretKey.count)
        }

        guard let jwt = payload?["jwt"] as? String,
              let participantId = payload?["participantId"] as? String
        else {
            throw AdapterError("ValidationError", "token/validate returned an unexpected body")
        }

        queue.sync {
            // 낡은 재검증 — 그사이 다른 재검증이 새 자격증명을 커밋했거나 트랜스포트가 끝났다. 커밋하지 않는다
            // (호출자는 지금 커밋된 자격증명을 쓴다).
            guard !ended, credentialEpoch == epoch else { return }
            credentialEpoch += 1
            auth = [
                "token": jwt,
                "stageId": options.stageId,
                "displayName": options.displayName,
                "protocolVersion": Protocol.protocolVersion,
                "clientType": Protocol.adapterClientType,
            ]
            self.participantId = participantId
        }
    }

    /// Test-only — passes only the REST validate, without a socket, to fill in the JWT.
    /// ``ensureStreamLayer()`` tests use it to set up the "after join" state
    /// (the real open() goes as far as the socket connection and cannot finish with stubbed HTTP alone).
    internal func validateForTest() async throws { try await validate() }
    internal func handleConnectErrorForTest(_ raw: String?) { onConnectError(raw) }
    /// Test-only — feeds join confirmation and disconnects **on the dedicated queue**, like socket events.
    internal func confirmJoinedForTest(_ me: Member) { queue.sync { confirmJoined(me: me, history: nil) } }
    internal func disconnectedForTest(_ reason: String?) { queue.sync { onDisconnected(reason) } }
    internal var pendingJoinForTest: Bool { queue.sync { joinedContinuation != nil } }
    internal func sessionEndedForTest(_ reason: String?) { queue.sync { handleSessionEnd(reason) } }
    internal func serverErrorForTest(_ code: String, _ message: String?) {
        queue.sync { onServerError(["code": code, "message": message as Any]) }
    }
    internal func awaitJoinForTest() async throws { _ = try await awaitJoin(endOnTimeout: false) }
    internal var authTokenForTest: String { queue.sync { auth["token"] ?? "" } }
    internal var managerForTest: SocketManager? { queue.sync { manager } }
    internal var fallbackPendingForTest: Bool { queue.sync { fallbackWork != nil } }
    internal var loopRunningForTest: Bool { queue.sync { reconnectTask != nil } }
    internal var endedForTest: Bool { queue.sync { ended } }
    /// Holds the dedicated queue briefly — makes the order of work on the queue deterministic.
    internal func blockQueueForTest(_ seconds: Double) { queue.async { Thread.sleep(forTimeInterval: seconds) } }
    internal func confirmJoinedAsyncForTest(_ me: Member) { queue.async { self.confirmJoined(me: me, history: nil) } }
    /// As if a late cancellation reached the last join attempt — if that attempt has already finished, it must do nothing.
    internal func cancelLastJoinAttemptForTest() {
        queue.sync { abandonJoinAttempt(joinAttempt, AdapterError("NotConnected", "the join was cancelled")) }
    }
    internal var hasSocketForTest: Bool { queue.sync { socket != nil } }
    /// As if another path (join timeout · resume loop) discarded the socket — the library's ack timeout does not fire after that.
    internal func disposeSocketForTest() { queue.sync { disposeSocket() } }
    /// Test-only — sets up one ack that waits for an answer, like ``call(socket:event:payload:onReply:)`` (nothing is sent —
    /// no answer comes). A disconnect, disposal or end wakes it, or the ack-timeout fallback does.
    internal func pendingAckForTest() async -> [String: Any]? {
        await withCheckedContinuation { continuation in
            let resumed = ResumeGuard()
            queue.async {
                let id = self.nextAckId
                self.nextAckId += 1
                self.pendingAcks[id] = {
                    guard resumed.claim() else { return }
                    continuation.resume(returning: nil)
                }
                self.queue.asyncAfter(deadline: .now() + self.options.ackTimeout + 0.5) {
                    self.pendingAcks[id] = nil
                    guard resumed.claim() else { return }
                    continuation.resume(returning: nil)
                }
            }
        }
    }
    internal var pendingAckCountForTest: Int { queue.sync { pendingAcks.count } }

    // ── Adapter Stream 레이어 자리 확보 ─────────────────────────────────────

    // (종전에는 일시적 5xx 한 번이 그 클라이언트의 수명 내내 STUN 만 남겼다.) 인증 경로의 네트워크 오류 · 시한 · 재검증 실패 처리는
    // TS 와 같다.
    /// This stage's ICE servers — `GET /api/stages/:id/ice` (requires the guest JWT).
    ///
    /// TURN credentials are **issued short-lived**, so they are not in the public `/api/about`
    /// (if they were, anyone could use the relay). Older servers lack this route, so on a 404 it
    /// falls back to `/api/about` and gets STUN only — the previous behaviour as is (the result is cached). If the authenticated
    /// route fails with a status other than 404 (5xx · a 401 after revalidation) or an unreadable body, it returns the public
    /// route's STUN marked as failed — the client does not cache it, and the next call asks for TURN again. A network error,
    /// timeout or revalidation failure on the authenticated route gives an empty list marked as failed, without asking the
    /// public route. Fail-open: when nothing is received, it is an empty list marked as failed.
    public func fetchIceServers() async -> IceServersResult {
        let base = trimTrailingSlash(options.url)
        let stagePath =
            options.stageId.addingPercentEncoding(withAllowedCharacters: .alphanumerics)
            ?? options.stageId

        func get(_ url: URL, jwt: String?) async -> (Data, Int)? {
            var request = URLRequest(url: url)
            request.httpMethod = "GET"
            request.timeoutInterval = options.joinTimeout
            if let jwt, !jwt.isEmpty {
                request.setValue("Bearer \(jwt)", forHTTPHeaderField: "Authorization")
            }
            guard let (data, response) = try? await session.data(for: request) else { return nil }
            return (data, (response as? HTTPURLResponse)?.statusCode ?? 0)
        }

        func parse(_ data: Data, withExpiry: Bool) -> IceServersResult? {
            guard let body = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                return nil
            }
            let servers = ConnectClient.parseIceServers(body["iceServers"])
            let expiry = withExpiry ? (body["expiresAt"] as? NSNumber)?.int64Value : nil
            return IceServersResult(iceServers: servers, expiresAt: expiry)
        }

        // 인증 경로가 404 아닌 이유로 실패했는가 — 그때 받은 공개 STUN 은 임시다(클라이언트가 캐시하지 않는다).
        var degraded = false
        if let endpoint = URL(string: "\(base)/api/stages/\(stagePath)/ice") {
            let jwt = queue.sync { auth["token"] ?? "" }
            if !jwt.isEmpty {
                // 네트워크 오류 · 시한 · 재검증 실패는 공개 경로를 묻지 않는다(TS 와 같다) — 클라이언트가 아직 캐시된 목록을 쓰거나 빈 목록이다.
                guard var result = await get(endpoint, jwt: jwt) else { return .lookupFailed }
                if result.1 == 401 {
                    // 장수 세션에서 JWT 만료 — ensureStreamLayer 와 같은 회복 경로.
                    do { try await validate() } catch { return .lookupFailed }
                    guard let retried = await get(endpoint, jwt: queue.sync { auth["token"] ?? "" }) else { return .lookupFailed }
                    result = retried
                }
                let (data, status) = result
                if (200..<300).contains(status) {
                    if let parsed = parse(data, withExpiry: true) { return parsed }
                    degraded = true // 읽을 수 없는 본문
                } else if status != 404 {
                    degraded = true // 그 밖의 상태(5xx · 재검증 뒤의 401) — 404 = 구 서버(라우트 없음 — STUN 이 정답이다)
                }
            }
        }

        if let publicEndpoint = URL(string: "\(base)/api/about"),
            let (data, status) = await get(publicEndpoint, jwt: nil),
            (200..<300).contains(status),
            let parsed = parse(data, withExpiry: false)
        {
            return degraded ? .degraded(parsed.iceServers) : parsed
        }
        // 실패로 표시한 빈 목록 — 클라이언트가 캐시하지 않고, 화면 송출 헬퍼는 마지막으로 받은 목록으로 답한다.
        return .lookupFailed
    }

    // 오류 매핑은 여섯 언어 같은 계약이다.
    /// Ensures the Adapter Stream layer — `POST /api/stages/:id/layers/ensure-adapter-stream`. The server itself creates and
    /// binds the layer of an adapter that joined with `providesView: 'stream'` when it joins, so this call is an idempotent
    /// compatibility path.
    ///
    /// There is no body: the server derives the layer name from **the calling member's displayName** (no one can create one
    /// under another's name). For the error mapping see ``mapEnsureError(status:payload:)``. The request timeout equals the
    /// ack timeout. A 401 revalidates once with the adapter key and asks again — a failure of that revalidation (AuthRevoked
    /// etc.) is thrown as is. While closing or after the end, it is SessionEnded without a request.
    public func ensureStreamLayer() async throws -> EnsuredStreamLayer {
        // 닫는 중 · 끝났으면 묻지 않는다 — 퇴장 대기 동안에도 REST 는 닿는다(조사 실측: 닫은 뒤 ensure 가 403 Unauthorized 로 갔다).
        if queue.sync(execute: { closing || ended }) {
            throw AdapterError("SessionEnded", "this transport is closed — cannot ensure a stream layer")
        }
        guard !(queue.sync { auth["token"] ?? "" }).isEmpty else {
            throw AdapterError(
                "NotConnected", "ensureStreamLayer needs a joined client — connect() first")
        }
        let base = trimTrailingSlash(options.url)
        let stagePath =
            options.stageId.addingPercentEncoding(withAllowedCharacters: .alphanumerics)
            ?? options.stageId
        guard let endpoint = URL(string: "\(base)/api/stages/\(stagePath)/layers/ensure-adapter-stream")
        else {
            throw AdapterError("ConfigError", "could not build the ensure endpoint from url \(base)")
        }

        let timedOut = AdapterError(
            "NotConnected",
            "ensure-adapter-stream did not answer within \(SocketTransport.seconds(options.ackTimeout)) s (the ack timeout)")
        let session = self.session
        let ackTimeout = options.ackTimeout

        // 호출 시점의 JWT 를 매번 읽는다 — 401 재검증 뒤의 재시도가 새 토큰을 실어야 한다.
        @Sendable func call() async throws -> (Data, Int) {
            var request = URLRequest(url: endpoint)
            request.httpMethod = "POST"
            // URLRequest 의 시한은 패킷 사이의 공백만 잰다 — 응답 전체는 아래의 겨룸이 ack 시한으로 묶는다.
            request.timeoutInterval = ackTimeout
            let jwt = queue.sync { auth["token"] ?? "" }
            request.setValue("Bearer \(jwt)", forHTTPHeaderField: "Authorization")
            do {
                let (data, response) = try await session.data(for: request)
                return (data, (response as? HTTPURLResponse)?.statusCode ?? 0)
            } catch {
                // 부른 쪽의 취소 · 겨룸의 시한은 취소로 전한다 — 판정은 겨룸이 한다.
                if Task.isCancelled || error is CancellationError { throw CancellationError() }
                if (error as? URLError)?.code == .timedOut { throw timedOut }
                throw AdapterError(
                    "NotConnected",
                    "ensure-adapter-stream request failed — is the server reachable at \(options.url)? "
                        + "(\(error.localizedDescription))"
                )
            }
        }

        // 시한은 ack 시한 하나가 **전부**를 덮는다 — 첫 요청 · 401 재검증 · 재시도(검수 실측: 401 회복 중의 취소가 NotConnected 로
        // 바뀌었다). 지면 진행 중인 요청 · 재검증을 끊는다.
        let (data, status): (Data, Int)
        do {
            (data, status) = try await withThrowingTaskGroup(of: (Data, Int)?.self) { group in
                group.addTask { [self] in
                    let first = try await call()
                    if first.1 != 401 { return first }
                    // 장수 세션에서 JWT 만료 — 어댑터 키 재검증 후 1회 재시도(재개 루프와 같은 회복 경로). 재검증 실패(AuthRevoked
                    // 등)는 그대로 던진다.
                    try await self.validate(serverErrorAsNotConnected: true)
                    if self.queue.sync(execute: { self.closing || self.ended }) {
                        throw AdapterError("SessionEnded", "this transport is closed — cannot ensure a stream layer")
                    }
                    return try await call()
                }
                group.addTask {
                    try await Task.sleep(nanoseconds: UInt64(ackTimeout * 1_000_000_000))
                    return nil
                }
                defer { group.cancelAll() }
                guard let first = try await group.next(), let reply = first else { throw timedOut }
                return reply
            }
        } catch let error as AdapterError {
            throw error
        } catch {
            // 부른 쪽의 취소는 취소로 전한다 — 끊김(NotConnected)으로 바꾸면 재시도 루프가 취소를 못 알아본다.
            if Task.isCancelled || error is CancellationError { throw CancellationError() }
            throw AdapterError(
                "NotConnected",
                "ensure-adapter-stream request failed — is the server reachable at \(options.url)? "
                    + "(\(error.localizedDescription))"
            )
        }
        let payload = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
        guard (200..<300).contains(status) else { throw SocketTransport.mapEnsureError(status: status, payload: payload) }
        guard let layerId = payload?["layerId"] as? String,
              let name = payload?["name"] as? String
        else {
            throw AdapterError("ValidationError", "ensure-adapter-stream returned an unexpected body")
        }
        // 서버가 붙인 이름(지금의 나) ≠ 요청한 이름 — 다른 키가 그 이름을 쥐고 있어(그 이름으로 붙은 다른 키의 프로그램 · 다른 키에 묶인 레이어) `이름 (2)` 로 붙었다.
        // 앞뒤 공백은 서버(JS trim)처럼 줄바꿈까지 걷는다.
        let boundName = queue.sync { joinedMe }?.displayName ?? name
        let trim: (String) -> String = { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
        return EnsuredStreamLayer(
            layerId: layerId,
            name: name,
            // created 가 빠진 구 응답이면 상태코드로 접는다(201=새로 만듦) — TS 는 body 를
            // 그대로 믿지만 Swift 는 타입을 세워야 해서 여기서 한 번 확정한다.
            created: payload?["created"] as? Bool ?? (status == 201),
            // nameConflict 는 renamed 의 옛 이름 — 트랜스포트를 바로 써도 같은 값이다.
            nameConflict: trim(boundName) != trim(options.displayName),
            renamed: trim(boundName) != trim(options.displayName)
        )
    }

    // 시한 문장의 초 — TS 의 `${ms / 1000}` 와 같은 모양(0.05 · 15).
    /// Seconds as written in timeout messages (0.05 · 15).
    static func seconds(_ interval: TimeInterval) -> String {
        var text = String(format: "%.3f", interval)
        while text.hasSuffix("0") { text.removeLast() }
        if text.hasSuffix(".") { text.removeLast() }
        return text
    }

    // 여섯 언어 같은 문장.
    /// Hint for an adapter that did not declare providesView 'stream'.
    static let streamUndeclaredHint =
        "Connect with providesView: 'stream' — the server only creates and binds a screen layer for adapters that declare a screen."

    // 여섯 언어 같은 문장.
    /// Hint for the cap on a stage's Adapter Stream layers.
    static let streamLayerCapHint =
        "This stage has too many Adapter Stream layers — remove unused ones in the editor, then retry."

    /// Whether a server message talks about the Adapter Stream layer cap — "Adapter Stream layer(s)" or the word cap · limit.
    static func mentionsLayerCap(_ message: String) -> Bool {
        message.range(of: "adapter stream layer", options: .caseInsensitive) != nil
            || message.range(of: #"\b(cap|limit)\b"#, options: [.regularExpression, .caseInsensitive]) != nil
    }

    // 여섯 언어 공통 순수 매퍼(서버 계약 그대로 · 같은 문장).
    /// ensure-adapter-stream failure → AdapterError. A pure mapper that follows the server contract:
    /// 404 → NotSupported (an older server without the route) · 409 + `error: 'RoomEnded'` → RoomEnded · 429 → TooManyRequests ·
    /// 5xx → NotConnected (worth retrying) · any other 4xx → ValidationError (the server `message` as is — for a 409 ValidationError,
    /// a hint that tells an undeclared providesView from the layer cap). A 401 is the response after the caller has revalidated
    /// once (a second 401 folds into 4xx).
    static func mapEnsureError(status: Int, payload: [String: Any]?) -> AdapterError {
        let error = payload?["error"] as? String
        let message = payload?["message"] as? String
        if status == 404 {
            // 라우트가 없다(구 서버). 라이브 세션이 없으면 새 서버는 409 RoomEnded 다.
            return AdapterError(
                "NotSupported",
                "ensure-adapter-stream returned 404 — this Connect server predates adapter layer ensure",
                hint: "This Connect server does not support adapter stream layers yet — update the server.")
        }
        if status == 409 && error == "RoomEnded" {
            return AdapterError("RoomEnded", message ?? "the stage has no live session")
        }
        if status == 429 {
            return AdapterError("TooManyRequests", message ?? "ensure-adapter-stream was rate limited (429)")
        }
        if status >= 500 {
            return AdapterError(
                "NotConnected", "ensure-adapter-stream failed on the server (\(status))" + (message.map { ": \($0)" } ?? ""))
        }
        // 그 밖의 4xx — 서버 메시지를 그대로 싣고, 409 ValidationError 면 서버 메시지가 말하는 경우에만 힌트를 단다:
        // providesView 를 말하면 신고 힌트, Adapter Stream 레이어(상한)를 말하면 상한 힌트, 그 밖은 메시지만(여섯 언어 같은 판별).
        var hint: String?
        if status == 409 && error == "ValidationError", let message {
            if message.range(of: "providesView", options: .caseInsensitive) != nil {
                hint = streamUndeclaredHint
            } else if mentionsLayerCap(message) {
                hint = streamLayerCapHint
            }
        }
        return AdapterError("ValidationError", message ?? "ensure-adapter-stream was refused (\(status))", hint: hint)
    }

    // (여섯 언어 같은 규칙) 프로브(서버가 ack 에 답하지 않음): 0 이면 라이브러리가 시한을 걸지 않아(`timingOut(after: 0)` = 시한 없음)
    // 폴백이 0.5초 뒤에야 AckTimeout 으로 끝냈고, −1 은 15 ms 만에 AckTimeout, `.infinity` · `.nan` 은 2.5초 뒤에도 끝나지 않았다.
    /// The ack timeout must be finite, greater than 0 and at most 2^31−1 ms — anything else is a `ConfigError`.
    static func validateAckTimeout(_ seconds: TimeInterval) throws {
        try validateTiming("ackTimeout", seconds)
    }

    // 여섯 언어 같은 규칙.
    /// Checks a timing option (ack timeout · join timeout · reconnect delay · rejoin delay) — finite, greater than 0 and at most
    /// 2^31−1 ms; anything else is a `ConfigError` whose message names the option.
    static func validateTiming(_ name: String, _ seconds: TimeInterval) throws {
        guard seconds.isFinite, seconds > 0, seconds * 1000 <= 2_147_483_647 else {
            // 값 표기는 send 의 거부와 같다 — Swift 의 `"\(Double.infinity)"` 는 `inf` · `nan` 이라 `Infinity` · `NaN` 으로 바꾼다.
            let shown = seconds.isFinite ? "\(seconds)" : ConnectClient.describeNonFinite(seconds)
            throw AdapterError(
                "ConfigError", "\(name) must be a finite number of seconds > 0 and at most 2147483.647 (2^31-1 ms) — got \(shown)")
        }
    }

    // ── ②③ Socket — 접속 + joined 대기 ──────────────────────────────────────

    public func open(handlers: TransportHandlers) async throws -> Joined {
        // connect() 가 자격증명 모양을 검사하지만 이 클래스는 공개라 직접 만들 수도 있다 — 여기서도
        // 막는다(심층 방어). 순수 검사라 "열림" 플래그보다 **먼저** — 실패한 뒤 다시 열어도
        // "already opened" 가 아닌 같은 ConfigError 를 받게.
        guard StageToken.isAdapterKey(options.secretKey) else {
            throw AdapterError(
                "ConfigError",
                "SocketTransport needs the adapter key (ck_…) as secretKey — a cst_ token belongs to connect(), which unwraps it"
            )
        }
        try SocketTransport.validateAckTimeout(options.ackTimeout)
        // 시간 옵션도 같은 규칙 · 같은 자리 — 0 이면 지연 없는 재접속 · 재조인 루프가 되고, 음수 · NaN · ∞ 는 대기의
        // `UInt64(초 * 1e9)` 가 트랩으로 프로세스를 멈춘다. 여섯 언어 같은 규칙.
        try SocketTransport.validateTiming("joinTimeout", options.joinTimeout)
        try SocketTransport.validateTiming("reconnectDelay", options.reconnectDelay)
        try SocketTransport.validateTiming("reconnectDelayMax", options.reconnectDelayMax)
        try SocketTransport.validateTiming("rejoinDelay", options.rejoinDelay)
        try SocketTransport.validateTiming("rejoinDelayMax", options.rejoinDelayMax)
        let alreadyOpened = queue.sync { () -> Bool in
            if opened { return true }
            opened = true
            return false
        }
        if alreadyOpened { throw AdapterError("ConfigError", "transport already opened") }

        queue.sync { self.handlers = handlers }
        try await validate()
        handlers.onState?(.connecting)

        return try await awaitJoin(endOnTimeout: true)
    }

    // 후자가 없으면 새 소켓이 아무 이벤트도 안 내는 TCP 블랙홀에 빠졌을 때 루프가 connectSocket 뒤 그냥 return 해 영구
    // reconnecting 에 갇혔다.
    /// Attaches the socket and waits for `stage:joined` up to the join timeout — ends with whichever
    /// comes first, the join (success) or the timeout.
    ///
    /// With `endOnTimeout` true (open), a timeout ends the session with `close()`; with false (the reconnect
    /// loop), it **keeps the session alive**, discards only the socket and throws, so the loop keeps retrying
    /// on the next backoff.
    private func awaitJoin(endOnTimeout: Bool) async throws -> Joined {
        // 시도마다 번호를 받는다 — 타임아웃 · 취소는 **그 시도가 아직 기다리고 있을 때만** 손을 댄다.
        let attempt = queue.sync { () -> Int in
            joinAttempt &+= 1
            return joinAttempt
        }
        do {
            // 기다리는 쪽이 취소되면(open()/connect() 를 부른 태스크 취소) 타임아웃 자식은 즉시 던지지만 합류
            // 자식은 아무도 깨우지 않는 continuation 을 물고 있어 그룹이 영영 반환하지 않았다. 취소는 그
            // 시도를 깨우기만 한다 — 정리(세션 종료 · 소켓 폐기)는 아래 catch 가 큐 위에서 한다. 취소 처리에서
            // 바로 끝내면, 그사이 합류가 성공해 open() 이 이미 돌아간 뒤에 트랜스포트를 조용히 끝냈다.
            return try await withTaskCancellationHandler {
                try await awaitJoinUncancelled(attempt: attempt, endOnTimeout: endOnTimeout)
            } onCancel: { [weak self] in
                guard let self else { return }
                self.queue.async {
                    self.abandonJoinAttempt(attempt, AdapterError("NotConnected", "the join was cancelled"))
                }
            }
        } catch {
            // 최초 open 이 실패로 끝났다(취소 · 타임아웃 · 거부) — 만든 것을 남기지 않는다. 합류가 성립했더라도
            // (취소와 stage:joined 가 겹치면 합류가 먼저 확정되고 취소된 타임아웃 자식이 그룹을 이긴다) open() 은
            // 던지므로 끝낸다 — 종전에는 everJoined 를 보고 건너뛰어, open() 이 실패했는데 트랜스포트는 합류한
            // 소켓을 쥔 채 살아 있었다(T2 400회 중 실패한 open 거의 전부). 타임아웃은 합류 뒤에 던지지 않으므로
            // 여기 오는 합류 상태는 취소뿐이다.
            if endOnTimeout {
                queue.sync {
                    terminalError = terminalError
                        ?? (error as? AdapterError)
                        ?? AdapterError("NotConnected", "the join was cancelled")
                    markEnded()
                    disposeSocket()
                }
            }
            throw error
        }
    }

    /// Abandons one attempt — wakes it **only while that attempt is still waiting** (does nothing for an attempt that
    /// already ended in a join or a failure). If it has not registered yet, marks it to fail as soon as it registers.
    /// - Important: Call only on `queue`.
    private func abandonJoinAttempt(_ attempt: Int, _ error: AdapterError) {
        if waitingAttempt == attempt {
            failJoin(error)
        } else if registeredAttempt < attempt {
            abandonedAttempt = (attempt, error)
        }
    }

    private func awaitJoinUncancelled(attempt: Int, endOnTimeout: Bool) async throws -> Joined {
        let timeout = options.joinTimeout
        return try await withThrowingTaskGroup(of: Joined.self) { group in
            group.addTask { [weak self] in
                guard let self else { throw AdapterError("NotConnected", "transport released") }
                return try await withCheckedThrowingContinuation { continuation in
                    self.queue.async {
                        self.registeredAttempt = attempt
                        // 그사이 끝났다(close · 종결 거부) — 버려진 시도가 새 소켓을 열어 아무도 쥐지 않은
                        // 좌석을 잡지 않게 한다(재개 루프의 ended 확인과 이 블록 사이의 창).
                        if self.ended {
                            continuation.resume(
                                throwing: self.terminalError
                                    ?? AdapterError("NotConnected", "transport closed before joining"))
                            return
                        }
                        // 등록하기도 전에 타임아웃 · 취소됐다 — 소켓을 열지 않는다.
                        if let abandoned = self.abandonedAttempt, abandoned.attempt == attempt {
                            self.abandonedAttempt = nil
                            continuation.resume(throwing: abandoned.error)
                            return
                        }
                        self.joinedContinuation = continuation
                        self.waitingAttempt = attempt
                        self.connectSocket()
                    }
                }
            }
            group.addTask { [weak self] in
                try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                guard let self else {
                    throw AdapterError("JoinTimeout", "stage:joined not received within the join timeout")
                }
                let timedOut = AdapterError(
                    "JoinTimeout",
                    endOnTimeout
                        ? "stage:joined not received within the join timeout"
                        : "stage:joined not received within the join timeout (rejoin)")
                // 깨어난 뒤 **같은 큐 턴에서** 이 시도가 아직 기다리는지 본다 — 직전에 stage:joined 가 처리됐으면
                // 합류가 이긴다. 종전에는 확인 없이 방금 합류한 소켓을 버려, 재개 루프는 소켓도 루프도 없는
                // connected 로 영영 남았고(20/20 재현) 최초 open 은 서버가 확인한 합류를 SessionEnded 로 던졌다.
                let outcome = self.queue.sync { () -> AdapterError? in
                    if self.waitingAttempt == attempt {
                        if endOnTimeout {
                            // open — 세션을 끝내고 소켓을 버린다(close() 를 기다리지 않고 같은 턴에).
                            let cause = self.terminalError ?? timedOut
                            self.terminalError = cause
                            self.markEnded()
                            self.disposeSocket()
                            return cause
                        }
                        // 재접속 루프용 — 세션은 끝내지 않는다. 붙잡은 합류를 깨우고 소켓만 버린다.
                        self.failJoin(timedOut)
                        self.disposeSocket()
                        return timedOut
                    }
                    if self.registeredAttempt < attempt {
                        // 등록 전(큐가 막혀 있었다) — 등록하는 즉시 실패하게 하고 여기서 던진다.
                        self.abandonedAttempt = (attempt, timedOut)
                        return timedOut
                    }
                    return nil // 이미 합류 · 실패로 끝났다 — 그 결과가 이긴다
                }
                if let outcome { throw outcome }
                // 합류 자식의 결과가 그룹을 끝낼 때까지(cancelAll) 기다린다 — 먼저 던지면 타임아웃이 이긴다.
                try await Task.sleep(nanoseconds: UInt64.max / 2)
                throw timedOut
            }

            defer { group.cancelAll() }
            guard let first = try await group.next() else {
                throw AdapterError("JoinTimeout", "stage:joined not received within the join timeout")
            }
            return first
        }
    }

    /// - Important: Call only on `queue`.
    private func createSocket() -> SocketIOClient {
        let base = trimTrailingSlash(options.url)
        let components = URLComponents(string: base)
        let basePath = trimTrailingSlash(components?.path ?? "")
        let socketPath = options.socketPath ?? "\(basePath)/socket.io"
        let port = components?.port.map { ":\($0)" } ?? ""
        let origin = URL(string: "\(components?.scheme ?? "http")://\(components?.host ?? "")\(port)")!

        var config: SocketIOClientConfiguration = [
            .path(socketPath),
            // 재접속은 우리가 돌린다(매 시도마다 새 JWT 재검증이 선행돼야 하므로).
            .reconnects(false),
            .handleQueue(queue), // 라이브러리가 스레드 안전하지 않아 전용 시리얼 큐로 몬다
            .compress,
        ]
        // ⚠️ auth 를 `.connectParams` 로 주면 JWT 가 engine.io **URL 쿼리**에 실려
        // 프록시/ALB 액세스 로그에 남는다. 다른 4개 어댑터처럼 CONNECT 패킷 payload
        // (= socket.handshake.auth)로 보낸다 — connect(withPayload:) 경로.
        if options.usePolling {
            // WS 업그레이드까지 끄고 polling 에 머문다. 기본값(미지정)은 polling 으로
            // 시작해 WebSocket 으로 업그레이드하므로 WS 가 막힌 망에서도 붙는다.
            config.insert(.forcePolling(true))
        }
        config.insert(.log(false))

        let manager = SocketManager(socketURL: origin, config: config)
        let socket = manager.socket(forNamespace: SocketTransport.namespace)

        socket.on(clientEvent: .connect) { [weak self] _, _ in self?.onConnected() }
        socket.on(clientEvent: .disconnect) { [weak self] data, _ in
            self?.onDisconnected(data.first as? String)
        }
        socket.on(clientEvent: .error) { [weak self] data, _ in
            self?.onConnectError(data.map { "\($0)" }.joined(separator: " "))
        }
        socket.on(Protocol.ServerEvents.joined) { [weak self] data, _ in
            self?.onJoined(data.first)
        }
        socket.on(Protocol.ServerEvents.message) { [weak self] data, _ in
            guard let self, let message = SocketTransport.toStageMessage(data.first) else { return }
            self.handlers.onMessage?(message)
        }
        socket.on(Protocol.ServerEvents.history) { [weak self] data, _ in
            guard let self,
                  let payload = data.first as? [String: Any],
                  let items = payload["history"] as? [Any] else { return }
            self.handlers.onHistory?(SocketTransport.mapHistory(items))
        }
        socket.on(Protocol.ServerEvents.memberAdded) { [weak self] data, _ in
            guard let self,
                  let payload = data.first as? [String: Any],
                  let member = payload["member"] as? [String: Any],
                  let id = member["participantId"] as? String else { return }
            self.handlers.onMemberJoin?(
                SocketTransport.toMember(member, fallbackId: id, fallbackName: "")
            )
        }
        socket.on(Protocol.ServerEvents.memberRemoved) { [weak self] data, _ in
            guard let self,
                  let payload = data.first as? [String: Any],
                  let id = payload["participantId"] as? String else { return }
            self.handlers.onMemberLeave?(id)
        }
        socket.on(Protocol.ServerEvents.adapterLeaseHolder) { [weak self] data, _ in
            guard let self, let payload = data.first as? [String: Any] else { return }
            self.handlers.onAdapterLeaseHolder?(payload)
        }
        // 원시 패스스루 — 파싱하지 않는다. 이 통로의 의미(ready/sdp/ice)는
        // 어댑터와 Stage 캔버스 사이의 계약이고 트랜스포트는 배달만 한다.
        socket.on(Protocol.ServerEvents.signaling) { [weak self] data, _ in
            guard let self, let payload = data.first as? [String: Any] else { return }
            self.handlers.onSignaling?(payload)
        }
        socket.on(Protocol.ServerEvents.ended) { [weak self] data, _ in
            guard let self else { return }
            let reason = (data.first as? [String: Any])?["reason"] as? String
            self.handleSessionEnd(reason)
        }
        socket.on(Protocol.ServerEvents.error) { [weak self] data, _ in
            self?.onServerError(data.first as? [String: Any])
        }

        self.manager = manager
        self.socket = socket
        return socket
    }

    private func onServerError(_ payload: [String: Any]?) {
        let code = payload?["code"] as? String ?? "Unknown"
        let error = AdapterError(code, payload?["message"] as? String)
        lastServerError = error
        handlers.onError?(error)
        if code == "RoomEnded" {
            handleSessionEnd("room-ended")
        }
    }

    /// Creates the socket and connects with the auth payload. - Important: Call only on `queue`.
    private func connectSocket() {
        // 직전 stage:error 는 **이 시도**의 것만 — 앞선 시도 · 세션 중의 오류를 합류 전 끊김의 이유로 싣지 않는다.
        lastServerError = nil
        let payload = auth
        createSocket().connect(withPayload: payload)
    }

    private func onConnected() {
        // 서버는 JWT claim 으로 이미 join 시켰다 — 구버전 호환 fire-and-forget.
        socket?.emit(
            Protocol.ClientEvents.joinRoom,
            ["clientInfo": ["peerName": options.displayName]]
        )
        startJoinedFallback()
    }

    /// The reason socket.io-client-swift gives when it receives **the server's namespace DISCONNECT packet**
    /// (`SocketIOClient.handlePacket`'s `.disconnect` → `didDisconnect(reason: "Got Disconnect")`, v16.1.1).
    /// It corresponds to JS's `"io server disconnect"`. A disconnect of our own (`socket.disconnect()`) is
    /// `"Namespace leave"`, and an engine close (ping timeout · transport drop) carries an engine reason, so they don't mix.
    static let serverDisconnectReason = "Got Disconnect"

    private func onDisconnected(_ reason: String?) {
        clearFallback()
        // 이 연결의 ack 는 더 오지 않는다 — 기다리는 쪽을 곧바로 깨운다(AckTimeout).
        failPendingAcks()
        if ended { return }
        // close() 가 퇴장 처리를 기다리는 중 — 서버가 leave 를 처리하고 끊었다(그 응답). 세션 종료가 아니라 퇴장의 끝이다.
        if closing {
            leaveWaiter?()
            leaveWaiter = nil
            return
        }
        // 서버가 네임스페이스를 끊었다 — 어댑터 키 회수·만료(`cutAdapterKeyMembers` · 만료 스윕)나 재접속 키
        // 게이트 거부. `stage:ended` 없이 오므로 세션 종료와 **같은 길**로 보낸다: autoRejoin 이면 REST
        // 재검증부터(회수된 키는 401/403 → AuthRevoked → 포기), 아니면 `server-disconnect` 로 끝낸다.
        // 이미 종료를 처리 중이면(stage:ended 가 먼저 왔다) handleSessionEnd 의 가드가 no-op 으로 만든다.
        let serverCut = reason == SocketTransport.serverDisconnectReason
        // 최초 open 이 아직 합류하지 못했다 — 이 끊김은 그 open 의 실패다. 재접속 루프를 띄우면 open() 은
        // 실패로 끝나는데 트랜스포트가 나중에 혼자 합류해, 아무도 쥐지 않은 좌석이 남았다.
        if opened && !everJoined {
            if serverCut {
                handleSessionEnd("server-disconnect")
            } else {
                terminalError = terminalError
                    ?? AdapterError("NotConnected", "socket disconnected before joining the stage")
                markEnded()
            }
            return
        }
        failJoin(serverCut
            ? preJoinEndError("server-disconnect")
            : AdapterError("NotConnected", "socket disconnected before joining the stage"))
        if serverCut {
            handleSessionEnd("server-disconnect")
            return
        }
        handlers.onState?(.reconnecting)
        startReconnectLoop()
    }

    private func onConnectError(_ raw: String?) {
        // 끝난 뒤의 거부는 다시 알리지 않는다 — 종결 결과마다 오류 이벤트 하나 · ended 하나.
        if ended { return }
        // JWT 만료 추정(Unauthorized): 소켓 생성 시 스냅샷된 auth 는 옛 토큰이라
        // 재전송이 계속 실패한다.
        //  - 세션 중(everJoined): 세션-종료 회복경로로 보내 REST 재검증부터 다시 한다
        //    (재접속 루프가 매 시도마다 새 JWT + 새 소켓). 재검증이 진짜 401/403 이면
        //    그 경로가 AuthRevoked 로 끝낸다. TS/Python 과 정렬.
        //  - 초기 연결 중: 방금 받은 JWT 가 거부된 것 → 종결(AuthRevoked), 무한 재시도 금지.
        if let raw = raw, raw.contains("Unauthorized"), everJoined {
            let refused = AdapterError("NotConnected", raw)  // 재검증으로 회복 중이라는 단서
            if reauthRefreshed && !options.autoRejoin {
                // 한 번뿐인 재검증이 받아 온 **새** 토큰도 거부됐다 — 다시 재검증 · 재접속을 되풀이하지 않고
                // 끝낸다(REST 는 키를 받는데 소켓이 계속 거부하면 루프가 된다). 오류 → ended 순서.
                handlers.onError?(refused)
                markEnded()
                handlers.onEnded?("reauth-failed")
                return
            }
            // 이 소켓을 기다리던 재개 시도가 있으면 그 시도의 실패로 깨운다 — 루프가 이 오류를 한 번 알리고
            // 재검증부터 다시 한다. 여기서도 알리면 같은 거부가 두 번 나갔다.
            if joinedContinuation != nil {
                failJoin(refused)
            } else {
                handlers.onError?(refused)
            }
            if options.autoRejoin {
                handleSessionEnd("jwt-expired")
            } else {
                // autoRejoin 을 끈 어댑터도 JWT 만료만으로 죽지는 않는다 — 한 번 재검증해 살아 있는 키면 새
                // 소켓으로 다시 붙고, 재검증이 실패하면 끝낸다(공유 다섯 코드는 `rejoin-abandoned: <code>`,
                // 그 밖은 `reauth-failed`). 종전에는 재검증 없이 곧바로 `jwt-expired` 로 끝냈다.
                reauthOnly = true
                handlers.onState?(.reconnecting)
                startReconnectLoop()
            }
            return
        }
        guard let terminal = SocketTransport.classifyHandshakeError(raw, includeAuth: !everJoined) else {
            // 비종결(네트워크·프록시·포트 오류) — 조용히 삼키면 사용자에게 단서가 0 이다.
            handlers.onError?(AdapterError("NotConnected", raw ?? "socket connect error"))
            return
        }
        terminalError = terminal
        handlers.onError?(terminal)
        failJoin(terminal)
        // 합류한 **뒤**(재개 루프가 새 소켓으로 핸드셰이크하다 거부됐다)면 이 실패를 세션 종료로 알릴 쪽이
        // 없다 — 재개 루프는 awaitJoin 실패를 일시 오류로 보고 ended 를 확인한 뒤 조용히 빠져나간다. 종전에는
        // 상태만 ended 가 되고 onEnded 가 불리지 않아 클라이언트의 종료 처리(큐 비우기 · ack 대기 거절 ·
        // ended 이벤트)가 돌지 않았고, ack 를 기다리던 발행이 영영 매달렸다. 최초 open 중이면 open() 이
        // 이 오류를 던지므로 알리지 않는다(ended 는 한 번만).
        let notify = everJoined && !ended
        markEnded()
        if notify { handlers.onEnded?("rejoin-abandoned: \(terminal.code)") }
    }

    // 세션이 닫혔다 — 종착점인가, 기다릴 일인가 (T5-3).
    /// The session closed — is it terminal, or something to wait for?
    ///
    /// With `autoRejoin` on it **does not end**: it leaves the state at `.reconnecting` and hands over to the
    /// existing reconnect loop. That loop revalidates over REST (new JWT) on every attempt and connects with a new
    /// socket, so it fits a return after the session disappeared as well — the only differences are the interval
    /// (longer for resume) and treating `RoomEnded` as **something to wait for**.
    ///
    /// Why not report `.ended`: the client treats `.ended` as a one-shot latch (it empties the queue and
    /// freezes the state), so once reported it cannot be undone.
    internal func handleSessionEnd(_ reason: String?) {
        // 이미 끝났다 — ended 는 한 번만(stage:ended 뒤 stage:error RoomEnded 가 와도). close() 가 퇴장 중이면 그 close 가
        // 끝낸다(재개 루프를 띄우지 않는다).
        if ended || closing { return }
        // 최초 open 이 아직 합류하지 못했다 — 이 종료는 그 open 의 실패다(RoomEnded / NotConnected).
        // 재개 루프를 띄우면 open() 은 타임아웃으로 실패하는데 트랜스포트가 나중에 혼자 합류해, 아무도
        // 쥐지 않은 좌석이 남았다. open() 이 이 오류를 던지므로 ended 는 알리지 않는다.
        if opened && !everJoined {
            terminalError = terminalError ?? preJoinEndError(reason)
            markEnded()
            return
        }
        if !options.autoRejoin {
            markEnded()
            handlers.onEnded?(reason)
            return
        }
        if rejoining {
            // 재개 시도의 새 소켓이 합류 전에 다시 닫혔다 — 기다리는 합류를 깨워 루프가 백오프로 다시
            // 시도하게 한다(안 깨우면 joinTimeout 까지 기다렸다). 그 소켓과 합류 폴백도 지금 버린다 —
            // 남기면 백오프 동안 폴백(4초)이 끝난 세션 위에서 가짜 connected 를 낼 수 있다.
            clearFallback()
            failJoin(preJoinEndError(reason))
            disposeSocket()
            return
        }
        rejoining = true
        clearFallback()
        // 소켓은 끊되 ended 는 세우지 않는다 — 여기서 세우면 클라이언트가 래치를 걸어버려
        // 뒤에 복귀해도 아무것도 흐르지 않는다.
        socket?.disconnect()
        handlers.onState?(.reconnecting)
        startReconnectLoop()
    }

    /// The error an attempt gets when the session closes before the join. If the server disconnected, it carries the last `stage:error`
    /// (the key gate sends `Unauthorized: adapter key revoked` right before disconnecting).
    private func preJoinEndError(_ reason: String?) -> AdapterError {
        if reason == "server-disconnect" {
            let last = lastServerError.map { " (last server error: \($0.code): \($0.message))" } ?? ""
            return AdapterError("NotConnected", "the server disconnected the socket before joining the stage" + last)
        }
        return AdapterError("RoomEnded", "the stage session ended before joining (\(reason ?? "unknown"))")
    }

    // 백오프는 다른 어댑터와 동률.
    /// Own reconnect loop — revalidates over REST (new JWT) on every attempt and connects with a new socket.
    /// The wait doubles from `reconnectDelay` up to `reconnectDelayMax` (default 0.5 s → 30 s), or from `rejoinDelay`
    /// up to `rejoinDelayMax` (default 2 s → 60 s) while waiting for a closed session to reopen. A `retryAfterMs` from
    /// the server (429 · 503) sets a floor that can exceed the cap.
    private func startReconnectLoop() {
        guard reconnectTask == nil, !ended else { return }
        // `reconnection: false` 는 **전송 끊김 재접속**만 끈다. 세션 재개(stage:ended · 서버 끊김 뒤의 재검증 ·
        // autoRejoin 을 끈 어댑터의 한 번뿐인 재검증)는 같은 루프를 타지만 그 옵션과 무관하다 — 종전에는
        // 함께 꺼져, 키가 회수돼도 끝나지 않고 세션이 다시 열려도 붙지 않은 채 reconnecting 에 갇혔다.
        guard options.reconnection || rejoining || reauthOnly else { return }
        reconnectTask = Task { [weak self] in
            guard let self else { return }
            // 세션 재개는 소켓 끊김보다 느긋하게 확인한다 — 닫힌 스테이지를 초당 두드릴
            // 이유가 없다.
            let waitingForSession = self.queue.sync(execute: { self.rejoining })
            var delay = waitingForSession ? self.options.rejoinDelay : self.options.reconnectDelay
            let delayMax =
                waitingForSession ? self.options.rejoinDelayMax : self.options.reconnectDelayMax
            // 서버가 말한 하한(429 · 503 의 `retryAfterMs`) — 그보다 일찍 두드리면 같은 거절을 받고 키당 입장 예산(분당 60)만 쓴다.
            // 다음 한 번의 기다림에만 적용하고(상한을 넘어도 된다 — 서버가 그렇게 말했다) 간격의 두 배 증가는 하한 전 값에서
            // 이어 간다(TS scheduleRejoin(minDelayMs) · 여섯 언어 같은 규칙).
            var retryAfterMs: Int? = nil
            while !Task.isCancelled {
                if self.queue.sync(execute: { self.ended }) { return }
                let wait = SocketTransport.rejoinWait(delay: delay, retryAfterMs: retryAfterMs)
                retryAfterMs = nil
                try? await Task.sleep(nanoseconds: UInt64(wait * 1_000_000_000))
                // 잠든 사이 close() · 종결로 끝났다(markEnded 가 이 태스크를 취소한다) — 재검증을 하나 더 보내지 않는다.
                // 취소된 잠은 `try?` 로 곧바로 깨어나 종전에는 그대로 validate 로 갔고, 취소된 태스크의 요청도
                // 서버까지 나갔다 — 닫힌 뒤의 token/validate 한 번(입장 예산을 쓴다)이고, 시험에서는 그 요청이 다음
                // 시험의 요청 수에 잡혀 `hits == 1` 확인 둘이 연달아 실패했다(CI macos-14).
                if Task.isCancelled || self.queue.sync(execute: { self.ended }) { return }
                delay = min(delayMax, delay * 2)

                do {
                    try await self.validate()
                    // 재검증이 성공했다 — autoRejoin 을 끈 어댑터의 "한 번뿐인 재검증" 은 여기서 끝난다. 이 뒤의
                    // 일시 실패(전송 끊김 뒤의 네트워크 오류)는 보통 재접속처럼 기다린다. 새 토큰이 소켓에서
                    // 또 거부되는지만 기억한다.
                    self.queue.sync {
                        if self.reauthOnly {
                            self.reauthOnly = false
                            self.reauthRefreshed = true
                        }
                    }
                } catch let error as AdapterError {
                    // RoomEnded 는 **아직 세션이 안 열렸다**는 뜻이다 — 자동 복귀를 켠
                    // 어댑터에게는 포기할 이유가 아니라 기다릴 일이다. 반대로 자격증명
                    // 무효화·버전 스큐·본문 거부는 기다려도 달라지지 않는다
                    // (`nonRejoinableCodes`).
                    let giveUp = SocketTransport.shouldAbandonRejoin(
                        code: error.code, autoRejoin: self.options.autoRejoin)
                    let reauth = self.queue.sync { self.reauthOnly }
                    if giveUp || reauth {
                        let reason: String
                        if SocketTransport.nonRejoinableCodes.contains(error.code) {
                            reason = "rejoin-abandoned: \(error.code)"
                        } else if reauth {
                            reason = "reauth-failed" // autoRejoin 을 끈 어댑터의 한 번뿐인 재검증이 실패했다
                        } else {
                            reason = error.code == "RoomEnded"
                                ? "room-ended" : "rejoin-abandoned: \(error.code)"
                        }
                        // 오류 → ended 상태 → ended 를 **한 큐 턴 안에서** 이 순서로 — 앱이 `.ended` 에서
                        // 루프를 끝내도 원인(.failure)은 이미 받았다. 그사이 close() 가 끝냈으면 알리지 않는다
                        // (ended 는 한 번만).
                        self.queue.sync {
                            if self.ended { return }
                            self.handlers.onError?(error)
                            self.markEnded()
                            self.handlers.onEnded?(reason)
                        }
                        return
                    }
                    // 일시적(세션이 아직 안 열림 = RoomEnded · 네트워크 · 429 · 503) — 다음 백오프에서 재시도. 서버가 말한
                    // 간격이 있으면 그보다 일찍은 아니다.
                    retryAfterMs = error.retryAfterMs
                    continue
                } catch {
                    continue
                }

                if self.queue.sync(execute: { self.ended }) { return } // sleep 중에 닫혔을 수 있다
                self.queue.sync { self.disposeSocket() }
                do {
                    // open() 과 **같은 join 타임아웃**으로 붙는다(endOnTimeout: false = 세션은
                    // 살린 채 실패 시 계속 재시도). 없이 connectSocket 뒤 return 하면 소켓이
                    // 조용히 hang 할 때 영구 reconnecting 에 갇힌다.
                    _ = try await self.awaitJoin(endOnTimeout: false)
                    // reconnectTask 는 합류를 확정한 confirmJoined 가 큐 위에서 이미 비웠다 — 여기서 비우면
                    // 그사이 새로 시작된 루프의 핸들을 지운다.
                    return // 합류 → connected
                } catch {
                    // 타임아웃/연결 실패 — awaitJoin 이 소켓을 버렸다. 세션은 살아있으니
                    // 다음 백오프로 계속(단서만 남긴다).
                    if !self.queue.sync(execute: { self.ended }) {
                        self.handlers.onError?(
                            (error as? AdapterError) ?? AdapterError("NotConnected", "\(error)"))
                    }
                    continue
                }
            }
        }
    }

    private func onJoined(_ payload: Any?) {
        let object = payload as? [String: Any]
        let member = object?["member"] as? [String: Any]
        let me = SocketTransport.toMember(
            member,
            fallbackId: participantId,
            fallbackName: options.displayName
        )
        let history = (object?["history"] as? [Any]).map(SocketTransport.mapHistory)
        confirmJoined(me: me, history: history)
    }

    private func startJoinedFallback() {
        clearFallback()
        let work = DispatchWorkItem { [weak self] in
            guard let self, self.socket?.status == .connected else { return }
            // 구버전 서버는 stage:joined 를 push 하지 않는다 — 다른 어댑터와 동률 폴백.
            self.confirmJoined(
                me: Member(participantId: self.participantId, displayName: self.options.displayName),
                history: nil
            )
        }
        fallbackWork = work
        queue.asyncAfter(deadline: .now() + SocketTransport.joinedFallback, execute: work)
    }

    /// - Important: Call only on `queue`.
    private func cancelReconnectLoop() {
        reconnectTask?.cancel()
        reconnectTask = nil
    }

    private func clearFallback() {
        fallbackWork?.cancel()
        fallbackWork = nil
    }

    /// internal — lets tests pass the "join confirmed" point without a socket.
    internal func confirmJoined(me: Member, history: [StageMessage]?) {
        // 끝난 트랜스포트에 늦게 온 합류 — 상태를 되살리지 않는다. close() 가 퇴장을 기다리는 중에 온 합류(재개 시도의
        // stage:joined)도 같다 — connected 를 내면 클라이언트가 leave 뒤에 큐를 흘린다. 기다리던 open 은 close 가 끝낸다.
        if ended || closing { return }
        clearFallback()
        // 합류가 성립한 **바로 이 지점**에서 재개 상태를 푼다 — 뒤로 미루면 그 사이 도착한
        // 종료 이벤트가 "이미 재개 중"으로 오인돼 **영구히 버려진다**.
        rejoining = false
        reauthOnly = false
        reauthRefreshed = false
        if let history { handlers.onHistory?(history) }
        // 재접속 루프의 합류다 — 루프는 이 합류로 끝나므로 핸들을 **여기서, 같은 큐 턴에** 비운다. 루프가
        // 깨어나 스스로 비우기 전에 stage:ended · 끊김이 큐에 오면 startReconnectLoop 이 남은 핸들을 보고
        // 아무것도 시작하지 않아 영영 reconnecting 에 갇혔다.
        if joinedContinuation != nil { reconnectTask = nil }
        everJoined = true
        // 기다리는 합류를 **매번** 깨운다 — 최초 open 이든 재개 루프의 awaitJoin 이든. 종전에는 최초
        // 합류에서만 깨워, 재개 루프가 성공한 합류를 joinTimeout 뒤 실패로 읽고 멀쩡한 소켓을 버린 뒤
        // 다시 붙기를 끝없이 반복했다(실측: 40초에 재합류 6회, 매번 ~2초만 살았다).
        joinedContinuation?.resume(returning: Joined(roomId: options.stageId, me: me))
        joinedContinuation = nil
        waitingAttempt = 0
        joinedMe = me
        // 이 합류의 나 — connected 보다 먼저(클라이언트가 그 connected 를 지금의 나로 처리하게). 재합류는 다른 participantId 를
        // 받을 수 있다(guest JWT 시한 · 쿠키 유실).
        handlers.onJoined?(me)
        handlers.onState?(.connected)
    }

    private func markEnded() {
        guard !ended else { return }
        ended = true
        rejoining = false
        clearFallback()
        cancelReconnectLoop()
        // ⚠️ 여기서 join 대기를 반드시 깨운다. 안 깨우면 open() 의 TaskGroup 이 미재개
        // continuation 을 물고 영원히 반환하지 않는다(적대검수 재현: 접속 실패 = 무한 대기).
        failJoin(terminalError ?? AdapterError("NotConnected", "socket closed before joining the stage"))
        handlers.onState?(.ended)
        failPendingAcks()
        socket?.disconnect()
    }

    /// - Important: Call only on `queue`.
    private func failJoin(_ error: AdapterError) {
        guard let continuation = joinedContinuation else { return }
        joinedContinuation = nil
        waitingAttempt = 0
        continuation.resume(throwing: error)
    }

    // ── ④ 송수신 ────────────────────────────────────────────────────────────

    public func publish(_ input: PublishInput, ack: Bool) async -> PublishResult {
        let snapshot = queue.sync { (ended: ended, socket: socket) }
        if snapshot.ended { return PublishResult(ok: false, code: "SessionEnded") }
        guard let socket = snapshot.socket, socket.status == .connected else {
            return PublishResult(ok: false, code: "NotConnected")
        }

        var payload: [String: Any] = [
            "channel": input.channel,
            "value": input.value.jsonValue,
            // 집 컨벤션: messageId 는 메시지 **이름**을 실어 나른다 — pie 트리거 매칭·
            // Console 이름 컬럼·Backstage 게이팅이 전부 이걸 이름으로 읽는다.
            "messageId": input.channel,
        ]
        if let target = input.targetParticipantId { payload["targetParticipantId"] = target }

        if !ack {
            queue.async { socket.emit(Protocol.ClientEvents.message, payload) }
            return PublishResult(ok: true)
        }

        guard let reply = await call(socket: socket, event: Protocol.ClientEvents.message, payload: payload)
        else {
            return PublishResult(ok: false, code: "AckTimeout")
        }
        guard let ok = reply["ok"] as? Bool else {
            return PublishResult(ok: false, code: "ValidationError", message: "malformed ack from server")
        }
        return PublishResult(
            ok: ok,
            code: reply["code"] as? String,
            message: reply["message"] as? String,
            messageId: reply["messageId"] as? String
        )
    }

    /// Best-effort — dropped before connecting or while disconnected. The viewer starts the negotiation again, so
    /// queueing would let frames of an old negotiation arrive late and disturb the state.
    public func sendSignaling(_ payload: [String: Any]) async {
        // 닫는 중 · 끝났으면 버린다(TS 와 같다) — 퇴장 대기 동안에도 소켓은 붙어 있다.
        let snapshot = queue.sync { closing || ended ? nil : socket }
        guard let socket = snapshot, socket.status == .connected else { return }
        queue.async {
            self.signalingEmitted += 1
            socket.emit(Protocol.ClientEvents.signaling, payload)
        }
    }

    /// Number of `stage:signaling` emitted — test-only (counted only on the socket queue).
    private var signalingEmitted = 0
    internal var signalingEmittedForTest: Int { queue.sync { signalingEmitted } }

    public func listMembers() async throws -> [Member] {
        // 닫는 중 · 끝났으면 NotConnected — close() 의 퇴장 대기(최대 1초) 동안 소켓이 붙어 있어도 roster 를 묻지 않는다(TS 와
        // 같다). 클라이언트는 이미 ended 라 ended 힌트가 붙는다.
        let snapshot = queue.sync { closing || ended ? nil : socket }
        guard let socket = snapshot, socket.status == .connected else {
            throw AdapterError("NotConnected", "socket is not connected")
        }
        guard let reply = await call(socket: socket, event: Protocol.ClientEvents.listMembers, payload: [:])
        else {
            throw AdapterError("AckTimeout", "stage:list-members timed out")
        }
        guard reply["ok"] as? Bool == true, let members = reply["members"] as? [[String: Any]] else {
            throw AdapterError("Unauthorized", "stage:list-members rejected")
        }
        return members.compactMap { member in
            guard let id = member["participantId"] as? String else { return nil }
            return SocketTransport.toMember(member, fallbackId: id, fallbackName: "")
        }
    }

    public func submitRegistrationCode(_ code: String) async throws -> AdapterRegistrationResult {
        try await registerAck(
            event: Protocol.ClientEvents.adapterRegisterSubmit,
            payload: ["code": code],
            wantSecret: true
        )
    }

    public func resumeRegistration(stageId: String, deviceSecret: String) async throws
        -> AdapterRegistrationResult
    {
        try await registerAck(
            event: Protocol.ClientEvents.adapterRegisterResume,
            payload: ["stageId": stageId, "deviceSecret": deviceSecret],
            wantSecret: false
        )
    }

    /// Shared ack round trip for register-submit/resume. Failures are registration-specific, so ValidationError.
    private func registerAck(
        event: String,
        payload: [String: Any],
        wantSecret: Bool
    ) async throws -> AdapterRegistrationResult {
        // 닫는 중이면 서버에 묻지 않는다 — ``listMembers()`` 와 같은 이유.
        let snapshot = queue.sync { closing || ended ? nil : socket }
        guard let socket = snapshot, socket.status == .connected else {
            throw AdapterError("NotConnected", "socket is not connected")
        }
        // 성공한 등록 id 는 ack 를 받은 **소켓 큐 턴 안에서**, 기다리는 쪽을 깨우기 전에 클라이언트 사본에 적는다 — 서버는
        // 등록 ack 바로 뒤에 새 등록의 보유자 통지를 보낸다. 사본을 클라이언트 액터에서 고치면 그 통지가 먼저 처리돼(같은
        // 소켓 큐의 다음 패킷) 옛 등록의 것으로 걸러졌고, 다음 renew 까지 게이트가 닫혔다(2차 검수: 5/5).
        let mirror = queue.sync { registrationMirror }
        guard let reply = await call(socket: socket, event: event, payload: payload, onReply: { reply in
            if let reply, reply["error"] == nil, let id = reply["registrationId"] as? String { mirror?.set(id) }
        }) else {
            throw AdapterError("AckTimeout", "\(event) timed out")
        }
        let error = reply["error"] as? String
        guard error == nil, let registrationId = reply["registrationId"] as? String else {
            let hint: String?
            switch error {
            case "invalid-code":
                hint = "the code is wrong or expired — issue a new one and retype it"
            case "invalid-secret":
                hint = "the device secret no longer resolves — re-register from a fresh code"
            default:
                hint = "the server rejected the request — check the stage matches this session "
                    + "and the server supports remote input"
            }
            throw AdapterError(
                "ValidationError",
                "registration rejected: \(error ?? "malformed reply")",
                hint: hint
            )
        }
        return AdapterRegistrationResult(
            registrationId: registrationId,
            deviceSecret: wantSecret ? reply["deviceSecret"] as? String : nil
        )
    }

    public func close() async {
        // 동시에 부른 close() 는 먼저 시작한 것을 함께 기다린다 — 둘째가 곧바로 끊으면 첫째가 기다리던 퇴장이 잘렸다(2차 검수,
        // 실서버 ack 400ms: 15번 중 10번 유실).
        let running: Task<Void, Never> = queue.sync {
            if let closeTask { return closeTask }
            // 퇴장할지와 "닫는 중" 은 첫 close 의 **이 큐 턴**에서 정한다 — 그 뒤 끼어든 서버 끊김(leave 에 대한 응답)이나
            // 세션 종료를 `server-disconnect` · 재개로 읽지 않게.
            let leaving = !ended && socket?.status == .connected
            closing = true
            let task = Task { await self.performClose(leaving: leaving) }
            closeTask = task
            return task
        }
        await running.value
    }

    private func performClose(leaving: Bool) async {
        // 붙어 있으면 `stage:leave` 를 보내고 서버가 처리할 때까지(ack, 또는 처리한 서버가 네임스페이스를 끊을 때까지)
        // 기다린 뒤에 끊는다 — 상한 ``leaveAckTimeout``. 보내자마자 엔진을 닫으면 leave 가 엔진 버퍼에 남은 채
        // 사라질 수 있어(실서버 socket.io 4.8.3 실측: Ctrl+C 종료 8번 중 1번) 서버는 유예(~10초 — 서버 PPC_GUEST_GRACE_SEC 기본 10,
        // 2026-10-19 전 서버는 8) 뒤에야 좌석을 회수했고,
        // 그 안에 다시 띄운 프로그램이 `이름 (2)` 로 붙었다. 기다리는 동안의 서버 끊김은 그 leave 의 응답이므로
        // `server-disconnect` 로 읽지 않는다(``closing``).
        if leaving { await leave() }
        queue.sync {
            markEnded()
            disposeSocket()
        }
    }

    // 서버의 leave 처리: `stageHandlers.ts` 의 Leave 핸들러.
    /// How long close() waits at most for `stage:leave` to be processed. On a leave the server records the member as left
    /// (one DB write), acks and disconnects — normally one round trip plus one write, tens to hundreds of ms. Past 1 second
    /// the server or network is already struggling, so waiting longer gains little, and shutdown (Ctrl+C · closing the app)
    /// must not be held any longer — then it just disconnects (the server's grace period reclaims the seat).
    static let leaveAckTimeout: TimeInterval = 1

    /// Sends `stage:leave` with an ack and waits for whichever comes first: the ack, a server disconnect or the cap. Never hangs.
    private func leave() async {
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            let resumed = ResumeGuard()
            let finish: @Sendable () -> Void = {
                if resumed.claim() { continuation.resume() }
            }
            queue.async {
                guard let socket = self.socket, socket.status == .connected else { return finish() }
                self.leaveWaiter = finish
                socket.emitWithAck(Protocol.ClientEvents.leave, [:] as [String: Any])
                    .timingOut(after: SocketTransport.leaveAckTimeout) { _ in finish() }
                // 라이브러리 타임아웃은 소켓이 해제되면 발화하지 않는다(``call(socket:event:payload:)`` 주석) — 같은 기한의
                // 폴백을 따로 건다.
                self.queue.asyncAfter(deadline: .now() + SocketTransport.leaveAckTimeout + 0.05) { finish() }
            }
        }
        queue.sync { leaveWaiter = nil }
    }

    /// - Important: Call only on `queue`.
    private func disposeSocket() {
        // 버린 소켓의 ack 는 오지 않는다 — 기다리는 쪽을 깨운다.
        failPendingAcks()
        socket?.removeAllHandlers()
        manager?.disconnect()
        socket = nil
        manager = nil
    }

    // (적대검수 재현) — continuation 영구 미재개 → flush 영구 정지.
    /// Ack request + timeout.
    ///
    /// ⚠️ Don't rely on the library's `timingOut(after:)` alone — its timeout closure captures
    /// `[weak socket]`, so once the socket is released (reconnect · close) it **never fires**.
    /// This continuation would then never resume and the flush would stall forever.
    /// So the adapter sets its own fallback timeout with the same deadline.
    /// `onReply` is called in the socket queue turn where the ack arrived, **before** the waiter is woken.
    private func call(
        socket: SocketIOClient,
        event: String,
        payload: [String: Any],
        onReply: (@Sendable ([String: Any]?) -> Void)? = nil
    ) async -> [String: Any]? {
        await withCheckedContinuation { continuation in
            let resumed = ResumeGuard()
            queue.async {
                let id = self.nextAckId
                self.nextAckId += 1
                // 끊김 · 소켓 해제 · 종료가 곧바로 깨운다(``failPendingAcks()``).
                self.pendingAcks[id] = {
                    guard resumed.claim() else { return }
                    continuation.resume(returning: nil)
                }
                socket.emitWithAck(event, payload).timingOut(after: self.options.ackTimeout) { data in
                    self.pendingAcks[id] = nil
                    guard resumed.claim() else { return }
                    onReply?(data.first as? [String: Any])
                    continuation.resume(returning: data.first as? [String: Any])
                }
                // 소켓이 사라져도 반드시 깨어나는 폴백(라이브러리 타임아웃보다 살짝 뒤).
                self.queue.asyncAfter(deadline: .now() + self.options.ackTimeout + 0.5) {
                    self.pendingAcks[id] = nil
                    guard resumed.claim() else { return }
                    continuation.resume(returning: nil)
                }
            }
        }
    }

    // 종전에는 ack 시한(15초)까지 기다렸다: 끊긴 순간 진행 중이던 큐 flush 의 발행이 15초 동안 flush 를 붙잡아 재접속 뒤에도
    // 큐가 흐르지 않았고, 재접속 직후의 대기 판정(roster 조회)이 끊김에 걸리면 판정 창이 그만큼 열려 있었다. TS · Python 과 같다.
    /// Acks waiting for an answer — once the socket drops or is discarded, the answer never comes (a socket.io ack is bound to one connection).
    ///
    /// A disconnect, socket disposal or end finishes them right away with nil (→ `AckTimeout`). Touched only on `queue`.
    private var pendingAcks: [Int: @Sendable () -> Void] = [:]
    private var nextAckId = 0

    /// - Important: Call only on `queue`.
    private func failPendingAcks() {
        let waiters = Array(pendingAcks.values)
        pendingAcks.removeAll()
        waiters.forEach { $0() }
    }

    private func trimTrailingSlash(_ raw: String) -> String {
        var value = raw
        while value.hasSuffix("/") { value.removeLast() }
        return value
    }

    /// A one-shot latch that keeps a continuation from resuming twice (guards against duplicate library callbacks).
    private final class ResumeGuard: @unchecked Sendable {
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

    // ── 순수부(테스트 고정) ─────────────────────────────────────────────────

    /// Classifies a handshake rejection — the server puts a code (or a human-readable sentence) in `err.message`
    /// and a code in `err.data.code`. It decides from the string the library hands up.
    ///
    /// - Parameter includeAuth: Whether Unauthorized is terminal — on the first connect the JWT just received
    ///   was rejected, so it is terminal; while reconnecting it may have expired, so it is left to revalidation.
    static func classifyHandshakeError(_ raw: String?, includeAuth: Bool = true) -> AdapterError? {
        guard let raw, !raw.isEmpty else { return nil }
        if raw.contains(Protocol.protocolVersionMismatch) {
            return AdapterError(
                "ProtocolVersionMismatch",
                "protocol version mismatch (this adapter speaks \(Protocol.protocolVersion)) — update the adapter"
            )
        }
        // 서버는 app-ahead 거절을 사람이 읽는 문장으로 보낸다 — 문장도 함께 본다.
        if raw.contains(Protocol.appVersionAheadOfServer)
            || raw.contains(Protocol.appAheadMessageMarker) {
            return AdapterError("ClientAheadOfServer", "client app version is ahead of the server")
        }
        if includeAuth && raw.contains("Unauthorized") {
            return AdapterError("AuthRevoked", "handshake rejected: unauthorized")
        }
        return nil
    }

    /// The next step for a 400 — values the local pre-check (``Connect``) catches never get this far,
    /// so a remaining 400 is usually a deployment skew between server and adapter.
    static let validationSkewHint =
        "Server and adapter disagree on the request shape — usually a version skew. "
        + "Update whichever is older, then retry."

    /// Number of issues shown in the message. Beyond it, the rest is shortened to "(+N more)".
    private static let maxShownIssues = 3

    // 여섯 언어가 같은 집합이며 손으로 미러링한 목록이다(드리프트 가드는 `protocol.ts` 만 본다) — 언어 사이는 파리티
    // 가드(`docs/parity.json` declaredSets)가 이 선언을 집합으로 대조하고, 언어 안에서는 `shouldAbandonRejoin` 테스트가 잠근다.
    /// Failures on which automatic return must give up — waiting does not change them.
    ///
    /// Failures not listed here (notably `RoomEnded` = the session is not open yet, network errors, 429)
    /// are **something to wait for**, so it keeps retrying.
    static let nonRejoinableCodes: Set<String> = [
        "AuthRevoked",
        "ProtocolVersionMismatch",
        "ClientAheadOfServer",
        "ConfigError",
        "ValidationError",
    ]

    // `AuthRevoked` 만 보던 판정은 버전 스큐(`ProtocolVersionMismatch` · `ClientAheadOfServer`)와 본문 거부(`ValidationError` = 400)에서
    // **영구 재시도**에 빠졌다. 그 경로는 `onError` 도 내지 않아 앱에서는 아무 일도 일어나지 않는 것처럼 보인다 — 무인 설비가 조용히
    // 죽는 바로 그 모양이다.
    /// Decides whether to end the resume loop.
    static func shouldAbandonRejoin(code: String, autoRejoin: Bool) -> Bool {
        nonRejoinableCodes.contains(code) || (code == "RoomEnded" && !autoRejoin)
    }

    /// The server 400 body's `details` → one human-readable line. An empty string when there is nothing to show.
    ///
    /// Accepts both shapes — zod (`path: [...]`) and Fastify/AJV (`instancePath: "/displayName"`). The server
    /// error handler emits both paths as `details`, so reading only one would silently yield an empty sentence on
    /// some deployments.
    static func describeValidationIssues(_ details: Any?) -> String {
        guard let issues = details as? [Any] else { return "" }
        var rendered: [String] = []
        for issue in issues {
            guard let issue = issue as? [String: Any] else { continue }
            var field = ""
            if let path = issue["path"] as? [Any] {
                field = path.compactMap { segment -> String? in
                    if let text = segment as? String { return text }
                    if let number = segment as? Int { return String(number) }
                    return nil
                }.joined(separator: ".")
            } else if let instancePath = issue["instancePath"] as? String {
                field = instancePath.hasPrefix("/")
                    ? String(instancePath.dropFirst()).replacingOccurrences(of: "/", with: ".")
                    : instancePath.replacingOccurrences(of: "/", with: ".")
            }
            let text = (issue["message"] as? String)?
                .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            let line: String
            if !field.isEmpty && !text.isEmpty {
                line = "\(field) — \(text)"
            } else {
                line = field.isEmpty ? text : field
            }
            if !line.isEmpty { rendered.append(line) }
        }
        if rendered.isEmpty { return "" }
        let shown = rendered.prefix(maxShownIssues).joined(separator: "; ")
        let hidden = rendered.count - maxShownIssues
        return hidden > 0 ? "\(shown) (+\(hidden) more)" : shown
    }

    /// A ck_ credential with no session — points to the switch that lets this key start the session itself.
    static let adapterKeyNoSessionHint =
        "Open the stage in Connect first, or use a key that can start the session: create one with \"Let adapters start a session\" turned on "
        + "(Stage → Share → Adapter keys — a key keeps the setting it was issued with), then retry."

    /// ck_ rejection — revocation and expiry are per key, so "re-issue the share link" is the wrong remedy.
    static let adapterKeyRevokedHint = "Issue a new adapter key: Stage → Share → Adapter keys."

    // 여섯 언어 같은 문장.
    /// token/validate 503 `KeyMaterialUnavailable` — the server cannot read this stage's adapter key material. It is worth
    /// waiting, but the fix is on the server (revoking old keys · restoring the key material). The same sentence as ``AdapterError``'s default hint.
    static let keyMaterialUnavailableHint =
        "The server could not read the stored material of an adapter key on this stage — usually an older key left behind by a server-side key change; after 30 such failures from one address within a minute every key on the stage gets this answer until the minute ends. Ask whoever runs your Connect to revoke the stage's old keys (Stage → Share → Adapter keys) or to restore the server's key material, then retry."

    // 여섯 언어 같은 문장.
    /// A 401 while the key length is not 46 — the server looks at a `ck_` token's length too before sending it down the key path,
    /// so a truncated key is treated as a share token and gets a 401. "Revoked" would be the wrong diagnosis, so it states the
    /// length (the revoked-hint sentence stays in front as is).
    static func adapterKeyLengthHint(_ length: Int) -> String {
        "\(adapterKeyRevokedHint) This key has \(length) characters, but a server-issued key is \(Protocol.adapterKeyLength) (ck_ plus 43) — it looks truncated or altered; copy it again (Copy appears right after Create key), or create a new key."
    }

    // 여섯 언어 같은 함수.
    /// Adds the key-length diagnosis to a 401 rejection — validate() and doctor use the same judgment. Only when it is a 401
    /// mapped to AuthRevoked and the configured key's length is not 46: appends the length to the message and switches the hint
    /// to "truncated key". Otherwise returns it unchanged.
    static func adapterKeyLengthRejection(_ mapped: AdapterError, status: Int, keyLength: Int) -> AdapterError {
        guard status == 401, mapped.code == "AuthRevoked", keyLength != Protocol.adapterKeyLength else { return mapped }
        return AdapterError(
            "AuthRevoked",
            "\(mapped.message); the key has \(keyLength) characters, a server-issued key has \(Protocol.adapterKeyLength)",
            hint: adapterKeyLengthHint(keyLength))
    }

    /// Cap on `retryAfterMs` (10 minutes) — the server never says more than 60 s. Keeps an absurd value from making the rejoin sleep for hours.
    static let retryAfterMaxMs = 600_000

    /// `details.retryAfterMs` of a 429 · 503 envelope — accepts only a finite positive number (ms), rounds it up and clips it to
    /// the cap (10 minutes). Anything else (absent · string · boolean · 0 · negative · NaN · infinite) is unknown. A
    /// JSONSerialization boolean is an NSNumber too, so it is told apart the same way as `strictBool`. Values like 1e300 are
    /// clipped to the cap before conversion to Int, so they don't overflow.
    static func retryAfterMsOf(_ details: Any?) -> Int? {
        guard let object = details as? [String: Any],
              let number = object[Protocol.errorDetailsRetryAfterMs] as? NSNumber,
              CFGetTypeID(number) != CFBooleanGetTypeID()
        else { return nil }
        let value = number.doubleValue
        guard value.isFinite, value > 0 else { return nil }
        if value >= Double(retryAfterMaxMs) { return retryAfterMaxMs }
        return min(Int(value.rounded(.up)), retryAfterMaxMs)
    }

    /// The rejoin loop's next wait (seconds) — the larger of the usual delay and the floor the server gave (ms). The floor may exceed the cap.
    static func rejoinWait(delay: TimeInterval, retryAfterMs: Int?) -> TimeInterval {
        guard let retryAfterMs, retryAfterMs > 0 else { return delay }
        return max(delay, TimeInterval(retryAfterMs) / 1000)
    }

    // 여섯 언어 같은 문장.
    /// Hint for a join refused with 429.
    static let validateRateLimitedHint =
        "The server is refusing joins for now: too many failed join attempts from this network address (20 within 5 minutes — a program here still uses a revoked or mistyped token), or more than 60 joins a minute with one adapter key (programs restarting in a loop), or the server is validating more than 200 joins at this moment. Servers without the per-key join limit also refuse a sixth join from one address within a minute. Wait up to 5 minutes, then retry — and fix the program that causes it, or it happens again."

    /// 403 stage-locked — the token is valid, but the host has closed the stage to guests.
    static let stageLockedHint =
        "The stage is closed to guests (allowAnonymousGuests is false), and adapters join as guests. "
        + "Ask the stage owner or whoever runs your Connect to reopen it to guests, then retry."

    // 6언어 공통 순수 매퍼(TS mapValidateError 와 같은 분기·문구).
    /// token/validate failure → AdapterError. A pure mapper.
    /// The caller reads `code` from the server envelope's `error` (fallback `code`) and passes it in. The credential is always an
    /// adapter key (`ck_`) — 401/403 point to "a new adapter key", 404 to the session auto-start switch.
    /// `message` is for detecting a 403 `stage-locked`.
    static func mapValidateError(
        status: Int,
        code: String?,
        base: String,
        details: Any? = nil,
        message: String? = nil
    ) -> AdapterError {
        let codeSuffix = code.map { " \($0)" } ?? ""
        switch status {
        case 403 where message == "stage-locked":
            return AdapterError(
                "AuthRevoked",
                "stage is closed to guests (403 stage-locked)",
                hint: SocketTransport.stageLockedHint
            )
        case 401, 403:
            return AdapterError(
                "AuthRevoked",
                "adapter key rejected (\(status)\(codeSuffix)) — revoked, expired, or issued for another stage",
                hint: SocketTransport.adapterKeyRevokedHint
            )
        case 429:
            // 키 입장 429 는 이 주소의 누적 실패 캡(5분에 20번)이거나 키 하나의 분당 입장 상한(60)이다 — 키 상한이 없는 서버(이전 버전)는
            // 키 입장에도 IP 당 분당 5회 관문을 쓴다(여섯 번째가 429).
            // 서버가 `details.retryAfterMs` 로 창의 남은 시간을 말하면 문장 · 힌트 · 재조인 하한에 싣는다.
            let retryAfterMs = SocketTransport.retryAfterMsOf(details)
            let wait = retryAfterMs.map { Int((Double($0) / 1000).rounded(.up)) }
            return AdapterError(
                "TooManyRequests",
                "token/validate rate limited (\(status)) — " + (wait.map { "retry in \($0) s" } ?? "retry later"),
                hint: wait.map { "Retry in \($0) s (the server's retryAfterMs). " + SocketTransport.validateRateLimitedHint }
                    ?? SocketTransport.validateRateLimitedHint,
                retryAfterMs: retryAfterMs
            )
        case 503 where code == "KeyMaterialUnavailable":
            // 서버가 이 스테이지의 어댑터 키 재료를 읽지 못한다 — 맞는 키도 창이 끝날 때까지 같은 답을 받는다. 기다릴
            // 일(재조인은 계속)이고 고치는 자리는 서버다. 다른 5xx 와 가르지 않으면 코드가 봉투값 그대로 올라가도 힌트 · 재시도 간격이 없다.
            let retryAfterMs = SocketTransport.retryAfterMsOf(details)
            let wait = retryAfterMs.map { Int((Double($0) / 1000).rounded(.up)) }
            return AdapterError(
                "KeyMaterialUnavailable",
                "token/validate refused (\(status) KeyMaterialUnavailable) — the server cannot read this stage's adapter keys right now"
                    + (wait.map { "; retry in \($0) s" } ?? ""),
                hint: wait.map { "Retry in \($0) s (the server's retryAfterMs). " + SocketTransport.keyMaterialUnavailableHint }
                    ?? SocketTransport.keyMaterialUnavailableHint,
                retryAfterMs: retryAfterMs
            )
        case 404 where code == "StageArchived":
            // 토큰은 유효하지만 스테이지가 보관됨 — "세션 없음" 과 처방이 다르다.
            return AdapterError("StageArchived", "stage is archived (404 StageArchived)")
        case 404:
            // cloud 에서 UI base(…/connect)로 치면 Next.js 404 — url 모양으로 진단.
            // 말미 슬래시를 떼고 본다 — 붙어 있으면 오설정을 놓친다(TS 미러).
            var trimmed = base
            while trimmed.hasSuffix("/") { trimmed.removeLast() }
            let uiBase = trimmed.hasSuffix("/connect") && !trimmed.hasSuffix("/api/connect")
            return AdapterError(
                "RoomEnded",
                "stage not found or has no live session (\(status))",
                hint: uiBase
                    ? "The url \"\(trimmed)\" looks like the Connect web UI base; the cloud API base "
                      + "is usually \"\(String(trimmed.dropLast("/connect".count)))/api/connect\" — "
                      + "re-copy the adapter token from the Share dialog."
                    : SocketTransport.adapterKeyNoSessionHint
            )
        case 400:
            // 서버가 **어느 필드가 왜** 틀렸는지 details 로 말해주는데, 그걸 버리면 "400" 만
            // 남아 토큰 문제처럼 읽힌다(실사고: clientType 개명 후 구 클라이언트).
            let desc = SocketTransport.describeValidationIssues(details)
            return AdapterError(
                "ValidationError",
                "token/validate rejected the request body (400)"
                    + (desc.isEmpty ? "" : ": \(desc)"),
                hint: SocketTransport.validationSkewHint
            )
        default:
            return AdapterError(code ?? "Unauthorized", "token/validate failed (\(status))")
        }
    }

    /// Server roster entry → ``Member``.
    ///
    /// Three places (joined's me · member-added · list-members) read the same shape. If only one place
    /// picked up a field, per-place mismatches like "visible in the list but absent from presence" would
    /// appear, so it is one function. An undeclared value stays nil — filling it with `"none"` would make
    /// "not declared" indistinguishable from "no screen" and make an older server look like a newer one.
    static func toMember(
        _ member: [String: Any]?,
        fallbackId: String,
        fallbackName: String
    ) -> Member {
        Member(
            participantId: member?["participantId"] as? String ?? fallbackId,
            displayName: member?["displayName"] as? String ?? fallbackName,
            providesView: member?["providesView"] as? String,
            // 불리언이 아니면(구 서버 = 부재) nil 로 남긴다.
            standby: strictBool(member?["standby"]),
            // 어댑터 자기신고 — 전부 optional 이고 부재/null/타입 불일치는 nil. 서버는 미신고를
            // null(NSNull) 로 보낸다(StageMember.receives?: string[] | null).
            clientType: member?["clientType"] as? String,
            receives: stringArray(member?["receives"]),
            sends: stringArray(member?["sends"]),
            strictRouting: strictBool(member?["strictRouting"]),
            exclusiveName: strictBool(member?["exclusiveName"]),
            osHostname: (member?["guestOSHostname"] as? String).flatMap { $0.isEmpty ? nil : $0 }
        )
    }

    /// Keeps only the string items of a string array. Not an array (absent · null · type mismatch) → nil.
    /// `as? [String]` turns a mixed array into nil as a whole, so it filters item by item.
    static func stringArray(_ value: Any?) -> [String]? {
        guard let items = value as? [Any] else { return nil }
        return items.compactMap { $0 as? String }
    }

    // 그러면 `"strictRouting": 1` 이 다른 다섯 언어(불리언 타입만 인정 → 부재)와 달리 true 로 읽혀 listeners() 분류가 갈린다.
    /// Reads only JSON booleans as Bool. A number 0/1 that went through JSONSerialization is an NSNumber, which `as? Bool`
    /// **lets through** (Foundation bridging), so `"strictRouting": 1` would read as true and split the listeners()
    /// classification. It is a value only when it is a CFBoolean.
    static func strictBool(_ value: Any?) -> Bool? {
        if let num = value as? NSNumber {
            return CFGetTypeID(num) == CFBooleanGetTypeID() ? num.boolValue : nil
        }
        return value as? Bool
    }

    static func mapHistory(_ items: [Any]) -> [StageMessage] {
        items.compactMap { toStageMessage($0) }
    }

    static func toStageMessage(_ payload: Any?) -> StageMessage? {
        guard let object = payload as? [String: Any],
              let channel = object["channel"] as? String else { return nil }

        let value: Primitive
        if object.keys.contains("value") {
            guard let primitive = Primitive.from(object["value"]) else { return nil }
            value = primitive // 객체/배열은 와이어 금지 — 잘못된 프레임은 버린다
        } else {
            value = .null
        }

        let timestamp = (object["timestamp"] as? NSNumber)?.int64Value
            ?? Int64(Date().timeIntervalSince1970 * 1000)

        return StageMessage(
            channel: channel,
            value: value,
            sourceParticipantId: object["sourceParticipantId"] as? String ?? "",
            timestamp: timestamp,
            messageId: object["messageId"] as? String
        )
    }
}

extension SocketTransport: DeclaresProvidesView {
    /// The connect option's providesView — the value whose declaration was requested.
    var declaredProvidesView: String? { options.providesView }
}
