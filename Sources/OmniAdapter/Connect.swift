import Foundation

/// Connection options — credentials come in two forms only: ① `secretKey` = a self-describing `cst_…` token
/// (issued from Share → Adapter keys) ② `url` + `stageId` + `secretKey` = a `ck_…` adapter key. Any other value
/// is a ConfigError. Explicit values take precedence over the token's contents.
public struct ConnectOptions: Sendable {
    public var secretKey: String?
    public var url: String?
    public var stageId: String?

    /// The name shown on the Backstage canvas and in the Console.
    public var displayName: String?

    /// The machine name on the node label.
    public var osHostname: String?

    /// Receive declarations (exact channel names, at most 32) — they become Backstage port chips.
    public var receives: [String]?

    /// Send declarations.
    public var sends: [String]?

    /// How a view is provided: none|web|stream. Unset = none.
    public var providesView: String?

    /// **One active per name, opt-in** — declares `displayName` as this adapter's contract.
    ///
    /// When two instances with the same name are alive, values go out twice and a view slot cannot tell which
    /// one it shows, so the server treats only the most recently joined one as active. An adapter that becomes
    /// standby keeps receiving and stops sending (standby is the spare — it takes over when the active one dies).
    public var exclusiveName: Bool?

    /// The server delivers only the declared channels (+ messages targeted at you). Requires exact `receives`
    /// (no wildcards).
    public var strictRouting = false

    public var socketPath: String?
    public var reconnection = true
    /// Whether to **wait for a closed session to reopen and rejoin automatically** (default true).
    /// While waiting, the state is `.reconnecting` and publishes pile up in the offline queue. If waiting cannot fix
    /// the failure — a revoked adapter key (`AuthRevoked`), a protocol version the server does not accept
    /// (`ProtocolVersionMismatch`, `ClientAheadOfServer`), or a rejected configuration or request (`ConfigError`,
    /// `ValidationError`) — it does not wait and ends with `.ended` right away.
    public var autoRejoin = true
    /// Session-resume check interval — initial/maximum (default 2s / 60s, growing exponentially).
    public var rejoinDelay: TimeInterval = 2
    public var rejoinDelayMax: TimeInterval = 60
    public var joinTimeout: TimeInterval = 15
    /// How long to wait for an ack (seconds, default 15) — finite, greater than 0 and at most 2^31−1 ms (2147483.647 s). Anything else is a `ConfigError`.
    public var ackTimeout: TimeInterval = 15
    public var queueLimit: Int = ConnectClient.defaultQueueLimit

    /// Receives diagnostics ring entries **as they are** and forwards them to the host logger. The ring fills even
    /// when the app does nothing (read it afterwards with diagnostics()), but an unattended program has to keep what
    /// happened "right now" in its own log — this is that channel. The adapter keeps running even if the sink throws.
    /// Credentials never appear in it.
    public var onLog: (@Sendable (LogEntry) -> Void)?

    /// Forces polling on networks where WebSocket is blocked.
    public var usePolling = false

    public init(
        secretKey: String? = nil,
        url: String? = nil,
        stageId: String? = nil,
        displayName: String? = nil,
        osHostname: String? = nil,
        receives: [String]? = nil,
        sends: [String]? = nil,
        providesView: String? = nil,
        strictRouting: Bool = false
    ) {
        self.secretKey = secretKey
        self.url = url
        self.stageId = stageId
        self.displayName = displayName
        self.osHostname = osHostname
        self.receives = receives
        self.sends = sends
        self.providesView = providesView
        self.strictRouting = strictRouting
    }
}

// 다른 언어 어댑터의 `connect()` 미러.
/// The entry point for joining a stage.
///
/// ```swift
/// let client = try await connect(ConnectOptions(secretKey: "cst_…", displayName: "iPad Wall"))
/// await client.on("actuator.cmd") { message in print(message.value) }
/// try await client.send("sensor.temperature", 23.5)
/// ```
public func connect(_ options: ConnectOptions) async throws -> ConnectClient {
    // secretKey 프리픽스 분기 — cst_ 는 풀어서 좌표+ck_ 키가 되고, ck_ 는 그대로 쓰되
    // url+stageId 를 함께 받아야 한다. 그 외 문자열은 거부 — 조용히 서버에 넘겨 401 로
    // 헤매게 두지 않는다.
    guard let given = options.secretKey, !given.isEmpty else {
        throw AdapterError(
            "ConfigError",
            "connect() requires a credential — secretKey: \"cst_…\" or url + stageId + secretKey: \"ck_…\"",
            hint: "pass an adapter token (Share → Adapter keys → Create key, then Copy) — if you read it from an environment variable, it is not set in this process"
        )
    }
    // 공백이 먼저다 — 앞 공백은 아래 프리픽스 검사에 걸려 "cst_ 토큰이 아니다" 로, `ck_…` 뒤 줄바꿈은 서버까지
    // 가서 "키가 회수됐다" 로 나가 원인(파일 · 환경변수에서 따라온 줄바꿈)이 가려진다.
    if StageToken.hasEdgeWhitespace(given) {
        throw AdapterError(
            "ConfigError", StageToken.credentialWhitespaceMessage, hint: StageToken.credentialWhitespaceHint)
    }
    guard StageToken.isStageToken(given) || StageToken.isAdapterKey(given) else {
        throw AdapterError(
            "ConfigError",
            "secretKey must be a cst_ token (Share → Adapter keys) or a \(StageToken.dedicatedKeyPrefix)… adapter key"
        )
    }
    let fromToken: StageTokenPayload? = StageToken.isStageToken(given) ? try StageToken.parse(given) : nil
    // 명시 값 > cst_ 토큰 내용 — 호출자가 아는 게 우선.
    let secretKey = fromToken?.secretKey ?? given
    guard let url = options.url ?? fromToken?.url, !url.isEmpty,
          let stageId = options.stageId ?? fromToken?.stageId, !stageId.isEmpty
    else {
        // ck_ 는 자기서술이 아니라서 이 실패에 잘 빠진다 — 무엇이 더 필요한지 말한다.
        throw AdapterError(
            "ConfigError",
            "an adapter key (ck_…) is not self-describing — pass url and stageId with it",
            hint: "create an adapter token instead (Share → Adapter keys → Create key, then Copy) — the cst_… token carries the server and stage for you"
        )
    }

    if options.strictRouting, options.receives?.isEmpty ?? true {
        throw AdapterError(
            "ConfigError",
            "strictRouting requires a receives declaration — the server delivers "
            + "only declared channels (plus messages targeted at you)"
        )
    }
    if options.strictRouting, options.receives?.contains(where: { $0.contains("*") }) == true {
        throw AdapterError(
            "ConfigError",
            "strictRouting receives must be exact channel names (no wildcards) — "
            + "the server matches declarations literally"
        )
    }

    let displayName = options.displayName ?? fromToken?.displayName ?? "Omni adapter"
    // 서버가 400 으로 거절하는 값들을 여기서 필드명과 함께 먼저 잡는다(TS 판 미러).
    guard (1...64).contains(displayName.count) else {
        throw AdapterError("ConfigError", "displayName must be 1..64 chars")
    }
    if let providesView = options.providesView, !Protocol.adapterProvidesViews.contains(providesView) {
        throw AdapterError(
            "ConfigError",
            "providesView must be one of \(Protocol.adapterProvidesViews.joined(separator: "|")) (got \"\(providesView)\")"
        )
    }

    let receives = try normalizeChannels("receives", options.receives)
    let sends = try normalizeChannels("sends", options.sends)
    try ConnectClient.validateQueueLimit(options.queueLimit)

    var transportOptions = SocketTransportOptions(url: url, stageId: stageId, secretKey: secretKey)
    transportOptions.displayName = displayName
    transportOptions.osHostname = options.osHostname
    transportOptions.receives = receives
    transportOptions.sends = sends
    transportOptions.providesView = options.providesView
    transportOptions.exclusiveName = options.exclusiveName
    transportOptions.strictRouting = options.strictRouting
    transportOptions.socketPath = options.socketPath
    transportOptions.reconnection = options.reconnection
    transportOptions.autoRejoin = options.autoRejoin
    transportOptions.rejoinDelay = options.rejoinDelay
    transportOptions.rejoinDelayMax = options.rejoinDelayMax
    transportOptions.joinTimeout = options.joinTimeout
    transportOptions.ackTimeout = options.ackTimeout
    transportOptions.usePolling = options.usePolling

    let client = ConnectClient(
        transport: SocketTransport(options: transportOptions),
        queueLimit: options.queueLimit,
        strictReceives: options.strictRouting ? receives : nil,
        // 대기 규칙은 신고한 어댑터에만 적용된다(TS/Python/Dart/Kotlin 과 같은 조건).
        exclusiveName: options.exclusiveName == true,
        onLog: options.onLog
    )
    do {
        try await client.open()
    } catch {
        // 실패한 접속이 만든 것을 남기지 않는다 — 호출자는 클라이언트 핸들을 받지 못하므로, 트랜스포트가
        // 남아 혼자 재접속 · 합류하면 아무도 닫을 수 없는 좌석이 된다.
        await client.close()
        throw error
    }
    return client
}

// 서버 zod 규칙과 같은 경계.
/// trim + dedupe + 1..512 characters + at most 32 entries — the same bounds as the server's validation.
private func normalizeChannels(_ field: String, _ input: [String]?) throws -> [String]? {
    guard let input else { return nil }
    // [] 는 "선언 없음"과 의미가 갈려 서버가 400 으로 거부한다(zod .min(1)). 그리고 ValidationError 는 terminal 이라 auto-rejoin 이 재시도하지 않는다 — 여기서 잡지 않으면 하드 크래시다. Dart 가 유일하게 막고 있던 것을 나머지 다섯에 맞춘다.
    guard !input.isEmpty else {
        throw AdapterError("ConfigError", "\(field) must not be empty — omit it entirely to declare nothing")
    }
    var seen: [String] = []
    for raw in input {
        let name = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard (1...512).contains(name.count) else {
            throw AdapterError("ConfigError", "\(field) entries must be 1..512 chars")
        }
        if !seen.contains(name) { seen.append(name) }
    }
    guard seen.count <= 32 else {
        throw AdapterError("ConfigError", "\(field) allows at most 32 channels")
    }
    return seen.isEmpty ? nil : seen
}

/// The shortest path: connect with a single `cst_…` token.
public func connect(secretKey: String, displayName: String? = nil) async throws -> ConnectClient {
    try await connect(ConnectOptions(secretKey: secretKey, displayName: displayName))
}
