import Foundation

/// A value that can travel on the wire — objects and arrays are forbidden; use the JSON-string convention (`sendJSON`).
public enum Primitive: Sendable, Equatable,
    ExpressibleByStringLiteral, ExpressibleByIntegerLiteral,
    ExpressibleByFloatLiteral, ExpressibleByBooleanLiteral, ExpressibleByNilLiteral {

    case string(String)
    case int(Int)
    case double(Double)
    case bool(Bool)
    case null

    public init(stringLiteral value: String) { self = .string(value) }
    public init(integerLiteral value: Int) { self = .int(value) }
    public init(floatLiteral value: Double) { self = .double(value) }
    public init(booleanLiteral value: Bool) { self = .bool(value) }
    public init(nilLiteral: ()) { self = .null }

    /// The representation sent over Socket.IO.
    public var jsonValue: Any {
        switch self {
        case .string(let value): return value
        case .int(let value): return value
        case .double(let value): return value
        case .bool(let value): return value
        case .null: return NSNull()
        }
    }

    public var stringValue: String? {
        if case .string(let value) = self { return value }
        return nil
    }

    public var doubleValue: Double? {
        switch self {
        case .int(let value): return Double(value)
        case .double(let value): return value
        default: return nil
        }
    }

    public var boolValue: Bool? {
        if case .bool(let value) = self { return value }
        return nil
    }

    public var isNull: Bool {
        if case .null = self { return true }
        return false
    }

    /// Converts a value from the socket or JSON into a primitive — nil for an object or array (the frame is discarded).
    public static func from(_ any: Any?) -> Primitive? {
        switch any {
        case nil, is NSNull: return Primitive.null
        case let value as String: return .string(value)
        // ⚠️ 여기에 `case let value as Bool` 를 두면 안 된다 — JSONSerialization 이 만드는
        // NSNumber 는 값이 0/1 이면 Bool 로 브리지되므로 숫자 0·1·0.0·1.0 이 전부 .bool 로
        // 뒤바뀐다(적대검수에서 실측). NSNumber 케이스가 CFType 으로 정확히 가른다.
        case let value as NSNumber:
            if CFGetTypeID(value) == CFBooleanGetTypeID() { return .bool(value.boolValue) }
            let double = value.doubleValue
            return double == double.rounded() && abs(double) < 9_007_199_254_740_992
                ? .int(value.intValue)
                : .double(double)
        case let value as Int: return .int(value)
        case let value as Double: return .double(value)
        default: return nil
        }
    }
}

/// A stage participant.
public struct Member: Sendable, Equatable {
    public let participantId: String
    public let displayName: String
    /// How the participant declared it provides a view — `"none" | "web" | "stream"` (nil when not declared).
    ///
    /// **The read side is deliberately a loose `String`** (the send side, `ConnectOptions.providesView`, is
    /// validated against a closed set). When the server adds a new kind later, code that reads it narrowly would
    /// discard the value and **hide an adapter that provides a view** — a silent regression. So read it as
    /// "provides a view unless it is nil or 'none'".
    public let providesView: String?
    // (설계 문서 T4)
    /// The server's judgment — this participant is in standby now because **another instance with the same name is active**.
    ///
    /// Present only for participants that declared `exclusiveName`. **nil and false differ**:
    /// nil = the rule does not apply (or an older server makes no judgment at all), false = declared and currently active.
    public let standby: Bool?
    /// Which client software the participant joined with — the only value today is `"adapter"`. Browsers, desktop
    /// and native clients do not declare it, so it is nil for them. It is self-declared, not an auth boundary — for display only.
    public let clientType: String?
    /// The channels the participant **declared** it receives (`receives` on join). nil = not declared = a participant
    /// the server delivers every broadcast to (browsers and prototypes included). Ask "does it listen to this channel"
    /// with `declaresChannel(_:)`, and read nil as "unknown".
    public let receives: [String]?
    /// The channels the participant declared it sends (`sends` on join). nil = not declared.
    public let sends: [String]?
    /// Whether declaration-based routing is on — when true, channels outside `receives` are not delivered to this participant.
    public let strictRouting: Bool?
    /// Whether the participant declared "only one instance with this display name is active at a time" (the premise of the `standby` judgment).
    public let exclusiveName: Bool?
    /// The OS hostname of the connecting device (`osHostname` on join, server field `guestOSHostname`). For diagnostics.
    public let osHostname: String?

    public init(
        participantId: String,
        displayName: String,
        providesView: String? = nil,
        standby: Bool? = nil,
        clientType: String? = nil,
        receives: [String]? = nil,
        sends: [String]? = nil,
        strictRouting: Bool? = nil,
        exclusiveName: Bool? = nil,
        osHostname: String? = nil
    ) {
        self.participantId = participantId
        self.displayName = displayName
        self.providesView = providesView
        self.standby = standby
        self.clientType = clientType
        self.receives = receives
        self.sends = sends
        self.strictRouting = strictRouting
        self.exclusiveName = exclusiveName
        self.osHostname = osHostname
    }

    /// Whether the participant **declared** `channel` — an exact match with a `receives` entry, or, when the entry
    /// is a wildcard pattern, a match by the same rules as `on()`. The argument is a **concrete channel name** —
    /// patterns live only on the declaring side (the `receives` entries).
    ///
    /// A participant whose `receives` is nil returns **false**. Undeclared does not mean "does not listen" but "did
    /// not declare" (a broadcast receiver), so deciding "does nobody listen to this channel" needs no match among the
    /// declaring participants **and** no undeclared participant — the caller checks the two conditions separately
    /// (counting undeclared ones with `receives == nil`).
    public func declaresChannel(_ channel: String) -> Bool {
        guard let declared = receives else { return false }
        return declared.contains { $0 == channel || Wildcard.matchChannel($0, channel) }
    }
}

/// The answer of `client.listeners(_:)` — the roster without yourself, split by who would receive this channel.
/// A participant that declared other channels and **turned strictRouting on** does not receive it, so it is in neither list.
public struct Listeners: Sendable, Equatable {
    public let channel: String
    /// Participants for which `Member.declaresChannel(_:)` is true — they declared they receive this channel.
    public let declared: [Member]
    /// Participants that receive it without declaring it — `receives` is nil (broadcast receivers such as browsers
    /// and prototypes), or they declared only other channels but did not turn on routing, so they still receive everything.
    public let broadcast: [Member]

    public init(channel: String, declared: [Member], broadcast: [Member]) {
        self.channel = channel
        self.declared = declared
        self.broadcast = broadcast
    }
}

/// An incoming message — live fan-out and history replay share this shape.
public struct StageMessage: Sendable, Equatable {
    public let channel: String
    public let value: Primitive
    public let sourceParticipantId: String
    public let timestamp: Int64
    public let messageId: String?

    public init(
        channel: String,
        value: Primitive,
        sourceParticipantId: String,
        timestamp: Int64,
        messageId: String? = nil
    ) {
        self.channel = channel
        self.value = value
        self.sourceParticipantId = sourceParticipantId
        self.timestamp = timestamp
        self.messageId = messageId
    }
}

/// Publish input.
public struct PublishInput: Sendable, Equatable {
    public let channel: String
    public let value: Primitive
    public let targetParticipantId: String?

    public init(channel: String, value: Primitive, targetParticipantId: String? = nil) {
        self.channel = channel
        self.value = value
        self.targetParticipantId = targetParticipantId
    }
}

/// The server's ack result — when no ack is requested, it returns at once with `ok == true`.
public struct PublishResult: Sendable, Equatable {
    public let ok: Bool
    public let code: String?
    public let message: String?
    public let messageId: String?

    public init(ok: Bool, code: String? = nil, message: String? = nil, messageId: String? = nil) {
        self.ok = ok
        self.code = code
        self.message = message
        self.messageId = messageId
    }
}

/// The join result.
public struct Joined: Sendable, Equatable {
    public let roomId: String
    public let me: Member

    public init(roomId: String, me: Member) {
        self.roomId = roomId
        self.me = me
    }
}

/// Events a transport pushes up to the core — all optional, so a fake can implement only some of them.
/// `onHistory` is the `stage:joined` payload of the first join and of every reconnect; an empty array is a
/// clearHistory broadcast.
public struct TransportHandlers: Sendable {
    public var onMessage: (@Sendable (StageMessage) -> Void)?
    public var onHistory: (@Sendable ([StageMessage]) -> Void)?
    public var onMemberJoin: (@Sendable (Member) -> Void)?
    public var onMemberLeave: (@Sendable (String) -> Void)?
    public var onState: (@Sendable (TransportState) -> Void)?
    public var onError: (@Sendable (AdapterError) -> Void)?
    public var onEnded: (@Sendable (String?) -> Void)?

    /// Raw `stage:signaling` frames (experimental). The adapter never interprets them — the path a
    /// screen-broadcasting adapter uses to negotiate directly with the Stage canvas.
    public var onSignaling: (@Sendable ([String: Any]) -> Void)?

    // (§6-5, N4)
    /// `adapter:lease-holder` — the remote-input holder notice. The adapter accepts input only from the holder
    /// named in this notice, and discards all input when there is no holder.
    public var onAdapterLeaseHolder: (@Sendable ([String: Any]) -> Void)?

    /// `stage:ice-changed` — fetch the ICE servers again (the server revoked this participant's TURN credential, or
    /// paused or resumed TURN issuance). The client drops its cached list.
    public var onIceChanged: (@Sendable ([String: Any]) -> Void)?

    /// Yourself as of each successful join (the first, a reconnect, a rejoin, and the older-server fallback without a
    /// payload) — called **right before** that join's `connected` is announced. A rejoin can carry a different
    /// participantId (the guest JWT lives 6 hours; a lost cookie) — the client updates ``ConnectClient/me`` with it
    /// before handling that `connected` (the standby judgment, listeners and the screen broadcast see the current self).
    public var onJoined: (@Sendable (Member) -> Void)?

    public init() {}
}

// (§6-2)
/// The result of a remote-input registration. A submit carries both fields; a resume carries only ``registrationId``
/// (``deviceSecret`` is its input, so it stays as it was).
public struct AdapterRegistrationResult: Sendable, Equatable, CustomStringConvertible {
    public let registrationId: String
    /// The secret for carrying the registration over a reconnect — the server issues it only on submit. **Secret**:
    /// never write it to logs or diagnostics. `description` redacts it too, so logging the whole result by mistake
    /// does not leak the secret.
    public let deviceSecret: String?

    public init(registrationId: String, deviceSecret: String? = nil) {
        self.registrationId = registrationId
        self.deviceSecret = deviceSecret
    }

    public var description: String {
        "AdapterRegistrationResult(registrationId: \(registrationId), "
            + "deviceSecret: \(deviceSecret == nil ? "nil" : "<redacted>"))"
    }
}

/// The result of an ICE-server lookup. ``expiresAt`` is when to ask again: with TURN credentials it is their expiry;
/// newer servers also send it with a STUN-only answer (a re-check time — TURN may appear later). Without it nothing
/// expires, so holding the cache for a long time is fine. The client asks again at half the remaining lifetime.
public struct IceServersResult: Sendable {
    public let iceServers: [IceServer]
    /// Epoch ms. Fetch again before this time or the relay drops.
    public let expiresAt: Int64?

    public init(iceServers: [IceServer], expiresAt: Int64?) {
        self.iceServers = iceServers
        self.expiresAt = expiresAt
    }

    /// The lookup failed — the list is either empty (fail-open) or the public STUN servers received because the
    /// authenticated route failed for a reason other than 404 (temporary). The client does not cache it, and the next
    /// call asks again (internal — the public shape is unchanged).
    internal var failed = false

    /// The value ``SocketTransport`` returns on failure.
    static var lookupFailed: IceServersResult { degraded([]) }

    /// A temporary list marked as failed.
    static func degraded(_ servers: [IceServer]) -> IceServersResult {
        var result = IceServersResult(iceServers: servers, expiresAt: nil)
        result.failed = true
        return result
    }
}

/// A transport that knows the providesView it **asked** to declare via the connect option — the screen-broadcast
/// helper decides `stream-undeclared` from this, not from the self the server returned (``ConnectClient/me``),
/// because on older servers and fallback joins that self lacks the field.
protocol DeclaresProvidesView: AnyObject {
    var declaredProvidesView: String? { get }
}

/// The result of ``ConnectClient/ensureStreamLayer()`` — points at one layer, whether it was just created or
/// already existed. The transport fills `renamed` (the name received on join ≠ the requested name), and the client
/// sets `nameConflict` to the same value.
public struct EnsuredStreamLayer: Sendable, Equatable {
    public let layerId: String
    public let name: String
    /// Whether this call created it — server 201 = true, 200 (already existed) = false.
    public let created: Bool
    // 호환용 옛 이름. roster 에서 같은 이름의 활성 참여자를 찾던 판정은 크래시 재시작 뒤 유예 중인 같은 키의 옛 행과 대기 짝을
    // 충돌로 잘못 셌다(검수) — 이제 ``renamed`` 와 같다.
    /// Deprecated — the same value as ``renamed``; read ``renamed``.
    ///
    /// A compatibility alias kept under its old name — now the same as ``renamed``.
    public let nameConflict: Bool
    /// The server attached this adapter under a different name than the one requested (``ConnectClient/me``'s
    /// displayName ≠ the requested displayName — e.g. `gauge (2)`) — a different adapter key holds that name: a program
    /// on another key is attached under that name, or a layer of that name is still bound to another valid key (a
    /// common cause: the key was rotated without revoking the old one). Rename, or revoke the old key. The same key is
    /// never renamed (on servers that share a name within one key). The layer is ensured under the attached name, so
    /// this screen does not appear in the layer the designer placed under the original name.
    public let renamed: Bool

    public init(layerId: String, name: String, created: Bool, nameConflict: Bool = false, renamed: Bool = false) {
        self.layerId = layerId
        self.name = name
        self.created = created
        self.nameConflict = nameConflict
        self.renamed = renamed
    }
}

/// Implementations: ``SocketTransport`` and test fakes.
public protocol Transport: AnyObject, Sendable {
    func open(handlers: TransportHandlers) async throws -> Joined
    func publish(_ input: PublishInput, ack: Bool) async -> PublishResult
    func listMembers() async throws -> [Member]
    /// Outbound `stage:signaling` (experimental). A no-op on transports without support.
    func sendSignaling(_ payload: [String: Any]) async
    /// Ensures an Adapter Stream layer exists (experimental) — only transports with REST.
    /// The server derives the name from the calling member's displayName, so it takes no arguments.
    func ensureStreamLayer() async throws -> EnsuredStreamLayer
    /// This stage's ICE servers — the **authenticated** route (`GET /api/stages/:id/ice`).
    /// TURN credentials are issued short-lived, so they never come through the public `/api/about`.
    func fetchIceServers() async -> IceServersResult
    // (§6-2)
    /// Submits a remote-input registration code — only transports with a socket surface.
    func submitRegistrationCode(_ code: String) async throws -> AdapterRegistrationResult
    // (§6-2)
    /// Carries the registration over a reconnect with the deviceSecret.
    func resumeRegistration(stageId: String, deviceSecret: String) async throws
        -> AdapterRegistrationResult
    func close() async
    // (E6) 테스트 대역(``MemoryTransport``)만 true 다 — 거기에 버킷을 걸면 100건 발행이 30건 뒤 초당 60건으로 늘어져, 그 대역으로
    // 앱을 시험하는 쪽이 실서버에 없는 지연을 본다.
    /// Whether the transport has no server publish limit — when true, the client skips the token bucket (60 per second,
    /// burst 30). Default false. Only the test double (``MemoryTransport``) sets it.
    var bypassRateLimit: Bool { get }
}

extension Transport {
    /// Default false — a real server (and fakes that imitate it) applies a publish limit per socket.
    public var bypassRateLimit: Bool { false }

    /// Default no-op — signaling is an **optional capability**, so implementations without this path (the Engine
    /// profile, test fakes) do not break because of it.
    public func sendSignaling(_ payload: [String: Any]) async {}

    /// Default rejects — ensuring a layer is an **optional capability** that needs REST.
    /// (⚠️ With only an extension default and no protocol requirement, calls through the existential would always be
    /// statically dispatched to this default and ignore the real implementation — hence the requirement-plus-default pair.)
    public func ensureStreamLayer() async throws -> EnsuredStreamLayer {
        throw AdapterError(
            "NotSupported",
            "this profile cannot ensure layers — this transport has no REST surface"
        )
    }

    /// Default empty result — for transports without a REST surface (Engine, fakes); the client reads it as "no ICE
    /// servers" (it does not fall back to the public route — only the transport talks to the server directly). (The same
    /// requirement-plus-default pair as ensureStreamLayer.)
    public func fetchIceServers() async -> IceServersResult {
        IceServersResult(iceServers: [], expiresAt: nil)
    }

    /// Default rejects — registration is an **optional capability** that needs a socket surface
    /// (the same requirement-plus-default pair as ensureStreamLayer).
    public func submitRegistrationCode(_ code: String) async throws -> AdapterRegistrationResult {
        throw AdapterError("NotSupported", "this transport cannot register for remote input")
    }

    /// Default rejects — for the same reason as ``submitRegistrationCode(_:)``.
    public func resumeRegistration(stageId: String, deviceSecret: String) async throws
        -> AdapterRegistrationResult
    {
        throw AdapterError("NotSupported", "this transport cannot register for remote input")
    }
}
