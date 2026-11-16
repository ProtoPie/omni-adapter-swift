import Foundation

// 와이어 계약의 인라인 사본 — 원본은 Connect Omni 서버의 프로토콜 패키지.
// TS/Python/.NET/Kotlin 어댑터와 같은 전략이며, 값이 하나라도 어긋나면 드리프트 가드
// 테스트(`ProtocolDriftTests`)가 TS 어댑터의 `protocol.ts` 를 대조해 잡는다.
/// An inline copy of the wire contract — the original lives in the Connect Omni server's protocol package.
public enum Protocol {
    /// The wire protocol version, stamped on the handshake as `auth.protocolVersion`.
    /// The server gates on the same-MAJOR rule (every 0.x interoperates).
    public static let protocolVersion = "0.1.0"

    public static let protocolVersionMismatch = "ProtocolVersionMismatch"
    public static let appVersionAheadOfServer = "AppVersionAheadOfServer"

    // 드리프트 가드가 TS protocol.ts 와 대조한다.
    /// The fragment that identifies the app-ahead rejection **message**. For this rejection alone the
    /// server puts a human-readable sentence in `message` and keeps the code only inside `err.data` — the
    /// socket library surfaces only the string, so the rejection has to be classified by this text.
    public static let appAheadMessageMarker = "is older than your app"

    /// The adapter's self-reported client type — stamped on both the token/validate body and the handshake auth.
    /// The server does not branch on it (display only); it is what makes Backstage draw the participant as a
    /// connect-client node.
    public static let adapterClientType = "adapter"

    // 원본: 서버 stage.ts providesView enum.
    /// Ways of providing a view — kept in lockstep with the server's enum.
    /// What the Stage's view slots (Web View / Adapter Stream layers) use to narrow candidate
    /// adapters. Not reported = "none".
    public static let adapterProvidesViews = ["none", "web", "stream"]

    // 원본: 서버 AdapterKeyCipher `ADAPTER_KEY_LENGTH`.
    /// Length of an adapter key (`ck_`.count + ceil(32 bytes × 4 / 3) = 46). The server routes a `ck_` token
    /// on token/validate to the key path by its **length** too — a `ck_…` of another length goes down the
    /// share token path and gets a 401 counted as a failure. The adapter uses this to recognise a "truncated
    /// key" in that 401's hint and in doctor's credential check (it does not pre-validate — short keys from
    /// tests and mock servers are accepted as they are).
    public static let adapterKeyLength = 46

    // 원본: 서버 http-error.ts `tooManyRequests(retryAfterMs)` · `keyMaterialUnavailable(msg, retryAfterMs)`.
    /// The delay before the next attempt (ms) that the server puts in the `details` of a 429 or 503 envelope.
    public static let errorDetailsRetryAfterMs = "retryAfterMs"

    /// What the adapter reads from the `GET /api/about` response — diagnostics (version floor, deployment
    /// display, capability flags) and the ICE fallback.
    public static let aboutFields = ["version", "protocolVersion", "minClientProtocol", "runMode", "iceServers", "capabilities"]

    // 원본: 서버 config/capabilities.ts.
    /// Capability flags the adapter reads from `about.capabilities` — a build where `adapterGuest` is false
    /// (not Connect Omni: Desktop, Embedded, the integrated edition) rejects adapter-key entry with a 401, so
    /// doctor reports "a server that does not accept adapters" instead of "the key was revoked".
    public static let aboutCapabilityFields = ["adapterGuest"]

    // 어댑터 신원 — 서버 zod `.regex(/…/)` 의 원문, TS protocol.ts 와 락스텝.
    /// Adapter identity — the server's patterns for `adapterVersion` and `runtimeVersion` on token/validate.
    /// A value that does not match is not sent; the field is omitted (`AdapterIdentity`).
    public static let adapterVersionPattern = "^[0-9][0-9a-zA-Z.+-]{0,31}$"
    public static let runtimeVersionPattern = "^[0-9][0-9a-z.+-]{0,15}$"

    /// Client → server events.
    public enum ClientEvents {
        public static let joinRoom = "stage:join-room"
        public static let leave = "stage:leave"
        public static let message = "stage:message"
        public static let listMembers = "stage:list-members"

        /// WebRTC signaling passthrough (experimental). The adapter never interprets the payload —
        /// it is the path between a screen-broadcasting adapter and the Stage canvas.
        public static let signaling = "stage:signaling"

        // (§6-2)
        /// Adapter Stream remote input — adapter to server.
        public static let adapterRegisterSubmit = "adapter:register-submit"
        public static let adapterRegisterResume = "adapter:register-resume"
    }

    /// Server → client events.
    public enum ServerEvents {
        public static let joined = "stage:joined"
        public static let memberAdded = "stage:member-added"
        public static let memberRemoved = "stage:member-removed"
        public static let message = "stage:message"

        /// Used only for the clearHistory broadcast — `{history: []}`.
        public static let history = "stage:history"

        public static let ended = "stage:ended"
        public static let error = "stage:error"

        /// Inbound signaling — the same name as `ClientEvents.signaling`.
        public static let signaling = "stage:signaling"

        // 서버 events.ts 에는 이벤트가 더 있지만
        // 어댑터가 듣지 않는 것은 여기 두지 않는다(이 파일은 어댑터가 쓰는 부분집합).
        /// Adapter Stream remote input — server to adapter.
        public static let adapterLeaseHolder = "adapter:lease-holder"
        /// Fetch this stage's ICE servers again — the server revoked this participant's TURN credential, or the deployment
        /// paused or resumed new TURN issuance. The adapter drops its cached list; connections already up are left alone.
        public static let iceChanged = "stage:ice-changed"
    }

    // 서버에 원본이 없는 P2P 계약이라
    // Protocol 드리프트 가드가 아니라 typescript/src/remote-input-protocol.ts 를
    // 기준본으로 하는 교차 가드가 대조한다(§7-4). 계약은 §7.
    /// `coc.input` — the contract of the remote-input P2P data channel. It has no original on the server:
    /// coordinates flow peer-to-peer between viewer and adapter and never pass through the server.
    public enum RemoteInput {
        public static let channel = "coc.input"
        public static let version = 1
        public static let moveHz = 30

        // 뷰어 판정이 백그라운드만으로 꺼진 채 남는 것은 실측으로 확인했다.
        /// Hello re-announce interval (ms). Hello is sent not once when the channel opens but periodically
        /// for as long as the channel is alive — the viewer's "input available" judgment can stay off after
        /// the tab merely visits the background, and there is no re-request path, so the adapter's
        /// re-announcement is the only way to recover. The viewer's judgment is idempotent.
        public static let helloReannounceMs = 10_000

        // (N2)
        /// The `t` values the adapter accepts (any other is discarded).
        public static let adapterAccepts = ["down", "move", "up", "cancel"]
    }
}

/// Transport state.
public enum TransportState: String, Sendable {
    case idle
    case connecting
    case connected
    case reconnecting
    case ended
}
