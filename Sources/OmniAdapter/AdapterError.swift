import Foundation

/// An adapter error. Codes the server or the protocol already uses are passed through unchanged (never
/// reinvented); adapter-originated codes are limited to client-side conditions (configuration errors,
/// timeouts, queue overflow). `message` says what went wrong, `hint` says what to do about it.
public struct AdapterError: Error, CustomStringConvertible, Sendable, Equatable {
    /// A stable identifier to branch on.
    public let code: String
    public let message: String

    /// One line telling the user what to do next — print it on error paths.
    public let hint: String?

    /// The interval (ms) the server asked to wait before the next attempt — `details.retryAfterMs` in a
    /// token/validate 429 · 503 envelope. Automatic rejoin uses it as the lower bound of its delay. Absent on other errors.
    public let retryAfterMs: Int?

    public init(_ code: String, _ message: String? = nil, hint: String? = nil, retryAfterMs: Int? = nil) {
        self.code = code
        self.message = message ?? code
        self.hint = hint ?? AdapterError.defaultHints[code]
        self.retryAfterMs = retryAfterMs
    }

    public var description: String { "[\(code)] \(message)" }

    // 다른 언어 어댑터와 같은 문구.
    /// Default hint per code.
    public static let defaultHints: [String: String] = [
        // 세션이 없다 — 기본값도 키 경로(SocketTransport 의 adapterKeyNoSessionHint)와 같은 문장이다. 종전 문장
        // ("호스트가 안에 있어야 한다 · 게스트는 세션을 못 연다")은 "Let adapters start a session" 스위치와 어긋났다.
        "RoomEnded":
            "Open the stage in Connect first, or use a key that can start the session: create one with \"Let adapters start a session\" turned on "
            + "(Stage → Share → Adapter keys — a key keeps the setting it was issued with), then retry.",
        "AuthRevoked":
            "The credential was revoked or re-issued, or belongs to a different server. "
            + "Create a fresh adapter token: Stage → Share → Adapter keys → Create key.",
        "NotConnected":
            "Check the server address in your token (LAN IPs change — re-copy the "
            + "adapter token, or pass url to override).",
        "StageFull": "The stage's participant cap is full — free a seat or raise the cap.",
        "StageArchived":
            "The stage is archived — ask its owner to restore it (Home → Archived → Restore), then retry.",
        "ProtocolVersionMismatch":
            "This adapter and the server speak incompatible protocol versions — update one side.",
        "TooManyRequests": "Rate limited — wait a moment and retry.",
        // token/validate 503 — 서버가 이 스테이지의 어댑터 키 재료를 읽지 못한다. 여섯 언어 같은 문장.
        "KeyMaterialUnavailable":
            "The server could not read the stored material of an adapter key on this stage — usually an older key left behind by a server-side key change; after 30 such failures from one address within a minute every key on the stage gets this answer until the minute ends. Ask whoever runs your Connect to revoke the stage's old keys (Stage → Share → Adapter keys) or to restore the server's key material, then retry.",
        // 대기 중 ack 발행의 거절 — TS/Python/Dart 와 같은 문장(옵션 이름만 이 언어의 것).
        "Standby":
            "Another instance with the same displayName is active, so this one is standby "
            + "(receiving continues). It resumes automatically when the active instance leaves "
            + "— or turn the rule off with exclusiveName: false.",
        "JoinTimeout":
            "Connected but never admitted — verify the stage has a live session and "
            + "the network allows WebSocket.",
    ]
}
