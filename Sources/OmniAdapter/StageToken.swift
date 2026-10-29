import Foundation

/// The connection coordinates a `cst_` container holds.
public struct StageTokenPayload: Sendable, Equatable {
    public let url: String
    public let stageId: String
    public let secretKey: String
    public let displayName: String?
}

/// The adapter credential — two forms, one surface (`secretKey`).
/// `cst_…` is a self-describing envelope issued from Share → Adapter keys; it holds the server address,
/// stageId and adapter key (`k` = `ck_…`) as base64url JSON. ``encode(url:stageId:secretKey:displayName:)``
/// is an auxiliary path for issuing one programmatically (tests, provisioning). `ck_…` is the server-issued
/// adapter key itself; it is not self-describing and needs url+stageId.
///
/// Any other value (such as a share token for human participants) is rejected — a share token cannot be
/// revoked individually, so it is not used as an adapter credential.
///
/// ⚠️ This is **encoding**, not encryption (base64url JSON) — handle it by the same rules as the adapter key
/// inside it. The server does not know this format (only the `ck_` key goes on the wire).
public enum StageToken {
    public static let prefix = "cst_"

    // 서버 AdapterKeyCipher.ADAPTER_KEY_PREFIX 와 락스텝.
    /// The prefix of server-issued adapter keys.
    public static let dedicatedKeyPrefix = "ck_"

    // 6언어 공통(parity 프로브).
    /// The message used when the `k` inside a cst_ token is not an adapter key.
    public static let notAdapterKeyMessage =
        "stage token does not carry an adapter key (ck_…) — issue one from Share → Adapter keys"

    // 6언어 공통(벡터가 `whitespace` 조각을 잠근다).
    /// The message used when the token has leading or trailing whitespace.
    private static let whitespaceMessage =
        "stage token has leading or trailing whitespace — remove it (often a newline from a file or an environment variable)"

    // 6언어 공통 문장, 파리티 가드가 잠근다.
    /// The message used when the credential itself (`cst_` · `ck_`) has leading or trailing whitespace — connect(),
    /// doctor and report raise it **before looking at the prefix**. Otherwise leading whitespace would fail the prefix
    /// check as "not a cst_ token", and a newline after `ck_…` would go to the server as is and come back as "the
    /// key was revoked", hiding the cause.
    static let credentialWhitespaceMessage =
        "credential has leading or trailing whitespace — remove it (often a newline from a file or an environment variable)"
    static let credentialWhitespaceHint =
        "Trim the value before passing it in — the token or key itself is probably fine."

    /// Whether JSON whitespace (space · tab · LF · CR) surrounds the value — neither tokens nor keys contain
    /// whitespace. Checked per scalar — `"\r\n"` is a single `Character`.
    static func hasEdgeWhitespace(_ value: String) -> Bool {
        let scalars = value.unicodeScalars
        guard let first = scalars.first, let last = scalars.last else { return false }
        return StrictJSON.isWhitespace(first) || StrictJSON.isWhitespace(last)
    }

    // (TS `startsWith` 는 참이다).
    /// Whether the value is an adapter key (`ck_…`). The prefix is compared as UTF-8 bytes — `hasPrefix` works per
    /// character (grapheme), so it would be false when a combining character attaches to the prefix's last character.
    public static func isAdapterKey(_ value: String) -> Bool { value.utf8.starts(with: dedicatedKeyPrefix.utf8) }

    public static func isStageToken(_ value: String) -> Bool { value.utf8.starts(with: prefix.utf8) }

    public static func encode(
        url: String,
        stageId: String,
        secretKey: String,
        displayName: String? = nil
    ) throws -> String {
        guard !url.isEmpty, !stageId.isEmpty, !secretKey.isEmpty else {
            throw AdapterError("ConfigError", "encode requires url, stageId and secretKey")
        }
        guard let components = URLComponents(string: url),
              let scheme = components.scheme, !scheme.isEmpty,
              let host = components.host, !host.isEmpty
        else {
            throw AdapterError("ConfigError", "encode: url is not a valid URL: \(url)")
        }
        guard isAdapterKey(secretKey) else {
            throw AdapterError("ConfigError", "encode: \(notAdapterKeyMessage)")
        }

        // 키 순서까지 다른 언어 판과 같게 유지 — 같은 입력이면 같은 문자열이 나온다.
        var json = "{\"v\":1,\"u\":\(quote(url)),\"s\":\(quote(stageId)),\"k\":\(quote(secretKey))"
        if let displayName { json += ",\"n\":\(quote(displayName))" }
        json += "}"
        return prefix + base64UrlEncode(Data(json.utf8))
    }

    // 판정 순서는 여섯 언어 공통이다(`docs/conformance.json` parseStageToken).
    /// `cst_…` → connection coordinates. A malformed token is an explicit ConfigError (no silent misbehaviour).
    ///
    /// Checks run in this order: leading/trailing whitespace → prefix → strict base64url · UTF-8 → strict JSON → fields.
    public static func parse(_ token: String) throws -> StageTokenPayload {
        // 앞뒤 공백은 벗기지 않고 알린다 — 파일·환경변수에서 읽은 줄바꿈이 흔한 원인이고, 조용히 벗기면
        // "어떤 언어에선 되고 어떤 언어에선 안 된다" 가 된다.
        if hasEdgeWhitespace(token) {
            throw AdapterError("ConfigError", whitespaceMessage)
        }
        guard isStageToken(token) else {
            throw AdapterError("ConfigError", "not a stage token (expected \(prefix) prefix)")
        }
        let scalars = token.unicodeScalars
        // 본문도 스칼라 단위로 뗀다 — `dropFirst(4)` 는 글자 단위라 `_` 에 붙은 결합 문자까지 먹는다.
        // 판정과 값은 `StrictJSON.parse` 하나가 낸다(`JSONSerialization` 은 판정을 통과한 텍스트에서도 갈렸다 —
        // 문자열 머리의 U+FEFF 를 벗겨 `"k"` 가 어댑터 키로 둔갑했다. StrictJSON.swift 참고).
        guard let text = fromBase64Url(String(scalars.dropFirst(prefix.unicodeScalars.count))),
              let parsed = StrictJSON.parse(text)
        else {
            throw AdapterError("ConfigError", "malformed stage token — could not decode")
        }
        // `cst_` + base64url("null") 은 JSON 으로는 유효하다 — 객체가 아니면 필드를 읽기 전에 거부한다.
        guard let object = parsed as? [String: Any] else {
            throw AdapterError("ConfigError", "malformed stage token — payload is not an object")
        }

        // `as? Int` 는 NSNumber 불리언도 받는다 — `{"v":true}` 가 버전 1 로 통과했다(공유 벡터가
        // 잡았다: TS·C#·Dart 는 거부한다). CFType 으로 불리언을 가르고 값으로 비교한다.
        guard let version = object["v"] as? NSNumber,
              CFGetTypeID(version) != CFBooleanGetTypeID(),
              version.doubleValue == 1
        else {
            let shown = object["v"].map { "\($0)" } ?? "?"
            throw AdapterError("ConfigError", "unsupported stage token version (\(shown)) — update the adapter")
        }
        guard let url = object["u"] as? String,
              let stageId = object["s"] as? String,
              let secretKey = object["k"] as? String
        else {
            throw AdapterError("ConfigError", "malformed stage token — missing url/stageId/secretKey")
        }
        // 공백 판정은 JSON 공백 넷만 — `.whitespacesAndNewlines` 는 NBSP 까지 벗겨 NBSP stageId 를 빈 값으로 봤다
        // (`trim()` 의 공백 정의는 언어마다 다르다).
        if StrictJSON.isBlank(url) || StrictJSON.isBlank(stageId) {
            // 비어 있는 봉투를 통과시키면 connect() 가 "ck_ 는 자기서술이 아니다" 라는 엉뚱한 조언을 한다.
            throw AdapterError("ConfigError", "malformed stage token — empty url/stageId (issue a new one from Share → Adapter keys)")
        }
        guard isAdapterKey(secretKey) else {
            throw AdapterError("ConfigError", notAdapterKeyMessage)
        }
        return StageTokenPayload(
            url: url,
            stageId: stageId,
            secretKey: secretKey,
            displayName: object["n"] as? String
        )
    }

    // JS `JSON.stringify` 와 같은 이스케이프 폭(비ASCII 를 \u 로 부풀리지 않는다).
    private static func quote(_ raw: String) -> String {
        var out = "\""
        for character in raw.unicodeScalars {
            switch character {
            case "\"": out += "\\\""
            case "\\": out += "\\\\"
            case "\n": out += "\\n"
            case "\r": out += "\\r"
            case "\t": out += "\\t"
            case "\u{08}": out += "\\b"
            case "\u{0C}": out += "\\f"
            default:
                if character.value < 0x20 {
                    out += String(format: "\\u%04x", character.value)
                } else {
                    out.unicodeScalars.append(character)
                }
            }
        }
        return out + "\""
    }

    private static func base64UrlEncode(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    private static let base64UrlAlphabet = Array("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".utf8)

    // 여섯 언어 공통 규칙.
    // 종전에는 본문을 `Data(base64Encoded:)` 형식으로 바꿔 넣어서 표준 문자(`+ /`) · 패딩(`=`) · 남는 비트가
    // 그대로 통과했다 — 다른 언어 디코더가 거부하는 토큰이었다.
    /// base64url body → text, read **strictly** — nil when it cannot be read.
    ///
    ///   - Only the alphabet `[A-Za-z0-9_-]` (no padding `=`, no standard characters `+ /`, no whitespace), length % 4 != 1.
    ///   - Canonical form only — the leftover bits of the last character must be 0.
    ///   - Strict UTF-8. A BOM is not stripped (the JSON check that follows rejects it).
    private static func fromBase64Url(_ body: String) -> String? {
        let encoded = Array(body.utf8)
        if encoded.isEmpty || encoded.count % 4 == 1 { return nil }
        guard encoded.allSatisfy({ base64UrlAlphabet.contains($0) }),
              let lastValue = base64UrlAlphabet.firstIndex(of: encoded[encoded.count - 1])
        else { return nil }
        let tail = encoded.count % 4
        if (tail == 2 && (lastValue & 0x0F) != 0) || (tail == 3 && (lastValue & 0x03) != 0) { return nil }
        // 판정을 통과한 본문만 표준 알파벳 + 패딩으로 바꿔 플랫폼 디코더에 준다(`Data(base64Encoded:)` 는 패딩을 요구한다).
        let standard = body.replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
            + String(repeating: "=", count: (4 - tail) % 4)
        guard let decoded = Data(base64Encoded: standard) else { return nil }
        return strictUTF8(decoded)
    }

    // (실측) 표준 라이브러리 디코더는 BOM 을 U+FEFF 로 남긴다.
    /// Strict UTF-8 → text — nil on invalid bytes, truncated sequences, overlong forms or encoded surrogates.
    /// `String(data:encoding: .utf8)` is not used: it rejects invalid bytes with nil but **silently strips** a
    /// leading BOM.
    private static func strictUTF8(_ bytes: Data) -> String? {
        var scalars = String.UnicodeScalarView()
        var decoder = UTF8()
        var iterator = bytes.makeIterator()
        while true {
            switch decoder.decode(&iterator) {
            case .scalarValue(let scalar): scalars.append(scalar)
            case .emptyInput: return String(scalars)
            case .error: return nil
            }
        }
    }
}
