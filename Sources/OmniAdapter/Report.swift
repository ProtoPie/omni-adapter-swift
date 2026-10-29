import Foundation

// TS `src/report.ts` / Python `report.py` 미러.
/// A reproduction bundle — the artifact that turns a support request from "it doesn't work" into
/// "look at this file".
///
/// An adapter leaves three kinds of trace, and they live apart: the diagnostics ring buffer inside the
/// process, the `doctor` result on screen, and the server's build info that nobody looks at. To diagnose
/// a failing adapter in the field remotely, all three must be **in one file**.
///
/// ⚠️ **Credentials never get in by any path.** The bundle holds only a masked fingerprint, the connection
/// coordinates, diagnostics entries, the doctor result and the server's `/api/about`.
public enum Report {
    /// Bundle format version — a future parser branches on it.
    public static let version = 1

    /// Credential fingerprint — the first 4 characters + the length.
    ///
    /// Enough to tell whether the same key was given twice, but impossible to restore.
    /// **The original is never included.**
    public static func fingerprint(_ secret: String) -> String {
        if secret.isEmpty { return "none" }
        let head = String(secret.prefix(4))
        return "\(head)…(\(secret.count))"
    }

    /// One bundle entry — the minimal representation for JSON serialization.
    public struct Bundle: Sendable {
        public let reportVersion: Int
        public let generatedAt: String
        public let protocolVersion: String
        public let packageVersion: String?
        public let runtime: [String: String]
        public let url: String
        public let stageId: String
        public let credentialFingerprint: String
        public let credentialKind: String
        public let server: String?
        public let doctor: [CheckResult]
        public let diagnostics: [LogEntry]
    }

    private struct Resolved {
        let url: String
        let stageId: String
        let secretKey: String
        let kind: String
    }

    private static func resolve(_ credential: String) throws -> Resolved {
        if StageToken.hasEdgeWhitespace(credential) {
            throw AdapterError(
                "ConfigError", StageToken.credentialWhitespaceMessage, hint: StageToken.credentialWhitespaceHint)
        }
        if StageToken.isStageToken(credential) {
            let t = try StageToken.parse(credential)
            return Resolved(url: t.url, stageId: t.stageId, secretKey: t.secretKey, kind: "cst_")
        }
        let hint = "create an adapter token on the stage: Share → Adapter keys → Create key"
        if credential.hasPrefix(StageToken.dedicatedKeyPrefix) {
            throw AdapterError(
                "ConfigError",
                "an adapter key (ck_…) carries no url/stageId — report needs a cst_ token",
                hint: hint)
        }
        throw AdapterError(
            "ConfigError",
            "report needs a cst_ token (Stage → Share → Adapter keys)",
            hint: hint)
    }

    /// Builds the bundle.
    ///
    /// It reuses `doctor` as is, so the diagnostic logic does not exist twice.
    /// The bundle **is built even when fetching the server's `/api/about` fails** — not reaching the server is
    /// exactly when the bundle is needed most, so throwing here would defeat its purpose.
    public static func build(
        credential: String,
        http: DoctorHttp? = nil,
        connectImpl: DoctorConnect? = nil,
        diagnostics: [LogEntry] = [],
        packageVersion: String? = nil,
        generatedAt: String? = nil,
        runtime: [String: String]? = nil
    ) async throws -> Bundle {
        let target = try resolve(credential)
        let doctorResults = await runDoctor(
            credential: credential, http: http, connectImpl: connectImpl)

        var server: String?
        if let call = http {
            do {
                let base = target.url.hasSuffix("/") ? String(target.url.dropLast()) : target.url
                let res = try await call("\(base)/api/about", nil)
                if (200..<300).contains(res.status) { server = res.body }
            } catch {
                // 서버 미도달도 번들의 정보다 — doctor 결과가 그 사실을 이미 담고 있다.
                server = nil
            }
        }

        return Bundle(
            reportVersion: version,
            generatedAt: generatedAt ?? ISO8601DateFormatter().string(from: Date()),
            protocolVersion: Protocol.protocolVersion,
            packageVersion: packageVersion,
            runtime: runtime ?? ["name": "swift"],
            url: target.url,
            stageId: target.stageId,
            credentialFingerprint: fingerprint(target.secretKey),
            credentialKind: target.kind,
            server: server,
            doctor: doctorResults,
            diagnostics: diagnostics
        )
    }

    /// A form that is easy to save to a file or paste — minimal JSON with no dependencies.
    public static func format(_ b: Bundle) -> String {
        var adapter = ["\"protocolVersion\": \(quote(b.protocolVersion))"]
        if let pv = b.packageVersion { adapter.append("\"packageVersion\": \(quote(pv))") }
        let runtimeBody = b.runtime.sorted { $0.key < $1.key }
            .map { "    \(quote($0.key)): \(quote($0.value))" }
            .joined(separator: ",\n")
        let doctorBody = b.doctor.map { r -> String in
            var fields = [
                "      \"id\": \(quote(r.id))",
                "      \"label\": \(quote(r.label))",
                "      \"status\": \(quote(r.status.rawValue))",
            ]
            if let d = r.detail { fields.append("      \"detail\": \(quote(d))") }
            if let h = r.hint { fields.append("      \"hint\": \(quote(h))") }
            return "    {\n" + fields.joined(separator: ",\n") + "\n    }"
        }.joined(separator: ",\n")
        let diagBody = b.diagnostics.map { e in
            "    {\n      \"at\": \(e.at),\n      \"level\": \(quote(e.level)),\n"
                + "      \"code\": \(quote(e.code)),\n      \"message\": \(quote(e.message))\n    }"
        }.joined(separator: ",\n")

        var out = "{\n"
        out += "  \"reportVersion\": \(b.reportVersion),\n"
        out += "  \"generatedAt\": \(quote(b.generatedAt)),\n"
        out += "  \"adapter\": {\n    " + adapter.joined(separator: ",\n    ") + "\n  },\n"
        out += "  \"runtime\": {\n" + runtimeBody + "\n  },\n"
        out += "  \"target\": {\n"
        out += "    \"url\": \(quote(b.url)),\n"
        out += "    \"stageId\": \(quote(b.stageId)),\n"
        out += "    \"credentialFingerprint\": \(quote(b.credentialFingerprint)),\n"
        out += "    \"credentialKind\": \(quote(b.credentialKind))\n  },\n"
        // `server` 는 **중첩 객체**로 낸다 — TS·Python·Dart 는 파서가 있어 객체를
        // 담고, 여기서 문자열로 담으면 같은 reportVersion 인데 생산자에 따라 타입이
        // 갈린다(번들을 읽는 도구가 언어별로 분기해야 한다). 파서 의존성을 새로 넣지
        // 않고 본문을 그대로 이어붙이되, JSON 이 아닌 응답이 번들을 깨지 않도록
        // 모양을 보고 판단한다.
        if let s = b.server { out += "  \"server\": \(asJsonValue(s)),\n" }
        out += "  \"doctor\": [\n" + doctorBody + "\n  ],\n"
        out += "  \"diagnostics\": [\n" + diagBody + "\n  ]\n}"
        return out
    }

    /// Verbatim when it looks like JSON, otherwise a quoted string.
    private static func asJsonValue(_ body: String) -> String {
        let t = body.trimmingCharacters(in: .whitespacesAndNewlines)
        return (t.hasPrefix("{") || t.hasPrefix("[")) ? t : quote(body)
    }

    private static func quote(_ s: String) -> String {
        var sb = "\""
        for ch in s.unicodeScalars {
            switch ch {
            case "\"": sb += "\\\""
            case "\\": sb += "\\\\"
            case "\n": sb += "\\n"
            case "\r": sb += "\\r"
            case "\t": sb += "\\t"
            default:
                if ch.value < 0x20 { sb += String(format: "\\u%04x", ch.value) } else { sb.unicodeScalars.append(ch) }
            }
        }
        return sb + "\""
    }
}
