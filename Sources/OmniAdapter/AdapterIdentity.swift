import Foundation

// 어댑터 신원(ADR-0016 · D4). TS `adapter-identity.ts` 미러. 정규화 두 함수는 여섯 언어가 같은 답을 내야 한다.
/// Adapter identity — the four fields sent on token/validate (`adapter` · `adapterVersion` · `runtime` ·
/// `runtimeVersion`). The Connect Omni server records them, on key joins only, in its usage ledger as
/// `adapter.joined{adapter, runtime}`.
///
/// Not a user option — sent on every key join. It carries no user data: only the language, this library's
/// version, the runtime kind and its version. A value that does not match the server's pattern is not sent;
/// the field is left out (the join still succeeds). The two normalization functions are pinned by
/// `normalizeAdapterVersion` · `normalizeRuntimeVersion` in `docs/conformance.json`.
enum AdapterIdentity {
    // 서버 어휘(TS `ADAPTER_LANGS`) 안.
    /// This library's language — within the server's vocabulary.
    static let lang = "swift"

    /// The runtime this library reports — Apple platforms (iOS · macOS · tvOS · watchOS). The version is the OS version.
    static let runtime = "apple"

    /// This package's version — SwiftPM has no manifest version, so the top version heading of `CHANGELOG.md` is
    /// the source of truth (VERSIONING.md · release-swift.yml). `AdapterIdentityTests` compares against that heading —
    /// change both when bumping the version.
    static let adapterVersion = "0.1.0-beta.1"

    private static let adapterVersionRe = try! NSRegularExpression(pattern: Protocol.adapterVersionPattern)
    private static let runtimeVersionRe = try! NSRegularExpression(pattern: Protocol.runtimeVersionPattern)
    // 앞머리의 숫자 판 — 마디 넷까지. \d 가 아니라 [0-9]: ICU 의 \d 는 유니코드 숫자도 받는다.
    private static let leadingVersion = try! NSRegularExpression(pattern: "^[0-9]+(?:\\.[0-9]+){0,3}")

    /// Whether the regex matches the **whole string** — ICU's `$` also matches before a trailing newline, so the length of the range is checked.
    private static func fullMatch(_ re: NSRegularExpression, _ value: String) -> Bool {
        let whole = NSRange(value.startIndex..., in: value)
        guard let m = re.firstMatch(in: value, range: whole) else { return false }
        return m.range.location == 0 && m.range.length == whole.length
    }

    /// Library version → wire value. Build metadata (`+…`) is stripped; if the rest does not match the server's
    /// pattern, nil (the field is left out). No trimming — leading/trailing whitespace or a `v` prefix is a non-matching value too.
    static func normalizeAdapterVersion(_ raw: String?) -> String? {
        guard let raw else { return nil }
        // 유니코드 스칼라로 자른다 — Character 로 찾으면 `+` 뒤에 결합 문자가 붙은 `+́` 를 못 찾는다(다른 다섯 언어와 갈림).
        let scalars = raw.unicodeScalars
        let value = scalars.firstIndex(of: "+").map { String(scalars[..<$0]) } ?? raw
        return fullMatch(adapterVersionRe, value) ? value : nil
    }

    /// Runtime version reported by the platform → wire value. Keeps only the leading numeric version; nil when that
    /// does not match the server's pattern (at most 16 characters) or does not start with a digit.
    static func normalizeRuntimeVersion(_ raw: String?) -> String? {
        guard let raw else { return nil }
        let whole = NSRange(raw.startIndex..., in: raw)
        guard let m = leadingVersion.firstMatch(in: raw, range: whole), m.range.location == 0 else { return nil }
        // NSString(UTF-16)으로 꺼낸다 — String 첨자는 Character 경계로 내림해 `1.2́` 에서 `1.` 을 돌려준다.
        let value = (raw as NSString).substring(with: m.range).lowercased()
        return fullMatch(runtimeVersionRe, value) ? value : nil
    }

    /// Runtime kind and version — on Apple platforms `apple` and the OS version (`major.minor.patch`); otherwise
    /// (Linux and the like — not in the server's vocabulary) both are left out. Takes the OS version as an argument so
    /// tests can inject a fake.
    static func detectRuntime(osVersion: OperatingSystemVersion?) -> [String: String] {
        guard let v = osVersion else { return [:] }
        var out = ["runtime": runtime]
        if let version = normalizeRuntimeVersion("\(v.majorVersion).\(v.minorVersion).\(v.patchVersion)") {
            out["runtimeVersion"] = version
        }
        return out
    }

    /// This process's OS version — Apple platforms only.
    static var currentOSVersion: OperatingSystemVersion? {
        #if canImport(Darwin)
            return ProcessInfo.processInfo.operatingSystemVersion
        #else
            return nil
        #endif
    }

    /// The identity fields put on token/validate — a field that could not be determined is absent, key and all.
    static func fields(
        osVersion: OperatingSystemVersion? = currentOSVersion,
        adapterVersion: String? = normalizeAdapterVersion(AdapterIdentity.adapterVersion)
    ) -> [String: String] {
        var out = ["adapter": lang]
        if let adapterVersion { out["adapterVersion"] = adapterVersion }
        out.merge(detectRuntime(osVersion: osVersion)) { _, new in new }
        return out
    }
}
