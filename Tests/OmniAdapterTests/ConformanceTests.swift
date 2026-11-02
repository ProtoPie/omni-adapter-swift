import XCTest
@testable import OmniAdapter

/// 6언어 공유 동작 벡터 — `docs/conformance.json` 을 읽어 그대로 돌린다.
///
/// 파리티 가드(`scripts/parity-guard.mjs`)는 "있다" 를 볼 뿐 "어떻게 동작한다" 를 못 본다.
/// 여섯 언어가 같은 벡터 파일을 읽으면, 순수 함수의 동작이 갈리는 순간 그 언어의 테스트가
/// 빨개진다. 케이스가 실패하면 **구현을 고친다** — 케이스를 지우거나 건너뛰지 않는다.
///
/// 테스트는 **벡터 파일이 몰고 간다**: 파일의 스위트마다 아래 실행기를 찾아 돌리고(없으면 실패),
/// 실행기가 평가한 케이스 수가 그 스위트의 cases 길이와 같은지 센다. 실행기 이름만 남기고 본문을
/// 비우면 여기서 빨개진다 — 가드는 테스트 파일이 스위트 이름을 **언급하는지**만 볼 수 있다.
///
/// 파일이 없으면(이 패키지만 떼어낸 체크아웃) skip. 경로는 이 파일(`#filePath`) 기준으로 푼다 —
/// 작업 디렉터리에 기대지 않는다. repo 안에서 파일이 사라진 경우는 파리티 가드가 잡는다
/// (`parity.json` conformance.vectors). CI 는 `OMNI_CONFORMANCE_REQUIRED=1` 을 켠다 — 그때는 못 찾는 것
/// 자체가 실패다(경로가 어긋나 전부 건너뛰는 거짓 초록을 막는다).
final class ConformanceTests: XCTestCase {
    /// 실행기 — 케이스마다 판정해 어긋난 케이스마다 한 줄을 `failures` 에 더하고, **평가한 케이스 수**를 돌려준다.
    private typealias Runner = (_ cases: [Any], _ failures: inout [String]) -> Int

    /// 스위트 이름 → 실행기.
    private let runners: [String: Runner] = [
        "matchChannel": ConformanceTests.runMatchChannel,
        "parseStageToken": ConformanceTests.runParseStageToken,
        "parseRecording": ConformanceTests.runParseRecording,
        "normalizeAdapterVersion": { cases, failures in
            ConformanceTests.runNormalize("normalizeAdapterVersion", AdapterIdentity.normalizeAdapterVersion, cases, &failures)
        },
        "normalizeRuntimeVersion": { cases, failures in
            ConformanceTests.runNormalize("normalizeRuntimeVersion", AdapterIdentity.normalizeRuntimeVersion, cases, &failures)
        },
        // ICE 목록을 다시 받을 때(epoch ms) — 받을 때 남았던 수명의 절반(내림)과 만료 1분 전 중 이른 쪽.
        "iceRefreshAt": { cases, failures in
            var ran = 0
            for item in cases {
                ran += 1
                guard let c = item as? [String: Any], let fetched = (c["fetchedAt"] as? NSNumber)?.int64Value else {
                    failures.append("malformed case: \(item)")
                    continue
                }
                let expiresAt = (c["expiresAt"] as? NSNumber)?.int64Value
                let want = (c["expect"] as? NSNumber)?.int64Value
                let got = ConnectClient.iceRefreshAt(expiresAt: expiresAt, fetchedAt: fetched)
                if got != want {
                    failures.append("iceRefreshAt(\(String(describing: expiresAt)), \(fetched)) = \(String(describing: got)), expected \(String(describing: want))")
                }
            }
            return ran
        },
    ]

    func testRunnersMatchTheVectorSuitesOneToOne() throws {
        let suites = try vectorSuites()
        XCTAssertEqual(runners.keys.sorted(), suites.keys.sorted(), "실행기와 벡터 파일의 스위트가 일대일이 아니다")
    }

    /// 벡터 파일의 스위트를 **전부** 돌린다 — 이 테스트가 "건너뛴 스위트가 없다" 를 보장한다.
    func testEverySuiteInTheVectorFile() throws {
        let suites = try vectorSuites()
        for name in suites.keys.sorted() {
            guard let run = runners[name] else {
                XCTFail("스위트 '\(name)' 의 실행기가 없다 — 여섯 언어 테스트 전부에 구현할 것")
                continue
            }
            let cases = (suites[name] as? [String: Any])?["cases"] as? [Any] ?? []
            XCTAssertFalse(cases.isEmpty, "\(name): cases 가 비어 있다")
            var failures: [String] = []
            let ran = run(cases, &failures)
            XCTAssertTrue(failures.isEmpty, "\(name):\n" + failures.joined(separator: "\n"))
            XCTAssertEqual(ran, cases.count, "\(name): 실행기가 평가한 케이스 수")
        }
    }

    // MARK: - 벡터 파일

    /// 이 파일에서 위로 올라가며 `docs/conformance.json` 을 찾는다. 없으면 skip(엄격 모드는 실패), 있는데 못 읽으면 실패.
    private func vectorSuites() throws -> [String: Any] {
        var dir = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        while dir.path != "/" {
            let candidate = dir.appendingPathComponent("docs/conformance.json")
            if FileManager.default.fileExists(atPath: candidate.path) {
                // 읽개가 입력을 바꾸지 않는지 먼저 본다 — 문자열 머리의 BOM 을 벗기는 읽개로는 BOM 벡터가 검사하는
                // 입력이 구현에 닿지 않는다(`JSONSerialization` 이 그랬다: `"\u{FEFF}\u{FEFF}{…}"` 가 BOM 하나로 왔다).
                let probe = try JSONDecoder().decode([String].self, from: Data("[\"\u{FEFF}\u{FEFF}x\"]".utf8))
                XCTAssertEqual(probe.first?.unicodeScalars.count, 3, "벡터 읽개(JSONDecoder)가 문자열 머리의 U+FEFF 를 벗긴다")
                let root = try JSONDecoder().decode(VectorValue.self, from: Data(contentsOf: candidate)).foundation
                return try XCTUnwrap((root as? [String: Any])?["suites"] as? [String: Any], "\(candidate.path) 에 suites 가 없다")
            }
            dir = dir.deletingLastPathComponent()
        }
        if ProcessInfo.processInfo.environment["OMNI_CONFORMANCE_REQUIRED"] == "1" { throw MissingVectors() }
        throw XCTSkip("docs/conformance.json 없음 — 이 패키지만 떼어낸 체크아웃")
    }

    /// CI(`OMNI_CONFORMANCE_REQUIRED=1`)에서 벡터 파일을 못 찾았다 — 건너뛰지 않고 실패한다.
    private struct MissingVectors: Error, CustomStringConvertible {
        var description: String { "docs/conformance.json 을 찾지 못했다 — OMNI_CONFORMANCE_REQUIRED=1 에서는 건너뛰지 않고 실패한다" }
    }

    /// 벡터 파일의 JSON 값 — `JSONDecoder` 로 읽는다. `JSONSerialization` 은 문자열 값 머리의 U+FEFF 하나를
    /// 벗겨(실측) BOM 을 다루는 벡터의 입력을 바꿨다. 실행기는 `foundation` 모양(String · NSNumber · NSNull ·
    /// [Any] · [String: Any])으로 읽는다.
    private enum VectorValue: Decodable {
        case null, bool(Bool), number(Double), string(String), array([VectorValue]), object([String: VectorValue])

        init(from decoder: Decoder) throws {
            let c = try decoder.singleValueContainer()
            if c.decodeNil() {
                self = .null
            } else if let b = try? c.decode(Bool.self) {
                self = .bool(b)
            } else if let d = try? c.decode(Double.self) {
                self = .number(d)
            } else if let s = try? c.decode(String.self) {
                self = .string(s)
            } else if let a = try? c.decode([VectorValue].self) {
                self = .array(a)
            } else {
                self = .object(try c.decode([String: VectorValue].self))
            }
        }

        var foundation: Any {
            switch self {
            case .null: return NSNull()
            case .bool(let b): return NSNumber(value: b)
            case .number(let d): return NSNumber(value: d)
            case .string(let s): return s
            case .array(let a): return a.map(\.foundation)
            case .object(let o): return o.mapValues(\.foundation)
            }
        }
    }

    // MARK: - 실행기

    /// 정규화 두 스위트 — 입력 하나에 값 또는 nil(필드를 뺀다).
    private static func runNormalize(
        _ name: String, _ normalize: (String?) -> String?, _ cases: [Any], _ failures: inout [String]
    ) -> Int {
        var ran = 0
        for item in cases {
            ran += 1
            guard let c = item as? [String: Any], let input = c["input"] as? String, c.keys.contains("expect") else {
                failures.append("malformed case: \(item)")
                continue
            }
            let want = c["expect"] as? String
            if !(c["expect"] is NSNull) && want == nil {
                failures.append("malformed expect: \(item)")
                continue
            }
            let got = normalize(input)
            if got != want {
                failures.append("\(name)(\(shown(input))) = \(got.map { shown($0) } ?? "nil"), expected \(want.map { shown($0) } ?? "nil")")
            }
        }
        return ran
    }

    private static func runMatchChannel(_ cases: [Any], _ failures: inout [String]) -> Int {
        var ran = 0
        for item in cases {
            ran += 1
            guard let c = item as? [String: Any], let pattern = c["pattern"] as? String,
                  let channel = c["channel"] as? String, let want = c["match"] as? Bool
            else {
                failures.append("malformed case: \(item)")
                continue
            }
            let got = Wildcard.matchChannel(pattern, channel)
            if got != want { failures.append("matchChannel(\(shown(pattern)), \(shown(channel))) = \(got)") }
        }
        return ran
    }

    private static func runParseStageToken(_ cases: [Any], _ failures: inout [String]) -> Int {
        var ran = 0
        for item in cases {
            ran += 1
            let c = item as? [String: Any] ?? [:]
            let name = c["name"] as? String ?? "?"
            guard let token = c["token"] as? String else {
                failures.append("\(name): malformed case (no token)")
                continue
            }
            // payload · payloadHex · note 는 사람이 읽으라고 둔 것이다(token 과의 일치는 TS 테스트가 지킨다).
            let got = outcome { try StageToken.parse(token) }
            if let error = c["error"] as? String {
                if let why = errorMismatch(got, error, c["messageContains"] as? String) { failures.append("\(name): \(why)") }
            } else if let want = c["expect"] as? [String: Any] {
                if case .ok(let payload) = got {
                    if !samePayload(payload, want) { failures.append("\(name): expected \(shown(want)), got \(shown(payload))") }
                } else {
                    failures.append("\(name): expected a payload, got \(describe(got))")
                }
            } else {
                failures.append("\(name): malformed case (neither error nor expect)")
            }
        }
        return ran
    }

    private static func runParseRecording(_ cases: [Any], _ failures: inout [String]) -> Int {
        var ran = 0
        for item in cases {
            ran += 1
            let c = item as? [String: Any] ?? [:]
            let name = c["name"] as? String ?? "?"
            guard let ndjson = c["ndjson"] as? String else {
                failures.append("\(name): malformed case (no ndjson)")
                continue
            }
            let got = outcome { try parseRecording(ndjson) }
            if let error = c["error"] as? String {
                if let why = errorMismatch(got, error, c["messageContains"] as? String) { failures.append("\(name): \(why)") }
            } else if let want = c["expect"] as? [[String: Any]] {
                if case .ok(let records) = got {
                    if records.count != want.count || !zip(records, want).allSatisfy(sameRecord) {
                        failures.append("\(name): expected [\(want.map(shown).joined(separator: " ; "))], got [\(records.map(shown).joined(separator: " ; "))]")
                    }
                } else {
                    failures.append("\(name): expected records, got \(describe(got))")
                }
            } else {
                failures.append("\(name): malformed case (neither error nor expect)")
            }
        }
        return ran
    }

    // MARK: - 판정

    /// 실행 결과 — AdapterError 는 code·문장으로 접고, 그 밖의 오류는 `unexpected` 로 남긴다
    /// (계약: 사용자 입력은 AdapterError 로만 거부한다 — 다른 오류는 어느 기대와도 맞지 않는다).
    private enum Outcome<T> {
        case ok(T)
        case adapterError(code: String, message: String)
        case unexpected(String)
    }

    private static func outcome<T>(_ run: () throws -> T) -> Outcome<T> {
        do {
            return .ok(try run())
        } catch let error as AdapterError {
            return .adapterError(code: error.code, message: error.message)
        } catch {
            return .unexpected("\(type(of: error)): \(error)")
        }
    }

    /// 기대 오류(code + 선택 문장 조각)와 대조한다. 어긋나면 사유, 맞으면 nil.
    private static func errorMismatch<T>(_ got: Outcome<T>, _ code: String, _ piece: String?) -> String? {
        switch got {
        case .ok: return "expected \(code), got a result"
        case .unexpected(let what): return "expected \(code), got a non-AdapterError \(what)"
        case let .adapterError(gotCode, message):
            if gotCode != code { return "expected \(code), got \(gotCode): \(message)" }
            if let piece, !message.contains(piece) { return "message \(shown(message)) lacks \(shown(piece))" }
            return nil
        }
    }

    private static func describe<T>(_ got: Outcome<T>) -> String {
        switch got {
        case .ok(let value): return "a result \(value)"
        case let .adapterError(code, message): return "\(code): \(message)"
        case .unexpected(let what): return "a non-AdapterError \(what)"
        }
    }

    /// payload 비교 — 네 필드를 다 본다. displayName 은 없음과 `""` 이 다르다.
    private static func samePayload(_ got: StageTokenPayload, _ want: [String: Any]) -> Bool {
        sameText(got.url, want["url"]) && sameText(got.stageId, want["stageId"])
            && sameText(got.secretKey, want["secretKey"]) && sameText(got.displayName, want["displayName"])
    }

    /// 녹화 한 줄 비교 — 숫자는 값으로(1 = 1.0), from 은 없음과 있음을 가른다.
    private static func sameRecord(_ got: RecordedMessage, _ want: [String: Any]) -> Bool {
        guard let ts = want["ts"] as? NSNumber, got.ts == ts.doubleValue,
              sameText(got.channel, want["channel"]), sameText(got.from, want["from"]),
              let raw = want["value"], let value = Primitive.from(raw)
        else { return false }
        switch (got.value, value) {
        case (.null, .null): return true
        case let (.bool(a), .bool(b)): return a == b
        case let (.string(a), .string(b)): return sameText(a, b)
        default:
            guard let a = got.value.doubleValue, let b = value.doubleValue else { return false }
            return a == b
        }
    }

    /// 문자열은 코드 유닛으로 비교한다 — Swift `String` 의 `==` 는 정규 등가라 `"é"` 와 `"e\u{301}"` 을
    /// 같다고 본다(TS 는 다르다). `want` 가 없으면 `got` 도 없어야 한다.
    private static func sameText(_ got: String?, _ want: Any?) -> Bool {
        guard let want else { return got == nil }
        guard let got, let wanted = want as? String else { return false }
        return Array(got.utf16) == Array(wanted.utf16)
    }

    // MARK: - 실패 문장

    private static func shown(_ text: String) -> String { String(reflecting: text) }

    private static func shown(_ payload: StageTokenPayload) -> String {
        "url=\(shown(payload.url)) stageId=\(shown(payload.stageId)) secretKey=\(shown(payload.secretKey)) "
            + "displayName=\(payload.displayName.map(shown) ?? "<none>")"
    }

    private static func shown(_ want: [String: Any]) -> String {
        want.keys.sorted().map { "\($0)=\(want[$0].map { "\($0)" } ?? "?")" }.joined(separator: " ")
    }

    private static func shown(_ record: RecordedMessage) -> String {
        "ts=\(record.ts) channel=\(shown(record.channel)) value=\(record.value) from=\(record.from.map(shown) ?? "<none>")"
    }
}
