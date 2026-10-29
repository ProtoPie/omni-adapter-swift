import XCTest
@testable import OmniAdapter

/// 지연 측정 + 재현 번들 — TS·Python·Kotlin·Dart 스위트 미러.
final class LatencyReportTests: XCTestCase {
    private let rawSecret = "ck_super-secret-adapter-key-value"
    private var token: String {
        get throws {
            try StageToken.encode(
                url: "https://cloud.example.com/api/connect",
                stageId: "01HZZZSTAGE",
                secretKey: rawSecret)
        }
    }

    private let failingHttp: DoctorHttp = { _, _ in
        throw AdapterError("NotConnected", "ECONNREFUSED")
    }
    private let noSleep: @Sendable (Int) async -> Void = { _ in }

    func testPercentileUsesNearestRank() {
        let s: [Double] = [10, 20, 30, 40]
        // 보간법이면 p50 = 25 (관측된 적 없는 값). 순위법은 실제 표본을 고른다.
        XCTAssertEqual(percentile(s, 50), 20)
        XCTAssertEqual(percentile(s, 95), 40)
        XCTAssertTrue(percentile([], 50).isNaN)
    }

    func testSummarizeDoesNotMutateInput() {
        let given: [Double] = [30, 10, 20]
        let stats = summarize(given)
        XCTAssertEqual(given, [30, 10, 20])
        XCTAssertEqual(stats.count, 3)
        XCTAssertEqual(stats.min, 10)
        XCTAssertEqual(stats.max, 30)
        XCTAssertEqual(stats.p50, 20)
    }

    func testMeasuresAndSummarizes() async throws {
        let values = ValueBox([12, 8, 30])
        let r = try await measureLatency(
            ping: { values.next() }, count: 3, sleep: noSleep, now: { 1000 })
        XCTAssertEqual(r.samples.compactMap(\.rttMs), [12, 8, 30])
        XCTAssertEqual(r.stats.count, 3)
        XCTAssertEqual(r.stats.min, 8)
        XCTAssertEqual(r.stats.max, 30)
        XCTAssertEqual(r.stats.failed, 0)
    }

    func testFailuresAreCountedNotMixedIntoPercentiles() async throws {
        let counter = Counter()
        let r = try await measureLatency(
            ping: {
                let n = counter.bump()
                if n == 2 { throw AdapterError("AckTimeout", "timed out") }
                return 10
            }, count: 3, sleep: noSleep, now: { 1000 })
        XCTAssertEqual(r.stats.count, 2)
        XCTAssertEqual(r.stats.failed, 1)
        // 타임아웃을 큰 수로 섞었다면 max 가 10 이 아니게 된다.
        XCTAssertEqual(r.stats.max, 10)
        XCTAssertEqual(r.samples[1].error, "AckTimeout")
        XCTAssertNil(r.samples[1].rttMs)
    }

    func testAllFailuresDoNotThrow() async throws {
        let r = try await measureLatency(
            ping: { throw AdapterError("NotConnected") }, count: 2, sleep: noSleep)
        XCTAssertEqual(r.stats.count, 0)
        XCTAssertEqual(formatLatencyStats(r.stats), "no successful samples (2 failed)")
    }

    func testSleepsBetweenSamplesOnly() async throws {
        let slept = IntBox()
        _ = try await measureLatency(
            ping: { 5 }, count: 3, intervalMs: 50, sleep: { slept.add($0) })
        XCTAssertEqual(slept.values, [50, 50])
    }

    func testCountMustBePositive() async {
        do {
            _ = try await measureLatency(ping: { 1 }, count: 0, sleep: noSleep)
            XCTFail("expected ConfigError")
        } catch let err as AdapterError {
            XCTAssertEqual(err.code, "ConfigError")
        } catch { XCTFail("wrong error: \(error)") }
    }

    func testFingerprintMasksEverythingButPrefixAndLength() {
        XCTAssertEqual(Report.fingerprint("cst_abcdefghijklmnop"), "cst_…(20)")
        XCTAssertEqual(Report.fingerprint("abc"), "abc…(3)")
        XCTAssertEqual(Report.fingerprint(""), "none")
    }

    func testReportNeverSerializesTheCredential() async throws {
        let bundle = try await Report.build(
            credential: try token, http: failingHttp, generatedAt: "2026-09-02T12:00:00Z")
        let text = Report.format(bundle)
        XCTAssertFalse(text.contains(rawSecret), text)
        XCTAssertFalse(text.contains(try token), text)
        XCTAssertEqual(bundle.credentialFingerprint, Report.fingerprint(rawSecret))
    }

    func testReportSurvivesAnUnreachableServer() async throws {
        let bundle = try await Report.build(
            credential: try token, http: failingHttp, generatedAt: "2026-09-02T12:00:00Z")
        // 서버에 못 닿는 상황이야말로 번들이 가장 필요한 때다.
        XCTAssertNil(bundle.server)
        XCTAssertFalse(bundle.doctor.isEmpty)
        XCTAssertEqual(bundle.reportVersion, Report.version)
        XCTAssertEqual(bundle.stageId, "01HZZZSTAGE")
        XCTAssertEqual(bundle.credentialKind, "cst_")
    }

    func testFormattedBundleIsActuallyParseableJson() async throws {
        // 손으로 조립한 직렬화기다 — **파싱되는지**가 이 기능의 성립 조건이다.
        // 번들의 존재 이유가 "파일로 남겨 남에게 보낸다"인데 파싱이 안 되면 무의미하다.
        let bundle = try await Report.build(
            credential: try token,
            http: failingHttp,
            diagnostics: [
                LogEntry(at: 1, level: "warn", code: "error", message: "quote\" and \\ and \n newline")
            ],
            generatedAt: "2026-09-02T12:00:00Z")
        let data = Data(Report.format(bundle).utf8)
        let root = try XCTUnwrap(
            try JSONSerialization.jsonObject(with: data) as? [String: Any])
        XCTAssertEqual(root["reportVersion"] as? Int, Report.version)
        XCTAssertEqual(root["generatedAt"] as? String, "2026-09-02T12:00:00Z")
        let target = try XCTUnwrap(root["target"] as? [String: Any])
        XCTAssertEqual(target["stageId"] as? String, "01HZZZSTAGE")
        XCTAssertFalse((root["doctor"] as? [Any] ?? []).isEmpty)
        // 이스케이프가 깨지면 위 파싱에서 이미 터진다 — 값까지 확인한다.
        let diag = try XCTUnwrap((root["diagnostics"] as? [[String: Any]])?.first)
        XCTAssertEqual(diag["message"] as? String, "quote\" and \\ and \n newline")
    }

    func testServerIsEmbeddedAsANestedObjectNotAnEscapedString() async throws {
        // 같은 reportVersion 인데 생산자 언어에 따라 `server` 타입이 갈리면
        // 번들을 읽는 도구가 언어별로 분기해야 한다 — 계약이 아니게 된다.
        let counter = Counter()
        let http: DoctorHttp = { _, _ in
            if counter.bump() == 1 { throw AdapterError("NotConnected", "ECONNREFUSED") }
            return DoctorHttpResponse(status: 200, body: "{\"version\":\"0.1.8\"}")
        }
        let bundle = try await Report.build(
            credential: try token, http: http, generatedAt: "2026-09-02T12:00:00Z")
        let root = try XCTUnwrap(
            try JSONSerialization.jsonObject(with: Data(Report.format(bundle).utf8))
                as? [String: Any])
        let server = try XCTUnwrap(root["server"] as? [String: Any])
        XCTAssertEqual(server["version"] as? String, "0.1.8")
    }

    func testNonJsonServerBodyDoesNotCorruptTheBundle() async throws {
        // 에러 페이지(HTML)를 그대로 이어붙이면 번들 전체가 못 읽는 파일이 된다.
        let counter = Counter()
        let http: DoctorHttp = { _, _ in
            if counter.bump() == 1 { throw AdapterError("NotConnected", "ECONNREFUSED") }
            return DoctorHttpResponse(status: 200, body: "<html>gateway timeout</html>")
        }
        let bundle = try await Report.build(
            credential: try token, http: http, generatedAt: "2026-09-02T12:00:00Z")
        let root = try XCTUnwrap(
            try JSONSerialization.jsonObject(with: Data(Report.format(bundle).utf8))
                as? [String: Any])
        XCTAssertEqual(root["server"] as? String, "<html>gateway timeout</html>")
    }

    func testEdgeWhitespaceIsNamed() async throws {
        let base = try token
        for credential in [" " + base, base + "\n"] {
            do {
                _ = try await Report.build(credential: credential, http: failingHttp)
                XCTFail("expected ConfigError")
            } catch let err as AdapterError {
                XCTAssertEqual(err.code, "ConfigError")
                XCTAssertTrue(err.message.contains("credential has leading or trailing whitespace"), err.message)
            }
        }
    }

    func testBareDedicatedKeyIsRejectedWithAHint() async {
        do {
            _ = try await Report.build(credential: "ck_abc", http: failingHttp)
            XCTFail("expected ConfigError")
        } catch let err as AdapterError {
            XCTAssertEqual(err.code, "ConfigError")
            XCTAssertTrue(err.message.contains("carries no url/stageId"), err.message)
        } catch { XCTFail("wrong error: \(error)") }
    }
}

/// `@Sendable` 클로저 안에서 상태를 나르기 위한 최소 상자들.
private final class ValueBox: @unchecked Sendable {
    private var values: [Double]
    private let lock = NSLock()
    init(_ v: [Double]) { values = v }
    func next() -> Double {
        lock.lock()
        defer { lock.unlock() }
        return values.removeFirst()
    }
}

private final class Counter: @unchecked Sendable {
    private var n = 0
    private let lock = NSLock()
    func bump() -> Int {
        lock.lock()
        defer { lock.unlock() }
        n += 1
        return n
    }
}

private final class IntBox: @unchecked Sendable {
    private(set) var values: [Int] = []
    private let lock = NSLock()
    func add(_ v: Int) {
        lock.lock()
        defer { lock.unlock() }
        values.append(v)
    }
}
