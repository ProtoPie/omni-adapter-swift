import XCTest
@testable import OmniAdapter

/// 심층 방어 — 검수에서 남긴 결함 (TS hardening.test.ts 미러).
final class HardeningTests: XCTestCase {
    private func envelope(_ wire: [String: Any]) -> String {
        let data = try! JSONSerialization.data(withJSONObject: wire)
        let b64 = data.base64EncodedString().replacingOccurrences(of: "=", with: "")
            .replacingOccurrences(of: "+", with: "-").replacingOccurrences(of: "/", with: "_")
        return "cst_" + b64
    }

    func testSocketTransportChecksCredentialShapeEvenWhenConstructedDirectly() async throws {
        let t = SocketTransport(options: SocketTransportOptions(url: "http://h:9981", stageId: "s", secretKey: "plain-share-token"))
        // validate(네트워크) 전에 던진다 — 서버 없이도 실패해야 한다
        do { _ = try await t.open(handlers: TransportHandlers()); XCTFail("expected ConfigError") }
        catch let err as AdapterError {
            XCTAssertEqual(err.code, "ConfigError")
            XCTAssertTrue(err.message.contains("adapter key (ck_"))
        }
        // cst_ 토큰을 트랜스포트에 직접 주는 것도 같은 오류다 — 봉투는 connect() 가 푼다
        let withToken = SocketTransport(options: SocketTransportOptions(url: "http://h:9981", stageId: "s", secretKey: envelope(["v": 1, "u": "http://h", "s": "s", "k": "ck_x"])))
        do { _ = try await withToken.open(handlers: TransportHandlers()); XCTFail("expected ConfigError") }
        catch let err as AdapterError { XCTAssertTrue(err.message.contains("adapter key (ck_")) }
    }

    func testEmptyUrlOrStageInEnvelopeIsMalformed() throws {
        XCTAssertThrowsError(try StageToken.parse(envelope(["v": 1, "u": "", "s": "stage", "k": "ck_x"]))) { XCTAssertTrue("\($0)".contains("empty url/stageId")) }
        XCTAssertThrowsError(try StageToken.parse(envelope(["v": 1, "u": "http://h", "s": "  ", "k": "ck_x"]))) { XCTAssertTrue("\($0)".contains("empty url/stageId")) }
        XCTAssertThrowsError(try StageToken.parse(envelope(["v": 1, "u": "\n\t", "s": "stage", "k": "ck_x"]))) { XCTAssertTrue("\($0)".contains("empty url/stageId")) } // 공백 판정은 줄바꿈 포함
        let ok = try StageToken.parse(envelope(["v": 1, "u": "http://h", "s": "stage", "k": "ck_x"]))
        XCTAssertEqual(ok.url, "http://h"); XCTAssertEqual(ok.stageId, "stage")
    }

    func testDoctorSaysTrimNotReissueForEdgeWhitespace() async throws {
        let token = try StageToken.encode(url: "http://h", stageId: "stage", secretKey: "ck_k")
        for credential in [token + "\n", " " + token, "ck_k\n"] {
            let results = await runDoctor(credential: credential)
            XCTAssertEqual(results.count, 1)
            XCTAssertEqual(results.first?.status, .fail)
            XCTAssertTrue(results.first?.detail?.contains("credential has leading or trailing whitespace") ?? false)
            XCTAssertTrue(results.first?.hint?.contains("Trim") ?? false)
            XCTAssertFalse(results.first?.hint?.contains("Create a fresh") ?? true)
        }
    }

    func testDoctorNamesABareAdapterKeyPrecisely() async throws {
        let results = await runDoctor(credential: "ck_abcdefghij")
        XCTAssertEqual(results.first?.status, .fail)
        XCTAssertTrue(results.first?.detail?.contains("carries no url/stageId") ?? false)
        XCTAssertTrue(results.first?.hint?.contains("cst_ token for diagnosis") ?? false)
    }
}
