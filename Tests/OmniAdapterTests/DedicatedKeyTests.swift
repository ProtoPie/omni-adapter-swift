import XCTest
@testable import OmniAdapter

/// 어댑터 전용 키(`ck_…`) — TS·Python·.NET·Kotlin·Dart 스위트 미러.
///
/// 이 스위트가 없던 동안 "ck_ 는 아직 발급되지 않는다" 거부를 지워도 어떤 테스트도
/// 깨지지 않았다. 초록은 갭 없음을 뜻하지 않는다 — 행동은 언어별 테스트가 잡는다.
final class DedicatedKeyTests: XCTestCase {
    /// 봉투(cst_) 안의 ck_ — 웹 Adapter keys 발급이 주는 통합 자격증명 형태.
    /// 자기서술(url·stageId 동봉)이면서 개별 회수 가능(안의 k 가 ck_). 서버 검증은
    /// k 의 프리픽스로 갈리므로 어댑터는 봉투를 풀어 그대로 실으면 된다.
    func testEnvelopeCanCarryADedicatedKey() throws {
        let token = try StageToken.encode(url: "http://10.0.0.9:9981", stageId: "st-1", secretKey: "ck_live")
        let parsed = try StageToken.parse(token)
        XCTAssertEqual(parsed.url, "http://10.0.0.9:9981")
        XCTAssertEqual(parsed.stageId, "st-1")
        XCTAssertEqual(parsed.secretKey, "ck_live")
    }

    func testDedicatedKeyNeedsCoordinates() async throws {
        do {
            _ = try await connect(ConnectOptions(secretKey: "ck_abc"))
            XCTFail("expected ConfigError")
        } catch let err as AdapterError {
            XCTAssertEqual(err.code, "ConfigError")
            // ck_ 는 자기서술이 아니다 — 무엇이 더 필요한지 말해야 한다.
            XCTAssertTrue(err.message.contains("not self-describing"), err.message)
            XCTAssertTrue((err.hint ?? "").contains("cst_"), err.hint ?? "")
            XCTAssertTrue((err.hint ?? "").contains("Create key, then Copy"), err.hint ?? "")
        }
    }

    /// 자격증명이 비면 무엇을 넘겨야 하는지와, 환경 변수에서 읽었다면 이 프로세스에 그 변수가 없다는 것을 말한다(6언어 같은
    /// 문장).
    func testAMissingCredentialSaysWhatToPassAndWhereItWentMissing() async throws {
        for secretKey in [nil, ""] as [String?] {
            do {
                _ = try await connect(ConnectOptions(secretKey: secretKey))
                XCTFail("expected ConfigError")
            } catch let err as AdapterError {
                XCTAssertEqual(err.code, "ConfigError")
                XCTAssertEqual(
                    err.hint,
                    "pass an adapter token (Share → Adapter keys → Create key, then Copy) — if you read it from an "
                        + "environment variable, it is not set in this process")
            }
        }
    }

    func testDedicatedKeyIsNotRejectedOutright() async throws {
        do {
            _ = try await connect(ConnectOptions(secretKey: "ck_abc"))
            XCTFail("expected ConfigError")
        } catch let err as AdapterError {
            // 회귀 가드 — 서버가 발급을 시작했으므로 이 거부는 되살아나면 안 된다.
            XCTAssertFalse(err.message.contains("not issued yet"), err.message)
            XCTAssertFalse(err.message.contains("reserved"), err.message)
        }
    }
}
