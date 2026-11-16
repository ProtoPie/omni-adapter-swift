import XCTest

@testable import OmniAdapter

/// `token/validate` 요청 본문 — **와이어 계약**이다. Dart `validate_body_test.dart` 를
/// 기준본으로 옮겼다.
///
/// 이 스위트가 없어서 Dart 검수 때 세 가지 변이(clientType 스탬프 제거 · strictRouting
/// 상시 전송 제거 · exclusiveName false 접기)가 **전부 통과**했다. 셋 다 서버 동작을
/// 조용히 바꾼다 — 파리티 가드(텍스트 프로브)는 심볼 존재만 보므로 "필드가 실려
/// 나가는가"는 이 테스트만이 지킨다.
final class ValidateBodyTests: XCTestCase {

    override func setUp() {
        super.setUp()
        SequencedStubURLProtocol.reset()
    }

    /// validate 를 한 번 치고 그 **요청 본문**과 URL 을 돌려준다.
    private func captureValidate(
        _ configure: (inout SocketTransportOptions) -> Void = { _ in }
    ) async throws -> (body: [String: Any], url: String) {
        var options = SocketTransportOptions(
            url: "http://192.168.0.10:9981", stageId: "stage1", secretKey: "ck_token-1")
        options.displayName = "Test Bridge"
        configure(&options)

        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [SequencedStubURLProtocol.self]
        let transport = SocketTransport(options: options, session: URLSession(configuration: config))

        // 200 + jwt 라 소켓을 만들지 않고 REST 왕복만 끝난다.
        SequencedStubURLProtocol.enqueue(status: 200, body: #"{"jwt":"jwt-1","participantId":"p-1"}"#)
        try await transport.validateForTest()

        let raw = SequencedStubURLProtocol.requestedBodies.last ?? ""
        let parsed = try JSONSerialization.jsonObject(with: Data(raw.utf8)) as? [String: Any]
        return (body: parsed ?? [:], url: SequencedStubURLProtocol.requestedURLs.last ?? "")
    }

    func testUrlEncodesTheStageId() async throws {
        // ⚠️ 공백만으로는 이 가드를 검증할 수 없다 — 슬래시·물음표는 **경로 구조 자체**를
        // 바꾸므로 그것으로 잠근다.
        let captured = try await captureValidate { $0.stageId = "a/b?c" }
        XCTAssertEqual(
            captured.url, "http://192.168.0.10:9981/api/stages/a%2Fb%3Fc/token/validate")
    }

    func testCarriesTokenDisplayNameAndClientTypeStamp() async throws {
        let captured = try await captureValidate()
        XCTAssertEqual(captured.body["token"] as? String, "ck_token-1")
        XCTAssertEqual(captured.body["displayName"] as? String, "Test Bridge")
        // Backstage 가 어댑터 참여자를 connect-client 노드로 그리는 판별자다.
        XCTAssertEqual(captured.body["clientType"] as? String, "adapter")
    }

    func testStrictRoutingIsAlwaysSentEvenWhenFalse() async throws {
        // 서버는 미신고 필드를 기존 값으로 보존한다. 옵션을 껐는데 옛 true 가 되살아나면
        // 선언 밖 채널이 조용히 안 온다.
        let off = try await captureValidate()
        XCTAssertNotNil(off.body["strictRouting"])
        XCTAssertEqual(off.body["strictRouting"] as? Bool, false)

        let on = try await captureValidate {
            $0.strictRouting = true
            $0.receives = ["a.b"]
        }
        XCTAssertEqual(on.body["strictRouting"] as? Bool, true)
    }

    func testExclusiveNameIsSentEvenWhenFalse() async throws {
        // false 를 접으면 서버에 남은 이전 true 때문에 어댑터가 영원히 대기로 판정된다.
        let off = try await captureValidate { $0.exclusiveName = false }
        XCTAssertNotNil(off.body["exclusiveName"])
        XCTAssertEqual(off.body["exclusiveName"] as? Bool, false)

        let on = try await captureValidate { $0.exclusiveName = true }
        XCTAssertEqual(on.body["exclusiveName"] as? Bool, true)
    }

    func testUndeclaredFieldsAreOmittedEntirely() async throws {
        // "신고 안 함"과 "기본값 주장"은 다르다 — 서버의 보존 규칙이 둘을 다르게 본다.
        let captured = try await captureValidate()
        for key in ["exclusiveName", "providesView", "receives", "sends"] {
            XCTAssertNil(captured.body[key], "\(key) should be absent")
        }
    }

    func testWithoutOptionsTheKeysAreExactlyTheAlwaysSentOnesPlusIdentity() async throws {
        // TS VALIDATE_TOKEN_BODY_ALWAYS_FIELDS + 알아낸 신원 필드 — 이 시험이 도는 Apple 플랫폼에서는 넷 다 알아낸다.
        let captured = try await captureValidate()
        XCTAssertEqual(
            captured.body.keys.sorted(),
            ["token", "displayName", "clientType", "strictRouting", "adapter", "adapterVersion", "runtime", "runtimeVersion"]
                .sorted())
        // 어댑터 신원 — 옵션이 아니라 언제나 싣는다. 사용자 데이터는 없다.
        let os = ProcessInfo.processInfo.operatingSystemVersion
        XCTAssertEqual(captured.body["adapter"] as? String, "swift")
        XCTAssertEqual(captured.body["adapterVersion"] as? String, AdapterIdentity.adapterVersion)
        XCTAssertEqual(captured.body["runtime"] as? String, "apple")
        XCTAssertEqual(
            captured.body["runtimeVersion"] as? String, "\(os.majorVersion).\(os.minorVersion).\(os.patchVersion)")
    }

    func testIdentityRidesAlongWithEveryOption() async throws {
        let captured = try await captureValidate {
            $0.osHostname = "line-3"
            $0.receives = ["a.in"]
            $0.sends = ["a.out"]
            $0.providesView = "web"
            $0.exclusiveName = true
            $0.strictRouting = true
        }
        for key in ["adapter", "adapterVersion", "runtime", "runtimeVersion"] {
            XCTAssertNotNil(captured.body[key], "\(key) should be present")
        }
    }

    func testDeclarationsAreCarriedThrough() async throws {
        let captured = try await captureValidate {
            $0.receives = ["actuator.cmd"]
            $0.sends = ["sensor.temperature"]
            $0.providesView = "stream"
        }
        XCTAssertEqual(captured.body["receives"] as? [String], ["actuator.cmd"])
        XCTAssertEqual(captured.body["sends"] as? [String], ["sensor.temperature"])
        XCTAssertEqual(captured.body["providesView"] as? String, "stream")
    }

    func testOsHostnameIsTruncatedToTheServerLimit() async throws {
        // 라벨 하나 때문에 400 을 받지 않게 — 절단은 무해하다(표시용).
        let captured = try await captureValidate { $0.osHostname = String(repeating: "h", count: 200) }
        XCTAssertEqual((captured.body["osHostname"] as? String)?.count, 128)
    }

    func testBlankOsHostnameIsOmitted() async throws {
        // 서버 zod 는 trim().min(1) — ""/" " 를 실어 보내면 400(ValidationError)이고,
        // 그건 자동 재조인이 **포기하는** 코드라 어댑터가 영영 붙지 못한다.
        for blank in ["", "   "] {
            let captured = try await captureValidate { $0.osHostname = blank }
            XCTAssertNil(captured.body["osHostname"], "blank hostname should be absent")
        }
    }
}
