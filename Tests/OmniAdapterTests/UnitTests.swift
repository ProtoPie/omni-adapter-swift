import XCTest
@testable import OmniAdapter

/// cst_ 토큰/ck_ 정책/와일드카드/에러 힌트 — 다른 언어 스위트 미러.
final class StageTokenTests: XCTestCase {
    func testRoundtrip() throws {
        let token = try StageToken.encode(
            url: "http://192.168.0.10:9981",
            stageId: "01HXYZ",
            secretKey: "ck_share-secret",
            displayName: "Kiosk #3"
        )
        XCTAssertTrue(StageToken.isStageToken(token))
        let payload = try StageToken.parse(token)
        XCTAssertEqual(payload.url, "http://192.168.0.10:9981")
        XCTAssertEqual(payload.stageId, "01HXYZ")
        XCTAssertEqual(payload.secretKey, "ck_share-secret")
        XCTAssertEqual(payload.displayName, "Kiosk #3")
    }

    func testParsesTokenIssuedByTheTypeScriptAdapter() throws {
        // 같은 wire shape {v:1,u,s,k,n} — 언어 간 상호운용 고정
        let wire = #"{"v":1,"u":"https://cloud.protopie.io/api/connect","s":"s1","k":"ck_k1"}"#
        let token = "cst_" + Data(wire.utf8).base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
        let payload = try StageToken.parse(token)
        XCTAssertTrue(payload.url.hasSuffix("/api/connect"))
        XCTAssertNil(payload.displayName)
    }

    func testEncodeMatchesOtherAdaptersByteForByte() throws {
        let token = try StageToken.encode(
            url: "https://cloud.protopie.io/api/connect",
            stageId: "s1",
            secretKey: "ck_k1"
        )
        let wire = #"{"v":1,"u":"https://cloud.protopie.io/api/connect","s":"s1","k":"ck_k1"}"#
        let expected = "cst_" + Data(wire.utf8).base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
        XCTAssertEqual(token, expected)
    }

    func testMalformedTokenIsAConfigError() {
        XCTAssertThrowsError(try StageToken.parse("cst_not-base64-json!!!")) { error in
            XCTAssertEqual((error as? AdapterError)?.code, "ConfigError")
        }
    }

    func testUnsupportedVersionIsRejected() {
        let wire = #"{"v":2,"u":"x","s":"y","k":"z"}"#
        let token = "cst_" + Data(wire.utf8).base64EncodedString()
            .replacingOccurrences(of: "=", with: "")
        XCTAssertThrowsError(try StageToken.parse(token)) { error in
            XCTAssertTrue((error as? AdapterError)?.message.contains("version") ?? false)
        }
    }

    func testEncodeValidatesTheUrl() {
        XCTAssertThrowsError(try StageToken.encode(url: "not-a-url", stageId: "s", secretKey: "ck_k"))
    }
}

final class AdapterKeyPolicyTests: XCTestCase {
    func testEncodeRejectsNonAdapterKeys() {
        // share 토큰은 어댑터 자격증명이 아니다 — 봉투에 담는 것부터 막는다.
        XCTAssertThrowsError(try StageToken.encode(url: "http://h:9981", stageId: "s", secretKey: "share-token")) { error in
            XCTAssertTrue((error as? AdapterError)?.message.contains("does not carry an adapter key") ?? false)
        }
    }

    func testParseRejectsEnvelopesWithoutAdapterKeys() {
        let wire = #"{"v":1,"u":"http://h","s":"y","k":"share"}"#
        let token = "cst_" + Data(wire.utf8).base64EncodedString()
            .replacingOccurrences(of: "=", with: "")
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
        XCTAssertThrowsError(try StageToken.parse(token)) { error in
            XCTAssertEqual((error as? AdapterError)?.code, "ConfigError")
            XCTAssertTrue((error as? AdapterError)?.message.contains("does not carry an adapter key") ?? false)
        }
    }

    func testConnectRejectsUnprefixedSecrets() async {
        do {
            _ = try await connect(ConnectOptions(secretKey: "plain-share-token", url: "http://h:9981", stageId: "s"))
            XCTFail("expected ConfigError")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "ConfigError")
            XCTAssertTrue(error.message.contains("cst_ token") && error.message.contains("ck_"))
        } catch {
            XCTFail("unexpected \(error)")
        }
    }

    func testConnectReportsEdgeWhitespaceBeforeThePrefixCheck() async throws {
        // 종전: 앞 공백 cst_ 는 "cst_ 토큰이 아니다", `ck_…\n` 은 그대로 서버로 가서 "키가 회수됐다" 로 나갔다.
        let token = try StageToken.encode(url: "http://h:9981", stageId: "s", secretKey: "ck_k")
        for secret in [" " + token, token + "\n", "ck_raw-key\n", "\tck_raw-key", "ck_raw-key\r\n"] {
            do {
                _ = try await connect(ConnectOptions(secretKey: secret, url: "http://h:9981", stageId: "s"))
                XCTFail("expected ConfigError")
            } catch let error as AdapterError {
                XCTAssertEqual(error.code, "ConfigError")
                XCTAssertTrue(error.message.contains("credential has leading or trailing whitespace"), error.message)
                XCTAssertTrue(error.hint?.contains("Trim") ?? false)
            } catch {
                XCTFail("unexpected \(error)")
            }
        }
    }

    func testConnectRequiresCoordinatesWithABareKey() async {
        do {
            _ = try await connect(ConnectOptions(secretKey: "ck_bare"))
            XCTFail("expected ConfigError")
        } catch let error as AdapterError {
            XCTAssertTrue(error.message.contains("not self-describing"))
        } catch {
            XCTFail("unexpected \(error)")
        }
    }

    /// 여섯 언어 같은 규칙 — ack 시한은 유한하고 0 보다 크고 2^31−1 ms 이하. 0 이면 폴백이 0.5초 뒤에야 AckTimeout 으로 끝냈고,
    /// `.infinity` · `.nan` 의 ack 발행은 끝나지 않았다(프로브 2.5초). 연결하기 전에(서버에 닿기 전에) ConfigError 로 거절한다.
    func testTheAckTimeoutMustBeFinitePositiveAndAtMost2To31Minus1Ms() async throws {
        for bad in [0, -1, -2, Double.infinity, -Double.infinity, Double.nan, 2_147_483.648, 1e12] {
            var options = ConnectOptions(secretKey: "ck_k", url: "http://127.0.0.1:9", stageId: "s")
            options.ackTimeout = bad
            do {
                _ = try await connect(options)
                XCTFail("\(bad): expected ConfigError")
            } catch let error as AdapterError {
                XCTAssertEqual(error.code, "ConfigError", "\(bad)")
                XCTAssertTrue(error.message.contains("ackTimeout"), error.message)
            }
            // 직접 만든 트랜스포트도 연결하기 전에 같은 오류다.
            var direct = SocketTransportOptions(url: "http://127.0.0.1:9", stageId: "s", secretKey: "ck_k")
            direct.ackTimeout = bad
            do {
                _ = try await SocketTransport(options: direct).open(handlers: TransportHandlers())
                XCTFail("\(bad): expected ConfigError")
            } catch let error as AdapterError {
                XCTAssertEqual(error.code, "ConfigError", "\(bad)")
                XCTAssertTrue(error.message.contains("ackTimeout"), error.message)
            }
        }
        // 경계 안의 값은 통과한다.
        XCTAssertNoThrow(try SocketTransport.validateAckTimeout(0.001))
        XCTAssertNoThrow(try SocketTransport.validateAckTimeout(2_147_483.647))
    }

    /// 같은 규칙 · 같은 자리(연결하기 전) — 0 이면 지연 없는 재접속 · 재조인 루프였고, 음수 · NaN · ∞ 는 대기의 `UInt64(초 * 1e9)` 가
    /// 트랩으로 프로세스를 멈췄다. 메시지가 옵션 이름을 댄다. 여섯 언어 같은 규칙.
    func testTheJoinTimeoutAndTheReconnectAndRejoinDelaysFollowTheAckTimeoutRule() async throws {
        let fields: [(String, WritableKeyPath<SocketTransportOptions, TimeInterval>)] = [
            ("joinTimeout", \.joinTimeout),
            ("reconnectDelay", \.reconnectDelay),
            ("reconnectDelayMax", \.reconnectDelayMax),
            ("rejoinDelay", \.rejoinDelay),
            ("rejoinDelayMax", \.rejoinDelayMax),
        ]
        for (name, path) in fields {
            for bad in [0, -1, -2, Double.infinity, -Double.infinity, Double.nan, 2_147_483.648, 1e12] {
                var options = SocketTransportOptions(url: "http://127.0.0.1:9", stageId: "s", secretKey: "ck_k")
                options[keyPath: path] = bad
                do {
                    _ = try await SocketTransport(options: options).open(handlers: TransportHandlers())
                    XCTFail("\(name)=\(bad): expected ConfigError")
                } catch let error as AdapterError {
                    XCTAssertEqual(error.code, "ConfigError", "\(name)=\(bad)")
                    XCTAssertTrue(error.message.hasPrefix("\(name) must be a finite number"), error.message)
                    // 값 표기는 send 의 거부와 같다 — Swift 의 `inf` · `nan` 이 아니라 `Infinity` · `-Infinity` · `NaN`.
                    let shown = bad.isNaN ? "NaN" : bad == .infinity ? "Infinity" : bad == -.infinity ? "-Infinity" : "\(bad)"
                    XCTAssertTrue(error.message.hasSuffix("— got \(shown)"), error.message)
                }
            }
            XCTAssertNoThrow(try SocketTransport.validateTiming(name, 0.001))
            XCTAssertNoThrow(try SocketTransport.validateTiming(name, 2_147_483.647))
        }
        // connect() 가 넘기는 셋도 같은 오류다.
        let viaConnect: [(String, WritableKeyPath<ConnectOptions, TimeInterval>)] = [
            ("joinTimeout", \.joinTimeout), ("rejoinDelay", \.rejoinDelay), ("rejoinDelayMax", \.rejoinDelayMax),
        ]
        for (name, path) in viaConnect {
            var options = ConnectOptions(secretKey: "ck_k", url: "http://127.0.0.1:9", stageId: "s")
            options[keyPath: path] = 0
            do {
                _ = try await connect(options)
                XCTFail("\(name): expected ConfigError")
            } catch let error as AdapterError {
                XCTAssertEqual(error.code, "ConfigError", name)
                XCTAssertTrue(error.message.hasPrefix("\(name) must be a finite number"), error.message)
            }
        }
        // 기본값은 통과한다 — 닿지 않는 주소라 연결 단계의 다른 오류로 끝난다.
        let defaults = SocketTransportOptions(url: "http://127.0.0.1:9", stageId: "s", secretKey: "ck_k")
        do {
            _ = try await SocketTransport(options: defaults).open(handlers: TransportHandlers())
            XCTFail("expected a connection error")
        } catch let error as AdapterError {
            XCTAssertNotEqual(error.code, "ConfigError", error.message)
        }
    }
}

/// 재개 포기 판정 — 기다릴 실패와 포기할 실패의 구분(T5-3).
///
/// 이 판정이 `AuthRevoked` 하나만 보던 동안, 버전 스큐와 400 본문 거부는 **영구
/// 재시도**로 흘렀다(그 경로는 onError 도 내지 않아 앱에서는 무증상). TS/Python/Dart
/// 와 같은 집합인지도 함께 잠근다 — 손으로 미러링한 목록이라 드리프트 가드 밖이다.
final class RejoinAbandonTests: XCTestCase {
    func testAbandonsWhenWaitingCannotHelp() {
        for code in [
            "AuthRevoked", "ProtocolVersionMismatch", "ClientAheadOfServer",
            "ConfigError", "ValidationError",
        ] {
            // 자동 복귀를 켰든 껐든 결론은 같다 — 기다림이 바꿔주지 않는 실패다.
            XCTAssertTrue(
                SocketTransport.shouldAbandonRejoin(code: code, autoRejoin: true), code)
            XCTAssertTrue(
                SocketTransport.shouldAbandonRejoin(code: code, autoRejoin: false), code)
        }
    }

    func testKeepsWaitingForTransientFailures() {
        // StageFull 도 기다릴 일이다 — 자리가 비면 들어간다(TS 판과 같은 판정).
        for code in ["TooManyRequests", "NotConnected", "AckTimeout", "SessionEnded", "StageFull"] {
            XCTAssertFalse(
                SocketTransport.shouldAbandonRejoin(code: code, autoRejoin: true), code)
            XCTAssertFalse(
                SocketTransport.shouldAbandonRejoin(code: code, autoRejoin: false), code)
        }
    }

    func testRoomEndedDependsOnAutoRejoin() {
        // 세션이 아직 안 열렸다는 뜻이다 — 자동 복귀를 켠 어댑터에게는 기다릴 일.
        XCTAssertFalse(
            SocketTransport.shouldAbandonRejoin(code: "RoomEnded", autoRejoin: true))
        XCTAssertTrue(
            SocketTransport.shouldAbandonRejoin(code: "RoomEnded", autoRejoin: false))
    }

    func testSetIsExactlyTheCanonicalFive() {
        XCTAssertEqual(
            SocketTransport.nonRejoinableCodes,
            [
                "AuthRevoked", "ProtocolVersionMismatch", "ClientAheadOfServer",
                "ConfigError", "ValidationError",
            ])
    }
}

final class WildcardTests: XCTestCase {
    func testExactMatch() {
        XCTAssertTrue(Wildcard.matchChannel("button.tap", "button.tap"))
        XCTAssertFalse(Wildcard.matchChannel("button.tap", "button.tap2"))
    }

    func testStarMatchesOneOrMoreSegments() {
        XCTAssertTrue(Wildcard.matchChannel("robot.*", "robot.status"))
        XCTAssertTrue(Wildcard.matchChannel("robot.*", "robot.arm.pos"))
        XCTAssertFalse(Wildcard.matchChannel("robot.*", "robot"))
        XCTAssertFalse(Wildcard.matchChannel("robot.*", "robots.status"))
    }

    func testBareStarMatchesEverything() {
        XCTAssertTrue(Wildcard.matchChannel("*", "a"))
        XCTAssertTrue(Wildcard.matchChannel("*", "a.b.c"))
    }

    func testRegexSpecialsAreEscaped() {
        XCTAssertFalse(Wildcard.matchChannel("a+b.tap", "aab.tap"))
        XCTAssertTrue(Wildcard.matchChannel("a+b.tap", "a+b.tap"))
    }

    func testEmptySegmentIsNotEatenByStar() {
        // 종전 `[^.]+` 와 같은 의미 — 빈 세그먼트는 세그먼트가 아니다.
        XCTAssertFalse(Wildcard.matchChannel("robot.*", "robot."))
        XCTAssertFalse(Wildcard.matchChannel("*", ""))
        XCTAssertTrue(Wildcard.matchChannel("a..b", "a..b")) // 리터럴 빈 세그먼트는 그대로 비교
    }

    func testHostilePatternFinishesImmediately() {
        // 패턴은 로스터에 실려 온 **남의** receives 항목이다. 종전 정규식은 이 입력에서
        // 역추적이 지수로 터졌다(실측 75초). 세그먼트 DP 는 O(패턴×채널) 이다.
        let hostile = String(repeating: "*.", count: 16) + "x"
        let channel = (0..<34).map { "s\($0)" }.joined(separator: ".")
        let started = Date()
        XCTAssertFalse(Wildcard.matchChannel(hostile, channel))
        XCTAssertLessThan(Date().timeIntervalSince(started), 1.0)
    }
}

final class AdapterErrorTests: XCTestCase {
    func testTheJoin429HintPointsAtFailedJoinsFromThisAddress() {
        // 키 입장 429 는 이 주소의 누적 실패 캡(5분에 20번)이거나 키 하나의 분당 입장 상한(60)이고, 키 상한이 없는 서버(connect-v2
        // 3464e4c4f 전)는 키 입장에도 IP 당 분당 5회 관문을 쓴다. 여섯 언어 바이트까지 같은 문장 — 메시지는 그대로다.
        let error = SocketTransport.mapValidateError(status: 429, code: "TooManyRequests", base: "http://h")
        XCTAssertEqual(error.code, "TooManyRequests")
        XCTAssertEqual(error.message, "token/validate rate limited (429) — retry later")
        XCTAssertNil(error.retryAfterMs)
        XCTAssertEqual(
            error.hint,
            "The server is refusing joins for now: too many failed join attempts from this network address (20 within 5 minutes — a program here still uses a revoked or mistyped token), or more than 60 joins a minute with one adapter key (programs restarting in a loop), or the server is validating more than 200 joins at this moment. Servers without the per-key join limit also refuse a sixth join from one address within a minute. Wait up to 5 minutes, then retry — and fix the program that causes it, or it happens again."
        )
        // 서버가 동시에 200 개 넘게 검증 중일 때의 429 도 말한다(2026-10-23 검수 뒤 추가 — 여섯 언어 같은 절).
        XCTAssertTrue(error.hint?.contains("more than 200 joins at this moment") ?? false)
    }

    /// retryAfterMs 는 10분(600000 ms)에서 자른다 — 터무니없는 값(1e300)도 넘치거나 던지지 않는다(여섯 언어 같은 상한).
    func testRetryAfterMsIsCappedAtTenMinutes() throws {
        func limited(_ details: Any?) -> AdapterError {
            SocketTransport.mapValidateError(status: 429, code: "TooManyRequests", base: "http://h", details: details)
        }
        XCTAssertEqual(SocketTransport.retryAfterMaxMs, 600_000)
        let huge = limited(["retryAfterMs": 3_000_000_000.0])
        XCTAssertEqual(huge.retryAfterMs, 600_000)
        XCTAssertEqual(huge.message, "token/validate rate limited (429) — retry in 600 s")
        XCTAssertTrue(huge.hint?.hasPrefix("Retry in 600 s (the server's retryAfterMs). ") ?? false)
        XCTAssertEqual(limited(["retryAfterMs": 1e300]).retryAfterMs, 600_000)
        XCTAssertEqual(limited(["retryAfterMs": Double.greatestFiniteMagnitude]).retryAfterMs, 600_000)
        XCTAssertEqual(limited(["retryAfterMs": Int.max]).retryAfterMs, 600_000)
        XCTAssertNil(limited(["retryAfterMs": Double.infinity]).retryAfterMs)
        XCTAssertEqual(limited(["retryAfterMs": 600_000]).retryAfterMs, 600_000)
        XCTAssertEqual(limited(["retryAfterMs": 600_001]).retryAfterMs, 600_000)
        XCTAssertEqual(limited(["retryAfterMs": 599_999.5]).retryAfterMs, 600_000)
        XCTAssertEqual(limited(["retryAfterMs": 599_000]).retryAfterMs, 599_000)
        // JSON 본문의 400자리 정수 — JSONSerialization 은 이것을 거부한다(NaN 오류). validate() 는 본문을 `try?` 로 읽으므로 봉투 전체가
        // nil 이 되고 간격은 "모름" 이다 — 어느 쪽이든 던지거나 넘치지 않는다. 1e300 처럼 파싱되는 거대 수는 상한으로 잘린다.
        let body = Data(("{\"retryAfterMs\":" + String(repeating: "9", count: 400) + "}").utf8)
        let parsed = try? JSONSerialization.jsonObject(with: body)
        let fromJSON = limited(parsed).retryAfterMs
        XCTAssertTrue(fromJSON == nil || fromJSON == 600_000, "\(String(describing: fromJSON))")
        let hugeBody = try JSONSerialization.jsonObject(with: Data("{\"retryAfterMs\":1e300}".utf8))
        XCTAssertEqual(limited(hugeBody).retryAfterMs, 600_000)
        // 503 도 같은 상한.
        let material = SocketTransport.mapValidateError(
            status: 503, code: "KeyMaterialUnavailable", base: "http://h", details: ["retryAfterMs": 3_000_000_000.0])
        XCTAssertEqual(material.retryAfterMs, 600_000)
        XCTAssertTrue(material.message.hasSuffix("; retry in 600 s"))
    }

    /// 잘린 키 판정은 validate() 와 doctor 가 같은 함수를 쓴다 — 401 · AuthRevoked · 길이≠46 일 때만 바꾼다.
    func testAdapterKeyLengthRejectionOnlyRewritesA401RevokedWithAShortKey() {
        let revoked = SocketTransport.mapValidateError(status: 401, code: "PinNotFound", base: "http://h")
        let short = SocketTransport.adapterKeyLengthRejection(revoked, status: 401, keyLength: 10)
        XCTAssertEqual(short.code, "AuthRevoked")
        XCTAssertEqual(
            short.message,
            "adapter key rejected (401 PinNotFound) — revoked, expired, or issued for another stage; the key has 10 characters, a server-issued key has 46")
        XCTAssertEqual(short.hint, SocketTransport.adapterKeyLengthHint(10))
        XCTAssertEqual(SocketTransport.adapterKeyLengthRejection(revoked, status: 401, keyLength: 46), revoked)
        let forbidden = SocketTransport.mapValidateError(status: 403, code: "Forbidden", base: "http://h")
        XCTAssertEqual(SocketTransport.adapterKeyLengthRejection(forbidden, status: 403, keyLength: 10), forbidden)
        let limited = SocketTransport.mapValidateError(status: 429, code: nil, base: "http://h")
        XCTAssertEqual(SocketTransport.adapterKeyLengthRejection(limited, status: 401, keyLength: 10), limited)
    }

    /// 429 · 503 의 `details.retryAfterMs`(connect-v2 3464e4c4f · 8fea9402d)는 문장 · 힌트 · retryAfterMs 에 실린다 — 올림, 유한한 양수만.
    func testRetryAfterMsRidesTheMessageTheHintAndTheError() {
        let limited = SocketTransport.mapValidateError(
            status: 429, code: "TooManyRequests", base: "http://h", details: ["retryAfterMs": 42_300])
        XCTAssertEqual(limited.retryAfterMs, 42_300)
        XCTAssertEqual(limited.message, "token/validate rate limited (429) — retry in 43 s")
        XCTAssertTrue(
            limited.hint?.hasPrefix("Retry in 43 s (the server's retryAfterMs). The server is refusing joins") ?? false,
            limited.hint ?? "nil")

        let material = SocketTransport.mapValidateError(
            status: 503, code: "KeyMaterialUnavailable", base: "http://h", details: ["retryAfterMs": 1000.2])
        XCTAssertEqual(material.code, "KeyMaterialUnavailable")
        XCTAssertEqual(material.retryAfterMs, 1001)
        XCTAssertEqual(
            material.message,
            "token/validate refused (503 KeyMaterialUnavailable) — the server cannot read this stage's adapter keys right now; retry in 2 s")
        XCTAssertTrue(material.hint?.hasPrefix("Retry in 2 s (the server's retryAfterMs). ") ?? false, material.hint ?? "nil")
        XCTAssertTrue(material.hint?.contains("revoke the stage's old keys") ?? false)
        // 간격 없는 503 KeyMaterialUnavailable — 문장은 그대로, 힌트는 기본 힌트 그대로.
        let bare = SocketTransport.mapValidateError(status: 503, code: "KeyMaterialUnavailable", base: "http://h")
        XCTAssertNil(bare.retryAfterMs)
        XCTAssertEqual(
            bare.message,
            "token/validate refused (503 KeyMaterialUnavailable) — the server cannot read this stage's adapter keys right now")
        XCTAssertEqual(bare.hint, AdapterError("KeyMaterialUnavailable").hint)

        // 모르는 모양은 없는 것으로 — 문자열 · 불리언 · 0 · 음수 · NaN · details 가 객체가 아님.
        let plain = SocketTransport.mapValidateError(status: 429, code: "TooManyRequests", base: "http://h")
        let rejected: [Any?] = [
            ["retryAfterMs": "5000"], ["retryAfterMs": true], ["retryAfterMs": 0], ["retryAfterMs": -1],
            ["retryAfterMs": Double.nan], "x", NSNull(), nil, [:] as [String: Any],
        ]
        for details in rejected {
            let err = SocketTransport.mapValidateError(
                status: 429, code: "TooManyRequests", base: "http://h", details: details)
            XCTAssertNil(err.retryAfterMs, "\(String(describing: details))")
            XCTAssertEqual(err.hint, plain.hint, "\(String(describing: details))")
            XCTAssertEqual(err.message, plain.message)
        }
        // 503 은 코드가 KeyMaterialUnavailable 일 때만 가른다 — 다른 503 은 종전대로 봉투 코드 그대로(힌트 · 간격 없음).
        let other = SocketTransport.mapValidateError(
            status: 503, code: "ServiceUnavailable", base: "http://h", details: ["retryAfterMs": 5000])
        XCTAssertEqual(other.code, "ServiceUnavailable")
        XCTAssertNil(other.retryAfterMs)
        XCTAssertNil(other.hint)
        XCTAssertEqual(other.message, "token/validate failed (503)")
    }

    func testKeyMaterialUnavailableHasADefaultHintAndStaysRejoinable() {
        XCTAssertTrue(AdapterError("KeyMaterialUnavailable").hint?.contains("revoke the stage's old keys") ?? false)
        XCTAssertEqual(AdapterError("KeyMaterialUnavailable").hint, SocketTransport.keyMaterialUnavailableHint)
        XCTAssertFalse(SocketTransport.nonRejoinableCodes.contains("KeyMaterialUnavailable"))
        XCTAssertFalse(SocketTransport.nonRejoinableCodes.contains("TooManyRequests"))
        XCTAssertFalse(SocketTransport.shouldAbandonRejoin(code: "KeyMaterialUnavailable", autoRejoin: true))
    }

    /// 재조인 하한 — 평소 간격과 서버의 retryAfterMs 중 큰 쪽. 하한은 늘리기만 하고 상한을 넘어도 된다.
    func testRejoinWaitFloorsOnlyUpwards() {
        XCTAssertEqual(SocketTransport.rejoinWait(delay: 4, retryAfterMs: 5000), 5)
        XCTAssertEqual(SocketTransport.rejoinWait(delay: 8, retryAfterMs: 1000), 8)
        XCTAssertEqual(SocketTransport.rejoinWait(delay: 60, retryAfterMs: 90_000), 90)
        XCTAssertEqual(SocketTransport.rejoinWait(delay: 2, retryAfterMs: nil), 2)
        XCTAssertEqual(SocketTransport.rejoinWait(delay: 2, retryAfterMs: 0), 2)
    }

    func testExistingCallSitesStillCompileWithoutRetryAfterMs() {
        let err = AdapterError("RoomEnded", "x", hint: "h")
        XCTAssertNil(err.retryAfterMs)
        XCTAssertEqual(AdapterError("TooManyRequests", "m", hint: "h", retryAfterMs: 1500).retryAfterMs, 1500)
    }

    func testKnownCodesCarryADefaultHint() {
        // 기본값은 키 경로의 문장과 **같다** — 세션을 여는 두 길(Connect 에서 열기 · "Let adapters start a session")을 모두 댄다.
        let hint = AdapterError("RoomEnded", "stage not found").hint
        XCTAssertEqual(
            hint,
            "Open the stage in Connect first, or use a key that can start the session: create one with \"Let adapters start a session\" turned on (Stage → Share → Adapter keys — a key keeps the setting it was issued with)"
                + ", then retry."
        )
        XCTAssertEqual(hint, SocketTransport.adapterKeyNoSessionHint)
    }

    func testStandbyCarriesTheSameHintAsTheOtherLanguages() {
        // TS/Python/Dart 와 같은 문장 — 대기 중 ack 발행이 받는 에러도 다음 행동을 들고 있어야 한다.
        let hint = AdapterError("Standby", "not sent").hint ?? ""
        XCTAssertTrue(hint.contains("resumes automatically"), hint)
        XCTAssertTrue(hint.contains("exclusiveName: false"), hint)
    }

    func testExplicitHintWins() {
        XCTAssertEqual(AdapterError("RoomEnded", "x", hint: "custom").hint, "custom")
    }

    func testUnknownCodeHasNoHint() {
        XCTAssertNil(AdapterError("SomethingElse").hint)
    }
}
