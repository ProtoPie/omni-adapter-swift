import XCTest

@testable import OmniAdapter

/// `ensureStreamLayer` — Adapter Stream 자리 확보 (TS `socket-transport.test.ts` 와
/// `client.test.ts` 의 ensure describe 미러).
///
/// 트랜스포트 층은 REST 계약(POST+Bearer · 401 재검증 재시도 · 계약대로의 오류 매핑 · 시한 · 취소)을,
/// 클라이언트 층은 roster 기반 `nameConflict` 판정을 본다. HTTP 는 주입된
/// `URLSession` 의 `SequencedStubURLProtocol` 로 가로챈다 — 401→재검증→재시도처럼
/// 호출마다 다른 응답이 필요해서 순서 소비 스텁이어야 한다.
final class EnsureStreamLayerTests: XCTestCase {

    override func setUp() {
        super.setUp()
        SequencedStubURLProtocol.reset()
    }

    // ── 트랜스포트: REST 계약 ───────────────────────────────────────────────

    private static let validateBody = #"{"jwt":"jwt-1","participantId":"p-1"}"#

    private static func ensureBody(created: Bool) -> String {
        #"{"layerId":"layer-1","name":"Test Bridge","created":\#(created)}"#
    }

    private func makeTransport() -> SocketTransport {
        var options = SocketTransportOptions(
            // stageId 는 하이픈 없이 — validate 와 같은 .alphanumerics 인코딩이라
            // "s-1" 은 URL 에 "s%2D1" 로 실리고, 그 인코딩 검증은 이 테스트의 관심사가
            // 아니다(TS 테스트의 관심사도 경로 모양이다).
            url: "http://192.168.0.10:9981", stageId: "s1", secretKey: "ck_token-1")
        options.displayName = "Test Bridge"
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [SequencedStubURLProtocol.self]
        return SocketTransport(options: options, session: URLSession(configuration: config))
    }

    /// validate 까지 끝난(=JWT 보유) 트랜스포트 — TS 의 joinedHarness 미러.
    /// 실제 open() 은 소켓 접속까지 가므로 REST 스텁만으로는 완주하지 못한다.
    private func validatedTransport() async throws -> SocketTransport {
        SequencedStubURLProtocol.enqueue(status: 200, body: Self.validateBody)
        let transport = makeTransport()
        try await transport.validateForTest()
        return transport
    }

    func testPostsWithBearerJwtAndReturnsServerResponse() async throws {
        let transport = try await validatedTransport()
        SequencedStubURLProtocol.enqueue(status: 201, body: Self.ensureBody(created: true))

        let result = try await transport.ensureStreamLayer()

        XCTAssertEqual(
            result, EnsuredStreamLayer(layerId: "layer-1", name: "Test Bridge", created: true))
        XCTAssertEqual(
            SequencedStubURLProtocol.requestedURLs.last,
            "http://192.168.0.10:9981/api/stages/s1/layers/ensure-adapter-stream")
        XCTAssertEqual(SequencedStubURLProtocol.requestedMethods.last, "POST")
        XCTAssertEqual(SequencedStubURLProtocol.requestedAuthorizations.last, "Bearer jwt-1")
    }

    func testAlreadyExistingLayerComesBackAsCreatedFalse() async throws {
        let transport = try await validatedTransport()
        SequencedStubURLProtocol.enqueue(status: 200, body: Self.ensureBody(created: false))

        let result = try await transport.ensureStreamLayer()
        XCTAssertFalse(result.created)
    }

    func testRevalidatesShareTokenOn401AndRetriesOnceWithFreshJwt() async throws {
        let transport = try await validatedTransport()
        SequencedStubURLProtocol.enqueue(status: 401, body: "{}")
        SequencedStubURLProtocol.enqueue(
            status: 200, body: #"{"jwt":"jwt-2","participantId":"p-1"}"#)
        SequencedStubURLProtocol.enqueue(status: 200, body: Self.ensureBody(created: false))

        let result = try await transport.ensureStreamLayer()

        XCTAssertFalse(result.created)
        // validate → ensure(401) → validate(재발급) → ensure(재시도)
        let tails = SequencedStubURLProtocol.requestedURLs.map {
            $0.split(separator: "/").last.map(String.init) ?? ""
        }
        XCTAssertEqual(
            tails, ["validate", "ensure-adapter-stream", "validate", "ensure-adapter-stream"])
        XCTAssertEqual(SequencedStubURLProtocol.requestedAuthorizations.last, "Bearer jwt-2")
    }

    func test404IsNotSupportedWithAHint() async throws {
        // 404 는 라우트가 없을 때뿐이다(구 서버) — 라이브 세션이 없으면 새 서버는 409 RoomEnded 다.
        let transport = try await validatedTransport()
        SequencedStubURLProtocol.enqueue(status: 404, body: "{}")

        do {
            _ = try await transport.ensureStreamLayer()
            XCTFail("expected NotSupported")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "NotSupported")
            // 사용자에게 보이는 문장 — 내부 브랜치 이름(종전 "feature/f3-adapter-mvp")이 아니라 할 일을 말한다(TS 와 같은 문장).
            XCTAssertEqual(error.hint, "This Connect server does not support adapter stream layers yet — update the server.")
            XCTAssertFalse(error.hint?.contains("feature/") ?? true)
        }
    }

    /// 계약의 오류 응답 하나 — 상태와 서버 봉투(`{error, message}`).
    private func ensureFails(_ status: Int, _ body: String) async throws -> AdapterError {
        SequencedStubURLProtocol.reset()
        let transport = try await validatedTransport()
        SequencedStubURLProtocol.enqueue(status: status, body: body)
        do {
            _ = try await transport.ensureStreamLayer()
        } catch let error as AdapterError {
            return error
        }
        XCTFail("expected an AdapterError for \(status)")
        return AdapterError("none", "none")
    }

    func testErrorsAreMappedPerTheContract() async throws {
        // 여섯 언어 같은 규칙: 409+RoomEnded → RoomEnded · 429 → TooManyRequests · 그 밖의 4xx → ValidationError(서버
        // message + 409 ValidationError 면 경우별 힌트) · 5xx → NotConnected. 종전에는 403 이 Unauthorized, 그 밖은 전부
        // "ensure-adapter-stream failed (409)" 의 Unauthorized 였다(조사 실측) — 무엇이 잘못인지 말하지 않았다.
        let ended = try await ensureFails(409, #"{"error":"RoomEnded","message":"no live session"}"#)
        XCTAssertEqual([ended.code, ended.message], ["RoomEnded", "no live session"])

        let limited = try await ensureFails(429, #"{"error":"TooManyRequests","message":"slow down"}"#)
        XCTAssertEqual([limited.code, limited.message], ["TooManyRequests", "slow down"])

        let undeclared = try await ensureFails(
            409, #"{"error":"ValidationError","message":"this adapter did not declare providesView: 'stream'"}"#)
        XCTAssertEqual(undeclared.code, "ValidationError")
        XCTAssertEqual(undeclared.message, "this adapter did not declare providesView: 'stream'")
        XCTAssertEqual(
            undeclared.hint,
            "Connect with providesView: 'stream' — the server only creates and binds a screen layer for adapters that declare a screen.")

        let cap = try await ensureFails(
            409, #"{"error":"ValidationError","message":"the stage reached its Adapter Stream layer limit (32)"}"#)
        XCTAssertEqual(cap.code, "ValidationError")
        XCTAssertEqual(
            cap.hint, "This stage has too many Adapter Stream layers — remove unused ones in the editor, then retry.")

        let forbidden = try await ensureFails(403, #"{"error":"Forbidden","message":"adapter members of this stage only"}"#)
        XCTAssertEqual([forbidden.code, forbidden.message], ["ValidationError", "adapter members of this stage only"])
        XCTAssertNil(forbidden.hint)

        let down = try await ensureFails(503, #"{"error":"InternalServerError","message":"db down"}"#)
        XCTAssertEqual([down.code, down.message], ["NotConnected", "ensure-adapter-stream failed on the server (503): db down"])
    }

    func testWithoutAMessageTheMapperUsesFixedSentences() {
        XCTAssertEqual(
            SocketTransport.mapEnsureError(status: 409, payload: ["error": "RoomEnded"]).message,
            "the stage has no live session")
        XCTAssertEqual(
            SocketTransport.mapEnsureError(status: 429, payload: nil).message,
            "ensure-adapter-stream was rate limited (429)")
        XCTAssertEqual(
            SocketTransport.mapEnsureError(status: 500, payload: nil).message,
            "ensure-adapter-stream failed on the server (500)")
        let bare = SocketTransport.mapEnsureError(status: 415, payload: nil)
        XCTAssertEqual([bare.code, bare.message], ["ValidationError", "ensure-adapter-stream was refused (415)"])
        XCTAssertNil(bare.hint)
        // 힌트는 409 ValidationError 에만 붙는다.
        XCTAssertNil(SocketTransport.mapEnsureError(status: 403, payload: ["error": "Forbidden", "message": "providesView"]).hint)
        // RoomEnded 는 409 + error 코드일 때만 — 다른 409 는 ValidationError 다.
        XCTAssertEqual(SocketTransport.mapEnsureError(status: 409, payload: ["message": "no live session"]).code, "ValidationError")
    }

    func testANetworkFailureIsNotConnected() async throws {
        let transport = try await validatedTransport()
        SequencedStubURLProtocol.enqueue(error: URLError(.cannotConnectToHost))
        do {
            _ = try await transport.ensureStreamLayer()
            XCTFail("expected NotConnected")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "NotConnected")
            XCTAssertTrue(error.message.contains("is the server reachable at http://192.168.0.10:9981?"), error.message)
        }
    }

    func testAFailedRevalidationAfter401IsThrownAsIs() async throws {
        // 401 뒤 재검증이 실패하면 그 오류(어댑터 키 회수 등)를 그대로 던진다 — NotConnected 로 뭉개지 않는다.
        let transport = try await validatedTransport()
        SequencedStubURLProtocol.enqueue(status: 401, body: "{}")
        SequencedStubURLProtocol.enqueue(status: 401, body: #"{"error":"PinNotFound"}"#)
        do {
            _ = try await transport.ensureStreamLayer()
            XCTFail("expected the revalidation error")
        } catch let error as AdapterError {
            XCTAssertNotEqual(error.code, "NotConnected")
            XCTAssertEqual(SequencedStubURLProtocol.hits, 3) // validate · ensure(401) · validate(실패) — 재시도 없음
        }
    }

    private func stallingTransport(ackTimeout: TimeInterval) async throws -> SocketTransport {
        var options = SocketTransportOptions(url: "http://192.168.0.10:9981", stageId: "s1", secretKey: "ck_token-1")
        options.displayName = "Test Bridge"
        options.ackTimeout = ackTimeout
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [SequencedStubURLProtocol.self]
        let transport = SocketTransport(options: options, session: URLSession(configuration: config))
        SequencedStubURLProtocol.enqueue(status: 200, body: Self.validateBody)
        try await transport.validateForTest()
        SequencedStubURLProtocol.enqueueStall()
        return transport
    }

    func testTheRequestTimesOutAtTheAckTimeout() async throws {
        let transport = try await stallingTransport(ackTimeout: 0.3)
        let started = Date()
        let task = Task { try await transport.ensureStreamLayer() }
        // 매달리면 5초 뒤 끊고 실패로 본다 — 시험이 영영 멈추지 않게.
        let watchdog = Task {
            try await Task.sleep(nanoseconds: 5_000_000_000)
            task.cancel()
        }
        defer { watchdog.cancel() }
        do {
            _ = try await task.value
            XCTFail("expected NotConnected")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "NotConnected")
            XCTAssertEqual(error.message, "ensure-adapter-stream did not answer within 0.3 s (the ack timeout)")
        } catch {
            XCTFail("expected NotConnected, got \(error)")
        }
        XCTAssertLessThan(Date().timeIntervalSince(started), 4.5)
        XCTAssertGreaterThanOrEqual(Date().timeIntervalSince(started), 0.2)
    }

    func testATricklingResponseStillTimesOutAtTheAckTimeout() async throws {
        // 본문을 조금씩 흘리는 서버 — URLRequest 의 시한(패킷 사이 공백)은 걸리지 않으니 응답 전체를 ack 시한으로 묶어야 한다.
        // 정수 시한의 문장 모양("1 s" — "1.0 s" 가 아니다)도 함께 본다.
        let transport = try await stallingTransport(ackTimeout: 1)
        SequencedStubURLProtocol.reset()
        SequencedStubURLProtocol.enqueueTrickle()
        let started = Date()
        let task = Task { try await transport.ensureStreamLayer() }
        let watchdog = Task {
            try await Task.sleep(nanoseconds: 6_000_000_000)
            task.cancel()
        }
        defer { watchdog.cancel() }
        do {
            _ = try await task.value
            XCTFail("expected NotConnected")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "NotConnected")
            XCTAssertEqual(error.message, "ensure-adapter-stream did not answer within 1 s (the ack timeout)")
        } catch {
            XCTFail("expected NotConnected, got \(error)")
        }
        XCTAssertLessThan(Date().timeIntervalSince(started), 5)
    }

    func testCancellationPropagatesAsCancellation() async throws {
        // 부른 쪽의 취소는 취소다 — 끊김(NotConnected)으로 바꾸면 재시도 루프가 취소를 못 알아봤다(조사 실측).
        let transport = try await stallingTransport(ackTimeout: 15)
        let task = Task { try await transport.ensureStreamLayer() }
        try await Task.sleep(nanoseconds: 150_000_000)
        task.cancel()
        let started = Date()
        // 취소가 요청을 끊지 못하면 ack 시한(15초)까지 매달린다 — 그 전에 실패로 본다.
        let done = DoneFlag()
        let outcome = Task { () -> Result<EnsuredStreamLayer, Error> in
            defer { done.set() }
            do { return .success(try await task.value) } catch { return .failure(error) }
        }
        let deadline = Date().addingTimeInterval(5)
        while Date() < deadline, !done.isSet { try await Task.sleep(nanoseconds: 20_000_000) }
        guard done.isSet else { return XCTFail("cancellation did not end the request within 5 s") }
        do {
            _ = try await outcome.value.get()
            XCTFail("expected CancellationError")
        } catch is CancellationError {
            // 기대한 대로
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
        XCTAssertLessThan(Date().timeIntervalSince(started), 5)
        XCTAssertEqual(SequencedStubURLProtocol.hits, 2)
    }

    /// validate 는 통과, ensure 는 401, 재검증은 답하지 않는다.
    private func revalidationStallTransport(ackTimeout: TimeInterval) async throws -> SocketTransport {
        var options = SocketTransportOptions(url: "http://192.168.0.10:9981", stageId: "s1", secretKey: "ck_token-1")
        options.displayName = "Test Bridge"
        options.ackTimeout = ackTimeout
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [SequencedStubURLProtocol.self]
        let transport = SocketTransport(options: options, session: URLSession(configuration: config))
        SequencedStubURLProtocol.enqueue(status: 200, body: Self.validateBody)
        try await transport.validateForTest()
        SequencedStubURLProtocol.enqueue(status: 401, body: "{}")
        SequencedStubURLProtocol.enqueueStall()
        return transport
    }

    func testTheAckTimeoutCoversThe401Revalidation() async throws {
        let transport = try await revalidationStallTransport(ackTimeout: 0.4)
        let started = Date()
        let task = Task { try await transport.ensureStreamLayer() }
        let watchdog = Task {
            try await Task.sleep(nanoseconds: 6_000_000_000)
            task.cancel()
        }
        defer { watchdog.cancel() }
        do {
            _ = try await task.value
            XCTFail("expected NotConnected")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "NotConnected")
            XCTAssertEqual(error.message, "ensure-adapter-stream did not answer within 0.4 s (the ack timeout)")
        } catch {
            XCTFail("expected NotConnected, got \(error)")
        }
        XCTAssertLessThan(Date().timeIntervalSince(started), 5)
        XCTAssertEqual(SequencedStubURLProtocol.hits, 3) // validate · ensure(401) · validate(막힘)
    }

    func testCancellationPropagatesThroughThe401Revalidation() async throws {
        // 검수 실측: 재검증 중의 취소가 NotConnected "token/validate request failed … (취소됨)" 으로 바뀌었다.
        let transport = try await revalidationStallTransport(ackTimeout: 15)
        let task = Task { try await transport.ensureStreamLayer() }
        try await Task.sleep(nanoseconds: 200_000_000)
        task.cancel()
        let done = DoneFlag()
        let outcome = Task { () -> Result<EnsuredStreamLayer, Error> in
            defer { done.set() }
            do { return .success(try await task.value) } catch { return .failure(error) }
        }
        let deadline = Date().addingTimeInterval(5)
        while Date() < deadline, !done.isSet { try await Task.sleep(nanoseconds: 20_000_000) }
        guard done.isSet else { return XCTFail("cancellation did not end the revalidation within 5 s") }
        switch await outcome.value {
        case .success: XCTFail("expected CancellationError")
        case .failure(let error): XCTAssertTrue(error is CancellationError, "got \(error)")
        }
    }

    func testCancellingAValidationIsCancellation() async throws {
        // 재검증 자체(재합류 루프도 쓴다)의 취소도 취소다 — NotConnected "… (취소됨)" 으로 바꾸지 않는다.
        var options = SocketTransportOptions(url: "http://192.168.0.10:9981", stageId: "s1", secretKey: "ck_token-1")
        options.displayName = "Test Bridge"
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [SequencedStubURLProtocol.self]
        let transport = SocketTransport(options: options, session: URLSession(configuration: config))
        SequencedStubURLProtocol.enqueueStall()
        let task = Task { try await transport.validateForTest() }
        try await Task.sleep(nanoseconds: 200_000_000)
        task.cancel()
        let done = DoneFlag()
        let outcome = Task { () -> Error? in
            defer { done.set() }
            do { try await task.value; return nil } catch { return error }
        }
        let deadline = Date().addingTimeInterval(5)
        while Date() < deadline, !done.isSet { try await Task.sleep(nanoseconds: 20_000_000) }
        guard done.isSet else { return XCTFail("cancellation did not end the validation within 5 s") }
        let error = await outcome.value
        XCTAssertTrue(error is CancellationError, "got \(String(describing: error))")
    }

    func testARevalidationFailureOnTheServerIsNotConnectedButARevokedKeyPassesThrough() async throws {
        // 401 회복의 재검증이 5xx 면 다시 해 볼 일(NotConnected)이다 — 접속 경로의 매핑(Unauthorized)을 쓰지 않는다.
        var transport = try await validatedTransport()
        SequencedStubURLProtocol.enqueue(status: 401, body: "{}")
        SequencedStubURLProtocol.enqueue(status: 502, body: #"{"message":"upstream down"}"#)
        do {
            _ = try await transport.ensureStreamLayer()
            XCTFail("expected NotConnected")
        } catch let error as AdapterError {
            XCTAssertEqual([error.code, error.message], ["NotConnected", "token/validate failed on the server (502): upstream down"])
        }

        // 회수된 키는 그대로 — 원인을 뭉개지 않는다.
        SequencedStubURLProtocol.reset()
        transport = try await validatedTransport()
        SequencedStubURLProtocol.enqueue(status: 401, body: "{}")
        SequencedStubURLProtocol.enqueue(status: 401, body: #"{"error":"PinNotFound"}"#)
        do {
            _ = try await transport.ensureStreamLayer()
            XCTFail("expected the revalidation error")
        } catch let error as AdapterError {
            XCTAssertNotEqual(error.code, "NotConnected")
        }

        // 접속 경로의 매핑은 그대로다(5xx → 종전 코드).
        SequencedStubURLProtocol.reset()
        SequencedStubURLProtocol.enqueue(status: 502, body: "{}")
        do {
            try await makeTransport().validateForTest()
            XCTFail("expected an error")
        } catch let error as AdapterError {
            XCTAssertNotEqual(error.code, "NotConnected")
        }
    }

    func testHintsAreGivenOnlyWhenTheServerMessageSaysWhy() {
        let cap = SocketTransport.mapEnsureError(status: 409, payload: [
            "error": "ValidationError",
            "message": "This stage already shows 32 Adapter Stream layers — delete one you no longer use before another adapter name can get its own",
        ])
        XCTAssertEqual(cap.hint, "This stage has too many Adapter Stream layers — remove unused ones in the editor, then retry.")
        let undeclared = SocketTransport.mapEnsureError(status: 409, payload: [
            "error": "ValidationError",
            "message": "This adapter did not declare providesView: 'stream' when it joined — an Adapter Stream layer is prepared only for adapters that broadcast a screen",
        ])
        XCTAssertEqual(
            undeclared.hint,
            "Connect with providesView: 'stream' — the server only creates and binds a screen layer for adapters that declare a screen.")
        let other = SocketTransport.mapEnsureError(
            status: 409, payload: ["error": "ValidationError", "message": "display name is not usable"])
        XCTAssertEqual([other.code, other.message], ["ValidationError", "display name is not usable"])
        XCTAssertNil(other.hint)
    }

    func testRenamedTrimsNewlinesLikeTheServer() async throws {
        // 서버는 JS trim(줄바꿈 포함)으로 걷는다 — 줄바꿈이 붙은 요청 이름을 renamed 로 잘못 읽지 않는다.
        var options = SocketTransportOptions(url: "http://192.168.0.10:9981", stageId: "s1", secretKey: "ck_token-1")
        options.displayName = "Test Bridge\n"
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [SequencedStubURLProtocol.self]
        let transport = SocketTransport(options: options, session: URLSession(configuration: config))
        SequencedStubURLProtocol.enqueue(status: 200, body: Self.validateBody)
        try await transport.validateForTest()
        transport.confirmJoinedForTest(Member(participantId: "p-1", displayName: "Test Bridge"))
        SequencedStubURLProtocol.enqueue(status: 200, body: Self.ensureBody(created: false))
        let result = try await transport.ensureStreamLayer()
        XCTAssertFalse(result.renamed)
    }

    func testAClosedTransportIsSessionEndedWithoutARequest() async throws {
        let transport = try await validatedTransport()
        await transport.close()
        do {
            _ = try await transport.ensureStreamLayer()
            XCTFail("expected SessionEnded")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "SessionEnded")
        }
        XCTAssertEqual(SequencedStubURLProtocol.hits, 1) // validate 하나뿐 — ensure 는 나가지 않았다
    }

    func testRenamedWhenTheServerBoundADifferentName() async throws {
        // 같은 이름의 다른 프로그램이 이미 붙어 있어 서버가 `Test Bridge (2)` 로 붙였다 — 레이어도 그 이름으로 확보된다.
        let transport = try await validatedTransport()
        transport.confirmJoinedForTest(Member(participantId: "p-1", displayName: "Test Bridge (2)"))
        SequencedStubURLProtocol.enqueue(status: 201, body: #"{"layerId":"layer-2","name":"Test Bridge (2)","created":true}"#)
        let result = try await transport.ensureStreamLayer()
        XCTAssertTrue(result.renamed)
        XCTAssertTrue(result.nameConflict) // 옛 이름 — 트랜스포트를 바로 써도 같은 값
        XCTAssertEqual(result.name, "Test Bridge (2)")
    }

    func testNotRenamedWhenTheServerKeptTheName() async throws {
        let transport = try await validatedTransport()
        transport.confirmJoinedForTest(Member(participantId: "p-1", displayName: "Test Bridge"))
        SequencedStubURLProtocol.enqueue(status: 200, body: Self.ensureBody(created: false))
        let result = try await transport.ensureStreamLayer()
        XCTAssertFalse(result.renamed)
    }

    func testBeforeValidateThrowsNotConnected() async {
        // JWT 없이 갈 곳이 없다 — 네트워크를 아예 타지 않는다.
        let transport = makeTransport()
        do {
            _ = try await transport.ensureStreamLayer()
            XCTFail("expected NotConnected")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "NotConnected")
        } catch {
            XCTFail("unexpected error: \(error)")
        }
        XCTAssertEqual(SequencedStubURLProtocol.hits, 0)
    }

    // ── 클라이언트: nameConflict 판정 ───────────────────────────────────────

    /// ensure 는 고정 성공, roster 는 주입 — 판정 로직만 격리해서 본다.
    private final class EnsureFakeTransport: Transport, @unchecked Sendable {
        var roster: [Member] = []
        var rosterFails = false
        var renamed = false
        private let lock = NSLock()
        private var ensureCallsStorage = 0
        var ensureCalls: Int { lock.withLock { ensureCallsStorage } }

        func open(handlers: TransportHandlers) async throws -> Joined {
            Joined(roomId: "stage-1", me: Member(participantId: "me-1", displayName: "Me"))
        }
        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult {
            PublishResult(ok: true)
        }
        func listMembers() async throws -> [Member] {
            if rosterFails { throw AdapterError("AckTimeout", "roster down") }
            return roster
        }
        func ensureStreamLayer() async throws -> EnsuredStreamLayer {
            lock.withLock { ensureCallsStorage += 1 }
            return EnsuredStreamLayer(layerId: "layer-1", name: "Me", created: true, renamed: renamed)
        }
        func close() async {}
    }

    private func openedClient(_ transport: Transport) async throws -> ConnectClient {
        let client = ConnectClient(transport: transport)
        try await client.open()
        return client
    }

    func testNameConflictIsTheSameAsRenamedAndIgnoresTheRoster() async throws {
        // roster 판정은 크래시 재시작 뒤 유예 중인 같은 키의 옛 행 · 대기 짝을 충돌로 잘못 셌다(검수) — 이제 renamed 의 옛 이름이다.
        let transport = EnsureFakeTransport()
        transport.roster = [
            Member(participantId: "me-1", displayName: "Me"),
            Member(participantId: "ghost-1", displayName: "Me", providesView: "stream"),
        ]
        let client = try await openedClient(transport)
        let plain = try await client.ensureStreamLayer()
        XCTAssertEqual(plain, EnsuredStreamLayer(layerId: "layer-1", name: "Me", created: true))

        transport.renamed = true
        let renamed = try await client.ensureStreamLayer()
        XCTAssertTrue(renamed.renamed)
        XCTAssertTrue(renamed.nameConflict)
    }

    func testRenamedComesThroughFromTheTransport() async throws {
        let transport = EnsureFakeTransport()
        transport.renamed = true
        let client = try await openedClient(transport)
        let result = try await client.ensureStreamLayer()
        XCTAssertTrue(result.renamed)
    }

    func testAnEndedClientIsSessionEndedWithoutAsking() async throws {
        let transport = EnsureFakeTransport()
        let client = try await openedClient(transport)
        await client.close()
        do {
            _ = try await client.ensureStreamLayer()
            XCTFail("expected SessionEnded")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "SessionEnded")
        }
        XCTAssertEqual(transport.ensureCalls, 0)
    }

    func testTransportWithoutRestSurfaceThrowsNotSupported() async throws {
        // ensureStreamLayer 를 구현하지 않은 트랜스포트 — protocol 확장 기본이 거부한다.
        final class BareTransport: Transport, @unchecked Sendable {
            func open(handlers: TransportHandlers) async throws -> Joined {
                Joined(roomId: "stage-1", me: Member(participantId: "me-1", displayName: "Me"))
            }
            func publish(_ input: PublishInput, ack: Bool) async -> PublishResult {
                PublishResult(ok: true)
            }
            func listMembers() async throws -> [Member] { [] }
            func close() async {}
        }
        let client = try await openedClient(BareTransport())

        do {
            _ = try await client.ensureStreamLayer()
            XCTFail("expected NotSupported")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "NotSupported")
        }
    }
}

/// 한 번 서는 깃발 — 매달릴 수 있는 태스크를 기다리지 않고 끝났는지만 본다.
private final class DoneFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var value = false
    func set() { lock.withLock { value = true } }
    var isSet: Bool { lock.withLock { value } }
}
