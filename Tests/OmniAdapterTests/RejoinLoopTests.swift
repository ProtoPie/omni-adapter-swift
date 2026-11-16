import Network
import XCTest

@testable import OmniAdapter

/// 재개 루프 **배선** — 순수 판정자(`shouldAbandonRejoin`)가 아니라 루프가 실제로 그
/// 판정을 쓰는지.
///
/// 판정자만 테스트하면 호출부를 지워도(`giveUp = false`) 전부 통과한다 — 실제로 이
/// 배선이 틀린 채(=`AuthRevoked` 만 보던 상태) 오래 살아 있었던 이유가 그것이다.
/// 기존 세션종료 테스트는 `reconnection = false` 로 루프를 껐으므로 여기서만 실제로
/// 루프가 돈다. HTTP 는 주입된 `URLSession` 의 `URLProtocol` 로 가로챈다.
///
/// 시간 판정은 `TestTiming` 의 규칙을 따른다. 합류 시한(joinTimeout)이 시험의 장치가 아닌 자리는 `TestTiming.farAway` 로 밀어
/// 둔다 — "대기를 본 뒤 합류시킨다" 사이가 그 시한보다 길어지면(멈춘 러너) 시도가 먼저 시간 초과로 끝나고, "시한을 기다리지
/// 않고 돌아온다"는 그 시한이 조건 대기(hangGuard)보다 멀어야 결함일 때 반드시 실패한다.
final class RejoinLoopTests: XCTestCase {
    override func tearDown() {
        CountingStubURLProtocol.reset()
        super.tearDown()
    }

    func testUnauthorizedMidSessionRevalidatesInsteadOfSpinning() {
        // 회귀: onConnectError 가 Unauthorized 를 비종결로 처리하고 내장 재연결에
        // 맡기면 소켓 생성 시 스냅샷된 옛 토큰만 재전송돼 무인 어댑터가 영구
        // reconnecting 에 갇힌다. 세션 중 Unauthorized 는 재검증(REST) 부터 다시 해야 한다.
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2"}"#)
        let transport = makeTransport()
        transport.setHandlersForTest(TransportHandlers())
        // 이미 합류한 세션으로 만든다(everJoined = true).
        transport.confirmJoined(me: Member(participantId: "me-1", displayName: "Bridge"), history: nil)

        transport.handleConnectErrorForTest("Unauthorized: token expired")

        // 배선이 없으면 여기서 재검증이 0회다(내장 재연결이 옛 토큰만 재전송).
        let revalidated = expectation(description: "revalidated after Unauthorized")
        DispatchQueue.global().async {
            let deadline = TestTiming.deadline(5)
            while CountingStubURLProtocol.hits < 1 && Date() < deadline { usleep(10_000) }
            revalidated.fulfill()
        }
        wait(for: [revalidated], timeout: TestTiming.hangGuard + 5)
        XCTAssertGreaterThanOrEqual(CountingStubURLProtocol.hits, 1,
            "세션 중 Unauthorized 는 REST 재검증부터 다시 해야 한다")
        closeAndWait(transport)
    }

    /// TCP 를 수락하되 **아무 응답도 하지 않는** 리스너 — socket.io 핸드셰이크가
    /// disconnect/connect_error 없이 hang 하는 진짜 블랙홀을 만든다(연결 거부는 disconnect
    /// 를 내 루프가 재무장하므로 버그가 안 잡힌다).
    private final class BlockingTCPServer {
        private let listener: NWListener
        private let lock = NSLock()
        private var held: [NWConnection] = []
        private var acceptedCount = 0
        private(set) var port: UInt16 = 0
        /// 지금까지 받은 TCP 연결 수 — "소켓을 열었는가" 의 증거.
        var accepted: Int { lock.lock(); defer { lock.unlock() }; return acceptedCount }
        init() throws {
            listener = try NWListener(using: .tcp, on: .any)
            listener.newConnectionHandler = { [weak self] conn in
                conn.start(queue: .global())
                self?.lock.lock(); self?.held.append(conn); self?.acceptedCount += 1; self?.lock.unlock() // 참조 유지 = 붙잡아 둠
            }
            let ready = DispatchSemaphore(value: 0)
            listener.stateUpdateHandler = { if case .ready = $0 { ready.signal() } }
            listener.start(queue: .global())
            _ = ready.wait(timeout: .now() + TestTiming.hangGuard)
            port = listener.port?.rawValue ?? 0
        }
        func stop() {
            listener.cancel()
            lock.lock(); held.forEach { $0.cancel() }; held.removeAll(); lock.unlock()
        }
    }

    func testRejoinDoesNotStallWhenTheNewSocketNeverJoins() throws {
        // 회귀: 재접속 루프가 connectSocket 뒤 join 타임아웃 없이 return 하면, 새 소켓이
        // 아무 이벤트도 안 내는 TCP 블랙홀에 빠졌을 때 루프가 이미 빠져나가 영구
        // reconnecting 에 갇힌다. join 타임아웃이 있으면 시도를 버리고 다음 백오프로 계속
        // 재검증한다(hits≥2). validate 는 200 으로 통과, socket.io 만 블랙홀로 막는다.
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: 0.1)
        transport.setHandlersForTest(TransportHandlers())
        transport.handleSessionEnd("session-closed")

        let retried = expectation(description: "rejoin keeps revalidating after a join that hangs")
        DispatchQueue.global().async {
            let deadline = TestTiming.deadline(6)
            while CountingStubURLProtocol.hits < 2 && Date() < deadline { usleep(20_000) }
            retried.fulfill()
        }
        wait(for: [retried], timeout: TestTiming.hangGuard + 5)
        XCTAssertGreaterThanOrEqual(CountingStubURLProtocol.hits, 2,
            "join 이 hang 하면 루프가 타임아웃 후 계속 재검증해야 한다")
        closeAndWait(transport)
    }

    func testAbandonsRejoinWhenServerRejectsTheBody() {
        // 400 = 버전 스큐/본문 거부. 기다려도 달라지지 않으므로 루프가 끝나야 한다.
        CountingStubURLProtocol.configure(
            status: 400,
            body: """
                {"error":"ValidationError","details":\
                [{"path":["clientType"],"message":"Invalid literal value"}]}
                """)
        let errors = Box<AdapterError>()
        let ended = expectation(description: "rejoin abandoned")
        let reason = Box<String?>()
        let transport = makeTransport()
        var handlers = TransportHandlers()
        handlers.onError = { errors.append($0) }
        handlers.onEnded = {
            reason.append($0)
            ended.fulfill()
        }
        transport.setHandlersForTest(handlers)

        transport.handleSessionEnd("host-ended")

        // 배선이 없으면(포기 판정을 안 쓰면) 여기서 영원히 재시도한다 → 타임아웃.
        wait(for: [ended], timeout: TestTiming.hangGuard + 5)
        XCTAssertEqual(reason.values.first ?? nil, "rejoin-abandoned: ValidationError")
        XCTAssertEqual(errors.values.map(\.code), ["ValidationError"])
        closeAndWait(transport)
    }

    func testKeepsRetryingWhileTheSessionIsMerelyClosed() {
        // 404 = 아직 세션이 안 열렸다. 여러 번 두드리고도 끝내지 않아야 한다.
        // (이 통제군이 없으면 위 테스트는 "루프가 죽어 있어도" 통과할 수 있다.)
        CountingStubURLProtocol.configure(status: 404, body: #"{"error":"NotFound"}"#)
        let ended = Box<String?>()
        let transport = makeTransport()
        var handlers = TransportHandlers()
        handlers.onEnded = { ended.append($0) }
        transport.setHandlersForTest(handlers)

        transport.handleSessionEnd("host-ended")

        // 최소 3회 재시도를 기다린다 — 루프가 살아 도는 것 자체의 증거다.
        let retried = expectation(description: "validate retried")
        DispatchQueue.global().async {
            let deadline = TestTiming.deadline(5)
            while CountingStubURLProtocol.hits < 3 && Date() < deadline {
                usleep(10_000)
            }
            retried.fulfill()
        }
        wait(for: [retried], timeout: TestTiming.hangGuard + 5)
        XCTAssertGreaterThanOrEqual(CountingStubURLProtocol.hits, 3, "rejoin loop stopped hitting validate")
        XCTAssertTrue(ended.values.isEmpty, "should still be waiting, ended with: \(ended.values)")
        closeAndWait(transport)
    }

    /// 재조인 실패의 retryAfterMs(429 · 503 의 `details.retryAfterMs`)는 다음 시도의 하한이다 — 그 뒤 간격은 평소대로(여섯 언어 같은
    /// 규칙). 평소 간격은 0.01→0.02 s 라 두드림 사이가 수십 ms 인데, 하한 0.4 s 를 받은 다음 한 번만 그만큼 기다려야 한다. 실시간으로
    /// 재는 시험이라 아래쪽 경계만 단단히 보고(부하에 밀려도 짧아지지는 않는다) 위쪽은 넉넉히 둔다.
    func testARetryAfterMsFromAFailedRejoinFloorsTheNextWaitOnly() {
        // 첫 답만 429 + 하한 0.4 s, 그 뒤는 503 KeyMaterialUnavailable + 작은 하한(1 ms) — 답의 차례는 스텁이 요청이 닿은 순서로
        // 정한다. 종전에는 첫 두드림을 본 뒤 시험이 답을 바꿨는데, 그 사이가 0.4 초보다 길어지면(멈춘 러너) 둘째도 429 를 받았다.
        CountingStubURLProtocol.configure(
            status: 503, body: #"{"error":"KeyMaterialUnavailable","details":{"retryAfterMs":1}}"#)
        CountingStubURLProtocol.answerFirst(
            status: 429, body: #"{"error":"TooManyRequests","details":{"retryAfterMs":400}}"#)
        let ended = Box<String?>()
        let errors = Box<AdapterError>()
        let waits = Box<TimeInterval>()
        let transport = makeTransport()
        transport.rejoinWaitForTest = { waits.append($0) }
        var handlers = TransportHandlers()
        handlers.onEnded = { ended.append($0) }
        handlers.onError = { errors.append($0) }
        transport.setHandlersForTest(handlers)

        transport.handleSessionEnd("host-ended")

        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits >= 3 }, "the rejoin loop stopped: \(CountingStubURLProtocol.hits) hits")
        // 실제로 그만큼 잤다 — 하한만 잰다(느린 러너는 간격을 늘릴 뿐이다). 시각은 스텁이 요청을 받은 때 찍는다: 시험 스레드가
        // 두드림을 "본" 때로 재면 첫 관측이 늦은 만큼 간격이 짧게 재어진다.
        let arrivals = CountingStubURLProtocol.hitTimes
        XCTAssertGreaterThanOrEqual(
            arrivals[1].timeIntervalSince(arrivals[0]), 0.35,
            "the server's retryAfterMs (400 ms) must floor the next rejoin wait (usual 0.02 s)")
        // 하한(1 ms)이 평소 간격(0.02 s)보다 작으면 평소 간격 — 하한은 늘리기만 하고, 0.4 s 하한은 한 번만 적용된다. 세 번째
        // 두드림이 "0.3 초 안에 왔는가"를 벽시계로 재는 대신 루프가 잔 값 자체를 본다(평소 0.01 → 0.02, 상한 0.02).
        XCTAssertEqual(
            Array(waits.values.prefix(3)), [0.01, 0.4, 0.02],
            "a retryAfterMs smaller than the usual delay must not stretch the wait — and the 400 ms floor applies once")
        // 429 · 503 은 포기 코드가 아니다 — 오류 알림 없이 기다렸다.
        XCTAssertTrue(ended.values.isEmpty, "a waitable refusal ended the session: \(ended.values)")
        XCTAssertFalse(errors.values.map(\.code).contains("KeyMaterialUnavailable"), "\(errors.values)")
        XCTAssertFalse(errors.values.map(\.code).contains("TooManyRequests"), "\(errors.values)")
        closeAndWait(transport)
    }

    /// 합류마다(재합류 포함) 그 합류의 나를 connected **직전에** 알린다 — 재합류는 다른 participantId 를 받을 수 있다(guest JWT
    /// 시한 · 쿠키 유실). 클라이언트는 그것으로 me 를 바꾼 뒤 그 connected 를 처리한다.
    func testEveryJoinAnnouncesItsMemberBeforeConnected() {
        let transport = makeTransport()
        let log = Box<String>()
        var handlers = TransportHandlers()
        handlers.onJoined = { log.append("joined:\($0.participantId)") }
        handlers.onState = { log.append("state:\($0)") }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.confirmJoinedForTest(Member(participantId: "me-2", displayName: "Bridge"))  // 재합류 — 새 참여자
        XCTAssertEqual(log.values, ["joined:me-1", "state:connected", "joined:me-2", "state:connected"])
        closeAndWait(transport)
    }

    // ── 재개 루프가 자기 합류를 알아본다 ────────────────────────────────────

    /// 회귀: `confirmJoined` 가 최초 합류에서만 join 대기를 깨워, 재개 루프의 `awaitJoin` 이 **성공한**
    /// 재합류를 joinTimeout 뒤 실패로 읽고 멀쩡한 소켓을 버린 뒤 다시 붙기를 끝없이 반복했다(실측:
    /// 40초에 재합류 6회). 최초 합류는 소켓 없이 주입하므로 validate 는 재개 한 번 = 정확히 1회다.
    func testRejoinRecognisesItsOwnJoin() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        // 합류 시한은 멀리 둔다 — 종전(0.3초)에는 대기를 본 뒤 합류시키기까지 0.3초가 넘으면(멈춘 러너) 그 시도가 정말로 시간
        // 초과로 끝나 JoinTimeout · 재검증이 났다. 결함(합류가 대기를 깨우지 않는다)은 대기가 남아 있는 것으로 직접 본다.
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
        let errors = Box<AdapterError>()
        let states = Box<TransportState>()
        let ended = Box<String?>()
        var handlers = TransportHandlers()
        handlers.onError = { errors.append($0) }
        handlers.onState = { states.append($0) }
        handlers.onEnded = { ended.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        transport.handleSessionEnd("host-ended")
        // 재개 루프가 재검증하고 새 소켓으로 stage:joined 를 기다리는 지점까지.
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 1 && transport.pendingJoinForTest },
            "rejoin loop never reached the join wait")
        // 서버의 stage:joined push 를 새 소켓 위에서 흉내 낸다.
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        // 합류가 루프의 대기를 깨웠다 — 고치기 전에는 대기가 시한까지 남아 있다가 JoinTimeout · 소켓 폐기 · 재검증이 난다.
        XCTAssertTrue(waitUntil { !transport.pendingJoinForTest }, "the rejoin loop's join wait was not resumed by its own join")
        Thread.sleep(forTimeInterval: 0.3)  // 그 뒤에 더 하는 일이 없는지 볼 창
        XCTAssertFalse(errors.values.contains { $0.code == "JoinTimeout" },
            "a successful rejoin was read as a join timeout: \(errors.values)")
        XCTAssertEqual(CountingStubURLProtocol.hits, 1, "the rejoin loop kept re-validating after a successful join")
        XCTAssertTrue(transport.hasSocketForTest, "the joined socket was disposed")
        XCTAssertEqual(states.values.last, .connected)
        XCTAssertTrue(ended.values.isEmpty)
        closeAndWait(transport)
    }

    // ── 게스트 쿠키는 트랜스포트마다 ───────────────────────────────────────

    /// 서버는 validate 응답에 게스트 쿠키를 심고, 그 쿠키가 다시 오면 같은 participantId 로 들인다. 종전에는 `URLSession.shared`
    /// 의 쿠키 저장소가 프로세스에 하나라 한 프로세스의 두 클라이언트가 같은 참여자가 됐다(실측). 이제 트랜스포트가 쿠키를
    /// 직접 들고 다닌다 — 같은 세션을 함께 써도 섞이지 않고, 자기 재검증에서는 같은 id 로 돌아온다.
    func testEachTransportCarriesItsOwnGuestCookieEvenOnASharedSession() async throws {
        CookieStubURLProtocol.reset()
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [CookieStubURLProtocol.self]
        let shared = URLSession(configuration: config) // 두 트랜스포트가 **같은** 세션을 쓴다
        func transport() -> SocketTransport {
            var options = SocketTransportOptions(url: "http://127.0.0.1:9", stageId: "s-1", secretKey: "ck_token-1")
            options.displayName = "Bridge"
            return SocketTransport(options: options, session: shared)
        }
        let a = transport()
        let b = transport()
        try await a.validateForTest()
        try await b.validateForTest()
        XCTAssertNotEqual(a.authTokenForTest, b.authTokenForTest, "two clients became the same participant")
        let first = a.authTokenForTest
        try await a.validateForTest() // a 의 재합류 — 자기 쿠키로 같은 id
        XCTAssertEqual(a.authTokenForTest, first)
        XCTAssertEqual(CookieStubURLProtocol.cookies, ["-", "-", "pp_stage_guest_s-1=guest-p-1"])
    }

    /// 서버가 쿠키를 새로 주면(재발급) 그 새 쿠키를 들고 다닌다 — 처음 받은 것을 고집하지 않는다.
    func testAReissuedGuestCookieReplacesTheOldOne() async throws {
        CookieStubURLProtocol.reset()
        CookieStubURLProtocol.rotate = true
        defer { CookieStubURLProtocol.rotate = false }
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [CookieStubURLProtocol.self]
        var options = SocketTransportOptions(url: "http://127.0.0.1:9", stageId: "s-1", secretKey: "ck_token-1")
        options.displayName = "Bridge"
        let transport = SocketTransport(options: options, session: URLSession(configuration: config))
        try await transport.validateForTest()  // 쿠키 #1
        try await transport.validateForTest()  // 쿠키 #1 을 보내고 #2 를 받는다
        try await transport.validateForTest()  // #2 를 보내야 한다
        XCTAssertEqual(CookieStubURLProtocol.cookies.count, 3)
        XCTAssertEqual(CookieStubURLProtocol.cookies[1], "pp_stage_guest_s-1=guest-p-1")
        XCTAssertEqual(CookieStubURLProtocol.cookies[2], "pp_stage_guest_s-1=guest-p-1-r2", "the reissued cookie was not carried")
    }

    /// URL 로딩 시스템의 쿠키 저장소를 쓰지 않는다 — 진짜 HTTP 서버로 확인한다(URLProtocol 대역은 저장소를 거치지 않아 볼 수
    /// 없다). 같은 세션(같은 저장소)을 쓰는 두 트랜스포트가 서로의 쿠키를 보내지 않는다.
    func testTheSessionCookieStoreIsNotUsedEvenOnARealHTTPServer() async throws {
        let server = try TinyValidateServer()
        defer { server.stop() }
        let session = URLSession(configuration: .ephemeral)  // 두 트랜스포트가 같은 세션 · 같은 쿠키 저장소
        func transport() -> SocketTransport {
            var options = SocketTransportOptions(url: "http://127.0.0.1:\(server.port)", stageId: "s-1", secretKey: "ck_token-1")
            options.displayName = "Bridge"
            return SocketTransport(options: options, session: session)
        }
        let a = transport()
        let b = transport()
        try await a.validateForTest()
        try await b.validateForTest()
        XCTAssertNotEqual(a.authTokenForTest, b.authTokenForTest, "two clients became one participant")
        try await a.validateForTest()
        XCTAssertEqual(server.cookies, ["-", "-", "pp_stage_guest_s-1=guest-p-1"])
    }

    // ── 끊긴 연결의 ack 는 오지 않는다 ─────────────────────────────────────

    /// socket.io 의 ack 는 연결 하나에 묶인다 — 끊긴 연결의 답은 오지 않는다. 종전에는 ack 시한(15초)까지 기다려, 끊긴 순간
    /// 진행 중이던 flush 발행이 재접속 뒤에도 큐를 붙잡았고 끊김에 걸린 대기 판정이 판정 창을 그만큼 열어 두었다. 이제
    /// 곧바로 nil(→ AckTimeout)이다(TS · Python 과 같다).
    func testAnInFlightAckIsReleasedAtOnceWhenTheConnectionDrops() async throws {
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        // ack 시한을 멀리 둔다 — 끊김이 풀어 주지 않으면 그 시한(60초)까지 매달려 아래 조건 대기(hangGuard)에 걸린다. 종전에는
        // 시한 15초에 "1초 안"을 쟀다(여유 1초).
        let transport = makeTransport(ackTimeout: TestTiming.farAway)
        transport.setHandlersForTest(TransportHandlers())
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        let released = Box<Bool>()
        let waiting = Task { () -> [String: Any]? in
            let reply = await transport.pendingAckForTest()
            released.append(true)
            return reply
        }
        await TestTiming.until { transport.pendingAckCountForTest == 1 }
        XCTAssertEqual(transport.pendingAckCountForTest, 1)
        transport.disconnectedForTest("transport close")
        let atOnce = await TestTiming.until { !released.values.isEmpty }
        XCTAssertTrue(atOnce, "the waiting ack was not released at once")
        guard atOnce else { return closeAndWait(transport) }
        let reply = await waiting.value
        XCTAssertNil(reply)
        XCTAssertEqual(transport.pendingAckCountForTest, 0)
        closeAndWait(transport)
    }

    // ── 서버가 네임스페이스를 끊었다 = 세션 종료 ────────────────────────────

    /// 키 회수·만료 때 서버는 `stage:ended` 없이 `disconnect(true)` 만 한다. 재검증이 401 이면 포기한다.
    func testServerDisconnectWithARevokedKeyAbandonsAsAuthRevoked() {
        CountingStubURLProtocol.configure(status: 401, body: #"{"error":"Unauthorized"}"#)
        let transport = makeTransport()
        let errors = Box<AdapterError>()
        let reason = Box<String?>()
        let ended = expectation(description: "ended")
        var handlers = TransportHandlers()
        handlers.onError = { errors.append($0) }
        handlers.onEnded = {
            reason.append($0)
            ended.fulfill()
        }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        transport.disconnectedForTest(SocketTransport.serverDisconnectReason)

        wait(for: [ended], timeout: TestTiming.hangGuard + 5)
        XCTAssertEqual(reason.values, ["rejoin-abandoned: AuthRevoked"])
        XCTAssertEqual(errors.values.filter { $0.code == "AuthRevoked" }.count, 1)
        closeAndWait(transport)
    }

    /// 살아 있는 키면 재검증 → 새 소켓으로 다시 붙는다(끝나지 않는다).
    func testServerDisconnectWithALiveKeyRejoinsOnANewSocket() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
        let states = Box<TransportState>()
        let ended = Box<String?>()
        var handlers = TransportHandlers()
        handlers.onState = { states.append($0) }
        handlers.onEnded = { ended.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        transport.disconnectedForTest(SocketTransport.serverDisconnectReason)
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 1 && transport.pendingJoinForTest },
            "server disconnect did not rejoin through token/validate")
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        XCTAssertEqual(states.values.suffix(2), [.reconnecting, .connected])
        XCTAssertTrue(transport.hasSocketForTest)
        XCTAssertTrue(ended.values.isEmpty, "a live key must not end the session: \(ended.values)")
        closeAndWait(transport)
    }

    /// autoRejoin 을 끈 어댑터는 `server-disconnect` 로 끝난다 — 재검증도 하지 않는다.
    func testServerDisconnectWithoutAutoRejoinEndsTheSession() {
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(autoRejoin: false)
        let states = Box<TransportState>()
        let reason = Box<String?>()
        var handlers = TransportHandlers()
        handlers.onState = { states.append($0) }
        handlers.onEnded = { reason.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        transport.disconnectedForTest(SocketTransport.serverDisconnectReason)

        XCTAssertEqual(reason.values, ["server-disconnect"])
        XCTAssertEqual(states.values.last, .ended)
        Thread.sleep(forTimeInterval: 0.2)
        XCTAssertEqual(CountingStubURLProtocol.hits, 0, "an ended session must not re-validate")
        closeAndWait(transport)
    }

    /// 우리가 끊은 것(close · 재개 중 소켓 폐기 = "Namespace leave")과 전송 끊김(핑 타임아웃)은
    /// 서버 끊김이 아니다 — 세션을 끝내지 않는다.
    func testOwnAndTransportDisconnectsAreNotServerDisconnects() async {
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        for raw in ["Namespace leave", "Ping timeout", "Socket Disconnected"] {
            let transport = makeTransport(autoRejoin: false, reconnection: false)
            let states = Box<TransportState>()
            let reason = Box<String?>()
            var handlers = TransportHandlers()
            handlers.onState = { states.append($0) }
            handlers.onEnded = { reason.append($0) }
            transport.setHandlersForTest(handlers)
            transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

            transport.disconnectedForTest(raw)

            XCTAssertTrue(reason.values.isEmpty, "\(raw) ended the session: \(reason.values)")
            XCTAssertEqual(states.values.last, .reconnecting, raw)
            await transport.close()
        }

        // close() 가 먼저 끝냈으면 뒤따르는 끊김 이벤트는(서버 사유여도) 아무것도 하지 않는다 —
        // reconnecting 도 알리지 않는다(우리가 닫은 것이다).
        let transport = makeTransport(autoRejoin: false)
        let reason = Box<String?>()
        let states = Box<TransportState>()
        var handlers = TransportHandlers()
        handlers.onEnded = { reason.append($0) }
        handlers.onState = { states.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        await transport.close()
        transport.disconnectedForTest("Namespace leave")
        transport.disconnectedForTest(SocketTransport.serverDisconnectReason)
        XCTAssertTrue(reason.values.isEmpty, "close() produced \(reason.values)")
        XCTAssertEqual(states.values, [.connected, .ended], "our own close announced \(states.values)")
    }

    // ── 합류 뒤의 종결 거부 = 세션 종료 ──────────────────────────────────────

    /// 회귀: 재개 루프의 새 소켓 핸드셰이크가 버전 스큐로 거부되면 상태만 ended 가 되고 onEnded 가
    /// 불리지 않았다 — 클라이언트의 종료 처리가 돌지 않아 ack 를 기다리던 발행이 영영 매달렸다.
    /// 재개 루프가 그 소켓의 합류를 **기다리는 중**이어도 ended 는 정확히 한 번이다.
    func testTerminalHandshakeRejectionAfterJoinEndsTheSessionOnce() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
        let errors = Box<AdapterError>()
        let states = Box<TransportState>()
        let reason = Box<String?>()
        var handlers = TransportHandlers()
        handlers.onError = { errors.append($0) }
        handlers.onState = { states.append($0) }
        handlers.onEnded = { reason.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.handleSessionEnd("host-ended")
        XCTAssertTrue(waitUntil { transport.pendingJoinForTest }, "rejoin loop never reached the join wait")

        transport.handleConnectErrorForTest("ProtocolVersionMismatch")
        Thread.sleep(forTimeInterval: 0.3) // 깨어난 재개 루프가 무엇을 더 하는지 볼 시간
        transport.handleConnectErrorForTest("ProtocolVersionMismatch") // 끝난 뒤의 거부는 다시 알리지 않는다

        XCTAssertEqual(reason.values, ["rejoin-abandoned: ProtocolVersionMismatch"])
        XCTAssertEqual(states.values.last, .ended)
        XCTAssertEqual(errors.values.map(\.code), ["ProtocolVersionMismatch"])
        XCTAssertEqual(CountingStubURLProtocol.hits, 1, "an ended session must not re-validate")
        closeAndWait(transport)
    }

    /// 합류 뒤 Unauthorized 핸드셰이크 → REST 재검증 → 401 이면 AuthRevoked 로 끝난다.
    func testUnauthorizedHandshakeAfterJoinWithARevokedKeyEndsAsAuthRevoked() {
        CountingStubURLProtocol.configure(status: 401, body: #"{"error":"Unauthorized"}"#)
        let transport = makeTransport()
        let errors = Box<AdapterError>()
        let reason = Box<String?>()
        let ended = expectation(description: "ended")
        var handlers = TransportHandlers()
        handlers.onError = { errors.append($0) }
        handlers.onEnded = {
            reason.append($0)
            ended.fulfill()
        }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        transport.handleConnectErrorForTest("Unauthorized: token expired")

        wait(for: [ended], timeout: TestTiming.hangGuard + 5)
        Thread.sleep(forTimeInterval: 0.2)
        XCTAssertEqual(reason.values, ["rejoin-abandoned: AuthRevoked"])
        XCTAssertTrue(errors.values.contains { $0.code == "AuthRevoked" })
        closeAndWait(transport)
    }

    /// 최초 open 중의 같은 거부는 open() 이 던진다 — ended 이벤트는 없다.
    func testTerminalHandshakeRejectionDuringOpenFailsOpenWithoutEnded() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-1","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
        let reason = Box<String?>()
        var handlers = TransportHandlers()
        handlers.onEnded = { reason.append($0) }
        // Swift 5.10 은 `var` 를 Task 안에서 참조하지 못한다 — 캡처 목록으로 값을 복사해 넘긴다.
        let opening = Task { [handlers] in try await transport.open(handlers: handlers) }
        let deadline = TestTiming.deadline(5)
        while !transport.pendingJoinForTest, Date() < deadline { try await Task.sleep(nanoseconds: 10_000_000) }
        XCTAssertTrue(transport.pendingJoinForTest, "open() never reached the join wait")

        transport.handleConnectErrorForTest("ProtocolVersionMismatch")

        do {
            _ = try await opening.value
            XCTFail("open() should fail")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "ProtocolVersionMismatch")
        }
        try await Task.sleep(nanoseconds: 200_000_000)
        XCTAssertTrue(reason.values.isEmpty, "open() reported the failure — no ended: \(reason.values)")
        await transport.close()
    }

    // ── 버려진 시도 · 합류 전 종료 · 한 번만 (A5) ────────────────────────────

    /// (a) 끝난 뒤 시작된 합류 시도는 소켓을 열지 않고(좌석을 잡는 좀비 연결 없음), 늦게 온 합류는
    /// 상태를 되살리지 않는다.
    func testAJoinAttemptAfterCloseOpensNoSocketAndALateJoinIsIgnored() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
        let states = Box<TransportState>()
        var handlers = TransportHandlers()
        handlers.onState = { states.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        await transport.close()

        // 곧바로 실패한다 — 소켓을 열고 기다리면 합류 시한(60초)까지 걸려 조건 대기(hangGuard)에 걸린다.
        let outcome = Box<Bool>()
        Task {
            do {
                try await transport.awaitJoinForTest()
                outcome.append(true)
            } catch { outcome.append(false) }
        }
        let returned = await TestTiming.until { !outcome.values.isEmpty }
        XCTAssertTrue(returned, "the attempt waited for a socket it should not have opened")
        XCTAssertEqual(outcome.values.first, false, "a join attempt on a closed transport must fail")
        try await Task.sleep(nanoseconds: 300_000_000)
        XCTAssertEqual(blackhole.accepted, 0, "a closed transport opened a socket")

        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        XCTAssertEqual(states.values.last, .ended, "a late join revived a closed transport")
    }

    /// (b) autoRejoin 을 끈 어댑터도 합류 뒤 Unauthorized(JWT 만료)에 한 번 재검증해, 살아 있는 키면 다시 붙는다.
    func testUnauthorizedAfterJoinWithoutAutoRejoinRevalidatesAndRejoins() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway, autoRejoin: false)
        let states = Box<TransportState>()
        let reason = Box<String?>()
        var handlers = TransportHandlers()
        handlers.onState = { states.append($0) }
        handlers.onEnded = { reason.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        transport.handleConnectErrorForTest("Unauthorized: token expired")
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 1 && transport.pendingJoinForTest },
            "no revalidation after a post-join Unauthorized")
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        XCTAssertEqual(states.values.last, .connected)
        XCTAssertTrue(reason.values.isEmpty, "a live key ended the session: \(reason.values)")
        closeAndWait(transport)
    }

    /// (b) 그 재검증이 실패하면 기다리지 않고 끝낸다 — 공유 다섯 코드는 rejoin-abandoned, 그 밖은 reauth-failed.
    func testUnauthorizedAfterJoinWithoutAutoRejoinEndsWhenRevalidationFails() {
        for (status, body, expected) in [
            (404, #"{"error":"NotFound"}"#, "reauth-failed"),
            (401, #"{"error":"Unauthorized"}"#, "rejoin-abandoned: AuthRevoked"),
        ] {
            CountingStubURLProtocol.configure(status: status, body: body)
            let transport = makeTransport(autoRejoin: false)
            let reason = Box<String?>()
            let ended = expectation(description: "ended \(status)")
            var handlers = TransportHandlers()
            handlers.onEnded = {
                reason.append($0)
                ended.fulfill()
            }
            transport.setHandlersForTest(handlers)
            transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

            transport.handleConnectErrorForTest("Unauthorized: token expired")

            wait(for: [ended], timeout: TestTiming.hangGuard + 5)
            Thread.sleep(forTimeInterval: 0.2)
            XCTAssertEqual(reason.values, [expected])
            XCTAssertEqual(CountingStubURLProtocol.hits, 1, "revalidated more than once")
            closeAndWait(transport)
        }
    }

    /// (c) 첫 합류 전의 세션 종료는 그 open() 의 실패다 — 재개 루프를 띄우지 않는다(나중에 혼자 합류한
    /// 트랜스포트가 아무도 쥐지 않은 좌석을 잡았다). 서버 끊김이면 직전 stage:error 를 문장에 싣는다.
    func testASessionEndBeforeTheFirstJoinFailsThatOpen() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        let cases: [(name: String, trigger: (SocketTransport) -> Void, code: String, mentions: String?)] = [
            ("stage:ended", { $0.sessionEndedForTest("host-ended") }, "RoomEnded", nil),
            ("stage:error RoomEnded", { $0.serverErrorForTest("RoomEnded", "room is gone") }, "RoomEnded", nil),
            ("server disconnect", {
                $0.serverErrorForTest("Unauthorized", "adapter key revoked")
                $0.disconnectedForTest(SocketTransport.serverDisconnectReason)
            }, "NotConnected", "adapter key revoked"),
        ]
        for c in cases {
            CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-1","participantId":"me-1"}"#)
            let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
            let reason = Box<String?>()
            let states = Box<TransportState>()
            var handlers = TransportHandlers()
            handlers.onEnded = { reason.append($0) }
            handlers.onState = { states.append($0) }
            let opening = Task { [handlers] in try await transport.open(handlers: handlers) }
            let deadline = TestTiming.deadline(5)
            while !transport.pendingJoinForTest, Date() < deadline { try await Task.sleep(nanoseconds: 10_000_000) }
            XCTAssertTrue(transport.pendingJoinForTest, "\(c.name): open() never reached the join wait")

            let started = Date()
            c.trigger(transport)
            do {
                _ = try await opening.value
                XCTFail("\(c.name): open() should fail")
            } catch let error as AdapterError {
                XCTAssertEqual(error.code, c.code, c.name)
                if let mentions = c.mentions {
                    XCTAssertTrue(error.message.contains(mentions), "\(c.name): \(error.message)")
                }
            }
            XCTAssertLessThan(Date().timeIntervalSince(started), TestTiming.hangGuard, "\(c.name): open() waited for its timeout")
            try await Task.sleep(nanoseconds: 300_000_000)
            XCTAssertEqual(CountingStubURLProtocol.hits, 1, "\(c.name): a failed open scheduled a rejoin")
            XCTAssertTrue(reason.values.isEmpty, "\(c.name): open() reports the failure — no ended")
            XCTAssertFalse(states.values.contains(.reconnecting),
                "\(c.name): a failed open announced reconnecting: \(states.values)")
            await transport.close()
        }
    }

    /// (c) 재개 시도의 새 소켓이 합류 전에 닫히면 루프가 joinTimeout 을 기다리지 않고 백오프로 다시 시도한다.
    func testASessionEndDuringARejoinAttemptRetriesWithoutWaitingForTheTimeout() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
        let reason = Box<String?>()
        var handlers = TransportHandlers()
        handlers.onEnded = { reason.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.handleSessionEnd("host-ended")
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 1 && transport.pendingJoinForTest })

        transport.sessionEndedForTest("host-ended")
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits >= 2 },
            "the rejoin attempt waited for its join timeout")
        XCTAssertTrue(reason.values.isEmpty)
        closeAndWait(transport)
    }

    /// (e) 세션 종료 신호가 여러 번 와도 ended 는 한 번이다.
    func testRepeatedSessionEndSignalsEndOnce() {
        let transport = makeTransport(autoRejoin: false)
        let reason = Box<String?>()
        var handlers = TransportHandlers()
        handlers.onEnded = { reason.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        transport.serverErrorForTest("RoomEnded", "room is gone")
        transport.sessionEndedForTest("host-ended")
        transport.disconnectedForTest(SocketTransport.serverDisconnectReason)

        XCTAssertEqual(reason.values, ["room-ended"])
        closeAndWait(transport)
    }

    /// (d) 실패한 connect() 는 만든 클라이언트를 닫는다 — 호출자는 핸들을 받지 못한다.
    func testAFailedConnectClosesTheClientItCreated() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-1","participantId":"me-1"}"#)
        // connect() 는 공유 URLSession 을 쓴다 — 이 테스트 동안만 전역 스텁을 건다.
        URLProtocol.registerClass(CountingStubURLProtocol.self)
        defer { URLProtocol.unregisterClass(CountingStubURLProtocol.self) }
        let log = Box<LogEntry>()
        var options = ConnectOptions(
            secretKey: "ck_token-1", url: "http://127.0.0.1:\(blackhole.port)", stageId: "s-1")
        options.joinTimeout = 0.3
        options.onLog = { log.append($0) }

        do {
            _ = try await connect(options)
            XCTFail("connect() should fail")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "JoinTimeout")
        }
        XCTAssertEqual(CountingStubURLProtocol.hits, 1, "the validate stub was not used")
        XCTAssertTrue(log.values.contains { $0.code == "ended" && $0.message == "client-closed" },
            "connect() did not close the client it created: \(log.values.map(\.description))")
    }

    // ── 낡은 재검증 · 거부된 새 토큰 (A6) ──────────────────────────────────

    /// (a) 끝난 뒤 돌아온 재검증은 자격증명을 커밋하지 않는다.
    func testARevalidationThatFinishesAfterCloseCommitsNothing() async throws {
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-late","participantId":"me-9"}"#)
        let transport = makeTransport()
        transport.setHandlersForTest(TransportHandlers())
        await transport.close()
        try await transport.validateForTest()
        XCTAssertEqual(transport.authTokenForTest, "", "a revalidation committed credentials on a closed transport")
    }

    /// (a) 먼저 시작해 늦게 끝난 재검증은 그사이 커밋된 새 자격증명을 덮어쓰지 않는다 — 덮어쓰면 다음
    /// 재접속이 다른 참여자로 인증된다.
    func testAnOutdatedRevalidationDoesNotOverwriteNewerCredentials() async throws {
        DelayedStubURLProtocol.configure([
            (delayMs: 400, body: #"{"jwt":"jwt-old","participantId":"me-old"}"#),
            (delayMs: 0, body: #"{"jwt":"jwt-new","participantId":"me-new"}"#),
        ])
        var options = SocketTransportOptions(url: "http://127.0.0.1:9", stageId: "s-1", secretKey: "ck_token-1")
        options.displayName = "Test Bridge"
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [DelayedStubURLProtocol.self]
        let transport = SocketTransport(options: options, session: URLSession(configuration: config))

        let slow = Task { try await transport.validateForTest() }
        // 느린 요청이 먼저 나간 뒤에 둘째를 시작한다 — 고정 100 ms 뒤에 시작하면 느린 러너에서 둘째가 "먼저 시작한" 쪽이 된다.
        await TestTiming.until { DelayedStubURLProtocol.started >= 1 }
        try await transport.validateForTest() // 나중에 시작해 먼저 커밋한다
        XCTAssertEqual(transport.authTokenForTest, "jwt-new")
        try await slow.value
        XCTAssertEqual(transport.authTokenForTest, "jwt-new", "an outdated revalidation overwrote the live JWT")
        await transport.close()
    }

    /// (b) autoRejoin 을 끈 어댑터 — 한 번뿐인 재검증이 받아 온 새 토큰도 소켓이 거부하면 다시 재검증하지
    /// 않고 `reauth-failed` 로 끝낸다(REST 는 키를 받는데 소켓이 계속 거부하면 루프가 됐다).
    func testARefreshedTokenStillRefusedEndsInsteadOfLooping() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway, autoRejoin: false)
        let reason = Box<String?>()
        var handlers = TransportHandlers()
        handlers.onEnded = { reason.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        transport.handleConnectErrorForTest("Unauthorized: token expired")
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 1 && transport.pendingJoinForTest })
        transport.handleConnectErrorForTest("Unauthorized: token expired") // 새 토큰도 거부됐다

        XCTAssertTrue(waitUntil { !reason.values.isEmpty }, "the refused refreshed token did not end the session")
        Thread.sleep(forTimeInterval: 0.3)  // 그 뒤에 다시 재검증하지 않는지 볼 창
        XCTAssertEqual(reason.values, ["reauth-failed"])
        XCTAssertEqual(CountingStubURLProtocol.hits, 1, "revalidated again after the refreshed token was refused")
        closeAndWait(transport)
    }

    // ── 적대 검수 2차 (R1–R4 · 순서 · LOW) ────────────────────────────────

    /// R1: 재합류 직후(루프가 깨어나기 전) 온 stage:ended 가 삼켜지지 않는다 — 새 재개가 실제로 시작된다.
    func testASessionEndRightAfterARejoinStartsANewRejoin() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
        let states = Box<TransportState>()
        var handlers = TransportHandlers()
        handlers.onState = { states.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.handleSessionEnd("host-ended")
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 1 && transport.pendingJoinForTest })

        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.sessionEndedForTest("host-ended") // 한 번 읽기에 온 두 패킷처럼 연달아

        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits >= 2 },
            "the second session end was swallowed: hits=\(CountingStubURLProtocol.hits) states=\(states.values)")
        closeAndWait(transport)
    }

    /// R1b: 재접속 직후의 전송 끊김도 마찬가지.
    func testADropRightAfterAReconnectReconnectsAgain() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
        transport.setHandlersForTest(TransportHandlers())
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.disconnectedForTest("Ping timeout")
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 1 && transport.pendingJoinForTest })

        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.disconnectedForTest("Ping timeout")

        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits >= 2 },
            "the drop right after the reconnect was swallowed")
        closeAndWait(transport)
    }

    /// R3: **실제 라이브러리 경로** — 엔진 메시지 → SocketIOClient.handlePacket → 우리 `.disconnect` 클로저.
    /// 상수 "Got Disconnect" 나 핸들러 배선이 바뀌면 여기서 잡힌다(나머지 A2 테스트는 사유를 직접 주입한다).
    /// 같은 경로로 진짜 stage:joined 가 재접속 루프의 합류 대기를 깨우는지(A1)도 본다.
    func testTheRealLibraryDisconnectPacketIsAServerDisconnect() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway, autoRejoin: false)
        let reason = Box<String?>()
        let states = Box<TransportState>()
        var handlers = TransportHandlers()
        handlers.onEnded = { reason.append($0) }
        handlers.onState = { states.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.disconnectedForTest("Ping timeout") // 전송 끊김 → 재접속 루프가 진짜 소켓을 만든다
        XCTAssertTrue(waitUntil { transport.pendingJoinForTest })
        guard let manager = transport.managerForTest else { return XCTFail("no manager") }
        manager.parseEngineMessage(#"0/stage,{"sid":"abc"}"#)
        manager.parseEngineMessage(#"2/stage,["stage:joined",{"member":{"participantId":"me-1","displayName":"B"}}]"#)
        XCTAssertTrue(waitUntil { states.values.last == .connected }, "a real stage:joined did not join: \(states.values)")
        XCTAssertFalse(transport.pendingJoinForTest, "the loop's join wait was not resumed by the real stage:joined")

        manager.parseEngineMessage("1/stage,") // 서버의 네임스페이스 DISCONNECT 패킷
        XCTAssertTrue(waitUntil { !reason.values.isEmpty })
        XCTAssertEqual(reason.values, ["server-disconnect"])
        closeAndWait(transport)
    }

    /// R4: open() 을 기다리는 태스크를 취소해도 open() 이 돌아오고, 만든 소켓을 닫는다.
    func testCancellingTheOpenReturnsAndClosesWhatItCreated() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-1","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
        let opening = Task { try await transport.open(handlers: TransportHandlers()) }
        let deadline = TestTiming.deadline(5)
        while !transport.pendingJoinForTest, Date() < deadline { try await Task.sleep(nanoseconds: 10_000_000) }
        XCTAssertTrue(transport.pendingJoinForTest)

        opening.cancel()
        // 곧바로 돌아온다 — 취소가 듣지 않으면 합류 시한(60초)까지 매달려 조건 대기(hangGuard)에 걸린다. 종전에는 시한 30초에
        // "3초 안"을 쟀다.
        let finished = Box<Bool>()
        Task {
            _ = try? await opening.value
            finished.append(true)
        }
        let returnedInTime = await TestTiming.until { !finished.values.isEmpty }
        XCTAssertTrue(returnedInTime, "a cancelled open() did not return")
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertFalse(transport.hasSocketForTest, "a cancelled open() left its socket open")
        await transport.close()
    }

    /// 포기할 때는 오류 → ended 상태 → ended 순서다 — `.ended` 에서 루프를 끝내는 앱도 원인을 받는다.
    func testAbandoningReportsTheErrorBeforeTheEnd() {
        CountingStubURLProtocol.configure(status: 401, body: #"{"error":"Unauthorized"}"#)
        let transport = makeTransport()
        let log = Box<String>()
        let ended = expectation(description: "ended")
        var handlers = TransportHandlers()
        handlers.onError = { log.append("error:\($0.code)") }
        handlers.onState = { log.append("state:\($0.rawValue)") }
        handlers.onEnded = {
            log.append("ended:\($0 ?? "nil")")
            ended.fulfill()
        }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.handleSessionEnd("host-ended")
        wait(for: [ended], timeout: TestTiming.hangGuard + 5)
        XCTAssertEqual(Array(log.values.suffix(3)),
            ["error:AuthRevoked", "state:ended", "ended:rejoin-abandoned: AuthRevoked"])
        closeAndWait(transport)
    }

    /// L1: autoRejoin 을 끈 어댑터의 재검증이 **성공한 뒤**의 일시 실패는 보통 재접속처럼 기다린다.
    func testATransientFailureAfterASuccessfulReauthKeepsRetrying() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway, autoRejoin: false)
        let reason = Box<String?>()
        var handlers = TransportHandlers()
        handlers.onEnded = { reason.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.handleConnectErrorForTest("Unauthorized: token expired")
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 1 && transport.pendingJoinForTest })

        CountingStubURLProtocol.configure(status: 429, body: #"{"error":"TooManyRequests"}"#)
        transport.disconnectedForTest("Ping timeout") // 새 토큰으로 붙던 중 전송 끊김
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits >= 2 }, "the reconnect loop stopped")
        Thread.sleep(forTimeInterval: 0.2)
        XCTAssertTrue(reason.values.isEmpty, "a transient failure after a successful reauth ended: \(reason.values)")
        closeAndWait(transport)
    }

    /// L2: 재개 시도의 새 소켓이 합류 전에 닫히면 그 소켓과 합류 폴백을 바로 버린다(폴백이 가짜 connected 를 낼 수 있었다).
    func testASessionEndDuringARejoinAttemptDropsItsSocketAndFallback() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
        transport.setHandlersForTest(TransportHandlers())
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.handleSessionEnd("host-ended")
        XCTAssertTrue(waitUntil { transport.pendingJoinForTest })
        guard let manager = transport.managerForTest else { return XCTFail("no manager") }
        manager.parseEngineMessage(#"0/stage,{"sid":"abc"}"#) // 네임스페이스 접속 → 합류 폴백 예약
        XCTAssertTrue(waitUntil { transport.fallbackPendingForTest }, "no joined fallback was scheduled")

        transport.sessionEndedForTest("host-ended")
        XCTAssertFalse(transport.fallbackPendingForTest, "the joined fallback survived the session end")
        XCTAssertFalse(transport.hasSocketForTest, "the attempt's socket survived the session end")
        closeAndWait(transport)
    }

    /// L4: 합류 전 서버 끊김의 오류는 **이 시도**의 stage:error 만 싣는다.
    func testAPreJoinServerDisconnectCitesOnlyThisAttemptsServerError() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
        let errors = Box<AdapterError>()
        var handlers = TransportHandlers()
        handlers.onError = { errors.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.serverErrorForTest("Unauthorized", "from-the-session") // 세션 중의 오류

        transport.handleSessionEnd("host-ended")
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 1 && transport.pendingJoinForTest })
        transport.disconnectedForTest(SocketTransport.serverDisconnectReason)
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 2 && transport.pendingJoinForTest })
        transport.serverErrorForTest("Unauthorized", "adapter key revoked") // 이 시도의 오류
        transport.disconnectedForTest(SocketTransport.serverDisconnectReason)
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits >= 3 })

        let cuts = errors.values.filter { $0.message.contains("disconnected the socket before joining") }
        guard cuts.count == 2 else {
            return XCTFail("expected two pre-join server-disconnect errors: \(errors.values)")
        }
        XCTAssertFalse(cuts[0].message.contains("from-the-session"), "an earlier error was cited: \(cuts[0].message)")
        XCTAssertTrue(cuts[1].message.contains("adapter key revoked"), cuts[1].message)
        closeAndWait(transport)
    }

    /// L5: 재개 시도 중의 Unauthorized 거부는 오류 이벤트 **한 번**이다.
    func testAnUnauthorizedRefusalDuringARejoinIsReportedOnce() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
        let errors = Box<AdapterError>()
        var handlers = TransportHandlers()
        handlers.onError = { errors.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.handleSessionEnd("host-ended")
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 1 && transport.pendingJoinForTest })

        transport.handleConnectErrorForTest("Unauthorized: jwt expired")
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits >= 2 })
        let refusals = errors.values.filter { $0.message == "Unauthorized: jwt expired" }
        XCTAssertEqual(refusals.count, 1, "\(errors.values)")
        closeAndWait(transport)
    }

    // ── 적대 검수 3차 (타임아웃 경계 · 늦은 취소 · close 한 턴) ─────────────────

    /// T1d: 재개 시도의 stage:joined 가 타임아웃 자식보다 **한 발 먼저** 큐에 오면 합류가 이긴다 — 방금 합류한
    /// 소켓을 버려 소켓도 루프도 없는 connected 로 남거나, 핸들 없는 루프가 계속 돌면 안 된다.
    func testAJoinQueuedJustBeforeTheRejoinTimeoutWins() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        let timeout = 0.6
        var round = 0
        var voided = 0
        while round < 3, voided < 10 {
            CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
            let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: timeout)
            let states = Box<TransportState>()
            var handlers = TransportHandlers()
            handlers.onState = { states.append($0) }
            transport.setHandlersForTest(handlers)
            transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
            let began = Date()  // 합류 대기(와 그 시한)는 이 뒤에 시작한다
            transport.handleSessionEnd("host-ended")
            XCTAssertTrue(waitUntil { transport.pendingJoinForTest })
            // 큐는 대기를 본 직후 길게 막는다 — 합류(confirmJoinedAsync)가 먼저 줄을 서고, 시간 초과는 막힌 사이에 깨어 그 뒤에 선다.
            // 종전에는 시한 0.05 초 전에 0.12 초만 막아, 대기를 본 시각이 CI 부하로 50 ms 넘게 늦으면 시간 초과가 먼저 깨어 합류가
            // 졌다(2026-10-25 CI). 지금은 관측이 0.5 초 늦어도 순서가 같고, 그보다 늦으면 그 판을 버린다(아래 premise).
            usleep(50_000)
            transport.blockQueueForTest(timeout + 0.2) // 타임아웃이 큐가 막힌 사이에 깬다
            transport.confirmJoinedAsyncForTest(Member(participantId: "me-1", displayName: "Bridge")) // 그 앞에 합류
            // 이 판의 전제 — 막기와 합류가 시한이 깨기 전에 줄을 섰다. 시한은 began 뒤에 시작했으니, 지금이 began + 시한보다
            // 이르면 참이다. 멈춘 러너에서 전제가 깨지면 시간 초과가 먼저 줄을 서 합류가 지는데, 그것은 제품의 결함이 아니다.
            let premise = Date().timeIntervalSince(began) < timeout - 0.05
            // 막힌 큐가 풀리고 합류가 처리되기를 기다린다(고정 대기 대신 상태를 본다 — 시간 초과가 이겼다면 그 뒤의 단언이 가린다).
            _ = waitUntil { states.values.last == .connected }
            let validated = CountingStubURLProtocol.hits
            Thread.sleep(forTimeInterval: 0.3)
            guard premise else {
                closeAndWait(transport)
                voided += 1
                XCTAssertLessThan(voided, 10, "the runner was too slow to set this scenario up ten times")
                continue
            }

            XCTAssertEqual(states.values.last, .connected, "round \(round)")
            XCTAssertTrue(transport.hasSocketForTest, "round \(round): the just-joined socket was disposed")
            XCTAssertFalse(transport.loopRunningForTest, "round \(round): a loop is still running")
            XCTAssertEqual(CountingStubURLProtocol.hits, validated, "round \(round): an orphan loop keeps validating")
            XCTAssertEqual(validated, 1, "round \(round)")
            closeAndWait(transport)
            round += 1
        }
    }

    /// T1o: 최초 open 도 같다 — 서버가 확인한 합류를 타임아웃이 SessionEnded 로 뒤집지 않는다.
    func testAJoinQueuedJustBeforeTheOpenTimeoutWins() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        let timeout = 0.6
        var round = 0
        var voided = 0
        while round < 3, voided < 10 {
            CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-1","participantId":"me-1"}"#)
            let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: timeout)
            let client = ConnectClient(transport: transport)
            let began = Date()  // 합류 대기(와 그 시한)는 이 뒤에 시작한다
            let opening = Task { try await client.open() }
            let deadline = TestTiming.deadline(3)
            while !transport.pendingJoinForTest, Date() < deadline { try await Task.sleep(nanoseconds: 100_000) }
            // 큐는 대기를 본 직후 길게 막는다 — 합류(confirmJoinedAsync)가 먼저 줄을 서고, 시간 초과는 막힌 사이에 깨어 그 뒤에 선다.
            // 종전에는 시한 0.05 초 전에 0.12 초만 막아, 대기를 본 시각이 CI 부하로 50 ms 넘게 늦으면 시간 초과가 먼저 깨어 합류가
            // 졌다(2026-10-25 CI). 지금은 관측이 0.5 초 늦어도 순서가 같고, 그보다 늦으면 그 판을 버린다(아래 premise).
            try await Task.sleep(nanoseconds: 50_000_000)
            transport.blockQueueForTest(timeout + 0.2)
            transport.confirmJoinedAsyncForTest(Member(participantId: "me-1", displayName: "B"))
            // 이 판의 전제 — 막기와 합류가 시한이 깨기 전에 줄을 섰다(시한은 began 뒤에 시작했다). 멈춘 러너에서 전제가 깨지면
            // 시간 초과가 먼저 줄을 서 합류가 지는데, 그것은 제품의 결함이 아니다 — 그 판은 버리고 다시 한다.
            let premise = Date().timeIntervalSince(began) < timeout - 0.05
            let outcome = await opening.result
            guard premise else {
                await client.close()
                voided += 1
                XCTAssertLessThan(voided, 10, "the runner was too slow to set this scenario up ten times")
                continue
            }
            if case .failure(let error) = outcome {
                XCTFail("round \(round): a server-confirmed join failed the open: \(error)")
            }
            try await Task.sleep(nanoseconds: 200_000_000)
            let state = await client.state
            XCTAssertEqual(state, .connected, "round \(round)")
            XCTAssertFalse(transport.endedForTest, "round \(round): the transport ended after a confirmed join")
            XCTAssertTrue(transport.hasSocketForTest, "round \(round)")
            await client.close()
            round += 1
        }
    }

    /// 취소가 합류보다 늦게 닿으면(합류가 이미 성립했다) 아무것도 하지 않는다 — 종전에는 open() 이 성공한
    /// 뒤 트랜스포트를 조용히 끝내(onEnded 없음) 클라이언트가 `ended` 를 못 받고 스트림도 끝나지 않았다.
    func testACancellationThatArrivesAfterTheJoinChangesNothing() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-1","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
        let opening = Task { try await transport.open(handlers: TransportHandlers()) }
        let deadline = TestTiming.deadline(5)
        while !transport.pendingJoinForTest, Date() < deadline { try await Task.sleep(nanoseconds: 10_000_000) }
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        _ = try await opening.value

        transport.cancelLastJoinAttemptForTest() // 늦게 닿은 취소
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertFalse(transport.endedForTest, "a late cancellation ended a joined transport")
        XCTAssertTrue(transport.hasSocketForTest, "a late cancellation disposed the joined socket")
        await transport.close()
    }

    /// 취소와 stage:joined 가 겹친다 — 합류가 먼저 확정돼도 취소된 타임아웃 자식이 그룹을 이겨 open() 이 던지면,
    /// 트랜스포트는 합류한 소켓을 쥔 채 살아 있으면 안 된다. 큐를 붙잡아 순서를 고정한다: 취소된 타임아웃
    /// 자식이 먼저 끝나고(큐가 막혀 있다) → 큐가 풀리면 stage:joined → 취소의 깨우기 → open 의 정리.
    func testACancellationRacingTheJoinLeavesNothingBehind() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        for round in 0..<3 {
            CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-1","participantId":"me-1"}"#)
            let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
            let client = ConnectClient(transport: transport)
            let opening = Task { try await client.open() }
            let deadline = TestTiming.deadline(5)
            while !transport.pendingJoinForTest, Date() < deadline { try await Task.sleep(nanoseconds: 5_000_000) }
            XCTAssertTrue(transport.pendingJoinForTest)

            transport.blockQueueForTest(0.3)
            transport.confirmJoinedAsyncForTest(Member(participantId: "me-1", displayName: "Bridge"))
            opening.cancel()
            var opened = false
            do {
                try await opening.value
                opened = true
            } catch {}
            try await Task.sleep(nanoseconds: 150_000_000)

            let state = await client.state
            if opened {
                XCTAssertFalse(transport.endedForTest, "round \(round): open() succeeded but the transport ended")
            } else {
                XCTAssertTrue(transport.endedForTest, "round \(round): open() failed but the transport stayed joined")
                XCTAssertFalse(transport.hasSocketForTest, "round \(round): open() failed but kept its socket")
                XCTAssertNotEqual(state, .connected, "round \(round): open() failed but the client reads connected")
            }
            await client.close()
        }
    }

    /// L3: close() 는 퇴장 알림과 종료를 한 큐 턴에 한다 — 그 사이에 서버의 네임스페이스 끊김(leave 에 대한
    /// 응답)이 끼어 우리 close 를 `server-disconnect` 로 읽으면 안 된다. 큐를 붙잡아 순서를 고정한다:
    /// [막힘] → close() 의 큐 작업 → 서버의 DISCONNECT 패킷.
    func testTheServersReplyToOurLeaveIsNotAServerDisconnect() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway, autoRejoin: false)
        let reason = Box<String?>()
        let states = Box<TransportState>()
        var handlers = TransportHandlers()
        handlers.onEnded = { reason.append($0) }
        handlers.onState = { states.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.disconnectedForTest("Ping timeout") // 재접속 루프가 진짜 소켓을 만든다
        XCTAssertTrue(waitUntil { transport.pendingJoinForTest })
        guard let manager = transport.managerForTest else { return XCTFail("no manager") }
        manager.parseEngineMessage(#"0/stage,{"sid":"abc"}"#)
        manager.parseEngineMessage(#"2/stage,["stage:joined",{"member":{"participantId":"me-1","displayName":"B"}}]"#)
        XCTAssertTrue(waitUntil { states.values.last == .connected })

        // close() 의 큐 작업이 서버의 끊김보다 먼저 줄을 서야 한다 — 막힌 큐는 들여다볼 수 없어 이 간격만은 고정 대기다. 종전의
        // 0.1초(막기 0.3초)는 느린 러너에서 close 태스크가 뜨기 전에 지나갔다. 0.8초를 준다(막기가 먼저 풀려도 무방하다 — 그러면
        // 끊김은 퇴장을 기다리는 사이에 닿고, 그것도 우리 close 의 응답이다).
        transport.blockQueueForTest(1.0)
        let closing = Task { await transport.close() }
        try await Task.sleep(nanoseconds: 800_000_000) // close() 의 큐 작업이 막힌 큐에 줄을 선다
        manager.parseEngineMessage("1/stage,")          // 그 뒤에 서버의 응답 끊김
        await closing.value
        try await Task.sleep(nanoseconds: 200_000_000)
        XCTAssertTrue(reason.values.isEmpty, "our own close was read as \(reason.values)")
    }

    // ── close() 는 서버가 퇴장을 처리할 때까지(상한 1초) 기다린다 ────────────────────────────────────────

    /// 진짜 소켓 · 매니저로 합류한 트랜스포트 — 서버 쪽 패킷은 돌려준 함수(`manager.parseEngineMessage`)로 흘린다.
    private func joinedTransport(_ blackhole: BlockingTCPServer, _ reason: Box<String?>) throws
        -> (SocketTransport, (String) -> Void)
    {
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway, autoRejoin: false)
        let states = Box<TransportState>()
        var handlers = TransportHandlers()
        handlers.onEnded = { reason.append($0) }
        handlers.onState = { states.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.disconnectedForTest("Ping timeout") // 재접속 루프가 진짜 소켓을 만든다
        XCTAssertTrue(waitUntil { transport.pendingJoinForTest })
        guard let manager = transport.managerForTest else {
            XCTFail("no manager")
            throw AdapterError("NotConnected", "no manager")
        }
        manager.parseEngineMessage(#"0/stage,{"sid":"abc"}"#)
        manager.parseEngineMessage(#"2/stage,["stage:joined",{"member":{"participantId":"me-1","displayName":"B"}}]"#)
        XCTAssertTrue(waitUntil { states.values.last == .connected })
        return (transport, { manager.parseEngineMessage($0) })
    }

    /// 서버의 leave ack — 이 소켓의 ack 번호는 0 부터 오른다(몇 번째든 맞도록 앞 몇 개를 다 보낸다).
    private func ackLeave(_ serverSends: (String) -> Void) {
        for id in 0..<4 { serverSends(#"3/stage,\#(id)[{"ok":true}]"#) }
    }

    /// close() 는 서버가 `stage:leave` 를 처리했다고(ack) 알릴 때까지 끊지 않는다 — 보내자마자 끊으면 leave 가 엔진
    /// 버퍼에 남은 채 사라져(실서버 실측 8번 중 1번) 다시 띄운 프로그램이 `이름 (2)` 로 붙었다.
    func testCloseWaitsForTheServerToAckTheLeave() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        let reason = Box<String?>()
        let (transport, serverSends) = try joinedTransport(blackhole, reason)
        // leave 상한을 멀리 밀어 둔다 — ack 를 받고도 상한까지 기다리면 조건 대기(hangGuard)에 걸리고, "ack 전에는 끊지 않는다"가
        // 1초 상한과 겨루지 않는다(종전: 0.3초 쉬는 사이 러너가 0.7초 멈추면 상한이 먼저 끝냈다).
        transport.leaveAckTimeoutForTest = TestTiming.farAway
        let closing = Task { await transport.close() }
        try await leaveWait(transport)
        try await Task.sleep(nanoseconds: 300_000_000)
        XCTAssertFalse(transport.endedForTest, "ack 를 기다리지 않고 끊었다")
        ackLeave(serverSends)
        await finishes(closing, "close after the leave ack")
        XCTAssertTrue(transport.endedForTest)
        XCTAssertTrue(reason.values.isEmpty, "our own close was read as \(reason.values)")
    }

    /// ack 가 오지 않아도 close() 는 상한(1초) 안에 끝난다 — 종료를 붙잡지 않는다.
    func testCloseGivesUpOnALeaveThatIsNeverAcked() async throws {
        XCTAssertEqual(SocketTransport.leaveAckTimeout, 1)
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        let reason = Box<String?>()
        let (transport, _) = try joinedTransport(blackhole, reason)
        let started = Date()
        await finishes(Task { await transport.close() }, "close without a leave ack")  // 결코 매달리지 않는다
        let elapsed = Date().timeIntervalSince(started)
        // 상한(제품 기본값 1초)까지 기다렸다 — 하한만 잰다(느린 러너는 경과를 늘릴 뿐이다). 종전의 상한 2초는 여유가 1초였다.
        XCTAssertGreaterThanOrEqual(elapsed, 0.9, "leave 를 기다리지 않았다")
        XCTAssertTrue(transport.endedForTest)
        XCTAssertFalse(transport.hasSocketForTest)
    }

    /// 퇴장 확인을 기다리는 동안(최대 1초) 소켓은 붙어 있다 — 그 사이의 시그널링은 내보내지 않는다(TS 와 같다). 서버는 시그널링을
    /// 다른 참여자에게 중계한다.
    func testSignalingDuringTheLeaveWaitIsNotEmitted() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        let reason = Box<String?>()
        let (transport, serverSends) = try joinedTransport(blackhole, reason)
        await transport.sendSignaling(["kind": "probe"])
        XCTAssertTrue(waitUntil { transport.signalingEmittedForTest == 1 }, "the live frame was not emitted")
        transport.leaveAckTimeoutForTest = TestTiming.farAway  // 기다림이 시험보다 먼저 끝나지 않게(종전: 1초 상한과 0.1초 대기)
        let closing = Task { await transport.close() }
        try await leaveWait(transport)
        XCTAssertFalse(transport.endedForTest, "the leave wait was over — the test did not exercise it")
        await transport.sendSignaling(["kind": "late"])
        try await Task.sleep(nanoseconds: 50_000_000)
        XCTAssertEqual(transport.signalingEmittedForTest, 1, "a frame was emitted while closing")
        ackLeave(serverSends)
        await finishes(closing, "close after the leave ack")
    }

    /// close() 는 퇴장 확인을 최대 1초 기다린다 — 그 사이 소켓은 붙어 있다. 종전에는 그동안 listMembers() · 등록이 실제로 서버에
    /// 물었다(ack 를 기다리다 닫기가 끝나야 AckTimeout). 닫는 중이면 곧바로 NotConnected 다(TS 와 같다 — 클라이언트가 ended 힌트를
    /// 붙인다).
    func testDuringCloseTheLeaveWaitAsksTheServerNothing() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        let reason = Box<String?>()
        let (transport, serverSends) = try joinedTransport(blackhole, reason)
        transport.leaveAckTimeoutForTest = TestTiming.farAway  // 기다림이 호출들보다 먼저 끝나지 않게(종전: 1초 상한과 0.1초 대기)
        let closing = Task { await transport.close() }
        try await leaveWait(transport)
        XCTAssertFalse(transport.endedForTest, "the leave wait was over before the calls — the test did not exercise it")
        let calls: [(String, () async throws -> Void)] = [
            ("listMembers", { _ = try await transport.listMembers() }),
            ("submit", { _ = try await transport.submitRegistrationCode("123456") }),
            ("resume", { _ = try await transport.resumeRegistration(stageId: "stage-1", deviceSecret: "secret") }),
        ]
        for (name, call) in calls {
            let started = Date()
            do {
                try await call()
                XCTFail("\(name): expected NotConnected")
            } catch let error as AdapterError {
                XCTAssertEqual(error.code, "NotConnected", name)
            }
            // 서버에 물었다면 ack 시한(15초) 뒤에 AckTimeout 이 온다 — 위의 코드 확인과 아래의 대기 중 ack 수가 그것을 가린다.
            // 벽시계 상한은 매달리지 않았음만 본다(종전의 0.3초는 느린 러너에서 넘는다).
            XCTAssertLessThan(Date().timeIntervalSince(started), TestTiming.hangGuard, "\(name) asked the server")
        }
        XCTAssertEqual(transport.pendingAckCountForTest, 0, "an ack is pending — the server was asked")
        ackLeave(serverSends)
        await finishes(closing, "close after the leave ack")
    }

    /// ack 없이 leave 를 처리하고 끊는 서버(공용 가짜 서버가 그렇다) — 그 끊김이 퇴장의 끝이다. 곧바로 끝나고,
    /// `server-disconnect` 로 읽지 않는다.
    func testCloseEndsWhenTheServerDisconnectsAfterTheLeave() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        let reason = Box<String?>()
        let (transport, serverSends) = try joinedTransport(blackhole, reason)
        // leave 상한을 멀리 밀어 둔다 — 끊김이 대기를 풀지 못하면 조건 대기(hangGuard)에 걸린다. 종전에는 1초 상한에 "0.8초 안"을
        // 쟀고(여유 0.6초), 0.2초 쉰 뒤의 끊김이 close 가 퇴장을 보내기 전에 닿으면 진짜 서버 끊김으로 읽혔다.
        transport.leaveAckTimeoutForTest = TestTiming.farAway
        let closing = Task { await transport.close() }
        try await leaveWait(transport)
        serverSends("1/stage,")
        await finishes(closing, "close after the server disconnected")
        try await Task.sleep(nanoseconds: 200_000_000)
        XCTAssertTrue(reason.values.isEmpty, "our own close was read as \(reason.values)")
    }

    /// 동시에 부른 close() 는 먼저 시작한 close 를 함께 기다린다 — 둘째가 곧바로 끊으면 첫째가 기다리던 퇴장이 잘렸다
    /// (2차 검수, 실서버 ack 400ms: 15번 중 10번 유실).
    func testASecondCloseWaitsForTheFirstsLeave() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        let reason = Box<String?>()
        let (transport, serverSends) = try joinedTransport(blackhole, reason)
        transport.leaveAckTimeoutForTest = TestTiming.farAway  // "둘째가 기다린다"가 1초 상한과 겨루지 않게
        let first = Task { await transport.close() }
        try await leaveWait(transport)
        let secondDone = Box<Bool>()
        let second = Task {
            await transport.close()
            secondDone.append(true)
        }
        try await Task.sleep(nanoseconds: 300_000_000)
        XCTAssertFalse(transport.endedForTest, "둘째 close 가 첫째의 퇴장을 끊었다")
        XCTAssertTrue(secondDone.values.isEmpty, "둘째 close 가 첫째를 기다리지 않았다")
        ackLeave(serverSends)
        await finishes(first, "the first close")
        await finishes(second, "the second close")
        XCTAssertTrue(transport.endedForTest)
        XCTAssertTrue(reason.values.isEmpty, "our own close was read as \(reason.values)")
    }

    /// 기다리는 사이 다른 길이 소켓을 버려도(라이브러리의 ack 타임아웃은 그 뒤 발화하지 않는다) close() 는 상한 안에
    /// 끝난다 — 따로 건 폴백 타이머가 그 몫이다.
    func testCloseReturnsEvenIfTheSocketIsDisposedMidWait() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        let reason = Box<String?>()
        let (transport, _) = try joinedTransport(blackhole, reason)
        let done = Box<Bool>()
        Task {
            await transport.close()
            done.append(true)
        }
        try await leaveWait(transport)  // 기다리기 시작한 뒤에 소켓을 버린다(고정 0.2초 대신)
        transport.disposeSocketForTest()
        // 폴백 타이머(제품 기본 상한 1초)가 끝낸다 — 매달리지 않는지만 본다. 종전의 상한 1.5초는 여유가 0.45초였다.
        let deadline = TestTiming.deadline()
        while done.values.isEmpty, Date() < deadline { try await Task.sleep(nanoseconds: 10_000_000) }
        XCTAssertFalse(done.values.isEmpty, "close() 가 매달렸다")
    }

    /// 퇴장을 기다리는 사이 온 `stage:ended` 는 이 close 가 끝낸다 — 그 사유로 세션을 끝내거나 재개 루프를 띄우지 않는다.
    func testAStageEndedDuringTheLeaveWaitDoesNotEndTheSessionItself() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        let reason = Box<String?>()
        let (transport, serverSends) = try joinedTransport(blackhole, reason)
        transport.leaveAckTimeoutForTest = TestTiming.farAway  // stage:ended 가 기다리는 사이에 닿게(종전: 1초 상한과 0.2초 대기)
        let closing = Task { await transport.close() }
        try await leaveWait(transport)
        serverSends(#"2/stage,["stage:ended",{"reason":"host-ended"}]"#)
        try await Task.sleep(nanoseconds: 100_000_000)
        ackLeave(serverSends)
        await finishes(closing, "close after the leave ack")
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertTrue(reason.values.isEmpty, "the leave wait ended the session as \(reason.values)")
        XCTAssertFalse(transport.loopRunningForTest)
    }

    /// 진짜 소켓 트랜스포트 위에서 합류한 클라이언트 — 서버 쪽 패킷은 돌려준 함수로 흘린다.
    private func joinedClient(_ blackhole: BlockingTCPServer) async throws
        -> (ConnectClient, (String) -> Void, SocketTransport)
    {
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway, autoRejoin: false)
        let client = ConnectClient(transport: transport)
        let opening = Task { try await client.open() }
        XCTAssertTrue(waitUntil { transport.managerForTest != nil })
        guard let manager = transport.managerForTest else {
            XCTFail("no manager")
            throw AdapterError("NotConnected", "no manager")
        }
        manager.parseEngineMessage(#"0/stage,{"sid":"abc"}"#)
        manager.parseEngineMessage(#"2/stage,["stage:joined",{"member":{"participantId":"me-1","displayName":"B"}}]"#)
        try await opening.value
        return (client, { manager.parseEngineMessage($0) }, transport)
    }

    /// 등록 두 개가 겹치고 클라이언트 액터가 바빠도 등록 id 는 ack 순서대로만 바뀐다 — 쓰는 쪽이 둘이면(트랜스포트의 ack
    /// 콜백과 액터의 결과 기록) 액터가 풀린 뒤 앞 등록의 결과를 늦게 써 값이 r1 → r0 → r1 로 되돌았고, 그 창에서 r1 통지가
    /// 걸러졌다(3차 검수). 별도 스레드가 값의 전이를 기록한다 — 되돌아가는 창은 짧아서 20번 겹쳐 본다.
    func testOverlappingRegistrationsNeverRegressTheRegistrationId() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        let (client, serverSends, transport) = try await joinedClient(blackhole)
        let transitions = Box<String>()
        let stop = Box<Bool>()
        let poller = Thread {
            var last: String? = "<start>"
            while stop.values.isEmpty {
                let now = client.currentRegistrationId
                if now != last {
                    transitions.append(now ?? "nil")
                    last = now
                }
            }
        }
        poller.start()
        let entered = Box<Int>()
        let release = DispatchSemaphore(value: 0)
        await client.on("gate") { message in
            if case .int(let round) = message.value { entered.append(round) }
            _ = release.wait(timeout: .now() + TestTiming.farAway)
        }
        let rounds = 20
        for round in 0..<rounds {
            // 요청이 나간(ack 를 기다리는) 것을 본 뒤에 다음으로 간다 — 고정 20 ms 뒤에 가면 느린 러너에서 A · B 의 순서가 바뀌거나
            // 요청보다 답이 먼저 가(그 답은 버려진다) 등록이 ack 시한까지 매달린다.
            let a = Task { try await client.submitRegistrationCode("A") }
            XCTAssertTrue(waitUntil { transport.pendingAckCountForTest == 1 })
            let b = Task { try await client.submitRegistrationCode("B") }
            XCTAssertTrue(waitUntil { transport.pendingAckCountForTest == 2 })
            // 클라이언트 액터를 붙잡은 채 두 ack 가 차례로 온다 — 두 등록의 액터 쪽 뒷부분은 풀린 뒤에 돈다.
            serverSends(
                #"2/stage,["stage:message",{"channel":"gate","value":\#(round),"sourceParticipantId":"p","timestamp":\#(round + 1)}]"#)
            XCTAssertTrue(waitUntil { entered.values.last == round })
            let first = 2 * round
            for id in [first, first + 1] { serverSends(#"3/stage,\#(id)[{"registrationId":"r\#(id)"}]"#) }
            XCTAssertTrue(waitUntil { client.currentRegistrationId == "r\(first + 1)" })
            release.signal()
            let ra = try await a.value
            let rb = try await b.value
            XCTAssertEqual([ra.registrationId, rb.registrationId], ["r\(first)", "r\(first + 1)"])
        }
        try await Task.sleep(nanoseconds: 50_000_000)
        stop.append(true)
        let ids = transitions.values.dropFirst().compactMap { Int($0.dropFirst()) } // "nil" 다음부터 r<n>
        XCTAssertEqual(transitions.values.first, "nil")
        XCTAssertEqual(ids, ids.sorted(), "등록 id 가 되돌았다: \(transitions.values)")
        XCTAssertEqual(Set(ids).count, ids.count, "등록 id 가 되돌았다: \(transitions.values)")
        let last = await client.lastRegistrationId
        XCTAssertEqual(last, "r\(2 * rounds - 1)")
        await client.close()
    }

    /// close() 가 퇴장을 기다리는 사이 온 합류(재개 시도의 stage:joined)는 connected 를 내지 않는다 — 내면 클라이언트가
    /// leave 뒤에 큐를 흘렸다(3차 검수).
    func testAJoinDuringTheLeaveWaitDoesNotReportConnected() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway, autoRejoin: false)
        let states = Box<TransportState>()
        var handlers = TransportHandlers()
        handlers.onState = { states.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.disconnectedForTest("Ping timeout") // 재접속 루프가 진짜 소켓을 만든다
        XCTAssertTrue(waitUntil { transport.pendingJoinForTest })
        guard let manager = transport.managerForTest else { return XCTFail("no manager") }
        manager.parseEngineMessage(#"0/stage,{"sid":"abc"}"#) // 붙었지만 아직 stage:joined 전
        transport.leaveAckTimeoutForTest = TestTiming.farAway  // 합류가 기다리는 사이에 닿게(종전: 1초 상한과 0.2초 대기)
        let closing = Task { await transport.close() }
        try await leaveWait(transport)
        let mark = states.values.count
        manager.parseEngineMessage(#"2/stage,["stage:joined",{"member":{"participantId":"me-1","displayName":"B"}}]"#)
        try await Task.sleep(nanoseconds: 100_000_000)
        let afterJoin = Array(states.values.dropFirst(mark))
        ackLeave { manager.parseEngineMessage($0) }
        await finishes(closing, "close after the leave ack")
        XCTAssertFalse(afterJoin.contains(.connected), "퇴장 중의 합류가 connected 를 냈다: \(afterJoin)")
    }

    /// 등록 ack 바로 뒤에 오는 새 등록의 첫 보유자 통지는 새 등록 id 로 판정된다 — 사본을 클라이언트 액터에서만 고치면
    /// 액터가 바쁜 동안 같은 소켓 큐의 다음 패킷(그 통지)이 옛 사본으로 걸러져, 다음 renew 까지 게이트가 닫혔다(2차 검수 5/5).
    func testTheRegistrationMirrorIsUpdatedBeforeTheNextPacket() async throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway, autoRejoin: false)
        let client = ConnectClient(transport: transport)
        let opening = Task { try await client.open() }
        XCTAssertTrue(waitUntil { transport.managerForTest != nil })
        guard let manager = transport.managerForTest else { return XCTFail("no manager") }
        manager.parseEngineMessage(#"0/stage,{"sid":"abc"}"#)
        manager.parseEngineMessage(#"2/stage,["stage:joined",{"member":{"participantId":"me-1","displayName":"B"}}]"#)
        try await opening.value
        func ackRegistration(_ id: String) {
            for n in 0..<4 { manager.parseEngineMessage(#"3/stage,\#(n)[{"registrationId":"\#(id)"}]"#) }
        }
        let first = Task { try await client.submitRegistrationCode("A") }
        XCTAssertTrue(waitUntil { transport.pendingAckCountForTest == 1 })  // 요청이 나갔다(고정 0.1초 대신)
        ackRegistration("r1")
        _ = try await first.value

        let seen = Box<String?>()
        _ = client.onRemoteInputHolder { _ in seen.append(client.currentRegistrationId) }
        let second = Task { try await client.submitRegistrationCode("B") }
        XCTAssertTrue(waitUntil { transport.pendingAckCountForTest == 1 }) // 요청이 나갔다
        // 클라이언트 액터를 채널 콜백으로 붙잡는다 — 등록의 뒷부분(액터)이 그 통지보다 먼저 돌 수 없게.
        let entered = Box<Bool>()
        let release = DispatchSemaphore(value: 0)
        await client.on("gate") { _ in
            entered.append(true)
            _ = release.wait(timeout: .now() + TestTiming.farAway)
        }
        manager.parseEngineMessage(
            #"2/stage,["stage:message",{"channel":"gate","value":1,"sourceParticipantId":"p","timestamp":1}]"#)
        XCTAssertTrue(waitUntil { !entered.values.isEmpty })
        ackRegistration("r2") // ack 와 곧바로 이어지는 새 등록의 부여 — 같은 소켓 큐
        manager.parseEngineMessage(
            #"2/stage,["adapter:lease-holder",{"registrationId":"r2","holderParticipantId":"new-holder","ttlMs":30000}]"#)
        XCTAssertTrue(waitUntil { !seen.values.isEmpty })
        XCTAssertEqual(seen.values, ["r2"], "새 등록의 첫 통지가 옛 등록 id 로 판정됐다")
        release.signal()
        let result = try await second.value
        XCTAssertEqual(result.registrationId, "r2")
        await client.close()
    }

    /// A7: 세션 재개를 기다리는 동안 상태는 줄곧 `reconnecting` 이다 — 재개 시도가 `connecting` 을 알리지 않는다
    /// (최초 open 만 알린다). connected → stage:ended → 시도 #1 합류 전 실패 → 시도 #2 합류.
    func testTheWholeResumeWaitIsReconnecting() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway)
        let states = Box<TransportState>()
        var handlers = TransportHandlers()
        handlers.onState = { states.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        transport.handleSessionEnd("host-ended")
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 1 && transport.pendingJoinForTest })
        transport.sessionEndedForTest("host-ended") // 시도 #1 이 합류 전에 실패한다
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 2 && transport.pendingJoinForTest })
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge")) // 시도 #2 합류

        XCTAssertEqual(states.values, [.connected, .reconnecting, .connected])
        closeAndWait(transport)
    }

    /// `reconnection: false` 는 전송 끊김 재접속만 끈다 — 세션 재개는 그대로 돈다(stage:ended → 다시 붙는다).
    func testSessionResumeRunsWithReconnectionOff() throws {
        let blackhole = try BlockingTCPServer()
        defer { blackhole.stop() }
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(
            url: "http://127.0.0.1:\(blackhole.port)", joinTimeout: TestTiming.farAway, autoRejoin: true, reconnection: false)
        let states = Box<TransportState>()
        var handlers = TransportHandlers()
        handlers.onState = { states.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        transport.sessionEndedForTest("host-ended")
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 1 && transport.pendingJoinForTest },
            "reconnection=false also switched off the session resume")
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        XCTAssertEqual(states.values, [.connected, .reconnecting, .connected])
        closeAndWait(transport)
    }

    /// 같은 조건에서 회수된 키는 끝난다(서버 끊김 → 재검증 401 → AuthRevoked).
    func testARevokedKeyEndsWithReconnectionOff() {
        CountingStubURLProtocol.configure(status: 401, body: #"{"error":"Unauthorized"}"#)
        let transport = makeTransport(autoRejoin: true, reconnection: false)
        let reason = Box<String?>()
        let ended = expectation(description: "ended")
        var handlers = TransportHandlers()
        handlers.onEnded = {
            reason.append($0)
            ended.fulfill()
        }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        transport.disconnectedForTest(SocketTransport.serverDisconnectReason)

        wait(for: [ended], timeout: TestTiming.hangGuard + 5)
        XCTAssertEqual(reason.values, ["rejoin-abandoned: AuthRevoked"])
        closeAndWait(transport)
    }

    /// 그리고 전송 끊김은 `reconnection: false` 면 여전히 다시 붙지 않는다.
    func testAPlainDropDoesNotReconnectWithReconnectionOff() {
        CountingStubURLProtocol.configure(status: 200, body: #"{"jwt":"jwt-2","participantId":"me-1"}"#)
        let transport = makeTransport(autoRejoin: true, reconnection: false)
        let states = Box<TransportState>()
        var handlers = TransportHandlers()
        handlers.onState = { states.append($0) }
        transport.setHandlersForTest(handlers)
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))

        transport.disconnectedForTest("Ping timeout")
        Thread.sleep(forTimeInterval: 0.3)
        XCTAssertEqual(CountingStubURLProtocol.hits, 0, "a transport drop reconnected although reconnection=false")
        XCTAssertFalse(transport.loopRunningForTest)
        XCTAssertEqual(states.values.last, .reconnecting)
        closeAndWait(transport)
    }

    /// 백오프 잠 도중의 close() 는 재검증을 하나 더 보내지 않는다 — 취소된 잠은 곧바로 깨어나 종전에는
    /// 그대로 validate 로 갔다(이 시험 3/3 재현). 그 요청은 close() 가 돌아온 뒤에 도착해, 다음 시험이
    /// `configure` 로 요청 수를 비운 뒤에 잡히면 그 시험의 `hits == 1` 이 영영 참이 되지 않았다
    /// (CI: `…RetriesWithoutWaitingForTheTimeout` · `…RightAfterARejoinStartsANewRejoin` 이 연달아 5초 시한 실패).
    func testACloseDuringTheBackoffSendsNoFurtherRevalidation() throws {
        CountingStubURLProtocol.configure(status: 404, body: #"{"error":"NotFound"}"#)
        var options = SocketTransportOptions(url: "http://127.0.0.1:9", stageId: "s-1", secretKey: "ck_token-1")
        options.rejoinDelay = 0.3 // 두 번째 시도 전의 잠이 길어야 그 한가운데서 닫을 수 있다
        options.rejoinDelayMax = 0.3
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [CountingStubURLProtocol.self]
        let transport = SocketTransport(options: options, session: URLSession(configuration: config))
        transport.setHandlersForTest(TransportHandlers())
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.handleSessionEnd("host-ended")
        XCTAssertTrue(waitUntil { CountingStubURLProtocol.hits == 1 }) // 404 = RoomEnded → 기다린다
        Thread.sleep(forTimeInterval: 0.1) // 다음 시도 전의 잠 한가운데

        closeAndWait(transport)
        let atClose = CountingStubURLProtocol.hits
        Thread.sleep(forTimeInterval: 0.5)
        XCTAssertEqual(CountingStubURLProtocol.hits, atClose,
            "a revalidation went out after close: \(atClose) → \(CountingStubURLProtocol.hits)")
        XCTAssertFalse(transport.loopRunningForTest)
    }

    // 백오프 확인을 통과한 **직후**에 닫힌 경우(CI 2026-11-04, 느린 러너) — 위 시험은 잠 한가운데만 잡아 이 틈을 우연에
    // 맡겼다. 훅으로 그 순간을 붙잡아 닫는다: close() 가 끝난 뒤에도, 그 뒤 0.5초 동안에도 재검증은 처음 한 번뿐이어야 한다.
    func testACloseRightAfterTheBackoffCheckSendsNoRevalidation() throws {
        CountingStubURLProtocol.configure(status: 404, body: #"{"error":"NotFound"}"#)
        var options = SocketTransportOptions(url: "http://127.0.0.1:9", stageId: "s-1", secretKey: "ck_token-1")
        options.rejoinDelay = 0.05
        options.rejoinDelayMax = 0.05
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [CountingStubURLProtocol.self]
        let transport = SocketTransport(options: options, session: URLSession(configuration: config))
        let reached = DispatchSemaphore(value: 0)
        let resume = DispatchSemaphore(value: 0)
        let passes = Box<Int>()
        transport.afterBackoffCheckForTest = {
            passes.append(1)
            guard passes.values.count == 2 else { return } // 첫 재검증(404) 뒤의 두 번째 확인
            reached.signal()
            resume.wait()
        }
        transport.setHandlersForTest(TransportHandlers())
        transport.confirmJoinedForTest(Member(participantId: "me-1", displayName: "Bridge"))
        transport.handleSessionEnd("host-ended")
        XCTAssertEqual(reached.wait(timeout: .now() + TestTiming.hangGuard), .success)
        XCTAssertEqual(CountingStubURLProtocol.hits, 1)

        let closed = expectation(description: "closed")
        Task {
            await transport.close()
            closed.fulfill()
        }
        // close() 가 루프를 취소할 때까지 기다렸다가 루프를 풀어 준다 — 루프는 확인을 이미 통과한 상태다.
        XCTAssertTrue(waitUntil { transport.endedForTest })
        resume.signal()
        wait(for: [closed], timeout: TestTiming.hangGuard + 5)
        XCTAssertEqual(CountingStubURLProtocol.hits, 1, "a revalidation went out after the backoff check")
        Thread.sleep(forTimeInterval: 0.5)
        XCTAssertEqual(CountingStubURLProtocol.hits, 1, "a revalidation went out after close")
        XCTAssertFalse(transport.loopRunningForTest)
    }

    /// 조건이 참이 될 때까지 폴링한다(동기 테스트용) — 시한은 TestTiming.hangGuard 이상이다. 참이 되면 곧바로 돌아온다.
    private func waitUntil(timeout: TimeInterval = 5, _ condition: () -> Bool) -> Bool {
        let deadline = TestTiming.deadline(timeout)
        while !condition() && Date() < deadline { usleep(10_000) }
        return condition()
    }

    /// close() 가 퇴장을 보내고 그 처리를 기다리기 시작할 때까지 — "기다리는 사이"의 일을 고정 대기 없이 그 안에 넣는다.
    private func leaveWait(_ transport: SocketTransport) async throws {
        let waiting = await TestTiming.until { transport.leaveWaitingForTest }
        XCTAssertTrue(waiting, "close() never reached the leave wait")
    }

    /// 그 작업이 TestTiming.hangGuard 안에 끝난다 — 매달리면 실패로 보고 돌아온다(시험이 멈추지 않는다).
    private func finishes(_ task: Task<Void, Never>, _ what: String, file: StaticString = #filePath, line: UInt = #line) async {
        let done = Box<Bool>()
        Task {
            await task.value
            done.append(true)
        }
        let finished = await TestTiming.until { !done.values.isEmpty }
        XCTAssertTrue(finished, "\(what) did not finish within \(Int(TestTiming.hangGuard)) s", file: file, line: line)
    }

    /// 루프를 남기지 않는다 — 테스트가 끝난 뒤에도 스텁을 계속 두드리면 다음
    /// 스위트의 시간에 얹힌다.
    private func closeAndWait(_ transport: SocketTransport) {
        let closed = expectation(description: "closed")
        Task {
            await transport.close()
            closed.fulfill()
        }
        wait(for: [closed], timeout: TestTiming.hangGuard + 5)
    }

    private func makeTransport(
        url: String = "http://127.0.0.1:9",
        joinTimeout: TimeInterval = 2,
        autoRejoin: Bool = true,
        reconnection: Bool = true,
        ackTimeout: TimeInterval? = nil
    ) -> SocketTransport {
        var options = SocketTransportOptions(
            url: url, stageId: "s-1", secretKey: "ck_token-1")
        options.displayName = "Test Bridge"
        options.autoRejoin = autoRejoin
        options.reconnection = reconnection
        options.joinTimeout = joinTimeout
        if let ackTimeout { options.ackTimeout = ackTimeout }
        // 테스트는 실시간으로 기다린다 — 재개 간격을 최소로 줄인다. 재개는 **재개 간격**을
        // 쓴다(소켓 재접속 간격이 아니다).
        options.rejoinDelay = 0.01
        options.rejoinDelayMax = 0.02
        options.reconnectDelay = 0.01
        options.reconnectDelayMax = 0.02

        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [CountingStubURLProtocol.self]
        return SocketTransport(options: options, session: URLSession(configuration: config))
    }

    /// 콜백이 어느 스레드에서 오든 모아 담는 작은 상자.
    private final class Box<T>: @unchecked Sendable {
        private let lock = NSLock()
        private var storage: [T] = []
        var values: [T] {
            lock.lock()
            defer { lock.unlock() }
            return storage
        }
        func append(_ value: T) {
            lock.lock()
            storage.append(value)
            lock.unlock()
        }
    }
}

/// validate 응답을 고정한 스텁 — 요청 수를 센다.
///
/// `ProtocolDriftTests` 의 `StubURLProtocol` 과 별개다: 재시도 횟수를 세야 하고,
/// 그 스텁을 고치면 다른 스위트의 기대가 흔들린다.
final class CountingStubURLProtocol: URLProtocol {
    private static let lock = NSLock()
    private static var status = 200
    private static var body = Data()
    private static var callCount = 0
    private static var firstAnswer: (status: Int, body: Data)?
    private static var arrivals: [Date] = []

    /// 요청이 닿은 시각들(닿은 순서) — 간격을 시험 스레드의 관측이 아니라 도착으로 잰다.
    static var hitTimes: [Date] {
        lock.lock()
        defer { lock.unlock() }
        return arrivals
    }

    /// 다음 요청 하나에만 이 답을 준다(그 뒤는 configure 로 정한 답) — configure 뒤에 부른다.
    static func answerFirst(status: Int, body: String) {
        lock.lock()
        firstAnswer = (status, Data(body.utf8))
        lock.unlock()
    }

    static var hits: Int {
        lock.lock()
        defer { lock.unlock() }
        return callCount
    }

    static func configure(status: Int, body: String) {
        lock.lock()
        self.status = status
        self.body = Data(body.utf8)
        callCount = 0
        firstAnswer = nil
        arrivals = []
        lock.unlock()
    }

    static func reset() { configure(status: 200, body: "") }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        CountingStubURLProtocol.lock.lock()
        CountingStubURLProtocol.callCount += 1
        CountingStubURLProtocol.arrivals.append(Date())
        let once = CountingStubURLProtocol.firstAnswer
        CountingStubURLProtocol.firstAnswer = nil
        let status = once?.status ?? CountingStubURLProtocol.status
        let body = once?.body ?? CountingStubURLProtocol.body
        CountingStubURLProtocol.lock.unlock()

        let response = HTTPURLResponse(
            url: request.url!, statusCode: status, httpVersion: "HTTP/1.1",
            headerFields: ["Content-Type": "application/json"])!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: body)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}

/// 요청 도착 순서대로 정해 둔 지연 뒤에 200 을 돌려주는 스텁 — 재검증 둘을 겹치게 만든다.
final class DelayedStubURLProtocol: URLProtocol {
    private static let lock = NSLock()
    private static var script: [(delayMs: Int, body: String)] = []

    private static var startedCount = 0
    /// 지금까지 받은 요청 수.
    static var started: Int { lock.lock(); defer { lock.unlock() }; return startedCount }

    static func configure(_ responses: [(delayMs: Int, body: String)]) {
        lock.lock(); script = responses; startedCount = 0; lock.unlock()
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        DelayedStubURLProtocol.lock.lock()
        DelayedStubURLProtocol.startedCount += 1
        let next = DelayedStubURLProtocol.script.isEmpty
            ? (delayMs: 0, body: "{}") : DelayedStubURLProtocol.script.removeFirst()
        DelayedStubURLProtocol.lock.unlock()
        DispatchQueue.global().asyncAfter(deadline: .now() + .milliseconds(next.delayMs)) { [self] in
            let response = HTTPURLResponse(
                url: request.url!, statusCode: 200, httpVersion: "HTTP/1.1",
                headerFields: ["Content-Type": "application/json"])!
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: Data(next.body.utf8))
            client?.urlProtocolDidFinishLoading(self)
        }
    }

    override func stopLoading() {}
}

/// 게스트 쿠키를 흉내 내는 validate — 쿠키가 없으면 새 participantId 와 쿠키를, 있으면 그 쿠키의 id 를 준다(Connect 와 같다).
final class CookieStubURLProtocol: URLProtocol {
    private static let lock = NSLock()
    private static var seen: [String] = []
    private static var next = 0

    static var cookies: [String] {
        lock.lock()
        defer { lock.unlock() }
        return seen
    }

    static func reset() {
        lock.lock()
        seen = []
        next = 0
        lock.unlock()
    }

    /// 켜면 쿠키를 받은 응답이 새 쿠키(`…-r2`)로 재발급한다.
    nonisolated(unsafe) static var rotate = false

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let cookie = request.value(forHTTPHeaderField: "Cookie")
        CookieStubURLProtocol.lock.lock()
        CookieStubURLProtocol.seen.append(cookie ?? "-")
        let pid: String
        if let cookie, let range = cookie.range(of: "guest-") {
            pid = String(cookie[range.upperBound...])
        } else {
            CookieStubURLProtocol.next += 1
            pid = "p-\(CookieStubURLProtocol.next)"
        }
        CookieStubURLProtocol.lock.unlock()
        let body = Data(#"{"jwt":"jwt-\#(pid)","participantId":"\#(pid)"}"#.utf8)
        let issued = (CookieStubURLProtocol.rotate && cookie != nil && !pid.hasSuffix("-r2")) ? "\(pid)-r2" : pid
        let response = HTTPURLResponse(
            url: request.url!, statusCode: 200, httpVersion: "HTTP/1.1",
            headerFields: [
                "Content-Type": "application/json",
                "Set-Cookie": "pp_stage_guest_s-1=guest-\(issued); Path=/; HttpOnly",
            ])!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: body)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}

/// 진짜 HTTP 로 답하는 validate 서버(NWListener) — URLSession 의 기본 HTTP 경로(쿠키 저장소 포함)를 그대로 탄다. Connect 처럼
/// 쿠키가 없으면 새 participantId 와 쿠키를, 있으면 그 쿠키의 id 를 준다.
final class TinyValidateServer: @unchecked Sendable {
    private let listener: NWListener
    private let lock = NSLock()
    private var seen: [String] = []
    private var next = 0
    private(set) var port: UInt16 = 0

    var cookies: [String] { lock.withLock { seen } }

    init() throws {
        listener = try NWListener(using: .tcp, on: .any)
        listener.newConnectionHandler = { [weak self] connection in
            connection.start(queue: .global())
            self?.receive(connection, Data())
        }
        let ready = DispatchSemaphore(value: 0)
        listener.stateUpdateHandler = { if case .ready = $0 { ready.signal() } }
        listener.start(queue: .global())
        _ = ready.wait(timeout: .now() + TestTiming.hangGuard)
        port = listener.port?.rawValue ?? 0
    }

    func stop() { listener.cancel() }

    private func receive(_ connection: NWConnection, _ buffer: Data) {
        connection.receive(minimumIncompleteLength: 1, maximumLength: 65_536) { [weak self] data, _, done, error in
            guard let self else { return }
            var buffer = buffer
            if let data { buffer.append(data) }
            if let end = buffer.range(of: Data("\r\n\r\n".utf8)) {
                let head = String(decoding: buffer[..<end.lowerBound], as: UTF8.self)
                let lines = head.components(separatedBy: "\r\n")
                func header(_ name: String) -> String? {
                    lines.first { $0.lowercased().hasPrefix(name.lowercased() + ":") }
                        .map { String($0.dropFirst(name.count + 1)).trimmingCharacters(in: .whitespaces) }
                }
                let length = Int(header("Content-Length") ?? "0") ?? 0
                if buffer.count - end.upperBound >= length {
                    self.respond(connection, cookie: header("Cookie"))
                    return
                }
            }
            if done || error != nil {
                connection.cancel()
                return
            }
            self.receive(connection, buffer)
        }
    }

    private func respond(_ connection: NWConnection, cookie: String?) {
        let pid: String = lock.withLock {
            seen.append(cookie ?? "-")
            if let cookie, let range = cookie.range(of: "guest-") { return String(cookie[range.upperBound...]) }
            next += 1
            return "p-\(next)"
        }
        let body = #"{"jwt":"jwt-\#(pid)","participantId":"\#(pid)"}"#
        let response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            + "Set-Cookie: pp_stage_guest_s-1=guest-\(pid); Path=/\r\n"
            + "Content-Length: \(body.utf8.count)\r\nConnection: close\r\n\r\n\(body)"
        connection.send(content: Data(response.utf8), completion: .contentProcessed { _ in connection.cancel() })
    }
}
