import XCTest
@testable import OmniAdapter

/// `fetchIceServers` — ICE 배포 설정 조회 (TS `client.test.ts` 의 fetchIceServers
/// describe 미러). 조회는 JWT 를 쥔 **트랜스포트**만 한다 — fake 트랜스포트가 결과를 준다.
final class IceServersTests: XCTestCase {

    /// join 만 통과시키고 ICE 결과를 주입받는 최소 fake.
    private final class IceTransport: Transport, @unchecked Sendable {
        private let lock = NSLock()
        private var replies: [IceServersResult]
        private(set) var calls = 0
        init(_ replies: [IceServersResult]) { self.replies = replies }
        func open(handlers: TransportHandlers) async throws -> Joined {
            Joined(roomId: "stage-1", me: Member(participantId: "me-1", displayName: "Me"))
        }
        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult {
            PublishResult(ok: true)
        }
        func listMembers() async throws -> [Member] { [] }
        func close() async {}
        func fetchIceServers() async -> IceServersResult {
            lock.withLock {
                calls += 1
                return replies.isEmpty ? IceServersResult(iceServers: [], expiresAt: nil) : replies.removeFirst()
            }
        }
    }

    private static let iceExpected = [IceServer(urls: ["stun:stun.example:3478"])]

    func testReturnsIceServersAndCachesSuccess() async throws {
        let transport = IceTransport([IceServersResult(iceServers: IceServersTests.iceExpected, expiresAt: nil)])
        let client = ConnectClient(transport: transport)

        let first = await client.fetchIceServers()
        let second = await client.fetchIceServers()
        XCTAssertEqual(first, IceServersTests.iceExpected)
        XCTAssertEqual(second, IceServersTests.iceExpected)
        // 캐시 — 두 번째 호출은 트랜스포트를 다시 부르지 않는다.
        XCTAssertEqual(transport.calls, 1)
    }

    func testFailureReturnsEmptyWithoutCachingSoNextCallRetries() async throws {
        // 첫 조회가 비면(실패 = 빈 결과) 캐시하지 않는다 — 다음 호출이 재시도한다.
        let transport = IceTransport([
            IceServersResult(iceServers: [], expiresAt: nil),
            IceServersResult(iceServers: IceServersTests.iceExpected, expiresAt: nil),
        ])
        let client = ConnectClient(transport: transport)
        let first = await client.fetchIceServers()
        XCTAssertEqual(first, [])
        let second = await client.fetchIceServers()
        XCTAssertEqual(second, IceServersTests.iceExpected)
        XCTAssertEqual(transport.calls, 2)
    }

    func testTransportWithoutIceMeansEmpty() async throws {
        // Engine 프로파일·목처럼 ICE 조회가 없는 트랜스포트 — 서버 직접 접근 없이 빈 배열.
        let client = ConnectClient(transport: IceTransport([]))
        let servers = await client.fetchIceServers()
        XCTAssertEqual(servers, [])
    }

    func testExpiringCredentialsAreRefetched() async throws {
        // TURN 자격증명은 만료된다 — 만료 임박이면 캐시를 버리고 다시 받는다.
        let soon = Int64(Date().timeIntervalSince1970 * 1000) + 30_000
        let later = Int64(Date().timeIntervalSince1970 * 1000) + 3_600_000
        let transport = IceTransport([
            IceServersResult(iceServers: [IceServer(urls: ["turn:1"], username: "u1", credential: "c")], expiresAt: soon),
            IceServersResult(iceServers: [IceServer(urls: ["turn:1"], username: "u2", credential: "c")], expiresAt: later),
        ])
        let client = ConnectClient(transport: transport)
        let first = await client.fetchIceServers()
        XCTAssertEqual(first.first?.username, "u1")
        let second = await client.fetchIceServers()
        XCTAssertEqual(second.first?.username, "u2")
        XCTAssertEqual(transport.calls, 2)
    }

    private func realTransport() async throws -> SocketTransport {
        SequencedStubURLProtocol.reset()
        let options = SocketTransportOptions(url: "http://192.168.0.10:9981", stageId: "s1", secretKey: "ck_token-1")
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [SequencedStubURLProtocol.self]
        let transport = SocketTransport(options: options, session: URLSession(configuration: config))
        SequencedStubURLProtocol.enqueue(status: 200, body: #"{"jwt":"jwt-1","participantId":"p-1"}"#)
        try await transport.validateForTest()
        return transport
    }

    private static let stunBody = #"{"iceServers":[{"urls":"stun:stun.example.org:3478"}]}"#
    private static let turnBody = #"{"iceServers":[{"urls":"turn:turn.example.org:3478"}]}"#

    func testAStageRouteFailureFallsBackToStunWithoutCachingIt() async throws {
        // 인증 경로가 404 아닌 이유로 실패하면 공개 /api/about 의 STUN 은 임시다 — 종전에는 그것을 성공으로 만료 없이 캐시해 일시적
        // 5xx 한 번이 그 클라이언트의 수명 내내 TURN 없이(STUN 만) 남겼다.
        let cases: [(String, () -> Void)] = [
            ("5xx", { SequencedStubURLProtocol.enqueue(status: 503, body: "{}") }),
            ("401 after revalidation", {
                SequencedStubURLProtocol.enqueue(status: 401, body: "{}")
                SequencedStubURLProtocol.enqueue(status: 200, body: #"{"jwt":"jwt-2","participantId":"p-1"}"#)
                SequencedStubURLProtocol.enqueue(status: 401, body: "{}")
            }),
            ("unreadable body", { SequencedStubURLProtocol.enqueue(status: 200, body: "not json") }),
        ]
        for (name, stageFailure) in cases {
            let transport = try await realTransport()
            stageFailure()
            SequencedStubURLProtocol.enqueue(status: 200, body: Self.stunBody)
            SequencedStubURLProtocol.enqueue(status: 200, body: Self.turnBody)
            let client = ConnectClient(transport: transport)
            let stopgap = await client.fetchIceServersOutcome()
            XCTAssertFalse(stopgap.ok, name)
            XCTAssertEqual(stopgap.servers, [IceServer(urls: ["stun:stun.example.org:3478"])], name)
            let fetched = await client.fetchIceServersOutcome() // 캐시하지 않았다 — 다시 묻는다
            XCTAssertTrue(fetched.ok, name)
            XCTAssertEqual(fetched.servers, [IceServer(urls: ["turn:turn.example.org:3478"])], name)
            let hits = SequencedStubURLProtocol.hits
            _ = await client.fetchIceServers() // 성공은 캐시한다
            XCTAssertEqual(SequencedStubURLProtocol.hits, hits, name)
        }
    }

    func testAStageNetworkErrorOrRevalidationFailureDoesNotAskThePublicRoute() async throws {
        // TS 와 같다 — 인증 경로에 닿지 못했거나(네트워크 오류 · 시한) 재검증이 실패하면 공개 경로를 묻지 않는다. 클라이언트는 아직
        // 캐시된 목록(없으면 빈 목록)을 ok=false 로 돌려준다.
        let cases: [(String, Int, () -> Void)] = [
            ("network error", 2, { SequencedStubURLProtocol.enqueue(error: URLError(.networkConnectionLost)) }),
            ("revalidation failure", 3, {
                SequencedStubURLProtocol.enqueue(status: 401, body: "{}")
                SequencedStubURLProtocol.enqueue(status: 401, body: #"{"error":"revoked"}"#)
            }),
        ]
        for (name, hits, stageFailure) in cases {
            let transport = try await realTransport()
            stageFailure()
            SequencedStubURLProtocol.enqueue(status: 200, body: Self.stunBody)
            let outcome = await ConnectClient(transport: transport).fetchIceServersOutcome()
            XCTAssertFalse(outcome.ok, name)
            XCTAssertEqual(outcome.servers, [], name)
            XCTAssertEqual(SequencedStubURLProtocol.hits, hits, name) // validate · ice(· validate) — 공개 경로는 묻지 않았다
        }
    }

    func testARetryNetworkErrorAfterRevalidationDoesNotAskThePublicRoute() async throws {
        // 401 → 재검증 성공 → 다시 묻다가 네트워크 오류 — 공개 경로를 묻지 않고 빈 목록 · ok=false(TS 와 같다).
        let transport = try await realTransport()
        SequencedStubURLProtocol.enqueue(status: 401, body: "{}")
        SequencedStubURLProtocol.enqueue(status: 200, body: #"{"jwt":"jwt-2","participantId":"p-1"}"#)
        SequencedStubURLProtocol.enqueue(error: URLError(.networkConnectionLost))
        SequencedStubURLProtocol.enqueue(status: 200, body: Self.stunBody)
        let outcome = await ConnectClient(transport: transport).fetchIceServersOutcome()
        XCTAssertFalse(outcome.ok)
        XCTAssertEqual(outcome.servers, [])
        XCTAssertEqual(SequencedStubURLProtocol.hits, 4) // validate · ice(401) · validate · ice(오류) — 공개 경로는 묻지 않았다
    }

    func testAStillCachedListIsPreferredOverTheStopgapStun() async throws {
        // 만료 임박 TURN 이 캐시에 있으면 갱신 실패(인증 경로 503 + 공개 STUN)에도 그 TURN 을 돌려준다(ok=false).
        let transport = try await realTransport()
        let soon = Int64(Date().timeIntervalSince1970 * 1000) + 30_000
        SequencedStubURLProtocol.enqueue(status: 200, body: #"{"iceServers":[{"urls":"turn:turn.example.org:3478"}],"expiresAt":\#(soon)}"#)
        SequencedStubURLProtocol.enqueue(status: 503, body: "{}")
        SequencedStubURLProtocol.enqueue(status: 200, body: Self.stunBody)
        let client = ConnectClient(transport: transport)
        let first = await client.fetchIceServers()
        XCTAssertEqual(first.count, 1)
        let outcome = await client.fetchIceServersOutcome()
        XCTAssertFalse(outcome.ok)
        XCTAssertEqual(outcome.servers.map(\.urls), [["turn:turn.example.org:3478"]])
        XCTAssertEqual(SequencedStubURLProtocol.hits, 4)
    }

    func testAnOldServerWithoutTheStageRouteKeepsItsStunCached() async throws {
        // 404 = 구 서버(라우트 없음) — STUN 이 정답이라 종전대로 캐시한다.
        let transport = try await realTransport()
        SequencedStubURLProtocol.enqueue(status: 404, body: "{}")
        SequencedStubURLProtocol.enqueue(status: 200, body: Self.stunBody)
        let client = ConnectClient(transport: transport)
        let outcome = await client.fetchIceServersOutcome()
        XCTAssertTrue(outcome.ok)
        XCTAssertEqual(outcome.servers, [IceServer(urls: ["stun:stun.example.org:3478"])])
        let hits = SequencedStubURLProtocol.hits
        _ = await client.fetchIceServers()
        XCTAssertEqual(SequencedStubURLProtocol.hits, hits)
    }

    func testAFailedLookupOverTheRealTransportIsMarkedFailedAndNotCached() async throws {
        // 트랜스포트는 실패를 던지지 않고 빈 목록으로 돌려준다 — 화면 송출 헬퍼가 실패를 가려 마지막으로 받은 목록으로 답하도록
        // 실패로 표시하고, 클라이언트는 그것을 캐시하지 않는다.
        SequencedStubURLProtocol.reset()
        let options = SocketTransportOptions(url: "http://192.168.0.10:9981", stageId: "s1", secretKey: "ck_token-1")
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [SequencedStubURLProtocol.self]
        let transport = SocketTransport(options: options, session: URLSession(configuration: config))
        SequencedStubURLProtocol.enqueue(status: 200, body: #"{"jwt":"jwt-1","participantId":"p-1"}"#)
        try await transport.validateForTest()
        SequencedStubURLProtocol.enqueue(status: 500, body: "{}") // 인증 경로 실패
        SequencedStubURLProtocol.enqueue(status: 404, body: "{}") // 공개 경로도 실패
        SequencedStubURLProtocol.enqueue(status: 200, body: #"{"iceServers":[{"urls":"stun:stun.example.org:3478"}]}"#)
        let client = ConnectClient(transport: transport)

        let failed = await client.fetchIceServersOutcome()
        XCTAssertFalse(failed.ok)
        XCTAssertEqual(failed.servers, [])
        let fetched = await client.fetchIceServersOutcome()
        XCTAssertTrue(fetched.ok)
        XCTAssertEqual(fetched.servers, [IceServer(urls: ["stun:stun.example.org:3478"])])
        XCTAssertEqual(SequencedStubURLProtocol.hits, 4)
        // 성공은 종전대로 캐시한다(만료 없음) — 다시 묻지 않는다.
        _ = await client.fetchIceServers()
        XCTAssertEqual(SequencedStubURLProtocol.hits, 4)
    }
}

/// 응답을 **순서대로 소비**하는 스텁 — 캐시(1회 호출)와 실패→재시도 성공을 검증하려면
/// 호출마다 다른 응답이 필요하다. `RejoinLoopTests` 의 CountingStub 은 고정 응답이라
/// 별개로 둔다(그쪽을 고치면 다른 스위트의 기대가 흔들린다).
final class SequencedStubURLProtocol: URLProtocol {
    private enum Reply {
        case http(status: Int, body: Data)
        case failure(Error)
        /// 답하지 않는다 — 시한 · 취소 시험용(요청이 끊기면 `stopLoading` 이 불린다).
        case stall
        /// 200 머리만 보내고 본문을 0.1초마다 한 바이트씩 흘리며 끝내지 않는다 — 패킷 사이 공백만 재는 시한은 걸리지 않는다.
        case trickle
    }

    private static let lock = NSLock()
    private static var replies: [Reply] = []
    private static var callCount = 0
    private static var urls: [String] = []
    private static var methods: [String] = []
    private static var auths: [String?] = []
    private static var bodies: [String] = []

    static var hits: Int { lock.withLock { callCount } }
    static var requestedURLs: [String] { lock.withLock { urls } }
    /// 요청 메서드/Authorization 헤더 — ensure 테스트가 "POST + Bearer" 를 고정한다.
    static var requestedMethods: [String] { lock.withLock { methods } }
    static var requestedAuthorizations: [String?] { lock.withLock { auths } }
    /// 요청 본문 — validate body 계약 테스트가 읽는다.
    ///
    /// ⚠️ `URLProtocol` 에는 `httpBody` 가 대개 nil 로 온다(URLSession 이 스트림으로
    /// 바꿔 넘긴다). 스트림에서 직접 읽어야 본문이 잡힌다 — 이걸 모르면 "본문이 비었다"
    /// 는 거짓 실패를 보게 된다.
    static var requestedBodies: [String] { lock.withLock { bodies } }

    static func reset() {
        lock.withLock {
            replies = []
            callCount = 0
            urls = []
            methods = []
            auths = []
            bodies = []
        }
    }

    static func enqueue(status: Int, body: String) {
        lock.withLock { replies.append(.http(status: status, body: Data(body.utf8))) }
    }

    static func enqueue(error: Error) {
        lock.withLock { replies.append(.failure(error)) }
    }

    static func enqueueStall() {
        lock.withLock { replies.append(.stall) }
    }

    static func enqueueTrickle() {
        lock.withLock { replies.append(.trickle) }
    }

    private var trickleTimer: DispatchSourceTimer?

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let reply: Reply? = SequencedStubURLProtocol.lock.withLock {
            SequencedStubURLProtocol.callCount += 1
            SequencedStubURLProtocol.urls.append(request.url?.absoluteString ?? "")
            SequencedStubURLProtocol.methods.append(request.httpMethod ?? "")
            SequencedStubURLProtocol.auths.append(request.value(forHTTPHeaderField: "Authorization"))
            SequencedStubURLProtocol.bodies.append(Self.readBody(of: request))
            return SequencedStubURLProtocol.replies.isEmpty
                ? nil
                : SequencedStubURLProtocol.replies.removeFirst()
        }
        switch reply {
        case .some(.http(let status, let body)):
            let response = HTTPURLResponse(
                url: request.url!, statusCode: status, httpVersion: "HTTP/1.1",
                headerFields: ["Content-Type": "application/json"])!
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: body)
            client?.urlProtocolDidFinishLoading(self)
        case .some(.failure(let error)):
            client?.urlProtocol(self, didFailWithError: error)
        case .some(.stall):
            break
        case .some(.trickle):
            let response = HTTPURLResponse(
                url: request.url!, statusCode: 200, httpVersion: "HTTP/1.1",
                headerFields: ["Content-Type": "application/json"])!
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            let timer = DispatchSource.makeTimerSource(queue: DispatchQueue(label: "stub.trickle"))
            timer.schedule(deadline: .now() + 0.1, repeating: 0.1)
            timer.setEventHandler { [weak self] in
                guard let self else { return }
                self.client?.urlProtocol(self, didLoad: Data(" ".utf8))
            }
            trickleTimer = timer
            timer.resume()
        case .none:
            // 시나리오에 없는 추가 요청 — 테스트가 기대보다 많이 부른 것이니 실패로 표면화.
            client?.urlProtocol(self, didFailWithError: URLError(.badServerResponse))
        }
    }

    override func stopLoading() {
        trickleTimer?.cancel()
        trickleTimer = nil
    }

    /// httpBody → 없으면 httpBodyStream 에서 읽는다.
    private static func readBody(of request: URLRequest) -> String {
        if let data = request.httpBody { return String(decoding: data, as: UTF8.self) }
        guard let stream = request.httpBodyStream else { return "" }
        stream.open()
        defer { stream.close() }
        var data = Data()
        let size = 4096
        var buffer = [UInt8](repeating: 0, count: size)
        while stream.hasBytesAvailable {
            let read = stream.read(&buffer, maxLength: size)
            if read <= 0 { break }
            data.append(contentsOf: buffer[0..<read])
        }
        return String(decoding: data, as: UTF8.self)
    }
}
