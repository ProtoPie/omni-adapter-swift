import XCTest
@testable import OmniAdapter

/// 코어 결정론 테스트 — fake 트랜스포트(TS client.test.ts / Python test_client.py 미러).
final class ConnectClientTests: XCTestCase {

    private final class FakeTransport: Transport, @unchecked Sendable {
        private let lock = NSLock()
        private var handlersStorage = TransportHandlers()
        private var publishedStorage: [(PublishInput, Bool)] = []
        private var resultStorage = PublishResult(ok: true)

        let me = Member(participantId: "me-1", displayName: "Me")
        private(set) var closed = false

        var handlers: TransportHandlers { lock.withLock { handlersStorage } }
        var published: [(PublishInput, Bool)] { lock.withLock { publishedStorage } }
        var values: [Primitive] { published.map { $0.0.value } }

        func setResult(_ result: PublishResult) { lock.withLock { resultStorage = result } }

        /// publish 처리 **도중에** 세션을 끝낸다 — enqueue 직전 창 재현.
        private var endDuringPublishStorage = false
        func setEndDuringPublish() { lock.withLock { endDuringPublishStorage = true } }

        /// 첫 발행만 이 코드로 실패시킨다(그 뒤는 `resultStorage`) — 타이밍에 기대지 않는 실패 주입.
        private var failFirstStorage: String?
        func setFailFirstWith(_ code: String) { lock.withLock { failFirstStorage = code } }

        func open(handlers: TransportHandlers) async throws -> Joined {
            lock.withLock { handlersStorage = handlers }
            return Joined(roomId: "stage-1", me: me)
        }

        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult {
            let endNow: Bool = lock.withLock {
                publishedStorage.append((input, ack))
                let flag = endDuringPublishStorage
                endDuringPublishStorage = false
                return flag
            }
            if endNow {
                end("ended-mid-publish")
                return PublishResult(ok: false, code: "NotConnected")
            }

            let failFirst: String? = lock.withLock {
                let code = failFirstStorage
                failFirstStorage = nil
                return code
            }
            if let failFirst { return PublishResult(ok: false, code: failFirst) }

            return lock.withLock { resultStorage }
        }

        func listMembers() async throws -> [Member] { [me] }

        func close() async { lock.withLock { closed = true } }

        // 테스트 헬퍼
        func push(_ channel: String, _ value: Primitive, ts: Int64 = 1000) {
            handlers.onMessage?(
                StageMessage(channel: channel, value: value, sourceParticipantId: "peer-1", timestamp: ts)
            )
        }

        func pushHistory(_ messages: [StageMessage]) { handlers.onHistory?(messages) }

        func setState(_ state: TransportState) { handlers.onState?(state) }

        /// 실 트랜스포트 순서 재현 — markEnded 가 onState(.ended) 를 먼저 쏜다.
        func end(_ reason: String?) {
            handlers.onState?(.ended)
            handlers.onEnded?(reason)
        }
    }

    private func h(_ channel: String, _ value: Primitive, _ ts: Int64) -> StageMessage {
        StageMessage(channel: channel, value: value, sourceParticipantId: "peer-1", timestamp: ts)
    }

    private func makeClient(
        queueLimit: Int = ConnectClient.defaultQueueLimit,
        strictReceives: [String]? = nil
    ) async throws -> (FakeTransport, ConnectClient) {
        let transport = FakeTransport()
        let client = ConnectClient(
            transport: transport,
            queueLimit: queueLimit,
            strictReceives: strictReceives
        )
        try await client.open()
        return (transport, client)
    }

    /// 고정 대기 — "아무 일도 일어나지 않았다"를 보기 전에만 쓴다. 긍정 결과는 waitFor · waitForState · waitUntil 로 기다린다
    /// (고정 sleep 뒤의 긍정 단언은 느린 러너에서 거짓 실패한다 — TestTiming).
    private func drain(_ ticks: Int = 3) async throws {
        for _ in 0..<ticks { try await Task.sleep(nanoseconds: 20_000_000) }
    }

    /// 큐 flush 는 백그라운드 태스크 — 조건이 참이 될 때까지 짧게 기다린다.
    /// 큐가 비워질 때까지 기다린다(actor 접근이라 동기 조건자로는 못 본다).
    private func waitForQueueDrain(
        _ client: ConnectClient,
        timeout: TimeInterval = 3
    ) async throws {
        let deadline = TestTiming.deadline(timeout)
        while await client.queuedCount != 0, Date() < deadline {
            try await Task.sleep(nanoseconds: 5_000_000)
        }
        let left = await client.queuedCount
        XCTAssertEqual(left, 0, "큐가 비지 않았다")
    }

    /// 상태 전이가 **인박스를 지나 실제로 반영될 때까지** 기다린다.
    private func waitForState(
        _ client: ConnectClient,
        _ expected: TransportState,
        timeout: TimeInterval = 3
    ) async throws {
        let deadline = TestTiming.deadline(timeout)
        while await client.state != expected, Date() < deadline {
            try await Task.sleep(nanoseconds: 2_000_000)
        }
        let actual = await client.state
        XCTAssertEqual(actual, expected, "상태가 반영되지 않았다")
    }

    private func waitFor(
        timeout: TimeInterval = 3,
        _ condition: @escaping () -> Bool
    ) async throws {
        let deadline = TestTiming.deadline(timeout)
        while !condition() && Date() < deadline {
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTAssertTrue(condition(), "condition not met within timeout")
    }

    /// 액터를 읽어야 하는 조건을 기다린다(시한은 TestTiming.hangGuard 이상).
    private func waitUntil(
        _ what: String, file: StaticString = #filePath, line: UInt = #line, _ condition: () async -> Bool
    ) async {
        let met = await TestTiming.until(0, condition)
        XCTAssertTrue(met, "\(what) — not met within \(Int(TestTiming.hangGuard)) s", file: file, line: line)
    }

    func testJoinExposesMeAndState() async throws {
        let (_, client) = try await makeClient()
        let state = await client.state
        let me = await client.me
        XCTAssertEqual(state, .connected)
        XCTAssertEqual(me?.participantId, "me-1")
    }

    func testSendPublishesAndFoldsLatest() async throws {
        let (transport, client) = try await makeClient()
        try await client.send("temp.reading", 23.5)
        XCTAssertEqual(transport.published.first?.0.channel, "temp.reading")
        // 자기 발행 fold (서버 무에코)
        let latest = await client.latest("temp.reading")
        XCTAssertEqual(latest, .double(23.5))
    }

    func testSendJSONAndOnJSONRoundtrip() async throws {
        let (transport, client) = try await makeClient()
        let seen = Box<[String]>([])
        await client.onJSON("robot.status") { value, message in
            if let object = value as? [String: Any], let pose = object["pose"] as? [Int] {
                seen.value.append("\(pose[0]):\(message.channel)")
            }
        }

        try await client.sendJSON("robot.status", ["pose": [1, 2]])
        XCTAssertEqual(transport.values.first?.stringValue, #"{"pose":[1,2]}"#)

        transport.push("robot.status", .string(#"{"pose":[3,4]}"#))
        try await waitFor { seen.value == ["3:robot.status"] }
    }

    private struct Pose { let pose: [Int] }

    func testSendJSONRejectsWhatItCannotEncodeInsteadOfCrashing() async throws {
        // 종전에는 `JSONSerialization.data` 가 이 값들에 Objective-C 예외를 던져 **프로세스가 죽었다**(실측 exit 134 —
        // `try` 로 못 잡는다). 이제 ValidationError 이고, 메시지가 타입을 대며, 아무것도 나가지 않는다.
        let (transport, client) = try await makeClient()
        let cases: [(Any, String)] = [
            (Pose(pose: [1, 2]), "Pose"),                       // 구조체
            (["robot": Pose(pose: [1])], "Pose"),               // 중첩 구조체
            ([1: "x"], "Int"),                                  // 문자열이 아닌 키
            (Double.nan, "Double"),                             // NaN
            (["speed": Double.infinity], "Double"),             // 중첩 Infinity
            (Primitive.double(1), "Primitive"),                 // send 용 값을 잘못 넘김
        ]
        for (value, needle) in cases {
            do {
                try await client.sendJSON("robot.status", value)
                XCTFail("\(value) 는 ValidationError 여야 한다")
            } catch let err as AdapterError {
                XCTAssertEqual(err.code, "ValidationError")
                XCTAssertTrue(err.message.contains(needle), err.message)
                XCTAssertTrue(err.hint?.contains("JSON-compatible") ?? false)
            }
        }
        XCTAssertTrue(transport.values.isEmpty, "거절된 값은 하나도 나가지 않는다: \(transport.values)")

        // 지원하는 모양은 종전처럼 — 최상위 조각(문자열 · 숫자 · NSNull)도 그대로다.
        try await client.sendJSON("a", [1, 2])
        try await client.sendJSON("a", "hi")
        try await client.sendJSON("a", 2.5)
        try await client.sendJSON("a", NSNull())
        XCTAssertEqual(transport.values.map { $0.stringValue }, ["[1,2]", #""hi""#, "2.5", "null"])
    }

    func testNonFiniteNumbersAreRejectedOnEveryPublishPath() async throws {
        // JSON 에는 NaN · Infinity 가 없다 — 종전에는 소켓 라이브러리의 JSONSerialization 이 Objective-C 예외를 던져 프로세스가
        // 죽었다. 이제 send · sendTo · throttle 이 그 자리에서 ValidationError(여섯 언어 같은 규칙). Float 은 .double 로 들어온다.
        let (transport, client) = try await makeClient()
        let push = await client.throttle("a.b", hz: 10)
        let bad: [(Double, String)] = [
            (.nan, "NaN"), (.infinity, "Infinity"), (-.infinity, "-Infinity"),
            (Double(Float.nan), "NaN"), (Double(-Float.infinity), "-Infinity"),
        ]
        for (number, shown) in bad {
            var errors: [Error] = []
            do { try await client.send("a.b", .double(number)) } catch { errors.append(error) }
            do { try await client.send("a.b", .double(number), ack: true) } catch { errors.append(error) }
            do { try await client.sendTo("peer-9", "a.b", .double(number)) } catch { errors.append(error) }
            do { try push(.double(number)) } catch { errors.append(error) }
            XCTAssertEqual(errors.count, 4, "\(shown)")
            for error in errors {
                XCTAssertEqual((error as? AdapterError)?.code, "ValidationError")
                XCTAssertEqual((error as? AdapterError)?.message, "value must be a finite number (got \(shown))")
            }
        }
        try await drain()
        XCTAssertTrue(transport.values.isEmpty, "\(transport.values)")
        // 0 · 음수 소수는 통과한다.
        try await client.send("a.b", .int(0))
        try await client.send("a.b", .double(0))
        try await client.send("a.b", .double(-1.5))
        XCTAssertEqual(transport.values, [.int(0), .double(0), .double(-1.5)])
    }

    func testQueueLimitBelowOneIsAConfigErrorBeforeTheClientIsMade() async throws {
        // 0 · 음수면 끊긴 동안의 발행과 토큰이 빈 순간의 발행을 모두 버렸다. 여섯 언어 같은 규칙 — 이니셜라이저는 throws 가
        // 아니라서(precondition) connect() · MemoryStage.join() 이 만들기 전에 거른다.
        for bad in [0, -1, Int.min] {
            XCTAssertThrowsError(try ConnectClient.validateQueueLimit(bad)) { error in
                XCTAssertEqual((error as? AdapterError)?.code, "ConfigError")
                XCTAssertEqual((error as? AdapterError)?.message, "queueLimit must be at least 1 (got \(bad))")
            }
            var options = ConnectOptions(secretKey: "ck_k", url: "http://127.0.0.1:9", stageId: "s")
            options.queueLimit = bad
            do {
                _ = try await connect(options)
                XCTFail("\(bad): expected ConfigError")
            } catch let error as AdapterError {
                XCTAssertEqual(error.code, "ConfigError", "\(bad)")
                XCTAssertTrue(error.message.contains("queueLimit"), error.message)
            }
            do {
                _ = try await MemoryStage().join(MemoryJoinOptions(queueLimit: bad))
                XCTFail("\(bad): expected ConfigError")
            } catch let error as AdapterError {
                XCTAssertEqual(error.code, "ConfigError", "\(bad)")
            }
        }
        XCTAssertNoThrow(try ConnectClient.validateQueueLimit(1))
        let one = try await MemoryStage().join(MemoryJoinOptions(queueLimit: 1))
        await one.close()
    }

    func testThrottlePushValidatesTheChannelAtTheCallSite() async throws {
        // 값은 Primitive 라 틀릴 수 없지만 채널은 틀릴 수 있다 — 종전에는 클로저가 던지지 않고 발행을 try? 로 삼켜 틀린 채널의
        // 값이 흔적 없이 사라졌다. 이제 send 와 같이 그 자리에서 ValidationError 다(다른 다섯 언어와 같은 계약).
        let (transport, client) = try await makeClient()
        let push = await client.throttle(String(repeating: "c", count: 513), hz: 10)
        XCTAssertThrowsError(try push(.int(1))) { error in
            XCTAssertEqual((error as? AdapterError)?.code, "ValidationError")
        }
        let empty = await client.throttle("", hz: 10)
        XCTAssertThrowsError(try empty(.int(1)))
        try await Task.sleep(nanoseconds: 150_000_000)
        XCTAssertTrue(transport.values.isEmpty)
        // 검증을 통과한 값의 발행 실패는 던지지 않는다 — 끝난 세션 뒤의 호출도.
        let ok = await client.throttle("nav.position", hz: 10)
        await client.close()
        XCTAssertNoThrow(try ok(.int(2)))
    }

    func testOnJSONDeliversAJSONNullAsNSNull() async throws {
        // JSON `null` 은 값이다 — TS 는 콜백에 null 을 건넨다. 종전에는 nil 로 접어 그 메시지를 통째로 건너뛰었다.
        let (transport, client) = try await makeClient()
        let seen = Box<[String]>([])
        await client.onJSON("robot.status") { value, _ in
            seen.value.append(value is NSNull ? "null" : "\(value)")
        }
        transport.push("robot.status", .string("null"))
        transport.push("robot.status", .string("not json"))  // 파싱 불가는 여전히 건너뛴다
        try await waitFor { seen.value == ["null"] }
        let latest = await client.latestJSON("robot.status")
        XCTAssertNil(latest, "the last value is not JSON")
        transport.push("robot.status", .string("null"))
        try await waitFor { seen.value.count == 2 }
        let latestNull = await client.latestJSON("robot.status")
        XCTAssertTrue(latestNull is NSNull)
    }

    func testWildcardSubscriptionAndUnsubscribe() async throws {
        let (transport, client) = try await makeClient()
        let seen = Box<[String]>([])
        let sub = await client.on("robot.*") { seen.value.append($0.channel) }

        transport.push("robot.arm.pos", 1)
        transport.push("other.ch", 2)
        try await waitFor { seen.value == ["robot.arm.pos"] }

        // 해제는 액터로 넘긴 Task 가 반영한다 — 고정 대기 뒤에 "반영됐을 것"이라 보면 느린 러너에서 해제 전의 값이 닿아
        // 거짓 실패한다. 살아 있는 구독(표지)에 닿은 값이 해제한 구독에는 닿지 않는 때를 기다린다 — 표지가 받았으면 그
        // 메시지의 콜백은 액터 위에서 한 번에 돌았거나 돌고 있다. 액터를 한 번 더 거치면 그 메시지의 콜백이 모두 끝났다.
        let processed = Box<[Int]>([])
        await client.on("robot.arm.pos") { message in
            if case .int(let n) = message.value { processed.value.append(n) }
        }
        sub.cancel()
        var probe = 2
        await waitUntil("a cancelled subscription stops receiving") {
            probe += 1
            let before = seen.value.count
            transport.push("robot.arm.pos", .int(probe))
            let sent = probe
            guard await TestTiming.until(0, { processed.value.contains(sent) }) else { return false }
            _ = await client.state
            return seen.value.count == before
        }
        let settled = seen.value.count
        transport.push("robot.arm.pos", .int(probe + 1))
        let last = probe + 1
        try await waitFor { processed.value.contains(last) }
        _ = await client.state
        XCTAssertEqual(seen.value.count, settled, "a cancelled subscription received a later message")
    }

    func testHistorySeedsLatestAndHistoryJSON() async throws {
        let (transport, client) = try await makeClient()
        transport.pushHistory([
            h("chat.message", .string(#"{"n":1}"#), 100),
            h("chat.message", .string("broken"), 150),
            h("chat.message", .string(#"{"n":2}"#), 200),
            h("temp.reading", .int(25), 300),
        ])
        try await waitForLatest(client, "temp.reading", .int(25))

        let latest = await client.latest("temp.reading")
        XCTAssertEqual(latest, .int(25))

        let historyJSON = await client.historyJSON("chat.message")
        XCTAssertEqual(historyJSON.compactMap { ($0 as? [String: Any])?["n"] as? Int }, [1, 2])

        let latestJSON = await client.latestJSON("chat.message") as? [String: Any]
        XCTAssertEqual(latestJSON?["n"] as? Int, 2)

        // 패턴 필터 — 백로그 렌더용(와일드카드 가능), 6언어 공통 표면.
        let chatOnly = await client.history("chat.*")
        let all = await client.history()
        XCTAssertEqual(Set(chatOnly.map(\.channel)), ["chat.message"])
        XCTAssertEqual(chatOnly.count, all.count - 1)

        // clearHistory 브로드캐스트 = 빈 스냅샷 → 캐시 클리어
        transport.pushHistory([])
        await waitUntil("the empty snapshot cleared the cache") { await client.latest("temp.reading") == nil }
        let clearedLatest = await client.latest("temp.reading")
        let clearedHistory = await client.history()
        XCTAssertNil(clearedLatest)
        XCTAssertTrue(clearedHistory.isEmpty)
    }

    func testOfflineQueueFlushesInOrder() async throws {
        let (transport, client) = try await makeClient()
        transport.setState(.reconnecting)
        try await waitForState(client, .reconnecting)

        try await client.send("a.b", 1)
        try await client.send("a.b", 2)
        XCTAssertTrue(transport.published.isEmpty) // 큐에 대기

        transport.setState(.connected)
        try await waitFor { transport.values.count == 2 }
        XCTAssertEqual(transport.values, [.int(1), .int(2)])
    }

    func testQueueOverflowDropsOldest() async throws {
        let (transport, client) = try await makeClient(queueLimit: 2)
        let dropped = Box<[Primitive]>([])
        let stream = await client.events()
        let collector = Task {
            for await event in stream {
                if case .queueOverflow(let input) = event { dropped.value.append(input.value) }
            }
        }
        try await drain(2)

        transport.setState(.reconnecting)
        try await waitForState(client, .reconnecting)
        try await client.send("a.b", 1)
        try await client.send("a.b", 2)
        try await client.send("a.b", 3)
        try await waitFor { dropped.value == [.int(1)] }

        transport.setState(.connected)
        try await waitFor { transport.values.count == 2 }
        XCTAssertEqual(transport.values, [.int(2), .int(3)])
        collector.cancel()
    }

    func testQueuedCountShowsTheBacklog() async throws {
        // "연결됐다는데 조용하다" 의 정체(큐 적체)는 이 값으로만 보인다.
        let (transport, client) = try await makeClient()
        var count = await client.queuedCount
        XCTAssertEqual(count, 0)
        transport.setState(.reconnecting)
        try await waitForState(client, .reconnecting)
        try await client.send("a.b", 1)
        try await client.send("a.b", 2)
        count = await client.queuedCount
        XCTAssertEqual(count, 2)
        transport.setState(.connected)
        try await waitForQueueDrain(client)
        await client.close()
    }

    func testAckTimeoutRearmsFlushWithoutAStateChange() async throws {
        // AckTimeout 은 **소켓이 연결된 채** ack 만 넘긴 것이라 깨울 상태 이벤트가 없다.
        // 큐에 남기기만 하고 끝내면 아무도 flush 를 다시 부르지 않아 큐가 영영 멈춘다.
        // (종전 Swift 는 AckTimeout 분기 자체가 없어 항목을 큐에서 빼 유실시켰다.)
        let (transport, client) = try await makeClient()
        transport.setState(.reconnecting)
        // Swift 는 상태 전이를 **FIFO 인박스**로 나른다 — setState 가 곧바로 client.state 를
        // 바꾸지 않는다. 반영을 기다리지 않고 보내면 그 send 가 직행 경로를 타 버린다
        // (전체 스위트에서만 이 테스트가 죽던 원인이 정확히 이것이었다).
        try await waitForState(client, .reconnecting)
        // fire-and-forget 은 enqueue 하고 바로 돌아온다 — 큐가 확실히 채워진 뒤 연결한다.
        try await client.send("a.b", 1)
        XCTAssertTrue(transport.published.isEmpty, "큐 대기여야 한다")
        transport.setFailFirstWith("AckTimeout")
        transport.setState(.connected)
        // 첫 flush 는 AckTimeout — 이후 상태는 그대로 connected 다. 재무장이 없으면
        // 아무도 flush 를 다시 부르지 않아 여기서 영영 멈춘다.
        try await waitFor(timeout: 5) { transport.values.count >= 2 }
        XCTAssertEqual(transport.values, [.int(1), .int(1)])
        // ⚠️ client 를 여기까지 살려 두는 것이 이 close 의 진짜 목적이다. 재무장 Task 는
        // `[weak self]` 라(백오프 Task 도 같은 모양), 마지막 client 사용 뒤 ARC 가 조기
        // 해제하면 `guard let self` 에서 조용히 끝난다 — 전체 스위트에서만 이 테스트가
        // 죽던 원인이 정확히 그것이었다(격리 실행은 통과했다).
        await client.close()
    }

    func testTooManyRequestsPausesThenResumes() async throws {
        let (transport, client) = try await makeClient()
        transport.setResult(PublishResult(ok: false, code: "TooManyRequests"))
        try await client.send("a.b", 1) // 직행 발행 → 429 → 큐 재수용 + 백오프
        XCTAssertEqual(transport.published.count, 1)

        transport.setResult(PublishResult(ok: true))
        try await client.send("a.b", 2) // 백오프 중 — 큐로
        XCTAssertEqual(transport.published.count, 1)

        // 초기 백오프 1s 경과 → flush (1 재시도 후 2)
        try await waitFor(timeout: 5) { transport.values.count == 3 }
        XCTAssertEqual(transport.values, [.int(1), .int(1), .int(2)])
    }

    func testLiveSendNeverOvertakesTheQueueAndFlushPipelinesFireAndForget() async throws {
        let (transport, client) = try await makeClient()
        transport.setState(.reconnecting)
        try await waitForState(client, .reconnecting)
        try await client.send("a.b", "queued-1")
        let acked = Task { try await client.send("a.b", "queued-2", ack: true) }
        await waitUntil("the acked send is queued") { await client.queuedCount >= 2 }
        try await client.send("a.b", "queued-3")
        transport.setState(.connected)
        try await drain()
        try await acked.value
        try await client.send("a.b", "live-4")

        try await waitFor { transport.values.count == 4 }
        XCTAssertEqual(
            transport.values.map { $0.stringValue },
            ["queued-1", "queued-2", "queued-3", "live-4"]
        )
        // D2: 큐 경유 fire-and-forget 은 ack 없이 내보낸다(기다리지 않는다) — ack 발행만 ack 를 요구한다. 종전에는 전부
        // ack 를 기다려 처리량이 왕복 1회당 1건이었다(그보다 빠른 발신자는 큐를 영영 비우지 못했다).
        XCTAssertEqual(transport.published.prefix(3).map { $0.1 }, [false, true, false])
    }

    func testEndedRejectsSends() async throws {
        let (transport, client) = try await makeClient()
        let ended = Box<[String?]>([])
        let stream = await client.events()
        let collector = Task {
            for await event in stream {
                if case .ended(let reason) = event { ended.value.append(reason) }
            }
        }
        try await drain(2)

        // 큐에 ack 대기 항목을 남긴 채 종료 — 영구 hang 회귀 가드.
        transport.setState(.reconnecting)
        try await waitForState(client, .reconnecting)
        let pending = Task { try await client.send("a.b", 1, ack: true) }
        await waitUntil("the acked send is queued") { await client.queuedCount == 1 }

        transport.end("host-closed")
        try await drain()

        do {
            try await pending.value
            XCTFail("pending ack send should fail once the session ended")
        } catch {
            XCTAssertEqual((error as? AdapterError)?.code, "SessionEnded")
        }
        try await waitFor { ended.value == ["host-closed"] }
        collector.cancel()

        let state = await client.state
        XCTAssertEqual(state, .ended)
        do {
            try await client.send("a.b", 1)
            XCTFail("send should throw after the session ended")
        } catch {
            XCTAssertEqual((error as? AdapterError)?.code, "SessionEnded")
        }
    }

    func testThrottleCoalesces() async throws {
        let (transport, client) = try await makeClient()
        let push = await client.throttle("nav.position", hz: 10) // 100ms 간격
        try push(.string("p1"))
        try push(.string("p2"))
        try push(.string("p3"))

        try await waitFor { transport.values.count == 2 }
        XCTAssertEqual(transport.values.first?.stringValue, "p1")  // 선두 즉시
        XCTAssertEqual(transport.values.last?.stringValue, "p3")   // 마지막 값 trailing
        XCTAssertEqual(transport.values.count, 2)                  // 중간(p2)은 뭉개진다
    }

    func testStrictReceivesWarnsOnUndeclaredChannel() async throws {
        let (_, client) = try await makeClient(strictReceives: ["a.b"])
        let warnings = Box<[AdapterError]>([])
        let stream = await client.events()
        let collector = Task {
            for await event in stream {
                if case .failure(let error) = event { warnings.value.append(error) }
            }
        }
        try await drain(2)

        await client.on("not.declared") { _ in }
        try await waitFor { warnings.value.contains { $0.code == "StrictRoutingWarning" } }

        warnings.value = []
        await client.on("a.b") { _ in }
        try await drain()
        XCTAssertTrue(warnings.value.isEmpty)
        collector.cancel()
    }

    func testAckFailureSurfacesAsAdapterError() async throws {
        let (transport, client) = try await makeClient()
        transport.setResult(PublishResult(ok: false, code: "ValidationError", message: "channel too long"))
        do {
            try await client.send("a.b", 1, ack: true)
            XCTFail("ack failure should throw")
        } catch {
            XCTAssertEqual((error as? AdapterError)?.code, "ValidationError")
            XCTAssertEqual((error as? AdapterError)?.message, "channel too long")
        }
    }

    /// publish 를 기다리는 사이 세션이 끝나면 그 항목은 이미 비워진 큐로 들어가
    /// 아무도 깨우지 않는다 — 영구 hang. SessionEnded 로 **완료**돼야 한다.
    func testSendThatEndsMidPublishFailsInsteadOfHanging() async throws {
        let (transport, client) = try await makeClient()
        transport.setEndDuringPublish()

        let done = Task { () -> String? in
            do {
                try await client.send("a.b", 1, ack: true)
                return nil
            } catch {
                return (error as? AdapterError)?.code
            }
        }
        let code = await done.value
        XCTAssertEqual(code, "SessionEnded")
    }

    func testCloseMarksEnded() async throws {
        let (transport, client) = try await makeClient()
        await client.close()
        let state = await client.state
        XCTAssertTrue(transport.closed)
        XCTAssertEqual(state, .ended)
    }

    // ── 종료 뒤 정리 (ARC) ──────────────────────────────────────────────────
    // 구독 핸들을 버리는 것이 정상 사용법이고 콜백이 클라이언트를 잡는 것도 흔하다(echo).
    // close() 가 구독을 놓지 않으면 클라이언트 → 구독 맵 → 콜백 → 클라이언트 순환이 남는다.

    /// 콜백마다 클라이언트를 잡는 구독 세 종류를 붙이고(핸들 버림) 닫은 뒤의 약한 참조.
    private func closedClientCapturedBySubscriptions(subscribeAfterClose: Bool) async throws
        -> () -> ConnectClient?
    {
        let (_, client) = try await makeClient()
        func subscribeAll() async {
            await client.on("cmd") { msg in Task { try? await client.send("echo", msg.value) } }
            _ = await client.onSignaling { _ in Task { await client.sendSignaling([:]) } }
            _ = client.onRemoteInputHolder { _ in Task { _ = await client.state } }
        }
        if !subscribeAfterClose { await subscribeAll() }
        await client.close()
        if subscribeAfterClose { await subscribeAll() }
        return { [weak client] in client }
    }

    func testCloseReleasesSubscriptionsThatCaptureTheClient() async throws {
        let probe = try await closedClientCapturedBySubscriptions(subscribeAfterClose: false)
        try await waitFor { probe() == nil }
    }

    func testSubscribingAfterCloseDoesNotRetainTheClient() async throws {
        let probe = try await closedClientCapturedBySubscriptions(subscribeAfterClose: true)
        try await waitFor { probe() == nil }
    }

    /// 스트림을 끝까지 읽는다 — 스스로 끝나지 않으면 `timeout` 뒤 취소하고 그 사실을 돌려준다
    /// (끝나지 않는 스트림이 스위트를 멈추지 않게).
    private func drainStream(
        _ stream: AsyncStream<ClientEvent>, timeout: TimeInterval = 2
    ) async -> (events: [ClientEvent], finishedByItself: Bool) {
        // 끝나는 스트림은 곧바로 돌아온다 — 감시 시한은 "끝나지 않는다"를 가르는 자리라 hangGuard 이상으로 둔다.
        let timeout = TestTiming.atLeastHangGuard(timeout)
        let reader = Task { () -> [ClientEvent] in
            var out: [ClientEvent] = []
            for await event in stream { out.append(event) }
            return out
        }
        let watchdog = Task {
            try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
            reader.cancel()
        }
        let events = await reader.value
        watchdog.cancel()
        return (events, !reader.isCancelled)
    }

    /// `.ended` 에서 break 하지 않는 소비 루프도 종료 뒤 빠져나와야 한다 — 안 그러면 그 태스크가
    /// 영원히 매달리고, 루프가 잡은 클라이언트도 함께 남는다.
    func testEventsStreamFinishesWhenTheSessionEnds() async throws {
        let (_, client) = try await makeClient()
        let stream = await client.events()
        await client.close()
        let (events, finished) = await drainStream(stream)
        XCTAssertTrue(finished, "세션이 끝났는데 events() 스트림이 끝나지 않는다")
        guard events.count == 2,
              case .stateChange(.ended) = events[0],
              case .ended(let reason) = events[1]
        else { return XCTFail("stateChange(.ended) 와 .ended 를 받은 뒤 끝나야 한다: \(events)") }
        XCTAssertEqual(reason, "client-closed")

        // 끝난 뒤에 구독한 스트림은 곧바로 끝난다.
        let (late, lateFinished) = await drainStream(await client.events(), timeout: 1)
        XCTAssertTrue(lateFinished, "종료 뒤 구독한 스트림이 영원히 매달린다")
        XCTAssertTrue(late.isEmpty)
    }

    func testEventsConsumerThatCapturesTheClientDoesNotPinIt() async throws {
        func run() async throws -> () -> ConnectClient? {
            let (_, client) = try await makeClient()
            Task {
                for await event in await client.events() {
                    if case .stateChange = event { _ = await client.state }
                }
            }
            try await drain()
            await client.close()
            return { [weak client] in client }
        }
        let probe = try await run()
        try await waitFor { probe() == nil }
    }

    /// 읽지 않고 쥐고만 있는 스트림은 최근 256개까지만 쌓는다(가장 오래된 것부터 버림).
    func testUnreadEventsAreBoundedToTheNewest256() async throws {
        let (transport, client) = try await makeClient()
        let stream = await client.events()
        // 스냅샷은 순서 확정 인박스를 지나므로 마지막 것이 반영됐으면 앞의 것도 전부 발화했다.
        for i in 0..<1000 { transport.pushHistory([h("c", .int(i), Int64(i))]) }
        let deadline = TestTiming.deadline(3)
        while await client.history().first?.value != .int(999), Date() < deadline {
            try await Task.sleep(nanoseconds: 5_000_000)
        }
        await client.close() // stateChange(.ended) · .ended 를 싣고 스트림을 끝낸다
        let (events, _) = await drainStream(stream)
        XCTAssertEqual(ConnectClient.eventBufferLimit, 256)
        XCTAssertEqual(events.count, 256, "읽지 않은 이벤트가 상한 없이 쌓였다")
        guard case .historyReplaced(let oldestKept) = events.first,
              case .ended = events.last
        else { return XCTFail("가장 새 256개가 남아야 한다") }
        XCTAssertEqual(oldestKept.first?.value, .int(746)) // 1000 + 2 − 256
    }
    /// 트랜스포트가 알린 오류는 종료 뒤에 스트림이 끝나도 **반드시** 배달되고, 트랜스포트가 알린 순서
    /// 그대로다(오류 → ended 상태 → ended). 종전에는 오류와 종료가 따로 Task 로 액터에 넘어가 종료가
    /// 먼저 스트림을 끝내면 오류가 사라졌다(실측 3000회 중 169회).
    func testTransportErrorsAreNeverLostAndKeepTheirOrderAgainstTheEnd() async throws {
        let q = DispatchQueue(label: "fake-socket-queue")
        var lost = 0
        var misordered: [[String]] = []
        for round in 0..<2000 {
            let (transport, client) = try await makeClient()
            let stream = await client.events()
            let errorFirst = round % 2 == 0
            q.async {
                let h = transport.handlers
                if errorFirst {
                    h.onError?(AdapterError("ProtocolVersionMismatch", "x"))
                    h.onState?(.ended)
                } else {
                    // 순서가 거꾸로 와도 오류는 잃지 않는다(클라이언트는 받은 순서를 지킬 뿐이다).
                    h.onState?(.ended)
                    h.onError?(AdapterError("AuthRevoked", "x"))
                }
                h.onEnded?("rejoin-abandoned")
            }
            var order: [String] = []
            for await event in stream {
                switch event {
                case .failure: order.append("failure")
                case .stateChange(.ended): order.append("state-ended")
                case .ended: order.append("ended")
                default: break
                }
            }
            let expected = errorFirst ? ["failure", "state-ended", "ended"] : ["state-ended", "failure", "ended"]
            if !order.contains("failure") { lost += 1 } else if order != expected { misordered.append(order) }
        }
        XCTAssertEqual(lost, 0, "terminal error events lost")
        XCTAssertTrue(misordered.isEmpty, "misordered: \(misordered.prefix(3))")
    }

    /// open() 을 기다리는 사이 close() 됐다 — 끝난 클라이언트를 connected 로 되살리지 않는다.
    func testOpenDoesNotReviveAClientClosedWhileItWaited() async throws {
        let transport = GatedOpenTransport()
        let client = ConnectClient(transport: transport)
        let opening = Task { try await client.open() }
        let deadline = TestTiming.deadline(3)
        while !transport.waiting, Date() < deadline { try await Task.sleep(nanoseconds: 5_000_000) }
        await client.close()
        transport.release()
        do {
            try await opening.value
            XCTFail("open() should report that the session ended")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "SessionEnded")
            XCTAssertTrue(error.message.contains("client-closed"), error.message)
        }
        let state = await client.state
        XCTAssertEqual(state, .ended)
    }

    /// 합류 직후 세션이 끝났다(autoRejoin 을 끈 어댑터의 stage:ended · 서버 끊김) — open() 의 오류가 그 이유를 싣는다.
    func testOpenNamesTheReasonWhenTheSessionEndsRightAfterJoining() async throws {
        let transport = EndsRightAfterJoinTransport()
        let client = ConnectClient(transport: transport)
        do {
            try await client.open()
            XCTFail("open() should report that the session ended")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "SessionEnded")
            XCTAssertTrue(error.message.contains("server-disconnect"), error.message)
        }
    }

    /// 합류를 돌려주기 직전에 세션 종료(상태 · 이유)를 알리는 트랜스포트.
    private final class EndsRightAfterJoinTransport: Transport, @unchecked Sendable {
        func open(handlers: TransportHandlers) async throws -> Joined {
            handlers.onState?(.ended)
            handlers.onEnded?("server-disconnect")
            return Joined(roomId: "stage-1", me: Member(participantId: "me-1", displayName: "Me"))
        }
        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult { PublishResult(ok: true) }
        func listMembers() async throws -> [Member] { [] }
        func close() async {}
    }

    /// 합류 응답을 테스트가 풀어 줄 때까지 붙잡는 트랜스포트.
    private final class GatedOpenTransport: Transport, @unchecked Sendable {
        private let lock = NSLock()
        private var gate: CheckedContinuation<Void, Never>?
        private var released = false
        var waiting: Bool { lock.withLock { gate != nil } }
        func release() {
            let g: CheckedContinuation<Void, Never>? = lock.withLock {
                released = true
                defer { gate = nil }
                return gate
            }
            g?.resume()
        }
        func open(handlers: TransportHandlers) async throws -> Joined {
            await withCheckedContinuation { (c: CheckedContinuation<Void, Never>) in
                let now: Bool = lock.withLock {
                    if released { return true }
                    gate = c
                    return false
                }
                if now { c.resume() }
            }
            return Joined(roomId: "stage-1", me: Member(participantId: "me-1", displayName: "Me"))
        }
        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult { PublishResult(ok: true) }
        func listMembers() async throws -> [Member] { [] }
        func close() async {}
    }

    // ── 원격 조작 코어 (§6-5·§6-2 — T8) ─────────────────────────────────────

    func testRemoteInputHolderNoticeReachesSubscribers() async throws {
        let (transport, client) = try await makeClient()
        let seen = Seen()
        let sub = client.onRemoteInputHolder { payload in
            seen.append(payload["holderParticipantId"] as? String ?? "-")
        }
        transport.handlers.onAdapterLeaseHolder?(["holderParticipantId": "v-1"])
        try await waitFor { seen.values == ["v-1"] }
        sub.cancel() // 해제는 그 자리에서 반영된다(잠금 아래 목록에서 뺀다)
        transport.handlers.onAdapterLeaseHolder?(["holderParticipantId": "v-2"])
        try await Task.sleep(nanoseconds: 50_000_000)
        XCTAssertEqual(seen.values, ["v-1"])
    }

    func testStageIdComesFromJoin() async throws {
        let (_, client) = try await makeClient()
        let stageId = await client.stageId
        XCTAssertEqual(stageId, "stage-1")
    }

    func testRegistrationResultDescriptionRedactsTheSecret() {
        let result = AdapterRegistrationResult(registrationId: "reg-1", deviceSecret: "super-secret")
        XCTAssertFalse("\(result)".contains("super-secret"))
        XCTAssertTrue("\(result)".contains("<redacted>"))
    }

    func testRegistrationIsNotSupportedWithoutSocketSurface() async throws {
        let (_, client) = try await makeClient()
        do {
            _ = try await client.submitRegistrationCode("CODE1")
            XCTFail("expected NotSupported")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "NotSupported")
        }
    }
    // ── client.ping() 자체 ───────────────────────────────────────────────────
    // measureLatency 는 fake pinger 로 덮이지만 **ping() 본체**(상태 게이트 +
    // listMembers 왕복)는 그것과 별개다. 비어 있으면 조용히 회귀한다.

    func testPingRequiresALiveConnection() async {
        let client = ConnectClient(transport: FakeTransport())
        do {
            _ = try await client.ping()
            XCTFail("expected NotConnected")
        } catch let err as AdapterError {
            XCTAssertEqual(err.code, "NotConnected")
        } catch { XCTFail("wrong error: \(error)") }
    }

    func testPingTimesListMembersWithoutPublishing() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport)
        try await client.open()
        let ms = try await client.ping()
        XCTAssertGreaterThanOrEqual(ms, 0)
        // 부작용 없음이 이 프리미티브의 계약이다 — 발행으로 재면 Console 이 오염된다.
        XCTAssertTrue(transport.published.isEmpty)
    }

    // ── latest() 상한 — 최근에 갱신된 1000채널(가장 오래 갱신되지 않은 것부터 잊는다) ──────────────

    /// 채널 `ch<i>` 에 값 i · timestamp 1000+i 를 차례로 보낸다.
    private func fill(_ transport: FakeTransport, _ range: Range<Int>) {
        for i in range { transport.push("ch\(i)", .int(i), ts: Int64(1000 + i)) }
    }

    /// 그 채널의 latest 가 그 값이 될 때까지 — 접기는 도착 순서라 마지막 것이 닿았으면 앞의 것도 다 닿았다.
    private func waitForLatest(
        _ client: ConnectClient, _ channel: String, _ expected: Primitive, timeout: TimeInterval = 5
    ) async throws {
        let deadline = TestTiming.deadline(timeout)
        while await client.latest(channel) != expected, Date() < deadline {
            try await Task.sleep(nanoseconds: 2_000_000)
        }
        let actual = await client.latest(channel)
        XCTAssertEqual(actual, expected, "\(channel) 이 접히지 않았다")
    }

    func testLatestKeepsOnlyTheThousandMostRecentlyUpdatedChannels() async throws {
        XCTAssertEqual(ConnectClient.latestLimit, 1000)
        let (transport, client) = try await makeClient()
        fill(transport, 0..<1500)
        try await waitForLatest(client, "ch1499", 1499)
        let count = await client.latestCount
        XCTAssertEqual(count, 1000)
        for i in [0, 1, 250, 499] {
            let gone = await client.latest("ch\(i)")
            XCTAssertNil(gone, "ch\(i) 은 잊혀야 한다")
        }
        let oldestKept = await client.latest("ch500")
        XCTAssertEqual(oldestKept, 500)
    }

    func testAnUpdatedChannelMovesToTheRecentEnd() async throws {
        let (transport, client) = try await makeClient()
        fill(transport, 0..<1000)
        transport.push("ch0", "fresh", ts: 5000)
        transport.push("new", 1, ts: 5001)
        try await waitForLatest(client, "new", 1)
        let kept = await client.latest("ch0")
        let evicted = await client.latest("ch1") // 가장 오래 갱신되지 않은 것
        let count = await client.latestCount
        XCTAssertEqual(kept, "fresh")
        XCTAssertNil(evicted)
        XCTAssertEqual(count, 1000)
    }

    func testAStaleValueRejectedByTimestampDoesNotRefreshTheChannel() async throws {
        let (transport, client) = try await makeClient()
        fill(transport, 0..<1000)
        transport.push("ch0", "stale", ts: 1) // ch0 의 1000 보다 낡았다 — 거절
        transport.push("new", 1, ts: 5001)
        try await waitForLatest(client, "new", 1)
        let evicted = await client.latest("ch0") // 갱신된 적이 없으니 맨 먼저 잊힌다
        let kept = await client.latest("ch1")
        XCTAssertNil(evicted)
        XCTAssertEqual(kept, 1)
    }

    func testOwnPublishCountsAsAnUpdate() async throws {
        let (transport, client) = try await makeClient()
        fill(transport, 0..<1000)
        try await waitForLatest(client, "ch999", 999)
        try await client.send("ch0", "mine")
        transport.push("new", 1, ts: Int64(Date().timeIntervalSince1970 * 1000) + 1)
        try await waitForLatest(client, "new", 1)
        let kept = await client.latest("ch0")
        let evicted = await client.latest("ch1")
        XCTAssertEqual(kept, "mine")
        XCTAssertNil(evicted)
    }

    func testAThousandChannelSnapshotSeedsAThousand() async throws {
        let (transport, client) = try await makeClient()
        fill(transport, 0..<1000)
        transport.pushHistory((0..<1000).map { h("h\($0)", .int($0), Int64(100 + $0)) })
        try await waitForLatest(client, "h999", 999)
        let count = await client.latestCount
        let first = await client.latest("h0")
        let live = await client.latest("ch999") // 스냅샷은 권위 — 라이브로 접힌 값은 비운다
        XCTAssertEqual(count, 1000)
        XCTAssertEqual(first, 0)
        XCTAssertNil(live)
    }

    /// 연결 리스트 구현을 단순한 모델(가장 오래된 것부터 늘어선 배열)과 견준다 — 갱신 · 거절 · 축출 · 비우기가
    /// 섞인 결정적 무작위 2만 단계에서 채널 순서와 값이 같아야 한다.
    func testLatestCacheMatchesAReferenceModel() {
        var rng = SplitMix64(seed: 0xC0C)
        let limit = 8
        var cache = LatestCache(limit: limit)
        var model: [(channel: String, value: Primitive, ts: Int64)] = []
        for step in 0..<20_000 {
            if Int.random(in: 0..<500, using: &rng) == 0 {
                cache.removeAll()
                model.removeAll()
                continue
            }
            let channel = "c\(Int.random(in: 0..<20, using: &rng))"
            let ts = Int64.random(in: 0..<60, using: &rng)
            cache.fold(channel, .int(step), ts)
            if let i = model.firstIndex(where: { $0.channel == channel }) {
                if model[i].ts <= ts {
                    model.remove(at: i)
                    model.append((channel, .int(step), ts))
                }
            } else {
                model.append((channel, .int(step), ts))
                if model.count > limit { model.removeFirst() }
            }
            guard cache.channelsOldestFirst == model.map(\.channel) else {
                return XCTFail("step \(step): \(cache.channelsOldestFirst) ≠ \(model.map(\.channel))")
            }
            for entry in model where cache[entry.channel] != entry.value {
                return XCTFail("step \(step): \(entry.channel) = \(String(describing: cache[entry.channel]))")
            }
        }
        XCTAssertEqual(cache.count, model.count)
    }

    /// 인박스가 콜백을 건너뛴 메시지의 접기를 모아 한 번에 합쳐도(``LatestCache/absorb(_:)``) 하나씩 접은 것과
    /// 같다 — timestamp 가 도착 순서대로 늘면(서버 도장) 양쪽이 잊는 채널이 있어도, 낡은 값 거절이 섞이면 잊는
    /// 채널이 없을 때. (거절과 잊기가 한 창에 겹치면 다를 수 있다 — `skipped` 주석.)
    func testAbsorbingCollectedFoldsEqualsFoldingThemOneByOne() {
        var rng = SplitMix64(seed: 0xF01D)
        for round in 0..<400 {
            let monotonic = round % 2 == 1
            let channels = monotonic ? 20 : 6 // 거절이 섞이는 판은 한도(8) 안의 채널만
            var ts: Int64 = 0
            func nextTs() -> Int64 {
                if monotonic { ts += 1; return ts }
                return Int64.random(in: 0..<60, using: &rng)
            }
            var direct = LatestCache(limit: 8)
            for _ in 0..<Int.random(in: 0..<30, using: &rng) {
                direct.fold("c\(Int.random(in: 0..<channels, using: &rng))", .int(-1), nextTs())
            }
            var absorbed = direct
            var collected = LatestCache(limit: 8)
            for step in 0..<Int.random(in: 0..<60, using: &rng) {
                let channel = "c\(Int.random(in: 0..<channels, using: &rng))"
                let stamp = nextTs()
                direct.fold(channel, .int(step), stamp)
                collected.fold(channel, .int(step), stamp)
            }
            absorbed.absorb(collected)
            guard absorbed.channelsOldestFirst == direct.channelsOldestFirst else {
                return XCTFail("round \(round): \(absorbed.channelsOldestFirst) ≠ \(direct.channelsOldestFirst)")
            }
            for channel in direct.channelsOldestFirst where absorbed[channel] != direct[channel] {
                return XCTFail("round \(round): \(channel)")
            }
        }
    }

    // ── 수신 순서 — 콜백은 트랜스포트가 알린 순서 그대로 ─────────────────────────────────────

    /// 2만 건을 소켓 큐처럼 한 직렬 큐에서 밀어 넣으면 콜백도 그 순서다. 종전에는 메시지마다 Task 를 띄워 액터
    /// 도착 순서가 뒤섞였다(실측: 인접 역전 680~1585). 받는 쪽은 인박스 상한(1000) 아래에서 밀어 넣는다 — 실
    /// 소켓처럼 — 건너뛰기 없이 순서만 잰다.
    func testTwentyThousandMessagesReachCallbacksInArrivalOrder() async throws {
        let (transport, client) = try await makeClient()
        let seen = Recorder<Int>()
        await client.on("order.*") { message in
            if case .int(let value) = message.value { seen.append(value) }
        }
        guard let onMessage = transport.handlers.onMessage else { return XCTFail("no message handler") }
        let total = 20_000
        DispatchQueue(label: "fake-socket-queue").async {
            for i in 0..<total {
                while client.inboxMessageCount > 500 { usleep(50) }
                onMessage(StageMessage(
                    channel: "order.x", value: .int(i), sourceParticipantId: "peer-1", timestamp: Int64(i)))
            }
        }
        try await waitFor(timeout: 30) { seen.count == total }
        let all = seen.all
        let inversions = zip(all, all.dropFirst()).filter { $0 > $1 }.count
        XCTAssertEqual(inversions, 0, "콜백이 도착 순서와 다르게 불렸다")
        XCTAssertEqual(all, Array(0..<total))
    }

    /// 스냅샷 바로 뒤에 온 라이브 값은 그 스냅샷에 지워지지 않는다 — 둘이 한 줄에 서야 한다(종전에는 메시지가
    /// 따로 Task 라 스냅샷보다 먼저 접히면 재시드가 지웠다).
    func testALiveValueRightAfterASnapshotSurvivesIt() async throws {
        let (transport, client) = try await makeClient()
        let delivered = Recorder<Int>()
        await client.on("live.x") { message in
            if case .int(let value) = message.value { delivered.append(value) }
        }
        var erased: [Int] = []
        for round in 0..<300 {
            transport.pushHistory([h("snap", .int(round), 1)])
            transport.push("live.x", .int(round), ts: Int64(10_000 + round))
            // 둘 다 처리됐다 — 배달 방식과 무관한 두 신호(스냅샷이 history 에, 메시지가 콜백에).
            try await waitFor { delivered.count == round + 1 }
            let deadline = TestTiming.deadline(3)
            while await client.history().first?.value != .int(round), Date() < deadline {
                try await Task.sleep(nanoseconds: 1_000_000)
            }
            if await client.latest("live.x") != .int(round) { erased.append(round) }
        }
        XCTAssertEqual(erased, [], "스냅샷이 그 뒤에 온 라이브 값을 지웠다")
    }

    /// 종료보다 먼저 온 메시지는 구독을 놓기 전에 배달된다.
    func testAMessageThatArrivesBeforeTheEndReachesItsCallback() async throws {
        var lost = 0
        for _ in 0..<200 {
            let (transport, client) = try await makeClient()
            let delivered = Recorder<Int>()
            await client.on("last.word") { _ in delivered.append(1) }
            transport.push("last.word", 1)
            transport.end("host-closed")
            try await waitForState(client, .ended)
            if delivered.count != 1 { lost += 1 }
        }
        XCTAssertEqual(lost, 0, "종료 직전에 온 메시지가 콜백에 닿지 않았다")
    }

    /// 시그널링 리스너도 도착 순서 그대로 받는다(종전: 프레임마다 Task — 2만 건에 인접 역전 350~396).
    func testSignalingListenersSeeFramesInArrivalOrder() async throws {
        let (transport, client) = try await makeClient()
        let seen = Recorder<Int>()
        _ = await client.onSignaling { frame in
            if let n = frame["n"] as? Int { seen.append(n) }
        }
        guard let onSignaling = transport.handlers.onSignaling else { return XCTFail("no signaling handler") }
        let total = 20_000
        DispatchQueue(label: "fake-socket-queue").async {
            for i in 0..<total { onSignaling(["kind": "ice", "n": i]) }
        }
        try await waitFor(timeout: 30) { seen.count == total }
        let all = seen.all
        XCTAssertEqual(zip(all, all.dropFirst()).filter { $0 > $1 }.count, 0, "시그널링이 도착 순서와 다르게 닿았다")
        XCTAssertEqual(all, Array(0..<total))
    }

    /// 같은 참여자의 join 뒤 leave 는 `events()` 에도 그 순서로 나간다(종전: 이벤트마다 Task 라 뒤집힐 수 있었다 —
    /// 떠난 참여자가 마지막에 "들어왔다" 로 남는다). 한 번으로는 드물어 여러 번 잰다.
    func testAJoinThenLeaveReachEventsInThatOrder() async throws {
        let (transport, client) = try await makeClient()
        let seen = Recorder<String>()
        let stream = await client.events()
        let reader = Task {
            for await event in stream {
                switch event {
                case .memberJoin(let member): seen.append("join:\(member.participantId)")
                case .memberLeave(let id): seen.append("leave:\(id)")
                default: break
                }
            }
        }
        let h = transport.handlers
        let socketQueue = DispatchQueue(label: "fake-socket-queue")
        var reversed = 0
        for round in 0..<500 {
            let id = "p-\(round)"
            socketQueue.async {
                h.onMemberJoin?(Member(participantId: id, displayName: "viewer"))
                h.onMemberLeave?(id)
            }
            let deadline = TestTiming.deadline(3)
            while seen.count < 2 * (round + 1), Date() < deadline { try await Task.sleep(nanoseconds: 500_000) }
            if Array(seen.all.suffix(2)) != ["join:\(id)", "leave:\(id)"] { reversed += 1 }
        }
        XCTAssertEqual(reversed, 0, "leave 가 join 보다 먼저 나갔다")
        reader.cancel()
        await client.close()
    }

    // ── 콜백이 밀릴 때 — 인박스 상한 1000, 넘치면 가장 오래된 콜백을 건너뛰고 latest 는 정확하게 ────────────

    /// 첫 `gate` 메시지의 콜백이 `release` 를 받을 때까지 액터를 붙잡는 클라이언트. 구독은 `subscribe` 에서 —
    /// 돌려받은 뒤에는 release 전까지 client 를 await 하면 안 된다(액터가 붙잡혀 있다).
    private func blockedClient(
        _ subscribe: (ConnectClient) async -> Void = { _ in }
    ) async throws -> (FakeTransport, ConnectClient, DispatchSemaphore) {
        let (transport, client) = try await makeClient()
        await subscribe(client)
        let entered = Box(false)
        let release = DispatchSemaphore(value: 0)
        await client.on("gate") { _ in
            entered.value = true
            _ = release.wait(timeout: .now() + TestTiming.farAway)
        }
        transport.push("gate", 1)
        try await waitFor { entered.value }
        return (transport, client, release)
    }

    func testASlowCallbackSkipsTheOldestCallbacksButLatestStaysExact() async throws {
        XCTAssertEqual(ConnectClient.inboundLimit, 1000)
        let flood = Recorder<Int>()
        let door = Recorder<Int>()
        let (transport, client, release) = try await blockedClient { client in
            await client.on("flood") { message in
                if case .int(let value) = message.value { flood.append(value) }
            }
            await client.on("door.state") { _ in door.append(1) }
        }
        // ⚠️ 여기부터 release 전까지 client 를 await 하지 않는다 — 액터가 붙잡혀 있다.
        for i in 0..<10 { transport.push("flood", .int(i), ts: Int64(1000 + i)) }
        transport.push("door.state", "open", ts: 1) // 드물게 오는 상태 채널 — 넘쳐 건너뛰어진다
        for i in 10..<1500 { transport.push("flood", .int(i), ts: Int64(1000 + i)) }
        XCTAssertEqual(client.inboxMessageCount, 1000, "미배달 메시지가 상한을 넘었다")
        release.signal()

        try await waitFor { flood.count == 1000 }
        try await waitForLatest(client, "flood", 1499)
        XCTAssertEqual(flood.all, Array(500..<1500), "가장 새 1000건이 순서대로 배달돼야 한다")
        XCTAssertEqual(door.count, 0, "건너뛴 메시지의 콜백이 불렸다")
        let doorState = await client.latest("door.state")
        XCTAssertEqual(doorState, "open", "콜백을 건너뛴 메시지가 latest() 에 접히지 않았다")
        try await waitFor { client.diagnostics().contains { $0.message.contains("caught up") } }
        let notes = client.diagnostics().filter { $0.code == "queue-overflow" }.map(\.message)
        XCTAssertEqual(notes.count, 2, "\(notes)")
        XCTAssertTrue(notes.first?.contains("1000 messages behind") == true, "\(notes)")
        XCTAssertTrue(notes.last?.contains("501 message callback(s) skipped") == true, "\(notes)")
    }

    /// 건너뛴 메시지의 접기는 제자리에서 합친다 — 아직 적용되지 않은 스냅샷보다 뒤에 온 것은 그 스냅샷 **뒤에**
    /// (먼저 합치면 재시드가 지운다), 앞에 온 것은 합치지 않는다(스냅샷이 덮은 값을 되살리면 안 된다).
    func testSkippedFoldsLandInTheirPlaceAroundAPendingSnapshot() async throws {
        // ① 스냅샷 → door → 넘침: door 는 스냅샷 뒤에 접혀 남는다.
        do {
            let (transport, client, release) = try await blockedClient()
            transport.pushHistory([h("snap.only", 7, 1)])
            transport.push("door.state", "open", ts: 5)
            for i in 0..<1000 { transport.push("flood", .int(i), ts: Int64(1000 + i)) }
            release.signal()
            try await waitForLatest(client, "flood", 999)
            let door = await client.latest("door.state")
            let snap = await client.latest("snap.only")
            XCTAssertEqual(door, "open", "스냅샷 뒤에 온 메시지의 접기가 스냅샷에 지워졌다")
            XCTAssertEqual(snap, 7)
        }
        // ② door → 스냅샷 → 넘침: door 는 스냅샷이 덮었다.
        do {
            let (transport, client, release) = try await blockedClient()
            transport.push("door.state", "open", ts: 5)
            transport.pushHistory([h("snap.only", 7, 1)])
            for i in 0..<1000 { transport.push("flood", .int(i), ts: Int64(1000 + i)) }
            release.signal()
            try await waitForLatest(client, "flood", 999)
            let door = await client.latest("door.state")
            let snap = await client.latest("snap.only")
            XCTAssertNil(door, "스냅샷보다 먼저 온 메시지의 접기가 스냅샷 뒤에 되살아났다")
            XCTAssertEqual(snap, 7)
        }
    }

    /// 콜백을 건너뛴 메시지의 접기는 그 자리에서 합친다 — 같은 ms 의 두 값이면 뒤에 온 값이 남아야 한다(합치기를 줄이
    /// 빈 뒤로 미루면 옛 값이 새 값을 덮었다).
    func testASkippedFoldDoesNotOverwriteALaterSameMillisecondValue() async throws {
        let (transport, client, release) = try await blockedClient()
        transport.push("door", "old", ts: 5) // 넘쳐 건너뛰어진다
        for i in 0..<999 { transport.push("flood", .int(i), ts: 6) }
        transport.push("door", "new", ts: 5)
        release.signal()
        try await waitForLatest(client, "flood", 998)
        let deadline = TestTiming.deadline(3)
        while client.inboxMessageCount > 0, Date() < deadline { try await Task.sleep(nanoseconds: 2_000_000) }
        let door = await client.latest("door")
        XCTAssertEqual(door, "new", "건너뛴 옛 값이 뒤에 온 같은 ms 의 값을 덮었다")
    }

    /// 인박스를 비우는 드레인은 64건마다 액터를 내준다 — 콜백이 다음 메시지를 부르는 동안(인박스가 비지 않는다)
    /// 에도 다른 호출이 끝까지 기다리지 않는다.
    func testALongDeliveryRunLetsOtherCallsIn() async throws {
        let (transport, client) = try await makeClient()
        guard let onMessage = transport.handlers.onMessage else { return XCTFail("no message handler") }
        let total = 100_000
        let delivered = Recorder<Int>()
        let deliveredAtRead = Box<Int?>(nil)
        await client.on("feed") { message in
            delivered.append(1)
            let n = delivered.count
            if n == 1 {
                Task.detached(priority: .medium) {
                    _ = await client.state
                    deliveredAtRead.value = delivered.count
                }
            }
            if n < total {
                onMessage(StageMessage(channel: "feed", value: .int(n), sourceParticipantId: "peer-1", timestamp: 1))
            }
        }
        // 드레인 태스크와 위 읽기가 같은 우선순위로 줄을 서게 한다(액터는 우선순위가 높은 일을 먼저 돌린다).
        await Task.detached(priority: .medium) { transport.push("feed", 0) }.value
        try await waitFor(timeout: 60) { delivered.count == total && deliveredAtRead.value != nil }
        let at = deliveredAtRead.value ?? total
        XCTAssertLessThan(at, total / 2, "다른 호출이 배달이 끝날 때까지 액터를 얻지 못했다(\(at))")
        await client.close() // 콜백이 client 를 잡고 있다 — 구독을 놓아 순환을 끊는다
    }
}

/// 콜백이 받은 값을 차례로 모은다 — `Box` 의 배열은 덧붙일 때마다 통째로 복사한다(2만 건이면 O(n²)).
final class Recorder<T>: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [T] = []
    func append(_ item: T) { lock.withLock { items.append(item) } }
    var count: Int { lock.withLock { items.count } }
    var all: [T] { lock.withLock { items } }
}

/// 결정적 난수 — 실패가 같은 순서로 재현되게.
struct SplitMix64: RandomNumberGenerator {
    private var state: UInt64
    init(seed: UInt64) { state = seed }
    mutating func next() -> UInt64 {
        state &+= 0x9E37_79B9_7F4A_7C15
        var z = state
        z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
        z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
        return z ^ (z >> 31)
    }
}

/// 테스트에서 클로저 밖으로 값을 모으기 위한 잠금 박스.
final class Box<T>: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: T

    init(_ value: T) { storage = value }

    var value: T {
        get { lock.withLock { storage } }
        set { lock.withLock { storage = newValue } }
    }
}

extension NSLock {
    func withLock<T>(_ body: () throws -> T) rethrows -> T {
        lock()
        defer { unlock() }
        return try body()
    }
}

/// 스레드 안전 문자열 기록기 — 보유자 통지 리스너(@Sendable)가 쓴다.
private final class Seen: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [String] = []
    var values: [String] { lock.withLock { storage } }
    func append(_ value: String) { lock.withLock { storage.append(value) } }
}
