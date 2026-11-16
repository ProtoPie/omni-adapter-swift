import XCTest
@testable import OmniAdapter

/// replay / simulate — TS playback.test.ts 미러. 클라이언트는 MemoryStage 위의 실제 ConnectClient.
final class PlaybackTests: XCTestCase {
    private let rec: [RecordedMessage] = [
        RecordedMessage(ts: 1000, channel: "a.x", value: .int(1), from: "p1"),
        RecordedMessage(ts: 1250, channel: "a.y", value: .string("two"), from: "p1"),
        RecordedMessage(ts: 1250, channel: "a.x", value: .bool(true)),
        RecordedMessage(ts: 2250, channel: "a.x", value: .null),
    ]

    private final class Waits: @unchecked Sendable {
        private let lock = NSLock(); private var items: [Double] = []
        var value: [Double] { lock.lock(); defer { lock.unlock() }; return items }
        func add(_ ms: Double) { lock.lock(); items.append((ms * 1000).rounded() / 1000); lock.unlock() }
    }

    private func harness() async throws -> (MemoryStage, ConnectClient, Waits, @Sendable (Double) async -> Void) {
        let stage = MemoryStage()
        let client = try await stage.join(MemoryJoinOptions(displayName: "player"))
        let waits = Waits()
        return (stage, client, waits, { ms in waits.add(ms) })
    }
    /// 고정 대기 — 뒤에 긍정 결과를 단언하지 않는다(TestTiming).
    private func settle() async { try? await Task.sleep(nanoseconds: 60_000_000) }

    /// 클라이언트의 단조 시계를 바꿔 끼운다 — 진척 없는 시간(15초)은 실제 단조 시계로 재므로, 시험은 이 시계를 앞으로 돌려 15초를
    /// 기다리지 않는다. 주입한 wait 는 발행 사이 간격에만 쓰인다.
    private final class FakeClock: @unchecked Sendable {
        private let lock = NSLock()
        private var nanos: UInt64 = 1_000
        func now() -> UInt64 { lock.withLock { nanos } }
        func advance(_ seconds: Double) { lock.withLock { nanos += UInt64(seconds * 1e9) } }
    }

    private func fakeClock(_ client: ConnectClient) async -> FakeClock {
        let clock = FakeClock()
        await client.setMonotonicClockForTest { clock.now() }
        return clock
    }

    /// 실행이 끝났는가 — Task 에는 그것을 묻는 자리가 없다.
    private final class Done: @unchecked Sendable {
        private let lock = NSLock()
        private var done = false
        func set() { lock.withLock { done = true } }
        var value: Bool { lock.withLock { done } }
    }

    /// 기한 안에 끝나기를 기다린다 — 결함(풀리지 않는 기다림)이 있으면 매달리지 않고 실패한다: 기한이 지나면 작업을 취소하고 던진다.
    /// 기한은 TestTiming.hangGuard 이상이다(끝나면 곧바로 돌아온다).
    private func bounded<T: Sendable>(_ seconds: Double = 10, _ operation: @escaping @Sendable () async throws -> T) async throws -> T {
        let seconds = TestTiming.atLeastHangGuard(seconds)
        let done = Done()
        let task = Task { () async throws -> T in
            defer { done.set() }
            return try await operation()
        }
        guard await until(seconds, { done.value }) else {
            task.cancel()
            XCTFail("did not complete within \(seconds) s")
            throw AdapterError("Timeout", "bounded wait")
        }
        return try await task.value
    }

    /// 조건이 참이 될 때까지 — 시한은 TestTiming.hangGuard 이상이다. "참이 되지 않는다"를 보는 자리에는 쓰지 않는다.
    private func until(_ seconds: Double = 3, _ condition: () async -> Bool) async -> Bool {
        await TestTiming.until(seconds, pollNanos: 10_000_000, condition)
    }

    func testParseAndFormatRoundTrip() throws {
        let text = rec.map { r -> String in
            var o: [String: Any] = ["ts": r.ts, "channel": r.channel, "value": r.value.jsonValue]
            if let f = r.from { o["from"] = f }
            return String(decoding: try! JSONSerialization.data(withJSONObject: o), as: UTF8.self)
        }.joined(separator: "\n") + "\n\n"
        XCTAssertEqual(try parseRecording(text), rec)
        let line = formatRecorded(StageMessage(channel: "a.x", value: .int(1), sourceParticipantId: "p1", timestamp: 1000))
        let obj = try JSONSerialization.jsonObject(with: line.data(using: .utf8)!) as! [String: Any]
        XCTAssertEqual(obj["ts"] as? Int, 1000); XCTAssertEqual(obj["channel"] as? String, "a.x")
        XCTAssertEqual(obj["value"] as? Int, 1); XCTAssertEqual(obj["from"] as? String, "p1")
        XCTAssertEqual(try parseRecording(line), [RecordedMessage(ts: 1000, channel: "a.x", value: .int(1), from: "p1")])
    }

    func testParseRejectsBadLinesWithLineNumbers() {
        XCTAssertThrowsError(try parseRecording("{\"ts\":1,\"channel\":\"a\",\"value\":1}\nnot json")) { XCTAssertTrue("\($0)".contains("line 2 is not JSON")) }
        XCTAssertThrowsError(try parseRecording("{\"channel\":\"a\",\"value\":1}")) { XCTAssertTrue("\($0)".contains("line 1: \"ts\"")) }
        XCTAssertThrowsError(try parseRecording("{\"ts\":1,\"channel\":\"\",\"value\":1}")) { XCTAssertTrue("\($0)".contains("\"channel\"")) }
        XCTAssertThrowsError(try parseRecording("{\"ts\":1,\"channel\":\"a\",\"value\":{\"x\":1}}")) { XCTAssertTrue("\($0)".contains("\"value\"")) }
        // 선두 BOM 은 벗기고, 최상위 스칼라/배열은 "ts" 오류다 — 여섯 언어 공통
        XCTAssertEqual(try parseRecording("\u{FEFF}{\"ts\":1,\"channel\":\"a\",\"value\":1}").count, 1)
        XCTAssertThrowsError(try parseRecording("42")) { XCTAssertTrue("\($0)".contains("line 1: \"ts\"")) }
        XCTAssertThrowsError(try parseRecording("[1,2]")) { XCTAssertTrue("\($0)".contains("line 1: \"ts\"")) }
        // 거부는 전부 ConfigError 다 — 코드로 분기하는 호출자가 있다.
        XCTAssertThrowsError(try parseRecording("{\"channel\":\"a\"}")) {
            XCTAssertEqual(($0 as? AdapterError)?.code, "ConfigError")
        }
        // 채널·값 오류도 **줄 번호**를 단다 — 종전엔 ts/not-JSON 만 고정돼 있었다.
        XCTAssertThrowsError(try parseRecording("{\"ts\":1,\"channel\":\"a\",\"value\":1}\n{\"ts\":2,\"channel\":\"\",\"value\":1}")) {
            XCTAssertTrue("\($0)".contains("line 2: \"channel\""), "\($0)")
        }
        XCTAssertThrowsError(try parseRecording("{\"ts\":1,\"channel\":\"a\",\"value\":1}\n{\"ts\":2,\"channel\":\"b\",\"value\":[1]}")) {
            XCTAssertTrue("\($0)".contains("line 2: \"value\""), "\($0)")
        }
    }

    func testReplayHonorsGapsScaledBySpeed() async throws {
        let (stage, client, waits, wait) = try await harness()
        let r = try await replay(client: client, records: rec, options: ReplayOptions(speed: 2, wait: wait))
        XCTAssertEqual(r, ReplayResult(sent: 4, loops: 1))
        XCTAssertEqual(stage.published.map { "\($0.input.channel):\($0.input.value):\($0.ack)" },
                       ["a.x:int(1):false", "a.y:string(\"two\"):false", "a.x:bool(true):false", "a.x:null:false"])
        XCTAssertEqual(waits.value, [125, 500]) // 250/2, 0 (같은 ts → 대기 없음), 1000/2
    }

    func testReplayInfiniteSpeedLoopAndAck() async throws {
        let (stage, client, waits, wait) = try await harness()
        let r = try await bounded {
            try await replay(client: client, records: Array(self.rec.prefix(2)), options: ReplayOptions(speed: .infinity, loop: 3, ack: true, wait: wait))
        }
        XCTAssertEqual(r, ReplayResult(sent: 6, loops: 3))
        XCTAssertTrue(waits.value.isEmpty)
        XCTAssertTrue(stage.published.allSatisfy(\.ack))
    }

    /// E3: 큐에 남은 fire-and-forget 은 close() 가 버린다 — 다 보낸 뒤 큐가 빌 때까지 기다렸다 돌아와야 "보냈다" 가 참이다.
    /// 끊긴 채 보내 큐에 쌓고, 잠시 뒤 다시 붙인다. 아무것도 안 하는 wait 여도 큐 비우기는 실제로 기다린다(wait 는 간격 전용).
    func testReplayAndSimulateReturnOnlyAfterTheQueueDrains() async throws {
        let (stage, client, _, wait) = try await harness()
        try await stage.disconnect(client)
        try await waitUntilState(client, .reconnecting)
        let done = Done()
        let replaying = Task {
            defer { done.set() }
            return try await replay(client: client, records: rec, options: ReplayOptions(speed: .infinity, wait: wait))
        }
        try await Task.sleep(nanoseconds: 150_000_000)
        XCTAssertFalse(done.value, "returned before the queue drained")
        try await stage.reconnect(client)
        let r = try await replaying.value
        XCTAssertEqual(r.sent, 4)
        let left = await client.queuedCount
        XCTAssertEqual(left, 0)
        XCTAssertEqual(stage.published.count, 4)

        try await stage.disconnect(client)
        try await waitUntilState(client, .reconnecting)
        let done2 = Done()
        let simulating = Task {
            defer { done2.set() }
            return try await simulate(client: client, channel: "s.x", options: SimulateOptions(waveform: "ramp", hz: 1000, count: 3, wait: wait))
        }
        try await Task.sleep(nanoseconds: 150_000_000)
        XCTAssertFalse(done2.value, "returned before the queue drained")
        try await stage.reconnect(client)
        let s = try await simulating.value
        XCTAssertEqual(s.values.count, 3)
        let left2 = await client.queuedCount
        XCTAssertEqual(left2, 0)
        XCTAssertEqual(stage.published.count, 7)
    }

    /// 끝내 비지 않으면(끊긴 채 · 대기) 진척 없는 15초까지만 기다리고 돌아온다 — 실제 단조 시계로 잰다(여기선 그 시계를 돌린다).
    /// 종전에는 주입한 wait 의 호출 수로 세서, 아무것도 안 하는 wait 면 곧바로 돌아왔다.
    func testTheDrainWaitIsBoundedAt15SecondsOfTheMonotonicClock() async throws {
        let (stage, client, _, wait) = try await harness()
        let clock = await fakeClock(client)
        try await stage.disconnect(client)
        try await waitUntilState(client, .reconnecting)
        let done = Done()
        let replaying = Task {
            defer { done.set() }
            return try await replay(client: client, records: Array(rec.prefix(1)), options: ReplayOptions(wait: wait))
        }
        let draining = await until { runningDrainsForTest.value > 0 }  // 큐 비우기가 기준 시각을 잡았다
        XCTAssertTrue(draining, "the drain did not start")
        clock.advance(14.9)
        try await Task.sleep(nanoseconds: 200_000_000)
        XCTAssertFalse(done.value, "the drain gave up before 15 s without progress")
        clock.advance(0.2)
        let returned = await until { done.value }
        XCTAssertTrue(returned, "the drain did not give up after 15 s")
        guard returned else { replaying.cancel(); return }  // 결함이 있으면 매달리지 않고 실패한다
        let r = try await replaying.value
        XCTAssertEqual(r.sent, 1)
        let left = await client.queuedCount
        XCTAssertEqual(left, 1)
    }

    /// ack 에 정해진 시간 뒤 성공으로 답하는 트랜스포트 — 끊긴 채로 시작할 수 있다(핸들러로 상태를 민다).
    private final class SlowAckTransport: Transport, @unchecked Sendable {
        private let ackNanos: UInt64
        init(ackNanos: UInt64 = 50_000_000) { self.ackNanos = ackNanos }
        private let lock = NSLock()
        private var handlersStorage = TransportHandlers()
        private var publishedStorage: [PublishInput] = []
        var handlers: TransportHandlers { lock.withLock { handlersStorage } }
        var published: [PublishInput] { lock.withLock { publishedStorage } }
        func open(handlers: TransportHandlers) async throws -> Joined {
            lock.withLock { handlersStorage = handlers }
            return Joined(roomId: "stage-1", me: Member(participantId: "me-1", displayName: "player"))
        }
        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult {
            lock.withLock { publishedStorage.append(input) }
            if ack { try? await Task.sleep(nanoseconds: ackNanos) }
            return PublishResult(ok: true)
        }
        func listMembers() async throws -> [Member] { [] }
        func close() async {}
    }

    /// 문서대로 아무것도 안 하는 wait 를 줬다(테스트가 실제로 기다리지 않게) — 진척 없는 시간을 그 호출 수로 세던 종전에는 큐에 든
    /// 발행 하나를 기다리는 사이 상한이 곧바로 차서 sent=0 으로 돌아왔다. 상한은 실제 단조 시계다. 끊긴 채 시작해(첫 발행이 큐에
    /// 든다) 100ms 뒤 붙고, ack 는 50ms 뒤에 온다.
    func testANoOpWaitDoesNotEndTheRunEarly() async throws {
        for kind in ["replay", "simulate"] {
            let transport = SlowAckTransport()
            let client = ConnectClient(transport: transport, exclusiveName: false)
            try await client.open()
            transport.handlers.onState?(.reconnecting)
            try await waitUntilState(client, .reconnecting)
            let reconnect = Task {
                try? await Task.sleep(nanoseconds: 100_000_000)
                transport.handlers.onState?(.connected)
            }
            let noWait: @Sendable (Double) async -> Void = { _ in }
            let sent: Int
            if kind == "replay" {
                let records = (1...5).map { RecordedMessage(ts: Double($0) * 100, channel: "w.x", value: .int($0)) }
                sent = try await bounded { try await replay(client: client, records: records, options: ReplayOptions(ack: true, wait: noWait)).sent }
            } else {
                sent = try await bounded {
                    try await simulate(
                        client: client, channel: "w.x",
                        options: SimulateOptions(waveform: "step", hz: 10, count: 5, steps: [.int(1), .int(2), .int(3), .int(4), .int(5)], ack: true, wait: noWait)
                    ).sent
                }
            }
            XCTAssertEqual(sent, 5, kind)
            XCTAssertEqual(transport.published.map { $0.value }, [.int(1), .int(2), .int(3), .int(4), .int(5)], "\(kind): 한 번씩, 순서대로")
            XCTAssertEqual(runningWatchdogsForTest.value, 0)
            await reconnect.value
            await client.close()
        }
    }

    /// ack 의 답을 하나씩 붙잡는 트랜스포트 — 큐가 천천히(그러나 꾸준히) 비는 흐름을 만든다.
    private final class GatedTransport: Transport, @unchecked Sendable {
        private let lock = NSLock()
        private var handlersStorage = TransportHandlers()
        private var gates: [CheckedContinuation<Void, Never>] = []
        private var publishedStorage: [PublishInput] = []
        private var rejectValue: Primitive?
        var handlers: TransportHandlers { lock.withLock { handlersStorage } }
        var published: [PublishInput] { lock.withLock { publishedStorage } }
        func reject(_ value: Primitive) { lock.withLock { rejectValue = value } }

        func open(handlers: TransportHandlers) async throws -> Joined {
            lock.withLock { handlersStorage = handlers }
            return Joined(roomId: "stage-1", me: Member(participantId: "me-1", displayName: "player"))
        }

        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult {
            lock.withLock { publishedStorage.append(input) }
            if ack {
                await withCheckedContinuation { (c: CheckedContinuation<Void, Never>) in lock.withLock { gates.append(c) } }
            }
            if lock.withLock({ rejectValue }) == input.value { return PublishResult(ok: false, code: "ValidationError", message: "nope") }
            return PublishResult(ok: true)
        }

        /// 기다리는 ack 하나에 답한다.
        func answerOne() {
            let next: CheckedContinuation<Void, Never>? = lock.withLock { gates.isEmpty ? nil : gates.removeFirst() }
            next?.resume()
        }

        func listMembers() async throws -> [Member] { [] }
        func close() async {}
    }

    /// 발행 작업이 결과를 적은 뒤 늦게 끝나도(40 ms) 다음 발행의 기다림을 풀지 않는다 — 기다림은 발행마다 따로다. 깨우개 하나를
    /// 나눠 쓰던 종전에는 늦은 깨우기가 다음 발행의 기다림을 풀어 그것을 포기로 셌다(검수 재현: sent=1/6, 서버는 2건).
    func testALateSettlingSendDoesNotReleaseTheNextSendsWait() async throws {
        sendSettledHookForTest.set { try? await Task.sleep(nanoseconds: 40_000_000) }
        defer { sendSettledHookForTest.set(nil) }
        let transport = SlowAckTransport(ackNanos: 100_000_000)
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        let records = (1...6).map { RecordedMessage(ts: Double($0), channel: "l.x", value: .int($0)) }
        let r = try await bounded { try await replay(client: client, records: records, options: ReplayOptions(speed: .infinity, ack: true, wait: { _ in })) }
        XCTAssertEqual(r.sent, 6)
        XCTAssertEqual(transport.published.map { $0.value }, (1...6).map { Primitive.int($0) })
        XCTAssertEqual(runningWatchdogsForTest.value, 0)
        await client.close()
    }

    /// 끝의 큐 비우기도 주입한 wait 를 쓰지 않는다 — 그 호출 수로 세던 판(PY · DA)은 아무것도 안 하는 wait + 간격 없는 120건에서 곧바로
    /// 돌아와(큐에 90건) 서버는 30건만 받았다. 발행 한도(burst 30 · 초당 60)로 90건이 큐에서 1.5초쯤 흐른다.
    func testTheFinalDrainIsMeasuredWithTheMonotonicClockToo() async throws {
        let transport = SlowAckTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        try await Task.sleep(nanoseconds: 100_000_000)
        let records = (1...120).map { RecordedMessage(ts: Double($0), channel: "d.x", value: .int($0)) }
        let r = try await replay(client: client, records: records, options: ReplayOptions(speed: .infinity, wait: { _ in }))
        XCTAssertEqual(r.sent, 120)
        let left = await client.queuedCount
        XCTAssertEqual(left, 0)  // 돌아왔을 때 다 나갔다
        XCTAssertEqual(transport.published.map { $0.value }, (1...120).map { Primitive.int($0) })
    }

    /// 상한은 누적이 아니라 **진척 없는** 15초다 — 큐가 줄 때마다 다시 센다. 누적 15초는 건강한 흐름도 끊었다(기본 큐가 가득 차면
    /// 초당 60건으로 비우는 데 ~17초). 여기선 큐가 12초(단조 시계)마다 한 건씩 줄어 다 비우는 데 36초가 넘게 걸린다.
    func testADrainThatKeepsProgressingIsNotCutOffAfter15Seconds() async throws {
        let transport = GatedTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        let clock = await fakeClock(client)
        transport.handlers.onState?(.reconnecting)
        try await waitUntilState(client, .reconnecting)
        var acked: [Task<Void, Error>] = []  // 다른 발신자의 ack 발행들 — 끊긴 동안 큐에 든다
        for i in 0..<3 {
            acked.append(Task { try await client.send("a.x", .int(i), ack: true) })
            await TestTiming.until { await client.queuedCount >= i + 1 }
        }
        let done = Done()
        let replaying = Task {
            defer { done.set() }
            return try await replay(client: client, records: Array(rec.prefix(1)), options: ReplayOptions(wait: { _ in }))
        }
        let draining = await until { runningDrainsForTest.value > 0 }
        XCTAssertTrue(draining, "the drain did not start")
        transport.handlers.onState?(.connected)  // flush 가 첫 ack 발행을 내보낸다
        try await advanceAndAnswer(transport, client, clock, done, answers: 3)
        let r = try await replaying.value
        XCTAssertEqual(r.sent, 1)
        let left = await client.queuedCount
        XCTAssertEqual(left, 0)
        for a in acked { _ = try await a.value }
    }

    /// 단조 시계를 12초씩 돌리고(상한 안) 그동안 실행이 끝나지 않았는지 본 뒤, ack 하나에 답해 큐를 한 건 줄인다 — 진척. 줄어든 것을
    /// 감시자가 보고 나서 다음으로 간다.
    private func advanceAndAnswer(_ transport: GatedTransport, _ client: ConnectClient, _ clock: FakeClock, _ done: Done, answers: Int) async throws {
        for step in 1...answers {
            let inFlight = await until { transport.published.count >= step }
            XCTAssertTrue(inFlight, "step \(step): nothing in flight")
            clock.advance(12)
            try await Task.sleep(nanoseconds: 250_000_000)
            XCTAssertFalse(done.value, "step \(step): gave up although the queue kept shrinking")
            let before = await client.queuedCount
            transport.answerOne()
            let shrank = await until { await client.queuedCount < before }
            XCTAssertTrue(shrank, "step \(step): the queue did not shrink")
            try await Task.sleep(nanoseconds: 250_000_000)  // 감시 루프(100ms) · 큐 비우기(20ms)가 준 것을 본다
        }
    }

    /// 끊긴 채 ack 재생 — 발행이 끝나지 않고 큐도 줄지 않은 채 15초(단조 시계)면 그만 기다린다. 끝나지 않은 발행은 보낸 것으로
    /// 세지 않는다. 15초 전에는 그만두지 않는다.
    func testAnAckedSendThatNeverSettlesIsNotCountedAndReplayReturns() async throws {
        let (stage, client, _, wait) = try await harness()
        let clock = await fakeClock(client)
        try await stage.disconnect(client)
        try await waitUntilState(client, .reconnecting)
        let done = Done()
        let replaying = Task {
            defer { done.set() }
            return try await replay(
                client: client, records: Array(rec.prefix(2)), options: ReplayOptions(speed: .infinity, ack: true, wait: wait))
        }
        let watching = await until { runningWatchdogsForTest.value > 0 }  // 기다리기 시작한 시각을 잡았다
        XCTAssertTrue(watching, "the watchdog did not start")
        clock.advance(14.9)
        try await Task.sleep(nanoseconds: 300_000_000)
        XCTAssertFalse(done.value, "gave up before 15 s without progress")
        clock.advance(0.2)
        let returned = await until { done.value }
        XCTAssertTrue(returned, "did not give up after 15 s")
        guard returned else { replaying.cancel(); return }  // 결함이 있으면 매달리지 않고 실패한다
        let r = try await replaying.value
        XCTAssertEqual(r.sent, 0)
        XCTAssertEqual(runningWatchdogsForTest.value, 0)
    }

    /// 진척 없는 시간은 그 발행을 기다리기 시작한 때부터 잰다 — 녹화의 긴 공백(발행 사이 간격)은 세지 않는다. 첫 ack 발행이 감시 루프를
    /// 띄우고 끝난 뒤, 끊긴 채 20초(단조 시계) 공백이 지나고 두 번째 ack 발행이 큐에 든다 — 곧바로 포기하지 않는다.
    func testAGapBetweenSendsIsNotNoProgressTime() async throws {
        let transport = GatedTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        let clock = await fakeClock(client)
        let records = [
            RecordedMessage(ts: 0, channel: "g.x", value: .int(1)), RecordedMessage(ts: 20_000, channel: "g.x", value: .int(2)),
        ]
        let gapPassed = Done()
        let gap: @Sendable (Double) async -> Void = { ms in
            transport.handlers.onState?(.reconnecting)
            // 상태 전이는 클라이언트의 인박스를 거쳐 비동기로 반영된다 — 반영되기 전에 보내면 다음 ack 발행이 큐 대신 직행해
            // 시험이 흔들렸다(부하에서 15번 중 1번).
            await TestTiming.until { await client.state == .reconnecting }
            try? await client.send("g.y", .string("other"))  // 공백 동안 큐에 남은 것이 있다
            clock.advance(ms / 1000)
            gapPassed.set()
        }
        let done = Done()
        let replaying = Task {
            defer { done.set() }
            return try await replay(client: client, records: records, options: ReplayOptions(ack: true, wait: gap))
        }
        let watching = await until { runningWatchdogsForTest.value > 0 }  // 첫 발행이 감시 루프를 띄웠다
        XCTAssertTrue(watching, "the watchdog did not start")
        transport.answerOne()
        let passed = await until { gapPassed.value }
        XCTAssertTrue(passed, "the gap never came")
        try await Task.sleep(nanoseconds: 400_000_000)
        XCTAssertFalse(done.value, "the gap between sends was counted as time without progress")
        clock.advance(16)
        let returned = await until { done.value }
        XCTAssertTrue(returned, "did not give up after 15 s")
        guard returned else { replaying.cancel(); return }  // 결함이 있으면 매달리지 않고 실패한다
        let r = try await replaying.value
        XCTAssertEqual(r.sent, 1)
    }

    /// ack 재생 중의 거절은 삼키지 않는다 — 그대로 던진다.
    func testAMidReplayRejectionThrows() async throws {
        let transport = GatedTransport()
        transport.reject(.string("two"))
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        // 취소될 때까지 답한다 — 횟수로 끊으면(200 × 10 ms) 느린 러너에서 재생이 끝나기 전에 답이 그친다.
        let answering = Task { while !Task.isCancelled { transport.answerOne(); try? await Task.sleep(nanoseconds: 10_000_000) } }
        do {
            let records = rec
            _ = try await bounded { try await replay(client: client, records: records, options: ReplayOptions(speed: .infinity, ack: true)) }
            XCTFail("expected ValidationError")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "ValidationError")
        }
        answering.cancel()
    }

    /// ack 에 끝내 답하지 않는 트랜스포트 — ack 시한(여기선 짧게)이 지나면 AckTimeout 이다.
    private final class SilentTransport: Transport, @unchecked Sendable {
        func open(handlers: TransportHandlers) async throws -> Joined {
            Joined(roomId: "stage-1", me: Member(participantId: "me-1", displayName: "player"))
        }
        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult {
            guard ack else { return PublishResult(ok: true) }
            try? await Task.sleep(nanoseconds: 300_000_000)
            return PublishResult(ok: false, code: "AckTimeout")
        }
        func listMembers() async throws -> [Member] { [] }
        func close() async {}
    }

    /// 큐에 없는데 끝나지 않은 발행은 이미 내보낸 ack 발행이다 — ack 시한 안에 스스로 끝난다(여기선 던지는 AckTimeout). 진척이 없다고
    /// 그것을 포기하면 답하지 않는 서버에 조용한 부분 결과가 나왔다(TS · PY · DA 의 CLI 가 성공이라고 했다). ack 시한이 포기 상한
    /// (15초)보다 길어도 기다린다 — 큐에 남은 것이 없으니 포기하지 않는다. 단조 시계를 30초 넘게 돌려 둔다.
    func testANeverAckingServerEndsTheReplayWithAckTimeoutNotAQuietPartialReturn() async throws {
        let client = ConnectClient(transport: SilentTransport(), exclusiveName: false)
        try await client.open()
        let clock = await fakeClock(client)
        let replaying = Task {
            try await replay(client: client, records: Array(rec.prefix(1)), options: ReplayOptions(ack: true, wait: { _ in }))
        }
        let watching = await until { runningWatchdogsForTest.value > 0 }
        XCTAssertTrue(watching, "the watchdog did not start")
        clock.advance(31)
        do {
            _ = try await bounded { try await replaying.value }
            XCTFail("expected AckTimeout")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "AckTimeout")
        }
        XCTAssertEqual(runningWatchdogsForTest.value, 0)
    }

    /// 한 발행을 기다리는 사이 취소 — 곧바로 돌아오고, 감시 루프가 남지 않는다.
    func testCancellingDuringASendWaitReturnsPromptlyAndLeavesNothingRunning() async throws {
        let (stage, client, _, _) = try await harness()
        // 단조 시계를 멈춰 둔다 — 취소가 듣지 않으면 진척 없는 시간(15초)이 영영 차지 않아 돌아오지 않는다. 실제 시계로 두고
        // "2초 안"을 재던 종전에는 멈춘 러너에서 거짓 실패했고, 상한을 넉넉히 늘리면 15초 뒤의 포기(같은 sent 0)와 구별되지 않았다.
        _ = await fakeClock(client)
        try await stage.disconnect(client)
        try await waitUntilState(client, .reconnecting)
        let run = Task {
            try await replay(client: client, records: Array(rec.prefix(2)), options: ReplayOptions(speed: .infinity, ack: true))
        }
        // 발행 하나를 기다리기 시작한 뒤에 취소한다(고정 대기 대신 감시 루프가 뜬 것을 본다).
        let watching = await until { runningWatchdogsForTest.value > 0 }
        XCTAssertTrue(watching, "the watchdog did not start")
        run.cancel()
        let r = try await bounded { try await run.value }  // 곧바로 돌아온다 — 결함이면 hangGuard 에 걸린다
        XCTAssertEqual(r.sent, 0)
        XCTAssertEqual(runningWatchdogsForTest.value, 0)
    }

    /// 포기한 뒤에 그 발행이 거절되어도(닫기 → SessionEnded) 아무 데로도 새지 않는다 — 뒤늦은 결과는 받아 버린다.
    func testALateRejectionAfterGivingUpIsHandled() async throws {
        let (stage, client, _, wait) = try await harness()
        let clock = await fakeClock(client)
        try await stage.disconnect(client)
        try await waitUntilState(client, .reconnecting)
        let done = Done()
        let replaying = Task {
            defer { done.set() }
            return try await replay(client: client, records: Array(rec.prefix(1)), options: ReplayOptions(ack: true, wait: wait))
        }
        let watching = await until { runningWatchdogsForTest.value > 0 }
        XCTAssertTrue(watching, "the watchdog did not start")
        clock.advance(16)
        let returned = await until { done.value }
        XCTAssertTrue(returned, "did not give up after 15 s")
        guard returned else { replaying.cancel(); return }  // 결함이 있으면 매달리지 않고 실패한다
        let r = try await replaying.value
        XCTAssertEqual(r.sent, 0)  // 큐에 남은 채 15초(단조 시계) — 포기
        await client.close()  // 그 발행이 이제 SessionEnded 로 끝난다
        try await Task.sleep(nanoseconds: 50_000_000)  // 뒤늦은 결과가 새지 않는지 볼 창 — 아래 값은 돌아온 때 이미 0 이다
        XCTAssertEqual(runningWatchdogsForTest.value, 0)
    }

    /// 긴 큐 뒤의 발행 하나를 기다리는 사이 큐가 준다 — 진척이다. 다 비우는 데 15초(단조 시계)가 넘게 걸려도 그 발행은 끝난다.
    func testAQueueThatShrinksDuringASendWaitCountsAsProgress() async throws {
        let transport = GatedTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        let clock = await fakeClock(client)
        transport.handlers.onState?(.reconnecting)
        try await waitUntilState(client, .reconnecting)
        var others: [Task<Void, Error>] = []  // 앞선 ack 발행들
        for i in 0..<3 {
            others.append(Task { try await client.send("a.x", .int(i), ack: true) })
            await TestTiming.until { await client.queuedCount >= i + 1 }
        }
        let done = Done()
        let replaying = Task {
            defer { done.set() }
            return try await replay(client: client, records: Array(rec.prefix(1)), options: ReplayOptions(ack: true, wait: { _ in }))
        }
        let watching = await until { runningWatchdogsForTest.value > 0 }
        XCTAssertTrue(watching, "the watchdog did not start")
        transport.handlers.onState?(.connected)
        try await advanceAndAnswer(transport, client, clock, done, answers: 3)  // 36초 — 그동안 큐가 12초마다 준다
        let out = await until { transport.published.count >= 4 }
        XCTAssertTrue(out, "the replayed send never went out")
        transport.answerOne()
        let r = try await bounded { try await replaying.value }
        XCTAssertEqual(r.sent, 1)
        for o in others { _ = try await o.value }
    }

    private func waitUntilState(_ client: ConnectClient, _ want: TransportState) async throws {
        let reached = await TestTiming.until { await client.state == want }
        if reached { return }
        XCTFail("state never became \(want)")
    }

    func testReplayStopsOnEnded() async throws {
        let (stage, client, _, _) = try await harness()
        let counter = Waits()
        let r = try await replay(client: client, records: rec, options: ReplayOptions(wait: { _ in
            counter.add(1)
            // end 는 액터 홉을 거쳐 클라이언트 상태에 반영된다 — 다음 발행 전에 반영되도록 잠깐 기다린다
            if counter.value.count == 2 { stage.end("x"); await untilEnded(client) }
        }))
        await settle()
        XCTAssertEqual(r, ReplayResult(sent: 3, loops: 0)) // 대기는 간격이 있을 때만 → 두 번째 대기는 4번째 발행 직전
    }

    func testReplayValidatesSpeedAndLoop() async throws {
        let (_, client, _, wait) = try await harness()
        do { _ = try await replay(client: client, records: rec, options: ReplayOptions(speed: 0, wait: wait)); XCTFail("speed") }
        catch let e as AdapterError { XCTAssertTrue(e.message.contains("speed")) }
        do { _ = try await replay(client: client, records: rec, options: ReplayOptions(loop: 0, wait: wait)); XCTFail("loop") }
        catch let e as AdapterError { XCTAssertTrue(e.message.contains("loop")) }
    }

    func testSimulateSineRampRandomStep() async throws {
        let (stage, client, waits, wait) = try await harness()
        let r = try await simulate(client: client, channel: "sensor.t", options: SimulateOptions(waveform: "sine", min: 10, max: 30, hz: 2, count: 5, periodMs: 2000, wait: wait))
        XCTAssertEqual(r.values, [.double(20), .double(30), .double(20), .double(10), .double(20)])
        XCTAssertEqual(waits.value, [500, 500, 500, 500])
        XCTAssertTrue(stage.published.allSatisfy { $0.input.channel == "sensor.t" })

        let (_, c2, _, w2) = try await harness()
        let ramp = try await simulate(client: c2, channel: "r", options: SimulateOptions(waveform: "ramp", min: 0, max: 100, hz: 4, count: 6, periodMs: 1000, wait: w2))
        XCTAssertEqual(ramp.values, [.double(0), .double(25), .double(50), .double(75), .double(0), .double(25)])
        // 반올림 규칙 고정(여섯 언어 공통 floor(v·f+0.5)/f): 0.0625 → 0.063, −0.9375 → −0.937 — 은행가(0.062)·절대값(−0.938) 방식과 갈리는 자리
        let (_, c5, _, w5) = try await harness()
        let half = try await simulate(client: c5, channel: "x", options: SimulateOptions(waveform: "ramp", min: 0, max: 1, hz: 8, count: 2, periodMs: 2000, wait: w5))
        XCTAssertEqual(half.values, [.double(0), .double(0.063)])
        let (_, c6, _, w6) = try await harness()
        let negHalf = try await simulate(client: c6, channel: "x", options: SimulateOptions(waveform: "ramp", min: -1, max: 0, hz: 8, count: 2, periodMs: 2000, wait: w6))
        XCTAssertEqual(negHalf.values, [.double(-1), .double(-0.937)])
        // CRLF 녹화도 줄 번호가 맞는다
        XCTAssertThrowsError(try parseRecording("{\"ts\":1,\"channel\":\"a\",\"value\":1}\r\nnot json")) { XCTAssertTrue("\($0)".contains("line 2 is not JSON")) }

        let seq = Waits(); seq.add(0); seq.add(0.5); seq.add(1 - 1e-9)
        let idx = Waits()
        let (_, c3, _, w3) = try await harness()
        let rnd = try await simulate(client: c3, channel: "x", options: SimulateOptions(waveform: "random", min: -1, max: 1, count: 3, wait: w3, random: {
            idx.add(0); return seq.value[idx.value.count - 1]
        }))
        XCTAssertEqual(rnd.values, [.double(-1), .double(0), .double(1)])

        let (_, c4, _, w4) = try await harness()
        let step = try await simulate(client: c4, channel: "gear", options: SimulateOptions(waveform: "step", count: 5, steps: [.string("P"), .string("R"), .string("D")], wait: w4))
        XCTAssertEqual(step.values, [.string("P"), .string("R"), .string("D"), .string("P"), .string("R")])
    }

    func testSimulateDurationAndDefaults() async throws {
        let (_, c1, _, w1) = try await harness()
        let byDuration = try await simulate(client: c1, channel: "x", options: SimulateOptions(waveform: "ramp", hz: 10, durationMs: 500, wait: w1))
        XCTAssertEqual(byDuration.sent, 5)
        let (_, c2, waits, w2) = try await harness()
        let r = try await simulate(client: c2, channel: "x", options: SimulateOptions(waveform: "sine", wait: w2))
        XCTAssertEqual(r.sent, 10)
        XCTAssertEqual(waits.value, Array(repeating: 1000, count: 9))
        XCTAssertTrue(r.values.allSatisfy { v in
            guard let d = v.doubleValue else { return false }
            return d >= 0 && d <= 1 && (d * 1000).rounded() / 1000 == d
        })
    }

    func testSimulateValidatesOptions() async throws {
        let (_, c, _, w) = try await harness()
        func expectConfig(_ needle: String, _ opts: SimulateOptions) async {
            do { _ = try await simulate(client: c, channel: "x", options: opts); XCTFail(needle) }
            catch let e as AdapterError { XCTAssertTrue(e.message.contains(needle), e.message) }
            catch { XCTFail("\(error)") }
        }
        await expectConfig("hz", SimulateOptions(waveform: "sine", hz: 0, wait: w))
        await expectConfig("min/max", SimulateOptions(waveform: "sine", min: 2, max: 1, wait: w))
        await expectConfig("steps", SimulateOptions(waveform: "step", wait: w))
        await expectConfig("count", SimulateOptions(waveform: "sine", count: 0, wait: w))
        await expectConfig("waveform", SimulateOptions(waveform: "square", wait: w))
    }

    func testSimulateStopsWhenEnded() async throws {
        let (stage, client, _, _) = try await harness()
        let r = try await simulate(client: client, channel: "x", options: SimulateOptions(waveform: "ramp", count: 5, wait: { _ in
            stage.end("x"); await untilEnded(client)
        }))
        await settle()
        XCTAssertEqual(r.sent, 1)
    }
}

/// end 는 액터 홉을 거쳐 클라이언트 상태에 반영된다 — 고정 sleep 은 부하가 걸린 러너에서 뒤집히므로 상태를 확인한다.
private func untilEnded(_ client: ConnectClient) async {
    await TestTiming.until { await client.state == .ended }
}
