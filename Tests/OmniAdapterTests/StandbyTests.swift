import XCTest
@testable import OmniAdapter

/// 대기(standby) 자동 억제 — TS `client.test.ts` / Kotlin `StandbyTest.kt` 미러.
///
/// 잠그는 규칙 다섯(전부 실제로 밟았던 함정이다):
///   ① 판정은 서버 것이다 — roster 의 `standby` 를 읽을 뿐 다시 계산하지 않는다
///   ② **부재 ≠ 대기** — 구 서버는 필드를 안 보낸다. nil 을 대기로 접으면 구 서버에서
///      어댑터가 조용히 발신을 멈춘다
///   ③ **큐 경유 경로도 막는다** — 직접 발신만 막으면 끊김 중 쌓인 값이 재접속 직후
///      그대로 나가 값이 이중으로 흐른다
///   ④ **접속마다 판정이 flush 보다 먼저다** — 내 재조인은 나에게 프레즌스를 주지 않고
///      `joined.me` 에는 standby 가 없다. roster 를 직접 읽어야 안다
///   ⑤ `ack: true` 는 거짓 성공을 주지 않는다 — 안 보냈으면 던진다
final class StandbyTests: XCTestCase {

    private final class FakeTransport: Transport, @unchecked Sendable {
        private let lock = NSLock()
        private var handlersStorage = TransportHandlers()
        private var publishedStorage: [PublishInput] = []
        private var rosterStorage: [Member] = []
        private var rosterFails = false

        let me = Member(participantId: "me-1", displayName: "unity-screen")

        var handlers: TransportHandlers { lock.withLock { handlersStorage } }
        var published: [PublishInput] { lock.withLock { publishedStorage } }
        func setRoster(_ roster: [Member]) { lock.withLock { rosterStorage = roster } }
        func failRoster() { lock.withLock { rosterFails = true } }

        func open(handlers: TransportHandlers) async throws -> Joined {
            lock.withLock { handlersStorage = handlers }
            // 서버는 joined.me 에 standby 를 싣지 않는다 — 런타임 맵에서 오기 때문이다.
            return Joined(roomId: "stage-1", me: me)
        }

        private var resultStorage = PublishResult(ok: true)
        private var rosterHeld = false
        func setResult(_ result: PublishResult) { lock.withLock { resultStorage = result } }
        /// 켜 두면 roster 응답을 풀 때까지 붙잡는다 — 재접속 직후의 판정 창을 연 채로 둔다.
        func holdRoster(_ held: Bool) { lock.withLock { rosterHeld = held } }

        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult {
            let result = lock.withLock { () -> PublishResult in
                publishedStorage.append(input)
                return resultStorage
            }
            if let held = lock.withLock({ publishGateStorage }) { await held.wait() }
            if ack, let gate = ackGate { await gate.wait() }
            if ack, let wait = ackWait?(input) { await wait.wait() }
            if let decide = resultFor, let decided = decide(input) { return decided }
            return result
        }

        private var ackWaitStorage: (@Sendable (PublishInput) -> Gate?)?
        private var resultForStorage: (@Sendable (PublishInput) -> PublishResult?)?
        /// 설정하면 ack 발행마다 이것이 돌려준 문이 열릴 때까지 답을 붙잡는다(값마다 따로 — 답이 오는 순서를 정한다).
        func setAckWait(_ f: (@Sendable (PublishInput) -> Gate?)?) { lock.withLock { ackWaitStorage = f } }
        var ackWait: (@Sendable (PublishInput) -> Gate?)? { lock.withLock { ackWaitStorage } }
        /// 설정하면 발행마다 이것이 준 결과로 답한다(nil 이면 정해 둔 결과) — 답을 받는 순간에 정한다.
        func setResultFor(_ f: (@Sendable (PublishInput) -> PublishResult?)?) { lock.withLock { resultForStorage = f } }
        var resultFor: (@Sendable (PublishInput) -> PublishResult?)? { lock.withLock { resultForStorage } }

        /// 차례로 쓰일 roster 응답 — 비어 있으면 기본 roster. 응답마다 따로 붙잡을 수 있다(응답 순서 뒤집기). nil 이면 실패.
        private var scriptStorage: [@Sendable () async -> [Member]?] = []
        func script(_ response: @escaping @Sendable () async -> [Member]) { lock.withLock { scriptStorage.append(response) } }
        /// 다음 roster 조회를 (붙잡았다가) 실패시킨다.
        func scriptFailure(_ wait: @escaping @Sendable () async -> Void) {
            lock.withLock { scriptStorage.append({ await wait(); return nil }) }
        }

        func listMembers() async throws -> [Member] {
            let scripted: (@Sendable () async -> [Member]?)? = lock.withLock {
                scriptStorage.isEmpty ? nil : scriptStorage.removeFirst()
            }
            if let scripted {
                guard let roster = await scripted() else { throw AdapterError("AckTimeout", "stage:list-members timed out") }
                return roster
            }
            while lock.withLock({ rosterHeld }) {
                try? await Task.sleep(nanoseconds: 5_000_000)
            }
            let (fails, roster) = lock.withLock { (rosterFails, rosterStorage) }
            if fails { throw AdapterError("NotConnected", "boom") }
            return roster
        }

        /// 설정하면 close() 가 풀릴 때까지 붙잡는다(leave 확인을 기다리는 꼴).
        private var closeGate: Gate?
        func holdClose(_ gate: Gate) { lock.withLock { closeGate = gate } }
        func close() async {
            if let gate = lock.withLock({ closeGate }) { await gate.wait() }
        }

        /// 설정하면 발행마다(ack 와 무관하게) 풀릴 때까지 트랜스포트 호출을 붙잡는다.
        private var publishGateStorage: Gate?
        func holdPublishes(_ gate: Gate?) { lock.withLock { publishGateStorage = gate } }

        /// 설정하면 ack 발행의 답을 풀릴 때까지 붙잡는다(답을 기다리는 flush 를 만든다).
        private var ackGateStorage: Gate?
        func holdAcks(_ gate: Gate?) { lock.withLock { ackGateStorage = gate } }
        var ackGate: Gate? { lock.withLock { ackGateStorage } }

        func setState(_ state: TransportState) { handlers.onState?(state) }
        /// 프레즌스 한 번 — roster 를 다시 읽게 하는 트리거.
        func presence() {
            handlers.onMemberJoin?(Member(participantId: "peer-9", displayName: "Peer"))
        }
    }

    private let meActive = Member(participantId: "me-1", displayName: "unity-screen", standby: false)
    private let meStandby = Member(participantId: "me-1", displayName: "unity-screen", standby: true)
    private let otherActive = Member(
        participantId: "other-1", displayName: "unity-screen", standby: false)

    /// 대기 판정이 반영될 때까지 기다린다(refreshStandby 는 비동기다) — TestTiming.hangGuard 까지. 종전의 0.4초는 멈춘
    /// 러너에서 모자랐다. 이미 그 값이면 곧바로 돌아온다.
    private func settle(_ client: ConnectClient, _ want: Bool) async {
        await TestTiming.until(0, pollNanos: 5_000_000) { await client.standby == want }
    }

    /// 접속 판정이 끝나 창이 닫힐 때까지 기다린다 — 그 뒤의 발행은 직행한다. 고정 50 ms 뒤에 "끝났을 것"이라 보면 느린
    /// 러너에서 발행이 창 안에 모여(직행하지 않아) 거짓 실패한다.
    private func judged(_ client: ConnectClient) async throws {
        try await waitUntil { await !client.judgmentPendingForTest }
    }

    func testExclusiveNameMiSingoMyeonGyuchikMiJeokyong() async throws {
        let transport = FakeTransport()
        // 서버가 나를 대기로 표시해도, 신고하지 않았으면 읽지 않는다.
        transport.setRoster([meStandby, otherActive])
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        transport.presence()
        try await Task.sleep(nanoseconds: 120_000_000)
        let standby = await client.standby
        XCTAssertFalse(standby, "미신고 클라이언트는 대기가 되지 않는다")
        try await client.send("a.b", 1)
        XCTAssertEqual(transport.published.count, 1, "발신이 그대로 나가야 한다")
    }

    func testStandbyBujaeNeunDaegiGaAnida() async throws {
        let transport = FakeTransport()
        // 구 서버: standby 필드 자체를 보내지 않는다(nil).
        transport.setRoster([transport.me])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        transport.presence()
        try await Task.sleep(nanoseconds: 120_000_000)
        let standby = await client.standby
        XCTAssertFalse(standby, "부재를 대기로 접으면 구 서버에서 발신이 멈춘다")
        try await client.send("a.b", 1)
        XCTAssertEqual(transport.published.count, 1)
    }

    func testDaegiMyeonBalsinEokjeAndAckNeunDeonjinda() async throws {
        let transport = FakeTransport()
        transport.setRoster([meStandby, otherActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        await settle(client, true)
        let standby = await client.standby
        XCTAssertTrue(standby)

        try await client.send("a.b", 1)  // fire-and-forget — 조용히 버린다
        XCTAssertEqual(transport.published.count, 0, "대기 중에는 보내지 않는다")

        // ack 를 요구했는데 안 보냈으면 거짓 성공을 주지 않는다.
        do {
            try await client.send("a.b", 2, ack: true)
            XCTFail("대기 중 ack 발행은 던져야 한다")
        } catch let err as AdapterError {
            XCTAssertEqual(err.code, "Standby")
            // 다음 행동을 들고 있다(TS 와 같은 기본 힌트).
            XCTAssertTrue(err.hint?.contains("resumes automatically") ?? false)
        }
        XCTAssertEqual(transport.published.count, 0)
        let queued = await client.queuedCount
        XCTAssertEqual(queued, 0, "연결된 채 대기면 버린다 — 쌓지 않는다")
        // 버린 발행은 진단 링에 남기지 않는다(TS · Python 과 같다) — 고빈도 발행이 링에서 standby 기록을 밀어내지 않게.
        XCTAssertFalse(client.diagnostics().contains { $0.code == "send-dropped" })
    }

    func testDaegiJungKkeunkinDongananBalhaengEunDaegiroJaejeopsokHamyeonBeoryeojigoSeunggyeokTtaeNalgeunGapIEopda() async throws {
        // 끊긴 동안의 발행은 대기 여부와 상관없이 큐로 간다(그 사이 판정이 바뀔 수 있다). 재접속 판정이 여전히 대기면 그 자리에서
        // 버린다(D3') — fire-and-forget 은 standby 기록 한 줄에 개수만 남고, ack 는 Standby 로 거절된다(매달리지 않는다).
        // 머물렀다가 승격 때 흘리던 종전에는 몇 분 뒤의 승격이 낡은 값으로 더 새 상태를 되돌렸다.
        let transport = FakeTransport()
        transport.setRoster([meStandby, otherActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        await settle(client, true)
        let standby = await client.standby
        XCTAssertTrue(standby)

        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        try await client.send("a.b", 1)  // fire-and-forget — 버리지 않고 쌓는다
        let acked = Task { try await client.send("a.b", 2, ack: true) }  // ack — Standby 로 던지지 않고 큐에서 기다린다
        try await waitUntil { await client.queuedCount == 2 }

        let standbyNotes = client.diagnostics().filter { $0.code == "standby" }.count
        transport.setState(.connected)  // 여전히 대기 — 큐는 흐르지 않고 버려진다(D3')
        // bounded 는 던진 작업을 끝난 것으로 보지 않는다 — 결과로 감싸 기다린다.
        let outcome = try await bounded(Task { () async throws -> Result<Void, Error> in await acked.result })
        XCTAssertEqual((outcome.failureError as? AdapterError)?.code, "Standby", "\(outcome)")
        let emptied = await client.queuedCount
        XCTAssertEqual(emptied, 0)
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertEqual(transport.published.count, 0)
        // 이미 대기였어도 버렸으면 그 한 줄을 남긴다 — 개수와 함께. 항목마다 한 줄이 아니다.
        let notes = client.diagnostics().filter { $0.code == "standby" }
        XCTAssertEqual(notes.count, standbyNotes + 1, "\(notes)")
        XCTAssertTrue(notes.last?.message.hasSuffix(" — dropped 2 queued sends") == true, "\(notes)")

        // D3: 대기인 동안의 새 발행은 직행 경로의 대기 규칙 — fire-and-forget 은 버리고 ack 는 Standby. 큐로 보내면 끊김을 거친
        // 대기 인스턴스가 큐 모드에 갇혀 넘침이 링을 덮고, 승격 때 낡은 값이 쏟아진다.
        try await client.send("a.b", 3)
        do {
            try await client.send("a.b", 4, ack: true)
            XCTFail("expected Standby")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "Standby")
        }
        let none = await client.queuedCount
        XCTAssertEqual(none, 0)
        XCTAssertEqual(transport.published.count, 0)

        transport.setRoster([meActive])
        transport.presence()
        await settle(client, false)
        try await Task.sleep(nanoseconds: 150_000_000)
        XCTAssertEqual(transport.published.count, 0, "승격 때 낡은 1 · 2 · 3 · 4 는 하나도 나가지 않는다")
        try await client.send("a.b", 5)  // 승격 뒤의 새 발행은 나간다
        try await waitUntil { transport.published.count == 1 }
        XCTAssertEqual(transport.published.map { $0.value }, [.int(5)])
    }

    /// D5: 접속 판정(roster 조회)이 실패하면 활성으로 본다 — 대기였던 인스턴스가 판정 실패로 영영 막혀 있지 않게. 프레즌스
    /// 판정의 실패는 상태를 뒤집지 않는다(이 연결의 판정이 이미 있다).
    func testDaegiroKkeunkyeottaGaJaejeopsokPanjeongISilpaehamyeonHwalseongEuroBogoHeullinda() async throws {
        let transport = FakeTransport()
        transport.setRoster([meStandby, otherActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        await settle(client, true)

        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        try await client.send("a.b", 7)
        transport.scriptFailure {}
        transport.setState(.connected)
        await settle(client, false)
        try await waitUntil { transport.published.count == 1 }
        XCTAssertEqual(transport.published.map { $0.value }, [.int(7)])
        XCTAssertTrue(
            client.diagnostics().contains { $0.code == "active" && $0.message == "judgment failed — treated as active" },
            "\(client.diagnostics())")
    }

    func testDaegiroKkeunkyeottaGaHwalseongEuroJaejeopsokHamyeonSsainBalhaengINaganda() async throws {
        // 끊긴 사이 활성이 떠났다 — 재접속 판정이 나를 활성으로 올리고, 그 판정 뒤의 flush 가 큐를 흘린다.
        let transport = FakeTransport()
        transport.setRoster([meStandby, otherActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        await settle(client, true)

        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        try await client.send("a.b", 5)
        let queued = await client.queuedCount
        XCTAssertEqual(queued, 1)

        transport.setRoster([meActive])
        transport.setState(.connected)
        await settle(client, false)
        try await waitUntil { transport.published.count == 1 }
        XCTAssertEqual(transport.published.first?.value, .int(5))
    }

    func testJaejeopsokJikhuDaegiPanjeongJeonenBackoffTimerGaKkaedoKyuGaHeureuJiAnneunda() async throws {
        // 판정(roster 왕복) 중의 대기 플래그는 끊기기 전 값이라 낡았다. 그 사이 깬 flush 가 끊긴 동안 쌓인 값을 그 판정으로
        // 내보내면, 서버가 이미 대기로 판정한 인스턴스가 값을 이중으로 흘린다(TS 가 실소켓에서 재현한 구멍 — 거기선 판정 창
        // 안의 발행이 깨웠고, 이 언어에선 큐에 넣기가 flush 를 깨우지 않으니 남은 계기는 타이머다).
        let transport = FakeTransport()
        transport.setRoster([meActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        try await judged(client)  // 첫 판정(활성)이 끝나게

        // 백오프 타이머 하나를 걸어 둔다 — 직행 발행이 TooManyRequests 를 받아 큐로 돌아가고 1초 뒤 flush 가 깬다.
        transport.setResult(PublishResult(ok: false, code: "TooManyRequests"))
        try await client.send("a.b", 1)
        XCTAssertEqual(transport.published.count, 1)
        let queued = await client.queuedCount
        XCTAssertEqual(queued, 1)
        transport.setResult(PublishResult(ok: true))

        // 끊겼다가 재접속 — 그 사이 서버가 나를 대기로 판정했다(프레즌스 없음). 판정을 붙잡아 창을 연 채로 둔다.
        transport.holdRoster(true)
        transport.setRoster([meStandby, otherActive])
        transport.setState(.reconnecting)
        transport.setState(.connected)
        try await Task.sleep(nanoseconds: 1_400_000_000)  // 백오프 타이머(1초)가 판정 창 안에서 깬다
        XCTAssertEqual(transport.published.count, 1, "판정 창 안에서는 큐가 흐르지 않는다")

        transport.holdRoster(false)
        await settle(client, true)
        try await Task.sleep(nanoseconds: 150_000_000)
        let standby = await client.standby
        XCTAssertTrue(standby)
        XCTAssertEqual(transport.published.count, 1, "대기로 판정되면 큐는 나가지 않고")
        let still = await client.queuedCount
        XCTAssertEqual(still, 0, "버려진다(D3') — 접속 판정이 대기였다")
    }

    /// 판정(roster 왕복) 중의 대기 플래그는 낡았다 — 직행하면 서버가 이미 대기로 판정한 인스턴스의 값이 나간다(TS · Python ·
    /// Dart 가 실측으로 잡은 구멍). 큐로 가서 판정을 기다린다. 최초 open() 도 같다(창은 connected 를 알린 그 액터 차례에
    /// 열린다 — 알리는 순간의 발행도 그 안이다).
    func testPanjeongChangAneseoNeunKyuGaBieoItseodoJikhaengHajiAnneunda() async throws {
        let transport = FakeTransport()
        transport.holdRoster(true)
        transport.setRoster([meStandby, otherActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        let events = await client.events()
        let sentOnConnected = Task {
            for await event in events {
                if case .stateChange(.connected) = event {
                    try? await client.send("a.b", 1)  // 알리는 순간의 발행
                    return
                }
            }
        }
        try await client.open()  // 첫 판정이 붙잡혀 있다
        await sentOnConnected.value
        try await client.send("a.b", 2)  // 접속 중 · 판정 전 — 창 안에 모인다
        let acked = Task { () async throws -> String in  // 창 안의 ack 발행 — 판정을 기다린다
            do { try await client.send("a.b", 3, ack: true); return "ok" } catch let e as AdapterError { return e.code } catch { return "\(error)" }
        }
        try await waitUntil { await client.queuedCount == 3 }
        XCTAssertEqual(transport.published.count, 0, "판정 전에는 직행하지 않는다")

        transport.holdRoster(false)
        await settle(client, true)
        // D1: 대기로 판정되면 창 안의 발행에 직행 경로의 대기 규칙 — fire-and-forget 은 버리고 ack 는 Standby. 순서 큐에
        // 남기지 않는다(남기면 그 뒤 발행이 전부 큐로 가 넘침이 링을 덮고, 승격 때 낡은 값이 쏟아진다).
        let outcome = try await bounded(acked)
        XCTAssertEqual(outcome, "Standby")
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertEqual(transport.published.count, 0)
        let still = await client.queuedCount
        XCTAssertEqual(still, 0)
        try await client.send("a.b", 4)  // 그 뒤의 발행은 직행 규칙(대기 → 버림) — 큐로 가지 않는다
        let after = await client.queuedCount
        XCTAssertEqual(after, 0)
    }

    /// 판정 실패는 활성으로 본다(D1) — 조회 실패가 발신을 영구히 막는 것이 더 나쁘다. 창 안에 모인 발행이 큐 끝으로 가 흐르고,
    /// 그 뒤의 발행은 직행한다.
    func testRosterJohoeSilpaeEdoPanjeongChangIDatchigoChangAnBalhaengIHeureunda() async throws {
        let transport = FakeTransport()
        transport.setRoster([meActive])
        let gate = Gate()
        transport.scriptFailure { await gate.wait() }
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()  // 첫 판정(실패할 것)이 붙잡혀 있다
        try await client.send("a.b", 1)
        let acked = Task { try await client.send("a.b", 2, ack: true) }
        try await waitUntil { await client.queuedCount == 2 }
        XCTAssertEqual(transport.published.count, 0)

        await gate.open()  // 판정 실패 → 창이 닫힌다
        try await bounded(acked)
        try await waitUntil { transport.published.count == 2 }
        try await client.send("a.b", 3)
        try await waitUntil { transport.published.count == 3 }
        XCTAssertEqual(transport.published.map { $0.value }, [.int(1), .int(2), .int(3)])
        let standby = await client.standby
        XCTAssertFalse(standby)
    }

    /// 가장 흔한 재접속 — 승격이 아니라 창 닫기만이 큐를 흘린다. 이 flush 가 빠지면 재접속 뒤 큐가 멈춘다(검수의 변이가 모든
    /// 시험을 초록으로 둔 채 그렇게 만들었다).
    func testHwalseongEuroKkeunkyeottaGaHwalseongEuroJaejeopsokHamyeonKkeunkinDonganuiBalhaengINaganda() async throws {
        let transport = FakeTransport()
        transport.setRoster([meActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        try await judged(client)

        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        try await client.send("a.b", 1)
        let acked = Task { try await client.send("a.b", 2, ack: true) }
        try await waitUntil { await client.queuedCount == 2 }
        transport.setState(.connected)
        try await bounded(acked)
        try await waitUntil { transport.published.count == 2 }
        XCTAssertEqual(transport.published.map { $0.value }, [.int(1), .int(2)])
        let queued = await client.queuedCount
        XCTAssertEqual(queued, 0)
    }

    /// D1: 창 안의 발행은 따로 모였다가 활성 판정이면 큐 **끝**에 붙는다 — 끊긴 동안 쌓인 것이 먼저 나간다.
    func testHwalseongPanjeongImyeonChangAnBalhaengEunKkeunkinDonganBalhaengDwie() async throws {
        let transport = FakeTransport()
        transport.setRoster([meActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        try await judged(client)

        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        try await client.send("a.b", "offline")
        transport.holdRoster(true)  // 재접속 판정을 붙잡는다
        transport.setState(.connected)
        try await waitUntil { await client.state == .connected }
        try await client.send("a.b", "window")
        try await Task.sleep(nanoseconds: 50_000_000)
        XCTAssertEqual(transport.published.count, 0)
        let queued = await client.queuedCount
        XCTAssertEqual(queued, 2)

        transport.holdRoster(false)
        try await waitUntil { transport.published.count == 2 }
        XCTAssertEqual(transport.published.map { $0.value.stringValue }, ["offline", "window"])
    }

    /// D1: 창 안의 발행은 따로 모였다가 활성 판정이면 큐 **끝**에 붙는다 — 끊긴 동안 쌓인 것이 먼저 나간다. 창 안에서 끊기면
    /// 모은 발행은 그 뒤의 끊김 중 발행보다 앞에 선다.
    func testChangAnBalhaengSunseo() async throws {
        let transport = FakeTransport()
        transport.setRoster([meActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        try await judged(client)

        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        try await client.send("a.b", "offline")
        transport.holdRoster(true)  // 재접속 판정을 붙잡는다
        transport.setState(.connected)
        try await waitUntil { await client.state == .connected }
        try await client.send("a.b", "window")
        transport.setState(.reconnecting)  // 창 안에서 끊겼다 — 모은 것은 큐로
        try await waitUntil { await client.state == .reconnecting }
        try await client.send("a.b", "offline-2")
        transport.holdRoster(false)
        transport.setState(.connected)
        try await waitUntil { transport.published.count == 3 }
        XCTAssertEqual(
            transport.published.map { $0.value.stringValue }, ["offline", "window", "offline-2"])
    }

    /// 끊기기 전에(또는 지난 연결에서) 시작한 판정의 늦은 답("활성")이 새 연결의 창을 닫거나 대기를 풀면 안 된다 — 창을 열 때
    /// 접속 번호를 올리고, 그 번호가 다른 답은 버린다. 판정은 한 번에 하나라(D6) 버린 뒤 새 연결의 판정이 한 번 더 돈다.
    func testJinanYeongyeoruiPanjeongEungdabeunJeogyongHajidoChangeulDatjidoAnneunda() async throws {
        let transport = FakeTransport()
        transport.setRoster([meActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        try await judged(client)  // 첫 판정(활성)이 끝나게

        let stale = Gate()
        let active = meActive
        let standbyRoster = [meStandby, otherActive]
        let staleAsked = Counter()
        transport.script { staleAsked.bump(); await stale.wait(); return [active] }  // 지난 연결 — 늦다
        // 새 연결의 판정도 붙잡는다 — 낡은 답의 효과(창 닫기 · flush)가 드러날 틈을 준다(바로 뒤따르면 대기 판정이 큐를 버려 가린다).
        let current = Gate()
        transport.script { await current.wait(); return standbyRoster }

        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        try await client.send("a.b", 1)  // 끊긴 동안 → 큐
        transport.setState(.connected)  // 판정 #1(붙잡힘)
        // 판정 #1 이 roster 를 물은(이 연결의 접속 번호를 잡은) 뒤에 끊는다 — 고정 50 ms 로는 느린 러너에서 판정 #1 이 새 연결의
        // 번호를 잡아 "지난 연결의 답"이 아니게 된다.
        try await waitUntil { staleAsked.value == 1 }
        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        transport.setState(.connected)  // 새 연결 — 도는 판정이 있으니 그 뒤에 한 번 더(D6)
        // 새 연결이 반영된(창이 다시 열린) 뒤에 지난 답을 푼다 — 그 전에 풀면 그 답이 아직 이 연결의 것이다.
        try await waitUntil { await client.state == .connected }

        await stale.open()  // 지난 연결의 답: 활성 — 버려진다(창도 닫지 않는다)
        try await Task.sleep(nanoseconds: 150_000_000)
        XCTAssertEqual(transport.published.count, 0, "낡은 답이 창을 닫고 큐를 흘렸다")
        let held = await client.queuedCount
        XCTAssertEqual(held, 1)

        await current.open()  // 새 연결의 판정(대기)이 돈다
        await settle(client, true)
        try await Task.sleep(nanoseconds: 100_000_000)
        let standby = await client.standby
        XCTAssertTrue(standby, "the stale answer unblocked the new connection")
        XCTAssertEqual(transport.published.count, 0, "낡은 답이 창을 닫고 큐를 흘렸다")
        let queued = await client.queuedCount
        XCTAssertEqual(queued, 0, "새 연결의 판정(대기)이 끊긴 동안의 큐를 버렸다(D3')")
    }

    /// D2': 서버는 소켓마다 초당 60 · burst 60 을 넘긴 fire-and-forget 을 조용히 버린다. 직행 · flush 모두 토큰을 써서 그 한도를
    /// 넘기지 않는다 — 한꺼번에 150개를 보내면 30개(messageBurst — 도착 몰림을 견디려 서버 burst 의 절반)가 곧바로 나가고
    /// 나머지는 큐에서 초당 60개로 흐른다(가장 새 값까지 닿는다).
    func testBalhaengEunSeobeoUiHandoMankeumManNaganda() async throws {
        XCTAssertEqual(ConnectClient.messageRate, 60)
        XCTAssertEqual(ConnectClient.messageBurst, 30)
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        // E1: open 이 걸어 둔 flush 가 끝나게 쉰다 — 그것이 남아 있으면 토큰이 없어 큐로 간 발행을 대신 깨워, 큐에 넣기가 flush 를
        // 깨우지 않는 결함을 가린다(Python 이 그래서 100건 중 30건만 보냈다).
        try await Task.sleep(nanoseconds: 100_000_000)
        let started = Date()
        for i in 1...150 { try await client.send("slider", .int(i)) }
        try await Task.sleep(nanoseconds: 100_000_000)
        let early = transport.published.count
        // 상한은 실제 경과에서 센다 — send 루프도 시간을 쓰므로, 느린 러너에서는 고정 상한 40(burst + 100ms)을 넘는다(CI: 42).
        let elapsed = Date().timeIntervalSince(started)
        let ceiling = 30 + Int(60 * elapsed) + 1
        XCTAssertTrue((30...ceiling).contains(early), "burst 30 + \(Int(elapsed * 1000))ms 동안 찬 몇 개: \(early) (상한 \(ceiling))")
        try await waitUntil { transport.published.count >= 150 }
        // 나머지 120개는 초당 60개 — 2초. 여유를 둔 하한.
        XCTAssertGreaterThanOrEqual(Date().timeIntervalSince(started), 1.8, "drained too fast for 60/s")
        XCTAssertEqual(transport.published.map { $0.value }, (1...150).map { Primitive.int($0) })
    }

    /// 동시에 보낸 ack 직행 8건 중 4–8 이 TooManyRequests — 답은 뒤섞여 온다(6, 4, 8, 5, 7). 답이 온 순서대로 하나씩 맨 앞에
    /// 꽂으면 뒤집혀 다시 나갔다(실소켓: [.., 8, 7, 6, 5, 4]). 발행 순번대로 자리를 찾는다. 거절 구간 하나에 백오프는 한 번만
    /// 늘어난다 — 거절마다 ×2 하면 5건이 1+2+4+8+16 초를 쉬었다.
    func testDongsieGeojeoldoenJikhaengBalhaengdeureunBonaenSunseodaeroDasiNaganda() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        try await Task.sleep(nanoseconds: 100_000_000)
        let gates = (0...8).map { _ in Gate() }
        let rejected = RejectOnce()
        // 다시 나가는 발행의 답은 따로 붙잡는다 — 사다리는 flush 의 ack 성공에 내려오므로, 거절 다섯이 모두 처리된 뒤 · 다시 보낸
        // 것이 성공하기 전의 값을 읽어야 한다. 종전에는 답을 연 뒤 30 ms 쉬고 읽어, 느린 러너에서 거절이 처리되기 전의 0 을 봤다.
        let resend = Gate()
        let firstTime = RejectOnce()
        transport.setAckWait { input in
            if case .int(let v) = input.value { return firstTime.first(v) ? gates[v] : resend }
            return nil
        }
        transport.setResultFor { input in
            if case .int(let v) = input.value, v >= 4, rejected.first(v) {
                return PublishResult(ok: false, code: "TooManyRequests")
            }
            return nil
        }
        var sends: [Task<Void, Error>] = []
        for i in 1...8 {
            sends.append(Task { try await client.send("t", .int(i), ack: true) })
            try await waitUntil { transport.published.count == i }  // 보낸 순서를 고정한다(작업마다 액터에 드는 순서가 다를 수 있다)
        }
        for v in [1, 2, 3] { await gates[v].open() }
        for v in [6, 4, 8, 5, 7] {
            await gates[v].open()  // 답이 뒤섞여 온다
            try await Task.sleep(nanoseconds: 30_000_000)
        }
        try await waitUntil { await client.queuedCount == 5 }  // 거절 다섯이 모두 큐로 돌아왔다(다시 나간 것의 답은 붙잡혀 있다)
        let ladder = await client.backoffForTest
        XCTAssertEqual(ladder, 1, "거절 다섯 번 — 사다리는 한 칸(1초)만 올랐다")
        await resend.open()
        for s in sends { try await bounded(s, seconds: 4) }
        XCTAssertEqual(Array(transport.published.map { $0.value }.dropFirst(8)), [.int(4), .int(5), .int(6), .int(7), .int(8)])
    }

    private final class RejectOnce: @unchecked Sendable {
        private let lock = NSLock()
        private var seen: Set<Int> = []
        func first(_ v: Int) -> Bool { lock.withLock { seen.insert(v).inserted } }
    }

    /// 직행 발행의 실패가 끊김 알림 **뒤에** 닿았다 — 그 사이 끊긴 동안 보낸 Y 가 이미 큐에 있다. 맨 뒤에 되돌리면 낡은 D 가 Y 뒤에
    /// 나가 서버의 마지막 값이 D 가 됐다. 보낸 순번 자리로 — D, Y.
    func testJikhaengINotConnectedRoDoraomyeonKkeunkinDongananSsainBalhaengBodaApeSeonda() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        try await Task.sleep(nanoseconds: 100_000_000)
        let gate = Gate()
        let once = RejectOnce()
        transport.setAckWait { $0.value == .string("D") ? gate : nil }
        transport.setResultFor { input in
            input.value == .string("D") && once.first(0) ? PublishResult(ok: false, code: "NotConnected") : nil
        }
        let d = Task { try await client.send("a.b", .string("D"), ack: true) }  // 직행 — 답을 기다린다
        try await waitUntil { transport.published.count == 1 }
        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        try await client.send("a.b", .string("Y"))  // 끊긴 동안 → 큐
        await gate.open()  // D 의 답: NotConnected — 큐로 되돌아온다
        // 되돌아온 것을 본 뒤에 잇는다 — 고정 50 ms 뒤에 이으면 느린 러너에서 Y 가 먼저 나가 [D, Y, D] 가 된다.
        try await waitUntil { await client.queuedCount == 2 }
        transport.setState(.connected)
        try await bounded(d)
        try await waitUntil { transport.published.count == 3 }
        XCTAssertEqual(transport.published.map { $0.value }, [.string("D"), .string("D"), .string("Y")])  // 서버의 마지막 값은 새 Y
    }

    /// D4 의 닿는 길: 직행 ack D 가 TooManyRequests 로 돌아오는 사이 flush 는 S 를 내보내고 답을 기다린다. D 는 순번대로 이미 나간
    /// S **앞에** 든다(TS 와 같다). 그래서 S 가 끝나면 S 를 자리(맨 앞 = D)가 아니라 정체로 빼야 한다 — 자리로 빼면 D 의 약속이 영영
    /// 안 끝나고 S 가 두 번 나갔다.
    func testDapeulGidarineunFlushHangmokDwiroGeojeoldoenJikhaengIDoraWadoDulDaHanBeonssikNaganda() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        try await Task.sleep(nanoseconds: 100_000_000)
        let dGate = Gate()
        let sGate = Gate()
        let once = RejectOnce()
        transport.setAckWait { input in
            input.value == .string("D") ? dGate : input.value == .string("S") ? sGate : nil
        }
        transport.setResultFor { input in
            input.value == .string("D") && once.first(0) ? PublishResult(ok: false, code: "TooManyRequests") : nil
        }
        let d = Task { try await client.send("a.b", .string("D"), ack: true) }  // 직행 — 답을 기다린다
        try await waitUntil { transport.published.count == 1 }
        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        let s = Task { try await client.send("a.b", .string("S"), ack: true) }  // 끊긴 동안 → 큐
        try await waitUntil { await client.queuedCount == 1 }
        transport.setState(.connected)  // flush 가 S 를 내보내고 답을 기다린다
        try await waitUntil { transport.published.count == 2 }
        await dGate.open()  // D: TooManyRequests — 큐로, 1초 백오프
        try await waitUntil { await client.queuedCount == 2 }  // D 가 S 앞에 든 뒤에 S 의 답을 연다(고정 50 ms 대신)
        await sGate.open()  // S: 성공
        try await bounded(s)
        try await bounded(d, seconds: 4)
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertEqual(transport.published.map { $0.value }, [.string("D"), .string("S"), .string("D")])
    }

    /// 직행 ack D 와 flush 가 내보낸 S 가 둘 다 TooManyRequests. D 는 순번대로 S 앞에 든다 — 서버가 보는 순서는 [D, S, D, S] 이고
    /// 마지막 값은 새 S 다. 내보낸 항목 뒤로 미루던 종전에는 [D, S, S, D] 로 낡은 D 가 마지막이었다(TS 는 앞).
    func testDapeulGidarineunFlushHangmokGwaGeojeoldoenJikhaengIDulDaGeojeoldoemyeonSunbeondaeroDasiNaganda() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        try await Task.sleep(nanoseconds: 100_000_000)
        let dGate = Gate()
        let sGate = Gate()
        let once = RejectOnce()
        transport.setAckWait { input in
            input.value == .string("D") ? dGate : input.value == .string("S") ? sGate : nil
        }
        transport.setResultFor { input in
            if input.value == .string("D") && once.first(0) { return PublishResult(ok: false, code: "TooManyRequests") }
            if input.value == .string("S") && once.first(1) { return PublishResult(ok: false, code: "TooManyRequests") }
            return nil
        }
        let d = Task { try await client.send("a.b", .string("D"), ack: true) }  // 직행 — 답을 기다린다
        try await waitUntil { transport.published.count == 1 }
        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        let s = Task { try await client.send("a.b", .string("S"), ack: true) }  // 끊긴 동안 → 큐
        try await waitUntil { await client.queuedCount == 1 }
        transport.setState(.connected)  // flush 가 S 를 내보내고 답을 기다린다
        try await waitUntil { transport.published.count == 2 }
        await dGate.open()  // D: TooManyRequests — 순번대로 S 앞에, 1초 백오프
        try await waitUntil { await client.queuedCount == 2 }
        await sGate.open()  // S: TooManyRequests — 그 자리에 남는다
        try await bounded(d, seconds: 4)
        try await bounded(s, seconds: 4)
        XCTAssertEqual(transport.published.map { $0.value }, [.string("D"), .string("S"), .string("D"), .string("S")])
    }

    /// 보내던 fire-and-forget 이 끝난 뒤에 NotConnected · TooManyRequests 로 돌아왔다 — 큐에 넣지 않고 그냥 끝난다. 호출자에게 던질
    /// 일이 아니다(보내던 중이었다). ack 발행만 SessionEnded 를 받는다.
    func testKkeunnanDwieDoraOnFireAndForgetJikhaengEunDeonjiJiAnneunda() async throws {
        for code in ["NotConnected", "TooManyRequests"] {
            let transport = FakeTransport()
            let client = ConnectClient(transport: transport, exclusiveName: false)
            try await client.open()
            try await Task.sleep(nanoseconds: 50_000_000)
            let gate = Gate()
            transport.holdPublishes(gate)
            transport.setResult(PublishResult(ok: false, code: code))
            let sending = Task { try await client.send("a.b", 1) }  // 직행 — 트랜스포트 호출이 붙잡혀 있다
            try await waitUntil { transport.published.count == 1 }
            await client.close()
            await gate.open()
            let outcome = try await bounded(Task { () async throws -> Result<Void, Error> in await sending.result })
            XCTAssertNil(outcome.failureError, "\(code): \(outcome)")
            let left = await client.queuedCount
            XCTAssertEqual(left, 0, code)
            let pending = await client.backoffPendingForTest
            XCTAssertFalse(pending, code)
        }
    }

    /// 재합류는 다른 participantId 를 받을 수 있다(guest JWT 시한 6시간 · 쿠키 유실). 처음 합류의 me 를 붙잡아 두던 종전에는 roster
    /// 에서 나를 못 찾아 "부재 = 활성" 으로 흘렀고(서버가 대기로 판정한 인스턴스가 계속 발신했다), listeners 는 나를 다른 참여자로
    /// 셌다. 트랜스포트가 그 합류의 connected 직전에 onJoined 로 새 나를 알린다.
    func testJaehapryuGaSaeParticipantIdReulJumyeonMeWaDaegiPanjeongGwaListenersGaSaeNareulBonda() async throws {
        let transport = FakeTransport()
        transport.setRoster([meActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        await settle(client, false)
        let first = await client.me?.participantId
        XCTAssertEqual(first, "me-1")

        let me2 = Member(participantId: "me-2", displayName: "unity-screen", standby: true, receives: ["robot.status"])
        let exact = Member(participantId: "p-exact", displayName: "exact", receives: ["robot.status"], strictRouting: true)
        transport.setRoster([me2, otherActive, exact])
        transport.setState(.reconnecting)
        transport.handlers.onJoined?(Member(participantId: "me-2", displayName: "unity-screen"))
        transport.setState(.connected)
        try await waitUntil { await client.me?.participantId == "me-2" }
        await settle(client, true)
        let standby = await client.standby
        XCTAssertTrue(standby, "the server marked the new self standby — the client must see it")
        do {
            try await client.send("a.b", 1, ack: true)
            XCTFail("expected Standby")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "Standby")
        }
        let listeners = try await client.listeners("robot.status")
        XCTAssertEqual(listeners.declared.map(\.participantId), ["p-exact"])  // 새 나는 빠진다
    }

    /// 사다리는 서버가 답한 ack 의 flush 성공으로만 내려온다(TS · C# 와 같다). 직행 fire-and-forget 의 ok 는 내보냈다는 뜻뿐이라,
    /// 리셋하면 거절이 이어져도 사다리가 1초 위로 자라지 못했다.
    func testJikhaengSeonggongEunBackoffSadarireulResetHajiAnneunda() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        try await Task.sleep(nanoseconds: 100_000_000)
        let once = RejectOnce()
        transport.setResultFor { input in
            input.value == .int(1) && once.first(0) ? PublishResult(ok: false, code: "TooManyRequests") : nil
        }
        try await client.send("a.b", 1)  // 직행 → TooManyRequests → 1초 백오프
        let first = await client.backoffForTest
        XCTAssertEqual(first, 1)
        // flush 가 1 을 다시 보낸다(fire-and-forget — 사다리는 그대로). 고정 1.3초 대신 다시 나가 큐가 빈 것을 본다 — 그 전에
        // 보내면 2 가 직행하지 않고 큐로 가 이 시험이 보려는 것(직행 성공)이 사라진다.
        try await resent(client, transport, count: 2)
        try await client.send("a.b", 2)  // 직행 성공
        try await waitUntil { transport.published.count == 3 }
        let after = await client.backoffForTest
        XCTAssertEqual(after, 1)
    }

    /// 직행 ack 의 답(다시 보낼 것)이 대기가 된 **뒤에** 닿았다 — 큐에 되돌리면 대기 내내 매달리다 승격 때 낡은 값으로 다시 나갔다.
    /// 대기 규칙으로 끝낸다(ack 는 Standby).
    func testDaegiGaDoenDwieOnJikhaengUiGeojeoreunKyuroDoragaJiAnneunda() async throws {
        for code in ["TooManyRequests", "NotConnected"] {
            let transport = FakeTransport()
            transport.setRoster([meActive])
            let client = ConnectClient(transport: transport, exclusiveName: true)
            try await client.open()
            try await judged(client)
            let gate = Gate()
            transport.holdAcks(gate)
            transport.setResult(PublishResult(ok: false, code: code))  // 이 가짜는 내보내는 순간에 결과를 정한다
            let d = Task { try await client.send("a.b", 1, ack: true) }  // 직행 — 답을 기다린다
            try await waitUntil { transport.published.count == 1 }
            transport.setResult(PublishResult(ok: true))
            transport.setRoster([meStandby, otherActive])
            transport.presence()
            await settle(client, true)
            await gate.open()
            let outcome = try await bounded(Task { () async throws -> Result<Void, Error> in await d.result })
            XCTAssertEqual((outcome.failureError as? AdapterError)?.code, "Standby", "\(code): \(outcome)")
            let none = await client.queuedCount
            XCTAssertEqual(none, 0, code)
            transport.setRoster([meActive])
            transport.presence()
            await settle(client, false)
            try await Task.sleep(nanoseconds: 150_000_000)
            XCTAssertEqual(transport.published.count, 1, "\(code): 승격 때 낡은 1 이 다시 나가지 않았다")
            await client.close()
        }
    }

    /// 사다리는 ack 의 flush 성공에서만 내려와, 서로 무관한 단발 거절이 1→2→4→8초로 올라갔다. 지난 백오프가 끝난 때보다 그 백오프의
    /// 두 배 넘게 뒤에 온 거절은 새 구간이다(단조 시계). 그 안에 온 거절은 같은 구간이라 한 칸 오른다. 경계를 사이에 둔 두 점(1.5배 ·
    /// 2.5배)으로 잰다 — 1배 · 4배로 바꾸면 둘 중 하나가 틀린다.
    func testJinanGeojeolBodaHanchamDwiUiGeojeoreunBackoffSadarireulCheotKanbuteoDasiOreunda() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        try await Task.sleep(nanoseconds: 100_000_000)
        let now = Counter()  // 단조 시계(ms)
        await client.setMonotonicClockForTest { UInt64(1_000 + now.value) * 1_000_000 }
        let once = RejectOnce()
        transport.setResultFor { input in
            if case .int(let v) = input.value, once.first(v) { return PublishResult(ok: false, code: "TooManyRequests") }
            return nil
        }
        try await client.send("a.b", 1)  // 거절 → 1초
        let first = await client.backoffForTest
        XCTAssertEqual(first, 1)
        // 백오프가 끝나고 1 이 다시 나간다(fire-and-forget — 사다리는 그대로). 고정 1.3초 뒤에 "끝났을 것"이라 보면 느린 러너에서
        // 2 가 직행하지 않아(쉬는 중 — 큐로) 거절을 못 받고 사다리가 오르지 않는다.
        try await resent(client, transport, count: 2)
        for _ in 0..<1_500 { now.bump() }  // 1.5초 뒤 — 같은 구간(2 × 1초 안)
        try await client.send("a.b", 2)
        let second = await client.backoffForTest
        XCTAssertEqual(second, 2)
        try await resent(client, transport, count: 4)  // 2초 백오프가 끝나고 2 가 다시 나갔다
        for _ in 0..<5_000 { now.bump() }  // 5초 뒤 = 2초 백오프의 2.5배 — 새 구간(2 × 2초 밖)
        try await client.send("a.b", 3)
        let third = await client.backoffForTest
        XCTAssertEqual(third, 1)
    }

    /// 구간의 기준은 지난 거절 답이 아니라 지난 백오프가 끝난(보내기를 다시 시작한) 때다. 답 왕복이 1.1초면 지난 답 기준으로는 1초
    /// 백오프 + 1.1초 왕복 = 2.1초가 "두 배 넘게" 라 늘 새 구간 — 사다리가 오르지 못했다([1,1,1,…]). 단조 시계만 가짜다.
    func testDapiNeuryeodoGateunGuganUiGeojeoreunBackoffSadarireulOreunda() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        try await Task.sleep(nanoseconds: 100_000_000)
        let now = Counter()  // 단조 시계(ms)
        await client.setMonotonicClockForTest { UInt64(1_000 + now.value) * 1_000_000 }
        let g1 = Gate()
        let g2 = Gate()
        let gates = GateQueue([g1, g2])
        let attempts = Counter()
        transport.setAckWait { _ in gates.next() }
        transport.setResultFor { _ in
            attempts.bump()
            return attempts.value <= 2 ? PublishResult(ok: false, code: "TooManyRequests") : nil
        }
        let a = Task { try await client.send("a.b", 1, ack: true) }  // 직행 — 답이 늦다
        try await waitUntil { transport.published.count == 1 }
        for _ in 0..<1_100 { now.bump() }  // 답 왕복 1.1초
        await g1.open()  // 거절 → 1초 백오프
        // 거절이 처리돼 백오프가 걸린 뒤에 시계를 돌린다 — 먼저 돌리면 거절이 그 뒤의 시각을 보고 구간 기준이 어긋났다.
        try await waitUntil { await client.backoffForTest >= 1 }
        for _ in 0..<1_000 { now.bump() }  // 그 1초가 지나서 백오프가 끝난다
        try await waitUntil { transport.published.count >= 2 }  // 다시 나간다
        for _ in 0..<1_100 { now.bump() }  // 다시 답 왕복 1.1초
        await g2.open()  // 거절 — 백오프가 끝난 지 1.1초(2 × 1초 안) → 같은 구간, 한 칸 오른다
        try await waitUntil { await client.backoffForTest >= 2 }
        let ladder = await client.backoffForTest
        XCTAssertEqual(ladder, 2)
        await client.close()
        _ = await a.result
    }

    private final class GateQueue: @unchecked Sendable {
        private let lock = NSLock()
        private var gates: [Gate]
        init(_ gates: [Gate]) { self.gates = gates }
        func next() -> Gate? { lock.withLock { gates.isEmpty ? nil : gates.removeFirst() } }
    }

    /// 닫기 · 세션 종료와 엇갈려 늦게 온 TooManyRequests — 끝난 클라이언트에 타이머가 남았다.
    func testKkeunnanDwieOnTooManyRequestsNeunBackoffTimerReulMandeulJiAnneunda() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        let gate = Gate()
        transport.holdAcks(gate)
        transport.setResult(PublishResult(ok: false, code: "TooManyRequests"))  // 이 가짜는 내보내는 순간에 결과를 정한다
        let sending = Task { try await client.send("a.b", 1, ack: true) }
        try await waitUntil { transport.published.count == 1 }
        await client.close()
        await gate.open()
        let outcome = try await bounded(Task { () async throws -> Result<Void, Error> in await sending.result })
        XCTAssertEqual((outcome.failureError as? AdapterError)?.code, "SessionEnded", "\(outcome)")
        let pending = await client.backoffPendingForTest
        XCTAssertFalse(pending)
    }

    /// 벽시계로 재면 시각이 뒤로 뛸 때 그만큼 다음 발행이 멈췄다(TS · Dart). 간격은 클라이언트의 단조 시계로 잰다 — 그 시계를
    /// 바꿔 끼우면 throttle 이 그것을 따른다(벽시계를 읽으면 따르지 않는다).
    func testThrottleNeunKeullaieonteuUiDanjoSigyeroGangyeogeulJaenda() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        let now = Counter()
        await client.setMonotonicClockForTest { UInt64(1_000 + now.value) * 1_000_000 }
        // 간격을 60초로 둔다 — 벽시계를 읽는 결함이면 둘째 값은 60초 뒤의 타이머로 나가 아래 조건 대기(hangGuard)에 걸린다.
        // 종전에는 100 ms 간격에 40 ms 를 쉬고 봤다 — 느린 러너에서 곧바로 나간 값이 40 ms 안에 닿지 않아 거짓 실패한다.
        let push = await client.throttle("nav.x", hz: 1 / TestTiming.farAway)
        try push(.int(1))  // 첫 값은 곧바로
        try await waitUntil { transport.published.count == 1 }
        for _ in 0..<61_000 { now.bump() }  // 단조 시계로 61초 뒤
        try push(.int(2))  // 간격이 지났다 — 곧바로 나간다(타이머를 기다리지 않는다)
        try await waitUntil { transport.published.count == 2 }
        XCTAssertEqual(transport.published.map { $0.value }, [.int(1), .int(2)])
    }

    /// 서버가 한도로 거절한 직행 발행은 그 사이 큐에 쌓인 것보다 먼저 보냈던 값이다 — 큐 끝에 붙이면 순서가 뒤집힌다(TS · Dart ·
    /// Python 과 같이 맨 앞으로).
    func testJikhaengITooManyRequestsRoDoraomyeonKyuMaenApeuroGanda() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        try await Task.sleep(nanoseconds: 100_000_000)
        let gate = Gate()
        transport.holdAcks(gate)
        // 이 가짜는 결과를 내보내는 순간에 정한다 — 1 의 답(TooManyRequests)을 미리 걸어 둔다.
        transport.setResult(PublishResult(ok: false, code: "TooManyRequests"))
        let first = Task { try await client.send("a.b", 1, ack: true) }  // 직행 — 답을 기다린다
        try await waitUntil { transport.published.count == 1 }
        transport.setResult(PublishResult(ok: true))
        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        try await client.send("a.b", 2)  // 끊긴 동안 → 큐
        await gate.open()  // 1 의 답: TooManyRequests — 큐 맨 앞으로, 1초 백오프
        // 되돌아온 것을 본 뒤에 잇는다 — 고정 50 ms 뒤에 이으면 느린 러너에서 2 가 먼저 나가 [1, 2, 1] 이 된다.
        try await waitUntil { await client.queuedCount == 2 }
        transport.setState(.connected)
        try await bounded(first, seconds: 4)
        try await waitUntil { transport.published.count == 3 }
        XCTAssertEqual(transport.published.map { $0.value }, [.int(1), .int(1), .int(2)])
    }

    /// 버킷 상한은 messageBurst(30)다 — 보충 속도(60/s)가 아니다. 쉬는 동안 60까지 차면 그다음 한꺼번에 60개가 나가 서버의
    /// 도착 기준 한도(받은 시각으로 센다)에서 몰림 하나에 넘친다.
    func testSwieodoTokeneunMessageBurstKkajiManChanda() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        try await Task.sleep(nanoseconds: 700_000_000)  // 상한이 60이면 30 + 0.7초 × 60 = 72 → 60까지 찼을 것이다
        let started = Date()
        for i in 1...60 { try await client.send("slider", .int(i)) }
        let early = transport.published.count
        // 상한은 실제 경과에서 센다 — send 루프가 쓰는 시간만큼 토큰이 다시 찬다(초당 60). 고정 +3(50 ms)은 느린 러너에서 넘는다.
        let elapsed = Date().timeIntervalSince(started)
        let ceiling = ConnectClient.messageBurst + Int(60 * elapsed) + 3
        XCTAssertTrue((ConnectClient.messageBurst...ceiling).contains(early), "early=\(early) (ceiling \(ceiling))")
    }

    func testJaejeopsokHamyeonTokeniDasiGadeukChanda() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        let burst = ConnectClient.messageBurst
        // 단조 시계를 멈춰 둔다 — 토큰은 다시 잇는 것으로만 찬다. 실제 시계로 "150 ms 안에 burst 개"를 재던 종전에는 느린
        // 러너에서 그 안에 다 나가지 못해 거짓 실패했고, 넉넉히 기다리면 다시 채우지 않는 결함도 0.5초 뒤에는 같은 수가 됐다.
        await client.setMonotonicClockForTest { 1_000_000_000 }
        for i in 0..<burst { try await client.send("burn", .int(i)) }  // 토큰을 다 쓴다
        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        for i in 1...burst { try await client.send("after", .int(i)) }  // 끊긴 동안 burst 개
        transport.setState(.connected)  // 서버의 한도는 소켓마다 새로 — burst 개가 곧바로 나간다
        // 다시 채우지 않았다면 (시계가 멈춰 있으니) 하나도 나가지 않는다.
        try await waitUntil { transport.published.filter { $0.channel == "after" }.count == burst }
        XCTAssertEqual(transport.published.filter { $0.channel == "after" }.count, burst)
    }

    /// E4: queue-overflow 를 버린 항목마다 남기면 한도를 조금 넘겨 보내는 앱이 진단 링(100칸)을 그것으로 덮는다. 구간마다 한 줄,
    /// 큐가 한도의 절반 이하로 빠진 뒤의 넘침은 새 구간이다. 이벤트(queueOverflow)는 종전처럼 항목마다다.
    func testNeomchimEunGuganMadaJindanHanJul() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport, queueLimit: 4, exclusiveName: false)
        try await client.open()
        let events = Counter()
        let stream = await client.events()
        let collector = Task { for await e in stream { if case .queueOverflow = e { events.bump() } } }
        func notes() -> [LogEntry] { client.diagnostics().filter { $0.code == "queue-overflow" } }

        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        for i in 0..<10 { try await client.send("q", .int(i)) }  // 6건 넘침
        try await waitUntil { events.value == 6 }
        XCTAssertEqual(notes().count, 1, "\(notes())")
        XCTAssertTrue(notes().last?.message.hasSuffix(" — queue full, dropping the oldest (noted once until it drains to half)") == true)

        transport.setState(.connected)  // 큐가 빈다
        try await waitUntil { transport.published.count == 4 }
        try await waitUntil { await client.queuedCount == 0 }
        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        for i in 0..<6 { try await client.send("q", .int(i)) }  // 새 구간 — 2건 넘침
        try await waitUntil { events.value == 8 }
        XCTAssertEqual(notes().count, 2, "\(notes())")
        collector.cancel()
    }

    /// D4: flush 가 내보내고 답을 기다리는 ack 항목을 넘침이 버리면, 서버가 받은 그 발행이 QueueOverflow 로 거절되고 flush 는
    /// 자리(맨 앞)로 빼다 엉뚱한 항목을 버렸다. 넘침은 그 다음 것부터 버리고, 큐는 정체로 뺀다.
    func testNeomchimEunDapeulGidarineunHangmogeulBeoriJiAnneunda() async throws {
        let transport = FakeTransport()
        let gate = Gate()
        transport.holdAcks(gate)
        let client = ConnectClient(transport: transport, queueLimit: 2, exclusiveName: false)
        try await client.open()
        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        let a1 = Task { () async throws -> String in try await client.send("q", "a1", ack: true); return "ok" }
        try await waitUntil { await client.queuedCount == 1 }
        transport.setState(.connected)  // flush 가 a1 을 내보내고 답을 기다린다
        try await waitUntil { transport.published.count == 1 }
        let a2 = Task { () async throws -> String in
            do { try await client.send("q", "a2", ack: true); return "ok" } catch let e as AdapterError { return e.code }
        }
        try await waitUntil { await client.queuedCount == 2 }
        let a3 = Task { () async throws -> String in try await client.send("q", "a3", ack: true); return "ok" }
        let overflow = try await bounded(a2)
        XCTAssertEqual(overflow, "QueueOverflow")
        await gate.open()
        let r1 = try await bounded(a1)
        XCTAssertEqual(r1, "ok")
        let r3 = try await bounded(a3)
        XCTAssertEqual(r3, "ok")
        XCTAssertEqual(transport.published.map { $0.value.stringValue }, ["a1", "a3"])
    }

    /// D6: 판정이 도는 중의 프레즌스는 "다시 판정" 표시만 남기고, 지금 판정이 닿은 뒤 한 번 더 돈다 — 갈아타지 않는다.
    func testPanjeongEunHanBeoneHanaDa() async throws {
        let transport = FakeTransport()
        transport.setRoster([meActive])
        let gate = Gate()
        let calls = Counter()
        let active = meActive
        transport.script { calls.bump(); await gate.wait(); return [active] }
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()  // 첫 판정이 붙잡혀 있다 — 창이 열려 있다
        try await client.send("a.b", 1)
        for _ in 0..<5 { transport.presence() }  // 잦은 프레즌스
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertEqual(calls.value, 1, "도는 판정은 하나뿐")
        XCTAssertEqual(transport.published.count, 0, "프레즌스 판정이 새로 돌아 창을 먼저 닫지 않았다")
        await gate.open()
        try await waitUntil { transport.published.count == 1 }
    }

    func testSesyeoniKkeunnamyeonChangAnAckBalhaengDoSessionEnded() async throws {
        let transport = FakeTransport()
        transport.setRoster([meActive])
        transport.holdRoster(true)
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()  // 창이 열린 채
        let acked = Task { () async throws -> String in
            do { try await client.send("a.b", 1, ack: true); return "ok" } catch let e as AdapterError { return e.code }
        }
        try await waitUntil { await client.queuedCount == 1 }
        await client.close()
        let outcome = try await bounded(acked)
        XCTAssertEqual(outcome, "SessionEnded")
        transport.holdRoster(false)
    }

    /// TS · Python · Dart · C# 와 같다: 클라이언트는 close() 를 부른 그 자리에서 끝나고, 소켓 정리(leave 확인)만 기다린다.
    func testCloseNeunGotbaroKkeunnaenda() async throws {
        let transport = FakeTransport()
        let leave = Gate()
        transport.holdClose(leave)
        let client = ConnectClient(transport: transport)
        try await client.open()
        let closing = Task { await client.close() }
        try await waitUntil { await client.state == .ended }
        do {
            try await client.send("a.b", 1)
            XCTFail("expected SessionEnded")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "SessionEnded")
        }
        await leave.open()
        await closing.value
    }

    func testKyuwaChangiHandoReulNanwoSseugoKyuUiGajangOraeDoenGeotButeoBeorinda() async throws {
        let transport = FakeTransport()
        transport.setRoster([meActive])
        let client = ConnectClient(transport: transport, queueLimit: 3, exclusiveName: true)
        try await client.open()
        try await judged(client)
        let events = await client.events()
        let dropped = Box2<[String]>()
        dropped.set([])
        Task { for await e in events { if case .queueOverflow(let input) = e { dropped.set((dropped.get() ?? []) + [input.value.stringValue ?? "?"]) } } }

        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        try await client.send("a.b", "q1")
        try await client.send("a.b", "q2")
        transport.holdRoster(true)
        transport.setState(.connected)  // 판정이 붙잡혀 창이 열려 있다
        try await waitUntil { await client.state == .connected }
        try await client.send("a.b", "w1")
        try await client.send("a.b", "w2")  // 4 > 3 — 큐의 가장 오래된 q1 을 버린다
        try await client.send("a.b", "w3")  // 다시 넘친다 — q2
        let count = await client.queuedCount
        XCTAssertEqual(count, 3, "창 안의 발행도 센다")
        try await waitUntil { (dropped.get() ?? []).count == 2 }
        XCTAssertEqual(dropped.get() ?? [], ["q1", "q2"])
        transport.holdRoster(false)
        try await waitUntil { transport.published.count == 3 }
        XCTAssertEqual(transport.published.map { $0.value.stringValue }, ["w1", "w2", "w3"])
    }

    private final class Counter: @unchecked Sendable {
        private let lock = NSLock()
        private var count = 0
        func bump() { lock.withLock { count += 1 } }
        var value: Int { lock.withLock { count } }
    }

    /// 기한 안에 끝나기를 기다린다 — 결함이 있으면 매달리지 않고 실패한다. 기한은 TestTiming.hangGuard 이상이다.
    @discardableResult
    private func bounded<T: Sendable>(_ task: Task<T, Error>, seconds: Double = 3) async throws -> T {
        let deadline = TestTiming.deadline(seconds)
        let done = Box2<T>()
        Task { done.set(try? await task.value) }
        while done.get() == nil && Date() < deadline { try await Task.sleep(nanoseconds: 10_000_000) }
        guard done.get() != nil else {
            XCTFail("did not complete within the bound")
            throw AdapterError("Timeout", "bounded wait")
        }
        return try await task.value
    }

    private final class Box2<T>: @unchecked Sendable {
        private let lock = NSLock()
        private var value: T?
        func set(_ v: T?) { lock.withLock { value = v } }
        func get() -> T? { lock.withLock { value } }
    }

    /// 한 번 열리는 문 — 응답 하나를 붙잡았다 놓는다.
    fileprivate actor Gate {
        private var opened = false
        private var waiters: [CheckedContinuation<Void, Never>] = []
        func wait() async {
            if opened { return }
            await withCheckedContinuation { waiters.append($0) }
        }
        func open() {
            opened = true
            waiters.forEach { $0.resume() }
            waiters.removeAll()
        }
    }

    /// 조건이 참이 될 때까지 — TestTiming.hangGuard 까지(종전의 2초는 멈춘 러너에서 모자랐다).
    private func waitUntil(
        file: StaticString = #filePath, line: UInt = #line, _ condition: () async -> Bool
    ) async throws {
        let met = await TestTiming.until(0, pollNanos: 5_000_000, condition)
        XCTAssertTrue(met, "condition not met within timeout", file: file, line: line)
    }

    /// 백오프가 끝나 큐에 든 것이 다시 나갔다 — 발행이 `count` 건이 되고, 큐가 비고, 쉬는 중이 아니다(다음 발행이 직행한다).
    private func resent(_ client: ConnectClient, _ transport: FakeTransport, count: Int) async throws {
        try await waitUntil {
            guard transport.published.count >= count else { return false }
            let queued = await client.queuedCount
            let pending = await client.backoffPendingForTest
            return queued == 0 && !pending
        }
    }

    func testJoyonghanJaejoinEseodoDaegiReulAnda() async throws {
        // 내 재조인은 **나에게** 프레즌스를 주지 않고, joined.me 에는 standby 가 없다.
        // roster 를 직접 읽지 않으면 대기 자리에 들어간 줄 영영 모른다.
        let transport = FakeTransport()
        transport.setRoster([meActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        let before = await client.standby
        XCTAssertFalse(before)

        transport.setState(.reconnecting)
        transport.setRoster([meStandby, otherActive])
        transport.setState(.connected)  // 프레즌스 이벤트는 **없다**

        await settle(client, true)
        let after = await client.standby
        XCTAssertTrue(after, "조용한 재조인에서도 대기를 알아야 한다")
        try await client.send("a.b", 99)
        XCTAssertEqual(transport.published.count, 0)
    }

    func testJaejeopsokFlushGaDaegiPanjeongEulApjiriMotHagoDaegimyeonKkeunkinDongananKyuneunBeoryeojinda() async throws {
        let transport = FakeTransport()
        transport.setRoster([meActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()

        transport.setState(.reconnecting)
        // 끊김이 반영된 뒤에 보낸다 — 고정 30 ms 뒤에 보내면 느린 러너에서 직행해 아래 0 이 깨진다.
        try await waitUntil { await client.state == .reconnecting }
        let queued = Task { try await client.send("a.b", 42) }
        try await waitUntil { await client.queuedCount == 1 }
        XCTAssertEqual(transport.published.count, 0, "끊긴 동안에는 나가지 않는다")

        // 끊긴 사이 다른 인스턴스가 활성을 가져갔다.
        transport.setRoster([meStandby, otherActive])
        transport.setState(.connected)
        await settle(client, true)
        try await Task.sleep(nanoseconds: 120_000_000)
        XCTAssertEqual(transport.published.count, 0, "대기 중에는 큐 경유도 막혀야 한다")
        // D3': 대기 판정이 끊긴 동안의 큐를 버렸다 — fire-and-forget 은 그냥 끝나고 개수는 standby 기록 한 줄에 남는다.
        _ = try await bounded(queued)
        let emptied = await client.queuedCount
        XCTAssertEqual(emptied, 0)
        XCTAssertTrue(
            client.diagnostics().contains { $0.code == "standby" && $0.message.hasSuffix(" — dropped 1 queued send") },
            "\(client.diagnostics())")

        // 승격돼도 낡은 값은 나가지 않는다 — 그동안 다른 인스턴스가 활성 발신자였다(되돌리면 더 새 상태를 덮는다).
        transport.setRoster([meActive])
        transport.presence()
        await settle(client, false)
        try await Task.sleep(nanoseconds: 150_000_000)
        XCTAssertEqual(transport.published.count, 0, "승격 때 낡은 값이 나갔다")
    }

    /// D3': 대기가 되는 판정이면 경로와 상관없이(접속 판정 · 프레즌스 판정) 남은 큐를 버린다 — 다른 인스턴스가 활성 발신자다.
    /// 이미 내보내고 답을 기다리는 항목(inFlight)은 그 결과에 맡긴다(D4 와 같은 규칙 — 서버가 이미 받았을 수 있다).
    func testPresenceroDaegiGaDoeeodoNameunKyuneunBeorigoDapeulGidarineunHangmogeunGyeolgwaeMatginda() async throws {
        let transport = FakeTransport()
        transport.setRoster([meActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        try await judged(client)  // 첫 판정(활성)이 끝나게

        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        let a = Task { try await client.send("a.b", 1, ack: true) }  // 재접속 뒤 flush 가 내보내고 답을 기다릴 항목
        try await waitUntil { await client.queuedCount == 1 }
        try await client.send("a.b", 2)
        let c = Task { try await client.send("a.b", 3, ack: true) }
        try await waitUntil { await client.queuedCount == 3 }
        let gate = Gate()
        transport.holdAcks(gate)
        transport.setState(.connected)  // 활성 판정 → 창이 닫히고 flush 가 1 을 내보내 답을 기다린다
        try await waitUntil { transport.published.count == 1 }

        transport.setRoster([meStandby, otherActive])
        transport.presence()  // 프레즌스 판정이 대기로 바꾼다
        await settle(client, true)
        // bounded 는 던진 작업을 끝난 것으로 보지 않는다 — 결과로 감싸 기다린다.
        let refused = try await bounded(Task { () async throws -> Result<Void, Error> in await c.result })
        XCTAssertEqual((refused.failureError as? AdapterError)?.code, "Standby", "\(refused)")
        XCTAssertTrue(
            client.diagnostics().contains { $0.code == "standby" && $0.message.hasSuffix(" — dropped 2 queued sends") },
            "\(client.diagnostics())")
        let one = await client.queuedCount
        XCTAssertEqual(one, 1, "답을 기다리는 항목은 버리지 않았다")

        // D3: 대기 중의 새 발행은 직행 경로의 대기 규칙 — 큐 뒤에 붙지 않는다(붙으면 승격 때 낡은 값이 나간다). 큐로 가면
        // ack 발행이 매달리므로 결과로 감싸 시한 안에 기다린다.
        try await client.send("a.b", 4)
        let fifth = try await bounded(Task { () async throws -> Result<Void, Error> in
            await Task { try await client.send("a.b", 5, ack: true) }.result
        })
        XCTAssertEqual((fifth.failureError as? AdapterError)?.code, "Standby", "\(fifth)")
        let stillOne = await client.queuedCount
        XCTAssertEqual(stillOne, 1)

        await gate.open()  // 답이 닿았다 — 그 결과(성공)대로 끝난다
        try await bounded(a)
        let none = await client.queuedCount
        XCTAssertEqual(none, 0)

        transport.setRoster([meActive])
        transport.presence()
        await settle(client, false)
        try await Task.sleep(nanoseconds: 150_000_000)
        XCTAssertEqual(transport.published.map { $0.value }, [.int(1)], "낡은 2 · 3 · 4 · 5 는 없다")
        try await client.send("a.b", 6)
        try await waitUntil { transport.published.count == 2 }
        XCTAssertEqual(transport.published.map { $0.value }, [.int(1), .int(6)])
    }

    /// E5: 대기가 될 때 답을 기다리던 항목은 그 결과에 맡기는데, 그 결과가 TooManyRequests · AckTimeout(다시 보낼 것)이면 다시
    /// 보낼 차례는 승격 뒤다 — 그때 이 값은 낡았다(D3' 와 같은 이유). 큐에 남기면 대기 내내 매달리다 승격 때 다시 나갔다
    /// (published = [1, 1]). 대기 규칙으로 끝낸다(ack 는 Standby).
    func testDaegiJungeDapITooManyRequestsNaAckTimeoutInHangmogeunStandbyroKkeunnanda() async throws {
        for code in ["TooManyRequests", "AckTimeout"] {
            let transport = FakeTransport()
            transport.setRoster([meActive])
            let client = ConnectClient(transport: transport, exclusiveName: true)
            try await client.open()
            try await judged(client)

            transport.setState(.reconnecting)
            try await waitUntil { await client.state == .reconnecting }
            let a = Task { try await client.send("a.b", 1, ack: true) }
            try await waitUntil { await client.queuedCount == 1 }
            let gate = Gate()
            transport.holdAcks(gate)
            // 이 가짜는 결과를 내보내는 순간에 정한다 — 1 의 답을 미리 걸어 둔다.
            transport.setResult(PublishResult(ok: false, code: code))
            transport.setState(.connected)
            try await waitUntil { transport.published.count == 1 }  // 1 을 내보내고 답을 기다린다

            transport.setRoster([meStandby, otherActive])
            transport.presence()
            await settle(client, true)
            await gate.open()  // 답: 다시 보낼 것 — 그러나 지금은 대기다
            let outcome = try await bounded(Task { () async throws -> Result<Void, Error> in await a.result })
            XCTAssertEqual((outcome.failureError as? AdapterError)?.code, "Standby", "\(code): \(outcome)")
            transport.setResult(PublishResult(ok: true))
            let none = await client.queuedCount
            XCTAssertEqual(none, 0, code)

            transport.setRoster([meActive])
            transport.presence()
            await settle(client, false)
            try await Task.sleep(nanoseconds: 150_000_000)
            XCTAssertEqual(transport.published.count, 1, "\(code): 승격 때 낡은 1 이 다시 나가지 않았다")
            await client.close()
        }
    }

    /// flush 가 ack 를 기다리는 사이 끊겼다 다시 붙으면 새 판정 창이 열려 있다 — 답이 닿아 루프가 이어 가도 판정 전에는 더
    /// 내보내지 않는다(루프 조건이 창을 매번 본다). 판정이 대기면 남은 것은 버려진다(D3').
    func testDapeulGidarideonFlushNeunSaePanjeongChangAneseoIeoGajiAnneunda() async throws {
        let transport = FakeTransport()
        transport.setRoster([meActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        try await judged(client)

        transport.setState(.reconnecting)
        try await waitUntil { await client.state == .reconnecting }
        let a = Task { try await client.send("a.b", 1, ack: true) }
        try await waitUntil { await client.queuedCount == 1 }
        try await client.send("a.b", 2)
        let ack = Gate()
        transport.holdAcks(ack)
        transport.setState(.connected)
        try await waitUntil { transport.published.count == 1 }  // 1 을 내보내고 답을 기다린다

        transport.holdRoster(true)
        transport.setRoster([meStandby, otherActive])
        transport.setState(.reconnecting)
        // 전이는 소켓 쪽 FIFO 를 거쳐 액터의 드레인 태스크가 처리한다 — 끊김이 반영된 것을 본 뒤에 connected 를 넣어야 아래의
        // connected 대기가 **이 재접속**의 새 창(판정 붙잡힘)이 열린 뒤를 본다. 종전에는 둘을 잇달아 넣고 곧바로 connected 를
        // 기다렸는데, 클라이언트는 첫 접속부터 connected 라 그 대기가 바로 지나갔고, 드레인이 밀린 CI 에서는 그 뒤에 연 답을
        // flush 루프가 창이 열리기 전에 받아 2 를 내보냈다(2026-10-25 CI macos-14; 사본의 드레인에 30 ms 를 끼우면 3/3 재현).
        try await waitUntil { await client.state == .reconnecting }
        transport.setState(.connected)  // 새 판정 창 — 판정이 붙잡혀 있다
        try await waitUntil { await client.state == .connected }
        await ack.open()  // 답이 닿아 flush 루프가 깬다
        try await bounded(a)
        try await Task.sleep(nanoseconds: 150_000_000)
        XCTAssertEqual(transport.published.count, 1, "창 안에서는 2 를 내보내지 않는다")

        transport.holdRoster(false)  // 판정: 대기 — 남은 2 는 버려진다
        await settle(client, true)
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertEqual(transport.published.count, 1)
        let none = await client.queuedCount
        XCTAssertEqual(none, 0)
        XCTAssertTrue(
            client.diagnostics().contains { $0.code == "standby" && $0.message.hasSuffix(" — dropped 1 queued send") },
            "\(client.diagnostics())")
    }

    func testRosterJohoeSilpaeNeunSangtaeReulDwijibjiAnneunda() async throws {
        let transport = FakeTransport()
        transport.setRoster([meActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        transport.failRoster()
        transport.presence()
        try await Task.sleep(nanoseconds: 120_000_000)
        let standby = await client.standby
        XCTAssertFalse(standby, "조회 실패를 판정 근거로 쓰지 않는다")
    }

    func testDaegiWaSeunggyeokIJindanEnamneunda() async throws {
        let transport = FakeTransport()
        transport.setRoster([meStandby, otherActive])
        let client = ConnectClient(transport: transport, exclusiveName: true)
        try await client.open()
        await settle(client, true)
        transport.setRoster([meActive])
        transport.presence()
        await settle(client, false)
        try await Task.sleep(nanoseconds: 60_000_000)
        let codes = client.diagnostics().map { $0.code }
        XCTAssertTrue(codes.contains("standby"), "대기 진입이 기록되어야 한다: \(codes)")
        XCTAssertTrue(codes.contains("active"), "승격이 기록되어야 한다: \(codes)")
    }
}

extension Result {
    /// 실패면 그 오류 — 시험에서 거절 코드를 읽는다.
    fileprivate var failureError: Failure? {
        if case .failure(let error) = self { return error }
        return nil
    }
}
