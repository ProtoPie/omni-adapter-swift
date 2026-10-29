import XCTest
@testable import OmniAdapter

/// 화면 송출 헬퍼 — Kotlin `ScreenBroadcastTest.kt` / C# `ScreenBroadcastTests.cs` 미러.
///
/// 잠그는 것은 **조용히 실패하는 지점**들이다:
///   ① `ready` 를 접속마다(재접속 포함) 보낸다 — 안 보내면 뷰어가 어댑터를 영영 못 찾고
///      증상은 "새로고침하면 된다" 로만 나타난다
///   ② 우리는 항상 answer 만 한다
///   ③ 세션이 nil 이면 답하지 않는다
///   ④ 한 뷰어의 실패가 다른 뷰어를 끊지 않는다
///   ⑤ 원격 조작은 보유자 게이트(N4)·방향 화이트리스트(N2)·clamp(N6)·
///      포인터 id 검증(N7)·보유자 이탈 시 cancel 합성(N7)을 지킨다
final class ScreenBroadcastTests: XCTestCase {

    private final class FakeTransport: Transport, @unchecked Sendable {
        private let lock = NSLock()
        private var handlersStorage = TransportHandlers()
        private var sentStorage: [[String: Any]] = []
        let me: Member

        /// 기본은 화면을 신고한 어댑터(`providesView: "stream"`) — 신고 경고(`stream-undeclared`)는 따로 시험한다.
        init(providesView: String? = "stream") {
            me = Member(participantId: "me-1", displayName: "unity-screen", providesView: providesView)
        }

        var handlers: TransportHandlers { lock.withLock { handlersStorage } }
        var sent: [[String: Any]] { lock.withLock { sentStorage } }
        var kinds: [String] { sent.compactMap { $0["kind"] as? String } }
        var answers: [[String: Any]] { sent.filter { ($0["kind"] as? String) == "sdp" } }

        func open(handlers: TransportHandlers) async throws -> Joined {
            lock.withLock { handlersStorage = handlers }
            return Joined(roomId: "stage-1", me: me)
        }
        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult {
            PublishResult(ok: true)
        }
        func listMembers() async throws -> [Member] { [me] }
        func sendSignaling(_ payload: [String: Any]) async {
            lock.withLock { sentStorage.append(payload) }
        }
        /// ICE 조회를 이만큼 붙잡는다(ms) — 0 이 아니면 결과가 이미 만료라 클라이언트가 캐시하지 않는다(매 offer 가 기다린다).
        private var iceDelayMsStorage: UInt64 = 0
        var iceDelayMs: UInt64 {
            get { lock.withLock { iceDelayMsStorage } }
            set { lock.withLock { iceDelayMsStorage = newValue } }
        }
        /// ICE 서버 목록(만료된 결과라 매번 다시 묻는다) — 설정하면 이것을 돌려준다.
        private var iceListStorage: [IceServer]?
        var iceList: [IceServer]? {
            get { lock.withLock { iceListStorage } }
            set { lock.withLock { iceListStorage = newValue } }
        }
        /// ICE 조회가 답하지 않는다(시한 시험).
        private var iceHangsStorage = false
        var iceHangs: Bool {
            get { lock.withLock { iceHangsStorage } }
            set { lock.withLock { iceHangsStorage = newValue } }
        }
        /// ICE 조회가 실패한다 — 실제 트랜스포트처럼 실패 표시의 빈 목록.
        private var iceFailsStorage = false
        var iceFails: Bool {
            get { lock.withLock { iceFailsStorage } }
            set { lock.withLock { iceFailsStorage = newValue } }
        }
        func fetchIceServers() async -> IceServersResult {
            if iceHangs { try? await Task.sleep(nanoseconds: 30_000_000_000) }
            if iceFails { return .lookupFailed }
            if let list = iceList { return IceServersResult(iceServers: list, expiresAt: 1) }
            let delay = iceDelayMs
            if delay == 0 { return IceServersResult(iceServers: [], expiresAt: nil) }
            try? await Task.sleep(nanoseconds: delay * 1_000_000)
            return IceServersResult(iceServers: [], expiresAt: 1)
        }
        func close() async {}

        func setState(_ state: TransportState) { handlers.onState?(state) }
        /// 다시 붙었다 — 그 합류의 나(participantId 가 바뀌었을 수 있다)를 먼저 알리고 connected.
        func rejoin(_ newMe: Member) {
            handlers.onState?(.reconnecting)
            joinedAndConnected(newMe)
        }
        /// 이미 끊긴 뒤 — 합류의 나를 알리고 connected.
        func joinedAndConnected(_ newMe: Member) {
            handlers.onJoined?(newMe)
            handlers.onState?(.connected)
        }
        func signal(_ frame: [String: Any]) { handlers.onSignaling?(frame) }
        func holder(_ id: String?, ttlMs: Int? = 30_000, registrationId: String = "r1") {
            var frame: [String: Any] = ["registrationId": registrationId]
            if let ttlMs { frame["ttlMs"] = ttlMs }
            if let id { frame["holderParticipantId"] = id }
            handlers.onAdapterLeaseHolder?(frame)
        }
        func message(_ channel: String) {
            handlers.onMessage?(StageMessage(channel: channel, value: 1, sourceParticipantId: "peer-1", timestamp: 1))
        }
        /// 등록하면 `lastRegistrationId` 가 r1, 다시 등록하면 r2 … 가 된다 — 다른 등록의 통지는 판정에 걸러진다.
        private var registrations = 0
        func submitRegistrationCode(_ code: String) async throws -> AdapterRegistrationResult {
            let n = lock.withLock { () -> Int in
                registrations += 1
                return registrations
            }
            return AdapterRegistrationResult(registrationId: "r\(n)", deviceSecret: "secret")
        }
    }

    private final class FakeSession: BroadcastSession, @unchecked Sendable {
        private let lock = NSLock()
        private let failAnswer: Bool
        private let answerDelayMs: UInt64
        private var closedStorage = false
        private var candidatesStorage: [[String: Any]] = []
        private var offersStorage: [String] = []
        private var afterOfferStorage: [Bool] = []
        private var answeringStorage = false
        private var appliedStorage = false
        private var answerGateStorage: AsyncGate?
        private var iceHoldStorage: AsyncGate?
        private var iceFailsStorage = false
        private var onIceStorage: (@Sendable (String) -> Void)?

        init(failAnswer: Bool = false, answerDelayMs: UInt64 = 0) {
            self.failAnswer = failAnswer
            self.answerDelayMs = answerDelayMs
        }

        /// 설정하면 answer 가 이것이 열릴 때까지 멎는다 — offer 를 적용하는 중을 만든다.
        var answerGate: AsyncGate? {
            get { lock.withLock { answerGateStorage } }
            set { lock.withLock { answerGateStorage = newValue } }
        }
        /// 설정하면 후보 적용이 이것이 열릴 때까지 멎는다 — 늦은 실패를 만든다.
        var iceHold: AsyncGate? {
            get { lock.withLock { iceHoldStorage } }
            set { lock.withLock { iceHoldStorage = newValue } }
        }
        /// 후보 적용이 실패한다(ice-candidate 시험).
        var iceFails: Bool {
            get { lock.withLock { iceFailsStorage } }
            set { lock.withLock { iceFailsStorage = newValue } }
        }
        /// 후보를 받을 때마다(기록한 뒤) — 넘기는 도중에 새 후보를 끼워 넣는 시험용.
        var onIce: (@Sendable (String) -> Void)? {
            get { lock.withLock { onIceStorage } }
            set { lock.withLock { onIceStorage = newValue } }
        }
        /// answer 를 시작했다.
        var answering: Bool { lock.withLock { answeringStorage } }

        var closed: Bool { lock.withLock { closedStorage } }
        var candidates: [[String: Any]] { lock.withLock { candidatesStorage } }
        /// 후보마다 — 그때 이미 offer 를 적용했는가(answer 가 끝난 뒤인가).
        var afterOffer: [Bool] { lock.withLock { afterOfferStorage } }
        /// 받은 후보의 `candidate` 문자열(도착 순서).
        var candidateNames: [String] { candidates.compactMap { $0["candidate"] as? String } }
        /// 이 세션이 받은 offer SDP 들.
        var offers: [String] { lock.withLock { offersStorage } }

        func answer(offerSdp: String) async throws -> String {
            let gate = lock.withLock { () -> AsyncGate? in
                offersStorage.append(offerSdp)
                answeringStorage = true
                return answerGateStorage
            }
            if let gate { await gate.wait() }
            if answerDelayMs > 0 { try? await Task.sleep(nanoseconds: answerDelayMs * 1_000_000) }
            if failAnswer { throw AdapterError("ConfigError", "no encoder") }
            lock.withLock { appliedStorage = true }
            return "answer:\(offerSdp)"
        }
        func addIceCandidate(_ candidate: [String: Any]) async throws {
            let (hook, hold, fails) = lock.withLock { () -> ((@Sendable (String) -> Void)?, AsyncGate?, Bool) in
                candidatesStorage.append(candidate)
                afterOfferStorage.append(appliedStorage)
                return (onIceStorage, iceHoldStorage, iceFailsStorage)
            }
            hook?(candidate["candidate"] as? String ?? "")
            if let hold { await hold.wait() }
            if fails { throw AdapterError("ValidationError", "bad candidate") }
        }
        func close() async { lock.withLock { closedStorage = true } }
    }

    private final class FakeInputChannel: RemoteInputChannel, @unchecked Sendable {
        private let lock = NSLock()
        let label: String
        private var sentStorage: [String] = []
        private var listener: (@Sendable (String) -> Void)?

        init(label: String) { self.label = label }
        var sent: [String] { lock.withLock { sentStorage } }
        private var failSendStorage = false
        var failSend: Bool {
            get { lock.withLock { failSendStorage } }
            set { lock.withLock { failSendStorage = newValue } }
        }
        func send(_ data: String) throws {
            if failSend { throw AdapterError("NotConnected", "channel closed") }
            lock.withLock { sentStorage.append(data) }
        }
        func onMessage(_ listener: @escaping @Sendable (String) -> Void) {
            lock.withLock { self.listener = listener }
        }
        func push(_ frame: String) {
            let l = lock.withLock { listener }
            l?(frame)
        }
    }

    private static func offer(_ from: String, sdp: String = "offer-sdp") -> [String: Any] {
        [
            "kind": "sdp", "fromParticipantId": from,
            "description": ["type": "offer", "sdp": sdp],
        ]
    }

    private func openClient(_ transport: FakeTransport) async throws -> ConnectClient {
        let client = ConnectClient(transport: transport)
        try await client.open()
        return client
    }

    private func settle(_ ms: UInt64 = 150) async {
        try? await Task.sleep(nanoseconds: ms * 1_000_000)
    }

    /// 조건이 참이 될 때까지 짧게 폴링한다 — 벽시계 간격 대신 관측으로 순서를 세운다(시한을 넘기면 실패).
    private func waitUntil(
        timeout: TimeInterval = 5, file: StaticString = #filePath, line: UInt = #line, _ condition: () async -> Bool
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if await condition() { return }
            try await Task.sleep(nanoseconds: 5_000_000)
        }
        XCTFail("condition not met within \(timeout) s", file: file, line: line)
        throw AdapterError("Timeout", "waitUntil")
    }

    /// 뷰어가 offer 에 실은 협상 번호(문자열 · 숫자)를 answer 와 그 협상의 ICE 에 그대로 돌려준다 — 뷰어는 다른 협상의 답을
    /// 버린다. 번호가 없는 offer(옛 뷰어)에는 싣지 않는다.
    func testEchoesTheOffersNegotiationIdInTheAnswerAndItsIce() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let wiring = WiringLog()
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, sendIce, _, wireInput in
                wiring.add(sendIce: sendIce, wireInput: wireInput)
                return FakeSession()
            })
        await settle()
        var tagged = Self.offer("v1")
        tagged["negotiationId"] = "n-7"
        transport.signal(tagged)
        var numbered = Self.offer("v2")
        numbered["negotiationId"] = 42
        transport.signal(numbered)
        transport.signal(Self.offer("v3"))
        var waited = 0
        while transport.answers.count < 3, waited < 60 {
            await settle(50)
            waited += 1
        }
        guard transport.answers.count == 3, wiring.count == 3 else {
            return XCTFail("answers \(transport.answers.count), sessions \(wiring.count)")
        }
        for i in 0..<3 { wiring.sendIce(i)?(["candidate": "c\(i)"]) }
        await settle(200)

        func frame(_ kind: String, to viewer: String) -> [String: Any]? {
            transport.sent.first { ($0["kind"] as? String) == kind && ($0["targetParticipantId"] as? String) == viewer }
        }
        XCTAssertEqual(frame("sdp", to: "v1")?["negotiationId"] as? String, "n-7")
        XCTAssertEqual(frame("ice", to: "v1")?["negotiationId"] as? String, "n-7")
        XCTAssertEqual((frame("sdp", to: "v2")?["negotiationId"] as? NSNumber)?.intValue, 42)
        XCTAssertEqual((frame("ice", to: "v2")?["negotiationId"] as? NSNumber)?.intValue, 42)
        XCTAssertNotNil(frame("sdp", to: "v3"))
        XCTAssertNil(frame("sdp", to: "v3")?["negotiationId"], "번호 없는 offer 의 answer 에 번호를 실었다")
        XCTAssertNil(frame("ice", to: "v3")?["negotiationId"], "번호 없는 offer 의 ICE 에 번호를 실었다")
        await b.stop()
    }

    /// 보낼 화면이 시작 뒤에 생겼다 — sourceChanged() 가 ready 를 다시 알린다. 끊긴 동안은 알리지 않는다(다시 붙으면 어차피
    /// 알린다).
    func testSourceChangedAnnouncesReadyAgain() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { _, _, _, _ in nil })
        await settle()
        let before = transport.kinds.filter { $0 == "ready" }.count
        XCTAssertEqual(before, 1)
        await b.sourceChanged()
        XCTAssertEqual(transport.kinds.filter { $0 == "ready" }.count, 2)
        transport.setState(.reconnecting)
        await settle()
        await b.sourceChanged()
        XCTAssertEqual(transport.kinds.filter { $0 == "ready" }.count, 2, "끊긴 동안 ready 를 보냈다")
        await b.stop()
        await b.sourceChanged()
        XCTAssertEqual(transport.kinds.filter { $0 == "ready" }.count, 2, "멈춘 뒤 ready 를 보냈다")
    }

    /// 화면을 신고하지 않은 어댑터가 송출을 시작했다 — 서버는 그런 어댑터에게 레이어를 만들지 않는다. 진단 링에 한 번 남긴다.
    func testAnUndeclaredAdapterIsWarnedOnce() async throws {
        let transport = FakeTransport(providesView: nil)
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { _, _, _, _ in nil })
        await settle()
        await b.sourceChanged()
        transport.setState(.reconnecting)
        transport.setState(.connected)
        await settle(250)
        let notes = client.diagnostics().filter { $0.code == "stream-undeclared" }
        XCTAssertEqual(notes.count, 1)
        XCTAssertEqual(notes.first?.level, "warn")
        XCTAssertTrue(notes.first?.message.contains("providesView: 'stream'") == true, notes.first?.message ?? "")
        XCTAssertTrue(notes.first?.message.hasPrefix("unity-screen:") == true, notes.first?.message ?? "")
        await b.stop()

        // 신고한 어댑터에는 남기지 않는다.
        let declared = FakeTransport()
        let declaredClient = try await openClient(declared)
        let d = await startScreenBroadcast(client: declaredClient, createSession: { _, _, _, _ in nil })
        await settle()
        XCTAssertTrue(declaredClient.diagnostics().filter { $0.code == "stream-undeclared" }.isEmpty)
        await d.stop()
    }

    func testAnnouncesOnEveryConnect() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { _, _, _, _ in nil })
        await settle()
        XCTAssertTrue(transport.kinds.contains("ready"), "최초 접속에서 알려야 한다")
        let before = transport.kinds.filter { $0 == "ready" }.count

        // 빠른 재접속 — 상태 스냅샷을 보면 이 창에서 재알림이 사라진다.
        transport.setState(.reconnecting)
        transport.setState(.connected)
        await settle(250)
        let after = transport.kinds.filter { $0 == "ready" }.count
        XCTAssertGreaterThan(after, before, "재접속에서도 다시 알려야 한다")
        await b.stop()
    }

    /// 재접속 사이사이에 프레즌스 이벤트가 몰려도 재접속마다 ready 를 한 번씩 다시 보낸다 — 헬퍼가 잠시 바빠도(앱의
    /// `onRemoteInput` 이 붙잡았다) 마찬가지다. 헬퍼는 상태 전이를 잃지 않는 내부 리스너로 받는다. `events()` 로 받으면
    /// 그 스트림이 최근 256개만 쥐어, 소비가 밀린 동안 몰린 이벤트 사이의 전이를 버렸다(Kotlin 실측: 재접속 200번 ×
    /// 입장 300건 → 재알림 71번 유실).
    func testEveryReconnectReannouncesEvenUnderAPresenceFlood() async throws {
        let transport = FakeTransport()
        let (b, _, _, release, client) = try await blockedInput(transport)
        // ⚠️ 여기부터 release 전까지 b 를 await 하지 않는다 — 액터가 붙잡혀 있다.
        let readyBefore = transport.kinds.filter { $0 == "ready" }.count
        let transitions = CallCounter()
        let counter = await client.onStateTransition { _ in _ = transitions.next() }
        let h = transport.handlers
        let rounds = 200
        for round in 0..<rounds {
            h.onState?(.reconnecting)
            h.onState?(.connected)
            for i in 0..<300 { h.onMemberJoin?(Member(participantId: "m\(round)-\(i)", displayName: "viewer")) }
        }
        // 클라이언트가 전이를 다 처리할 때까지 헬퍼를 붙잡아 둔다 — 그 사이 events() 에는 6만 건이 몰렸다.
        let flooded = Date().addingTimeInterval(20)
        while transitions.peek < 2 * rounds, Date() < flooded { try await Task.sleep(nanoseconds: 5_000_000) }
        counter?.cancel()
        release.signal()
        let deadline = Date().addingTimeInterval(20)
        while transport.kinds.filter({ $0 == "ready" }).count < readyBefore + rounds, Date() < deadline {
            await settle(20)
        }
        await settle(200)
        let readyAfter = transport.kinds.filter { $0 == "ready" }.count
        XCTAssertEqual(readyAfter - readyBefore, rounds, "재접속 재알림을 잃었다")
        await b.stop()
        await client.close()
    }

    func testAnswersOfferAndIgnoresAnswer() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let session = FakeSession()
        let b = await startScreenBroadcast(client: client, createSession: { _, _, _, _ in session })
        await settle()

        transport.signal(Self.offer("viewer-1"))
        await settle(250)
        XCTAssertEqual(transport.answers.count, 1)
        let answer = transport.answers[0]
        XCTAssertEqual(answer["targetParticipantId"] as? String, "viewer-1")
        XCTAssertEqual((answer["description"] as? [String: Any])?["type"] as? String, "answer")
        let count = await b.viewerCount
        XCTAssertEqual(count, 1)

        // answer 를 받으면 아무 일도 하지 않는다.
        transport.signal([
            "kind": "sdp", "fromParticipantId": "viewer-1",
            "description": ["type": "answer", "sdp": "x"],
        ])
        await settle()
        XCTAssertEqual(transport.answers.count, 1)
        await b.stop()
    }

    func testNoSessionMeansNoAnswer() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { _, _, _, _ in nil })
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(250)
        XCTAssertEqual(transport.answers.count, 0, "보낼 화면이 없으면 답하지 않는다")
        let count = await b.viewerCount
        XCTAssertEqual(count, 0)
        let generations = await b.generationCount
        XCTAssertEqual(generations, 0, "세션을 만들지 않은 offer 의 세대 항목이 남았다")
        await b.stop()
    }

    func testRelaysIceBothWays() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let session = FakeSession()
        let publishBox = SendBox()
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, sendIce, _, _ in
                publishBox.set(sendIce)
                return session
            })
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(250)

        publishBox.get()?(["candidate": "cand-local"])
        await settle()
        let ice = transport.sent.filter { ($0["kind"] as? String) == "ice" }
        XCTAssertEqual(ice.count, 1)
        XCTAssertEqual(ice[0]["targetParticipantId"] as? String, "viewer-1")

        transport.signal([
            "kind": "ice", "fromParticipantId": "viewer-1",
            "candidate": ["candidate": "cand-remote"],
        ])
        await settle()
        XCTAssertEqual(session.candidates.count, 1)
        await b.stop()
    }

    func testPeerLeftClosesOnlyThatViewer() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let s1 = FakeSession()
        let s2 = FakeSession()
        let b = await startScreenBroadcast(
            client: client,
            createSession: { viewer, _, _, _ in viewer == "viewer-1" ? s1 : s2 })
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(200)
        transport.signal(Self.offer("viewer-2"))
        await settle(250)
        var count = await b.viewerCount
        XCTAssertEqual(count, 2)

        transport.signal(["kind": "peer-left", "participantId": "viewer-1"])
        await settle(200)
        count = await b.viewerCount
        XCTAssertEqual(count, 1)
        XCTAssertTrue(s1.closed)
        XCTAssertFalse(s2.closed)
        await b.stop()
    }

    func testOneViewerFailureDoesNotDropOthers() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let ok = FakeSession()
        let errors = ErrorBox()
        let b = await startScreenBroadcast(
            client: client,
            createSession: { viewer, _, _, _ in
                viewer == "bad" ? FakeSession(failAnswer: true) : ok
            },
            onError: { _, viewer in errors.add(viewer) })
        await settle()
        transport.signal(Self.offer("bad"))
        await settle(200)
        transport.signal(Self.offer("good"))
        await settle(300)

        XCTAssertEqual(errors.all, ["bad"])
        let count = await b.viewerCount
        XCTAssertEqual(count, 1, "실패한 뷰어만 빠진다")
        await b.stop()
    }

    // ── 원격 조작 ────────────────────────────────────────────────────────────

    private func wiredInput(_ transport: FakeTransport, _ got: PointerBox) async throws
        -> (ScreenBroadcast, FakeInputChannel)
    {
        let client = try await openClient(transport)
        let channel = FakeInputChannel(label: Protocol.RemoteInput.channel)
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, wireInput in
                wireInput(channel)
                return FakeSession()
            },
            onRemoteInput: { pointer, _ in got.add(pointer) })
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(250)
        return (b, channel)
    }

    /// hello 는 채널이 살아 있는 동안 주기적으로 다시 나간다 — 뷰어의 조작 가능 판정은
    /// 탭 백그라운드만으로 꺼진 채 남을 수 있고 재요청 경로가 없어, 재알림이 유일한
    /// 복구 수단이다. stop() 뒤에는 멎어야 한다.
    func testHelloIsReannouncedPeriodically() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let channel = FakeInputChannel(label: Protocol.RemoteInput.channel)
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, wireInput in
                wireInput(channel)
                return FakeSession()
            },
            onRemoteInput: { _, _ in })
        await b.setHelloReannounceMs(30) // 10초를 기다릴 수는 없다
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(120)
        // 정확한 횟수는 시간 경합이라 세지 않는다 — "보냈다"와 "계속 늘어난다"만 본다.
        let initial = channel.sent.count
        XCTAssertGreaterThanOrEqual(initial, 1, "개설 즉시 hello 가 나가야 한다")

        await settle(150)
        XCTAssertGreaterThanOrEqual(
            channel.sent.count, initial + 2,
            "재알림이 이어져야 한다 (\(initial) → \(channel.sent.count))")
        XCTAssertEqual(channel.sent.first, channel.sent.last, "같은 hello (멱등)")

        await b.stop()
        await settle(60) // 진행 중이던 재알림 한 건이 배수될 시간
        let after = channel.sent.count
        await settle(150)
        XCTAssertEqual(after, channel.sent.count, "stop 뒤에는 늘지 않는다")
    }

    /// 채널 개설·hello·게이트 폐기가 진단 링에 남는다 — 사후 3분기의 유일한 단서.
    func testRemoteInputDiagnosticsPoints() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let channel = FakeInputChannel(label: Protocol.RemoteInput.channel)
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, wireInput in
                wireInput(channel)
                return FakeSession()
            },
            onRemoteInput: { _, _ in })
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(250)

        let codes = client.diagnostics().map(\.code)
        XCTAssertTrue(codes.contains("input-channel"), "채널 개설이 링에 남아야 한다")
        XCTAssertTrue(codes.contains("hello"), "hello 첫 송신이 링에 남아야 한다")

        transport.holder("someone-else")
        await settle(80)
        channel.push(#"{"t":"down","p":0,"x":0.5,"y":0.5}"#)
        channel.push(#"{"t":"down","p":0,"x":0.5,"y":0.5}"#)
        await settle(120)
        let dropped = client.diagnostics().filter { $0.code == "input-dropped" }.count
        XCTAssertEqual(dropped, 1, "(뷰어,사유)당 1회")
        await b.stop()
    }

    func testSendsHelloAndGatesOnHolder() async throws {
        let transport = FakeTransport()
        let got = PointerBox()
        let (b, channel) = try await wiredInput(transport, got)

        XCTAssertEqual(channel.sent.count, 1, "hello 를 보내야 뷰어가 조작 UI 를 연다")
        let hello = try JSONSerialization.jsonObject(
            with: channel.sent[0].data(using: .utf8)!) as! [String: Any]
        XCTAssertEqual(hello["t"] as? String, "hello")
        XCTAssertEqual(hello["accepts"] as? Bool, true)

        // 통지를 받은 배포에서 보유자 없음(nil) — 입력은 통과하지 않는다(N4).
        transport.holder(nil)
        await settle(80)
        channel.push(#"{"t":"down","p":0,"x":0.5,"y":0.5}"#)
        await settle(80)
        XCTAssertTrue(got.all.isEmpty, "보유자 없음 통지 뒤에는 입력을 받지 않는다")

        transport.holder("viewer-1")
        await settle(80)
        channel.push(#"{"t":"down","p":0,"x":0.5,"y":0.5}"#)
        await settle(80)
        XCTAssertEqual(got.all.count, 1)
        XCTAssertEqual(got.all[0].t, "down")
        await b.stop()
    }

    /// 보유자 통지가 없는 배포(=Editor 즉시 양방향)에서는 채널을 연 뷰어를 받는다.
    ///
    /// 서버는 보유자 통지를 등록/리스 플로우에서만 보낸다. 그 통지가 오지 않는
    /// 배포에서 게이트를 fail-closed 로 두면 원격 조작이 아예 불가능하다 — 실기
    /// 증상은 "뷰어는 조작 UI 를 열고 누르는데 어댑터가 전부 조용히 버림"이었다.
    func testFlowsWhenDeploymentSendsNoHolderNotice() async throws {
        let transport = FakeTransport()
        let got = PointerBox()
        let (b, channel) = try await wiredInput(transport, got)

        channel.push(#"{"t":"down","p":0,"x":0.5,"y":0.5}"#)
        await settle(80)
        XCTAssertEqual(got.all.count, 1, "리스 없는 배포에서 포인터가 흘러야 한다")
        XCTAssertEqual(got.all[0].t, "down")
        await b.stop()
    }

    /// 통지가 한 번 오면 그 뒤로는 보유자만 받는다(하강 뒤 구멍 없음).
    /// 보유자 통지의 ttlMs 가 지나면 어댑터 스스로 보유자를 내린다(N11·N22).
    func testHolderExpiresAfterTtl() async throws {
        let transport = FakeTransport()
        let got = PointerBox()
        let (b, channel) = try await wiredInput(transport, got)

        transport.holder("viewer-1", ttlMs: 300)
        await settle(40)
        channel.push(#"{"t":"down","p":1,"x":0.5,"y":0.5}"#)
        await settle(40)
        XCTAssertEqual(got.all.count, 1)
        await settle(500)  // renew 가 통지를 갱신하지 못했다
        // 만료 후 입력은 폐기 + 열린 포인터는 cancel 합성
        channel.push(#"{"t":"move","p":1,"x":0.6,"y":0.5}"#)
        await settle(80)
        XCTAssertEqual(got.all.count, 2)
        XCTAssertEqual(got.all.last?.t, "cancel")
        await b.stop()
    }

    /// ttlMs 가 없는 통지는 만료 타이머를 걸지 않는다 — 부재는 "모름"이지 "이미 끝났다"가 아니다.
    func testHolderWithoutTtlIsNotExpired() async throws {
        let transport = FakeTransport()
        let got = PointerBox()
        let (b, channel) = try await wiredInput(transport, got)

        transport.holder("viewer-1", ttlMs: nil) // ttlMs 필드 없음
        await settle(40)
        channel.push(#"{"t":"down","p":1,"x":0.5,"y":0.5}"#)
        await settle(200) // 종전에는 0ms 타이머가 여기서 보유자를 내렸다
        channel.push(#"{"t":"move","p":1,"x":0.6,"y":0.5}"#)
        await settle(80)
        XCTAssertEqual(got.all.map(\.t), ["down", "move"])
        await b.stop()
    }

    func testHolderNoticePromotesGateToStrict() async throws {
        let transport = FakeTransport()
        let got = PointerBox()
        let (b, channel) = try await wiredInput(transport, got)

        transport.holder("viewer-1")
        await settle(80)
        // up 으로 보낸다 — down 을 남기면 하강 통지가 cancel 을 합성해(N7) 건수가
        // 하나 늘고, 그게 "게이트가 열렸다"로 오독된다.
        channel.push(#"{"t":"up","p":0,"x":0.5,"y":0.5}"#)
        await settle(80)
        XCTAssertEqual(got.all.count, 1)

        transport.holder(nil)
        await settle(80)
        channel.push(#"{"t":"down","p":0,"x":0.5,"y":0.5}"#)
        await settle(80)
        XCTAssertEqual(got.all.count, 1, "하강 통지 뒤에는 다시 전부 폐기한다")
        await b.stop()
    }

    func testRejectsNonHolderAndBadFrames() async throws {
        let transport = FakeTransport()
        let got = PointerBox()
        let (b, channel) = try await wiredInput(transport, got)
        transport.holder("someone-else")
        await settle(80)
        channel.push(#"{"t":"down","p":0,"x":0.5,"y":0.5}"#)
        await settle(80)
        XCTAssertTrue(got.all.isEmpty, "보유자가 아닌 채널의 입력은 버린다")

        transport.holder("viewer-1")
        await settle(80)
        channel.push("not-json")
        channel.push(#"{"t":"keydown","p":0,"x":0.1,"y":0.1}"#)   // N2 밖
        channel.push(#"{"t":"down","p":0,"y":0.1}"#)              // x 누락
        channel.push(#"{"t":"down","p":-1,"x":0.1,"y":0.1}"#)     // N7 음수
        channel.push(#"{"t":"down","p":70000,"x":0.1,"y":0.1}"#)  // N7 상한
        channel.push(#"{"t":"down","p":1.5,"x":0.1,"y":0.1}"#)    // N7 비정수
        await settle(120)
        XCTAssertTrue(got.all.isEmpty, "잘못된 프레임은 전부 버린다")
        await b.stop()
    }

    func testClampsCoordinates() async throws {
        let transport = FakeTransport()
        let got = PointerBox()
        let (b, channel) = try await wiredInput(transport, got)
        transport.holder("viewer-1")
        await settle(80)
        channel.push(#"{"t":"move","p":0,"x":-3.0,"y":9.0}"#)
        await settle(120)
        XCTAssertEqual(got.all.count, 1)
        XCTAssertEqual(got.all[0].x, 0)
        XCTAssertEqual(got.all[0].y, 1)
        await b.stop()
    }

    func testHolderChangeSynthesizesCancel() async throws {
        let transport = FakeTransport()
        let got = PointerBox()
        let (b, channel) = try await wiredInput(transport, got)
        transport.holder("viewer-1")
        await settle(80)
        channel.push(#"{"t":"down","p":7,"x":0.25,"y":0.75}"#)
        await settle(120)
        XCTAssertEqual(got.all.count, 1)

        // 조작권 회수 — 누른 채 잃으면 앱이 영영 눌린 상태로 남는다(N7).
        transport.holder(nil)
        await settle(150)
        XCTAssertEqual(got.all.count, 2, "cancel 이 합성되어야 한다")
        XCTAssertEqual(got.all[1].t, "cancel")
        XCTAssertEqual(got.all[1].p, 7)
        await b.stop()
    }

    // ── 보유자 통지의 적용 순서 ─────────────────────────────────────────────────────────

    /// 적용한 보유자 통지가 `applied` 건이 될 때까지(배달 방식과 무관한 신호).
    private func waitForHolderNotices(_ b: ScreenBroadcast, applied: Int, timeout: TimeInterval = 5) async {
        let deadline = Date().addingTimeInterval(timeout)
        while await b.appliedHolderNoticeCount < applied, Date() < deadline {
            try? await Task.sleep(nanoseconds: 1_000_000)
        }
    }

    /// 부여 뒤 곧바로 회수 — 보유자가 없어야 한다(TS 미러).
    func testGrantThenImmediateRevokeLeavesNoHolder() async throws {
        let transport = FakeTransport()
        let got = PointerBox()
        let (b, channel) = try await wiredInput(transport, got)
        transport.holder("viewer-1")
        transport.holder(nil)
        await waitForHolderNotices(b, applied: 2)
        channel.push(#"{"t":"down","p":0,"x":0.5,"y":0.5}"#)
        await settle(80)
        XCTAssertTrue(got.all.isEmpty, "회수 뒤인데 입력이 통과했다")
        let holder = await b.currentHolder
        XCTAssertNil(holder)
        await b.stop()
    }

    /// 보유자 교대 A→B 는 B 로 끝난다(TS 미러).
    func testHandoverEndsWithTheNewHolder() async throws {
        let transport = FakeTransport()
        let got = PointerBox()
        let (b, channel) = try await wiredInput(transport, got)
        transport.holder("someone-else")
        transport.holder("viewer-1")
        await waitForHolderNotices(b, applied: 2)
        channel.push(#"{"t":"down","p":0,"x":0.5,"y":0.5}"#)
        await settle(80)
        XCTAssertEqual(got.all.count, 1, "교대 뒤의 보유자 입력이 막혔다")
        let holder = await b.currentHolder
        XCTAssertEqual(holder, "viewer-1")
        await b.stop()
    }

    /// 통지는 소켓 스레드에서 차례로 온다 — 액터에도 그 순서로 적용돼야 한다. 종전에는 통지마다 Task 라 두 통지가
    /// 뒤집혀 적용될 수 있었다(회수가 먼저 적용되면 옛 보유자가 ttlMs 동안 남는다). 한 번으로는 드물어 여러 번 잰다.
    func testHolderNoticesApplyInArrivalOrderEveryTime() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { _, _, _, _ in nil })
        let socketQueue = DispatchQueue(label: "fake-socket-queue")
        var applied = 0
        var holderLeft = 0
        for _ in 0..<500 {
            socketQueue.async {
                transport.holder("viewer-1")
                transport.holder(nil)
            }
            applied += 2
            await waitForHolderNotices(b, applied: applied)
            if await b.currentHolder != nil { holderLeft += 1 }
        }
        var wrongHolder = 0
        for _ in 0..<500 {
            socketQueue.async {
                transport.holder("viewer-a")
                transport.holder("viewer-b")
            }
            applied += 2
            await waitForHolderNotices(b, applied: applied)
            if await b.currentHolder != "viewer-b" { wrongHolder += 1 }
        }
        XCTAssertEqual(holderLeft, 0, "부여 → 회수 뒤에 보유자가 남았다")
        XCTAssertEqual(wrongHolder, 0, "교대 A→B 가 A 로 끝났다")
        await b.stop()
    }

    /// 적용이 멎은 동안(헬퍼가 앱 콜백에 붙잡혔다) 쌓이는 통지는 256건이다 — 넘치면 **가장 오래된** 것을 버리고, 보유자는
    /// 마지막 통지가 정한다.
    func testPiledHolderNoticesAreBoundedAndTheNewestDecides() async throws {
        let transport = FakeTransport()
        let (b, _, _, release, client) = try await blockedInput(transport)
        // ⚠️ 여기부터 release 전까지 b 를 await 하지 않는다 — 액터가 붙잡혀 있다.
        for i in 0..<300 { transport.holder("a\(i)") }
        transport.holder("final")
        XCTAssertEqual(b.pendingHolderNoticeCount, 256, "밀린 보유자 통지가 상한을 넘었다")
        let notes = client.diagnostics().filter { $0.message.hasPrefix("holder notices:") }
        XCTAssertEqual(notes.map(\.code), ["queue-overflow"], "밀림 한 번에 한 줄")
        release.signal()
        let deadline = Date().addingTimeInterval(5)
        while await !b.holderNoticesSettled, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        let holder = await b.currentHolder
        XCTAssertEqual(holder, "final", "넘친 뒤 옛 통지가 보유자를 정했다")
        await b.stop()
    }

    /// 다른 등록의 통지는 줄에 서지 않는다 — 적용할 때 어차피 걸러지는데, 세워 두면 넘칠 때 내 등록의 통지(회수)를
    /// 밀어낸다.
    func testNoticesForAnotherRegistrationNeverQueue() async throws {
        let transport = FakeTransport()
        let (b, _, _, release, _) = try await blockedInput(transport) { b, _, _, client in
            _ = try await client.submitRegistrationCode("CODE") // 내 등록 = r1
            transport.holder("viewer-1")
            let deadline = Date().addingTimeInterval(3)
            while await b.currentHolder != "viewer-1", Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        }
        for i in 0..<300 { transport.holder("x\(i)", registrationId: "r-other") }
        XCTAssertEqual(b.pendingHolderNoticeCount, 0, "다른 등록의 통지가 줄에 섰다")
        transport.holder(nil) // 내 등록의 회수
        XCTAssertEqual(b.pendingHolderNoticeCount, 1)
        release.signal()
        let deadline = Date().addingTimeInterval(5)
        while await !b.holderNoticesSettled, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        let holder = await b.currentHolder
        XCTAssertNil(holder)
        await b.stop()
    }

    /// 같은 보유자의 통지가 밀려 넘쳐도 보유자가 흔들리지 않는다 — 넘칠 때 "보유자 없음" 을 끼워 넣으면 열린 포인터에
    /// cancel 이 합성됐다(눌린 손가락이 떨어진다).
    func testAPileOfRenewalsDoesNotBounceTheHolder() async throws {
        let transport = FakeTransport()
        let (b, _, got, release, client) = try await blockedInput(transport) { b, channel, got, _ in
            transport.holder("viewer-1")
            let deadline = Date().addingTimeInterval(3)
            while await b.currentHolder != "viewer-1", Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
            channel.push(Self.pointerFrame("down", 7)) // 누른 채
            while got.all.count < 1, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        }
        for _ in 0..<600 { transport.holder("viewer-1") } // renew 가 밀렸다
        release.signal()
        let deadline = Date().addingTimeInterval(5)
        while await !b.holderNoticesSettled, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        await settle(50)
        XCTAssertFalse(got.all.contains { $0.t == "cancel" }, "밀린 renew 가 보유자를 흔들어 누름을 닫았다")
        let holder = await b.currentHolder
        XCTAssertEqual(holder, "viewer-1")
        let notes = client.diagnostics().filter { $0.message.hasPrefix("holder notices:") }
        XCTAssertEqual(notes.count, 1)
        await b.stop()
    }

    /// 회수는 포인터 드레인 한가운데서도 다음 프레임 전에 적용된다 — k 번째 프레임의 콜백 안에서(헬퍼가 드레인 중일 때)
    /// 회수가 소켓에 닿고 그 뒤 옛 보유자의 프레임 30장이 데이터 채널에 닿는다. 그 30장은 하나도 앱에 닿으면 안 된다
    /// (종전에는 드레인 묶음(64)이 끝날 때까지 회수가 기다려 30장 모두 닿았다 — 2차 검수).
    func testARevokeArrivingMidDrainStopsTheNextFrames() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let channel = FakeInputChannel(label: Protocol.RemoteInput.channel)
        let got = PointerBox()
        let revoked = Box(false)
        let socketThread = DispatchQueue(label: "fake-socket-queue")
        let dataThread = DispatchQueue(label: "fake-data-channel")
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, wireInput in
                wireInput(channel)
                return FakeSession()
            },
            onRemoteInput: { pointer, _ in
                got.add(pointer)
                if Int((pointer.x * 1000).rounded()) == 10, !revoked.value {
                    revoked.value = true
                    socketThread.sync { transport.holder(nil) } // 회수가 도착했다
                    dataThread.sync {
                        for i in 100..<130 { channel.push(Self.pointerFrame("move", 0, Double(i) / 1000)) }
                    }
                }
            })
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(250)
        transport.holder("viewer-1")
        let deadline = Date().addingTimeInterval(3)
        while await b.currentHolder != "viewer-1", Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        dataThread.sync { for i in 0..<20 { channel.push(Self.pointerFrame("move", 0, Double(i) / 1000)) } }
        await settle(400)
        let afterRevoke = got.all.filter { Int(($0.x * 1000).rounded()) >= 100 }.count
        XCTAssertEqual(afterRevoke, 0, "회수 뒤에 도착한 옛 보유자의 프레임이 앱에 닿았다")
        await b.stop()
    }

    /// 꾸준한 부하(콜백 1ms, 0.9ms 마다 한 장)에서 100장째 앞에 회수 — 회수 뒤 도착한 50장은 하나도 닿지 않는다.
    func testARevokeUnderSteadyLoadStopsEveryLaterFrame() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let channel = FakeInputChannel(label: Protocol.RemoteInput.channel)
        let got = PointerBox()
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, wireInput in
                wireInput(channel)
                return FakeSession()
            },
            onRemoteInput: { pointer, _ in
                got.add(pointer)
                usleep(1000)
            })
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(250)
        var leaked: [Int] = []
        for _ in 0..<5 {
            transport.holder("viewer-1")
            let deadline = Date().addingTimeInterval(3)
            while await b.currentHolder != "viewer-1", Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
            let offset = got.all.count
            await withCheckedContinuation { (done: CheckedContinuation<Void, Never>) in
                DispatchQueue.global().async {
                    for i in 0..<150 {
                        if i == 100 { transport.holder(nil) }
                        channel.push(Self.pointerFrame("move", 0, Double(i) / 1000))
                        usleep(900)
                    }
                    done.resume()
                }
            }
            await settle(300)
            leaked.append(got.all.dropFirst(offset).filter { Int(($0.x * 1000).rounded()) >= 100 }.count)
        }
        XCTAssertEqual(leaked, [0, 0, 0, 0, 0], "회수 뒤 도착한 프레임이 앱에 닿았다")
        await b.stop()
    }

    /// 재협상으로 낡은 채널이 넘쳐도 현재 채널의 누름이 눌린 채 남지 않는다 — 뷰어의 밀린 프레임을 통째로 버리면 현재
    /// 채널의 up 까지 버리므로, 닫는 표시는 세대와 상관없이 그 뷰어의 열린 포인터를 닫아야 한다.
    func testAStaleChannelFloodDoesNotLeaveTheCurrentPointerPressed() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let stale = FakeInputChannel(label: Protocol.RemoteInput.channel)
        let current = FakeInputChannel(label: Protocol.RemoteInput.channel)
        let made = CallCounter()
        let got = PointerBox()
        let entered = Box(false)
        let release = DispatchSemaphore(value: 0)
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, wireInput in
                wireInput(made.next() == 0 ? stale : current)
                return FakeSession()
            },
            onRemoteInput: { pointer, _ in
                if pointer.p == 999 {
                    entered.value = true
                    _ = release.wait(timeout: .now() + 20)
                    return
                }
                got.add(pointer)
            })
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(250)
        transport.signal(Self.offer("viewer-1")) // 재협상 — 앞 채널은 낡은 세대다(닫히지 않았다)
        await settle(250)
        current.push(Self.pointerFrame("down", 5))
        await settle(50)
        current.push(Self.pointerFrame("up", 999))
        let deadline = Date().addingTimeInterval(3)
        while !entered.value, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        current.push(Self.pointerFrame("up", 5))                       // 현재 세대의 놓기(대기)
        for _ in 0..<300 { stale.push(Self.pointerFrame("down", 1)) } // 낡은 채널의 홍수
        release.signal()
        await settle(300)
        let seen = got.all.map { "\($0.t)\($0.p)" }
        XCTAssertEqual(seen.first, "down5")
        XCTAssertTrue(seen.contains("up5") || seen.contains("cancel5"), "포인터 5 가 눌린 채 남았다: \(seen)")
        await b.stop()
    }

    /// 재협상 뒤 두 채널을 가진 뷰어 — 첫 세션의 채널이 낡은 세대다. `before` 는 헬퍼를 붙잡기 전에 할 일.
    private func twoChannelViewer(
        recordInto got: PointerBox,
        before: (FakeInputChannel, FakeInputChannel) async throws -> Void = { _, _ in }
    ) async throws -> (ScreenBroadcast, stale: FakeInputChannel, current: FakeInputChannel, DispatchSemaphore) {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let stale = FakeInputChannel(label: Protocol.RemoteInput.channel)
        let current = FakeInputChannel(label: Protocol.RemoteInput.channel)
        let made = CallCounter()
        let entered = Box(false)
        let release = DispatchSemaphore(value: 0)
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, wireInput in
                wireInput(made.next() == 0 ? stale : current)
                return FakeSession()
            },
            onRemoteInput: { pointer, _ in
                if pointer.p == 999 {
                    entered.value = true
                    _ = release.wait(timeout: .now() + 20)
                    return
                }
                got.add(pointer)
            })
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(250)
        transport.signal(Self.offer("viewer-1")) // 재협상 — 앞 채널은 낡은 세대다(닫히지 않았다)
        await settle(250)
        try await before(stale, current)
        current.push(Self.pointerFrame("up", 999))
        let deadline = Date().addingTimeInterval(3)
        while !entered.value, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        XCTAssertTrue(entered.value, "헬퍼를 붙잡지 못했다")
        return (b, stale, current, release)
    }

    /// 홍수 리셋이 합성한 cancel 뒤에는 그 포인터의 move · up 이 다음 down 전까지 앱에 닿지 않는다 — 현재 채널에서 버려진
    /// 것이 없어도 리셋은 드래그를 끊으므로, 뒤이은 move · up 이 down 없이 닿으면 앱의 포인터 상태가 어긋났다(3차 검수).
    func testAFloodCancelledDragStaysEndedUntilTheNextDown() async throws {
        let got = PointerBox()
        let (b, stale, current, release) = try await twoChannelViewer(recordInto: got) { _, current in
            current.push(Self.pointerFrame("down", 5))
            let deadline = Date().addingTimeInterval(3)
            while got.all.count < 1, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        }
        for _ in 0..<300 { stale.push(Self.pointerFrame("down", 1)) } // 낡은 채널의 홍수(현재 채널의 밀린 것은 없다)
        current.push(Self.pointerFrame("move", 5, 0.6))                // 드래그는 계속된다
        current.push(Self.pointerFrame("move", 5, 0.7))
        current.push(Self.pointerFrame("up", 5, 0.7))
        release.signal()
        await settle(300)
        XCTAssertEqual(got.all.map { "\($0.t)\($0.p)" }, ["down5", "cancel5"], "cancel 뒤의 move · up 이 down 없이 닿았다")
        current.push(Self.pointerFrame("down", 5, 0.2)) // 새 제스처는 다시 받는다
        current.push(Self.pointerFrame("move", 5, 0.3))
        await settle(100)
        XCTAssertEqual(got.all.map { "\($0.t)\($0.p)" }, ["down5", "cancel5", "down5", "move5"])
        await b.stop()
    }

    /// 상한은 뷰어의 모든 채널을 합쳐 센다 — 낡은 채널 200 + 현재 채널 100 이면 257번째에서 통째로 버리고 그 뒤 43건이 남는다.
    func testThePerViewerCapCountsAcrossChannels() async throws {
        let got = PointerBox()
        let (b, stale, current, release) = try await twoChannelViewer(recordInto: got)
        for _ in 0..<200 { stale.push(Self.pointerFrame("down", 1)) }
        for _ in 0..<100 { current.push(Self.pointerFrame("down", 2)) }
        XCTAssertEqual(b.pendingPointerFrameCount, 1 + 43)
        release.signal()
        await b.stop()
    }

    /// 낡은 채널의 홍수는 그 뷰어의 밀린 프레임을 모든 채널에 걸쳐 버린다 — 현재 채널의 down6 도 함께 버려진다.
    func testAFloodDropsTheViewersBacklogOnEveryChannel() async throws {
        let got = PointerBox()
        let (b, stale, current, release) = try await twoChannelViewer(recordInto: got)
        current.push(Self.pointerFrame("down", 6))
        for _ in 0..<300 { stale.push(Self.pointerFrame("down", 1)) }
        release.signal()
        await settle(300)
        XCTAssertFalse(got.all.contains { $0.t == "down" && $0.p == 6 }, "현재 채널의 밀린 프레임이 남았다")
        await b.stop()
    }

    /// 콜백이 붙잡힌 동안 통지 둘(옛 보유자의 renew → 회수)이 밀린 뒤 옛 보유자의 프레임 — 다음 프레임 전에 밀린 통지를
    /// **전부** 적용하므로 하나도 닿지 않는다.
    func testEveryPendingHolderNoticeAppliesBeforeTheNextFrame() async throws {
        let transport = FakeTransport()
        let (b, channel, got, release, _) = try await blockedInput(transport) { b, _, _, _ in
            transport.holder("viewer-1")
            let deadline = Date().addingTimeInterval(3)
            while await b.currentHolder != "viewer-1", Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        }
        transport.holder("viewer-1") // renew
        transport.holder(nil)        // 회수
        for i in 0..<10 { channel.push(Self.pointerFrame("move", 3, Double(i) / 10)) }
        release.signal()
        await settle(300)
        XCTAssertTrue(got.all.isEmpty, "회수 뒤 옛 보유자의 프레임이 닿았다: \(got.all.count)")
        await b.stop()
    }

    /// 한 뷰어의 홍수가 다른 뷰어의 밀린 프레임을 건드리지도, 순서를 바꾸지도 않는다.
    func testAFloodResetTouchesOnlyTheFloodingViewer() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let channels = [
            "v1": FakeInputChannel(label: Protocol.RemoteInput.channel),
            "v2": FakeInputChannel(label: Protocol.RemoteInput.channel),
        ]
        let log = ViewerPointerLog()
        let entered = Box(false)
        let release = DispatchSemaphore(value: 0)
        let b = await startScreenBroadcast(
            client: client,
            createSession: { viewer, _, _, wireInput in
                if let channel = channels[viewer] { wireInput(channel) }
                return FakeSession()
            },
            onRemoteInput: { pointer, viewer in
                if pointer.p == 999 {
                    entered.value = true
                    _ = release.wait(timeout: .now() + 20)
                    return
                }
                log.add(pointer, viewer)
            })
        await settle()
        transport.signal(Self.offer("v1"))
        transport.signal(Self.offer("v2"))
        await settle(300)
        channels["v1"]?.push(Self.pointerFrame("down", 3)) // v1 의 열린 포인터
        await settle(50)
        channels["v2"]?.push(Self.pointerFrame("up", 999))
        let deadline = Date().addingTimeInterval(3)
        while !entered.value, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        var expectedV2: [String] = []
        for i in 0..<300 {
            channels["v1"]?.push(Self.pointerFrame("down", 1))
            if i % 50 == 0 {
                for t in ["down", "move", "up"] {
                    channels["v2"]?.push(Self.pointerFrame(t, 2, Double(i) / 1000))
                    expectedV2.append("\(t)\(i)")
                }
            }
        }
        release.signal()
        await settle(300)
        let v2 = log.all.filter { $0.viewer == "v2" }.map { "\($0.pointer.t)\(Int(($0.pointer.x * 1000).rounded()))" }
        let v1 = log.all.filter { $0.viewer == "v1" }.map(\.pointer.t)
        XCTAssertEqual(v2, expectedV2, "다른 뷰어의 프레임이 버려지거나 순서가 바뀌었다")
        XCTAssertEqual(Array(v1.prefix(2)), ["down", "cancel"], "v1 의 열린 포인터를 닫지 않았다")
        await b.stop()
    }

    /// 적용할 때도 등록을 다시 본다 — 줄에 선 뒤 재등록했으면 옛 등록의 통지는 적용하지 않는다.
    func testANoticeQueuedBeforeAReRegistrationIsNotApplied() async throws {
        let transport = FakeTransport()
        let (b, _, _, release, client) = try await blockedInput(transport) { _, _, _, client in
            _ = try await client.submitRegistrationCode("A") // r1
        }
        transport.holder("viewer-x", registrationId: "r1") // 줄에 선다(지금은 내 등록)
        XCTAssertEqual(b.pendingHolderNoticeCount, 1)
        let second = try await client.submitRegistrationCode("B") // 헬퍼가 붙잡힌 사이 재등록 — r2
        XCTAssertEqual(second.registrationId, "r2")
        release.signal()
        let deadline = Date().addingTimeInterval(3)
        while await !b.holderNoticesSettled, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        let holder = await b.currentHolder
        XCTAssertNil(holder, "재등록 전 등록의 통지가 게이트를 열었다")
        await b.stop()
    }

    /// 보유자 통지 넘침의 진단은 밀림 한 번(줄이 빌 때까지)에 한 줄 — 드레인이 꺼내는 사이 다시 넘쳐도 더 남기지 않는다.
    func testAHolderOverflowIsNotedOncePerEpisodeEvenWhileItDrains() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let channel = FakeInputChannel(label: Protocol.RemoteInput.channel)
        let entered = Box(false)
        let release = DispatchSemaphore(value: 0)
        let pushedMidDrain = Box(false)
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, wireInput in
                wireInput(channel)
                return FakeSession()
            },
            onRemoteInput: { pointer, _ in
                if pointer.p == 999 {
                    entered.value = true
                    _ = release.wait(timeout: .now() + 20)
                } else if pointer.t == "cancel", pointer.p == 7, !pushedMidDrain.value {
                    // 드레인 한가운데(앞 통지를 적용하며 합성된 cancel) — 줄은 255건, 두 건 더 넣어 다시 넘치게 한다.
                    pushedMidDrain.value = true
                    transport.holder("y")
                    transport.holder("y")
                }
            })
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(250)
        transport.holder("viewer-1")
        var deadline = Date().addingTimeInterval(3)
        while await b.currentHolder != "viewer-1", Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        channel.push(Self.pointerFrame("down", 7)) // 열린 포인터
        channel.push(Self.pointerFrame("up", 999))
        deadline = Date().addingTimeInterval(3)
        while !entered.value, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        transport.holder("viewer-1") // 넘쳐 버려진다
        for _ in 0..<256 { transport.holder("y") }
        XCTAssertEqual(b.pendingHolderNoticeCount, 256)
        release.signal()
        deadline = Date().addingTimeInterval(3)
        while await !b.holderNoticesSettled || !pushedMidDrain.value, Date() < deadline {
            try await Task.sleep(nanoseconds: 1_000_000)
        }
        XCTAssertTrue(pushedMidDrain.value)
        let notes = client.diagnostics().filter { $0.message.hasPrefix("holder notices:") }
        XCTAssertEqual(notes.count, 1, "같은 밀림에서 넘침을 다시 남겼다")
        await b.stop()
    }

    /// 회수는 클라이언트 액터가 바빠도 곧바로 적용된다 — 등록 id 를 읽으러 클라이언트를 기다리면 그동안 옛 보유자의
    /// 입력이 통과했다(검수 실측: 회수 뒤 400건 중 144~168).
    func testARevokeAppliesWhileTheClientActorIsBusy() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        _ = try await client.submitRegistrationCode("CODE") // 내 등록 = r1 — 판정에 등록 id 가 쓰인다
        let channel = FakeInputChannel(label: Protocol.RemoteInput.channel)
        let got = PointerBox()
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, wireInput in
                wireInput(channel)
                return FakeSession()
            },
            onRemoteInput: { pointer, _ in got.add(pointer) })
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(250)
        transport.holder("viewer-1")
        var deadline = Date().addingTimeInterval(3)
        while await b.currentHolder != "viewer-1", Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }

        // 클라이언트 액터를 채널 콜백으로 붙잡는다.
        let entered = Box(false)
        let release = DispatchSemaphore(value: 0)
        await client.on("gate") { _ in
            entered.value = true
            _ = release.wait(timeout: .now() + 20)
        }
        transport.message("gate")
        deadline = Date().addingTimeInterval(5)
        while !entered.value, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        // ⚠️ 여기부터 release 전까지 client 를 await 하지 않는다.
        transport.holder(nil)
        deadline = Date().addingTimeInterval(2)
        while await b.currentHolder != nil, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        let holder = await b.currentHolder
        XCTAssertNil(holder, "클라이언트가 바쁜 동안 회수가 적용되지 않았다")
        channel.push(Self.pointerFrame("down", 0))
        await settle(80)
        XCTAssertTrue(got.all.isEmpty, "회수 뒤 옛 보유자의 입력이 통과했다")
        release.signal()
        await b.stop()
        await client.close()
    }

    // ── 포인터 프레임 순서 · 상한 ─────────────────────────────────────────────────────────

    private static func pointerFrame(_ t: String, _ p: Int, _ x: Double = 0.5) -> String {
        #"{"t":"\#(t)","p":\#(p),"x":\#(x),"y":0.5}"#
    }

    /// 데이터 채널 스레드에서 차례로 온 프레임은 앱에도 그 순서로 닿는다(종전: 프레임마다 Task — 2만 건에 인접 역전
    /// 1813~2945). 보내는 쪽은 상한(1000) 아래에서 밀어 넣는다 — 버림 없이 순서만 잰다.
    func testTwentyThousandPointerFramesReachTheAppInOrder() async throws {
        let transport = FakeTransport()
        let got = PointerBox()
        let (b, channel) = try await wiredInput(transport, got)
        let total = 20_000
        DispatchQueue(label: "fake-data-channel").async {
            for i in 0..<total {
                while b.pendingPointerFrameCount > 500 { usleep(50) }
                channel.push(Self.pointerFrame("move", 0, Double(i) / Double(total)))
            }
        }
        let deadline = Date().addingTimeInterval(30)
        while got.all.count < total, Date() < deadline { try await Task.sleep(nanoseconds: 10_000_000) }
        let xs = got.all.map(\.x)
        XCTAssertEqual(xs.count, total)
        XCTAssertEqual(zip(xs, xs.dropFirst()).filter { $0 > $1 }.count, 0, "포인터 프레임이 도착 순서와 다르게 닿았다")
        await b.stop()
    }

    /// 누름 바로 뒤의 놓기가 앞서 닿지 않는다 — 뒤집히면 앱은 떼지 않은 손가락을 쥔 채 남는다.
    func testADownUpPairNeverReverses() async throws {
        let transport = FakeTransport()
        let got = PointerBox()
        let (b, channel) = try await wiredInput(transport, got)
        let dataChannel = DispatchQueue(label: "fake-data-channel")
        var reversed = 0
        for round in 0..<500 {
            let p = round % 16
            dataChannel.async {
                channel.push(Self.pointerFrame("down", p))
                channel.push(Self.pointerFrame("up", p))
            }
            let deadline = Date().addingTimeInterval(3)
            while got.all.count < 2 * (round + 1), Date() < deadline { try await Task.sleep(nanoseconds: 500_000) }
            if got.all.suffix(2).map(\.t) != ["down", "up"] { reversed += 1 }
        }
        XCTAssertEqual(reversed, 0, "up 이 down 보다 먼저 닿았다")
        await b.stop()
    }

    /// 포인터 드레인은 64건마다 액터를 내준다 — 프레임이 끊이지 않는 동안에도(여기서는 콜백이 다음 프레임을 부른다)
    /// 다른 호출이 배달이 끝날 때까지 기다리지 않는다.
    func testALongPointerRunLetsOtherCallsIn() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let channel = FakeInputChannel(label: Protocol.RemoteInput.channel)
        let total = 50_000
        let delivered = CallCounter()
        let deliveredAtRead = Box<Int?>(nil)
        let broadcast = Box<ScreenBroadcast?>(nil)
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, wireInput in
                wireInput(channel)
                return FakeSession()
            },
            onRemoteInput: { _, _ in
                let n = delivered.next() + 1
                if n == 1, let b = broadcast.value {
                    Task.detached(priority: .medium) {
                        _ = await b.viewerCount
                        deliveredAtRead.value = delivered.peek
                    }
                }
                if n < total { channel.push(#"{"t":"move","p":0,"x":0.5,"y":0.5}"#) }
            })
        broadcast.value = b
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(250)
        // 드레인 태스크와 위 읽기가 같은 우선순위로 줄을 서게 한다(액터는 우선순위가 높은 일을 먼저 돌린다).
        await Task.detached(priority: .medium) { channel.push(#"{"t":"move","p":0,"x":0.5,"y":0.5}"#) }.value
        let deadline = Date().addingTimeInterval(60)
        while delivered.peek < total || deliveredAtRead.value == nil, Date() < deadline {
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        let at = deliveredAtRead.value ?? total
        XCTAssertLessThan(at, total / 2, "다른 호출이 포인터 배달이 끝날 때까지 액터를 얻지 못했다(\(at))")
        await b.stop()
    }

    /// p=999 프레임의 콜백이 `release` 를 받을 때까지 액터를 붙잡는 송출 — 그 사이 데이터 채널이 프레임을 쌓는다.
    /// 붙잡는 프레임은 up 이다 — 열린 포인터로 남지 않아, 뒤의 cancel 합성이 다시 붙잡지 않는다. `before` 는 붙잡기
    /// 전에(헬퍼가 자유로울 때) 할 일.
    private func blockedInput(
        _ transport: FakeTransport = FakeTransport(),
        before: (ScreenBroadcast, FakeInputChannel, PointerBox, ConnectClient) async throws -> Void = { _, _, _, _ in }
    ) async throws -> (ScreenBroadcast, FakeInputChannel, PointerBox, DispatchSemaphore, ConnectClient) {
        let client = try await openClient(transport)
        let channel = FakeInputChannel(label: Protocol.RemoteInput.channel)
        let got = PointerBox()
        let entered = Box(false)
        let release = DispatchSemaphore(value: 0)
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, wireInput in
                wireInput(channel)
                return FakeSession()
            },
            onRemoteInput: { pointer, _ in
                if pointer.p == 999 {
                    entered.value = true
                    _ = release.wait(timeout: .now() + 20)
                    return
                }
                got.add(pointer)
            })
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(250)
        try await before(b, channel, got, client)
        channel.push(Self.pointerFrame("up", 999))
        let deadline = Date().addingTimeInterval(5)
        while !entered.value, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        XCTAssertTrue(entered.value, "헬퍼를 붙잡지 못했다")
        return (b, channel, got, release, client)
    }

    /// 앱 콜백이 밀리면 쌓이는 프레임은 1000건 — 넘치면 가장 오래된 move 만 버리고 down · up · cancel 은 지킨다.
    func testPointerOverflowDropsOnlyTheOldestMoves() async throws {
        XCTAssertEqual(PointerFrameInbox.limit, 1000)
        let (b, channel, got, release, client) = try await blockedInput()
        // ⚠️ 여기부터 release 전까지 b 를 await 하지 않는다 — 액터가 붙잡혀 있다.
        var plan: [(t: String, p: Int, x: Double)] = []
        for i in 0..<1500 {
            if i % 10 == 0 { plan.append(("down", 2, 0.5)) }
            plan.append(("move", 1, Double(i) / 1500))
            if i % 30 == 5 { plan.append(("up", 2, 0.5)) }
            if i % 30 == 15 { plan.append(("cancel", 2, 0.5)) }
        }
        // 지키는 프레임 250 — 뷰어당 상한(256) 아래다.
        for f in plan { channel.push(Self.pointerFrame(f.t, f.p, f.x)) }
        XCTAssertEqual(b.pendingPointerFrameCount, 1000, "밀린 포인터 프레임이 상한을 넘었다")
        release.signal()

        // 지키는 프레임 250 + 남은 move 750(가장 새 것) — 받은 순서 그대로.
        let firstKeptMove = 1500 - 750
        let expected = plan.filter { $0.t != "move" || Int(($0.x * 1500).rounded()) >= firstKeptMove }
        let deadline = Date().addingTimeInterval(10)
        while got.all.count < expected.count, Date() < deadline { try await Task.sleep(nanoseconds: 2_000_000) }
        let delivered = got.all
        XCTAssertEqual(delivered.count, expected.count)
        XCTAssertEqual(delivered.filter { $0.t != "move" }.count, 250, "down · up · cancel 이 버려졌다")
        XCTAssertTrue(
            zip(delivered, expected).allSatisfy { $0.t == $1.t && $0.p == $1.p && abs($0.x - $1.x) < 1e-9 },
            "가장 오래된 move 만 빠진 채 받은 순서 그대로여야 한다")
        let notes = client.diagnostics().filter { $0.message.hasPrefix("pointer frames:") }
        XCTAssertEqual(notes.map(\.code), ["queue-overflow"], "넘침은 밀림 한 번에 한 줄")
        await b.stop()
    }

    /// 형식이 틀린 프레임(N2 · N6 · N7)은 줄에 서지 않는다 — 어차피 버려질 것이 콜백이 느린 동안 쌓이면 상한이 없었다
    /// (검수 실측: `p:-1` · `x:"nan"` 인 down 10만 건이 그대로 대기).
    func testMalformedPointerFramesNeverQueue() async throws {
        let (b, channel, got, release, _) = try await blockedInput()
        for _ in 0..<100_000 { channel.push(#"{"t":"down","p":-1,"x":"nan","y":0.5}"#) }
        for junk in [
            "not-json", #"{"t":"keydown","p":0,"x":0.1,"y":0.1}"#, #"{"t":"down","p":0,"y":0.1}"#,
            #"{"t":"up","p":70000,"x":0.1,"y":0.1}"#, #"{"t":"cancel","p":1.5,"x":0.1,"y":0.1}"#,
        ] {
            for _ in 0..<1000 { channel.push(junk) }
        }
        XCTAssertEqual(b.pendingPointerFrameCount, 0, "형식이 틀린 프레임이 줄에 섰다")
        release.signal()
        await settle(200)
        XCTAssertTrue(got.all.isEmpty)
        await b.stop()
    }

    /// 한 뷰어의 down · up · cancel 이 뷰어당 상한(256)을 넘으면 그 뷰어의 밀린 프레임을 통째로 버리고, 이미 앱에 닿은
    /// 누름은 cancel 로 닫는다(정리와 같다). 그 뒤 프레임은 다시 받는다.
    func testAViewerFloodingPressesLosesItsBacklogAndItsOpenPointersAreCancelled() async throws {
        XCTAssertEqual(PointerFrameInbox.keptLimitPerViewer, 256)
        let (b, channel, got, release, client) = try await blockedInput { _, channel, got, _ in
            channel.push(Self.pointerFrame("down", 7)) // 앱에 닿아 열린 포인터
            let deadline = Date().addingTimeInterval(3)
            while got.all.count < 1, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        }
        // ⚠️ 여기부터 release 전까지 b 를 await 하지 않는다 — 액터가 붙잡혀 있다.
        for _ in 0..<300 { channel.push(Self.pointerFrame("down", 1)) }
        // 257번째에서 앞의 256건과 그 프레임을 버리고 닫는 표시 하나 — 그 뒤 43건이 다시 쌓였다.
        XCTAssertEqual(b.pendingPointerFrameCount, 1 + 43)
        // 같은 밀림에서 한 번 더 넘친다(514번째) — 앞의 표시도 함께 버리고 새 표시 하나, 그 뒤 86건.
        for _ in 300..<600 { channel.push(Self.pointerFrame("down", 1)) }
        XCTAssertEqual(b.pendingPointerFrameCount, 1 + 86)
        // 그 뒤의 move 홍수가 전체 상한(1000)을 넘겨도 닫는 표시는 버려지지 않는다(지키는 줄에 선다).
        for i in 0..<1100 { channel.push(Self.pointerFrame("move", 2, Double(i) / 1100)) }
        XCTAssertEqual(b.pendingPointerFrameCount, PointerFrameInbox.limit)
        release.signal()
        let expected = 1 + 1 + 86 + (PointerFrameInbox.limit - 87)
        let deadline = Date().addingTimeInterval(5)
        while got.all.count < expected, Date() < deadline { try await Task.sleep(nanoseconds: 2_000_000) }
        let delivered = Array(got.all.dropFirst()) // 첫 down(p=7)
        XCTAssertEqual(delivered.first, RemoteInputPointer(t: "cancel", p: 7, x: 0.5, y: 0.5), "열린 포인터를 닫지 않았다")
        XCTAssertEqual(delivered.dropFirst().prefix(86).map(\.t), Array(repeating: "down", count: 86))
        let moves = delivered.dropFirst(87)
        XCTAssertEqual(moves.count, PointerFrameInbox.limit - 87)
        XCTAssertTrue(moves.allSatisfy { $0.t == "move" })
        XCTAssertEqual(moves.first?.x ?? -1, Double(1100 - moves.count) / 1100, accuracy: 1e-9, "가장 오래된 move 부터 버려야 한다")
        XCTAssertEqual(delivered.count, expected - 1)
        let notes = client.diagnostics().filter { $0.message.contains("down/up/cancel pending") }
        XCTAssertEqual(notes.map(\.code), ["queue-overflow"], "밀림 한 번에 한 줄")
        await b.stop()
    }

    func testIgnoresNonInputChannel() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let other = FakeInputChannel(label: "chat")
        let got = PointerBox()
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, wireInput in
                wireInput(other)
                return FakeSession()
            },
            onRemoteInput: { p, _ in got.add(p) })
        await settle()
        transport.signal(Self.offer("viewer-1"))
        await settle(250)
        XCTAssertTrue(other.sent.isEmpty, "라벨이 다르면 hello 도 보내지 않는다")
        await b.stop()
    }

    // ── 뷰어 세션 정리 (재접속 대조 · 완전한 정리 · 겹친 offer · 클라이언트 종료) ──────

    /// 뷰어마다 새 세션을 만들고 기록한다(만든 순서대로).
    private final class SessionLog: @unchecked Sendable {
        private let lock = NSLock()
        private var items: [(viewer: String, session: FakeSession)] = []
        func add(_ viewer: String, _ s: FakeSession) { lock.withLock { items.append((viewer, s)) } }
        var all: [(viewer: String, session: FakeSession)] { lock.withLock { items } }
        func sessions(of viewer: String) -> [FakeSession] { all.filter { $0.viewer == viewer }.map(\.session) }
        var open: [FakeSession] { all.map(\.session).filter { !$0.closed } }
    }

    private typealias ChannelLog = ChannelLogBox<FakeInputChannel>

    private func reconnect(_ transport: FakeTransport) {
        transport.setState(.reconnecting)
        transport.setState(.connected)
    }

    /// 서버가 ready 에 답하는 `peers` 프레임(지금 방에 있는 소켓).
    private static func peers(_ ids: [String]) -> [String: Any] {
        ["kind": "peers", "peers": ids.map { ["participantId": $0, "displayName": $0, "role": "viewer"] }]
    }

    private func loggedBroadcast(_ transport: FakeTransport, _ log: SessionLog) async throws -> ScreenBroadcast {
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { viewer, _, _, _ in
            let s = FakeSession()
            log.add(viewer, s)
            return s
        })
        await settle()
        return b
    }

    // ── 한 참여자의 여러 수신기(receiverId) — PIN 게스트의 두 탭이 서로의 화면을 끊던 결함 ─────────────

    private func offerFrom(_ viewer: String, _ receiverId: String, _ negotiationId: String = "n-1") -> [String: Any] {
        var frame = Self.offer(viewer)
        frame["receiverId"] = receiverId
        frame["negotiationId"] = negotiationId
        return frame
    }

    /// 조건이 설 때까지 기다린다(상한 3초) — 섰으면 true.
    private func until(_ condition: () async -> Bool) async -> Bool {
        let deadline = Date().addingTimeInterval(3)
        while Date() < deadline {
            if await condition() { return true }
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
        return await condition()
    }

    /// 같은 참여자의 두 탭(수신기 r1 · r2) — 세션이 따로고, 뒤의 offer 가 앞의 화면을 끊지 않는다. answer · ICE 는 두 id 를
    /// 되돌려 주고, 뷰어 ICE 는 그 수신기의 세션에만 붙는다.
    func testTwoReceiversOfOneParticipantBothStream() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let log = SessionLog()
        let b = await startScreenBroadcast(client: client, createSession: { viewer, sendIce, _, _ in
            let s = FakeSession()
            log.add(viewer, s)
            sendIce(["candidate": "local"])
            return s
        })
        await settle()
        transport.signal(offerFrom("v1", "r1", "n-a"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("r1 not answered") }
        transport.signal(offerFrom("v1", "r2", "n-b"))
        guard await until({ transport.answers.count == 2 }) else { return XCTFail("r2 not answered") }
        await settle()

        let count = await b.viewerCount
        XCTAssertEqual(count, 2)
        XCTAssertEqual(log.sessions(of: "v1").map(\.closed), [false, false], "뒤의 수신기가 앞의 화면을 끊었다")
        XCTAssertEqual(transport.answers.compactMap { $0["receiverId"] as? String }, ["r1", "r2"])
        XCTAssertEqual(transport.answers.compactMap { $0["negotiationId"] as? String }, ["n-a", "n-b"])
        let ices = transport.sent.filter { ($0["kind"] as? String) == "ice" }
        XCTAssertEqual(ices.compactMap { $0["receiverId"] as? String }, ["r1", "r2"])

        transport.signal([
            "kind": "ice", "fromParticipantId": "v1", "receiverId": "r2", "candidate": ["candidate": "remote-2"],
        ])
        guard await until({ log.sessions(of: "v1")[1].candidates.count == 1 }) else {
            return XCTFail("r2 did not get its candidate")
        }
        XCTAssertTrue(log.sessions(of: "v1")[0].candidates.isEmpty)
        await b.stop()
    }

    func testANewOfferFromTheSameReceiverReplacesOnlyThatSession() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(offerFrom("v1", "r1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("r1 not answered") }
        transport.signal(offerFrom("v1", "r2"))
        guard await until({ transport.answers.count == 2 }) else { return XCTFail("r2 not answered") }
        transport.signal(offerFrom("v1", "r1", "n-2"))
        guard await until({ transport.answers.count == 3 }) else { return XCTFail("r1 not answered again") }
        await settle()

        let count = await b.viewerCount
        XCTAssertEqual(count, 2)
        XCTAssertEqual(log.sessions(of: "v1").map(\.closed), [true, false, false]) // r1 의 앞 세션만 닫혔다
        await b.stop()
    }

    /// 보유자 게이트는 참여자 단위다 — 보유자의 어느 탭에서 온 포인터든 받고(앱 콜백의 viewer 는 participantId), 보유자가
    /// 바뀌면 그 참여자의 모든 탭의 열린 포인터에 cancel 을 합성한다.
    func testTheHolderIsJudgedPerParticipantAcrossItsReceivers() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        // 수신기마다 채널 하나 — 만든 순서(0 · 1)로 쥔다(같은 viewer id 라 viewer 로는 못 가른다).
        let channels = ChannelLog()
        let made = CallCounter()
        let got = ViewerPointerLog()
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, wireInput in
                let channel = FakeInputChannel(label: Protocol.RemoteInput.channel)
                channels.add("\(made.next())", channel)
                wireInput(channel)
                return FakeSession()
            },
            onRemoteInput: { pointer, viewer in got.add(pointer, viewer) })
        await settle()
        transport.signal(offerFrom("v1", "r1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("r1 not answered") }
        transport.signal(offerFrom("v1", "r2"))
        guard await until({ transport.answers.count == 2 && made.peek == 2 }) else { return XCTFail("r2 not up") }
        await settle()

        transport.holder("v1")
        guard await until({ await b.appliedHolderNoticeCount == 1 }) else { return XCTFail("holder not applied") }
        channels.get("0")?.push(#"{"t":"down","p":0,"x":0.1,"y":0.1}"#)
        channels.get("1")?.push(#"{"t":"down","p":1,"x":0.2,"y":0.2}"#)
        guard await until({ got.all.count == 2 }) else { return XCTFail("holder input not delivered: \(got.all)") }
        XCTAssertEqual(got.all.map { "\($0.pointer.t)\($0.pointer.p)@\($0.viewer)" }, ["down0@v1", "down1@v1"])

        transport.holder("someone-else")
        guard await until({ got.all.count == 4 }) else { return XCTFail("open pointers not cancelled: \(got.all)") }
        XCTAssertEqual(
            got.all.dropFirst(2).map { "\($0.pointer.t)\($0.pointer.p)@\($0.viewer)" }.sorted(), ["cancel0@v1", "cancel1@v1"])
        await b.stop()
    }

    // ── 2차 검수 — 내 participantId 가 바뀐 재합류 · 수신기 상한 · 세션 종료 알림 · ready 묶음 · 신고 판정 ─────────────

    /// 뷰어는 어댑터를 participantId 로 부른다 — 새 id 로 다시 붙으면 옛 세션은 모두 고아다(검수 실측: 재합류 5번 → 죽은 연결 5개).
    func testRejoiningAsANewParticipantClosesEveryOldSession() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(Self.offer("v1"))
        transport.signal(offerFrom("v2", "r1"))
        guard await until({ await b.viewerCount == 2 }) else { return XCTFail("two sessions not up") }

        // 같은 id 로 다시 붙으면(일시 끊김) 세션은 그대로다.
        transport.rejoin(Member(participantId: "me-1", displayName: "unity-screen", providesView: "stream"))
        guard await until({ transport.kinds.filter { $0 == "ready" }.count == 2 }) else { return XCTFail("no re-announce") }
        let kept = await b.viewerCount
        XCTAssertEqual(kept, 2)

        transport.rejoin(Member(participantId: "me-2", displayName: "unity-screen", providesView: "stream"))
        guard await until({ await b.viewerCount == 0 }) else { return XCTFail("old sessions not closed") }
        guard await until({ log.all.allSatisfy { $0.session.closed } }) else { return XCTFail("sessions not closed") }
        let tracked = await b.generationCount
        XCTAssertEqual(tracked, 0)
        guard await until({ transport.kinds.filter { $0 == "ready" }.count == 3 }) else { return XCTFail("no announce") }

        transport.signal(Self.offer("v1"))
        guard await until({ await b.viewerCount == 1 }) else { return XCTFail("v1 not back") }
        await b.stop()
    }

    func testAFifthReceiverEvictsTheOldestSessionOfThatParticipant() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(Self.offer("other"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("other not answered") }
        for i in 1...4 {
            transport.signal(offerFrom("v1", "r\(i)"))
            guard await until({ transport.answers.count == i + 1 }) else { return XCTFail("r\(i) not answered") }
        }
        // 이미 있는 수신기의 재협상은 밀어내지 않는다(가장 오래된 r1 이 아닌 r3 으로 본다) — r3 이 가장 최근이 된다.
        transport.signal(offerFrom("v1", "r3", "n-2"))
        guard await until({ transport.answers.count == 6 }) else { return XCTFail("r3 not answered again") }
        let five = await b.viewerCount
        XCTAssertEqual(five, 5)

        transport.signal(offerFrom("v1", "r5"))
        guard await until({ transport.answers.count == 7 }) else { return XCTFail("r5 not answered") }
        await settle()
        let still = await b.viewerCount
        XCTAssertEqual(still, 5)
        // 만든 순서: r1 · r2 · r3 · r4 · r3(재협상) · r5. 첫 r3 은 재협상이 닫았고, 밀려난 것은 가장 오래전에 offer 한 r1 이다.
        XCTAssertEqual(log.sessions(of: "v1").map(\.closed), [true, false, true, false, false, false])
        XCTAssertFalse(log.sessions(of: "other")[0].closed)
        await b.stop()
    }

    /// answer 를 끝내지 않는 세션 — 협상이 아직 answer 를 보내지 않은 수신기.
    private final class HangingSession: BroadcastSession, @unchecked Sendable {
        private let lock = NSLock()
        private var closedStorage = false
        var closed: Bool { lock.withLock { closedStorage } }
        func answer(offerSdp: String) async throws -> String {
            while !Task.isCancelled { try await Task.sleep(nanoseconds: 50_000_000) }
            throw CancellationError()
        }
        func addIceCandidate(_ candidate: [String: Any]) async throws {}
        func close() async { lock.withLock { closedStorage = true } }
    }

    /// AGENTS.md 8 — 상한의 희생자는 살아 있다고 알 수 없는 세션(answer 를 아직 보내지 않은 협상)이 먼저, 그다음 가장 작은 세대.
    /// 세대만 보던 종전에는 가장 오래된(answer 를 보낸) r1 을 밀어냈다.
    func testAFifthReceiverEvictsAnUnansweredNegotiationBeforeTheOldestLiveSession() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let hanging = HangingSession()
        let log = SessionLog()
        let made = CallCounter()
        let b = await startScreenBroadcast(client: client, createSession: { viewer, _, _, _ in
            // 네 번째 수신기(r4)만 answer 를 끝내지 않는다.
            if made.next() == 3 { return hanging }
            let s = FakeSession()
            log.add(viewer, s)
            return s
        })
        await settle()
        for i in 1...4 {
            transport.signal(offerFrom("v1", "r\(i)"))
            guard await until({ made.peek == i }) else { return XCTFail("r\(i) not made") }
        }
        guard await until({ transport.answers.count == 3 }) else { return XCTFail("three not answered") }
        guard await until({ await b.viewerCount == 4 }) else { return XCTFail("four sessions not up") }

        transport.signal(offerFrom("v1", "r5"))
        guard await until({ transport.answers.count == 4 }) else { return XCTFail("r5 not answered") }
        guard await until({ hanging.closed }) else { return XCTFail("the unanswered r4 was not evicted") }
        XCTAssertEqual(log.sessions(of: "v1").map(\.closed), [false, false, false, false]) // r1 · r2 · r3 · r5 는 산다
        let count = await b.viewerCount
        XCTAssertEqual(count, 4)
        await b.stop()
    }

    /// 올라간 세션이 아직 없는 키(첫 협상 중 — 팩토리가 아직 돌려주지 않았다)는 살아 있다고 알 수 없다 — 가장 오래된 r1 보다
    /// 먼저 밀려난다(검수 프로브 F).
    func testAFifthReceiverEvictsAFirstNegotiationStillInFlight() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let log = SessionLog()
        let made = CallCounter()
        let b = await startScreenBroadcast(client: client, createSession: { viewer, _, _, _ in
            if made.next() == 3 { // r4 는 끝나지 않는다
                while !Task.isCancelled { try await Task.sleep(nanoseconds: 50_000_000) }
                return nil
            }
            let s = FakeSession()
            log.add(viewer, s)
            return s
        })
        await settle()
        for i in 1...4 {
            transport.signal(offerFrom("v1", "r\(i)"))
            guard await until({ made.peek == i }) else { return XCTFail("r\(i) not made") }
        }
        guard await until({ transport.answers.count == 3 }) else { return XCTFail("three not answered") }

        transport.signal(offerFrom("v1", "r5"))
        guard await until({ transport.answers.count == 4 }) else { return XCTFail("r5 not answered") }
        await settle()
        XCTAssertEqual(log.sessions(of: "v1").map(\.closed), [false, false, false, false]) // r1 · r2 · r3 · r5 는 산다
        let count = await b.viewerCount
        XCTAssertEqual(count, 4)
        let tracked = await b.generationCount
        XCTAssertEqual(tracked, 4) // r4 의 세대는 지워졌다
        await b.stop()
    }

    /// 검수 실측(프로브 A): rA 가 재협상(ICE 대기)하는 동안 다섯 번째 수신기가 오면 rA 가 "아직 answer 하지 않은 협상" 으로
    /// 밀려났다. answer 를 보낸 세션이 올라가 있으면 재협상 중에도 살아 있다 — 희생자는 가장 작은 세대(rB)다.
    func testAReceiverRenegotiatingWithAnAnsweredSessionStaysLiveForTheCap() async throws {
        let transport = FakeTransport()
        transport.iceDelayMs = 1 // 캐시되지 않는 ICE — 매 offer 가 조회를 기다린다
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        for (i, r) in ["rA", "rB", "rC", "rD"].enumerated() {
            transport.signal(offerFrom("v1", r))
            guard await until({ transport.answers.count == i + 1 }) else { return XCTFail("\(r) not answered") }
        }
        transport.iceDelayMs = 300
        transport.signal(offerFrom("v1", "rA", "n-2")) // rA 재협상 — 옛 세션은 산다
        try await Task.sleep(nanoseconds: 30_000_000)
        transport.signal(offerFrom("v1", "rE")) // 다섯 번째 수신기
        guard await until({ transport.answers.count == 6 }) else { return XCTFail("six answers not sent") }
        guard await until({ log.sessions(of: "v1")[1].closed }) else { return XCTFail("rB not evicted") }
        await settle()
        let sessions = log.sessions(of: "v1")
        XCTAssertEqual(sessions.prefix(4).map(\.closed), [true, true, false, false]) // rA(재협상) · rB(희생자)
        XCTAssertEqual(sessions.count, 6)
        XCTAssertTrue(sessions.dropFirst(4).allSatisfy { !$0.closed })
        let count = await b.viewerCount
        XCTAssertEqual(count, 4)
        await b.stop()
    }

    func testSessionClosedForgetsThatSessionOnly() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(offerFrom("v1", "r1"))
        transport.signal(offerFrom("v1", "r2"))
        guard await until({ await b.viewerCount == 2 }) else { return XCTFail("two sessions not up") }

        await b.sessionClosed(log.sessions(of: "v1")[0])
        let left = await b.viewerCount
        XCTAssertEqual(left, 1)
        guard await until({ log.sessions(of: "v1")[0].closed }) else { return XCTFail("not closed") }
        XCTAssertFalse(log.sessions(of: "v1")[1].closed)
        let tracked = await b.generationCount
        XCTAssertEqual(tracked, 1)

        // 모르는 · 이미 잊은 세션은 무시한다.
        await b.sessionClosed(log.sessions(of: "v1")[0])
        await b.sessionClosed(FakeSession())
        let still = await b.viewerCount
        XCTAssertEqual(still, 1)
        await b.stop()
    }

    /// 검수 실측: 같은 수신기의 새 offer 가 ICE 를 기다리는 동안 옛 세션의 종료를 알리면 세대가 지워져 새 시도가 answer 없이
    /// 물러났다(answer 1 · viewerCount 0). 옛 세션만 잊고 새 시도는 끝까지 간다.
    func testSessionClosedOfTheOldSessionDoesNotCancelARenegotiationInFlight() async throws {
        let transport = FakeTransport()
        transport.iceDelayMs = 1 // 결과가 이미 만료라 캐시되지 않는다 — 매 offer 가 조회를 기다린다
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(offerFrom("v1", "r1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("r1 not answered") }
        let first = log.sessions(of: "v1")[0]

        transport.iceDelayMs = 300 // 다음 ICE 조회는 망 왕복이다(TURN 갱신)
        transport.signal(offerFrom("v1", "r1", "n-2"))
        try await Task.sleep(nanoseconds: 50_000_000)
        await b.sessionClosed(first) // 옛 피어 연결이 failed — 새 offer 는 ICE 를 기다리는 중
        guard await until({ first.closed }) else { return XCTFail("old session not closed") }
        guard await until({ transport.answers.count == 2 }) else { return XCTFail("the renegotiation was cancelled") }
        let count = await b.viewerCount
        XCTAssertEqual(count, 1)
        XCTAssertFalse(log.sessions(of: "v1")[1].closed)
        await b.stop()
    }

    /// 새 id 로 다시 붙은 바로 뒤(세 번째 ready 전)에 온 offer 는 새 id 로 건 협상이다 — 닫지 않는다(세대 기준을 잠근다).
    func testAnOfferRightAfterARejoinSurvives() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(Self.offer("old"))
        guard await until({ await b.viewerCount == 1 }) else { return XCTFail("old not up") }

        transport.rejoin(Member(participantId: "me-2", displayName: "unity-screen", providesView: "stream"))
        transport.signal(Self.offer("fresh")) // connected 처리 바로 뒤 — 정리가 끝나기 전
        guard await until({ log.sessions(of: "old")[0].closed }) else { return XCTFail("old not closed") }
        guard await until({ transport.answers.count == 2 }) else { return XCTFail("fresh not answered") }
        await settle()
        XCTAssertEqual(log.sessions(of: "fresh").map(\.closed), [false])
        let count = await b.viewerCount
        XCTAssertEqual(count, 1)
        await b.stop()
    }

    /// 끊긴 순간에 있던 세션만 닫는다 — 끊긴 뒤에 온 offer(서버가 소켓을 방에 넣은 뒤 stage:joined 전)는 새 id 로 건 협상이다.
    func testARejoinKeepsSessionsCreatedAfterTheDisconnect() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(Self.offer("old"))
        transport.signal(offerFrom("both", "r1"))
        guard await until({ await b.viewerCount == 2 }) else { return XCTFail("two sessions not up") }

        transport.setState(.reconnecting)
        transport.signal(Self.offer("fresh")) // 끊긴 뒤 · 합류 전에 온 offer
        transport.signal(offerFrom("both", "r1", "n-2")) // 끊긴 뒤 다시 협상한 수신기
        guard await until({ log.sessions(of: "fresh").count == 1 && log.sessions(of: "both").count == 2 }) else {
            return XCTFail("offers after the disconnect not answered")
        }
        guard await until({ await b.viewerCount == 3 }) else { return XCTFail("three sessions not up") }
        transport.joinedAndConnected(Member(participantId: "me-2", displayName: "unity-screen", providesView: "stream"))

        guard await until({ log.sessions(of: "old")[0].closed }) else { return XCTFail("old not closed") }
        await settle()
        XCTAssertFalse(log.sessions(of: "fresh")[0].closed)
        XCTAssertFalse(log.sessions(of: "both")[1].closed)
        let count = await b.viewerCount
        XCTAssertEqual(count, 2)
        await b.stop()
    }

    // ── 이른 뷰어 ICE(AGENTS.md 10) — 세션 · offer 적용 전에 온 후보를 버리지 않는다 ─────────────

    private func iceWith(_ viewer: String, _ receiverId: String, _ candidate: String, _ negotiationId: String?) -> [String: Any] {
        var frame: [String: Any] = [
            "kind": "ice", "fromParticipantId": viewer, "receiverId": receiverId, "candidate": ["candidate": candidate],
        ]
        if let negotiationId { frame["negotiationId"] = negotiationId }
        return frame
    }

    /// (a) 실브라우저 실측: ICE 서버 조회 동안 온 후보(세션이 아직 없다)를 버려 세션마다 100여 개 중 36–50개가 사라졌다.
    func testCandidatesBetweenTheOfferAndTheSessionAreAppliedAfterTheOffer() async throws {
        let transport = FakeTransport()
        transport.iceDelayMs = 200
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(offerFrom("v1", "r1", "n-1"))
        for i in 0..<5 { transport.signal(iceWith("v1", "r1", "c\(i)", "n-1")) }
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("not answered") }
        let session = log.sessions(of: "v1")[0]
        guard await until({ session.candidates.count == 5 }) else { return XCTFail("candidates: \(session.candidateNames)") }
        XCTAssertEqual(session.candidateNames, ["c0", "c1", "c2", "c3", "c4"])
        XCTAssertTrue(session.afterOffer.allSatisfy { $0 }, "a candidate was added before the offer was applied")
        // offer 를 적용한 뒤에 온 후보는 곧바로 넘긴다.
        transport.signal(iceWith("v1", "r1", "late", "n-1"))
        guard await until({ session.candidates.count == 6 }) else { return XCTFail("late candidate not applied") }
        await b.stop()
    }

    /// (b) 서버는 손님의 offer 를 DB 확인 뒤에 넘기고 ICE 는 바로 넘긴다 — 후보가 offer 보다 먼저 올 수 있다.
    func testCandidatesBeforeTheOfferAreHandedToTheMatchingOffer() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        for i in 0..<3 { transport.signal(iceWith("v1", "r1", "e\(i)", "n-1")) }
        transport.signal(offerFrom("v1", "r1", "n-1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("not answered") }
        let session = log.sessions(of: "v1")[0]
        guard await until({ session.candidates.count == 3 }) else { return XCTFail("candidates: \(session.candidateNames)") }
        XCTAssertEqual(session.candidateNames, ["e0", "e1", "e2"])
        await b.stop()
    }

    /// (c) 지금 시도보다 낡은 협상의 후보는 버린다 — offer 뒤에 와도, offer 앞에 쥐어 두었어도.
    func testAnOlderNegotiationsCandidatesAreDropped() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(iceWith("v1", "r2", "held-old", "n-1")) // r2: 낡은 협상의 후보를 먼저 쥔다
        transport.signal(offerFrom("v1", "r1", "n-2"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("r1 not answered") }
        transport.signal(offerFrom("v1", "r2", "n-2"))
        guard await until({ transport.answers.count == 2 }) else { return XCTFail("r2 not answered") }
        let r1 = log.sessions(of: "v1")[0]
        let r2 = log.sessions(of: "v1")[1]
        transport.signal(iceWith("v1", "r1", "old", "n-1"))
        transport.signal(iceWith("v1", "r1", "current", "n-2"))
        guard await until({ !r1.candidates.isEmpty }) else { return XCTFail("current not applied") }
        await settle()
        XCTAssertEqual(r1.candidateNames, ["current"])
        XCTAssertTrue(r2.candidates.isEmpty)
        await b.stop()
    }

    /// (c') 뒤따르는 offer 가 없어도 — 지금 시도보다 낡은 협상의 후보는 도착 때 버린다(쥐지도, 쓸기 타이머를 걸지도 않는다).
    func testAnOlderNegotiationsCandidateAfterTheAnswerIsDroppedOnArrival() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(offerFrom("v1", "r1", "n-2"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("not answered") }
        transport.signal(iceWith("v1", "r1", "stale", "n-1"))
        // 앞 프레임이 처리됐음을 아는 표지 — 지금 협상의 후보는 곧바로 넘어간다(프레임은 도착 순서대로 처리된다).
        transport.signal(iceWith("v1", "r1", "live", "n-2"))
        guard await until({ log.sessions(of: "v1").first?.candidates.count == 1 }) else { return XCTFail("live not applied") }
        await settle(30)
        let keys = await b.iceQueueKeyCount
        let running = await b.iceHoldTimerRunning
        XCTAssertEqual(keys, 0, "the stale candidate was held")
        XCTAssertFalse(running)
        XCTAssertEqual(log.sessions(of: "v1").first?.candidateNames, ["live"])
        await b.stop()
    }

    /// (d) 상한 — 키마다 200개, 넘치면 가장 오래된 것부터 버리고 일화마다 한 번 기록한다.
    func testEarlyCandidatesAreBoundedPerViewer() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { viewer, _, _, _ in
            let s = FakeSession()
            log.add(viewer, s)
            return s
        })
        await settle()
        for i in 0..<250 { transport.signal(iceWith("v1", "r1", "k\(i)", "n-1")) }
        transport.signal(offerFrom("v1", "r1", "n-1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("not answered") }
        let session = log.sessions(of: "v1")[0]
        guard await until({ session.candidates.count == 200 }) else { return XCTFail("count \(session.candidates.count)") }
        XCTAssertEqual(session.candidateNames.first, "k50")
        XCTAssertEqual(session.candidateNames.last, "k249")
        XCTAssertEqual(client.diagnostics().filter { $0.code == "ice-queue" }.count, 1)
        await b.stop()
    }

    /// (e) 첫 조회가 20초 멎으면 그동안 아무 뷰어도 답을 받지 못했다 — 시한을 넘기면 마지막으로 받은 목록으로 답한다.
    func testTheIceLookupTimeoutAnswersWithTheLastKnownServers() async throws {
        let transport = FakeTransport()
        transport.iceList = [IceServer(urls: ["stun:stun.example.org:3478"])]
        let client = try await openClient(transport)
        let seen = IceListLog()
        let b = await startScreenBroadcast(client: client, createSession: { _, _, iceServers, _ in
            seen.add(iceServers)
            return FakeSession()
        })
        await b.setIceLookupTimeoutMs(200)
        await settle()
        transport.signal(offerFrom("v1", "r1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("first not answered") }

        transport.iceHangs = true
        let started = Date()
        transport.signal(offerFrom("v2", "r1"))
        guard await until({ transport.answers.count == 2 }) else { return XCTFail("second not answered") }
        let elapsed = Date().timeIntervalSince(started)
        XCTAssertGreaterThanOrEqual(elapsed, 0.15)
        XCTAssertLessThan(elapsed, 3)
        XCTAssertEqual(seen.all.last?.first?.urls, ["stun:stun.example.org:3478"])
        let notes = client.diagnostics().filter { $0.code == "ice-servers" }.map(\.message)
        XCTAssertEqual(notes, ["ICE server lookup: no answer within 0.2 s — answering with the last known ICE servers (1)"])
        await b.stop()
    }

    /// (f) 쥠의 키는 뷰어가 고르는 receiverId 다 — 상한이 없으면 offer 없이 보낸 후보가 10.5초 뒤에도 키 5051개로 남았다(검수 실측 — 시한도 없었다). 참여자당
    /// 넷(수신기 상한과 같다), 다섯 번째는 가장 오래된 쥠을 밀어낸다. 기록은 일화마다 한 번.
    func testPreOfferHoldsAreCappedPerViewer() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { viewer, _, _, _ in
            let s = FakeSession()
            log.add(viewer, s)
            return s
        })
        await settle()
        for i in 0..<5000 { transport.signal(iceWith("v1", "r\(i)", "c\(i)", "n-1")) }
        guard await until({ client.diagnostics().contains { $0.code == "ice-queue" } }) else { return XCTFail("no note") }
        await settle(300)
        let keys = await b.iceQueueKeyCount
        XCTAssertEqual(keys, ScreenBroadcast.maxReceiversPerViewer)
        XCTAssertEqual(
            client.diagnostics().filter { $0.code == "ice-queue" }.map(\.message),
            ["too many receivers waiting for an offer with early ICE (4 per viewer, 64 in all) — dropped the oldest viewer=v1 receiver=r0"])
        // 남은 것은 가장 최근 넷(r4996..r4999) — 밀려난 수신기의 offer 는 후보를 받지 않는다.
        transport.signal(offerFrom("v1", "r4999"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("r4999 not answered") }
        transport.signal(offerFrom("v1", "r4996"))
        guard await until({ transport.answers.count == 2 }) else { return XCTFail("r4996 not answered") }
        transport.signal(offerFrom("v1", "r0"))
        guard await until({ transport.answers.count == 3 }) else { return XCTFail("r0 not answered") }
        let s = log.sessions(of: "v1")
        guard await until({ s[0].candidates.count == 1 && s[1].candidates.count == 1 }) else {
            return XCTFail("held: \(s.map(\.candidateNames))")
        }
        await settle(50)
        XCTAssertEqual(s[0].candidateNames, ["c4999"])
        XCTAssertEqual(s[1].candidateNames, ["c4996"])
        XCTAssertEqual(s[2].candidateNames, [])
        await b.stop()
    }

    /// (g) 참여자도 서로 다를 수 있다 — offer 없이 쥔 키는 전체 64개까지, 넘치면 가장 오래된 쥠부터 밀어낸다.
    func testPreOfferHoldKeysAreCappedInTotal() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { viewer, _, _, _ in
            let s = FakeSession()
            log.add(viewer, s)
            return s
        })
        await settle()
        for i in 0..<100 { transport.signal(iceWith("v\(i)", "r1", "c\(i)", "n-1")) }
        guard await until({ client.diagnostics().contains { $0.code == "ice-queue" } }) else { return XCTFail("no note") }
        await settle(100)
        let keys = await b.iceQueueKeyCount
        XCTAssertEqual(keys, ScreenBroadcast.earlyIceMaxHeldKeys)
        XCTAssertEqual(
            client.diagnostics().filter { $0.code == "ice-queue" }.map(\.message),
            ["too many receivers waiting for an offer with early ICE (4 per viewer, 64 in all) — dropped the oldest viewer=v0 receiver=r1"])
        // 100 - 64 = 36 — v0..v35 가 밀려났다.
        transport.signal(offerFrom("v35", "r1"))
        transport.signal(offerFrom("v36", "r1"))
        guard await until({ transport.answers.count == 2 }) else { return XCTFail("not answered") }
        guard await until({ log.sessions(of: "v36").first?.candidates.count == 1 }) else { return XCTFail("v36 not applied") }
        await settle(50)
        XCTAssertEqual(log.sessions(of: "v36").first?.candidateNames, ["c36"])
        XCTAssertEqual(log.sessions(of: "v35").first?.candidateNames, [])
        await b.stop()
    }

    /// 두 상한이 한 표시를 나눈다 — 쥠이 남아 있는 동안은 한 번, 쥠이 모두 비면(일화 끝) 다음 상한은 다시 기록한다.
    func testTheHeldKeyCapIsNotedOncePerEpisode() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { viewer, _, _, _ in
            let s = FakeSession()
            log.add(viewer, s)
            return s
        })
        await b.setEarlyIceHoldMs(300)
        await settle()
        func notes() -> Int { client.diagnostics().filter { $0.code == "ice-queue" }.count }
        for i in 0..<6 { transport.signal(iceWith("v1", "r\(i)", "a\(i)", "n-1")) }
        for i in 0..<70 { transport.signal(iceWith("w\(i)", "r1", "b\(i)", "n-1")) }
        guard await until({ await b.iceQueueKeyCount == ScreenBroadcast.earlyIceMaxHeldKeys }) else { return XCTFail("not capped") }
        XCTAssertEqual(notes(), 1)
        guard await until({ await b.iceQueueKeyCount == 0 }) else { return XCTFail("holds did not expire") }
        for i in 0..<5 { transport.signal(iceWith("v1", "s\(i)", "c\(i)", "n-1")) }
        guard await until({ notes() == 2 }) else { return XCTFail("the next episode was not noted: \(notes())") }
        await b.stop()
    }

    /// (h) 다른 도착이 없어도 쥠은 시한에 걷힌다 — 타이머는 쥠이 있는 동안만 돈다.
    func testPreOfferHoldsExpireOnATimerThatStopsWhenNoneAreLeft() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        await b.setEarlyIceHoldMs(150)
        var running = await b.iceHoldTimerRunning
        XCTAssertFalse(running)
        transport.signal(iceWith("v1", "r1", "stale", "n-1"))
        guard await until({ await b.iceQueueKeyCount == 1 }) else { return XCTFail("not held") }
        running = await b.iceHoldTimerRunning
        XCTAssertTrue(running)
        guard await until({ await b.iceQueueKeyCount == 0 }) else { return XCTFail("hold did not expire") }
        guard await until({ await !b.iceHoldTimerRunning }) else { return XCTFail("timer still running") }
        // 쥠이 없으면 타이머는 다시 걸리지 않는다 — 시한(150ms)보다 오래 지켜본다.
        for _ in 0..<20 {
            let running = await b.iceHoldTimerRunning
            XCTAssertFalse(running)
            await settle(20)
        }
        transport.signal(offerFrom("v1", "r1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("not answered") }
        await settle(50)
        XCTAssertEqual(log.sessions(of: "v1").first?.candidateNames, [])
        await b.stop()
    }

    /// (i) 시한은 offer 전 쥠에만 — 지금 시도의 큐(세션 · offer 적용을 기다린다)는 적용 · 대체 · 잊기까지 산다(상한 200).
    func testTheCurrentAttemptsQueueIsNotAged() async throws {
        let transport = FakeTransport()
        transport.iceDelayMs = 500
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        await b.setEarlyIceHoldMs(100)
        transport.signal(offerFrom("v1", "r1", "n-1"))
        transport.signal(iceWith("v1", "r1", "q0", "n-1"))
        await settle(250)
        transport.signal(iceWith("v1", "r1", "q1", "n-1")) // 이 도착의 쓸기가 q0 을 걷으면 안 된다
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("not answered") }
        guard await until({ log.sessions(of: "v1").first?.candidates.count == 2 }) else {
            return XCTFail("candidates: \(log.sessions(of: "v1").first?.candidateNames ?? [])")
        }
        XCTAssertEqual(log.sessions(of: "v1").first?.candidateNames, ["q0", "q1"])

        // 더 새 협상의 후보는 지금 시도의 것이 아니다 — 그 offer 를 기다리는 쥠이라 같은 시한이 걸린다(TS 와 같다).
        transport.signal(offerFrom("v1", "r1", "n-2"))
        transport.signal(iceWith("v1", "r1", "stale-next", "n-3"))
        await settle(250)
        transport.signal(iceWith("v1", "r1", "fresh-next", "n-3"))
        transport.signal(offerFrom("v1", "r1", "n-3"))
        guard await until({ transport.answers.count == 2 }) else { return XCTFail("n-3 not answered") }
        guard await until({ log.sessions(of: "v1").last?.candidates.count == 1 }) else { return XCTFail("fresh-next not applied") }
        await settle(50)
        XCTAssertEqual(log.sessions(of: "v1").last?.candidateNames, ["fresh-next"])
        await b.stop()
    }

    /// (j) 후보 적용 실패는 (세션, 세대)당 한 번 — 대체된 세션의 늦은 실패는 표시를 가져가지 않고, 잊힌 키를 되살리지 않는다
    /// (검수 실측: 떠난 뷰어 100명의 늦은 실패가 진단 링 100칸을 채웠다).
    func testAnIceCandidateFailureIsNotedOncePerAttemptAndNeverByAReplacedSession() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { viewer, _, _, _ in
            let s = FakeSession()
            s.iceFails = true
            log.add(viewer, s)
            return s
        })
        await settle()
        func notes() -> [String] { client.diagnostics().filter { $0.code == "ice-candidate" }.map(\.message) }

        transport.signal(offerFrom("v1", "r1", "n-1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("not answered") }
        transport.signal(iceWith("v1", "r1", "a1", "n-1"))
        transport.signal(iceWith("v1", "r1", "a2", "n-1"))
        let first = log.sessions(of: "v1")[0]
        guard await until({ first.candidates.count == 2 && notes().count == 1 }) else { return XCTFail("notes: \(notes())") }
        await settle(50)
        XCTAssertEqual(notes().count, 1)
        XCTAssertTrue(notes()[0].hasPrefix("adding a viewer ICE candidate failed viewer=v1 receiver=r1: "), notes()[0])

        // 첫 세션의 적용을 붙잡아 둔 채 재협상 — 그 실패가 늦게 와도 새 세대의 표시를 가져가지 않는다.
        let late = AsyncGate()
        first.iceHold = late
        transport.signal(iceWith("v1", "r1", "a3", "n-1"))
        guard await until({ first.candidates.count == 3 }) else { return XCTFail("a3 not tried") }
        transport.signal(offerFrom("v1", "r1", "n-2"))
        guard await until({ transport.answers.count == 2 }) else { return XCTFail("not renegotiated") }
        late.open()
        await settle(80)
        XCTAssertEqual(notes().count, 1)
        transport.signal(iceWith("v1", "r1", "b1", "n-2"))
        guard await until({ notes().count == 2 }) else { return XCTFail("new generation not noted") }

        // 잊힌 뒤의 늦은 실패는 기록하지 않고 표시도 남기지 않는다.
        let second = log.sessions(of: "v1")[1]
        let gone = AsyncGate()
        second.iceHold = gone
        transport.signal(iceWith("v1", "r1", "b2", "n-2"))
        guard await until({ second.candidates.count == 2 }) else { return XCTFail("b2 not tried") }
        transport.signal(["kind": "peer-left", "participantId": "v1"])
        guard await until({ await b.viewerCount == 0 }) else { return XCTFail("not forgotten") }
        gone.open()
        await settle(80)
        XCTAssertEqual(notes().count, 2)
        let marks = await b.iceFailureNotedCount
        XCTAssertEqual(marks, 0)
        await b.stop()
    }

    /// (k) 쥔 후보를 넘기는 도중에 온 후보가 앞지르지 않는다 — 후보마다 따로 띄운 태스크는 순서가 보장되지 않는다.
    func testCandidatesArrivingWhileQueuedOnesAreHandedOverWaitTheirTurn() async throws {
        let transport = FakeTransport()
        transport.iceDelayMs = 100
        let log = SessionLog()
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { viewer, _, _, _ in
            let s = FakeSession()
            s.onIce = { candidate in
                if candidate == "q0" {
                    transport.signal([
                        "kind": "ice", "fromParticipantId": "v1", "receiverId": "r1", "negotiationId": "n-1",
                        "candidate": ["candidate": "late"],
                    ])
                    Thread.sleep(forTimeInterval: 0.03)
                }
            }
            log.add(viewer, s)
            return s
        })
        await settle()
        transport.signal(offerFrom("v1", "r1", "n-1"))
        for name in ["q0", "q1", "q2"] { transport.signal(iceWith("v1", "r1", name, "n-1")) }
        guard await until({ log.sessions(of: "v1").first?.candidates.count == 4 }) else {
            return XCTFail("candidates: \(log.sessions(of: "v1").first?.candidateNames ?? [])")
        }
        XCTAssertEqual(log.sessions(of: "v1").first?.candidateNames, ["q0", "q1", "q2", "late"])
        // 다 넘긴 뒤에 온 후보들도 도착 순서대로 넘긴다.
        for i in 0..<20 { transport.signal(iceWith("v1", "r1", "after\(i)", "n-1")) }
        guard await until({ log.sessions(of: "v1").first?.candidates.count == 24 }) else { return XCTFail("after not applied") }
        XCTAssertEqual(Array(log.sessions(of: "v1").first!.candidateNames.dropFirst(4)), (0..<20).map { "after\($0)" })
        await b.stop()
    }

    /// (l) 세션을 맵에 올린 뒤 answer 가 끝나기 전에 온 후보는 쥔다 — offer 를 적용하기 전의 세션에 넘기면 실패한다.
    func testCandidatesDuringTheAnswerWaitForTheOfferToBeApplied() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let gate = AsyncGate()
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { viewer, _, _, _ in
            let s = FakeSession()
            s.answerGate = gate
            log.add(viewer, s)
            return s
        })
        await settle()
        transport.signal(offerFrom("v1", "r1", "n-1"))
        guard await until({ log.sessions(of: "v1").first?.answering == true }) else { return XCTFail("not answering") }
        transport.signal(iceWith("v1", "r1", "during", "n-1"))
        await settle(80)
        XCTAssertEqual(log.sessions(of: "v1").first?.candidates.count, 0, "a candidate reached the session before the offer was applied")
        gate.open()
        guard await until({ log.sessions(of: "v1").first?.candidates.count == 1 }) else { return XCTFail("not applied") }
        XCTAssertEqual(log.sessions(of: "v1").first?.afterOffer, [true])
        await b.stop()
    }

    /// (m) 지금 시도의 큐에 선 더 새 협상의 후보는 이 시도의 세션에 넘기지 않는다 — 그 offer 가 받는다.
    func testTheFlushLeavesTheNextNegotiationsCandidatesForItsOffer() async throws {
        let transport = FakeTransport()
        transport.iceDelayMs = 150
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(offerFrom("v1", "r1", "n-1"))
        transport.signal(iceWith("v1", "r1", "mine", "n-1"))
        transport.signal(iceWith("v1", "r1", "next", "n-2"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("first not answered") }
        guard await until({ log.sessions(of: "v1").first?.candidates.count == 1 }) else { return XCTFail("mine not applied") }
        transport.signal(offerFrom("v1", "r1", "n-2"))
        guard await until({ transport.answers.count == 2 }) else { return XCTFail("second not answered") }
        guard await until({ log.sessions(of: "v1").count == 2 && log.sessions(of: "v1")[1].candidates.count == 1 }) else {
            return XCTFail("next not applied")
        }
        await settle(50)
        XCTAssertEqual(log.sessions(of: "v1")[0].candidateNames, ["mine"])
        XCTAssertEqual(log.sessions(of: "v1")[1].candidateNames, ["next"])
        await b.stop()
    }

    /// (n) offer 전에 쥔 후보도 그 참여자의 몫이다 — 떠나면 함께 버린다.
    func testPeerLeftDropsTheViewersHeldCandidates() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(iceWith("v1", "r1", "held", "n-1"))
        guard await until({ await b.iceQueueKeyCount == 1 }) else { return XCTFail("not held") }
        transport.signal(["kind": "peer-left", "participantId": "v1"])
        guard await until({ await b.iceQueueKeyCount == 0 }) else { return XCTFail("held candidates kept") }
        transport.signal(offerFrom("v1", "r1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("not answered") }
        await settle(50)
        XCTAssertEqual(log.sessions(of: "v1").first?.candidateNames, [])
        await b.stop()
    }

    /// (o) 협상 id 없는 후보는 쥘 때의 시도 몫이다 — 그 시도를 대체하는 offer 에 넘기지 않는다.
    func testASupersedingOfferDropsTheEarlierAttemptsUntaggedCandidates() async throws {
        let transport = FakeTransport()
        transport.iceDelayMs = 150
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(offerFrom("v1", "r1", "n-1"))
        transport.signal(iceWith("v1", "r1", "old-untagged", nil))
        transport.signal(offerFrom("v1", "r1", "n-2"))
        transport.signal(iceWith("v1", "r1", "new", "n-2"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("not answered") }
        guard await until({ (log.sessions(of: "v1").last?.candidates.count ?? 0) >= 1 }) else { return XCTFail("not applied") }
        await settle(50)
        XCTAssertEqual(log.sessions(of: "v1").last?.candidateNames, ["new"])
        await b.stop()
    }

    /// (p) 새 participantId 로 다시 붙으면 끊기기 전에 쥔 offer 전 후보는 옛 나에게 온 것이다 — 버린다(끊긴 뒤 온 것은 남긴다).
    func testANewParticipantIdDropsCandidatesHeldBeforeTheDisconnect() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(iceWith("v1", "r1", "before", "n-1"))
        guard await until({ await b.iceQueueKeyCount == 1 }) else { return XCTFail("not held") }
        await settle(20)
        transport.setState(.reconnecting)
        await settle(20)
        transport.signal(iceWith("v1", "r1", "while-away", "n-1"))
        transport.joinedAndConnected(Member(participantId: "me-2", displayName: "unity-screen", providesView: "stream"))
        await settle(80)
        transport.signal(offerFrom("v1", "r1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("not answered") }
        guard await until({ (log.sessions(of: "v1").first?.candidates.count ?? 0) >= 1 }) else { return XCTFail("not applied") }
        await settle(50)
        XCTAssertEqual(log.sessions(of: "v1").first?.candidateNames, ["while-away"])
        await b.stop()
    }

    /// 규칙 10 — 끊김 스냅샷이 없어도(세션은 대조하지 않는다) 떠난 참여자의 offer 전 쥠은 peers 답에서 버린다.
    func testAPeersReplyDropsDepartedViewersHeldCandidatesEvenWithoutADisconnect() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(Self.offer("s1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("s1 not answered") }
        transport.signal(iceWith("gone", "r1", "g", "n-1"))
        transport.signal(iceWith("stay", "r1", "k", "n-1"))
        guard await until({ await b.iceQueueKeyCount == 2 }) else { return XCTFail("not held") }
        transport.signal(Self.peers(["stay"]))
        guard await until({ await b.iceQueueKeyCount == 1 }) else { return XCTFail("departed holds kept") }
        let count = await b.viewerCount
        XCTAssertEqual(count, 1)
        transport.signal(offerFrom("stay", "r1"))
        guard await until({ log.sessions(of: "stay").first?.candidates.count == 1 }) else { return XCTFail("stay not applied") }
        XCTAssertEqual(log.sessions(of: "stay").first?.candidateNames, ["k"])
        await b.stop()
    }

    func testAPeersReplyAfterADisconnectDropsDepartedViewersHeldCandidatesToo() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(Self.offer("s1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("s1 not answered") }
        transport.signal(iceWith("gone", "r1", "g", "n-1"))
        guard await until({ await b.iceQueueKeyCount == 1 }) else { return XCTFail("not held") }
        reconnect(transport)
        await settle(80)
        transport.signal(Self.peers(["s1"]))
        guard await until({ await b.iceQueueKeyCount == 0 }) else { return XCTFail("departed holds kept") }
        let count = await b.viewerCount
        XCTAssertEqual(count, 1)
        await b.stop()
    }

    /// R6g — peers 에 없는 참여자라도 세션이 있으면(끊김 스냅샷이 없어 세션은 대조하지 않는다) 그 키의 ICE 상태는 지우지 않는다.
    func testAPeersReplyKeepsTheIceStateOfAViewerWithASession() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(offerFrom("v1", "r1", "n-1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("not answered") }
        transport.signal(iceWith("v1", "r1", "next", "n-2")) // 이 키가 큐에 선다(다음 협상의 쥠)
        transport.signal(Self.peers(["other"]))
        transport.signal(iceWith("v1", "r1", "live", "n-1"))
        guard await until({ log.sessions(of: "v1").first?.candidates.count == 1 }) else { return XCTFail("live not applied") }
        XCTAssertEqual(log.sessions(of: "v1").first?.candidateNames, ["live"])
        let count = await b.viewerCount
        XCTAssertEqual(count, 1)
        await b.stop()
    }

    /// R6f — 다음 협상의 쥠이 넘치면 그 쥠의 가장 오래된 것을 버린다(지금 시도의 후보는 건드리지 않는다).
    func testAnOverflowDropsFromItsOwnQueueOnly() async throws {
        let transport = FakeTransport()
        transport.iceDelayMs = 300
        let log = SessionLog()
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { viewer, _, _, _ in
            let s = FakeSession()
            log.add(viewer, s)
            return s
        })
        await settle()
        transport.signal(offerFrom("v1", "r1", "n-1"))
        for i in 0..<5 { transport.signal(iceWith("v1", "r1", "cur\(i)", "n-1")) }
        for i in 0..<205 { transport.signal(iceWith("v1", "r1", String(format: "next%03d", i), "n-2")) }
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("first not answered") }
        guard await until({ log.sessions(of: "v1").first?.candidates.count == 5 }) else { return XCTFail("current not applied") }
        XCTAssertEqual(log.sessions(of: "v1").first?.candidateNames, (0..<5).map { "cur\($0)" })
        XCTAssertEqual(client.diagnostics().filter { $0.code == "ice-queue" }.count, 1)
        transport.signal(offerFrom("v1", "r1", "n-2"))
        guard await until({ transport.answers.count == 2 }) else { return XCTFail("second not answered") }
        guard await until({ log.sessions(of: "v1").count == 2 && log.sessions(of: "v1")[1].candidates.count == 200 }) else {
            return XCTFail("next not applied")
        }
        XCTAssertEqual(log.sessions(of: "v1")[1].candidateNames, (5..<205).map { String(format: "next%03d", $0) })
        await b.stop()
    }

    /// 한 목록 · 한 상한(200)을 나누면 다음 협상의 후보 200개가 지금 시도의 후보를 모두 밀어냈다 — 따로 센다(TS 의 두 큐).
    func testTheCurrentAttemptsCandidatesAndNextNegotiationHoldsAreCappedSeparately() async throws {
        let transport = FakeTransport()
        transport.iceDelayMs = 300
        let log = SessionLog()
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { viewer, _, _, _ in
            let s = FakeSession()
            log.add(viewer, s)
            return s
        })
        await settle()
        transport.signal(offerFrom("v1", "r1", "n-1"))
        for i in 0..<5 { transport.signal(iceWith("v1", "r1", "cur\(i)", "n-1")) }
        for i in 0..<200 { transport.signal(iceWith("v1", "r1", "next\(i)", "n-2")) }
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("first not answered") }
        guard await until({ log.sessions(of: "v1").first?.candidates.count == 5 }) else {
            return XCTFail("current: \(log.sessions(of: "v1").first?.candidateNames ?? [])")
        }
        XCTAssertEqual(log.sessions(of: "v1").first?.candidateNames, (0..<5).map { "cur\($0)" })
        transport.signal(offerFrom("v1", "r1", "n-2"))
        guard await until({ transport.answers.count == 2 }) else { return XCTFail("second not answered") }
        guard await until({ log.sessions(of: "v1").count == 2 && log.sessions(of: "v1")[1].candidates.count == 200 }) else {
            return XCTFail("next not applied")
        }
        XCTAssertEqual(log.sessions(of: "v1")[1].candidateNames.first, "next0")
        XCTAssertFalse(client.diagnostics().contains { $0.code == "ice-queue" })
        await b.stop()
    }

    /// 마지막으로 받은 목록이 없으면 클라이언트가 실패에도 돌려준 목록(만료 임박 캐시)으로 답한다 — 종전에는 빈 목록으로 답하고
    /// "no ICE servers (host candidates only)" 를 남겼다(검수 N8).
    func testTheHelperAnswersWithWhatTheClientStillReturned() async throws {
        let transport = FakeTransport()
        transport.iceList = [IceServer(urls: ["turn:turn.example.org:3478"], username: "u", credential: "c")]
        let client = try await openClient(transport)
        let first = await client.fetchIceServers() // 앱이 먼저 받은 만료 임박 목록 — 캐시에 남는다
        XCTAssertEqual(first.count, 1)
        transport.iceList = nil
        transport.iceFails = true
        let seen = IceListLog()
        let b = await startScreenBroadcast(client: client, createSession: { _, _, iceServers, _ in
            seen.add(iceServers)
            return FakeSession()
        })
        await settle()
        transport.signal(offerFrom("v1", "r1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("not answered") }
        XCTAssertEqual(seen.all.first?.first?.urls, ["turn:turn.example.org:3478"])
        XCTAssertEqual(
            client.diagnostics().filter { $0.code == "ice-servers" }.map(\.message),
            ["ICE server lookup: the lookup failed — answering with the ICE servers still available (1)"])
        await b.stop()
    }

    /// 조회가 실패해도(트랜스포트의 실패 표시) 시한을 넘긴 것과 같이 다룬다 — 마지막으로 받은 목록으로 답하고 한 번 남긴다.
    func testAFailedIceLookupAnswersWithTheLastKnownServers() async throws {
        let transport = FakeTransport()
        transport.iceList = [IceServer(urls: ["stun:stun.example.org:3478"])]
        let client = try await openClient(transport)
        let seen = IceListLog()
        let b = await startScreenBroadcast(client: client, createSession: { _, _, iceServers, _ in
            seen.add(iceServers)
            return FakeSession()
        })
        await settle()
        transport.signal(offerFrom("v1", "r1"))
        guard await until({ transport.answers.count == 1 }) else { return XCTFail("first not answered") }
        transport.iceList = nil
        transport.iceFails = true
        transport.signal(offerFrom("v2", "r1"))
        guard await until({ transport.answers.count == 2 }) else { return XCTFail("second not answered") }
        XCTAssertEqual(seen.all.last?.first?.urls, ["stun:stun.example.org:3478"])
        XCTAssertEqual(
            client.diagnostics().filter { $0.code == "ice-servers" }.map(\.message),
            ["ICE server lookup: the lookup failed — answering with the last known ICE servers (1)"])
        await b.stop()
    }

    /// 파리티 가드는 상수를 본다 — 쓰이는 값은 테스트 훅이므로, 훅의 첫 값이 상수와 어긋나면 가드가 못 본다.
    func testTestHookValuesStartAtTheirConstants() async throws {
        let client = try await openClient(FakeTransport())
        let b = await startScreenBroadcast(client: client, createSession: { _, _, _, _ in nil })
        let window = await b.sourceChangedWindowMsValue
        let lookup = await b.iceLookupTimeoutMsValue
        let hold = await b.earlyIceHoldMsValue
        let hello = await b.helloReannounceMs
        XCTAssertEqual(window, ScreenBroadcast.sourceChangedWindowMs)
        XCTAssertEqual(lookup, ScreenBroadcast.iceLookupTimeoutMs)
        XCTAssertEqual(hold, ScreenBroadcast.earlyIceHoldMs)
        XCTAssertEqual(hello, Protocol.RemoteInput.helloReannounceMs)
        await b.stop()
    }

    func testSourceChangedSendsAtMostOneReadyPerWindow() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { _, _, _, _ in nil })
        await b.setSourceChangedWindowMs(300)
        await settle()
        XCTAssertEqual(transport.kinds.filter { $0 == "ready" }.count, 1)

        for _ in 0..<20 { await b.sourceChanged() } // 프레임마다 부르는 앱
        XCTAssertEqual(transport.kinds.filter { $0 == "ready" }.count, 2) // 앞 하나는 곧바로
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertEqual(transport.kinds.filter { $0 == "ready" }.count, 2, "창 안에서 더 보냈다")
        guard await until({ transport.kinds.filter { $0 == "ready" }.count == 3 }) else { return XCTFail("no trailing ready") }
        try await Task.sleep(nanoseconds: 450_000_000)
        XCTAssertEqual(transport.kinds.filter { $0 == "ready" }.count, 3, "부르지 않았는데 보냈다")
        await b.stop()
    }

    /// 신고를 요청한 providesView 를 아는 트랜스포트 — SocketTransport · MemoryTransport 처럼.
    private final class DeclaringTransport: Transport, DeclaresProvidesView, @unchecked Sendable {
        let declaredProvidesView: String?
        let me: Member
        init(_ declared: String?, _ me: Member) {
            declaredProvidesView = declared
            self.me = me
        }
        func open(handlers: TransportHandlers) async throws -> Joined { Joined(roomId: "stage-1", me: me) }
        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult { PublishResult(ok: true) }
        func listMembers() async throws -> [Member] { [me] }
        func close() async {}
    }

    func testStreamUndeclaredIsJudgedFromTheRequestedProvidesView() async throws {
        // 요청은 stream 인데 서버가 돌려준 나에 그 칸이 없다(구 서버 · 폴백 합류) — 경고하지 않는다.
        let asked = ConnectClient(transport: DeclaringTransport("stream", Member(participantId: "me-1", displayName: "Me")))
        try await asked.open()
        let a = await startScreenBroadcast(client: asked, createSession: { _, _, _, _ in nil })
        await settle()
        XCTAssertTrue(asked.diagnostics().filter { $0.code == "stream-undeclared" }.isEmpty)
        await a.stop()

        // 요청하지 않았는데 서버가 stream 으로 돌려줘도 — 요청한 것이 기준이다.
        let notAsked = ConnectClient(
            transport: DeclaringTransport(nil, Member(participantId: "me-1", displayName: "Me", providesView: "stream")))
        try await notAsked.open()
        let n = await startScreenBroadcast(client: notAsked, createSession: { _, _, _, _ in nil })
        guard await until({ !notAsked.diagnostics().filter { $0.code == "stream-undeclared" }.isEmpty }) else {
            return XCTFail("not warned")
        }
        await n.stop()
    }

    func testPeerLeftClosesEverySessionOfThatParticipant() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(offerFrom("v1", "r1"))
        transport.signal(offerFrom("v1", "r2"))
        transport.signal(Self.offer("v2"))
        guard await until({ await b.viewerCount == 3 }) else { return XCTFail("three sessions not up") }

        transport.signal(["kind": "peer-left", "participantId": "v1"])
        guard await until({ await b.viewerCount == 1 }) else { return XCTFail("v1 sessions not closed") }
        await settle()
        XCTAssertEqual(log.sessions(of: "v1").map(\.closed), [true, true])
        XCTAssertFalse(log.sessions(of: "v2")[0].closed)
        let tracked = await b.generationCount
        XCTAssertEqual(tracked, 1) // v2 의 세대만 남는다
        await b.stop()
    }

    func testPeersReconciliationClosesEverySessionOfAParticipantThatLeft() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(offerFrom("v1", "r1"))
        transport.signal(offerFrom("v1", "r2"))
        transport.signal(offerFrom("v2", "r1"))
        guard await until({ await b.viewerCount == 3 }) else { return XCTFail("three sessions not up") }

        reconnect(transport)
        await settle(150)
        transport.signal(Self.peers(["v2"]))
        guard await until({ await b.viewerCount == 1 }) else { return XCTFail("v1 sessions not closed") }
        await settle()
        XCTAssertEqual(log.sessions(of: "v1").map(\.closed), [true, true])
        XCTAssertFalse(log.sessions(of: "v2")[0].closed)
        await b.stop()
    }

    /// (1) 어댑터가 재접속하는 동안 떠난 뷰어에게는 peer-left 가 오지 않는다 — 다시 붙은 뒤 ready 에 대한
    /// `peers` 답에 없으면 닫는다. 답에 있는 뷰어는 그대로다(어댑터 쪽 순간 끊김에도 P2P 는 살아 있다).
    func testPeersReplyAfterReconnectClosesViewersThatLeft() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        for v in ["v1", "v2", "v3"] { transport.signal(Self.offer(v)) }
        await settle(250)

        reconnect(transport)
        await settle(150)
        transport.signal(Self.peers(["host", "v2"]))
        await settle(200)
        XCTAssertTrue(log.sessions(of: "v1")[0].closed)
        XCTAssertTrue(log.sessions(of: "v3")[0].closed)
        XCTAssertFalse(log.sessions(of: "v2")[0].closed, "답에 있는 뷰어를 닫았다")

        // 스냅샷은 한 번 쓰면 버린다 — 끊김 없이 온 다음 답(빈 목록이어도)은 아무것도 닫지 않는다.
        transport.signal(Self.peers([]))
        await settle(200)
        XCTAssertFalse(log.sessions(of: "v2")[0].closed, "쓴 스냅샷으로 다시 대조했다")
        let count = await b.viewerCount
        XCTAssertEqual(count, 1)
        await b.stop()
    }

    /// (2) 빈 목록 = 모두 떠났다.
    func testEmptyPeersReplyClosesEverySnapshotViewer() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        for v in ["v1", "v2"] { transport.signal(Self.offer(v)) }
        await settle(250)

        reconnect(transport)
        await settle(150)
        transport.signal(Self.peers([]))
        await settle(200)
        XCTAssertTrue(log.all.allSatisfy { $0.session.closed })
        let count = await b.viewerCount
        XCTAssertEqual(count, 0)
        await b.stop()
    }

    /// (3) 답이 없거나(구 서버) `peers` 가 배열이 아니면 판정하지 않는다.
    func testNoReplyOrNonArrayPeersClosesNothing() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(Self.offer("v1"))
        await settle(250)

        reconnect(transport)
        await settle(250) // 답 없음
        transport.signal(["kind": "peers", "peers": "nope"])
        transport.signal(["kind": "peers"])
        await settle(200)
        XCTAssertFalse(log.sessions(of: "v1")[0].closed)
        let count = await b.viewerCount
        XCTAssertEqual(count, 1)
        await b.stop()
    }

    /// (4) 끊긴 적이 없으면(첫 접속의 답) 아무것도 닫지 않는다.
    func testPeersFrameWithoutAPriorDisconnectClosesNothing() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(Self.offer("v1"))
        await settle(250)

        transport.signal(Self.peers([]))
        await settle(200)
        XCTAssertFalse(log.sessions(of: "v1")[0].closed, "스냅샷 없이 대조했다")
        let count = await b.viewerCount
        XCTAssertEqual(count, 1)
        await b.stop()
    }

    /// (5) 재접속 뒤 답이 오기 전에 다시 협상한 뷰어(세대가 바뀌었다)와 새로 온 뷰어(스냅샷에 없다)는
    /// 빈 답에도 남는다. 두 번째 답은 아무것도 닫지 않는다(스냅샷은 한 번 쓰면 버린다).
    func testViewersThatRenegotiatedOrJoinedAfterReconnectAreKept() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(Self.offer("v1"))
        await settle(250)

        reconnect(transport)
        await settle(150)
        transport.signal(Self.offer("v1")) // 재협상
        transport.signal(Self.offer("v2")) // 새 뷰어
        await settle(250)
        transport.signal(Self.peers([]))
        await settle(200)
        XCTAssertTrue(log.sessions(of: "v1")[0].closed, "옛 v1 세션은 재협상이 닫는다")
        XCTAssertFalse(log.sessions(of: "v1")[1].closed, "스냅샷 뒤 다시 협상한 뷰어를 닫았다")
        XCTAssertFalse(log.sessions(of: "v2")[0].closed, "재접속 뒤 온 뷰어를 닫았다")

        transport.signal(Self.peers([]))
        await settle(200)
        XCTAssertEqual(log.open.count, 2, "쓴 스냅샷으로 다시 대조했다")
        let count = await b.viewerCount
        XCTAssertEqual(count, 2)
        await b.stop()
    }

    /// (6) 다음 끊김은 새 스냅샷을 찍는다.
    func testTheNextDisconnectTakesAFreshSnapshot() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(Self.offer("v1"))
        await settle(250)

        reconnect(transport)
        await settle(150)
        transport.signal(Self.peers(["v1"]))
        await settle(150)
        XCTAssertFalse(log.sessions(of: "v1")[0].closed)

        transport.signal(Self.offer("v2"))
        await settle(250)
        reconnect(transport)
        await settle(150)
        transport.signal(Self.peers(["v1"]))
        await settle(200)
        XCTAssertTrue(log.sessions(of: "v2")[0].closed, "두 번째 끊김의 스냅샷이 v2 를 담지 않았다")
        XCTAssertFalse(log.sessions(of: "v1")[0].closed)
        let count = await b.viewerCount
        XCTAssertEqual(count, 1)
        await b.stop()
    }

    /// (7) `peers` 답 하나가 사라져도(서버의 ready 처리 실패) 다음 끊김의 새 스냅샷이 그 사이 온 뷰어를 담는다.
    func testALostPeersReplyDoesNotHideViewersFromTheNextReconcile() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(Self.offer("v1"))
        await settle(250)

        reconnect(transport) // 답이 오지 않는다
        await settle(250)
        transport.signal(Self.offer("v3"))
        await settle(250)
        reconnect(transport)
        await settle(150)
        transport.signal(Self.peers(["v1"]))
        await settle(200)
        XCTAssertTrue(log.sessions(of: "v3")[0].closed, "답을 잃은 뒤 온 뷰어가 대조되지 않았다")
        XCTAssertFalse(log.sessions(of: "v1")[0].closed)
        let count = await b.viewerCount
        XCTAssertEqual(count, 1)
        await b.stop()
    }

    // ── 정리된 · 대체된 세션은 조용하다 (B5) ────────────────────────────────

    /// 앱의 채널 구현이 세션 close 에 채널을 닫지 않아도, 떠난 뷰어의 프레임은 앱에 닿지 않는다.
    func testAForgottenViewersChannelDeliversNothing() async throws {
        let transport = FakeTransport()
        let got = PointerBox()
        let (b, channel) = try await wiredInput(transport, got)
        transport.signal(["kind": "peer-left", "participantId": "viewer-1"])
        await settle(150)
        channel.push(#"{"t":"down","p":0,"x":0.5,"y":0.5}"#)
        await settle(150)
        XCTAssertTrue(got.all.isEmpty, "떠난 뷰어의 채널이 입력을 배달했다")
        await b.stop()
    }

    /// 대체된 시도의 늦은 데이터 채널은 핸들러도 hello 도 받지 않고, 그 세션의 ICE 후보는 중계되지 않는다.
    func testASupersededSessionStaysSilent() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let wiring = WiringLog()
        let got = PointerBox()
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, sendIce, _, wireInput in
                wiring.add(sendIce: sendIce, wireInput: wireInput)
                return FakeSession()
            },
            onRemoteInput: { p, _ in got.add(p) })
        await settle()
        transport.signal(Self.offer("v"))
        await settle(250)
        transport.signal(Self.offer("v")) // 첫 세션을 대체한다
        await settle(250)
        XCTAssertEqual(wiring.count, 2)

        let stale = FakeInputChannel(label: Protocol.RemoteInput.channel)
        wiring.wireInput(0)?(stale)
        wiring.sendIce(0)?(["candidate": "stale"])
        wiring.sendIce(1)?(["candidate": "live"])
        await settle(150)
        stale.push(#"{"t":"down","p":0,"x":0.5,"y":0.5}"#)
        await settle(150)

        XCTAssertTrue(stale.sent.isEmpty, "대체된 세션의 채널에 hello 를 보냈다")
        XCTAssertTrue(got.all.isEmpty, "대체된 세션의 채널이 입력을 배달했다")
        let relayed = transport.sent.filter { ($0["kind"] as? String) == "ice" }
            .compactMap { ($0["candidate"] as? [String: Any])?["candidate"] as? String }
        XCTAssertEqual(relayed, ["live"], "대체된 세션의 ICE 후보를 중계했다")
        await b.stop()
    }

    /// 뷰어 정리는 hello 재알림 태스크와 input-dropped 키까지 걷는다. 스스로 끝난 재알림 루프의
    /// 항목도 남기지 않는다.
    func testViewerCleanupClearsHelloTasksAndDropDedupe() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let channels = ChannelLog()
        let b = await startScreenBroadcast(
            client: client,
            createSession: { viewer, _, _, wireInput in
                let channel = FakeInputChannel(label: Protocol.RemoteInput.channel)
                channels.add(viewer, channel)
                wireInput(channel)
                return FakeSession()
            },
            onRemoteInput: { _, _ in })
        await b.setHelloReannounceMs(30)
        await settle()
        transport.holder("someone-else") // 게이트를 엄격하게 — 두 뷰어의 입력은 폐기 · 기록된다
        await settle(80)
        transport.signal(Self.offer("v1"))
        transport.signal(Self.offer("v2"))
        await settle(250)
        for v in ["v1", "v2"] { channels.get(v)?.push(#"{"t":"down","p":0,"x":0.5,"y":0.5}"#) }
        await settle(150)
        var hello = await b.helloTaskCount
        var dropped = await b.droppedNotedCount
        XCTAssertEqual(hello, 2)
        XCTAssertEqual(dropped, 2)

        transport.signal(["kind": "peer-left", "participantId": "v1"])
        await settle(150)
        hello = await b.helloTaskCount
        dropped = await b.droppedNotedCount
        XCTAssertEqual(hello, 1, "peer-left 가 그 뷰어의 hello 재알림을 멈추지 않았다")
        XCTAssertEqual(dropped, 1, "peer-left 가 그 뷰어의 input-dropped 키를 지우지 않았다")
        let v1Sent = channels.get("v1")?.sent.count ?? -1
        await settle(150)
        XCTAssertEqual(channels.get("v1")?.sent.count, v1Sent, "떠난 뷰어에게 hello 가 계속 나간다")

        // 채널이 닫혀 송신이 실패하면 루프가 끝나고 — 그 항목도 지워진다(세션은 아직 있다).
        channels.get("v2")?.failSend = true
        await settle(200)
        hello = await b.helloTaskCount
        XCTAssertEqual(hello, 0, "끝난 재알림 루프의 항목이 남았다")
        let count = await b.viewerCount
        XCTAssertEqual(count, 1)
        await b.stop()
    }

    /// 같은 뷰어의 offer 가 겹치면 세션은 정확히 하나만 남고 나머지는 닫힌다. 떠나는 중에 만들어진
    /// 세션도 닫힌다.
    func testOverlappingOffersFromOneViewerKeepExactlyOneSession() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let log = SessionLog()
        let entered = CallCounter()
        let b = await startScreenBroadcast(client: client, createSession: { viewer, _, _, _ in
            _ = entered.next()
            try? await Task.sleep(nanoseconds: 100_000_000) // 실제 피어 생성처럼 시간이 든다
            let s = FakeSession()
            log.add(viewer, s)
            return s
        })
        await settle()
        transport.signal(Self.offer("v"))
        // 첫 시도가 세션을 만드는 **도중**에 둘째 offer 가 온다(재진입 창).
        let deadline = Date().addingTimeInterval(3)
        while entered.peek == 0, Date() < deadline { await settle(5) }
        transport.signal(Self.offer("v"))
        await settle(500)
        XCTAssertEqual(log.sessions(of: "v").count, 2)
        XCTAssertEqual(log.sessions(of: "v").filter { !$0.closed }.count, 1, "겹친 offer 가 세션을 새게 했다")
        XCTAssertEqual(transport.answers.count, 1, "낡은 시도도 answer 를 보냈다")
        var count = await b.viewerCount
        XCTAssertEqual(count, 1)

        // 만드는 도중 떠난 뷰어 — 끝나는 즉시 스스로 닫는다.
        transport.signal(Self.offer("gone"))
        await settle(10)
        transport.signal(["kind": "peer-left", "participantId": "gone"])
        await settle(300)
        XCTAssertTrue(log.sessions(of: "gone").allSatisfy(\.closed))
        count = await b.viewerCount
        XCTAssertEqual(count, 1)
        let generations = await b.generationCount
        XCTAssertEqual(generations, 1, "떠난 뷰어의 세대 항목이 남았다")
        await b.stop()
    }

    /// 낡은 시도의 실패가 새 세션을 닫지 않는다.
    func testFailureOfAnOutdatedAttemptDoesNotCloseTheLiveSession() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        // 첫 세션의 실패는 문으로 잡아 둔다 — 둘째 시도가 answer 를 보낸 **뒤에** 연다. 종전에는 "200 ms 뒤 실패" 와 "60 ms 뒤
        // 둘째 offer" 를 벽시계로 겹쳤는데, 그 사이에서 CI 가 밀리면 둘째 offer 가 첫 실패 뒤에 닿아 첫 시도가 아직 현재라 그
        // 실패가 onError 로 올라갔다(2026-10-25 CI macos-14; 사본에서 둘째 offer 를 300 ms 뒤로 미루면 3/3 재현).
        let failGate = AsyncGate()
        let first = FakeSession(failAnswer: true)
        first.answerGate = failGate
        let second = FakeSession()
        let calls = CallCounter()
        let errors = ErrorBox()
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, _ in calls.next() == 0 ? first : second },
            onError: { _, viewer in errors.add(viewer) })
        await settle()
        transport.signal(Self.offer("v"))
        try await waitUntil { first.answering } // 첫 세션이 맵에 들어가 answer 를 기다리는 중
        transport.signal(Self.offer("v"))
        try await waitUntil { transport.answers.count == 1 } // 둘째 시도가 답했다 — 첫 세션은 이미 놓였다
        failGate.open() // 이제 첫 answer 가 실패한다 — 낡은 시도의 실패다
        await settle() // 그 실패가 현재 뷰어의 것으로 올라갔다면 닿고도 남을 시간

        XCTAssertTrue(first.closed)
        XCTAssertFalse(second.closed, "낡은 시도의 실패가 새 세션을 닫았다")
        XCTAssertTrue(errors.all.isEmpty, "낡은 시도의 실패를 현재 뷰어의 실패로 알렸다")
        let count = await b.viewerCount
        XCTAssertEqual(count, 1)
        XCTAssertEqual(transport.answers.count, 1)
        await b.stop()
    }

    // ── 시그널링 디스패치 순서 — 장부는 도착 순서대로, 협상 본문만 따로(TS `onFrame` 과 같다) ──────────────

    /// 세션 생성에 시간이 드는(실제 피어처럼) 송출 — 뒤 프레임이 디스패치되는 동안 앞 협상이 아직 진행 중이다.
    private func slowSessionBroadcast(_ transport: FakeTransport, _ log: SessionLog) async throws -> ScreenBroadcast {
        let client = try await openClient(transport)
        let b = await startScreenBroadcast(client: client, createSession: { viewer, _, _, _ in
            try? await Task.sleep(nanoseconds: 20_000_000)
            let s = FakeSession()
            log.add(viewer, s)
            return s
        })
        await settle()
        return b
    }

    private func answeredSdps(_ transport: FakeTransport, _ viewer: String) -> [String] {
        transport.answers
            .filter { ($0["targetParticipantId"] as? String) == viewer }
            .compactMap { ($0["description"] as? [String: Any])?["sdp"] as? String }
    }

    /// 같은 뷰어가 offer 를 연달아 두 번 보내면 뒤의 것만 답하고, 남는 세션도 뒤의 것이다. 세대를 협상 본문에서
    /// 매기던 때는 본문 태스크가 닿는 순서로 정해져 옛 offer 가 이길 수 있었다(낡은 answer 를 보냈다).
    func testTheLaterOfTwoQuickOffersFromOneViewerWins() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await slowSessionBroadcast(transport, log)
        let viewers = (0..<500).map { "v\($0)" }
        let socketQueue = DispatchQueue(label: "fake-socket-queue")
        socketQueue.async {
            for v in viewers {
                transport.signal(Self.offer(v, sdp: "old-\(v)"))
                transport.signal(Self.offer(v, sdp: "new-\(v)"))
            }
        }
        let deadline = Date().addingTimeInterval(10)
        while transport.answers.count < viewers.count, Date() < deadline { await settle(10) }
        await settle(150) // 늦게 오는 낡은 answer 까지 기다린다
        var staleAnswer: [String] = []
        var staleSession: [String] = []
        for v in viewers {
            if answeredSdps(transport, v) != ["answer:new-\(v)"] { staleAnswer.append(v) }
            let open = log.sessions(of: v).filter { !$0.closed }
            if open.count != 1 || open.first?.offers != ["new-\(v)"] { staleSession.append(v) }
        }
        XCTAssertEqual(staleAnswer, [], "옛 offer 에 답했다(또는 답이 둘)")
        XCTAssertEqual(staleSession, [], "남은 세션이 뒤의 offer 의 것이 아니다")
        let count = await b.viewerCount
        XCTAssertEqual(count, viewers.count)
        await b.stop()
    }

    /// offer 바로 뒤의 peer-left — 떠난 뷰어의 세션도 세대 항목도 남지 않는다(장부를 디스패치에서 고친다).
    func testAnOfferFollowedByPeerLeftLeavesNothingBehind() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await slowSessionBroadcast(transport, log)
        let viewers = (0..<200).map { "gone\($0)" }
        let socketQueue = DispatchQueue(label: "fake-socket-queue")
        socketQueue.async {
            for v in viewers {
                transport.signal(Self.offer(v))
                transport.signal(["kind": "peer-left", "participantId": v])
            }
        }
        await settle(400)
        let count = await b.viewerCount
        let generations = await b.generationCount
        XCTAssertEqual(count, 0, "떠난 뷰어의 세션이 남았다")
        XCTAssertEqual(generations, 0, "떠난 뷰어의 세대 항목이 남았다")
        XCTAssertTrue(log.all.allSatisfy { $0.session.closed }, "만든 세션이 닫히지 않았다")
        XCTAssertEqual(transport.answers.count, 0, "떠난 뷰어에게 answer 를 보냈다")
        await b.stop()
    }

    /// peer-left 바로 뒤에 같은 참여자가 새로 offer 한다(PIN 게스트처럼 id 가 그대로인 재입장) — 떠남은 그 offer 보다
    /// 먼저 처리돼야 한다. 늦게 처리되면 방금 시작한 새 협상의 세대를 지워 새 세션까지 닫는다.
    func testAPeerLeftFollowedByAFreshOfferKeepsTheFreshSession() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await slowSessionBroadcast(transport, log)
        let viewers = (0..<200).map { "pin\($0)" }
        for v in viewers { transport.signal(Self.offer(v, sdp: "first-\(v)")) }
        let deadline = Date().addingTimeInterval(10)
        while transport.answers.count < viewers.count, Date() < deadline { await settle(10) }
        let socketQueue = DispatchQueue(label: "fake-socket-queue")
        socketQueue.async {
            for v in viewers {
                transport.signal(["kind": "peer-left", "participantId": v])
                transport.signal(Self.offer(v, sdp: "back-\(v)"))
            }
        }
        let backBy = Date().addingTimeInterval(10)
        while transport.answers.count < 2 * viewers.count, Date() < backBy { await settle(10) }
        await settle(150)
        var lost: [String] = []
        for v in viewers {
            let open = log.sessions(of: v).filter { !$0.closed }
            if open.count != 1 || open.first?.offers != ["back-\(v)"] { lost.append(v) }
        }
        XCTAssertEqual(lost, [], "늦게 처리된 peer-left 가 새로 들어온 뷰어의 세션을 닫았다")
        let count = await b.viewerCount
        XCTAssertEqual(count, viewers.count)
        await b.stop()
    }

    /// `peers` 대조는 받은 그 자리에서 한다 — 늦게 하면 다음 끊김의 스냅샷(그 사이 새로 온 뷰어가 든)을 옛 답으로 대조해
    /// 새 뷰어를 닫았다. 순서: 끊김 → 붙음 → 답[v1] → offer(v2) → 끊김 → 붙음.
    func testAPeersReplyIsReconciledBeforeTheNextDisconnect() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        transport.signal(Self.offer("v1"))
        await settle(200)
        let socketQueue = DispatchQueue(label: "fake-socket-queue")
        socketQueue.async {
            transport.setState(.reconnecting)
            transport.setState(.connected)
            transport.signal(Self.peers(["v1"])) // 첫 재접속의 답 — v2 는 아직 없다
            transport.signal(Self.offer("v2"))   // 새 뷰어
            transport.setState(.reconnecting)    // 두 번째 끊김 — 스냅샷 {v1, v2}
            transport.setState(.connected)
        }
        await settle(400)
        XCTAssertEqual(log.sessions(of: "v2").count, 1)
        XCTAssertFalse(log.sessions(of: "v2").first?.closed ?? true, "앞 접속의 peers 답이 새 뷰어를 닫았다")
        XCTAssertFalse(log.sessions(of: "v1").first?.closed ?? true)
        await b.stop()
    }

    /// 끊김 → 붙음 전이가 헬퍼에 도착 순서대로 닿아야 끊김마다 스냅샷이 찍히고 `peers` 답이 떠난 뷰어를 닫는다 —
    /// 붙음이 끊김보다 먼저 처리되면 "끊긴 중" 으로 굳어 다음 스냅샷을 찍지 못했다. 한 번으로는 드물어 300번 잰다.
    func testEveryDisconnectIsReconciledInTransitionOrder() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await loggedBroadcast(transport, log)
        let socketQueue = DispatchQueue(label: "fake-socket-queue")
        var leaked: [Int] = []
        for round in 0..<300 {
            let viewer = "v\(round)"
            transport.signal(Self.offer(viewer))
            var deadline = Date().addingTimeInterval(2)
            while log.sessions(of: viewer).isEmpty, Date() < deadline { try await Task.sleep(nanoseconds: 200_000) }
            let readyBefore = transport.kinds.filter { $0 == "ready" }.count
            socketQueue.async {
                transport.setState(.reconnecting)
                transport.setState(.connected)
            }
            deadline = Date().addingTimeInterval(2)
            while transport.kinds.filter({ $0 == "ready" }).count <= readyBefore, Date() < deadline {
                try await Task.sleep(nanoseconds: 200_000)
            }
            guard transport.kinds.filter({ $0 == "ready" }).count > readyBefore else {
                return XCTFail("round \(round): 재접속 뒤 ready 를 보내지 않았다")
            }
            transport.signal(Self.peers([])) // 모두 떠났다
            deadline = Date().addingTimeInterval(1)
            while log.sessions(of: viewer).first?.closed != true, Date() < deadline {
                try await Task.sleep(nanoseconds: 200_000)
            }
            if log.sessions(of: viewer).first?.closed != true { leaked.append(round) }
        }
        XCTAssertEqual(leaked, [], "끊긴 동안 떠난 뷰어가 대조되지 않았다")
        await b.stop()
    }

    /// 재접속 뒤 `peers` 답보다 먼저 온 재협상 offer 는 그 답에 닫히지 않는다 — 세대가 디스패치에서 바뀌었으므로
    /// 대조가 건너뛴다. 재협상하지 않은 채 답에서 빠진 뷰어는 닫힌다.
    func testARenegotiationDispatchedBeforeThePeersReplySurvivesIt() async throws {
        let transport = FakeTransport()
        let log = SessionLog()
        let b = try await slowSessionBroadcast(transport, log)
        for v in ["v1", "v2"] { transport.signal(Self.offer(v, sdp: "first-\(v)")) }
        await settle(250)
        reconnect(transport)
        await settle(150)
        let socketQueue = DispatchQueue(label: "fake-socket-queue")
        socketQueue.async {
            transport.signal(Self.offer("v1", sdp: "again-v1"))
            transport.signal(Self.peers(["host"])) // v1 · v2 둘 다 없다
        }
        await settle(300)
        let v1 = log.sessions(of: "v1")
        XCTAssertEqual(v1.count, 2)
        XCTAssertTrue(v1[0].closed, "재협상이 옛 세션을 닫지 않았다")
        XCTAssertFalse(v1[1].closed, "peers 답이 그보다 먼저 온 재협상의 세션을 닫았다")
        XCTAssertEqual(answeredSdps(transport, "v1").last, "answer:again-v1")
        XCTAssertTrue(log.sessions(of: "v2").allSatisfy(\.closed), "답에서 빠진 뷰어가 남았다")
        let count = await b.viewerCount
        XCTAssertEqual(count, 1)
        await b.stop()
    }

    /// 앱이 stop() 을 잊고 클라이언트만 닫아도 헬퍼가 스스로 멈춘다 — 재알림 태스크 0, 세션 close.
    func testClientEndTearsTheBroadcastDown() async throws {
        let transport = FakeTransport()
        let client = try await openClient(transport)
        let channel = FakeInputChannel(label: Protocol.RemoteInput.channel)
        let session = FakeSession()
        let b = await startScreenBroadcast(
            client: client,
            createSession: { _, _, _, wireInput in
                wireInput(channel)
                return session
            },
            onRemoteInput: { _, _ in })
        await b.setHelloReannounceMs(30)
        await settle()
        transport.signal(Self.offer("v1"))
        await settle(250)
        var hello = await b.helloTaskCount
        XCTAssertEqual(hello, 1)

        await client.close()
        await settle(250)
        XCTAssertTrue(session.closed, "클라이언트가 끝났는데 뷰어 세션이 열려 있다")
        hello = await b.helloTaskCount
        XCTAssertEqual(hello, 0)
        let count = await b.viewerCount
        XCTAssertEqual(count, 0)
        let sent = channel.sent.count
        await settle(150)
        XCTAssertEqual(channel.sent.count, sent, "클라이언트가 끝난 뒤에도 hello 가 나간다")
    }
}

// ── 테스트 보조 상자 (actor 격리 밖에서 값을 모은다) ──────────────────────────

/// 팩토리가 받은 ICE 서버 목록 기록.
final class IceListLog: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [[IceServer]] = []
    func add(_ list: [IceServer]) { lock.withLock { items.append(list) } }
    var all: [[IceServer]] { lock.withLock { items } }
}

final class PointerBox: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [RemoteInputPointer] = []
    var all: [RemoteInputPointer] { lock.withLock { items } }
    func add(_ p: RemoteInputPointer) { lock.withLock { items.append(p) } }
}

final class ErrorBox: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [String] = []
    var all: [String] { lock.withLock { items } }
    func add(_ v: String) { lock.withLock { items.append(v) } }
}

final class SendBox: @unchecked Sendable {
    private let lock = NSLock()
    private var fn: (@Sendable ([String: Any]) -> Void)?
    func set(_ f: @escaping @Sendable ([String: Any]) -> Void) { lock.withLock { fn = f } }
    func get() -> (@Sendable ([String: Any]) -> Void)? { lock.withLock { fn } }
}

/// 뷰어별 입력 채널 기록.
final class ChannelLogBox<C: AnyObject>: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [String: C] = [:]
    func add(_ viewer: String, _ c: C) { lock.withLock { items[viewer] = c } }
    func get(_ viewer: String) -> C? { lock.withLock { items[viewer] } }
}

/// 뷰어별로 받은 포인터 기록.
final class ViewerPointerLog: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [(pointer: RemoteInputPointer, viewer: String)] = []
    func add(_ pointer: RemoteInputPointer, _ viewer: String) { lock.withLock { items.append((pointer, viewer)) } }
    var all: [(pointer: RemoteInputPointer, viewer: String)] { lock.withLock { items } }
}

/// 호출 순번.
final class CallCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0
    func next() -> Int { lock.withLock { defer { value += 1 }; return value } }
    var peek: Int { lock.withLock { value } }
}

/// 시도마다 받은 sendIce · wireInput — 대체된 시도의 것을 나중에 불러 본다.
final class WiringLog: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [(sendIce: @Sendable ([String: Any]) -> Void,
                         wireInput: @Sendable (RemoteInputChannel) -> Void)] = []
    func add(sendIce: @escaping @Sendable ([String: Any]) -> Void,
             wireInput: @escaping @Sendable (RemoteInputChannel) -> Void) {
        lock.withLock { items.append((sendIce, wireInput)) }
    }
    var count: Int { lock.withLock { items.count } }
    func sendIce(_ i: Int) -> (@Sendable ([String: Any]) -> Void)? { lock.withLock { i < items.count ? items[i].sendIce : nil } }
    func wireInput(_ i: Int) -> (@Sendable (RemoteInputChannel) -> Void)? { lock.withLock { i < items.count ? items[i].wireInput : nil } }
}

/// 한 번 열리면 기다리는 쪽을 모두 깨우는 문 — 세션의 answer · 후보 적용을 붙잡는다.
final class AsyncGate: @unchecked Sendable {
    private let lock = NSLock()
    private var opened = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            let now = lock.withLock { () -> Bool in
                if opened { return true }
                waiters.append(continuation)
                return false
            }
            if now { continuation.resume() }
        }
    }

    func open() {
        let ready = lock.withLock { () -> [CheckedContinuation<Void, Never>] in
            opened = true
            let all = waiters
            waiters = []
            return all
        }
        for waiter in ready { waiter.resume() }
    }
}
