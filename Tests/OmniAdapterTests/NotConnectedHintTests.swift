import XCTest
@testable import OmniAdapter

/// 세션 중 NotConnected 의 힌트 — 기본 힌트("토큰의 서버 주소를 확인하라 — LAN IP 가 바뀐다")는 접속 실패용이다. 합류 전 · 재접속
/// 중 · 끝난 뒤에 members · listeners · ping · 등록을 부르면 그 자리의 상태에 맞는 힌트를 준다(현장 시험이 찾은 엉뚱한 안내).
/// 문장은 여섯 언어가 같다.
final class NotConnectedHintTests: XCTestCase {
    private let notJoined =
        "This client has not joined the stage yet — wait for connect to finish, then call again."
    private let reconnecting =
        "The connection dropped and the adapter is reconnecting on its own — call again once the state is connected, and re-read the member list then (joins and leaves during the drop are not replayed)."
    private let ended =
        "This client has ended (it was closed, or the session ended) — create a new client to join again."

    /// 소켓이 없을 때의 실제 트랜스포트처럼 roster · 등록 왕복이 NotConnected 로 실패한다.
    private final class OfflineTransport: Transport, @unchecked Sendable {
        private let lock = NSLock()
        private var handlersStorage = TransportHandlers()
        private var openHeld = false
        private var onlineStorage = false
        private var ownHintStorage: String?
        /// 트랜스포트가 스스로 다는 힌트(없으면 기본 힌트).
        func setOwnHint(_ hint: String?) { lock.withLock { ownHintStorage = hint } }

        var handlers: TransportHandlers { lock.withLock { handlersStorage } }
        func holdOpen(_ held: Bool) { lock.withLock { openHeld = held } }
        func setOnline(_ online: Bool) { lock.withLock { onlineStorage = online } }

        func open(handlers: TransportHandlers) async throws -> Joined {
            lock.withLock { handlersStorage = handlers }
            while lock.withLock({ openHeld }) { try? await Task.sleep(nanoseconds: 5_000_000) }
            return Joined(roomId: "stage-1", me: Member(participantId: "me-1", displayName: "hint"))
        }

        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult { PublishResult(ok: true) }

        func listMembers() async throws -> [Member] {
            if lock.withLock({ onlineStorage }) { return [] }
            throw AdapterError("NotConnected", "socket is not connected", hint: lock.withLock { ownHintStorage })
        }

        func submitRegistrationCode(_ code: String) async throws -> AdapterRegistrationResult {
            throw AdapterError("NotConnected", "socket is not connected")
        }

        func resumeRegistration(stageId: String, deviceSecret: String) async throws -> AdapterRegistrationResult {
            throw AdapterError("NotConnected", "socket is not connected")
        }

        func close() async {}
    }

    private func failure(_ body: () async throws -> Void) async -> AdapterError? {
        do {
            try await body()
            return nil
        } catch {
            return error as? AdapterError
        }
    }

    private func allCallsCarry(_ client: ConnectClient, _ hint: String, line: UInt = #line) async {
        let members = await failure { _ = try await client.members() }
        XCTAssertEqual(members?.code, "NotConnected", line: line)
        XCTAssertEqual(members?.message, "socket is not connected", line: line)
        XCTAssertEqual(members?.hint, hint, line: line)
        let listeners = await failure { _ = try await client.listeners("a.b") }
        XCTAssertEqual(listeners?.code, "NotConnected", line: line)
        XCTAssertEqual(listeners?.hint, hint, line: line)
        let ping = await failure { _ = try await client.ping() }
        XCTAssertEqual(ping?.code, "NotConnected", line: line)
        XCTAssertEqual(ping?.hint, hint, line: line)
        let submit = await failure { _ = try await client.submitRegistrationCode("123456") }
        XCTAssertEqual(submit?.code, "NotConnected", line: line)
        XCTAssertEqual(submit?.message, "socket is not connected", line: line)
        XCTAssertEqual(submit?.hint, hint, line: line)
    }

    func testBeforeJoiningTheHintSaysWaitForConnect() async throws {
        let transport = OfflineTransport()
        let client = ConnectClient(transport: transport)
        await allCallsCarry(client, notJoined)  // idle — open 을 부르지 않았다
        let resume = await failure { _ = try await client.resumeRegistration(deviceSecret: "secret") }
        XCTAssertEqual(resume?.code, "NotConnected")
        XCTAssertEqual(resume?.hint, notJoined)

        transport.holdOpen(true)
        let opening = Task { try await client.open() }  // connecting — 합류 응답을 기다린다
        for _ in 0..<200 where await client.state != .connecting { try await Task.sleep(nanoseconds: 5_000_000) }
        let state = await client.state
        XCTAssertEqual(state, .connecting)
        await allCallsCarry(client, notJoined)
        transport.holdOpen(false)
        try await opening.value
    }

    func testWhileReconnectingTheHintSaysItRecoversOnItsOwn() async throws {
        let transport = OfflineTransport()
        let client = ConnectClient(transport: transport)
        try await client.open()
        transport.handlers.onState?(.reconnecting)
        for _ in 0..<200 where await client.state != .reconnecting { try await Task.sleep(nanoseconds: 5_000_000) }
        await allCallsCarry(client, reconnecting)
        let resume = await failure { _ = try await client.resumeRegistration(deviceSecret: "secret") }
        XCTAssertEqual(resume?.code, "NotConnected")
        XCTAssertEqual(resume?.hint, reconnecting)
    }

    func testAfterTheEndTheHintSaysCreateANewClient() async throws {
        let transport = OfflineTransport()
        let client = ConnectClient(transport: transport)
        try await client.open()
        await client.close()
        await allCallsCarry(client, ended)
    }

    /// 상태 힌트는 기본 힌트만 바꾼다 — 트랜스포트가 스스로 단 힌트는 그 자리를 더 잘 안다.
    func testAHintTheTransportSetItselfIsKept() async throws {
        let transport = OfflineTransport()
        transport.setOwnHint("the transport knows better")
        let client = ConnectClient(transport: transport)
        try await client.open()
        transport.handlers.onState?(.reconnecting)
        for _ in 0..<200 where await client.state != .reconnecting { try await Task.sleep(nanoseconds: 5_000_000) }
        let members = await failure { _ = try await client.members() }
        XCTAssertEqual(members?.hint, "the transport knows better")
    }

    /// MemoryStage 의 트랜스포트도 끊기면 기본 힌트의 NotConnected 를 낸다 — 클라이언트를 거치며 상태 힌트로 바뀐다.
    func testMemoryStageErrorsGoThroughTheClientToo() async throws {
        let stage = MemoryStage()
        let client = try await stage.join(MemoryJoinOptions(displayName: "m"))
        try await stage.disconnect(client)
        for _ in 0..<200 where await client.state != .reconnecting { try await Task.sleep(nanoseconds: 5_000_000) }
        let dropped = await failure { _ = try await client.members() }
        XCTAssertEqual(dropped?.code, "NotConnected")
        XCTAssertEqual(dropped?.hint, reconnecting)
        try await stage.reconnect(client)
        await client.close()
        let gone = await failure { _ = try await client.members() }
        XCTAssertEqual(gone?.code, "NotConnected")
        XCTAssertEqual(gone?.hint, ended)
    }

    /// 상태가 connected 인데 트랜스포트가 NotConnected 를 냈다(끊김을 아직 못 들었다) — 고를 상태 힌트가 없으니 원래 오류 그대로다.
    func testWhileConnectedTheTransportsOwnHintIsKept() async throws {
        let transport = OfflineTransport()
        let client = ConnectClient(transport: transport)
        try await client.open()
        let members = await failure { _ = try await client.members() }
        XCTAssertEqual(members?.hint, AdapterError("NotConnected").hint)
        transport.setOnline(true)
        let roster = try await client.members()
        XCTAssertEqual(roster, [])
    }
}

/// 끝난 뒤의 시그널링 — 트랜스포트에 넘기지 않는다(TS 와 같다). 서버는 시그널링을 다른 참여자에게 중계한다.
final class SignalingAfterEndTests: XCTestCase {
    private final class SignalTransport: Transport, @unchecked Sendable {
        private let lock = NSLock()
        private var handlersStorage = TransportHandlers()
        private var sentStorage = 0
        private var closeHeld = true
        var handlers: TransportHandlers { lock.withLock { handlersStorage } }
        var sent: Int { lock.withLock { sentStorage } }
        func open(handlers: TransportHandlers) async throws -> Joined {
            lock.withLock { handlersStorage = handlers }
            return Joined(roomId: "stage-1", me: Member(participantId: "me-1", displayName: "probe"))
        }
        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult { PublishResult(ok: true) }
        func listMembers() async throws -> [Member] { [] }
        func sendSignaling(_ payload: [String: Any]) async { lock.withLock { sentStorage += 1 } }
        /// 퇴장 확인을 기다리는 꼴 — 풀 때까지 붙잡는다(그동안 소켓은 붙어 있다).
        func close() async {
            while lock.withLock({ closeHeld }) { try? await Task.sleep(nanoseconds: 5_000_000) }
        }
        func releaseClose() { lock.withLock { closeHeld = false } }
    }

    private func untilEnded(_ client: ConnectClient) async {
        for _ in 0..<400 where await client.state != .ended { try? await Task.sleep(nanoseconds: 5_000_000) }
    }

    /// close() 는 먼저 끝내고 퇴장 확인을 기다린다(최대 1초) — 그 사이의 시그널링이 서버에 닿아 중계됐다(검수 실측 90/90).
    func testSignalingDuringTheLeaveWaitIsNotForwarded() async throws {
        let transport = SignalTransport()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        await client.sendSignaling(["kind": "probe"])
        XCTAssertEqual(transport.sent, 1)
        let closing = Task { await client.close() }
        await untilEnded(client)
        await client.sendSignaling(["kind": "late"])
        XCTAssertEqual(transport.sent, 1, "a frame sent after the end reached the transport")
        transport.releaseClose()
        await closing.value
    }

    /// 서버의 세션 종료도 같다 — 끝난 뒤의 시그널링은 넘기지 않는다.
    func testSignalingAfterTheServerEndedTheSessionIsNotForwarded() async throws {
        let transport = SignalTransport()
        transport.releaseClose()
        let client = ConnectClient(transport: transport, exclusiveName: false)
        try await client.open()
        transport.handlers.onEnded?("host-closed")
        await untilEnded(client)
        await client.sendSignaling(["kind": "late"])
        XCTAssertEqual(transport.sent, 0, "a frame sent after the end reached the transport")
    }
}
