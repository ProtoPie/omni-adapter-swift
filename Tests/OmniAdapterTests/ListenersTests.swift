import XCTest
@testable import OmniAdapter

/// `listeners(_:)` — 이 채널을 받을 사람 (다중 프로그램 토폴로지). 가장 흔한 조용한 실패(채널
/// 이름 오타·아직 안 뜬 상대)를 발행 전에 잡는다.
final class ListenersTests: XCTestCase {
    private final class RosterFakeTransport: Transport, @unchecked Sendable {
        private let lock = NSLock()
        private var rosterStorage: [Member] = []
        func setRoster(_ roster: [Member]) { lock.withLock { rosterStorage = roster } }
        func open(handlers: TransportHandlers) async throws -> Joined {
            Joined(roomId: "stage-1", me: Member(participantId: "me-1", displayName: "me"))
        }
        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult { PublishResult(ok: true) }
        func listMembers() async throws -> [Member] { lock.withLock { rosterStorage } }
        func close() async {}
    }

    // 나도 이 채널을 선언한다 — 자기 자신은 결과에서 빠져야 한다
    private let me = Member(participantId: "me-1", displayName: "me", receives: ["robot.status"])
    private let exact = Member(participantId: "p-exact", displayName: "exact", receives: ["robot.status"], strictRouting: true)
    private let wild = Member(participantId: "p-wild", displayName: "wild", receives: ["robot.*"])
    private let strictOther = Member(participantId: "p-strict", displayName: "strict-other", receives: ["door.open"], strictRouting: true)
    private let looseOther = Member(participantId: "p-loose", displayName: "loose-other", receives: ["door.open"])
    private let looseFalse = Member(participantId: "p-false", displayName: "loose-false", receives: ["door.open"], strictRouting: false)
    private let browser = Member(participantId: "p-browser", displayName: "host")
    // 빈 receives 는 "아무것도 선언 안 함"(미선언이 아니다) — strict 면 어느 목록에도 없고, 아니면 브로드캐스트
    private let emptyStrict = Member(participantId: "p-empty-strict", displayName: "empty-strict", receives: [], strictRouting: true)
    private let emptyLoose = Member(participantId: "p-empty-loose", displayName: "empty-loose", receives: [])

    private func openWith(_ roster: [Member]) async throws -> (RosterFakeTransport, ConnectClient) {
        let transport = RosterFakeTransport()
        transport.setRoster(roster)
        let client = ConnectClient(transport: transport)
        try await client.open()
        return (transport, client)
    }
    private func noted(_ client: ConnectClient) -> [LogEntry] { client.diagnostics().filter { $0.code == "no-listener" } }

    func testSplitsDeclaredAndBroadcastAndDropsSelfAndStrictNonMatch() async throws {
        let (_, client) = try await openWith([me, exact, wild, strictOther, looseOther, looseFalse, browser, emptyStrict, emptyLoose])
        let r = try await client.listeners("robot.status")
        XCTAssertEqual(r.channel, "robot.status")
        XCTAssertEqual(r.declared.map(\.participantId), ["p-exact", "p-wild"])
        // 다른 채널만 선언 + strict → 안 받는다(어느 목록에도 없다). 명시적 false 와 부재는 브로드캐스트로 받는다.
        XCTAssertEqual(r.broadcast.map(\.participantId), ["p-loose", "p-false", "p-browser", "p-empty-loose"])
        XCTAssertTrue(noted(client).isEmpty)
    }

    func testNobodyIsWarnOncePerChannelAndRearmsWhenADeclarerReturns() async throws {
        let (transport, client) = try await openWith([me, strictOther])
        let r = try await client.listeners("robot.status")
        // 어느 쪽이 비었는지 따로 본다 — 합쳐 두면 실패 출력이 원인을 못 말한다.
        XCTAssertTrue(r.declared.isEmpty, "declared: \(r.declared)")
        XCTAssertTrue(r.broadcast.isEmpty, "broadcast: \(r.broadcast)")
        XCTAssertEqual(noted(client).map(\.level), ["warn"])
        // 접미사까지 고정한다 — "몇 명이 그래도 받는가" 가 이 기록의 절반이다.
        XCTAssertEqual(noted(client).first?.message, "robot.status: no member declared it (0 broadcast receiver(s))")
        _ = try await client.listeners("robot.status") // 두 번째 호출은 기록하지 않는다
        XCTAssertEqual(noted(client).count, 1)
        transport.setRoster([me, exact]) // 선언자가 나타남 → 해제
        let again = try await client.listeners("robot.status")
        XCTAssertEqual(again.declared.count, 1)
        transport.setRoster([me, strictOther]) // 다시 사라짐 → 한 번 더
        _ = try await client.listeners("robot.status")
        XCTAssertEqual(noted(client).count, 2)
    }

    func testBroadcastOnlyIsInfoAndEmptyRosterRecordsNothing() async throws {
        let (_, some) = try await openWith([me, browser])
        let r = try await some.listeners("robot.status")
        XCTAssertTrue(r.declared.isEmpty)
        XCTAssertEqual(r.broadcast.map(\.participantId), ["p-browser"])
        XCTAssertEqual(noted(some).map(\.level), ["info"])

        let (_, none) = try await openWith([])
        let r2 = try await none.listeners("robot.status")
        XCTAssertTrue(r2.declared.isEmpty && r2.broadcast.isEmpty)
        XCTAssertTrue(noted(none).isEmpty)
    }
}
