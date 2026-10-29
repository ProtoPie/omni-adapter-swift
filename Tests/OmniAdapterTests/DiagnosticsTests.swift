import Foundation
import XCTest

@testable import OmniAdapter

/// 진단 기록 — 무인 어댑터의 사후 단서 (TS `diagnostics.test.ts` 미러).
///
/// 실사고(2026-08-21)에서 원격 뷰어가 화면을 못 봤는데 어댑터 쪽 로그에 결과가
/// 없어 원인을 가릴 수 없었다. 앱이 `events()` 를 구독하지 **않아도** 흔적이 남아야
/// 하고, 그 흔적에 **자격증명이 섞이면 안 된다**.
final class DiagnosticsTests: XCTestCase {
    private final class FakeTransport: Transport, @unchecked Sendable {
        private let lock = NSLock()
        private var handlersStorage = TransportHandlers()
        var result = PublishResult(ok: true)
        let me = Member(participantId: "me-1", displayName: "Kiosk #3")

        var handlers: TransportHandlers {
            lock.withLock { handlersStorage }
        }

        func open(handlers: TransportHandlers) async throws -> Joined {
            lock.withLock { handlersStorage = handlers }
            return Joined(roomId: "stage-1", me: me)
        }

        func publish(_ input: PublishInput, ack: Bool) async -> PublishResult { result }
        func listMembers() async throws -> [Member] { [me] }
        func close() async {}
    }

    func testDropsOldestOverLimit() {
        // 실패는 마지막 구간에 있다 — 오래된 것을 버려야 한다.
        let diag = Diagnostics(limit: 3, now: { 1 })
        for name in ["a", "b", "c", "d"] { diag.note("state", name) }
        XCTAssertEqual(diag.entries().map(\.message), ["b", "c", "d"])
    }

    func testEntriesIsACopy() {
        let diag = Diagnostics()
        diag.note("state", "connected")
        var copy = diag.entries()
        copy.removeAll()
        XCTAssertEqual(diag.entries().count, 1)
    }

    func testLimitMatchesOtherLanguages() {
        XCTAssertEqual(Diagnostics.limit, 100)
    }

    func testRecordsStateEndedAndErrorWithoutSubscribers() async throws {
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport)
        try await client.open()
        transport.handlers.onError?(AdapterError("ValidationError", "clientType — bad"))
        transport.handlers.onEnded?("rejoin-abandoned: AuthRevoked")
        // 종료 처리는 Task 로 넘어간다 — 기록이 정착할 창을 준다.
        try await Task.sleep(nanoseconds: 100_000_000)

        let seen = client.diagnostics().map { "\($0.code):\($0.message)" }
        XCTAssertTrue(seen.contains("state:connected"), "실제: \(seen)")
        XCTAssertTrue(seen.contains("error:ValidationError: clientType — bad"), "실제: \(seen)")
        XCTAssertTrue(seen.contains("ended:rejoin-abandoned: AuthRevoked"), "실제: \(seen)")
        await client.close()
    }

    func testSinkReceivesTheSameEntries() async throws {
        let box = Box<String>()
        let client = ConnectClient(transport: FakeTransport(), onLog: { box.append($0.code) })
        try await client.open()
        XCTAssertTrue(box.values.contains("state"), "실제: \(box.values)")
        await client.close()
    }

    func testNeverRecordsCredentials() async throws {
        // 토큰은 트랜스포트가 들고 있고 클라이언트는 만지지 않는다 — 그 사실을 고정한다.
        let transport = FakeTransport()
        let client = ConnectClient(transport: transport)
        try await client.open()
        transport.handlers.onError?(AdapterError("AuthRevoked", "token rejected (401)"))

        let entries = client.diagnostics()
        // ⚠️ 비어 있으면 아래 단언이 **공허하게** 통과한다 — 기록 존재를 먼저 고정한다.
        XCTAssertFalse(entries.isEmpty, "기록이 비었다 — 단언이 공허해진다")
        let dump = entries.map { "\($0.code) \($0.message)" }.joined(separator: " ")
        XCTAssertFalse(dump.contains("cst_"), "자격증명이 기록됐다: \(dump)")
        await client.close()
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
