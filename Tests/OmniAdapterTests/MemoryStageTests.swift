import XCTest
@testable import OmniAdapter

/// MemoryStage — 서버 없이 여러 프로그램이 서로를 트리거한다 (TS memory-stage.test.ts 미러).
final class MemoryStageTests: XCTestCase {
    /// 조건을 기다린다(최대 2초) — 고정 80ms 는 부하가 걸린 러너에서 뒤집힌다. 조건이 없으면 종전처럼 80ms.
    private func settle(_ until: (() async -> Bool)? = nil) async {
        guard let until else { try? await Task.sleep(nanoseconds: 80_000_000); return }
        for _ in 0..<400 {
            if await until() { return }
            try? await Task.sleep(nanoseconds: 5_000_000)
        }
    }
    private func p(_ any: Any?) -> Primitive { Primitive.from(any)! }

    /// E6: 테스트 대역에는 서버 발행 한도가 없다 — 토큰 버킷을 걸면 100건 발행이 30건 뒤 초당 60건으로 늘어져, 대역으로 앱을
    /// 시험하는 쪽이 실서버에 없는 지연을 본다. 100건이 곧바로 나가고 큐에 남지 않는다.
    func testTheTestDoubleHasNoServerRateLimitSoTheBucketIsBypassed() async throws {
        let stage = MemoryStage()
        let a = try await stage.join(MemoryJoinOptions(displayName: "burst"))
        try await Task.sleep(nanoseconds: 50_000_000)
        for i in 0..<100 { try await a.send("burst.x", .int(i)) }
        let left = await a.queuedCount
        XCTAssertEqual(left, 0)
        XCTAssertEqual(stage.published.count, 100)
    }

    /// 메모리 레이어 확보 — 이름(앞뒤 공백 무시)마다 하나인 고정된 가짜 레이어. 처음이면 created, 다시 부르면 같은 자리.
    func testEnsureStreamLayerGivesAStableFakeLayerPerName() async throws {
        let stage = MemoryStage()
        let a = try await stage.join(MemoryJoinOptions(displayName: "gauge", providesView: "stream"))
        let b = try await stage.join(MemoryJoinOptions(displayName: " other ", providesView: "stream"))
        let first = try await a.ensureStreamLayer()
        let again = try await a.ensureStreamLayer()
        let other = try await b.ensureStreamLayer()
        XCTAssertEqual(first, EnsuredStreamLayer(layerId: "memory-layer:gauge", name: "gauge", created: true))
        XCTAssertEqual(again, EnsuredStreamLayer(layerId: "memory-layer:gauge", name: "gauge", created: false))
        XCTAssertEqual(other, EnsuredStreamLayer(layerId: "memory-layer:other", name: "other", created: true))
        XCTAssertEqual(stage.streamLayers().map(\.layerId), ["memory-layer:gauge", "memory-layer:other"])

        await a.close()
        do {
            _ = try await a.ensureStreamLayer()
            XCTFail("expected SessionEnded")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "SessionEnded")
        }
    }

    func testEnsureStreamLayerNeedsProvidesViewStreamLikeTheServer() async throws {
        let stage = MemoryStage()
        let a = try await stage.join(MemoryJoinOptions(displayName: "panel"))
        do {
            _ = try await a.ensureStreamLayer()
            XCTFail("expected ValidationError")
        } catch let error as AdapterError {
            XCTAssertEqual(error.code, "ValidationError")
            XCTAssertTrue(error.message.contains("providesView: 'stream'"), error.message)
            XCTAssertEqual(
                error.hint,
                "Connect with providesView: 'stream' — the server only creates and binds a screen layer for adapters that declare a screen.")
        }
        XCTAssertTrue(stage.streamLayers().isEmpty)
    }

    func testAReachesBWithoutEchoAndWildcards() async throws {
        let stage = MemoryStage()
        let a = try await stage.join(MemoryJoinOptions(displayName: "sensor", sends: ["sensor.temperature"]))
        let b = try await stage.join(MemoryJoinOptions(displayName: "dashboard", receives: ["sensor.*"]))
        let gotB = Box<[StageMessage]>([]); let gotA = Box<[StageMessage]>([])
        _ = await b.on("sensor.*") { m in gotB.append(m) }
        _ = await a.on("sensor.temperature") { m in gotA.append(m) }

        try await a.send("sensor.temperature", p(23.5), ack: true)
        await settle { gotB.value.count >= 1 }
        let aId = await a.me?.participantId
        XCTAssertEqual(gotB.value.map { [$0.channel, "\($0.value.doubleValue ?? -1)", $0.sourceParticipantId, $0.messageId ?? ""] },
                       [["sensor.temperature", "23.5", aId ?? "", "sensor.temperature"]])
        XCTAssertTrue(gotA.value.isEmpty) // 에코 없음
        let la = await a.latest("sensor.temperature"); let lb = await b.latest("sensor.temperature")
        XCTAssertEqual(la?.doubleValue, 23.5)
        XCTAssertEqual(lb?.doubleValue, 23.5)
        XCTAssertEqual(stage.published.map { [$0.from, $0.input.channel, "\($0.ack)"] }, [[aId ?? "", "sensor.temperature", "true"]])
    }

    func testStrictRoutingGetsOnlyDeclaredAndTargeted() async throws {
        let stage = MemoryStage()
        let a = try await stage.join(MemoryJoinOptions(displayName: "controller"))
        let c = try await stage.join(MemoryJoinOptions(displayName: "door", receives: ["door.open"], strictRouting: true))
        let got = Box<[String]>([])
        _ = await c.on("door.open") { m in got.append("door:\(m.value.boolValue ?? false)") }
        _ = await c.on("robot.*") { m in got.append("robot:\(m.value.stringValue ?? "")") }

        try await a.send("robot.status", p("moving"), ack: true)
        try await a.send("door.open", p(true), ack: true)
        await settle { got.value.count >= 1 }
        XCTAssertEqual(got.value, ["door:true"])
        let cId = await c.me!.participantId
        try await a.sendTo(cId, "robot.status", p("targeted"), ack: true)
        await settle { got.value.count >= 2 }
        XCTAssertEqual(got.value, ["door:true", "robot:targeted"])
    }

    func testLateJoinerCatchesUpFromHistorySnapshot() async throws {
        let stage = MemoryStage(historyLimit: 2)
        let a = try await stage.join(MemoryJoinOptions(displayName: "sensor"))
        for v in 1...3 { try await a.send("sensor.temperature", p(v), ack: true) }
        let late = try await stage.join(MemoryJoinOptions(displayName: "late"))
        await settle { await late.history().count == 2 }
        let hist = await late.history()
        XCTAssertEqual(hist.map { $0.value.doubleValue }, [2, 3])
        let l = await late.latest("sensor.temperature")
        XCTAssertEqual(l?.doubleValue, 3)
        XCTAssertEqual(stage.history().count, 2)
    }

    func testExclusiveNameNewestIsActiveAndStandbyIsPromotedOnLeave() async throws {
        let stage = MemoryStage()
        let first = try await stage.join(MemoryJoinOptions(displayName: "wheel", exclusiveName: true))
        let second = try await stage.join(MemoryJoinOptions(displayName: "wheel", exclusiveName: true))
        await settle { await first.standby }
        let s1 = await first.standby; let s2 = await second.standby
        XCTAssertTrue(s1); XCTAssertFalse(s2)
        do { try await first.send("wheel.angle", p(1), ack: true); XCTFail("expected Standby") }
        catch let err as AdapterError { XCTAssertEqual(err.code, "Standby") }
        let meStandby = await first.me?.standby
        XCTAssertNil(meStandby) // joined.me 에는 판정이 없다
        let f = await first.me!.participantId; let s = await second.me!.participantId
        XCTAssertEqual(stage.members().map { "\($0.participantId):\($0.standby.map(String.init) ?? "nil")" }, ["\(f):true", "\(s):false"])

        await second.close()
        await settle { await first.standby == false }
        let s1b = await first.standby
        XCTAssertFalse(s1b)
        try await first.send("wheel.angle", p(2), ack: true)
        await settle { stage.members().count == 1 }
        XCTAssertEqual(stage.members().count, 1)
    }

    func testDisconnectQueuesAndReconnectFlushesAndReseeds() async throws {
        let stage = MemoryStage()
        let a = try await stage.join(MemoryJoinOptions(displayName: "a"))
        let b = try await stage.join(MemoryJoinOptions(displayName: "b"))
        let states = Box<[TransportState]>([])
        let stream = await b.events()
        let seenAtConnected = Box<[String]>([])
        let collector = Task {
            for await e in stream {
                if case .stateChange(let s) = e {
                    states.append(s)
                    // 재접속 순서: 히스토리 스냅샷이 connected 보다 먼저 와야 한다 — 그 순간 latest 가 이미 시드돼 있다
                    if s == .connected { seenAtConnected.append((await b.latest("a.out"))?.stringValue ?? "nil") }
                }
            }
        }

        try await stage.disconnect(b)
        await settle { await b.state == .reconnecting }
        let st = await b.state
        XCTAssertEqual(st, .reconnecting)
        let pending = Task { try await b.send("b.out", p("queued"), ack: true) }
        await settle()
        try await a.send("a.out", p("missed"), ack: true)
        await settle()
        let missed = await b.latest("a.out")
        XCTAssertNil(missed)

        try await stage.reconnect(b)
        try await pending.value
        await settle { await b.state == .connected }
        let st2 = await b.state
        XCTAssertEqual(st2, .connected)
        // 수집 태스크는 스트림을 따로 읽는다 — 상태가 connected 인 순간과 수집자가 그 전이를 꺼낸 순간은 다르다. 종전에는 상태만
        // 보고 곧바로 states 를 읽어, 수집자가 밀린 CI 에서 connected 가 빠져 있었다(2026-10-25 CI macos-14; 사본의 수집자에
        // 30 ms 를 끼우면 3/3 재현). 수집자는 connected 를 states 에 넣은 뒤 latest 를 await 하고서야 seenAtConnected 를
        // 채우므로, states 가 둘인 것만으로는 seenAtConnected 가 아직 비어 있을 수 있다 — 마지막에 채워지는 seenAtConnected 를
        // 본다(이것이 채워졌으면 전이 둘도 이미 수집됐다).
        await settle { seenAtConnected.value.count >= 1 }
        let caught = await b.latest("a.out")
        XCTAssertEqual(caught?.stringValue, "missed")
        XCTAssertEqual(stage.published.map { $0.input.channel }, ["a.out", "b.out"])
        XCTAssertEqual(states.value, [.reconnecting, .connected])
        XCTAssertEqual(seenAtConnected.value, ["missed"])
        collector.cancel()
    }

    func testClearHistoryEndReachesDisconnectedAndReconnectAfterEndFails() async throws {
        let stage = MemoryStage()
        let a = try await stage.join(MemoryJoinOptions(displayName: "a"))
        let b = try await stage.join(MemoryJoinOptions(displayName: "b"))
        try await a.send("x.y", p(1), ack: true)
        let c = try await stage.join(MemoryJoinOptions(displayName: "late")) // 스냅샷으로 x.y 를 받는다
        await settle { await c.history().count == 1 }
        let before = await c.history()
        XCTAssertEqual(before.count, 1)
        try await stage.disconnect(b)
        stage.clearHistory()
        await settle { await c.history().isEmpty }
        let after = await c.history()
        XCTAssertTrue(after.isEmpty) // 연결된 쪽은 빈 스냅샷을 받는다
        XCTAssertTrue(stage.history().isEmpty)
        let reasons = Box<[String]>([])
        let sb = await b.events()
        let cb = Task { for await e in sb { if case .ended(let r) = e { reasons.append(r ?? "") } } }
        await settle()
        stage.end("closed")
        await settle { reasons.value.count >= 1 }
        XCTAssertEqual(reasons.value, ["closed"])
        let st = await b.state
        XCTAssertEqual(st, .ended)
        do { try await stage.reconnect(b); XCTFail("expected SessionEnded") }
        catch let err as AdapterError { XCTAssertEqual(err.code, "SessionEnded") }
        cb.cancel()
    }

    func testTwoStagesHaveDistinctIdsAndRejectForeignClients() async throws {
        let s1 = MemoryStage()
        let s2 = MemoryStage()
        let a = try await s1.join(MemoryJoinOptions(displayName: "a"))
        let b = try await s2.join(MemoryJoinOptions(displayName: "b"))
        let aId = await a.me?.participantId; let bId = await b.me?.participantId
        XCTAssertNotEqual(aId, bId)
        do { try await s1.disconnect(b); XCTFail("expected ConfigError") }
        catch let err as AdapterError { XCTAssertEqual(err.code, "ConfigError") }
        let st = await b.state
        XCTAssertEqual(st, .connected)
    }

    func testEndNotifiesEveryoneAndBlocksFurtherPublishes() async throws {
        let stage = MemoryStage()
        let a = try await stage.join(MemoryJoinOptions(displayName: "a"))
        let b = try await stage.join(MemoryJoinOptions(displayName: "b"))
        let reasons = Box<[String]>([])
        let sa = await a.events(); let sb = await b.events()
        let ca = Task { for await e in sa { if case .ended(let r) = e { reasons.append(r ?? "") } } }
        let cb = Task { for await e in sb { if case .ended(let r) = e { reasons.append(r ?? "") } } }
        await settle()
        stage.end("host closed the stage")
        await settle { reasons.value.count >= 2 }
        XCTAssertEqual(reasons.value, ["host closed the stage", "host closed the stage"])
        let st = await a.state
        XCTAssertEqual(st, .ended)
        do { try await a.send("x.y", p(1)); XCTFail("expected SessionEnded") }
        catch let err as AdapterError { XCTAssertEqual(err.code, "SessionEnded") }
        XCTAssertTrue(stage.members().isEmpty)
        XCTAssertThrowsError(try stage.transport())
        ca.cancel(); cb.cancel()
    }

    func testListenersWorksOverTheMemoryRoster() async throws {
        let stage = MemoryStage()
        let a = try await stage.join(MemoryJoinOptions(displayName: "a", sends: ["robot.status"]))
        let b = try await stage.join(MemoryJoinOptions(displayName: "b", receives: ["robot.*"]))
        _ = try await stage.join(MemoryJoinOptions(displayName: "browser-like"))
        let r = try await a.listeners("robot.status")
        let bId = await b.me!.participantId
        XCTAssertEqual(r.declared.map(\.participantId), [bId])
        XCTAssertEqual(r.broadcast.map(\.displayName), ["browser-like"])
        let none = try await a.listeners("nobody.listens")
        XCTAssertTrue(none.declared.isEmpty)
        XCTAssertTrue(a.diagnostics().contains { $0.code == "no-listener" })
    }

    func testInjectActsLikeAPrototype() async throws {
        let stage = MemoryStage()
        let loose = try await stage.join(MemoryJoinOptions(displayName: "loose"))
        let strict = try await stage.join(MemoryJoinOptions(displayName: "strict", receives: ["ui.tap"], strictRouting: true))
        let got = Box<[String]>([])
        _ = await loose.on("ui.*") { m in got.append("loose:\(m.channel):\(m.sourceParticipantId)") }
        _ = await strict.on("ui.*") { m in got.append("strict:\(m.channel)") }
        try stage.inject("ui.tap", p(1))
        try stage.inject("ui.drag", p(0.5))
        await settle { got.value.count >= 3 }
        // 두 클라이언트는 서로 다른 액터라 액터 **간** 배달 순서는 보장되지 않는다 — 집합과
        // 액터별 순서만 단언한다(loose 는 tap 다음 drag).
        XCTAssertEqual(Set(got.value), ["loose:ui.tap:prototype", "strict:ui.tap", "loose:ui.drag:prototype"])
        let looseOnly = got.value.filter { $0.hasPrefix("loose:") }
        XCTAssertEqual(looseOnly, ["loose:ui.tap:prototype", "loose:ui.drag:prototype"])
        XCTAssertEqual(stage.history().map(\.channel), ["ui.tap", "ui.drag"])
    }

    func testRejectPublishInjectsFailures() async throws {
        let stage = MemoryStage(rejectPublish: { input, _ in input.channel == "bad.channel" ? "ValidationError" : nil })
        let a = try await stage.join(MemoryJoinOptions(displayName: "a"))
        do { try await a.send("bad.channel", p(1), ack: true); XCTFail("expected ValidationError") }
        catch let err as AdapterError { XCTAssertEqual(err.code, "ValidationError") }
        try await a.send("good.channel", p(1), ack: true)
        await settle { a.diagnostics().contains { $0.code == "send-dropped" } }
        XCTAssertEqual(stage.published.map { $0.input.channel }, ["good.channel"])
        XCTAssertTrue(a.diagnostics().contains { $0.code == "send-dropped" })
    }

    func testQueuedFireAndForgetRejectionLeavesSendDroppedNote() async throws {
        // 직행 경로와 같은 흔적 — 큐 경유라고 send-dropped 가 빠지면 안 된다(TS/Dart 와 동일).
        let stage = MemoryStage(rejectPublish: { input, _ in input.channel == "bad.channel" ? "ValidationError" : nil })
        let a = try await stage.join(MemoryJoinOptions(displayName: "a"))
        try await stage.disconnect(a)
        let pending = Task { try await a.send("bad.channel", p(1)) } // 끊긴 동안 → 큐
        await settle()
        try await stage.reconnect(a)
        _ = try await pending.value
        await settle { a.diagnostics().contains { $0.code == "send-dropped" } }
        XCTAssertEqual(a.diagnostics().filter { $0.code == "send-dropped" }.map(\.message), ["ValidationError (queued)"])
    }

    func testInjectFromALiveParticipantDoesNotEchoToIt() async throws {
        let stage = MemoryStage()
        let a = try await stage.join(MemoryJoinOptions(displayName: "a"))
        let b = try await stage.join(MemoryJoinOptions(displayName: "b"))
        let seenA = Box<[Primitive]>([])
        let seenB = Box<[Primitive]>([])
        _ = await a.on("x.y") { seenA.append($0.value) }
        _ = await b.on("x.y") { seenB.append($0.value) }
        let me = await a.me!.participantId
        _ = try stage.inject("x.y", p(1), from: me)
        await settle { seenB.value.count >= 1 }
        XCTAssertEqual(seenA.value, [])
        XCTAssertEqual(seenB.value, [p(1)])
    }

    func testJoinOptionsAreValidatedLikeConnect() throws {
        let stage = MemoryStage()
        XCTAssertThrowsError(try stage.transport(MemoryJoinOptions(displayName: ""))) { XCTAssertTrue("\($0)".contains("displayName")) }
        XCTAssertThrowsError(try stage.transport(MemoryJoinOptions(strictRouting: true))) { XCTAssertTrue("\($0)".contains("receives")) }
        XCTAssertThrowsError(try stage.transport(MemoryJoinOptions(receives: ["a.*"], strictRouting: true))) { XCTAssertTrue("\($0)".contains("wildcard")) }
        XCTAssertThrowsError(try stage.transport(MemoryJoinOptions(receives: (0..<33).map { "c.\($0)" }))) { XCTAssertTrue("\($0)".contains("32")) }
        let t = try stage.transport(MemoryJoinOptions(displayName: "x", receives: [" a.b ", "a.b"]))
        XCTAssertEqual(t.member.displayName, "x")
        XCTAssertEqual(t.member.receives, ["a.b"])
        XCTAssertEqual(t.member.clientType, "adapter")
    }

    /// 콜백에서 모으는 값 — 락으로 감싼 상자(@Sendable 클로저에서 안전).
    private final class Box<T>: @unchecked Sendable {
        private let lock = NSLock()
        private var storage: T
        init(_ v: T) { storage = v }
        var value: T { lock.lock(); defer { lock.unlock() }; return storage }
        func append<E>(_ e: E) where T == [E] { lock.lock(); storage.append(e); lock.unlock() }
    }
}
