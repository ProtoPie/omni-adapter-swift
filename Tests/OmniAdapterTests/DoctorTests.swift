import XCTest
@testable import OmniAdapter

/// doctor — Kotlin `DoctorTest.kt` / TS `doctor.test.ts` 미러.
///
/// 잠그는 것:
///   ① 앞 단계가 fail 이면 **뒤 단계를 돌리지 않는다** — 원인 하나를 가리키는 게 목적
///   ② cloud UI base(`/connect`)를 어댑터 base 로 오인한 토큰을 잡는다
///   ③ 화면 지원 프로브는 **일부러 400** 을 내서 좌석을 쓰지 않고 판별한다
///   ④ 구 서버(providesView 무시)는 fail 이 아니라 warn — 데이터 메시징은 그대로 된다
///   ⑤ 자격증명 원문은 리포트에 실리지 않는다(마스킹)
final class DoctorTests: XCTestCase {

    /// 서버 발급 키의 길이(46 = `ck_` + 43) — 짧으면 자격증명 검사가 "잘린 키" warn 을 낸다(아래 시험).
    private let fixtureKey = "ck_sekret-token-value-0123456789012345678-alue"

    private func token(url: String = "http://127.0.0.1:9981", secretKey: String? = nil) -> String {
        // encode 는 입력 검증에서 던질 수 있다 — 테스트 값은 항상 유효하므로 강제 언랩.
        try! StageToken.encode(url: url, stageId: "st_1", secretKey: secretKey ?? fixtureKey)
    }

    private func http(
        about: DoctorHttpResponse = DoctorHttpResponse(
            status: 200,
            body:
                #"{"runMode":"local","version":"1.2.3","protocolVersion":"0.1.0","iceServers":[]}"#),
        probe: DoctorHttpResponse = DoctorHttpResponse(
            status: 400, body: #"{"details":[{"path":["providesView"]}]}"#),
        validate: DoctorHttpResponse = DoctorHttpResponse(
            status: 200, body: #"{"jwt":"j","participantId":"p"}"#),
        // 무인증 존재 프로브 — 기본 401(표면 있음).
        registrations: DoctorHttpResponse = DoctorHttpResponse(
            status: 401, body: #"{"error":"unauthorized"}"#),
        seen: URLBox? = nil
    ) -> DoctorHttp {
        { url, body in
            seen?.add(url)
            if url.hasSuffix("/api/about") { return about }
            if url.contains("/adapter-registrations") { return registrations }
            if let body, body.contains("__omni_doctor_probe__") { return probe }
            return validate
        }
    }

    /// 401 이면 ok(표면 있음), 404 면 warn(보기 전용).
    func testRemoteInputSurfaceOkAndWarn() async throws {
        let withSurface = await runDoctor(
            credential: token(), http: http(), connectImpl: failingConnect())
        XCTAssertEqual(withSurface.first { $0.id == "remote-input" }?.status, .ok)

        let without = await runDoctor(
            credential: token(),
            http: http(registrations: DoctorHttpResponse(status: 404, body: "{}")),
            connectImpl: failingConnect())
        let check = without.first { $0.id == "remote-input" }
        XCTAssertEqual(check?.status, .warn)
        XCTAssertTrue(check?.detail?.contains("view-only") == true)
        XCTAssertTrue(check?.hint?.contains("Remote input runs on standalone servers only — other servers never have it.") == true)
    }

    private func failingConnect() -> DoctorConnect {
        { _ in throw AdapterError("RoomEnded", "no session") }
    }

    func testStopsAtBadCredential() async {
        let seen = URLBox()
        let results = await runDoctor(credential: "nonsense", http: http(seen: seen))
        XCTAssertEqual(results.count, 1, "뒤 단계를 돌리지 않는다")
        XCTAssertEqual(results[0].id, "credential")
        XCTAssertEqual(results[0].status, .fail)
        XCTAssertTrue(seen.all.isEmpty, "네트워크를 치지 않는다")
    }

    func testCatchesWebUiBaseMistake() async {
        let results = await runDoctor(
            credential: token(url: "https://ee.example.com/connect"), http: http())
        let shape = results.first { $0.id == "url-shape" }!
        XCTAssertEqual(shape.status, .fail)
        XCTAssertTrue(shape.hint!.contains("/api/connect"), "고칠 값을 알려줘야 한다")
        XCTAssertEqual(results.count, 2, "url 이 틀리면 서버를 치지 않는다")
    }

    func testServerProtocolFloorFails() async {
        let results = await runDoctor(
            credential: token(),
            http: http(
                about: DoctorHttpResponse(
                    status: 200,
                    body: #"{"minClientProtocol":"9.0.0","version":"1","protocolVersion":"9.0.0"}"#))
        )
        let server = results.first { $0.id == "server" }!
        XCTAssertEqual(server.status, .fail)
        XCTAssertEqual(results.count, 3, "서버가 안 맞으면 뒤를 돌리지 않는다")
    }

    func testScreenProbeOk() async {
        let results = await runDoctor(
            credential: token(), http: http(), connectImpl: failingConnect())
        XCTAssertEqual(results.first { $0.id == "screen" }!.status, .ok)
    }

    func testOldServerIgnoringProvidesViewIsWarnNotFail() async {
        let results = await runDoctor(
            credential: token(),
            http: http(
                probe: DoctorHttpResponse(
                    status: 400, body: #"{"details":[{"path":["displayName"]}]}"#)),
            connectImpl: failingConnect())
        XCTAssertEqual(results.first { $0.id == "screen" }!.status, .warn)
        XCTAssertTrue(results.contains { $0.id == "stage" }, "warn 은 뒤 단계를 멈추지 않는다")
    }

    func testProbeNot400IsSkip() async {
        let results = await runDoctor(
            credential: token(),
            http: http(probe: DoctorHttpResponse(status: 500, body: "boom")),
            connectImpl: failingConnect())
        XCTAssertEqual(
            results.first { $0.id == "screen" }!.status, .skip, "모르는 것을 아는 척하지 않는다")
    }

    /// 세션 확인 validate 는 진짜 키 입장이다(서버가 멤버를 만든다) — 트랜스포트처럼 신원 네 필드를 싣는다.
    func testTheSessionCheckCarriesTheAdapterIdentity() async throws {
        let bodies = URLBox()
        let inner = http(validate: DoctorHttpResponse(status: 404, body: ""))
        let capturing: DoctorHttp = { url, body in
            if url.contains("/token/validate"), let body, !body.contains("__omni_doctor_probe__") { bodies.add(body) }
            return try await inner(url, body)
        }
        _ = await runDoctor(credential: token(), http: capturing)
        XCTAssertEqual(bodies.all.count, 1)
        let json = try JSONSerialization.jsonObject(with: Data((bodies.all.first ?? "{}").utf8)) as? [String: Any] ?? [:]
        XCTAssertEqual(json["adapter"] as? String, "swift")
        for key in ["adapterVersion", "runtime", "runtimeVersion"] {
            XCTAssertNotNil(json[key], "\(key) should be present")
        }
    }

    func testNoLiveSessionExplains404() async {
        let results = await runDoctor(
            credential: token(),
            http: http(validate: DoctorHttpResponse(status: 404, body: "")))
        let stage = results.first { $0.id == "stage" }!
        XCTAssertEqual(stage.status, .fail)
        XCTAssertTrue(stage.hint!.contains("Open the stage in Connect first"))
        XCTAssertFalse(results.contains { $0.id == "join" }, "세션이 없으면 접속을 시도하지 않는다")
    }

    func testShowsScreenAdapterNames() async {
        let transport = RosterTransport(members: [
            Member(participantId: "me-1", displayName: "doctor"),
            Member(participantId: "s-1", displayName: "unity-screen", providesView: "stream"),
            Member(participantId: "s-2", displayName: "data-only"),
        ])
        let results = await runDoctor(
            credential: token(), http: http(),
            connectImpl: { _ in
                let c = ConnectClient(transport: transport)
                try await c.open()
                return c
            })
        XCTAssertEqual(results.first { $0.id == "join" }!.status, .ok)
        let screens = results.first { $0.id == "screen-adapters" }!
        XCTAssertEqual(screens.status, .ok)
        XCTAssertTrue(screens.detail!.contains("unity-screen (stream)"))
        XCTAssertFalse(screens.detail!.contains("data-only"), "화면 미신고는 목록에 없다")
    }

    func testDropsScreenAdaptersLineWhenServerDoesNotKnowScreens() async {
        let transport = RosterTransport(members: [
            Member(participantId: "me-1", displayName: "doctor")
        ])
        let results = await runDoctor(
            credential: token(),
            http: http(
                probe: DoctorHttpResponse(
                    status: 400, body: #"{"details":[{"path":["displayName"]}]}"#)),
            connectImpl: { _ in
                let c = ConnectClient(transport: transport)
                try await c.open()
                return c
            })
        XCTAssertFalse(
            results.contains { $0.id == "screen-adapters" }, "원인은 screen 줄이 이미 말했다")
    }

    func testReportMasksTheCredential() async {
        let results = await runDoctor(
            credential: token(), http: http(), connectImpl: failingConnect())
        let report = formatDoctorReport(results)
        XCTAssertFalse(report.contains(fixtureKey), "토큰 원문 노출 금지")
        XCTAssertTrue(report.contains("ck_s…alue"), "마스킹된 형태로만 보인다")
        XCTAssertTrue(report.contains("not ready"), "join 이 실패했으므로 not ready")
    }

    func testFixtureKeyHasTheServerIssuedLength() {
        XCTAssertEqual(fixtureKey.count, Protocol.adapterKeyLength)
        XCTAssertEqual(fixtureKey.count, 46)
    }

    /// validate 503 KeyMaterialUnavailable — warn(기다릴 일)이고 힌트가 서버의 retryAfterMs 와 처방을 말한다 · join 까지 간다.
    func testValidate503KeyMaterialUnavailableIsWarnAndContinuesToJoin() async {
        let transport = RosterTransport(members: [Member(participantId: "me-1", displayName: "doctor")])
        let results = await runDoctor(
            credential: token(),
            http: http(
                validate: DoctorHttpResponse(
                    status: 503, body: #"{"error":"KeyMaterialUnavailable","details":{"retryAfterMs":12000}}"#)),
            connectImpl: { _ in
                let c = ConnectClient(transport: transport)
                try await c.open()
                return c
            })
        let stage = results.first { $0.id == "stage" }!
        XCTAssertEqual(stage.status, .warn)
        XCTAssertTrue(stage.detail?.contains("retry in 12 s") ?? false, stage.detail ?? "nil")
        XCTAssertTrue(stage.hint?.contains("Retry in 12 s") ?? false, stage.hint ?? "nil")
        XCTAssertTrue(stage.hint?.contains("revoke the stage's old keys") ?? false, stage.hint ?? "nil")
        XCTAssertEqual(results.first { $0.id == "join" }?.status, .ok, "warn 은 join 까지 간다")
    }

    /// about.capabilities.adapterGuest == false — 어댑터를 받지 않는 서버: server 에서 fail 하고 stage 로 가지 않는다.
    func testAdapterGuestFalseFailsAtServerAndSkipsTheStage() async {
        let results = await runDoctor(
            credential: token(),
            http: http(
                about: DoctorHttpResponse(
                    status: 200,
                    body:
                        #"{"protocolVersion":"0.1.0","minClientProtocol":null,"version":"0.1.8","runMode":"local","capabilities":{"adapterGuest":false,"localOnlyPies":false}}"#
                )))
        let server = results.first { $0.id == "server" }!
        XCTAssertEqual(server.status, .fail)
        XCTAssertEqual(
            server.detail, "local v0.1.8 — this server does not take adapters (capabilities.adapterGuest is false)")
        XCTAssertTrue(server.hint?.contains("Connect Omni") ?? false, server.hint ?? "nil")
        XCTAssertFalse(results.contains { $0.id == "stage" }, "server 가 fail 이면 stage 로 가지 않는다")
        // 플래그가 참이거나 없는(옛) 서버 · 불리언이 아닌 0 은 종전대로 통과한다.
        for capabilities in [#""capabilities":{"adapterGuest":true}"#, #""capabilities":{"adapterGuest":0}"#, #""capabilities":null"#, #""x":1"#] {
            let ok = await runDoctor(
                credential: token(),
                http: http(
                    about: DoctorHttpResponse(
                        status: 200,
                        body: #"{"protocolVersion":"0.1.0","minClientProtocol":null,"version":"0.1.8","runMode":"cloud","# + capabilities + "}")),
                connectImpl: failingConnect())
            XCTAssertEqual(ok.first { $0.id == "server" }?.status, .ok, capabilities)
        }
    }

    /// 키 길이가 46 이 아닌 cst_ 토큰 — credential 은 warn(잘린 키)이고 검사는 계속된다(모의 서버는 짧은 키를 받는다).
    func testAShortKeyWarnsAtCredentialAndContinues() async {
        let results = await runDoctor(
            credential: token(secretKey: "ck_short"), http: http(), connectImpl: failingConnect())
        let credential = results.first { $0.id == "credential" }!
        XCTAssertEqual(credential.status, .warn)
        XCTAssertTrue(credential.detail?.hasPrefix("cst_ token → url=http://127.0.0.1:9981  stage=st_1  key=****") ?? false, credential.detail ?? "nil")
        XCTAssertTrue(credential.detail?.hasSuffix(" — the key has 8 characters, a server-issued key has 46") ?? false, credential.detail ?? "nil")
        XCTAssertEqual(
            credential.hint,
            "The token looks truncated or altered — copy it again (Stage → Share → Adapter keys; Copy appears right after Create key), or create a new key.")
        XCTAssertTrue(results.contains { $0.id == "server" }, "warn 은 뒤 단계를 멈추지 않는다")
        XCTAssertTrue(results.contains { $0.id == "stage" })
        let full = await runDoctor(credential: token(), http: http(), connectImpl: failingConnect())
        XCTAssertEqual(full.first { $0.id == "credential" }?.status, .ok)
        XCTAssertNil(full.first { $0.id == "credential" }?.hint)
    }

    /// 짧은 키의 401 — stage 단계도 connect() 와 같은 판정(adapterKeyLengthRejection)으로 "회수됨" 대신 길이를 말한다.
    func testAShortKeys401AtTheStageStepSaysTruncatedNotRevoked() async {
        let results = await runDoctor(
            credential: token(secretKey: "ck_short"),
            http: http(validate: DoctorHttpResponse(status: 401, body: #"{"error":"PinNotFound"}"#)))
        let stage = results.first { $0.id == "stage" }!
        XCTAssertEqual(stage.status, .fail)
        XCTAssertTrue(stage.detail?.contains("the key has 8 characters, a server-issued key has 46") ?? false, stage.detail ?? "nil")
        XCTAssertTrue(stage.hint?.contains("looks truncated or altered") ?? false, stage.hint ?? "nil")
        // 46자 키의 401 은 종전 그대로 "회수됨".
        let full = await runDoctor(
            credential: token(), http: http(validate: DoctorHttpResponse(status: 401, body: #"{"error":"PinNotFound"}"#)))
        let revoked = full.first { $0.id == "stage" }!
        XCTAssertEqual(revoked.status, .fail)
        XCTAssertFalse(revoked.detail?.contains("characters") ?? true, revoked.detail ?? "nil")
        XCTAssertEqual(revoked.hint, SocketTransport.adapterKeyRevokedHint)
    }
}

/// roster 를 통제하는 최소 트랜스포트.
final class RosterTransport: Transport, @unchecked Sendable {
    private let members: [Member]
    init(members: [Member]) { self.members = members }

    func open(handlers: TransportHandlers) async throws -> Joined {
        Joined(roomId: "stage-1", me: members[0])
    }
    func publish(_ input: PublishInput, ack: Bool) async -> PublishResult { PublishResult(ok: true) }
    func listMembers() async throws -> [Member] { members }
    func close() async {}
}

final class URLBox: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [String] = []
    var all: [String] { lock.withLock { items } }
    func add(_ v: String) { lock.withLock { items.append(v) } }
}
