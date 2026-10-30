import XCTest
@testable import OmniAdapter

/// 프로토콜 드리프트 가드 — 같은 repo 의 TS 어댑터 `protocol.ts` 와 상수 대조.
/// TS 어댑터가 connect-v2 원본(`@ppc/protocol`)과의 드리프트를 감시하므로, 여기서
/// TS 어댑터와 잠그면 이행적으로 서버 계약과 잠긴다. TS 소스가 없으면(독립 배포
/// 체크아웃) 검사를 건너뛴다.
///
/// ⚠️ "못 찾음"에는 두 종류가 있다(Dart `protocol_drift_test.dart` 와 같은 판정): 형제 `typescript/` 자체가
/// 없으면 독립 체크아웃이라 건너뛰는 것이 맞지만, `typescript/` 는 있는데 기준본 파일이 없으면 옮겨졌거나 이름이
/// 바뀐 것이다 — 그때 조용히 건너뛰면 가드 전체가 무력해지므로 `testReferenceFoundOrStandaloneCheckout` 이 실패한다.
/// CI 는 `OMNI_CONFORMANCE_REQUIRED=1` 을 켠다 — 그때는 독립 체크아웃도 실패다(`typescript/` 이름 자체가 바뀐
/// 경우까지 잡는다).
final class ProtocolDriftTests: XCTestCase {
    enum ReferenceLookup { case found, standalone, moved }

    /// CI 가 켠다 — 기준본을 못 찾으면 독립 체크아웃이어도 건너뛰지 않고 실패한다.
    private let required = ProcessInfo.processInfo.environment["OMNI_CONFORMANCE_REQUIRED"] == "1"

    private let start = URL(fileURLWithPath: #filePath).deletingLastPathComponent()

    /// `start` 에서 위로 올라가며 `typescript/` 를 처음 가진 곳에서 기준본 `name` 을 본다. 시험이 임시 디렉터리로
    /// 세 경우를 만들 수 있게 시작점을 받는다. `standalone` 이면 가리킬 파일이 없어 `file` 은 nil 이다.
    ///
    /// 올라가는 범위는 `Package.swift` 가 있는 디렉터리(패키지 루트)의 **바로 위까지**다 — 저장소에서는 그 위가 저장소
    /// 루트이고 형제 `typescript/` 가 거기 있다. 끝까지(`/`) 올라가던 판은 미러 체크아웃이 `~/typescript/omni-adapter-swift`
    /// 처럼 이름이 `typescript` 인 디렉터리 아래에 있으면 그 위(`~`)에서 `typescript/` 를 찾아 `moved` 로 오판했다.
    /// `Package.swift` 를 못 찾으면(시험의 임시 디렉터리) `start` 만 본다.
    static func locateReference(from start: URL, _ name: String) -> (status: ReferenceLookup, file: URL?) {
        let fm = FileManager.default
        let ceiling = packageRoot(from: start)?.deletingLastPathComponent() ?? start
        var dir = start
        while true {
            let ts = dir.appendingPathComponent("typescript")
            var isDir: ObjCBool = false
            if fm.fileExists(atPath: ts.path, isDirectory: &isDir), isDir.boolValue {
                let file = ts.appendingPathComponent("src/\(name)")
                // 형제 패키지는 있는데 기준본만 없다 — 옮겨졌다.
                return (fm.fileExists(atPath: file.path) ? .found : .moved, file)
            }
            if dir.path == ceiling.path || dir.path == "/" { break }
            dir = dir.deletingLastPathComponent()
        }
        return (.standalone, nil)
    }

    /// `start` 와 그 조상 가운데 `Package.swift` 를 가진 가장 가까운 디렉터리.
    private static func packageRoot(from start: URL) -> URL? {
        var dir = start
        while true {
            if FileManager.default.fileExists(atPath: dir.appendingPathComponent("Package.swift").path) { return dir }
            if dir.path == "/" { return nil }
            dir = dir.deletingLastPathComponent()
        }
    }

    /// 기준본 내용. 못 찾으면 **건너뜀**으로 보고한다 — 왜 못 찾았는지는 `testReferenceFoundOrStandaloneCheckout` 이 판정한다.
    private func reference(_ name: String) throws -> String {
        let (status, file) = Self.locateReference(from: start, name)
        guard status == .found, let file else { throw XCTSkip("typescript/src/\(name) 없음 (\(status))") }
        return try String(contentsOf: file, encoding: .utf8)
    }

    private func tsSource() throws -> String { try reference("protocol.ts") }

    /// 이 시험이 이 파일에서 가장 중요하다: 나머지는 기준본을 못 찾으면 건너뛰므로, 기준본이 사라진 것을 여기서
    /// 잡지 않으면 가드 전체가 조용히 무력해진다. 대조하는 기준본마다 따로 판정한다.
    func testReferenceFoundOrStandaloneCheckout() {
        for name in ["protocol.ts", "remote-input-protocol.ts", "socket-transport.ts"] {
            let (status, file) = Self.locateReference(from: start, name)
            XCTAssertNotEqual(
                status, .moved,
                "typescript/ 는 있는데 \(file?.path ?? name) 없음 — 기준본이 옮겨졌거나 이름이 바뀌었으니 이 시험이 읽는 경로를 새 위치로 고칠 것"
            )
            XCTAssertFalse(
                required && status == .standalone,
                "\(start.path) 위로 typescript/ 디렉터리 없음 — OMNI_CONFORMANCE_REQUIRED=1 에서는 독립 체크아웃으로 보지 않으니, "
                    + "typescript/ 이름이 바뀌었다면 이 시험이 읽는 경로를 고칠 것"
            )
        }
    }

    /// 탐색 판정 자체 — typescript/ 가 없으면 standalone, 있는데 파일이 없으면 moved.
    func testLocateReferenceTellsStandaloneFromMoved() throws {
        let fm = FileManager.default
        let tmp = fm.temporaryDirectory.appendingPathComponent("omni-ref-\(UUID().uuidString)")
        try fm.createDirectory(at: tmp, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: tmp) }
        XCTAssertEqual(Self.locateReference(from: tmp, "protocol.ts").status, .standalone)
        try fm.createDirectory(at: tmp.appendingPathComponent("typescript/src"), withIntermediateDirectories: true)
        XCTAssertEqual(Self.locateReference(from: tmp, "protocol.ts").status, .moved)
        try Data("marker".utf8).write(to: tmp.appendingPathComponent("typescript/src/protocol.ts"))
        XCTAssertEqual(Self.locateReference(from: tmp, "protocol.ts").status, .found)
        // 기준본마다 따로 판정한다 — 하나만 옮겨져도 잡는다.
        XCTAssertEqual(Self.locateReference(from: tmp, "remote-input-protocol.ts").status, .moved)
    }

    /// 탐색 범위 — 패키지 루트의 바로 위까지만 본다. 저장소 배치(`<repo>/swift/Package.swift` + `<repo>/typescript/`)는
    /// 찾고, 미러 체크아웃이 이름이 `typescript` 인 디렉터리 아래에 있어도(`<x>/typescript/omni-adapter-swift/`) 그 위의
    /// `typescript/` 를 형제로 착각하지 않는다.
    func testLocateReferenceStopsAboveThePackageRoot() throws {
        let fm = FileManager.default
        let tmp = fm.temporaryDirectory.appendingPathComponent("omni-ref-\(UUID().uuidString)")
        defer { try? fm.removeItem(at: tmp) }
        func package(_ root: URL) throws -> URL {
            let tests = root.appendingPathComponent("Tests/OmniAdapterTests")
            try fm.createDirectory(at: tests, withIntermediateDirectories: true)
            try Data("// swift-tools-version:5.9".utf8).write(to: root.appendingPathComponent("Package.swift"))
            return tests
        }

        // 저장소 배치 — 형제 typescript/ 를 찾는다.
        let repo = tmp.appendingPathComponent("repo")
        let repoTests = try package(repo.appendingPathComponent("swift"))
        try fm.createDirectory(at: repo.appendingPathComponent("typescript/src"), withIntermediateDirectories: true)
        try Data("marker".utf8).write(to: repo.appendingPathComponent("typescript/src/protocol.ts"))
        XCTAssertEqual(Self.locateReference(from: repoTests, "protocol.ts").status, .found)

        // 미러 배치 — <tmp>/typescript/omni-adapter-swift/. <tmp> 에 typescript/ 가 있지만 패키지 루트의 바로 위
        // (<tmp>/typescript) 보다 위라서 보지 않는다.
        let mirrorTests = try package(tmp.appendingPathComponent("typescript/omni-adapter-swift"))
        XCTAssertEqual(Self.locateReference(from: mirrorTests, "protocol.ts").status, .standalone)
    }

    func testProtocolVersionLockstep() throws {
        let src = try tsSource()
        XCTAssertTrue(src.contains("PROTOCOL_VERSION = '\(Protocol.protocolVersion)'"))
    }

    func testErrorCodeConstantsLockstep() throws {
        let src = try tsSource()
        XCTAssertTrue(src.contains("'\(Protocol.protocolVersionMismatch)'"))
        XCTAssertTrue(src.contains("'\(Protocol.appVersionAheadOfServer)'"))
    }

    /// ⚠️ "값이 어딘가 있다"가 아니라 **키:값 쌍**으로 대조한다. 값만 보면 두 이벤트의
    /// 값이 서로 뒤바뀌어도(joined↔memberAdded) 통과한다(적대검수 지적).
    func testEventNamePairsLockstep() throws {
        let src = try tsSource()
        let pairs: [(String, String)] = [
            ("JoinRoom", Protocol.ClientEvents.joinRoom),
            ("Leave", Protocol.ClientEvents.leave),
            ("Message", Protocol.ClientEvents.message),
            ("ListMembers", Protocol.ClientEvents.listMembers),
            ("Joined", Protocol.ServerEvents.joined),
            ("MemberAdded", Protocol.ServerEvents.memberAdded),
            ("MemberRemoved", Protocol.ServerEvents.memberRemoved),
            ("History", Protocol.ServerEvents.history),
            ("Ended", Protocol.ServerEvents.ended),
            ("Error", Protocol.ServerEvents.error),
            ("Signaling", Protocol.ServerEvents.signaling),
            ("AdapterRegisterSubmit", Protocol.ClientEvents.adapterRegisterSubmit),
            ("AdapterRegisterResume", Protocol.ClientEvents.adapterRegisterResume),
            ("AdapterLeaseHolder", Protocol.ServerEvents.adapterLeaseHolder),
        ]
        for (key, value) in pairs {
            XCTAssertTrue(
                src.contains("\(key): '\(value)'"),
                "pair \(key): '\(value)' missing from TS protocol.ts"
            )
        }
    }

    /// 어댑터 키 길이 · 429/503 봉투의 retryAfterMs 이름 · about 필드 · 능력 플래그 — TS protocol.ts 와 대조(connect-v2 3464e4c4f · 8fea9402d).
    func testAdapterKeyLengthRetryAfterAndAboutFieldsLockstep() throws {
        let src = try tsSource()
        XCTAssertTrue(src.contains("ADAPTER_KEY_LENGTH = \(Protocol.adapterKeyLength);"))
        XCTAssertTrue(src.contains("ERROR_DETAILS_RETRY_AFTER_MS = '\(Protocol.errorDetailsRetryAfterMs)';"))
        // 목록은 순서까지 같다 — 항목 하나만 대조하면 TS 가 더한 필드를 놓친다.
        let aboutFields = "ABOUT_FIELDS = [" + Protocol.aboutFields.map { "'\($0)'" }.joined(separator: ", ") + "] as const;"
        XCTAssertTrue(src.contains(aboutFields), "protocol.ts 의 ABOUT_FIELDS 가 \(Protocol.aboutFields) 와 다르다")
        let capabilityFields =
            "ABOUT_CAPABILITY_FIELDS = [" + Protocol.aboutCapabilityFields.map { "'\($0)'" }.joined(separator: ", ") + "] as const;"
        XCTAssertTrue(src.contains(capabilityFields), "protocol.ts 의 ABOUT_CAPABILITY_FIELDS 가 \(Protocol.aboutCapabilityFields) 와 다르다")
        XCTAssertTrue(Protocol.aboutFields.contains("capabilities"))
        XCTAssertEqual(Protocol.aboutCapabilityFields, ["adapterGuest"])
        XCTAssertEqual(Protocol.adapterKeyLength, "ck_".count + Int((Double(32) * 4 / 3).rounded(.up)))
    }

    /// 여섯 언어 같은 문장 — TS 기준본 socket-transport.ts 의 문자열 리터럴과 바이트까지 같다(Python · Dart 와 같은 방식: 따옴표 안의
    /// 리터럴이 그대로 들어 있는지). 템플릿으로 조립하는 문장은 템플릿 조각을 대조하고 같은 값을 넣어 비교한다.
    func testHintSentencesAreByteEqualToTheTSReference() throws {
        let src = try reference("socket-transport.ts")
        XCTAssertTrue(
            src.contains("VALIDATE_RATE_LIMITED_HINT =\n  '\(SocketTransport.validateRateLimitedHint)';"),
            "429 힌트가 TS 와 다르다")
        XCTAssertTrue(
            src.contains("KEY_MATERIAL_UNAVAILABLE_HINT =\n  \"\(SocketTransport.keyMaterialUnavailableHint)\";"),
            "503 KeyMaterialUnavailable 힌트가 TS 와 다르다")
        XCTAssertEqual(AdapterError("KeyMaterialUnavailable").hint, SocketTransport.keyMaterialUnavailableHint)
        XCTAssertTrue(src.contains("ADAPTER_KEY_REVOKED_HINT = '\(SocketTransport.adapterKeyRevokedHint)';"))
        // 잘린 키 힌트 — TS 는 템플릿. 조각이 그대로 있는지 보고, 같은 값을 넣어 우리 문장과 비교한다.
        let hintTemplate =
            "${ADAPTER_KEY_REVOKED_HINT} This key has ${length} characters, but a server-issued key is ${ADAPTER_KEY_LENGTH} (ck_ plus 43) — it looks truncated or altered; copy it again (Copy appears right after Create key), or create a new key."
        XCTAssertTrue(src.contains("`\(hintTemplate)`"), "TS 의 잘린 키 힌트 템플릿이 바뀌었다")
        let expectedHint = hintTemplate
            .replacingOccurrences(of: "${ADAPTER_KEY_REVOKED_HINT}", with: SocketTransport.adapterKeyRevokedHint)
            .replacingOccurrences(of: "${length}", with: "10")
            .replacingOccurrences(of: "${ADAPTER_KEY_LENGTH}", with: "\(Protocol.adapterKeyLength)")
        XCTAssertEqual(SocketTransport.adapterKeyLengthHint(10), expectedHint)
        // 잘린 키 메시지 꼬리(adapterKeyLengthRejection) — 역시 템플릿.
        let tail = "${mapped.message}; the key has ${keyLength} characters, a server-issued key has ${ADAPTER_KEY_LENGTH}"
        XCTAssertTrue(src.contains("`\(tail)`"), "TS 의 잘린 키 메시지 템플릿이 바뀌었다")
        let base = SocketTransport.mapValidateError(status: 401, code: "PinNotFound", base: "http://h")
        let expectedMessage = tail
            .replacingOccurrences(of: "${mapped.message}", with: base.message)
            .replacingOccurrences(of: "${keyLength}", with: "8")
            .replacingOccurrences(of: "${ADAPTER_KEY_LENGTH}", with: "\(Protocol.adapterKeyLength)")
        XCTAssertEqual(SocketTransport.adapterKeyLengthRejection(base, status: 401, keyLength: 8).message, expectedMessage)
        XCTAssertTrue(src.contains("RETRY_AFTER_MAX_MS = 600_000;"))
        XCTAssertEqual(SocketTransport.retryAfterMaxMs, 600_000)
    }

    /// coc.input P2P 계약(§7) — TS 기준본 remote-input-protocol.ts 와 대조.
    func testRemoteInputContractLockstep() throws {
        let src = try reference("remote-input-protocol.ts")
        XCTAssertTrue(src.contains("REMOTE_INPUT_CHANNEL = '\(Protocol.RemoteInput.channel)'"))
        XCTAssertTrue(src.contains("REMOTE_INPUT_PROTOCOL_VERSION = \(Protocol.RemoteInput.version)"))
        XCTAssertTrue(src.contains("REMOTE_INPUT_MOVE_HZ = \(Protocol.RemoteInput.moveHz)"))
        for t in Protocol.RemoteInput.adapterAccepts {
            XCTAssertTrue(src.contains("'\(t)'"), "adapter-accept \(t) missing")
        }
    }

    func testAppAheadMessageMarkerLockstep() throws {
        let src = try tsSource()
        // 서버 산문에 결합된 분류 — TS 쪽 가드가 서버 handshake.ts 와 대조한다.
        XCTAssertTrue(src.contains("APP_AHEAD_MESSAGE_MARKER = '\(Protocol.appAheadMessageMarker)'"))
    }

    func testClientTypeAndProvidesViewsLockstep() throws {
        let src = try tsSource()
        XCTAssertTrue(src.contains("ADAPTER_CLIENT_TYPE = '\(Protocol.adapterClientType)'"))
        // providesView 열거도 기준본에 고정(C#/Dart 는 이미 함) — 화면 갈래 선별 필드.
        for view in Protocol.adapterProvidesViews {
            XCTAssertTrue(src.contains("'\(view)'"), "providesView \(view) missing from TS protocol.ts")
        }
    }

    /// 어댑터 신원(ADR-0016 · D4) — 이 언어가 보내는 언어 · 런타임 값이 TS 어휘(서버와 대조된다) 안이고, 두 판 패턴이
    /// TS 와 같으며, 필드 이름 넷이 TS 필드 목록에 있다. 어휘 밖의 값은 서버가 받지 않는다.
    func testAdapterIdentityLockstep() throws {
        let src = try tsSource()
        XCTAssertTrue(try Self.tsStringList(src, "ADAPTER_LANGS").contains(AdapterIdentity.lang))
        XCTAssertTrue(try Self.tsStringList(src, "ADAPTER_RUNTIMES").contains(AdapterIdentity.runtime))
        XCTAssertTrue(src.contains("ADAPTER_VERSION_PATTERN = '\(Protocol.adapterVersionPattern)'"))
        XCTAssertTrue(src.contains("RUNTIME_VERSION_PATTERN = '\(Protocol.runtimeVersionPattern)'"))
        let fields = try Self.tsStringList(src, "VALIDATE_TOKEN_BODY_FIELDS")
        for key in ["adapter", "adapterVersion", "runtime", "runtimeVersion"] {
            XCTAssertTrue(fields.contains(key), "\(key) missing from TS VALIDATE_TOKEN_BODY_FIELDS")
        }
    }

    /// TS `export const NAME = [ ... ] as const` 의 문자열 값들(순서째).
    static func tsStringList(_ src: String, _ name: String) throws -> [String] {
        let list = try NSRegularExpression(
            pattern: "export const \(name) = \\[(.*?)\\] as const\\b", options: [.dotMatchesLineSeparators])
        let whole = NSRange(src.startIndex..., in: src)
        guard let m = list.firstMatch(in: src, range: whole), let body = Range(m.range(at: 1), in: src) else {
            XCTFail("\(name) 를 TS protocol.ts 에서 찾지 못했다")
            return []
        }
        let inner = String(src[body])
        let quoted = try NSRegularExpression(pattern: "'([^']*)'")
        return quoted.matches(in: inner, range: NSRange(inner.startIndex..., in: inner)).compactMap {
            Range($0.range(at: 1), in: inner).map { String(inner[$0]) }
        }
    }
}

/// 트랜스포트의 순수부 — 프레임 파싱과 핸드셰이크 거절 분류.
final class SocketTransportUnitTests: XCTestCase {
    func testToStageMessageAcceptsPrimitivesOnly() {
        let text = SocketTransport.toStageMessage([
            "channel": "a.b", "value": "x", "timestamp": 5, "sourceParticipantId": "p1",
        ])
        XCTAssertEqual(text?.value, .string("x"))
        XCTAssertEqual(text?.timestamp, 5)
        XCTAssertEqual(text?.sourceParticipantId, "p1")

        XCTAssertEqual(
            SocketTransport.toStageMessage(["channel": "a.b", "value": 23.5])?.value,
            .double(23.5)
        )
        XCTAssertEqual(
            SocketTransport.toStageMessage(["channel": "a.b", "value": 7])?.value,
            .int(7)
        )
        XCTAssertEqual(
            SocketTransport.toStageMessage(["channel": "a.b", "value": true])?.value,
            .bool(true)
        )
        XCTAssertEqual(
            SocketTransport.toStageMessage(["channel": "a.b", "value": NSNull()])?.value,
            .null
        )

        // 객체/배열은 와이어 금지 — 프레임을 버린다
        XCTAssertNil(SocketTransport.toStageMessage(["channel": "a.b", "value": ["x": 1]]))
        // channel 없는 프레임도 버린다
        XCTAssertNil(SocketTransport.toStageMessage(["value": 1]))
    }

    /// roster 의 standby — 서버가 판정한 활성/대기 (T4). nil 과 false 는 다르다.
    func testToMemberCarriesStandby() {
        XCTAssertEqual(
            SocketTransport.toMember(
                ["participantId": "p1", "standby": true], fallbackId: "f", fallbackName: "f"
            ).standby,
            true
        )
        XCTAssertEqual(
            SocketTransport.toMember(
                ["participantId": "p2", "standby": false], fallbackId: "f", fallbackName: "f"
            ).standby,
            false
        )
        // 구 서버(부재) — nil. false 로 접으면 "규칙 없음"과 "지금 활성"이 구별되지 않는다.
        XCTAssertNil(
            SocketTransport.toMember(
                ["participantId": "p3"], fallbackId: "f", fallbackName: "f"
            ).standby
        )
    }

    /// T5-3 세션 재개 자동 복귀 — 호스트가 스테이지를 닫아도 어댑터가 죽지 않는다.
    ///
    /// 핵심은 **`.ended` 를 알리지 않는 것**이다: 클라이언트는 `.ended` 를 1회성 래치로
    /// 다루므로(큐를 비우고 상태를 고정) 한 번 알리면 뒤에 복귀해도 아무것도 흐르지 않는다.
    /// 여기서는 재접속 루프를 끄고(reconnection=false) 분기만 고정한다.
    func testSessionEndHoldsForRejoinInsteadOfEnding() {
        let (transport, states, ended) = makeTransport(autoRejoin: true)
        transport.handleSessionEnd("host-ended")

        XCTAssertTrue(ended.values.isEmpty, "세션 종료를 종착점으로 알리면 안 된다")
        XCTAssertEqual(states.values.last, .reconnecting)
    }

    func testAutoRejoinOffKeepsTheOldContract() {
        let (transport, states, ended) = makeTransport(autoRejoin: false)
        transport.handleSessionEnd("host-ended")

        XCTAssertEqual(ended.values.count, 1)
        XCTAssertEqual(ended.values.first ?? nil, "host-ended")
        XCTAssertEqual(states.values.last, .ended)
    }

    func testSecondSessionEndIsIgnoredWhileWaiting() {
        // 기다리는 중에 또 종료가 와도 루프를 두 번 띄우지 않는다.
        let (transport, states, ended) = makeTransport(autoRejoin: true)
        transport.handleSessionEnd("host-ended")
        let before = states.values.count
        transport.handleSessionEnd("host-ended")

        XCTAssertEqual(states.values.count, before)
        XCTAssertTrue(ended.values.isEmpty)
    }

    func testRejoiningResetsWhenTheJoinLands() {
        // 합류가 성립하면 재개 상태가 풀려야 한다 — 안 풀면 그 뒤의 세션 종료가
        // "이미 재개 중"으로 오인돼 **영구히 버려진다**(다시는 복귀하지 않는다).
        let (transport, states, _) = makeTransport(autoRejoin: true)
        transport.handleSessionEnd("host-ended")
        transport.confirmJoined(
            me: Member(participantId: "p-1", displayName: "Test Bridge"), history: nil
        )
        let before = states.values.count
        transport.handleSessionEnd("host-ended")

        XCTAssertEqual(states.values.count, before + 1, "두 번째 종료도 재개로 이어져야 한다")
        XCTAssertEqual(states.values.last, .reconnecting)
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

    private func makeTransport(
        autoRejoin: Bool
    ) -> (SocketTransport, Box<TransportState>, Box<String?>) {
        var options = SocketTransportOptions(
            url: "http://192.168.0.10:9981", stageId: "s-1", secretKey: "ck_token-1"
        )
        options.displayName = "Test Bridge"
        options.autoRejoin = autoRejoin
        // 재접속 루프는 실제 HTTP 를 치므로 유닛 테스트에서는 끈다 — 여기서 고정하려는
        // 것은 "끝낼지 기다릴지" 분기다.
        options.reconnection = false

        let transport = SocketTransport(options: options)
        let states = Box<TransportState>()
        let ended = Box<String?>()
        var handlers = TransportHandlers()
        handlers.onState = { states.append($0) }
        handlers.onEnded = { ended.append($0) }
        transport.setHandlersForTest(handlers)
        return (transport, states, ended)
    }

    /// 서버 StageMember 는 join 시 신고한 값을 전부 로스터에 되돌려 주는데, 매퍼가 그것을 버리면
    /// 프로그램 A 는 B 가 어느 채널을 듣는지 알 수 없다 — 여러 프로그램이
    /// 한 스테이지에서 서로를 트리거하는 토폴로지의 첫 번째 조용한 실패.
    /// 서버가 아직 싣는 `nodeType` 은 어댑터가 더는 읽지 않는 필드다 — `Member` 에 그 속성이 없으니
    /// 입력에 있어도 매퍼가 무시한다(컴파일러가 증명).
    func testToMemberCarriesAdapterDeclarations() {
        let m = SocketTransport.toMember(
            [
                "participantId": "p1", "displayName": "rpi-bridge", "clientType": "adapter",
                "nodeType": "hardware", "receives": ["actuator.cmd", "robot.*"],
                "sends": ["sensor.temperature"], "strictRouting": true, "exclusiveName": false,
                "guestOSHostname": "rpi-4b",
            ],
            fallbackId: "f", fallbackName: "f"
        )
        XCTAssertEqual(m.clientType, "adapter")
        XCTAssertEqual(m.receives, ["actuator.cmd", "robot.*"])
        XCTAssertEqual(m.sends, ["sensor.temperature"])
        XCTAssertEqual(m.strictRouting, true)
        XCTAssertEqual(m.exclusiveName, false)
        XCTAssertEqual(m.osHostname, "rpi-4b")

        // 브라우저 참여자 — 서버는 미신고를 null(NSNull) 로 보낸다. null 은 부재와 같다.
        let browser = SocketTransport.toMember(
            [
                "participantId": "p2", "displayName": "host", "clientType": NSNull(),
                "receives": NSNull(), "sends": NSNull(),
                "strictRouting": NSNull(), "exclusiveName": NSNull(), "guestOSHostname": NSNull(),
            ],
            fallbackId: "f", fallbackName: "f"
        )
        XCTAssertNil(browser.clientType)
        XCTAssertNil(browser.receives)
        XCTAssertNil(browser.sends)
        XCTAssertNil(browser.strictRouting)
        XCTAssertNil(browser.exclusiveName)
        XCTAssertNil(browser.osHostname)

        // 타입 불일치는 버린다 — 문자열이 아닌 항목만 걸러내고 나머지는 남긴다
        let odd = SocketTransport.toMember(
            ["participantId": "p3", "displayName": "odd", "receives": ["a.b", 7, NSNull()]],
            fallbackId: "f", fallbackName: "f"
        )
        XCTAssertEqual(odd.receives, ["a.b"])

        // 실제 경로는 JSONSerialization 을 지난다 — 숫자 0/1 은 NSNumber 로 와서 `as? Bool` 을 통과하므로
        // 매퍼가 CFBoolean 만 인정해야 다른 다섯 언어(불리언 타입만 인정)와 같다. 문자열 불리언과
        // 빈 호스트명도 부재다.
        let wire = try! JSONSerialization.jsonObject(with: Data(
            #"{"participantId":"p4","displayName":"w","receives":["a.b"],"strictRouting":1,"exclusiveName":"true","standby":0,"guestOSHostname":""}"#
                .utf8)) as! [String: Any]
        let typed = SocketTransport.toMember(wire, fallbackId: "f", fallbackName: "f")
        XCTAssertNil(typed.strictRouting)
        XCTAssertNil(typed.exclusiveName)
        XCTAssertNil(typed.standby)
        XCTAssertNil(typed.osHostname)
        XCTAssertEqual(typed.receives, ["a.b"])
        // 진짜 불리언은 같은 경로를 지나도 살아 있다
        let real = try! JSONSerialization.jsonObject(with: Data(
            #"{"strictRouting":true,"exclusiveName":false,"standby":true}"#.utf8)) as! [String: Any]
        let realMember = SocketTransport.toMember(real, fallbackId: "f", fallbackName: "f")
        XCTAssertEqual(realMember.strictRouting, true)
        XCTAssertEqual(realMember.exclusiveName, false)
        XCTAssertEqual(realMember.standby, true)
    }

    func testDeclaresChannelMatchesExactOrWildcardAndUndeclaredIsFalse() {
        let declared = Member(participantId: "p", displayName: "d", receives: ["actuator.cmd", "robot.*"])
        XCTAssertTrue(declared.declaresChannel("actuator.cmd"))
        XCTAssertTrue(declared.declaresChannel("robot.status"))
        XCTAssertFalse(declared.declaresChannel("robot")) // 와일드카드는 1+ 세그먼트
        XCTAssertFalse(declared.declaresChannel("sensor.temperature"))
        // 미선언(브로드캐스트 수신자)은 "모름" — false 를 "안 듣는다" 로 읽으면 안 된다
        XCTAssertFalse(Member(participantId: "p", displayName: "d").declaresChannel("actuator.cmd"))
        XCTAssertFalse(Member(participantId: "p", displayName: "d", receives: []).declaresChannel("actuator.cmd"))
    }

    /// roster 매핑 — 화면 자리가 어댑터를 **이름**으로 찾으므로 이 필드가 없으면 진단도
    /// UI 도 그 이름을 알 수 없다. 세 자리(joined me · member-added · list-members)가
    /// 같은 함수를 쓰므로 여기서 한 번 고정한다.
    func testToMemberCarriesProvidesView() {
        let m = SocketTransport.toMember(
            ["participantId": "p1", "displayName": "unity-screen", "providesView": "stream"],
            fallbackId: "fallback",
            fallbackName: "fallback"
        )
        XCTAssertEqual(m.participantId, "p1")
        XCTAssertEqual(m.displayName, "unity-screen")
        XCTAssertEqual(m.providesView, "stream")

        // 미신고는 nil 로 남는다 — "none" 으로 채우면 "신고 안 함"과 "화면 없음"이
        // 구별되지 않고, 구 서버를 신 서버처럼 보이게 만든다.
        XCTAssertNil(
            SocketTransport.toMember(
                ["participantId": "p2", "displayName": "wheel"],
                fallbackId: "f",
                fallbackName: "f"
            ).providesView
        )

        // 모르는 값도 남긴다 — 좁게 받으면 새 종류가 조용히 사라진다.
        XCTAssertEqual(
            SocketTransport.toMember(
                ["providesView": "hologram"], fallbackId: "f", fallbackName: "f"
            ).providesView,
            "hologram"
        )

        // 문자열이 아니면 버리고 fallback 이 적용된다
        let fallback = SocketTransport.toMember(
            ["providesView": 3], fallbackId: "pid", fallbackName: "name"
        )
        XCTAssertEqual(fallback.participantId, "pid")
        XCTAssertEqual(fallback.displayName, "name")
        XCTAssertNil(fallback.providesView)
    }

    /// ⚠️ 실경로 재현 — 소켓 페이로드는 JSONSerialization 산출물(NSNumber)이다.
    /// Swift 딕셔너리 리터럴로만 검증하면 NSNumber→Bool 브리지 결함을 못 잡는다
    /// (적대검수에서 숫자 0/1/0.0/1.0 이 .bool 로 뒤바뀌는 blocker 가 이 사각지대에 숨어 있었다).
    func testToStageMessageFromRealJSONPayload() throws {
        func parse(_ raw: String) throws -> StageMessage? {
            let object = try JSONSerialization.jsonObject(with: Data(raw.utf8))
            return SocketTransport.toStageMessage(object)
        }

        XCTAssertEqual(try parse(#"{"channel":"a.b","value":1}"#)?.value, .int(1))
        XCTAssertEqual(try parse(#"{"channel":"a.b","value":0}"#)?.value, .int(0))
        XCTAssertEqual(try parse(#"{"channel":"a.b","value":1.0}"#)?.value, .int(1))
        XCTAssertEqual(try parse(#"{"channel":"a.b","value":23.5}"#)?.value, .double(23.5))
        XCTAssertEqual(try parse(#"{"channel":"a.b","value":true}"#)?.value, .bool(true))
        XCTAssertEqual(try parse(#"{"channel":"a.b","value":false}"#)?.value, .bool(false))
        XCTAssertEqual(try parse(#"{"channel":"a.b","value":null}"#)?.value, .null)
        XCTAssertEqual(try parse(#"{"channel":"a.b","value":"1"}"#)?.value, .string("1"))
        XCTAssertNil(try parse(#"{"channel":"a.b","value":{"x":1}}"#))
    }

    func testMapHistorySkipsMalformedFrames() {
        let history = SocketTransport.mapHistory([
            ["channel": "a", "value": 1],
            ["value": 2],
            ["channel": "b", "value": ["x": 1]],
            ["channel": "c", "value": "z"],
        ])
        XCTAssertEqual(history.count, 2)
        XCTAssertEqual(history[0].channel, "a")
        XCTAssertEqual(history[1].channel, "c")
    }

    /// validate 상태코드 매핑 — 지금까지 미검증이던 표면(적대검수 A7).
    func testValidateErrorMapping() {
        XCTAssertEqual(SocketTransport.mapValidateError(status: 401, code: nil, base: "http://h").code, "AuthRevoked")
        XCTAssertEqual(SocketTransport.mapValidateError(status: 403, code: nil, base: "http://h").code, "AuthRevoked")
        // 서버의 stageFull 은 409 — 폴스루로 코드가 그대로, 기본 힌트(정원)가 붙는다.
        let full = SocketTransport.mapValidateError(status: 409, code: "StageFull", base: "http://h")
        XCTAssertEqual(full.code, "StageFull")
        XCTAssertTrue(full.hint?.contains("participant cap") ?? false)
        // ck_ 는 회수·만료가 개별 단위라 "share 링크 재발급" 은 틀린 처방이다.
        let revokedKey = SocketTransport.mapValidateError(status: 401, code: "PinNotFound", base: "http://h")
        XCTAssertEqual(revokedKey.code, "AuthRevoked")
        XCTAssertTrue(revokedKey.message.contains("adapter key rejected (401 PinNotFound)"))
        XCTAssertTrue(revokedKey.hint?.contains("Issue a new adapter key") ?? false)
        let locked = SocketTransport.mapValidateError(
            status: 403, code: "Forbidden", base: "http://h", message: "stage-locked"
        )
        XCTAssertEqual(locked.code, "AuthRevoked")
        XCTAssertTrue(locked.hint?.contains("reopen it to guests") ?? false)
        let archived = SocketTransport.mapValidateError(status: 404, code: "StageArchived", base: "http://h")
        XCTAssertEqual(archived.code, "StageArchived")
        XCTAssertTrue(archived.hint?.contains("restore") ?? false)
        let noSession = SocketTransport.mapValidateError(status: 404, code: nil, base: "http://192.168.0.10:9981")
        XCTAssertEqual(noSession.code, "RoomEnded")
        XCTAssertTrue(noSession.hint?.contains("Let adapters start a session") ?? false)
        XCTAssertEqual(SocketTransport.mapValidateError(status: 429, code: nil, base: "http://h").code, "TooManyRequests")
        XCTAssertEqual(SocketTransport.mapValidateError(status: 400, code: nil, base: "http://h").code, "ValidationError")
        XCTAssertEqual(SocketTransport.mapValidateError(status: 500, code: "Boom", base: "http://h").code, "Boom")
        XCTAssertEqual(SocketTransport.mapValidateError(status: 500, code: nil, base: "http://h").code, "Unauthorized")
        // 503 KeyMaterialUnavailable(connect-v2 8fea9402d)만 가른다 — 다른 503 은 종전대로 폴스루.
        let material = SocketTransport.mapValidateError(
            status: 503, code: "KeyMaterialUnavailable", base: "http://h", details: ["retryAfterMs": 1500])
        XCTAssertEqual(material.code, "KeyMaterialUnavailable")
        XCTAssertEqual(material.retryAfterMs, 1500)
        XCTAssertTrue(material.message.contains("cannot read this stage's adapter keys"))
        let other503 = SocketTransport.mapValidateError(status: 503, code: nil, base: "http://h")
        XCTAssertEqual(other503.code, "Unauthorized")
        XCTAssertEqual(other503.message, "token/validate failed (503)")
        XCTAssertNil(other503.hint)
        XCTAssertEqual(
            SocketTransport.mapValidateError(
                status: 429, code: "TooManyRequests", base: "http://h", details: ["retryAfterMs": 1500]
            ).retryAfterMs, 1500)

        let plain = SocketTransport.mapValidateError(status: 401, code: nil, base: "http://h")
        XCTAssertTrue(plain.message.contains("adapter key rejected"))
        XCTAssertTrue(plain.hint?.contains("Issue a new adapter key") ?? false)

        let uiBase = SocketTransport.mapValidateError(
            status: 404, code: nil, base: "https://cloud.protopie.io/connect"
        )
        XCTAssertTrue(uiBase.hint?.contains("https://cloud.protopie.io/api/connect") ?? false)
    }

    /// 400 은 서버가 지목한 필드를 문장에 실어야 한다 — 실사고: clientType 의 **옛 와이어
    /// 값**(지금은 쓰지 않는 문자열)에서 현재의 'adapter' 로 넘어갈 때, 구 클라이언트가 신
    /// 서버에 붙어 400 을 맞았고 "(400)" 뿐인 메시지는 토큰 문제처럼 읽혔다.
    func testValidateError400NamesTheRejectedField() {
        let details: Any = [
            ["path": ["clientType"], "message": "Invalid literal value, expected \"adapter\""]
        ]
        let err = SocketTransport.mapValidateError(
            status: 400, code: nil, base: "http://h", details: details
        )
        XCTAssertEqual(err.code, "ValidationError")
        XCTAssertEqual(
            err.message,
            "token/validate rejected the request body (400): "
                + "clientType — Invalid literal value, expected \"adapter\""
        )
        XCTAssertTrue(err.hint?.contains("version skew") ?? false)

        // details 가 없으면 종전 메시지 그대로
        XCTAssertEqual(
            SocketTransport.mapValidateError(status: 400, code: nil, base: "http://h").message,
            "token/validate rejected the request body (400)"
        )
    }

    func testDescribeValidationIssuesReadsBothServerShapes() {
        // zod(path 배열) · 중첩 path · Fastify/AJV(instancePath) — 서버 error-handler 가
        // 두 경로를 모두 details 로 내보낸다.
        XCTAssertEqual(
            SocketTransport.describeValidationIssues([
                ["path": ["clientType"], "message": "Invalid literal"]
            ]),
            "clientType — Invalid literal"
        )
        XCTAssertEqual(
            SocketTransport.describeValidationIssues([["path": ["sends", 0], "message": "too long"]]),
            "sends.0 — too long"
        )
        XCTAssertEqual(
            SocketTransport.describeValidationIssues([
                ["instancePath": "/displayName", "message": "must NOT be shorter"]
            ]),
            "displayName — must NOT be shorter"
        )

        // 3개까지만 싣고 나머지는 개수로
        XCTAssertEqual(
            SocketTransport.describeValidationIssues(
                ["a", "b", "c", "d", "e"].map { ["path": [$0], "message": "bad"] }
            ),
            "a — bad; b — bad; c — bad (+2 more)"
        )

        // 실을 것이 없으면 빈 문자열 — 호출부가 콜론을 붙이지 않는다
        XCTAssertEqual(SocketTransport.describeValidationIssues(nil), "")
        XCTAssertEqual(SocketTransport.describeValidationIssues([]), "")
        XCTAssertEqual(SocketTransport.describeValidationIssues("nope"), "")
        XCTAssertEqual(SocketTransport.describeValidationIssues([42, [String: Any]()]), "")

        // 한쪽만 있으면 있는 쪽만
        XCTAssertEqual(
            SocketTransport.describeValidationIssues([["message": "Unexpected body"]]),
            "Unexpected body"
        )
        XCTAssertEqual(SocketTransport.describeValidationIssues([["path": ["token"]]]), "token")
    }

    /// 401 인데 키 길이가 46 이 아니면 힌트가 "잘린 키" 를 말한다 — 서버는 길이로 키 경로를 가른다(connect-v2 8fea9402d). 호출부
    /// (validate)의 배선이라 순수 매퍼로는 못 잡는다.
    func testTruncatedKey401HintThroughOpen() async {
        func openError(status: Int, body: String, secretKey: String) async -> AdapterError? {
            StubURLProtocol.status = status
            StubURLProtocol.body = Data(body.utf8)
            let config = URLSessionConfiguration.ephemeral
            config.protocolClasses = [StubURLProtocol.self]
            var options = SocketTransportOptions(url: "http://127.0.0.1:9", stageId: "s-1", secretKey: secretKey)
            options.displayName = "Test Bridge"
            let transport = SocketTransport(options: options, session: URLSession(configuration: config))
            var thrown: AdapterError?
            do {
                _ = try await transport.open(handlers: TransportHandlers())
                XCTFail("\(status) 이면 던져야 한다")
            } catch let err as AdapterError {
                thrown = err
            } catch {
                XCTFail("AdapterError 가 아니라 \(error)")
            }
            await transport.close()
            return thrown
        }
        // 기본 키 'ck_token-1' 은 10자.
        let short = await openError(status: 401, body: #"{"error":"PinNotFound"}"#, secretKey: "ck_token-1")
        XCTAssertEqual(short?.code, "AuthRevoked")
        XCTAssertTrue(short?.message.contains("adapter key rejected (401 PinNotFound)") ?? false, short?.message ?? "nil")
        XCTAssertTrue(short?.hint?.hasPrefix("Issue a new adapter key: Stage → Share → Adapter keys. This key has 10 characters, but a server-issued key is 46 (ck_ plus 43)") ?? false, short?.hint ?? "nil")
        XCTAssertTrue(short?.hint?.contains("truncated or altered") ?? false)
        XCTAssertTrue(
            short?.message.hasSuffix("; the key has 10 characters, a server-issued key has 46") ?? false, short?.message ?? "nil")
        // 46자 키의 401 은 종전 힌트 그대로.
        let full = await openError(
            status: 401, body: #"{"error":"PinNotFound"}"#, secretKey: "ck_" + String(repeating: "a", count: 43))
        XCTAssertEqual(full?.code, "AuthRevoked")
        XCTAssertEqual(full?.hint, SocketTransport.adapterKeyRevokedHint)
        XCTAssertFalse(full?.hint?.contains("truncated") ?? true)
        // 403 은 길이와 무관(키 경로를 지난 뒤의 거절).
        let forbidden = await openError(status: 403, body: #"{"error":"Forbidden"}"#, secretKey: "ck_token-1")
        XCTAssertEqual(forbidden?.code, "AuthRevoked")
        XCTAssertFalse(forbidden?.hint?.contains("truncated") ?? true)
        StubURLProtocol.status = 200
        StubURLProtocol.body = Data()
    }

    /// 순수 매퍼가 아니라 **호출부까지** — 서버 400 본문에서 details 를 실제로 꺼내
    /// 문장에 싣는지. 배선이 틀리면 매퍼 테스트는 통과하면서 실전에서는 조용히
    /// "(400)" 만 남는다.
    func testValidate400CarriesServerDetailsThroughOpen() async {
        StubURLProtocol.status = 400
        StubURLProtocol.body = Data("""
        {"error":"ValidationError","details":\
        [{"path":["clientType"],"message":"Invalid literal value"}]}
        """.utf8)
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [StubURLProtocol.self]

        var options = SocketTransportOptions(
            url: "http://127.0.0.1:9", stageId: "s-1", secretKey: "ck_token-1"
        )
        options.displayName = "Test Bridge"
        let transport = SocketTransport(
            options: options, session: URLSession(configuration: config)
        )
        do {
            _ = try await transport.open(handlers: TransportHandlers())
            XCTFail("400 이면 던져야 한다")
        } catch let err as AdapterError {
            XCTAssertEqual(err.code, "ValidationError")
            XCTAssertEqual(
                err.message,
                "token/validate rejected the request body (400): "
                    + "clientType — Invalid literal value"
            )
        } catch {
            XCTFail("AdapterError 가 아니라 \(error)")
        }
        await transport.close()
    }

    func testClassifyHandshakeErrorPromotesTerminalCodes() {
        XCTAssertEqual(
            SocketTransport.classifyHandshakeError("handshake failed: ProtocolVersionMismatch")?.code,
            "ProtocolVersionMismatch"
        )
        XCTAssertEqual(
            SocketTransport.classifyHandshakeError("AppVersionAheadOfServer")?.code,
            "ClientAheadOfServer"
        )
        // 서버는 app-ahead 를 사람이 읽는 문장으로 보낸다(코드는 err.data 안)
        XCTAssertEqual(
            SocketTransport.classifyHandshakeError(
                "This Connect Cloud server (v0.1.0) is older than your app (v0.2.0)."
            )?.code,
            "ClientAheadOfServer"
        )
        XCTAssertEqual(SocketTransport.classifyHandshakeError("Unauthorized")?.code, "AuthRevoked")

        // 재접속 경로에서는 Unauthorized 를 종결로 보지 않는다(재검증에 맡긴다)
        XCTAssertNil(SocketTransport.classifyHandshakeError("Unauthorized", includeAuth: false))
        XCTAssertNil(SocketTransport.classifyHandshakeError("transport close"))
        XCTAssertNil(SocketTransport.classifyHandshakeError(nil))
    }
}

/// URLSession 을 가로채는 스텁 — validate 응답을 코드/본문 그대로 돌려준다.
final class StubURLProtocol: URLProtocol {
    static var status = 200
    static var body = Data()

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let response = HTTPURLResponse(
            url: request.url!,
            statusCode: StubURLProtocol.status,
            httpVersion: nil,
            headerFields: ["Content-Type": "application/json"]
        )!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: StubURLProtocol.body)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}
