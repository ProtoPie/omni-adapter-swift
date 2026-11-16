import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

/// The verdict of one diagnostic check.
public enum CheckStatus: String, Sendable { case ok, warn, fail, skip }

/// One diagnostic check — a human-readable line + (on failure) the next action.
public struct CheckResult: Sendable, Equatable {
    public let id: String
    public let label: String
    public let status: CheckStatus
    public let detail: String?
    public let hint: String?

    public init(
        id: String, label: String, status: CheckStatus, detail: String? = nil,
        hint: String? = nil
    ) {
        self.id = id
        self.label = label
        self.status = status
        self.detail = detail
        self.hint = hint
    }
}

/// HTTP response for diagnostics — only the status code and body are looked at.
public struct DoctorHttpResponse: Sendable {
    public let status: Int
    public let body: String
    public init(status: Int, body: String) {
        self.status = status
        self.body = body
    }
}

/// HTTP call for diagnostics — tests replace it. `body` nil means GET, otherwise a JSON POST.
public typealias DoctorHttp = @Sendable (_ url: String, _ body: String?) async throws ->
    DoctorHttpResponse

/// The factory diagnostics use when they try to connect — tests replace it.
public typealias DoctorConnect = @Sendable (_ credential: String) async throws -> ConnectClient

// TS `doctor.ts` / Python `doctor.py` 미러. (터미널 CLI 배포는 TS·PY 의 몫이다.)
/// Diagnoses one credential **end to end**.
///
/// When an earlier step fails, later steps do not run: the goal is to **point at exactly one cause**, not to
/// list cascading failures.
///
/// The Swift package ships no terminal CLI — instead this function is open as a library API, so an app can put
/// it straight into its own diagnostics screen.
public func runDoctor(
    credential: String,
    http: DoctorHttp? = nil,
    connectImpl: DoctorConnect? = nil
) async -> [CheckResult] {
    let call = http ?? defaultDoctorHttp
    var results: [CheckResult] = []

    let (target, credResult) = resolveTarget(credential)
    results.append(credResult)
    guard let target else { return results }

    let shape = checkUrlShape(target.url)
    results.append(shape)
    if shape.status == .fail { return results }

    let about = await checkAbout(target, call)
    results.append(about)
    if about.status == .fail { return results }

    // 화면 지원은 **세션보다 먼저** 본다 — 프로브가 body 검증 단계에서 끝나므로 라이브
    // 세션이 없어도 답이 나오고, 서버 배포 시차를 세션을 열기 전에 알 수 있다.
    // 이 항목은 절대 fail 이 아니다(warn/skip) — 데이터 메시징은 그대로 되므로.
    let screen = await checkScreenSupport(target, call)
    results.append(screen)
    results.append(await checkRemoteInputSurface(target, call))

    let stage = await checkValidate(target, call)
    results.append(stage)
    if stage.status == .fail { return results }

    let joinResults = await checkJoin(credential, connectImpl)
    // 서버가 providesView 를 모르면 roster 에 그 필드가 아예 없다 — 그 상태에서 "붙은
    // 화면 어댑터 없음" 을 덧붙이면 원인을 두 번 다르게 말하는 셈이 된다.
    let serverKnowsScreens = screen.status == .ok
    results.append(
        contentsOf: joinResults.filter { $0.id != "screen-adapters" || serverKnowsScreens })
    return results
}

/// A human-readable checklist + what to do on failure.
public func formatDoctorReport(_ results: [CheckResult]) -> String {
    let glyph: [CheckStatus: String] = [.ok: "✓", .warn: "△", .fail: "✖", .skip: "·"]
    var lines: [String] = []
    for r in results {
        let label = r.label.padding(toLength: max(18, r.label.count), withPad: " ", startingAt: 0)
        lines.append("\(glyph[r.status] ?? "?") \(label)\(r.detail ?? "")")
        if let hint = r.hint, r.status != .ok { lines.append("  → \(hint)") }
    }
    lines.append("")
    lines.append(
        results.contains(where: { $0.status == .fail })
            ? "✖ not ready — fix the first failing step above and run doctor again."
            : "✓ ready — connect() will work with this credential.")
    return lines.joined(separator: "\n")
}

// ── 내부 ────────────────────────────────────────────────────────────────────

private struct DoctorTarget {
    let url: String
    let stageId: String
    let secretKey: String
}

private func mask(_ secret: String) -> String {
    guard secret.count > 8 else { return "****" }
    return "\(secret.prefix(4))…\(secret.suffix(4))"
}

private func trimSlash(_ url: String) -> String {
    url.hasSuffix("/") ? String(url.dropLast()) : url
}

private func resolveTarget(_ credential: String) -> (DoctorTarget?, CheckResult) {
    // 공백이 먼저다(connect() 와 같은 순서) — 아니면 앞 공백은 "cst_ 토큰이 아니다" 로, 끝 줄바꿈은 "새 토큰을
    // 만들라" 로 안내돼 멀쩡한 토큰을 버리게 된다.
    if StageToken.hasEdgeWhitespace(credential) {
        return (
            nil,
            CheckResult(
                id: "credential", label: "credential", status: .fail,
                detail: StageToken.credentialWhitespaceMessage, hint: StageToken.credentialWhitespaceHint)
        )
    }
    do {
        if StageToken.isStageToken(credential) {
            let t = try StageToken.parse(credential)
            let target = DoctorTarget(url: t.url, stageId: t.stageId, secretKey: t.secretKey)
            let detail = "cst_ token → url=\(t.url)  stage=\(t.stageId)  key=\(mask(t.secretKey))"
            // 길이가 46 이 아닌 키 — 서버는 길이로 키 경로를 가르므로 그 토큰은 401 을 받는다. 계속 진행하되
            // (모의 서버는 짧은 키를 받는다) 뒤의 401 을 "회수됨" 으로 읽지 않게 먼저 말한다.
            if t.secretKey.count != Protocol.adapterKeyLength {
                return (
                    target,
                    CheckResult(
                        id: "credential", label: "credential", status: .warn,
                        detail:
                            "\(detail) — the key has \(t.secretKey.count) characters, a server-issued key has \(Protocol.adapterKeyLength)",
                        hint:
                            "The token looks truncated or altered — copy it again (Stage → Share → Adapter keys; Copy appears right after Create key), or create a new key."
                    )
                )
            }
            return (target, CheckResult(id: "credential", label: "credential", status: .ok, detail: detail))
        }
    } catch {
        let message = (error as? AdapterError)?.message ?? "\(error)"
        return (
            nil,
            CheckResult(
                id: "credential", label: "credential", status: .fail, detail: message,
                hint: "Create a fresh adapter token: Stage → Share → Adapter keys → Create key.")
        )
    }
    if StageToken.isAdapterKey(credential) {
        // bare ck_ 는 유효한 키지만 좌표가 없어 진단할 수 없다 — "cst_ 를 내라" 보다 정확히 말한다(TS 와 같은 문구).
        return (
            nil,
            CheckResult(
                id: "credential", label: "credential", status: .fail,
                detail: "adapter key (\(mask(credential))) carries no url/stageId",
                hint: "doctor needs a self-describing credential — use the cst_ token for diagnosis; the ck_ key itself works fine with connect(ConnectOptions(secretKey:url:stageId:))."
            )
        )
    }
    return (
        nil,
        CheckResult(
            id: "credential", label: "credential", status: .fail,
            detail: "not a cst_ token",
            hint: "Pass \"cst_…\" (Stage → Share → Adapter keys → Create key)."
        )
    )
}

private func checkUrlShape(_ url: String) -> CheckResult {
    let base = trimSlash(url)
    // cloud 는 UI base 가 `/connect`, 어댑터 base 가 `/api/connect` 다 — 손으로 만든 URL 이
    // 가장 자주 틀리는 지점이고, 증상은 Next 404 라 "세션 없음" 처럼 보인다.
    if base.hasSuffix("/connect") && !base.hasSuffix("/api/connect") {
        let origin = String(base.dropLast("/connect".count))
        return CheckResult(
            id: "url-shape", label: "url shape", status: .fail,
            detail: "\"\(base)\" looks like the Connect web UI base",
            hint:
                "The cloud API base is usually \"\(origin)/api/connect\" — re-copy the adapter token from the Share dialog."
        )
    }
    return CheckResult(
        id: "url-shape", label: "url shape", status: .ok,
        detail: base.hasSuffix("/api/connect") ? "cloud API base" : "origin base (local/LAN)")
}

private func jsonObject(_ text: String) -> [String: Any]? {
    guard let data = text.data(using: .utf8),
        let parsed = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
    else { return nil }
    return parsed
}

private func checkAbout(_ target: DoctorTarget, _ http: DoctorHttp) async -> CheckResult {
    do {
        let res = try await http("\(trimSlash(target.url))/api/about", nil)
        guard (200..<300).contains(res.status) else {
            return CheckResult(
                id: "server", label: "server reachable", status: .fail,
                detail: "GET /api/about → \(res.status)",
                hint: "The address answers but is not a Connect server — check the url in the token."
            )
        }
        let about = jsonObject(res.body) ?? [:]
        let floor = about["minClientProtocol"] as? String
        if let floor, compareTriples(Protocol.protocolVersion, floor) < 0 {
            return CheckResult(
                id: "server", label: "server reachable", status: .fail,
                detail:
                    "server requires protocol ≥ \(floor), this adapter speaks \(Protocol.protocolVersion)",
                hint: "Update OmniAdapter.")
        }
        // 어댑터를 받지 않는 조립(Connect Omni 가 아닌 Desktop · Embedded · 연동판) — `ck_` 입장이 401 로 거절되므로 뒤의 stage 검사가
        // "키가 회수됐다" 로 오진한다. 능력 플래그를 내는 서버(2026-10 이후)에서만 판정하고, 없으면 종전대로 넘어간다. 불리언 false 만
        // 본다(`strictBool` — JSON 0 은 false 가 아니다).
        if let capabilities = about["capabilities"] as? [String: Any],
            SocketTransport.strictBool(capabilities[Protocol.aboutCapabilityFields[0]]) == false
        {
            return CheckResult(
                id: "server", label: "server reachable", status: .fail,
                detail:
                    "\(about["runMode"] as? String ?? "?") v\(about["version"] as? String ?? "?") — this server does not take adapters (capabilities.adapterGuest is false)",
                hint:
                    "Adapters join Connect Omni servers only — this is a Connect without the Omni package. Point the token at a Connect Omni server, or ask whoever runs your Connect."
            )
        }
        // ICE 배포 설정도 이 줄에 싣는다 — "타 망 뷰어만 화면이 안 나온다" 의 1차 원인이
        // ICE 부재인데 그 상태는 어댑터 쪽에 아무 흔적을 안 남긴다(doctor 가 유일한 관측점).
        let ice = (about["iceServers"] as? [Any])?.count ?? 0
        let iceText = ice > 0 ? "\(ice)" : "none (cross-network screen viewing may fail)"
        return CheckResult(
            id: "server", label: "server reachable", status: .ok,
            detail:
                "\(about["runMode"] as? String ?? "?") v\(about["version"] as? String ?? "?") · protocol \(about["protocolVersion"] as? String ?? "?") (min: \(floor ?? "none")) — compatible · ICE servers: \(iceText)"
        )
    } catch {
        return CheckResult(
            id: "server", label: "server reachable", status: .fail, detail: "\(error)",
            hint: "Server unreachable — check the address, VPN/firewall, and that Connect is running."
        )
    }
}

// 조작이 안 될 때 "이 배포에 표면 자체가 있는가"가 첫 갈림길이다(실사고 2026-09-03).
/// Whether the remote-input (control) surface exists — looks **only at the status code** of the registration route
/// (an unauthenticated GET). 401/403 → the route exists (standalone, control surface included) · 404 → it does not
/// (another server, or an older one; a stream view is view-only). No seat, no authentication, no side effect.
/// When control does not work, "does this deployment have the surface at all" is the first fork.
private func checkRemoteInputSurface(_ target: DoctorTarget, _ http: DoctorHttp) async -> CheckResult {
    let label = "remote input"
    do {
        let stage = target.stageId.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed)
            ?? target.stageId
        let base = target.url.hasSuffix("/") ? String(target.url.dropLast()) : target.url
        let res = try await http("\(base)/api/stages/\(stage)/adapter-registrations", nil)
        switch res.status {
        case 401, 403:
            return CheckResult(
                id: "remote-input", label: label, status: .ok,
                detail: "server has the remote-input surface — an editor can operate a streamed screen")
        case 404:
            return CheckResult(
                id: "remote-input", label: label, status: .warn,
                detail: "no remote-input surface — streamed screens are view-only on this deployment",
                hint: "Screens still broadcast; only operating them from the stage is unavailable. "
                    + "Remote input runs on standalone servers only — other servers never have it.")
        default:
            return CheckResult(
                id: "remote-input", label: label, status: .skip,
                detail: "probe returned HTTP \(res.status) (expected 401/403/404) — cannot tell")
        }
    } catch {
        return CheckResult(
            id: "remote-input", label: label, status: .skip, detail: String(describing: error))
    }
}

/// Whether views (adapter screens) are supported — checks whether the server **knows** `providesView`.
///
/// An older server silently drops keys it does not know. The adapter joins normally and is only missing from the
/// view layer's candidate list — the symptom is "it joined, but I can't pick my adapter in the view slot", which
/// does not reveal the cause.
///
/// How to tell without writing anything: send a body that is **deliberately a 400**. `displayName: ""` (violates
/// min(1)) is rejected by every server, so **no member is created and no guest seat is used.**
private func checkScreenSupport(_ target: DoctorTarget, _ http: DoctorHttp) async -> CheckResult {
    let label = "adapter screens"
    do {
        let body = jsonString([
            "token": target.secretKey,
            "displayName": "",  // 의도적 위반 — 이 요청은 성공하면 안 된다
            "providesView": "__omni_doctor_probe__",
        ])
        let res = try await http(validateEndpoint(target), body)
        guard res.status == 400 else {
            // 프로브 전제가 깨졌다 — 모르는 것을 아는 척하지 않고 skip 으로 남긴다.
            return CheckResult(
                id: "screen", label: label, status: .skip,
                detail: "probe returned HTTP \(res.status) (expected 400) — cannot tell")
        }
        let details = (jsonObject(res.body)?["details"] as? [Any]) ?? []
        let mentions = details.contains { issue in
            guard let path = (issue as? [String: Any])?["path"] as? [Any] else { return false }
            return path.contains { ($0 as? String) == "providesView" }
        }
        if mentions {
            return CheckResult(
                id: "screen", label: label, status: .ok,
                detail:
                    "server accepts providesView — 'stream' adapters can be picked by an Adapter Stream layer"
            )
        }
        return CheckResult(
            id: "screen", label: label, status: .warn,
            detail: "server ignores providesView — this build predates adapter screens",
            hint:
                "Data messaging works. Screens do not: the adapter connects but never appears as a candidate in an Adapter Stream layer. Update the Connect server."
        )
    } catch {
        return CheckResult(id: "screen", label: label, status: .skip, detail: "\(error)")
    }
}

private func checkValidate(_ target: DoctorTarget, _ http: DoctorHttp) async -> CheckResult {
    let label = "stage session"
    do {
        // 진짜 키 입장이다(서버가 멤버를 만든다) — 트랜스포트처럼 어댑터 신원을 싣는다.
        var fields: [String: Any] = ["token": target.secretKey, "displayName": "omni-adapter doctor"]
        for (key, value) in AdapterIdentity.fields() { fields[key] = value }
        let body = jsonString(fields)
        let res = try await http(validateEndpoint(target), body)
        if (200..<300).contains(res.status) {
            return CheckResult(
                id: "stage", label: label, status: .ok, detail: "live — token accepted")
        }
        // 실패 분류는 connect() 와 **같은 매퍼**를 쓴다 — doctor 만 다른 문장을 내면
        // stage-locked·StageArchived 같은 분기가 여기서만 빠진다.
        let payload = jsonObject(res.body)
        // 잘린 키의 401 은 connect() 와 같은 판정으로 "회수됨" 대신 길이를 말한다(credential 단계의 warn 과 짝).
        let err = SocketTransport.adapterKeyLengthRejection(
            SocketTransport.mapValidateError(
                status: res.status,
                code: (payload?["error"] as? String) ?? (payload?["code"] as? String),
                base: target.url,
                details: payload?["details"],
                message: payload?["message"] as? String
            ),
            status: res.status, keyLength: target.secretKey.count)
        // 429 · 503 은 기다릴 일 — warn 으로 두고 join 까지 간다(둘 다 서버의 retryAfterMs 를 힌트에 싣는다).
        let waitable = err.code == "TooManyRequests" || err.code == "KeyMaterialUnavailable"
        return CheckResult(
            id: "stage", label: label, status: waitable ? .warn : .fail,
            detail: err.message, hint: err.hint)
    } catch {
        return CheckResult(id: "stage", label: label, status: .fail, detail: "\(error)")
    }
}

/// The **names of view-providing adapters** currently on the stage.
///
/// A view slot (layer) finds its adapter by **display name** (participant ids are issued anew on every run).
/// The most common failure is a typo or mismatch, and the only symptom is "the layer keeps waiting", which does
/// not tell which side is wrong — this line shows the names the server actually knows.
private func screenAdapterNames(_ members: [Member]) -> [String] {
    members.compactMap { m in
        guard let view = m.providesView, view != "none" else { return nil }
        return "\(m.displayName.isEmpty ? "(unnamed)" : m.displayName) (\(view))"
    }
}

private func checkJoin(_ credential: String, _ connectImpl: DoctorConnect?) async -> [CheckResult] {
    let started = Date()
    var client: ConnectClient?
    do {
        if let connectImpl {
            client = try await connectImpl(credential)
        } else {
            var options = ConnectOptions()
            options.secretKey = credential
            options.displayName = "omni-adapter doctor"
            client = try await connect(options)
        }
        guard let live = client else { return [] }
        let ms = Int(Date().timeIntervalSince(started) * 1000)
        let members = (try? await live.members()) ?? []
        let names = screenAdapterNames(members)
        let me = await live.me?.displayName ?? "?"
        await live.close()
        let plural = members.count == 1 ? "" : "s"
        return [
            CheckResult(
                id: "join", label: "socket join", status: .ok,
                detail:
                    "joined in \(ms)ms as \(me) · \(members.count) participant\(plural) on stage"),
            names.isEmpty
                // warn 이 아니다 — 데이터만 쓰는 스테이지에서는 이게 정상이고, 정상을
                // 노랗게 칠하면 다음부터 아무도 안 읽는다.
                ? CheckResult(
                    id: "screen-adapters", label: "screen adapters", status: .skip,
                    detail: "none connected right now — a screen layer will sit in \"waiting\"")
                : CheckResult(
                    id: "screen-adapters", label: "screen adapters", status: .ok,
                    detail:
                        "\(names.joined(separator: " · ")) — put one of these names in the layer's adapter name"
                ),
        ]
    } catch {
        if let live = client { await live.close() }
        let adapterError = error as? AdapterError
        return [
            CheckResult(
                id: "join", label: "socket join", status: .fail,
                detail: "[\(adapterError?.code ?? "Error")] \(adapterError?.message ?? "\(error)")",
                hint: adapterError?.hint)
        ]
    }
}

private func validateEndpoint(_ target: DoctorTarget) -> String {
    "\(trimSlash(target.url))/api/stages/\(target.stageId)/token/validate"
}

private func jsonString(_ object: [String: Any]) -> String {
    guard let data = try? JSONSerialization.data(withJSONObject: object),
        let text = String(data: data, encoding: .utf8)
    else { return "{}" }
    return text
}

/// Compares 'x.y.z' semver triples — 0 when unparsable (verdict withheld).
private func compareTriples(_ a: String, _ b: String) -> Int {
    let pa = a.split(separator: ".").compactMap { Int($0) }
    let pb = b.split(separator: ".").compactMap { Int($0) }
    guard pa.count == 3, pb.count == 3 else { return 0 }
    for i in 0..<3 where pa[i] != pb[i] { return pa[i] - pb[i] }
    return 0
}

/// Default HTTP — the same URLSession the adapter already uses (no new dependency).
private let defaultDoctorHttp: DoctorHttp = { url, body in
    guard let target = URL(string: url) else {
        throw AdapterError("ConfigError", "bad url: \(url)")
    }
    var request = URLRequest(url: target)
    request.timeoutInterval = 8
    if let body {
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "content-type")
        request.httpBody = body.data(using: .utf8)
    }
    let (data, response) = try await URLSession.shared.data(for: request)
    let status = (response as? HTTPURLResponse)?.statusCode ?? 0
    return DoctorHttpResponse(status: status, body: String(data: data, encoding: .utf8) ?? "")
}
