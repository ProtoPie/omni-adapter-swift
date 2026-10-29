import Foundation

/// One recorded line — the same shape as the `omni-adapter watch`/`record` output. `ts` is the server stamp (epoch ms).
public struct RecordedMessage: Sendable, Equatable {
    public let ts: Double
    public let channel: String
    public let value: Primitive
    public let from: String?

    public init(ts: Double, channel: String, value: Primitive, from: String? = nil) {
        self.ts = ts
        self.channel = channel
        self.value = value
        self.from = from
    }
}

/// Options for `replay(client:records:options:)`.
public struct ReplayOptions: Sendable {
    /// 1 = the recorded intervals as they are, 2 = twice as fast, `.infinity` = no intervals.
    public var speed: Double
    /// Number of repetitions.
    public var loop: Int
    /// Waits for the server ack on every publish.
    public var ack: Bool
    /// Time injection (ms) for the interval between publishes — so tests do not actually wait. Defaults to `Task.sleep`.
    /// Used only for the intervals — the no-progress limit (15 s) and the final queue drain are measured on the real
    /// monotonic clock (a wait that does nothing does not make the replay give up at once).
    public var wait: (@Sendable (Double) async -> Void)?
    public var onSent: (@Sendable (RecordedMessage, Int) -> Void)?

    public init(
        speed: Double = 1, loop: Int = 1, ack: Bool = false,
        wait: (@Sendable (Double) async -> Void)? = nil,
        onSent: (@Sendable (RecordedMessage, Int) -> Void)? = nil
    ) {
        self.speed = speed
        self.loop = loop
        self.ack = ack
        self.wait = wait
        self.onSent = onSent
    }
}

/// The `replay` result — the number of messages actually published and the number of completed repetitions.
public struct ReplayResult: Sendable, Equatable {
    public let sent: Int
    public let loops: Int
}

/// Options for `simulate(client:channel:options:)`. Waveforms: `"sine"`, `"ramp"`, `"random"`, `"step"`.
public struct SimulateOptions: Sendable {
    public var waveform: String
    public var min: Double
    public var max: Double
    /// Publishes per second.
    public var hz: Double
    /// How many to send — either this or `durationMs`. When neither is given, 10.
    public var count: Int?
    public var durationMs: Double?
    /// One period of sine and ramp (ms).
    public var periodMs: Double
    /// The values the step waveform cycles through.
    public var steps: [Primitive]?
    /// Decimal places (sine, ramp, random).
    public var decimals: Int
    public var ack: Bool
    /// Time injection (ms) for the interval between publishes — the same as ``ReplayOptions/wait`` (used only for the intervals).
    public var wait: (@Sendable (Double) async -> Void)?
    /// Random number injection (0 ≤ r < 1) — for deterministic tests.
    public var random: (@Sendable () -> Double)?
    public var onSent: (@Sendable (Primitive, Int) -> Void)?

    public init(
        waveform: String, min: Double = 0, max: Double = 1, hz: Double = 1,
        count: Int? = nil, durationMs: Double? = nil, periodMs: Double = 2000,
        steps: [Primitive]? = nil, decimals: Int = 3, ack: Bool = false,
        wait: (@Sendable (Double) async -> Void)? = nil,
        random: (@Sendable () -> Double)? = nil,
        onSent: (@Sendable (Primitive, Int) -> Void)? = nil
    ) {
        self.waveform = waveform
        self.min = min
        self.max = max
        self.hz = hz
        self.count = count
        self.durationMs = durationMs
        self.periodMs = periodMs
        self.steps = steps
        self.decimals = decimals
        self.ack = ack
        self.wait = wait
        self.random = random
        self.onSent = onSent
    }
}

/// The `simulate` result.
public struct SimulateResult: Sendable, Equatable {
    public let values: [Primitive]
    public var sent: Int { values.count }
}

private let waveforms: Set<String> = ["sine", "ramp", "random", "step"]

/// Incoming message → one recorded line (JSON). The same as `watch`'s live line (without the `replay` flag).
public func formatRecorded(_ msg: StageMessage) -> String {
    let obj: [String: Any] = [
        "ts": msg.timestamp,
        "channel": msg.channel,
        "value": msg.value.jsonValue,
        "from": msg.sourceParticipantId,
    ]
    let data = (try? JSONSerialization.data(withJSONObject: obj, options: [.sortedKeys])) ?? Data()
    return String(decoding: data, as: UTF8.self)
}

/// NDJSON recording → list of messages. Blank lines are skipped; a single malformed line rejects the whole
/// recording **with its line number** (silently replaying only part would leave "why doesn't this value show up" unanswerable).
public func parseRecording(_ ndjson: String) throws -> [RecordedMessage] {
    var out: [RecordedMessage] = []
    // 선두 BOM(U+FEFF) 하나는 벗긴다(메모장 등, 여섯 언어 공통). 이하 전부 스칼라로 본다 — `Character` 로는
    // "\r\n" 이 글자 하나라 "\n" 에서 갈라지지 않는다.
    var text = ndjson.unicodeScalars[...]
    if text.first == "\u{FEFF}" { text = text.dropFirst() }
    // 줄 구분은 \r\n | \n — 빈 줄도 줄 번호에 센다(외톨이 \r 은 구분이 아니다).
    let lines = text.split(separator: "\n", omittingEmptySubsequences: false)
        .map { String($0.last == "\r" ? $0.dropLast() : $0) }
    for (idx, raw) in lines.enumerated() {
        // 줄 앞뒤는 JSON 공백 넷만 벗긴다 — `.whitespaces` 는 NBSP 같은 유니코드 공백까지 먹어 다른 언어와 갈렸다.
        let line = StrictJSON.trim(raw)
        if line.isEmpty { continue }
        let n = idx + 1
        // 문법은 여섯 언어 공통 엄격 판정이 정한다(중복 키 · ∞ 숫자 · 외톨이 서로게이트 · 깊이 64 — `JSONSerialization`
        // 은 중복 키에서 앞의 값을 쓰고 줄 머리의 BOM 을 건너뛰었다). 값도 `StrictJSON.parse` 가 만든다 — 최상위
        // 스칼라(42, "abc")도 값으로 돌아와 다른 다섯 언어처럼 "ts" 오류로 간다.
        guard let any = StrictJSON.parse(line) else {
            throw AdapterError("ConfigError", "recording line \(n) is not JSON")
        }
        // NSNumber 1 은 Bool 로도 브리지된다 — `is Bool` 로 거르면 정상 ts 가 죽는다. CFType 으로 가른다.
        guard let obj = any as? [String: Any], let num = obj["ts"] as? NSNumber,
              CFGetTypeID(num) != CFBooleanGetTypeID(), case let ts = num.doubleValue, ts.isFinite else {
            throw AdapterError("ConfigError", "recording line \(n): \"ts\" must be a number (epoch ms)")
        }
        guard let channel = obj["channel"] as? String, !channel.isEmpty else {
            throw AdapterError("ConfigError", "recording line \(n): \"channel\" must be a non-empty string")
        }
        let value: Primitive
        if let v = obj["value"] {
            if v is NSNull {
                value = .null
            } else if let p = Primitive.from(v) {
                value = p
            } else {
                throw AdapterError("ConfigError", "recording line \(n): \"value\" must be a primitive (string | number | boolean | null)")
            }
        } else {
            value = .null
        }
        out.append(RecordedMessage(ts: ts, channel: channel, value: value, from: obj["from"] as? String))
    }
    return out
}

/// Republishes a recording with the same time intervals. The first message goes at once, each later one after
/// waiting the `ts` difference / `speed`. Stops when the client is `ended` or the Task is cancelled (returns the
/// partial result). Publish failures follow the client's rules — with `ack: false` they go to the queue or
/// `send-dropped`; with `ack: true` they throw. Waits for each publish to finish, and after sending everything
/// waits until the queue is empty before returning — `close()` discards whatever remains queued. After 15 s without
/// progress (disconnected, standby) it stops waiting and returns the partial result — publishes that did not finish
/// are not counted as sent.
public func replay(client: ConnectClient, records: [RecordedMessage], options: ReplayOptions = ReplayOptions()) async throws -> ReplayResult {
    guard options.speed > 0 else { throw AdapterError("ConfigError", "replay speed must be > 0 (Infinity = no gaps)") }
    guard options.loop >= 1 else { throw AdapterError("ConfigError", "replay loop must be a positive integer") }
    let wait = options.wait ?? defaultWait
    var sent = 0
    var completed = 0
    let watchdog = RunWatchdog(client: client)
    var finished = true
    do {
        outer: for _ in 0..<options.loop {
            for (i, rec) in records.enumerated() {
                if i > 0 {
                    let gapMs = Swift.max(0, rec.ts - records[i - 1].ts) / options.speed
                    if gapMs > 0, gapMs.isFinite { await wait(gapMs) }
                }
                let ended = await client.state == .ended
                if Task.isCancelled || ended {
                    finished = false
                    break outer
                }
                let ack = options.ack
                guard try await watchdog.awaitSend(ack: ack, { try await client.send(rec.channel, rec.value, ack: ack) }) else {
                    finished = false
                    break outer
                }
                sent += 1
                options.onSent?(rec, i)
            }
            completed += 1
        }
    } catch {
        await watchdog.stop()
        throw error
    }
    // 돌아올 때 남겨 두는 것이 없다 — 감시 루프를 멈추고 끝날 때까지 기다린다.
    await watchdog.stop()
    if finished { await drain(client) }
    return ReplayResult(sent: sent, loops: completed)
}

/// Streams synthetic values into one channel. Sample i is at `i · 1000/hz` ms and the waveform is a function of
/// that time: sine = min + (max−min)·(½+½·sin(2πt/period)) · ramp = min + (max−min)·(t mod period)/period ·
/// random = min + (max−min)·r · step = steps[i mod n]. The first sample goes at once, later ones at `1000/hz`
/// intervals. After sending everything it waits until the queue is empty before returning (gives up after 15 s
/// without progress).
public func simulate(client: ConnectClient, channel: String, options: SimulateOptions) async throws -> SimulateResult {
    guard options.hz > 0, options.hz.isFinite else { throw AdapterError("ConfigError", "simulate hz must be a positive number") }
    guard options.min.isFinite, options.max.isFinite, options.min <= options.max else {
        throw AdapterError("ConfigError", "simulate min/max must be finite with min <= max")
    }
    guard options.periodMs > 0 else { throw AdapterError("ConfigError", "simulate periodMs must be > 0") }
    guard (0...15).contains(options.decimals) else { throw AdapterError("ConfigError", "simulate decimals must be an integer 0..15") }
    if options.waveform == "step", (options.steps ?? []).isEmpty {
        throw AdapterError("ConfigError", "simulate step waveform requires a non-empty steps list")
    }
    guard waveforms.contains(options.waveform) else { throw AdapterError("ConfigError", "unknown waveform: \(options.waveform)") }
    let count: Int
    if let c = options.count {
        guard c >= 1 else { throw AdapterError("ConfigError", "simulate count must be a positive integer") }
        count = c
    } else if let d = options.durationMs {
        guard d > 0 else { throw AdapterError("ConfigError", "simulate durationMs must be > 0") }
        count = Swift.max(1, Int((d * options.hz / 1000).rounded(.down)))
    } else {
        count = 10
    }
    let wait = options.wait ?? defaultWait
    let random = options.random ?? { Double.random(in: 0..<1) }
    let intervalMs = 1000 / options.hz
    let span = options.max - options.min
    let steps = options.steps ?? []
    var values: [Primitive] = []
    let watchdog = RunWatchdog(client: client)
    var finished = true
    do {
        for i in 0..<count {
            if i > 0 { await wait(intervalMs) }
            let ended = await client.state == .ended
            if Task.isCancelled || ended {
                finished = false
                break
            }
            let t = Double(i) * intervalMs
            let value: Primitive
            switch options.waveform {
            case "sine": value = .double(roundTo(options.min + span * (0.5 + 0.5 * sin(2 * Double.pi * t / options.periodMs)), options.decimals))
            case "ramp": value = .double(roundTo(options.min + span * (t.truncatingRemainder(dividingBy: options.periodMs) / options.periodMs), options.decimals))
            case "random": value = .double(roundTo(options.min + span * random(), options.decimals))
            default: value = steps[i % steps.count]
            }
            let ack = options.ack
            guard try await watchdog.awaitSend(ack: ack, { try await client.send(channel, value, ack: ack) }) else {
                finished = false
                break
            }
            values.append(value)
            options.onSent?(value, i)
        }
    } catch {
        await watchdog.stop()
        throw error
    }
    // 돌아올 때 남겨 두는 것이 없다 — 감시 루프를 멈추고 끝날 때까지 기다린다.
    await watchdog.stop()
    if finished { await drain(client) }
    return SimulateResult(values: values)
}

// (E3) 누적 15초는 건강한 흐름도 끊었다: 기본 큐가 가득 차면 초당
// 60건으로 비우는 데 ~17초, 50ms 망에서 ack 재생 400건은 ~20초.
/// The limit on waiting without progress (ms) — the same as the default ack timeout. `close()` discards
/// fire-and-forget sends left in the queue, so without waiting, closing right after returning "sent 100" would
/// deliver only 30 (the publish limit of 60 per second, burst 30). It is time **without progress**, not
/// cumulative — the count restarts whenever a publish finishes or the queue shrinks.
private let noProgressLimitMs = 15_000.0
// (TS 와 같다)
/// How often the watchdog checks progress (ns) — one loop per run.
private let watchdogPollNanos: UInt64 = 100_000_000
/// How often the final queue drain checks the queue (ns).
private let drainPollNanos: UInt64 = 20_000_000

// 주입한 wait 의 호출
// 수로 세던 종전에는 아무것도 안 하는 wait(테스트가 기다리지 않게 하는 문서의 방법)를 주면 재생이 곧바로 포기했고(sent=0), 실제
// sleep 으로도 폴링 수 × 20ms 가 실제 시간보다 짧아 상한이 16–21초로 늘었다.
/// Milliseconds between two monotonic instants (``ConnectClient/monotonicNanos()``). Time without progress is
/// measured on the **real monotonic clock**.
private func elapsedMs(since: UInt64, now: UInt64) -> Double {
    now >= since ? Double(now - since) / 1_000_000 : 0
}

/// Test only — the number of running watchdog loops (to check nothing is left behind after returning).
let runningWatchdogsForTest = WatchdogCounter()

/// Test only — the number of queue drains that have taken their reference time and are watching the queue (the test advances the clock after that).
let runningDrainsForTest = WatchdogCounter()

/// Test-only counter.
final class WatchdogCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var count = 0
    func add(_ delta: Int) { lock.withLock { count += delta } }
    var value: Int { lock.withLock { count } }
}

/// Test only — called **after** one acked publish has finished and recorded its result (inside that publish task). Simulates a publish task that finishes late.
let sendSettledHookForTest = SendSettledHook()

/// Test-only hook slot.
final class SendSettledHook: @unchecked Sendable {
    private let lock = NSLock()
    private var hook: (@Sendable () async -> Void)?
    func set(_ newHook: (@Sendable () async -> Void)?) { lock.withLock { hook = newHook } }
    func run() async {
        if let hook = lock.withLock({ hook }) { await hook() }
    }
}

// 그것을 포기하면 답하지 않는 서버에 대해 조용한 부분 결과가 나왔다(TS · PY · DA 의 CLI 가
// 성공이라고 했다). 깨우개 하나를
// 나눠 쓰던 종전에는 늦게 끝난 발행 작업의 깨우기가 다음 발행의 기다림을 풀어(검수 재현: 결과를 적고 깨우기 사이 40 ms) 그 발행을
// 포기로 셌다 — 서버는 2건을 받았는데 sent=1/6 이 조용히 돌아왔다. 곧바로 끝나는 ack 를 보려고 1 ms 씩 20번 쉬던 것도 없앴다
// (ack 발행마다 ~1.4 ms 지연 · CPU 최대 2.6배).
/// One per run (replay, simulate) — starts a watchdog loop when waiting for an acked publish (one per run), and when
/// the run ends stops it and waits for it to finish (no watchdog task is left behind after returning).
/// Fire-and-forget publishes return at once, so nothing more is created for them.
///
/// It gives up **only while waiting for a publish and only when something is left in the queue** — once
/// ``noProgressLimitMs`` has passed on the real monotonic clock since it started waiting for that publish (or since the
/// queue last shrank). A publish that is not queued yet not finished is an acked publish already emitted: it settles
/// by itself within the ack timeout (success, or a thrown AckTimeout); giving up on it produced a silent partial
/// result against a server that never answers. The publish is a task outside the structure, so giving up or
/// cancelling does not cut it — only the wait is withdrawn.
///
/// Each publish has its own wait (``SendWait``) — a publish task releases only its own wait, and giving up reaches
/// only the wait in progress.
private final class RunWatchdog: @unchecked Sendable {
    private let client: ConnectClient
    private let lock = NSLock()
    private var stalledSince: UInt64 = 0
    private var lastQueued = 0
    private var stopped = false
    private var loop: Task<Void, Never>?
    /// The publish being waited for now — giving up reaches only this (never during the interval between publishes).
    private var current: SendWait?

    init(client: ConnectClient) {
        self.client = client
    }

    /// Waits for one publish — true when it finishes (a rejection is rethrown). false when given up or cancelled —
    /// that publish is not counted as sent (the wait is withdrawn; the publish stays in the queue and its wait absorbs
    /// a late result).
    func awaitSend(ack: Bool, _ send: @escaping @Sendable () async throws -> Void) async throws -> Bool {
        if !ack {
            // fire-and-forget 은 곧바로 돌아온다(큐에 넣거나 내보낸다) — 아무것도 더 만들지 않는다.
            try await send()
            return true
        }
        let wait = SendWait()
        Task {
            do {
                try await send()
                wait.finish(.success(()))
            } catch {
                wait.finish(.failure(error))
            }
            await sendSettledHookForTest.run()
        }
        // 기다리기 시작한 때부터 잰다 — 발행 사이 간격(녹화의 긴 공백)은 진척 없는 시간이 아니다.
        let now = await client.monotonicNanos()
        let queued = await client.queuedCount
        let stoppedNow: Bool = lock.withLock {
            stalledSince = now
            lastQueued = queued
            current = wait
            return stopped
        }
        if stoppedNow { wait.giveUp() }
        ensureStarted()
        // 취소되면 곧바로 깬다 — 감시 루프는 이 작업의 자식이 아니라 취소를 모른다.
        await withTaskCancellationHandler {
            await wait.wait()
        } onCancel: {
            wait.giveUp()
        }
        lock.withLock { if current === wait { current = nil } }
        guard let result = wait.result else { return false }  // 포기했다(또는 취소됐다)
        try result.get()
        return true
    }

    private func ensureStarted() {
        let start: Bool = lock.withLock {
            if loop != nil || stopped { return false }
            return true
        }
        guard start else { return }
        let task = Task { [client] in
            runningWatchdogsForTest.add(1)
            defer { runningWatchdogsForTest.add(-1) }
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: watchdogPollNanos)
                if Task.isCancelled { return }
                let queued = await client.queuedCount
                let now = await client.monotonicNanos()
                let target: SendWait? = self.lock.withLock {
                    if queued < lastQueued { stalledSince = now }  // 큐가 줄었다 — 진척
                    lastQueued = queued
                    guard let waiting = current, queued > 0,
                        elapsedMs(since: stalledSince, now: now) >= noProgressLimitMs
                    else { return nil }
                    return waiting
                }
                target?.giveUp()
            }
        }
        lock.withLock { loop = task }
    }

    /// Stops the watchdog loop and waits until it finishes.
    func stop() async {
        let (task, waiting): (Task<Void, Never>?, SendWait?) = lock.withLock {
            stopped = true
            return (loop, current)
        }
        task?.cancel()
        waiting?.giveUp()
        await task?.value
    }
}

/// The wait for one publish — released only by that publish task (the result), the watchdog (giving up) or cancellation. Wakes once.
private final class SendWait: @unchecked Sendable {
    private let lock = NSLock()
    private var outcome: Result<Void, Error>?
    private var released = false
    private var continuation: CheckedContinuation<Void, Never>?

    var result: Result<Void, Error>? { lock.withLock { outcome } }

    /// The publish finished — records the result and releases this wait.
    func finish(_ result: Result<Void, Error>) {
        let waiter: CheckedContinuation<Void, Never>? = lock.withLock {
            if outcome == nil { outcome = result }
            return releaseLocked()
        }
        waiter?.resume()
    }

    /// Giving up, cancellation, stop — releases this wait without a result (does nothing if it already finished).
    func giveUp() {
        let waiter: CheckedContinuation<Void, Never>? = lock.withLock { releaseLocked() }
        waiter?.resume()
    }

    func wait() async {
        await withCheckedContinuation { (c: CheckedContinuation<Void, Never>) in
            let resumeNow: Bool = lock.withLock {
                if released { return true }
                continuation = c
                return false
            }
            if resumeNow { c.resume() }
        }
    }

    private func releaseLocked() -> CheckedContinuation<Void, Never>? {
        released = true
        let waiter = continuation
        continuation = nil
        return waiter
    }
}

/// Waits until the queue is empty (stops when the run ends or is cancelled) — gives up when ``noProgressLimitMs``
/// passes on the real monotonic clock without the queue shrinking. Does not use the injected wait (that is for
/// intervals only) — running on a wait that does nothing returned before the queue was empty.
private func drain(_ client: ConnectClient) async {
    var lastQueued = await client.queuedCount
    var since = await client.monotonicNanos()
    runningDrainsForTest.add(1)
    defer { runningDrainsForTest.add(-1) }
    while true {
        if Task.isCancelled { return }
        if await client.queuedCount == 0 { return }
        if await client.state == .ended { return }
        try? await Task.sleep(nanoseconds: drainPollNanos)
        let queued = await client.queuedCount
        let now = await client.monotonicNanos()
        if queued < lastQueued { since = now }
        lastQueued = queued
        if elapsedMs(since: since, now: now) >= noProgressLimitMs { return }
    }
}

// 여섯 언어 공통 규칙.
/// floor(v·f + 0.5) / f — the rounding direction is not left to the language default (`.rounded()` = half away from zero on the absolute value).
private func roundTo(_ v: Double, _ decimals: Int) -> Double {
    let f = pow(10.0, Double(decimals))
    let r = (v * f + 0.5).rounded(.down) / f
    return r == 0 ? 0 : r
}

@Sendable private func defaultWait(_ ms: Double) async {
    try? await Task.sleep(nanoseconds: UInt64(Swift.max(0, ms) * 1_000_000))
}
