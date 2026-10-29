import Foundation

// 6개 언어 어댑터가 같은 모양을 쓴다.
/// One diagnostics entry.
public struct LogEntry: Sendable, Equatable, CustomStringConvertible {
    /// Epoch ms.
    public let at: Int64
    /// `debug` | `info` | `warn` | `error`.
    public let level: String
    /// A stable identifier — branch and aggregate on this value.
    public let code: String
    public let message: String

    public init(at: Int64, level: String, code: String, message: String) {
        self.at = at
        self.level = level
        self.code = code
        self.message = message
    }

    public var description: String { "[\(level)] \(code): \(message)" }
}

// 실사고(2026-08-21): 원격 뷰어가 화면을 못 받았는데 어댑터 로그에는 "협상 시작"
// 만 있고 결과가 없어 원인을 로그로 가릴 수 없었다. TS `diagnostics.ts` 미러.
// (`DiagnosticsTests` 가 잠근다.)
/// The diagnostics recorder — a **trace you can read after the fact** when an unattended adapter fails.
///
/// A headless adapter running on a kiosk leaves **no trace at all** when the app does not subscribe to
/// `events()`. So there are two outlets: ① a sink that forwards to the host's logging system (`os_log`) and
/// ② a bounded ring buffer for environments without file logs.
///
/// ⚠️ Credentials are never recorded — entries carry only state names, error codes and channel names.
final class Diagnostics: @unchecked Sendable {
    /// Ring buffer limit — enough to hold the stretch right before a failure, with negligible memory.
    static let limit = 100

    private let sink: (@Sendable (LogEntry) -> Void)?
    private let cap: Int
    private let now: @Sendable () -> Int64
    private let lock = NSLock()
    private var ring: [LogEntry] = []

    init(
        sink: (@Sendable (LogEntry) -> Void)? = nil,
        limit: Int = Diagnostics.limit,
        now: (@Sendable () -> Int64)? = nil
    ) {
        self.sink = sink
        self.cap = limit
        self.now = now ?? { Int64(Date().timeIntervalSince1970 * 1000) }
    }

    func note(_ code: String, _ message: String, level: String = "info") {
        let entry = LogEntry(at: now(), level: level, code: code, message: message)
        lock.lock()
        ring.append(entry)
        // 상한을 넘으면 **가장 오래된 것**을 버린다 — 실패는 마지막 구간에 있다.
        if ring.count > cap { ring.removeFirst(ring.count - cap) }
        lock.unlock()
        // 싱크가 던질 수 없는 시그니처다(Swift 클로저는 throws 아님) — 그래도 어댑터는
        // 진단 때문에 멈추지 않는다는 규율은 다른 언어와 같다.
        sink?(entry)
    }

    /// A copy, oldest first.
    func entries() -> [LogEntry] {
        lock.lock()
        defer { lock.unlock() }
        return ring
    }
}
