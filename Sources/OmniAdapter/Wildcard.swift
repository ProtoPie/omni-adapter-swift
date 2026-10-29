import Foundation

// ⚠️ 정규식을 쓰지 않는다. 종전에는 `*` 를 `[^.]+(?:\.[^.]+)*` 로 컴파일했는데, 그런 그룹이
// 여러 개 이어지면 역추적이 지수로 터진다(ReDoS).
/// Channel wildcards — a client-side filter (the server only fans out to the room).
/// Rule: `*` matches **one or more** dot segments; there is no `**`.
/// `button.tap` matches exactly · `robot.*` → `robot.status`/`robot.arm.pos` · `*` matches everything.
///
/// ⚠️ **No regular expressions.** A pattern can come **from someone else**: `listeners(_:)` matches the
/// `receives` entries of **other participants**, as carried on the roster, against your channel. The server
/// validates those entries only as 1..512-character strings, so a regex-based matcher would let one
/// participant that declares `*.*.*…x` stall the CPU of whoever looks it up. The segment DP below is
/// O(pattern × channel) and never backtracks.
public enum Wildcard {
    /// Upper bound of the split cache — the keys are **strings from others**, so without a bound it would grow forever.
    private static let cacheMax = 256
    private static let cacheLock = NSLock()
    private static var cache: [String: [String]] = [:]

    /// Splits a pattern into segments (cached so the receive hot path does not split it every time).
    public static func compileChannelPattern(_ pattern: String) -> [String] {
        cacheLock.lock()
        defer { cacheLock.unlock() }
        if let cached = cache[pattern] { return cached }
        let segments = pattern.components(separatedBy: ".")
        // 상한 초과 시 통째로 비운다 — 다시 채우는 비용이 분할 한 번이다.
        if cache.count >= cacheMax { cache.removeAll(keepingCapacity: true) }
        cache[pattern] = segments
        return segments
    }

    public static func matchChannel(_ pattern: String, _ channel: String) -> Bool {
        if pattern == channel { return true }
        guard pattern.contains("*") else { return false }
        return matchSegments(compileChannelPattern(pattern), channel.components(separatedBy: "."))
    }

    // 종전 `[^.]+` 와 같은 의미.
    /// Segment DP — `reachable[j]` = whether the pattern segments seen so far can consume the first j channel segments.
    /// `*` **never consumes an empty segment**.
    private static func matchSegments(_ pattern: [String], _ channel: [String]) -> Bool {
        let n = channel.count
        var reachable = [Bool](repeating: false, count: n + 1)
        reachable[0] = true
        for segment in pattern {
            var next = [Bool](repeating: false, count: n + 1)
            var j = 1
            while j <= n {
                if segment == "*" {
                    next[j] = !channel[j - 1].isEmpty && (next[j - 1] || reachable[j - 1])
                } else {
                    next[j] = reachable[j - 1] && segment == channel[j - 1]
                }
                j += 1
            }
            reachable = next
        }
        return reachable[n]
    }
}
