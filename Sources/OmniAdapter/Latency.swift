import Foundation

// TS `src/latency.ts` / Python `latency.py` 미러.
/// Round-trip latency measurement — the primitive that answers "how many ms does a value take to cross"
/// **with a number**.
///
/// **What it measures**: the adapter ↔ server socket round trip (RTT). It uses `stage:list-members`,
/// the only surface that has a request/response and **leaves no side effect on the stage**. Measuring
/// with publishes (`stage:message`) would fill the Console with noise and wake the prototype's Receive triggers.
///
/// **What it does not measure**: end-to-end latency to another participant. The server does not echo a
/// sender's own messages, so a single participant cannot measure it in principle.

/// Sample summary — percentiles instead of a mean (latency trouble lives in the tail).
public struct LatencyStats: Sendable, Equatable {
    /// Number of successful samples.
    public let count: Int
    /// Number of failures (timeouts, rejections) — never mixed into the success percentiles.
    public let failed: Int
    public let min: Double
    public let p50: Double
    public let p95: Double
    public let max: Double
}

/// The result of one measurement — failures are recorded as samples too (no silent omissions).
public struct LatencySample: Sendable, Equatable {
    /// Epoch ms.
    public let at: Int64
    /// Round trip in ms. nil on failure.
    public let rttMs: Double?
    /// Failure reason code (`AdapterError.code`) — nil on success.
    public let error: String?
}

/// Percentile — nearest-rank method.
///
/// With few samples, interpolation **invents values that were never observed**, so at field sample sizes
/// (a few dozen) nearest-rank is the honest choice.
public func percentile(_ sortedAsc: [Double], _ p: Double) -> Double {
    guard !sortedAsc.isEmpty else { return Double.nan }
    let rank = Int(ceil(p / 100.0 * Double(sortedAsc.count)))
    let idx = Swift.min(sortedAsc.count, Swift.max(1, rank)) - 1
    return sortedAsc[idx]
}

/// Samples → summary. Does not modify its input.
public func summarize(_ samples: [Double], failed: Int = 0) -> LatencyStats {
    let ordered = samples.sorted()
    guard let first = ordered.first, let last = ordered.last else {
        return LatencyStats(
            count: 0, failed: failed, min: .nan, p50: .nan, p95: .nan, max: .nan)
    }
    return LatencyStats(
        count: ordered.count,
        failed: failed,
        min: first,
        p50: percentile(ordered, 50),
        p95: percentile(ordered, 95),
        max: last
    )
}

/// Measures `count` times and returns the summary and the samples.
///
/// A failed attempt is **left out of the samples and counted in `failed`** — mixing a timeout in as a large
/// number silently skews the percentiles, and mixing it in as 0 makes latency look better. Both are lies.
public func measureLatency(
    ping: @Sendable () async throws -> Double,
    count: Int = 10,
    intervalMs: Int = 200,
    onSample: (@Sendable (LatencySample) -> Void)? = nil,
    sleep: (@Sendable (Int) async -> Void)? = nil,
    now: (@Sendable () -> Int64)? = nil
) async throws -> (stats: LatencyStats, samples: [LatencySample]) {
    guard count >= 1 else {
        throw AdapterError(
            "ConfigError", "latency count must be a positive integer (got \(count))")
    }
    let doSleep =
        sleep ?? { ms in try? await Task.sleep(nanoseconds: UInt64(ms) * 1_000_000) }
    let clock = now ?? { Int64(Date().timeIntervalSince1970 * 1000) }

    var samples: [LatencySample] = []
    var ok: [Double] = []
    var failed = 0

    for i in 0..<count {
        if i > 0, intervalMs > 0 { await doSleep(intervalMs) }
        var sample: LatencySample
        do {
            let rtt = try await ping()
            ok.append(rtt)
            sample = LatencySample(at: clock(), rttMs: rtt, error: nil)
        } catch let err as AdapterError {
            failed += 1
            sample = LatencySample(at: clock(), rttMs: nil, error: err.code)
        } catch {
            failed += 1
            sample = LatencySample(at: clock(), rttMs: nil, error: "\(error)")
        }
        samples.append(sample)
        onSample?(sample)
    }

    return (summarize(ok, failed: failed), samples)
}

/// A one-line human-readable summary.
public func formatLatencyStats(_ stats: LatencyStats) -> String {
    if stats.count == 0 { return "no successful samples (\(stats.failed) failed)" }
    func ms(_ n: Double) -> String { String(format: "%.1fms", n) }
    let tail = stats.failed > 0 ? " · \(stats.failed) failed" : ""
    return
        "n=\(stats.count) · min \(ms(stats.min)) · p50 \(ms(stats.p50)) · "
        + "p95 \(ms(stats.p95)) · max \(ms(stats.max))\(tail)"
}
