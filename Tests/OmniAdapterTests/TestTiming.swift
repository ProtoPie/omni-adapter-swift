import Foundation

// 시험의 시간 판정 규칙 — 벽시계 상한으로 "빨랐다"를 재지 않는다(C# 의 TestTiming.cs 와 같은 규칙).
//
// CI 실행기는 1초 넘게 멈출 수 있다(2026-11 릴리스 게이트: 평소 2.0초인 C# 닫기가 3.4초 · Swift 도 2026-10-25 에
// 50 ms 여유의 타이머 순서가 뒤집혔다). 평소 값에 수백 ms ~ 몇 초를 얹은 상한이나 1 ~ 5초짜리 조건 대기, "수십 ms 쉬면
// 비동기 처리가 끝났을 것"이라는 고정 대기는 제품이 맞아도 간헐 실패한다. 그래서:
//  · "곧바로 돌아온다 / 조건이 곧 참이 된다"는 hangGuard 안에 끝나는지만 본다. 결함일 때의 길(시한 · 상한 · 간격)은
//    hangGuard 보다 넉넉히 멀리(보통 60초) 둔다 — 결함이면 반드시 실패하고, 맞는 구현은 실행기가 20초 가까이 멈춰야
//    실패한다.
//  · "여기까지는 기다린다"는 하한만 잰다(느린 실행기는 경과를 늘릴 뿐 줄이지 않는다).
//  · 조건 대기 도우미(waitFor · waitUntil · until …)는 넘겨받은 시한이 더 짧아도 hangGuard 까지 기다린다 — 조건이 참이
//    되면 곧바로 돌아오므로 통과하는 시험은 느려지지 않는다.
//  · 고정 대기 뒤에 긍정 결과를 단언하지 않는다 — 그 결과를 조건으로 기다린다. "참이 되지 않는다 / 아무 일도 없다"를
//    보려는 자리에는 조건 대기를 쓰지 않는다(그런 자리는 고정 대기 뒤에 값을 본다 — 느려도 거짓 실패가 없다).
//  · 주입한 가짜 시계로 도는 시험은 이 규칙의 대상이 아니다.
enum TestTiming {
    /// 조건 대기가 기다리는 최소 시한(초).
    static let hangGuard: TimeInterval = 20

    /// 결함일 때의 길(시한 · 상한 · 간격)에 주는 값(초) — hangGuard 보다 넉넉히 멀다.
    static let farAway: TimeInterval = 60

    static func atLeastHangGuard(_ timeout: TimeInterval) -> TimeInterval { max(timeout, hangGuard) }

    /// 조건 대기의 마감 시각 — 넘겨받은 시한이 더 짧아도 hangGuard 뒤다.
    static func deadline(_ timeout: TimeInterval = 0) -> Date {
        Date().addingTimeInterval(atLeastHangGuard(timeout))
    }

    /// 조건이 참이 될 때까지 기다린다(최소 hangGuard). 참이 되면 곧바로 true, 끝내 안 되면 false.
    @discardableResult
    static func until(
        _ timeout: TimeInterval = 0, pollNanos: UInt64 = 2_000_000, _ condition: () async -> Bool
    ) async -> Bool {
        let end = deadline(timeout)
        while Date() < end {
            if await condition() { return true }
            try? await Task.sleep(nanoseconds: pollNanos)
        }
        return await condition()
    }
}
