import Foundation

// (`docs/conformance.json` 규칙 5)
//
// 왜 플랫폼 파서에 맡기지 않는가: 관대함이 언어마다 달라 같은 입력이 갈렸다. `JSONSerialization` 은 중복
// 키에서 **앞의** 값을 쓰고(TS·Python·Dart 는 뒤의 값, Kotlin 은 예외), 끝 쉼표(`{"a":1,}` · `[1,]`)와 선두
// BOM 을 받고, 배열·객체를 64 단 너머까지(513 단도) 읽었다(실측).
//
// 규칙(여섯 언어 공통 — 하나라도 바꾸면 여섯 구현과 벡터를 같이 바꾼다).
//
// 다른 언어는 판정을 통과한 텍스트를 플랫폼 파서로 읽지만, Swift 는 **값도 여기서 만든다**(``parse(_:)``).
// `JSONSerialization` 은 판정을 통과한 텍스트에서도 TS 와 갈렸다(실측):
//   - 문자열 값·키 머리의 U+FEFF 하나를 벗긴다(이스케이프 `\ufeff` 도) — `"k":"\ufeffck_x"` 가 어댑터 키로
//     통과하고, `"u":"\ufeff"` 는 빈 값이 되고, 키 `"\ufeffv"` 는 `"v"` 와 겹쳐 앞의 값이 이긴다.
//   - 유한한 수 일부를 못 읽는다(`0e99999` · `1e-99999` · 309자리 정수).
//   - 긴 소수를 `NSDecimalNumber` 로 읽어 다른 double 을 낸다(`0.0021367390602654926` → `0.002136739060265493`).
//
// Swift 에서는 `Character` 로 판정하지 않는다 — `"\r\n"` 이 `Character` **하나**다. TS 와 똑같이 UTF-16 코드
// 유닛을 훑고, 키도 코드 유닛 배열로 비교한다(`String` 의 `==` 는 정규 등가라 `"é"` 와 `"e\u{301}"` 을 같은
// 키로 본다 — TS 는 다른 키다). Swift 문자열에는 짝 없는 서로게이트가 없어 날 서로게이트 규칙이 걸릴 일은
// 없지만, 구조는 TS 와 같게 둔다.
/// Strict RFC 8259 check — every adapter library screens JSON text with **the same rules** before parsing it.
///
/// Rules:
///   - Whitespace is exactly four characters: space · \t · \n · \r.
///   - Strings: no raw control characters (U+0000–U+001F) · escapes are `\" \\ \/ \b \f \n \r \t \uXXXX` (ASCII hex) ·
///     a high surrogate must be followed immediately by a low surrogate of the same form (both escaped or both
///     raw), and an unpaired low surrogate is rejected too.
///   - Numbers: `-? (0 | [1-9][0-9]*) (.[0-9]+)? ([eE][+-]?[0-9]+)?`, and the value must be **finite** as an IEEE double.
///   - Duplicate keys in one object are rejected — compared by the unescaped name (`"v"` and `"\u0076"` are the same key).
///   - Array and object nesting up to 64, counting the top level as 1.
///   - Only whitespace may follow the value.
///
/// In Swift this type also **builds the value** (``parse(_:)``). It scans UTF-16 code units (not `Character`s, where
/// `"\r\n"` is a single `Character`) and compares keys as code-unit arrays.
enum StrictJSON {
    /// The nesting limit for arrays and objects — the top level counts as 1.
    static let maxDepth = 64

    /// Whether one code unit is JSON whitespace — the grammar's whitespace, and the only set this package calls "whitespace".
    static func isWhitespace(_ unit: UTF16.CodeUnit) -> Bool {
        unit == 0x20 || unit == 0x09 || unit == 0x0A || unit == 0x0D
    }

    /// Whether one scalar is JSON whitespace — all four are ASCII, so this matches the code-unit check.
    static func isWhitespace(_ scalar: Unicode.Scalar) -> Bool {
        scalar.isASCII && isWhitespace(UTF16.CodeUnit(scalar.value))
    }

    /// Whether the text is empty or JSON whitespace only — the platform's definition (`.whitespacesAndNewlines` counts NBSP as whitespace) is not used.
    static func isBlank(_ s: String) -> Bool {
        s.utf16.allSatisfy { isWhitespace($0) }
    }

    /// Strips only leading and trailing JSON whitespace (NBSP, BOM, vertical tab and the like remain). Cuts by scalar —
    /// cutting by `Character` would fail to strip a space followed by a combining character (" \u{301}").
    static func trim(_ s: String) -> String {
        let scalars = s.unicodeScalars
        var start = scalars.startIndex
        var end = scalars.endIndex
        while start < end, isWhitespace(scalars[start]) { start = scalars.index(after: start) }
        while end > start, isWhitespace(scalars[scalars.index(before: end)]) { end = scalars.index(before: end) }
        return String(scalars[start..<end])
    }

    /// Whether the whole text is a single JSON value that follows the rules above.
    static func isValid(_ text: String) -> Bool { parse(text) != nil }

    /// The value when the text passes the check, otherwise nil. The value has the same shapes as `JSONSerialization`
    /// (`String`, `NSNumber`, `NSNull`, `[Any]`, `[String: Any]`; booleans are `NSNumber`s that are CFBoolean) — reading
    /// code uses it as is. Numbers are read with `Double(_:)` (correctly rounded), strings unescaped code unit for code unit.
    static func parse(_ text: String) -> Any? {
        var scanner = StrictJSONScanner(Array(text.utf16))
        do {
            scanner.skipWhitespace()
            let value = try scanner.value(depth: 0)
            scanner.skipWhitespace()
            return scanner.atEnd ? value : nil
        } catch {
            return nil
        }
    }
}

/// A check failure — never leaves the scanner (`parse` folds it into nil).
private struct InvalidJSON: Error {}

// TS `StrictJsonScanner` 의 이식.
/// A recursive descent over a UTF-16 code-unit array. The recursion depth is bounded by the nesting limit.
private struct StrictJSONScanner {
    private let s: [UTF16.CodeUnit]
    private var pos = 0

    init(_ units: [UTF16.CodeUnit]) { s = units }

    var atEnd: Bool { pos == s.count }

    /// The code unit at `at` — -1 past the end.
    private func code(_ at: Int) -> Int { at < s.count ? Int(s[at]) : -1 }

    private var current: Int { code(pos) }

    mutating func skipWhitespace() {
        while pos < s.count, StrictJSON.isWhitespace(s[pos]) { pos += 1 }
    }

    /// `depth` = the number of arrays and objects enclosing this value.
    mutating func value(depth: Int) throws -> Any {
        switch current {
        case ASCIICode.openBrace: return try object(depth: depth + 1)
        case ASCIICode.openBracket: return try array(depth: depth + 1)
        case ASCIICode.quote: return String(decoding: try string(), as: UTF16.self)
        case ASCIICode.t:
            try literal("true")
            return NSNumber(value: true)
        case ASCIICode.f:
            try literal("false")
            return NSNumber(value: false)
        case ASCIICode.n:
            try literal("null")
            return NSNull()
        case ASCIICode.minus, ASCIICode.zero...ASCIICode.nine: return try number()
        default: throw InvalidJSON()
        }
    }

    private mutating func literal(_ word: String) throws {
        for unit in word.utf16 {
            if current != Int(unit) { throw InvalidJSON() }
            pos += 1
        }
    }

    private mutating func object(depth: Int) throws -> [String: Any] {
        if depth > StrictJSON.maxDepth { throw InvalidJSON() }
        pos += 1 // {
        skipWhitespace()
        var members: [String: Any] = [:]
        if current == ASCIICode.closeBrace {
            pos += 1
            return members
        }
        var keys = Set<[UTF16.CodeUnit]>()
        while true {
            skipWhitespace()
            if current != ASCIICode.quote { throw InvalidJSON() }
            // 이스케이프를 푼 코드 유닛으로 중복을 본다. 코드 유닛이 다르고 정규 등가인 두 키(`"é"` · `"e\u{301}"`)는
            // 판정을 통과하지만 Swift 딕셔너리에서는 한 키가 된다(뒤의 값) — 이 패키지가 읽는 키는 전부 ASCII 라
            // 결과가 달라지지 않는다.
            let key = try string()
            if !keys.insert(key).inserted { throw InvalidJSON() }
            skipWhitespace()
            if current != ASCIICode.colon { throw InvalidJSON() }
            pos += 1
            skipWhitespace()
            members[String(decoding: key, as: UTF16.self)] = try value(depth: depth)
            skipWhitespace()
            let separator = current
            pos += 1
            if separator == ASCIICode.closeBrace { return members }
            if separator != ASCIICode.comma { throw InvalidJSON() }
        }
    }

    private mutating func array(depth: Int) throws -> [Any] {
        if depth > StrictJSON.maxDepth { throw InvalidJSON() }
        pos += 1 // [
        skipWhitespace()
        var items: [Any] = []
        if current == ASCIICode.closeBracket {
            pos += 1
            return items
        }
        while true {
            skipWhitespace()
            items.append(try value(depth: depth))
            skipWhitespace()
            let separator = current
            pos += 1
            if separator == ASCIICode.closeBracket { return items }
            if separator != ASCIICode.comma { throw InvalidJSON() }
        }
    }

    /// The value of the `\uXXXX` starting at `at`. -1 when absent or not ASCII hex.
    private func unicodeEscape(at: Int) -> Int {
        guard code(at) == ASCIICode.backslash, code(at + 1) == ASCIICode.u else { return -1 }
        var value = 0
        for i in at + 2 ..< at + 6 {
            guard let digit = hexDigitValue(code(i)) else { return -1 }
            value = value * 16 + digit
        }
        return value
    }

    /// Checks one string literal and returns its unescaped code units.
    private mutating func string() throws -> [UTF16.CodeUnit] {
        let start = pos
        pos += 1 // 여는 따옴표
        while true {
            let c = current
            if c == -1 { throw InvalidJSON() }
            if c == ASCIICode.quote {
                pos += 1
                return decodedString(from: start, to: pos)
            }
            if c < 0x20 { throw InvalidJSON() }
            if c == ASCIICode.backslash {
                let escaped = code(pos + 1)
                if escaped == ASCIICode.u {
                    let v = unicodeEscape(at: pos)
                    if v == -1 { throw InvalidJSON() }
                    pos += 6
                    if isLowSurrogate(v) { throw InvalidJSON() } // 짝 없는 하위
                    if isHighSurrogate(v) {
                        // 상위 뒤에는 이스케이프된 하위가 와야 한다
                        if !isLowSurrogate(unicodeEscape(at: pos)) { throw InvalidJSON() }
                        pos += 6
                    }
                    continue
                }
                if !StrictJSONScanner.simpleEscapes.contains(escaped) { throw InvalidJSON() }
                pos += 2
                continue
            }
            if isLowSurrogate(c) { throw InvalidJSON() }
            if isHighSurrogate(c) {
                if !isLowSurrogate(code(pos + 1)) { throw InvalidJSON() } // 날 상위 뒤에는 날 하위
                pos += 2
                continue
            }
            pos += 1
        }
    }

    /// The single-character escapes allowed after `\` — `" \ / b f n r t`.
    private static let simpleEscapes: Set<Int> = [
        ASCIICode.quote, ASCIICode.backslash, ASCIICode.slash,
        ASCIICode.b, ASCIICode.f, ASCIICode.n, ASCIICode.r, ASCIICode.t,
    ]

    /// The unescaped code units of the checked string literal `s[start..<end]` (quotes included) — an escaped
    /// surrogate pair becomes the same two units as the raw characters, so it is caught as the same key.
    private func decodedString(from start: Int, to end: Int) -> [UTF16.CodeUnit] {
        var out: [UTF16.CodeUnit] = []
        var i = start + 1
        while i < end - 1 {
            guard code(i) == ASCIICode.backslash else {
                out.append(s[i])
                i += 1
                continue
            }
            let escaped = code(i + 1)
            switch escaped {
            case ASCIICode.u:
                out.append(UTF16.CodeUnit(unicodeEscape(at: i)))
                i += 6
                continue
            case ASCIICode.b: out.append(0x08)
            case ASCIICode.f: out.append(0x0C)
            case ASCIICode.n: out.append(0x0A)
            case ASCIICode.r: out.append(0x0D)
            case ASCIICode.t: out.append(0x09)
            default: out.append(UTF16.CodeUnit(escaped)) // " \ /
            }
            i += 2
        }
        return out
    }

    private mutating func number() throws -> NSNumber {
        let start = pos
        if current == ASCIICode.minus { pos += 1 }
        if current == ASCIICode.zero {
            pos += 1
        } else if isDigit(current) {
            while isDigit(current) { pos += 1 }
        } else {
            throw InvalidJSON()
        }
        if current == ASCIICode.dot {
            pos += 1
            if !isDigit(current) { throw InvalidJSON() }
            while isDigit(current) { pos += 1 }
        }
        if current == ASCIICode.e || current == ASCIICode.upperE {
            pos += 1
            if current == ASCIICode.plus || current == ASCIICode.minus { pos += 1 }
            if !isDigit(current) { throw InvalidJSON() }
            while isDigit(current) { pos += 1 }
        }
        // 문법이 맞아도 double 로 ∞ 가 되는 수(1e400 · 401자리 정수)는 거부한다 — 언어마다 ∞ · 오류 ·
        // 정확한 큰 정수(Python)로 갈렸다. `Double(_:)` 는 넘치면 nil 이 아니라 ±inf 를 준다(실측) — 그래서
        // 유한한지를 따로 본다(nil 도 거부).
        guard let parsed = Double(String(decoding: s[start..<pos], as: UTF16.self)), parsed.isFinite else {
            throw InvalidJSON()
        }
        return NSNumber(value: parsed)
    }
}

/// ASCII codes the scanner compares — code units are handled as `Int` (end = -1).
private enum ASCIICode {
    static let quote = Int(UInt8(ascii: "\""))
    static let plus = Int(UInt8(ascii: "+"))
    static let comma = Int(UInt8(ascii: ","))
    static let minus = Int(UInt8(ascii: "-"))
    static let dot = Int(UInt8(ascii: "."))
    static let slash = Int(UInt8(ascii: "/"))
    static let zero = Int(UInt8(ascii: "0"))
    static let nine = Int(UInt8(ascii: "9"))
    static let colon = Int(UInt8(ascii: ":"))
    static let upperE = Int(UInt8(ascii: "E"))
    static let openBracket = Int(UInt8(ascii: "["))
    static let backslash = Int(UInt8(ascii: "\\"))
    static let closeBracket = Int(UInt8(ascii: "]"))
    static let b = Int(UInt8(ascii: "b"))
    static let e = Int(UInt8(ascii: "e"))
    static let f = Int(UInt8(ascii: "f"))
    static let n = Int(UInt8(ascii: "n"))
    static let r = Int(UInt8(ascii: "r"))
    static let t = Int(UInt8(ascii: "t"))
    static let u = Int(UInt8(ascii: "u"))
    static let openBrace = Int(UInt8(ascii: "{"))
    static let closeBrace = Int(UInt8(ascii: "}"))
}

private func isDigit(_ c: Int) -> Bool { c >= ASCIICode.zero && c <= ASCIICode.nine }
private func isHighSurrogate(_ c: Int) -> Bool { c >= 0xD800 && c <= 0xDBFF }
private func isLowSurrogate(_ c: Int) -> Bool { c >= 0xDC00 && c <= 0xDFFF }

/// The value of one ASCII hex character — only `0-9 A-F a-f` (digits of other scripts are not accepted). Otherwise nil.
private func hexDigitValue(_ c: Int) -> Int? {
    switch c {
    case 0x30...0x39: return c - 0x30
    case 0x41...0x46: return c - 0x41 + 10
    case 0x61...0x66: return c - 0x61 + 10
    default: return nil
    }
}
