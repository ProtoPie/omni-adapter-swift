import XCTest
@testable import OmniAdapter

/// Swift 고유 함정 — 공유 벡터(`docs/conformance.json`)에 적히지 않은 입력에서 `JSONSerialization` 과 `Character`
/// 단위 API 가 TS 와 갈렸던 자리(실측). 갈림이 이 언어의 플랫폼 API 에만 있어 벡터가 아니라 여기 둔다.
final class StrictJSONTests: XCTestCase {
    /// JSON 이스케이프 `\ufeff` · `\ud83d\ude00` (raw 문자열 — 역슬래시가 그대로 JSON 에 들어간다).
    private let bom = #"\ufeff"#
    private let grinning = #"\ud83d\ude00"#

    private func token(_ json: String) -> String {
        "cst_" + Data(json.utf8).base64EncodedString()
            .replacingOccurrences(of: "+", with: "-").replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    /// `JSONSerialization` 은 문자열 머리의 U+FEFF 하나를 벗겼다 — 키에서도 값에서도, 이스케이프여도.
    func testLeadingBOMInsideAStringIsPartOfTheString() throws {
        let named = try StageToken.parse(token(#"{"v":1,"u":"http://h","s":"s","k":"ck_x","n":"\#(bom)Kiosk"}"#))
        XCTAssertEqual(named.displayName, "\u{FEFF}Kiosk")
        // BOM 으로 시작하는 k 는 어댑터 키가 아니다 — 벗겨지면 "ck_x" 로 둔갑해 통과했다(TS 는 거부).
        XCTAssertThrowsError(try StageToken.parse(token(#"{"v":1,"u":"http://h","s":"s","k":"\#(bom)ck_x"}"#))) {
            XCTAssertTrue("\($0)".contains("does not carry an adapter key"), "\($0)")
        }
        // BOM 하나인 u 는 비어 있지 않다(JSON 공백이 아니다) — 벗겨지면 "empty url/stageId" 였다.
        XCTAssertEqual(try StageToken.parse(token(#"{"v":1,"u":"\#(bom)","s":"s","k":"ck_x"}"#)).url, "\u{FEFF}")
        // 키 "\ufeffv" 는 "v" 와 다른 키다 — 벗겨지면 둘이 겹쳐 앞의 값(2)이 이겼다.
        XCTAssertNoThrow(try StageToken.parse(token(#"{"\#(bom)v":2,"v":1,"u":"http://h","s":"s","k":"ck_x"}"#)))

        let rec = try parseRecording(#"{"ts":1,"channel":"\#(bom)a","value":"\#(bom)"}"#)
        XCTAssertEqual(rec.first?.channel, "\u{FEFF}a")
        XCTAssertEqual(rec.first?.value, .string("\u{FEFF}"))
    }

    /// `JSONSerialization` 이 못 읽거나(0e99999 · 1e-99999 · 309자리 정수) 다른 double 로 읽던(NSDecimalNumber)
    /// 유한한 수 — TS `Number()` 와 같은 값이어야 한다.
    func testFiniteNumbersReadLikeTypeScript() throws {
        func value(_ literal: String) throws -> Primitive? {
            try parseRecording(#"{"ts":1,"channel":"a","value":\#(literal)}"#).first?.value
        }
        XCTAssertEqual(try value("0.0021367390602654926"), .double(0.0021367390602654926))
        XCTAssertEqual(try value("0e99999"), .int(0))
        XCTAssertEqual(try value("1e-99999"), .int(0))
        XCTAssertEqual(try value("1" + String(repeating: "0", count: 308)), .double(1e308))
        XCTAssertNoThrow(try StageToken.parse(token(#"{"v":1,"u":"http://h","s":"s","k":"ck_x","x":0e99999}"#)))
    }

    /// 중복 키는 이스케이프를 푼 **코드 유닛**으로 본다 — `String` 의 `==` 는 정규 등가라 `"é"` 와 `"e\u{301}"` 이
    /// 겹친다(TS 는 다른 키다).
    func testDuplicateKeysCompareDecodedCodeUnits() {
        XCTAssertTrue(StrictJSON.isValid("{\"\u{E9}\":1,\"e\u{301}\":2}"))
        XCTAssertFalse(StrictJSON.isValid("{\"\u{E9}\":1,\"\u{E9}\":2}"))
        XCTAssertFalse(StrictJSON.isValid(#"{"\#(grinning)":1,"😀":2}"#)) // 이스케이프된 쌍 = 날 글자
    }

    /// `hasPrefix` · `dropFirst` 는 글자(grapheme) 단위라 프리픽스 끝에 붙은 결합 문자를 앞 글자로 센다.
    func testPrefixAndTrimWorkOnScalarsNotGraphemes() {
        // TS 처럼 프리픽스는 인정하고 본문에서 거부한다("not a stage token" 이 아니라).
        XCTAssertTrue(StageToken.isStageToken("cst_\u{301}AAAA"))
        XCTAssertThrowsError(try StageToken.parse("cst_\u{301}AAAA")) {
            XCTAssertTrue("\($0)".contains("could not decode"), "\($0)")
        }
        XCTAssertTrue(StageToken.isAdapterKey("ck_\u{301}x"))
        // 공백 + 결합 문자는 Character 하나지만, 벗기는 것은 공백뿐이다.
        XCTAssertEqual(StrictJSON.trim(" \u{301}x\r\n").unicodeScalars.map(\.value), [0x301, 0x78])
    }
}
