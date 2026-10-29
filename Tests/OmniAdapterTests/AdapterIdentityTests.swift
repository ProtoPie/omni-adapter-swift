import Foundation
import XCTest

@testable import OmniAdapter

/// 어댑터 신원(ADR-0016 · D4) — 판 상수가 CHANGELOG 의 맨 위 판과 같은가, 런타임 판별이 맞게 고르는가.
/// 정규화 두 함수의 입력별 답은 여섯 언어 공통 벡터(`docs/conformance.json`)가 지킨다(ConformanceTests).
final class AdapterIdentityTests: XCTestCase {
    func testAdapterVersionMatchesTheTopChangelogHeading() throws {
        // SwiftPM 에는 매니페스트 판이 없다 — CHANGELOG 의 맨 위 판 제목이 정본이다(release-swift.yml 이 태그와 견준다).
        let changelog = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
            .appendingPathComponent("CHANGELOG.md")
        let text = try String(contentsOf: changelog, encoding: .utf8)
        let heading = try NSRegularExpression(pattern: "^## ([0-9][^ \\n]*)", options: [.anchorsMatchLines])
        guard let m = heading.firstMatch(in: text, range: NSRange(text.startIndex..., in: text)),
              let range = Range(m.range(at: 1), in: text)
        else { return XCTFail("CHANGELOG.md 에서 판 제목(## X.Y.Z)을 찾지 못했다") }
        XCTAssertEqual(AdapterIdentity.adapterVersion, String(text[range]))
        XCTAssertEqual(AdapterIdentity.normalizeAdapterVersion(AdapterIdentity.adapterVersion), String(text[range]))
    }

    func testAppleWithTheOsVersion() {
        let v = OperatingSystemVersion(majorVersion: 17, minorVersion: 4, patchVersion: 1)
        XCTAssertEqual(AdapterIdentity.detectRuntime(osVersion: v), ["runtime": "apple", "runtimeVersion": "17.4.1"])
    }

    func testUnknownPlatformSendsNoRuntime() {
        // Linux 등 — 서버 어휘에 없는 런타임을 보내지 않는다.
        XCTAssertEqual(AdapterIdentity.detectRuntime(osVersion: nil), [:])
        XCTAssertEqual(AdapterIdentity.fields(osVersion: nil, adapterVersion: "0.1.0"), ["adapter": "swift", "adapterVersion": "0.1.0"])
    }

    func testFieldsOfThisProcess() {
        let fields = AdapterIdentity.fields()
        XCTAssertEqual(fields["adapter"], "swift")
        XCTAssertEqual(fields["adapterVersion"], AdapterIdentity.adapterVersion)
        XCTAssertEqual(fields["runtime"], "apple")
        // 판을 모르면 그 필드만 빠진다.
        XCTAssertNil(AdapterIdentity.fields(adapterVersion: nil)["adapterVersion"])
    }
}
