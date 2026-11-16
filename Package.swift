// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "OmniAdapter",
    platforms: [
        // Socket.IO 클라이언트와 Swift Concurrency 백디플로이가 함께 서는 하한선.
        .iOS(.v13),
        .macOS(.v10_15),
        .tvOS(.v13),
        .watchOS(.v6),
    ],
    products: [
        .library(name: "OmniAdapter", targets: ["OmniAdapter"]),
    ],
    dependencies: [
        // v16 부터 Socket.IO v3/v4(EIO4) 를 말한다. Connect Omni 서버는 옛 EIO3
        // 클라이언트도 받지만, 이 어댑터는 현행 프로토콜로 붙는다.
        .package(url: "https://github.com/socketio/socket.io-client-swift", from: "16.1.0"),
    ],
    targets: [
        .target(
            name: "OmniAdapter",
            dependencies: [
                .product(name: "SocketIO", package: "socket.io-client-swift"),
            ]
        ),
        .testTarget(
            name: "OmniAdapterTests",
            dependencies: ["OmniAdapter"]
        ),
        // 예제도 빌드 게이트에 포함 — README 의 코드가 API 드리프트로 썩는 것을 막는다.
        .executableTarget(
            name: "Quickstart",
            dependencies: ["OmniAdapter"],
            path: "Examples/Quickstart"
        ),
        // 6언어 공통 시나리오 2 — 양방향 + 로스터.
        .executableTarget(
            name: "RosterBridge",
            dependencies: ["OmniAdapter"],
            path: "Examples/RosterBridge"
        ),
        // 6언어 공통 시나리오 3 — 재연결·오프라인 큐·자동 재조인.
        .executableTarget(
            name: "Resilience",
            dependencies: ["OmniAdapter"],
            path: "Examples/Resilience"
        ),
    ],
    // 하한은 Swift 6.0(Xcode 16)이다 — CI 러너(macos-15)에서 검증할 수 있는 가장 낮은 판이다. 언어 모드는 5 로 둔다:
    // 6 모드의 엄격한 동시성 검사는 공개 API 를 바꾸는 별도 작업이다.
    swiftLanguageModes: [.v5]
)
