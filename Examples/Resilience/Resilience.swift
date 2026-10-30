// 03 — Resilience: 네트워크가 끊기고 스테이지가 닫혀도 살아 있는 어댑터.
//
//   cd swift && swift run Resilience "cst_..."
//   # or: CONNECT_TOKEN=cst_... swift run Resilience
//
// 여섯 언어 공통 시나리오 3번 — 운영에서 가장 자주 묻는 것을 한 파일에 모았다.
//
// 직접 해볼 것(둘 다 로그로 보인다):
//   1. **Wi-Fi 를 몇 초 끊는다** → 상태가 `reconnecting` 이 되고, 그 사이 발행은
//      오프라인 큐에 쌓이며, 재접속 시 **순서대로** 흘러나간다.
//   2. **호스트가 스테이지를 닫는다** → 어댑터는 죽지 않는다(`autoRejoin` 기본 켬).
//      다시 열면 새 guest JWT 로 재합류한다. 자격증명이 폐기됐다면 즉시 끝난다.
//
// 알아둘 것:
//   - 큐 상한은 1000 이고 넘치면 **가장 오래된 것**이 버려진다(`queueOverflow`).
//   - history 는 재접속마다 **통째로 다시 시드**된다(`historyReplaced`) — 저장소가 아니다.
//   - 진단 링(100개)은 앱이 아무것도 구독하지 않아도 채워진다. 사고 후 읽으면 된다.

import OmniAdapter
import Foundation

@main
enum Resilience {
    static func main() async {
        let credential = CommandLine.arguments.dropFirst().first
            ?? ProcessInfo.processInfo.environment["CONNECT_TOKEN"]
        guard let credential, !credential.isEmpty else {
            FileHandle.standardError.write(Data("usage: swift run Resilience \"cst_…\"\n".utf8))
            exit(1)
        }

        let client: ConnectClient
        do {
            client = try await connect(
                ConnectOptions(
                    secretKey: credential,
                    displayName: "Swift Resilience",
                    osHostname: ProcessInfo.processInfo.hostName,
                    sends: ["heartbeat.tick"]
                    // autoRejoin 은 기본 true — 스테이지가 닫히면 같이 끝나야 하는
                    // 일회성 스크립트만 `options.autoRejoin = false` 로 끈다.
                )
            )
        } catch let error as AdapterError {
            print("✖ connect failed \(error)")
            if let hint = error.hint { print("  → \(hint)") }
            exit(1)
        } catch {
            print("✖ connect failed \(error)")
            exit(1)
        }

        let me = await client.me
        print("✔ joined as \(me?.displayName ?? "?") — Wi-Fi 를 끊거나 스테이지를 닫아보세요")

        // 심장박동 — 끊긴 동안에도 던지지 않는다. 큐에 쌓이고 재접속 때 순서대로 나간다.
        let beat = Task {
            var tick = 0
            while !Task.isCancelled {
                tick += 1
                do {
                    try await client.send("heartbeat.tick", .int(tick))
                } catch let error as AdapterError {
                    print("[send] refused: \(error.code)")
                } catch {
                    print("[send] refused: \(error)")
                }
                if tick % 5 == 0 {
                    let state = await client.state
                    let entries = client.diagnostics().count
                    print("  tick \(tick) · state=\(state.rawValue) · diagnostics=\(entries) entries")
                }
                try? await Task.sleep(nanoseconds: 1_000_000_000)
            }
        }

        for await event in await client.events() {
            switch event {
            case .stateChange(let state):
                print("[state] \(state.rawValue)")
            // 큐가 넘쳤다 = 데이터를 잃었다. 조용히 지나가면 원인을 못 찾는다.
            case .queueOverflow(let input):
                print("[queue] overflow — dropped oldest \(input.channel)")
            case .historyReplaced(let history):
                print("[history] reseeded with \(history.count) messages (재접속마다 일어난다)")
            case .failure(let error):
                print("[error] \(error.code): \(error.message)")
            case .ended(let reason):
                // 여기까지 오면 기다려도 안 되는 실패다(자격증명 폐기·버전 스큐 등).
                print("[ended] \(reason ?? "unknown")")
                beat.cancel()
                // 사고 후 진단 — 자격증명은 절대 담기지 않는다.
                print("last diagnostics:")
                for entry in client.diagnostics().suffix(5) {
                    print("   · \(entry.code) \(entry.message)")
                }
                await client.close()
                return
            default:
                break
            }
        }
        beat.cancel()
        await client.close()
    }
}
