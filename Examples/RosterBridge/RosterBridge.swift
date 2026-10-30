// 02 — Bidirectional + roster: receive a command, answer on another channel, and
// show who else is on the stage.
//
//   cd swift && swift run RosterBridge "cst_..."
//   # or: CONNECT_TOKEN=cst_... swift run RosterBridge
//
// 여섯 언어 공통 시나리오 2번 — 계약의 코어를 한 파일에 모았다.
//
// What to look for in the stage UI:
//   - Backstage 의 이 노드에 receives/sends 칩이 그려진다(선언을 실어 보내므로).
//   - Console 에서 `door.command` 로 open/close 를 보내면 `door.state` 가 되돌아온다.
//   - 참가자가 들어오고 나갈 때마다 로스터가 다시 찍힌다.
//
// 두 가지 함정이 여기 들어 있다:
//   - 서버는 **자기 발행을 되돌려주지 않는다**. 내 상태는 내가 들고 있어야 한다.
//   - roster 의 `standby` 는 **없음 ≠ false** 다 — 구 서버는 아예 안 보낸다.

import OmniAdapter
import Foundation

/// 문 상태를 들고 있는 액터 — 서버가 내 발행을 되돌려주지 않으므로 내가 소유한다.
actor DoorState {
    private var open = false

    /// 바뀌었으면 새 상태를, 그대로면 nil 을 돌려준다.
    func apply(_ wanted: Bool) -> String? {
        guard wanted != open else { return nil }
        open = wanted
        return label
    }

    var label: String { open ? "open" : "closed" }
}

@main
enum RosterBridge {
    static func main() async {
        let credential = CommandLine.arguments.dropFirst().first
            ?? ProcessInfo.processInfo.environment["CONNECT_TOKEN"]
        guard let credential, !credential.isEmpty else {
            FileHandle.standardError.write(Data("usage: swift run RosterBridge \"cst_…\"\n".utf8))
            exit(1)
        }

        let client: ConnectClient
        do {
            client = try await connect(
                ConnectOptions(
                    secretKey: credential,
                    displayName: "Swift Roster Bridge",
                    osHostname: ProcessInfo.processInfo.hostName,
                    // 선언 = Backstage 포트 칩 + 흐름 점등. 정확한 이름만(와일드카드 불가).
                    receives: ["door.command"],
                    sends: ["door.state"]
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
        print("✔ joined as \(me?.displayName ?? "?") (\(me?.participantId ?? "?"))")
        await printRoster(client)

        let door = DoorState()

        await client.on("door.command") { message in
            let wanted: Bool
            switch message.value {
            case .string("open"), .bool(true): wanted = true
            case .string("close"), .bool(false): wanted = false
            default:
                print("← ignoring unknown command \(message.value.jsonValue)")
                return
            }
            Task {
                guard let label = await door.apply(wanted) else {
                    print("← \(message.value.jsonValue) — already \(await door.label), nothing to do")
                    return
                }
                do {
                    // ack: true → 서버가 받았다는 확인까지 기다린다.
                    try await client.send("door.state", .string(label), ack: true)
                    print("→ door.state = \(label) (acked)")
                } catch {
                    print("✖ send failed \(error)")
                }
            }
        }

        // 현재 상태를 한 번 알린다 — 늦게 들어온 화면도 즉시 맞출 수 있게.
        try? await client.send("door.state", .string(await door.label))
        print("  Console 에서 door.command 로 open/close 를 보내보세요 — Ctrl+C 로 종료")

        for await event in await client.events() {
            switch event {
            case .stateChange(let state):
                print("[state] \(state.rawValue)")
            // 참가자 변화는 이벤트로만 알 수 있다 — roster 는 그때 다시 물어본다.
            case .memberJoin, .memberLeave:
                await printRoster(client)
            case .ended(let reason):
                print("○ stage session ended \(reason ?? "")")
                await client.close()
                return
            default:
                break
            }
        }
        await client.close()
    }

    private static func printRoster(_ client: ConnectClient) async {
        do {
            let members = try await client.members()
            print("  participants (\(members.count)):")
            for member in members {
                // standby 는 **명시적 true 만** 대기다 — 구 서버는 필드 자체를 안 보낸다.
                let standby = member.standby == true ? " [standby]" : ""
                let view = member.providesView.map { " provides:\($0)" } ?? ""
                print("   · \(member.displayName)\(standby)\(view)")
            }
        } catch {
            print("  (roster unavailable: \(error))")
        }
    }
}
