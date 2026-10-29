// 01 — Quickstart: join a stage and complete the first acked round-trip.
//
//   cd swift && swift run Quickstart "cst_..."
//   # or: CONNECT_TOKEN=cst_... swift run Quickstart
//
// Get the credential from the stage: Share → Adapter keys → Create key. Verify in the
// stage UI: the Console shows `hello.world` (Kind adapter) and Backstage shows a
// "Swift Quickstart" node pulsing on every send.

import OmniAdapter
import Foundation

@main
enum Quickstart {
    static func main() async {
        let credential = CommandLine.arguments.dropFirst().first
            ?? ProcessInfo.processInfo.environment["CONNECT_TOKEN"]
        guard let credential, !credential.isEmpty else {
            FileHandle.standardError.write(Data("usage: swift run Quickstart \"cst_…\"\n".utf8))
            exit(1)
        }

        let client: ConnectClient
        do {
            client = try await connect(
                ConnectOptions(
                    secretKey: credential,
                    displayName: "Swift Quickstart",
                    osHostname: ProcessInfo.processInfo.hostName,
                    sends: ["hello.world"], // Backstage 포트 칩 + 정확한 흐름 점등
                    nodeType: "service"
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
        print("✔ joined stage as \(me?.displayName ?? "?") (\(me?.participantId ?? "?"))")

        await client.on("*") { message in
            print("← \(message.channel) = \(message.value.jsonValue) (from \(message.sourceParticipantId))")
        }

        do {
            try await client.send("hello.world", "Hello from the Swift adapter!", ack: true)
            print("✔ server acked hello.world — first round-trip complete")
        } catch {
            print("✖ send failed \(error)")
        }
        print("  (send anything to this stage and it will print here — Ctrl+C to quit)")

        for await event in await client.events() {
            if case .stateChange(let state) = event { print("[state] \(state.rawValue)") }
            if case .ended(let reason) = event {
                print("○ stage session ended \(reason ?? "")")
                break
            }
        }
        await client.close()
    }
}
