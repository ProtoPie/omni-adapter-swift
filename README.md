# OmniAdapter (Swift)

Join a **Connect Omni** stage from Swift — iOS/iPadOS apps, macOS tools,
tvOS installations — and exchange messages with prototypes in real time.
Mirrors the TypeScript/Python/.NET/Kotlin/Dart adapters' wire contract and concepts on
an async/await surface.

**First time?** [Connect your first program to a stage](https://github.com/ProtoPie/omni-adapter/blob/main/docs/getting-started.md) walks
through the adapter key, the install and the adapter name step by step.

**For AI assistants** — the full usage guide `llms.txt` sits at the root of the main repository ([`llms.txt`](https://github.com/ProtoPie/omni-adapter/blob/main/llms.txt));
the Claude Code skill installs with `/plugin marketplace add ProtoPie/omni-adapter` then
`/plugin install omni-adapter@studioxid` (or copy
[`skills/using-omni-adapter/`](https://github.com/ProtoPie/omni-adapter/tree/main/skills/using-omni-adapter) into `.claude/skills/`). Must-know rules for Swift:

- **Credential**: only a `cst_…` adapter token (Share → Adapter keys → Create key, then Copy) or a bare `ck_…` key with
  `url` + `stageId`; read it from an environment variable (`ProcessInfo.processInfo.environment["CONNECT_TOKEN"]`),
  never hard-code it. Whitespace around it → `ConfigError`.
- **Close on exit**: `await client.close()` on SIGINT/SIGTERM, or when your app is done with the stage — it leaves the
  stage (waits ≤1 s for the leave ack). An unclean exit holds the seat a few seconds; the same key coming back keeps
  its name.
- **Lifecycle events**: `for await event in await client.events()` with the cases `.stateChange`, `.failure`,
  `.memberJoin`, `.memberLeave`, `.historyReplaced`, `.ended`, `.queueOverflow` — **not** `client.on("error")`, which
  subscribes to a stage *channel* named `error`. Callbacks are synchronous and run off the main actor — hop to UI with
  `Task { @MainActor in … }`.
- **`latest()` is not a delivery check**: it folds your own sends in. To confirm a publish use `ack: true` (a server
  ack, not a delivery receipt) or watch the channel from a second connection.
- **`exclusiveName` standby**: only instances on the same adapter key; while standby new sends are dropped (or,
  acked, fail with `Standby`), and becoming standby drops whatever was queued — nothing stale goes out on promotion.
  Read `await client.standby`.
- **60 messages a second per connection**: the adapter paces sends to the server's limit; a faster source fills the
  queue and loses its oldest values — `throttle()` it. `close()` drops queued fire-and-forget sends.
- **Values are an enum**: `print(message.value)` shows `string("go")` — read `.stringValue` / `.doubleValue` /
  `.boolValue` (or print `.jsonValue`); a literal converts itself (`send("ch", 23.5)`), a variable needs `.double(x)`.

```swift
import OmniAdapter
import Foundation

// One credential is all you need (stage → Share → Adapter keys → Create key, then Copy):
let client = try await connect(
    ConnectOptions(
        secretKey: ProcessInfo.processInfo.environment["CONNECT_TOKEN"], // "cst_…"
        displayName: "iPad Wall",
        receives: ["actuator.cmd"],          // declared → Backstage port chips
        sends: ["sensor.temperature"],
        nodeType: "display"                  // web|service|hardware|sensor|controller|display
    )
)

await client.on("actuator.cmd") { message in
    print("prototype asked for \(message.value.jsonValue)")   // not \(message.value) — that prints string("…")
}
try await client.send("sensor.temperature", 23.5)
try await client.send("door.open", true, ack: true)   // wait for the server ack
await client.close()                                  // when you are done — leaves the stage
```

## Install

The package is served from the public mirror repository
[`ProtoPie/omni-adapter-swift`](https://github.com/ProtoPie/omni-adapter-swift): every
release copies this `swift/` folder to the mirror's root and tags it `X.Y.Z`, which is what
SwiftPM resolves.

```swift
// swift-tools-version:5.9
// Package.swift
import PackageDescription

let package = Package(
    name: "MyAdapter",
    platforms: [.macOS(.v10_15), .iOS(.v13)],   // required — without it SwiftPM refuses the dependency
    dependencies: [
        .package(url: "https://github.com/ProtoPie/omni-adapter-swift", from: "0.1.0-beta.0"),
    ],
    targets: [
        .executableTarget(
            name: "MyAdapter",
            // the identity of a URL dependency is the repository name
            dependencies: [.product(name: "OmniAdapter", package: "omni-adapter-swift")]
        ),
    ]
)
```

In an Xcode app project: File → Add Package Dependencies… and enter the URL above.

Only beta versions are published so far, and SwiftPM never picks a prerelease unless the
requirement names one: `from: "0.1.0"` finds nothing, while `from: "0.1.0-beta.0"` takes the beta
and later 0.1.0 as well. In Xcode, set the dependency rule's lower bound to `0.1.0-beta.0` (or
choose Exact Version). Mirror tags carry no prefix — the version is the tag itself (`0.1.0-beta.0`).

Working from a checkout of the main repository instead (contributors, or a change that has
not reached the mirror yet)? Depend on its `swift/` folder by path —
`.package(path: "<repo>/swift")` with `.product(name: "OmniAdapter", package: "swift")`,
because the identity of a path dependency is the directory name — or, in Xcode, File → Add
Package Dependencies… → Add Local… and pick `<repo>/swift`.

Platforms: iOS 13+, macOS 10.15+, tvOS 13+, watchOS 6+ — a consumer package must declare
at least these in its own `platforms:` (SwiftPM's default deployment target is older). The
only dependency is [socket.io-client-swift](https://github.com/socketio/socket.io-client-swift)
(v16 = Socket.IO v3/v4).

Two app-only settings, both of which otherwise fail the join with `NotConnected`:

- **Sandboxed macOS app** — turn on *Outgoing Connections (Client)*
  (`com.apple.security.network.client`) in the App Sandbox entitlements.
- **Plain `http://` server addressed by host name** (a LAN deployment) — App Transport
  Security blocks the join's HTTP request (the error message names App Transport Security).
  Add an `NSAppTransportSecurity` exception for that host to the app's Info.plist. A
  command-line tool has no Info.plist and is not affected.

## Connecting

Two forms, same precedence as the other adapters. Both carry an adapter key
(`ck_…`) issued from **Stage → Share → Adapter keys**; anything else (a share
token, for instance) is rejected with `ConfigError` before any network call.

```swift
try await connect(secretKey: "cst_…")                                      // ① self-describing token (recommended)
try await connect(                                                         // ② raw key + explicit coordinates
    // Swift enforces the initializer's parameter order: secretKey comes first.
    ConnectOptions(secretKey: "ck_…", url: "http://192.168.0.10:9981", stageId: "01HXYZ")
)
```

Without `strictRouting`, `receives` may use wildcards (`robot.*`). `strictRouting: true`
requires exact `receives` names (no wildcards); the
server then delivers only those channels plus messages targeted at you.

Options that the initializer does not take are properties — set them before `connect`:

```swift
var options = ConnectOptions(secretKey: ProcessInfo.processInfo.environment["CONNECT_TOKEN"],
                             displayName: "Wall Sensor")
options.exclusiveName = true   // one active per name (below)
options.autoRejoin = true      // the default — wait for a closed session to reopen
options.ackTimeout = 15        // seconds an `ack: true` send waits (the default; finite, > 0, ≤ 2147483.647)
let client = try await connect(options)
```

**A screen** — set `options.providesView = "stream"` if this program broadcasts a screen
(`startScreenBroadcast`, below). The server then creates the **Adapter Stream** layer for your name
on the stage, or binds the one a designer already placed, as soon as you connect — nothing else to
call. `providesView = "web"` is for an Adapter View page; leave it unset otherwise.

## Names and closing

Instances on the same adapter key share the `displayName` — a restart is never renamed (this needs a
server with same-key name sharing, the Connect Omni server change shipping with this release; older servers
renamed a restart that came within a few seconds of an unclean exit). You join as `name (2)` only when
the name is held by another adapter key: another key's adapter is connected under it, or (with
`providesView = "stream"`) the stage's Adapter Stream layer of that name is bound to another key that
is still valid. Another program on a different key uses this name, or you rotated keys without
revoking the old one — rename one, or revoke the old key. A person never takes a program's name. An unclean exit (a crash, a kill, Ctrl+C
without `close()`) holds the seat for a few seconds; the same key coming back keeps its name (on a server with same-key name sharing — older servers renamed a restart that came within a few seconds of an unclean exit). Call
`close()` on exit: it ends the client at the call
(later sends throw `SessionEnded`, `.ended` arrives) and leaves the stage, waiting up to 1 s for
the server's leave ack. Each client carries its own guest cookie, so two clients in one app are
two participants. In a command-line tool:

```swift
var signalSources: [DispatchSourceSignal] = []
for sig in [SIGINT, SIGTERM] {
    signal(sig, SIG_IGN)                      // let the dispatch source handle it
    let source = DispatchSource.makeSignalSource(signal: sig, queue: .global())
    source.setEventHandler {
        Task {
            await client.close()              // leaves the stage
            exit(0)
        }
    }
    source.resume()
    signalSources.append(source)              // keep the sources alive
}
```

In an app, call it when the app is done with the stage (the window closes, the feature is
dismissed).

**`exclusiveName`** — for an adapter whose name is its contract (a screen layer binds by
name), `options.exclusiveName = true` makes the server keep the **newest** instance active
and mark the rest standby. Only instances on the **same adapter key** are judged together:
a copy with a different key joins as `name (2)` instead, and its `exclusiveName` is dropped.
A standby client keeps receiving and is promoted automatically when the active one leaves.
While standby, new sends are **dropped** (fire-and-forget, debug log only) or throw
`Standby` (`ack: true`) — never deferred. When an instance **becomes** standby — after a reconnect
or when presence changes — everything it had queued is dropped the same way (the `standby`
entry in `diagnostics()` ends with `— dropped N queued sends`), so a later promotion sends nothing
stale. After each (re)connect the adapter needs one roster round trip to judge standby; sends
made meanwhile are held (sharing `queueLimit`), then queued if this instance is active (or the
judgment failed) or treated by the standby rule. There is no standby-change event: read `await client.standby`, or watch the
diagnostics ring for the codes `standby` / `active` (`onLog`). Leave it off for crowds that
share a name on purpose (20 visitor phones at a booth).

## API sketch

`ConnectClient` is an `actor` — every call is `await`ed:

```swift
try await client.send("door.open", true)                     // fire-and-forget
try await client.send("door.open", true, ack: true)          // wait for the server ack
try await client.sendTo(participantId, "cmd.stop", 1)        // unicast
try await client.sendJSON("robot.status", ["pose": [1, 2]])

await client.on("button.tap") { message in }                 // exact channel
await client.on("robot.*") { message in }                    // wildcard — 1+ dot segments
await client.onJSON("robot.status") { value, message in }    // parsed JSON values
let sub = await client.on("chat.message") { _ in }           // sub.cancel() unsubscribes

await client.latest("temp.reading")                          // last value (history-seeded; 1000 most recent channels)
await client.latestJSON("robot.status")
await client.history()                                       // replay ring (≤1000, not storage)
await client.history("chat.*")                               // …filtered (wildcards ok)
await client.historyJSON("chat.message")
try await client.members()  // each Member also carries nodeType · receives · sends · clientType · strictRouting · exclusiveName · osHostname (nil on older servers); member.declaresChannel("robot.status")
try await client.listeners("robot.status")  // who will receive it: .declared / .broadcast (self excluded); records no-listener when nobody declared it

let push = await client.throttle("nav.position", hz: 10)     // coalesced sender (trailing flush)
try push(.double(37.5665))                                   // a bad channel throws ValidationError here

let slot = try await client.ensureStreamLayer()               // optional — the server already created/bound it on connect (providesView = "stream"); .renamed / .nameConflict
let ice = await client.fetchIceServers()                      // server's WebRTC ICE config


for await event in await client.events() {                   // AsyncStream<ClientEvent>
    switch event {
    case .stateChange(let state): print(state.rawValue)
    case .memberJoin(let member): print(member.displayName)
    case .ended(let reason): print(reason ?? "")             // the last event —
    default: break                                           // the stream finishes right after it
    }
}
await client.close()                                         // reached once the session has ended
```

**Lifecycle events** come only through `events()` — `client.on("error")` (or any other
name) subscribes to a stage *channel* of that name:

| Case | Payload | When |
| --- | --- | --- |
| `.stateChange` | `TransportState` (`.idle` `.connecting` `.connected` `.reconnecting` `.ended`) | every state transition |
| `.failure` | `AdapterError` | a server-pushed or transport error (rejoin refusals included), or `StrictRoutingWarning` from `on()` |
| `.memberJoin` | `Member` | a participant joined |
| `.memberLeave` | `String` (participant id) | a participant left |
| `.historyReplaced` | `[StageMessage]` | the history snapshot was re-seeded (every (re)join that carries one) |
| `.queueOverflow` | `PublishInput` | the offline queue dropped its oldest item |
| `.ended` | `String?` (reason) | the session is over — the last event; the stream finishes right after it |

A stream sees only events that happen after you call `events()` — read `await client.state`
for the current state.

Values are the `Primitive` enum (`.string`/`.int`/`.double`/`.bool`/`.null`) with literal
conveniences — `client.send("ch", 23.5)` works with a **literal**; a variable needs its case
(`.double(x)`, `.string(name)`). To read one use `.stringValue`, `.doubleValue` (an `.int`
reads as a `Double` too), `.boolValue` or `.isNull`, and `.jsonValue` to print it —
`print(message.value)` shows the enum case (`string("go")`, `double(23.5)`):

```swift
let speed = 12.5
try await client.send("car.speed", .double(speed))          // a variable needs its case
await client.on("car.gear") { message in
    guard let gear = message.value.stringValue else { return }
    print("gear \(gear)")                                    // "gear D", not "gear string("D")"
}
```

Prototypes and the Console send every value as a **string** — a prototype's `42` arrives as
`.string("42")`, and `.doubleValue` is `nil` for it. Parse it yourself
(`Double(message.value.stringValue ?? "")`); values from other programs keep their case.

Objects go through `sendJSON` (the JSON-string convention prototypes parse): pass
JSON-compatible values only — `[String: Any]` dictionaries, arrays, strings, finite
numbers, booleans, `NSNull`, nested as deep as you like. Anything else, at any depth (your
own `struct`, a `Date`, a `Primitive`, a dictionary with non-`String` keys, NaN), throws
`ValidationError` at the call site. For an `Encodable` struct, encode it with
`JSONEncoder` and send the resulting string with `send`.

The server never echoes your own publishes; `latest()` still folds them locally, so it is
not a delivery check. **Acks**: `ack: true` waits for the **server's** ack (not a delivery
receipt) and throws `AckTimeout` after `ackTimeout` (default 15 s; it must be finite, above 0
and at most 2147483.647 s — anything else fails with `ConfigError` before the client contacts
the server) — a timeout does not
prove the server didn't get it. An acked send already on the wire when the connection
drops or you `close()` ends in `AckTimeout` at once (it is not re-sent); one still waiting
in the offline queue stays queued across a reconnect and fails with `SessionEnded` at
`close()`. While standby an acked send throws `Standby`. A plain send is
**at-most-once**, direct or queued: it asks for no ack, so a server rejection of it (the rate
limit included) is never seen, and one lost to a drop right after it went out is not retried
— only acked sends report the server's answer. A queued acked send that already went out once before a drop and is later failed with
`QueueOverflow`, `Standby` or `SessionEnded` may have reached the server too — on a queued
acked send, none of these codes proves non-delivery.

**Throttle**: the returned `push` is `(Primitive) throws -> Void`. The value is type-safe
already; `push` checks the channel name where you call it and throws `ValidationError` for a
bad one (`try push(.double(x))`) — a push after `close()` does not throw. Delivery after that
is fire-and-forget, like a plain `send`. `hz` must be greater than 0 (a precondition — it
traps; the other languages throw `ConfigError`).

Offline sends queue (default 1000, oldest dropped with a `.queueOverflow`
event) and flush in order on reconnect, as fast as the rate limit below allows — plain items
without waiting for acks, an acked item only after its server answer;
`TooManyRequests` (only acked sends see it) pauses sending with exponential backoff (1s → ×2 → 60s) and requeues silently (no
`send-dropped`) — it looks like `.connected` with `await client.queuedCount` above 0, not
standby. Every reconnect revalidates the
adapter key for a fresh JWT; a revoked key surfaces as `AuthRevoked`. `close()` drops queued fire-and-forget sends (queued acked sends fail with `SessionEnded`): to be
sure the last values went out, await an acked send or wait until `await client.queuedCount` is 0 first.

**Rate limit — 60 messages a second.** The server accepts at most 60 messages a second per
connection and silently drops fire-and-forget messages above that, so the adapter paces every
send — direct or queued — with a token bucket (60 per second, bursts of up to 30); a send with
no token free waits in the queue. Keep a source above 60/s up and the queue fills: the oldest
items are dropped (a `.queueOverflow` event for each, one `queue-overflow` line in the diagnostics
ring per episode) and the newest value still goes out. After a disconnect, a sender at 60/s or
more stays behind by up to `queueLimit` ÷ 60 seconds (the queue drains at 60 a
second, so a full default queue of 1000 takes about 17 s) —
throttle it or lower `queueLimit`. Acked sends go one per round trip, so never use `ack: true`
for a high-rate stream.

Errors are `AdapterError` with `.code` and usually `.hint` — print the code and
`error.hint ?? error.message` in your error paths: several codes carry no hint
(`AckTimeout`, `SessionEnded`, most `ValidationError`s, some `ConfigError`s), and an error
without a message of its own has the code as its message (`AckTimeout`) — so also say which
call failed.

**Roster.** `try await client.members()` returns `[Member]` (you included) and
`try await client.listeners(channel)` a `Listeners` with `channel`, `declared` and
`broadcast` (`[Member]`, you excluded). Re-read `members()` after every (re)connect — joins and leaves that happened while you were
disconnected are not replayed as events. `members()` and `listeners(_:)` are server round trips:
they fail with `NotConnected` while the client is not connected.

Driving the raw `stage:signaling` passthrough with a platform WebRTC framework?
`fetchIceServers()` returns the deployment's ICE (STUN/TURN) config, preferring
the authenticated `GET /api/stages/:id/ice` (which mints short-lived TURN
credentials) and falling back to the public STUN-only `GET /api/about` on older
servers — hand it to your `RTCConfiguration`. A peer built
without it gathers host candidates only and connects **inside the same NAT
only** (the classic symptom: works in the office, a remote viewer sees
nothing). It never throws: on failure it returns a still-cached list, else the public
STUN list, else `[]` (fail-open; success is cached, failures retry on the next call).

Three Swift-specific notes worth knowing:

- **Subscriptions are not auto-cancelled on `deinit`.** Discarding the handle
  (`await client.on("x") { … }`) keeps the subscription alive, matching every
  other adapter. Call `cancel()` to unsubscribe. When the session ends
  (`close()`, or `.ended` from the server side) the client drops every
  subscription and **finishes every `events()` stream**, so a callback or a
  `for await` loop that captures the client does not keep it alive. Each
  `events()` stream buffers at most the newest 256 unread events.
- **Your callbacks run off the main actor.** The socket library is not thread-safe,
  so the transport funnels all socket access onto one serial queue; on top of it each
  kind of callback has its own home: channel (`on`) and signaling (`onSignaling`)
  callbacks run on the client actor (`ConnectClient`), holder listeners (`onRemoteInputHolder`) run
  synchronously on the socket queue, and `onRemoteInput` runs on the broadcast helper's
  actor. All of them are synchronous, so hop explicitly with `Task { @MainActor in … }`
  before touching UI (a synchronous callback cannot `await MainActor.run`) — and return
  quickly: a slow callback delays the ones behind it on
  the same home (a slow `onRemoteInput` delays the next pointer frames, but a holder
  revoke still applies before the next frame). For example:

  ```swift
  await client.on("panel.level") { message in
      let level = message.value.doubleValue ?? 0           // read it here, synchronously…
      Task { @MainActor in model.level = level }           // …then hop to the main actor for UI
  }
  ```

- **Channel callbacks run one at a time, in the order messages arrived.** A slow
  callback delays the ones behind it, so hand heavy work off (`Task { … }`). Up to
  1000 undelivered messages wait; beyond that the oldest ones skip their callback —
  `latest(_:)` still reflects them, and the diagnostics ring records `queue-overflow`
  (`inbound: …`). `latest(_:)` remembers the 1000 most recently updated channels.
  Signaling listeners and the presence events (`.memberJoin` / `.memberLeave`) arrive in
  order too and are never skipped. The broadcast helper hands remote-input pointers to
  `onRemoteInput` in arrival order; past 1000 waiting frames it drops the oldest `move`
  frames only — the backlog rule never drops `down`, `up` or `cancel`. (Frames are still
  discarded as before when they are malformed, come from a viewer that is not the
  announced holder, or belong to a viewer that was cleaned up; and a viewer with more
  than 256 waiting `down`/`up`/`cancel` frames, counted across all of its channels,
  loses its whole backlog, and the app gets a `cancel` for each pointer of that viewer it
  still holds. After any synthesized `cancel`, that pointer's `move`/`up` frames are
  dropped until its next `down`.)

## Error codes

Every failure is an `AdapterError` (a `struct` conforming to `Error`) with `.code`
(branch on this — it is the stable identifier), `.message` and usually **`.hint`** —
a one-line next action worth logging. Errors throw from the call that caused them,
and lifecycle failures also arrive as `.failure` on `client.events()`.

| Code | Meaning | adapter behaviour |
| --- | --- | --- |
| `AuthRevoked` | Adapter key rejected, revoked, or expired | Terminal — auto-rejoin gives up (`ended: "rejoin-abandoned: AuthRevoked"`); issue a new key |
| `ProtocolVersionMismatch` / `ClientAheadOfServer` | Wire version gate | Terminal — upgrade one side; rejoin gives up rather than retrying forever |
| `ValidationError` | Bad channel/value locally, or the server rejected the join body (400) | Local checks fail before any network call; a join 400 names the fields the server rejected (see below) |
| `RoomEnded` | No live session — an Editor/Owner has to open the stage in Connect (people who joined through a share link cannot start one), or the key was created with **Let adapters start a session** turned on (Share → Adapter keys) | `connect` throws it at once; after a successful join, auto-rejoin (on by default) **keeps waiting** and joins when the session opens |
| `SessionEnded` | The client has ended (closed, or the session is over for good) | Sends fail; queued acks reject |
| `StageFull` | Participant cap reached | Surfaced from the join; rejoin keeps waiting — a seat may free up |
| `StageArchived` | The stage is archived | Surfaced from the join — ask its owner to restore it |
| `TooManyRequests` | Server rate limit (60 messages a second per connection), seen only on acked sends | Automatic backoff (1s → ×2 → 60s) and retry from the offline queue — silently (no `send-dropped`), not thrown |
| `TooManyRequests` from `connect()` | The server refused the join (429): too many failed join attempts from this network address — 20 within 5 minutes, usually another program here still using a revoked or mistyped token — or more than 60 joins a minute with one adapter key (a restart loop); servers without the per-key join limit also refuse a sixth join from one address within a minute | Thrown from `connect()`, not retried — wait up to 5 minutes, and fix the program that causes it |
| `KeyMaterialUnavailable` from `connect()` | The server (503) cannot read the stored material of an older adapter key on this stage — a server-side key change left it behind; after 30 of these from one address within a minute, a correct key gets the same answer until the minute ends | Thrown from `connect()`, not retried (a joined client keeps retrying while it rejoins) — ask whoever runs your Connect to revoke the old keys |
| `AckTimeout` | No server ack within `ackTimeout` (default 15 s), or the connection closed with the ack pending | Surfaced to the caller of an acked send (at once when the connection closes) — not proof that the server didn't get it |
| `Standby` | An acked send while this instance is standby (`exclusiveName`), or a queued acked send discarded when it became standby | Not sent (a plain send is dropped instead) |
| `QueueOverflow` | Queue cap hit (default 1000 — sending faster than 60/s, or a long disconnect) | Oldest item dropped and rejected, plus `.queueOverflow` event |
| `NotConnected` / `JoinTimeout` / `ConfigError` | Client side: unreachable server (or `members()`/`listeners(_:)` while not connected) · connected but never admitted · bad options | Surfaced to the caller — `ConfigError` before any connection is attempted |
| `Unauthorized` / `NotSupported` | The server refused a roster request (e.g. this connection was already marked as left) · the call is not available on this transport or server (`ensureStreamLayer()` on a server without the route) | Thrown from that call |
| `ensureStreamLayer()` errors | `NotSupported` (server without the route) · `RoomEnded` (no live session) · `TooManyRequests` · `ValidationError` (the server's message; a hint when it says no `providesView = "stream"` or the stage's Adapter Stream layer limit) · `NotConnected` (network or server error, or no answer within the ack timeout — the timeout covers a 401 re-validation too) · errors from that 401 re-validation pass through as `connect()` would report them (e.g. `AuthRevoked`), except a server (5xx) or network failure during it, which is `NotConnected` · `SessionEnded` (the client has ended — nothing is sent) | Thrown from that call; cancelling the task throws `CancellationError` |

A 400 from the join carries the server's own issue list instead of a bare `(400)` —
that failure is almost always a **version skew** between server and adapter, not a bad
token:

```text
[ValidationError] token/validate rejected the request body (400):
clientType — Invalid literal value, expected "adapter"
```

Up to three fields are shown, then `(+N more)`.

An `AuthRevoked` whose message says `stage-locked` means the stage is closed to guests, and
adapters join as guests; the hint asks you to have the stage owner (or whoever runs your
Connect) reopen it to guests.

`exclusiveName` standby is enforced automatically — the server's judgment arrives as
`Member.standby`; see [Names and closing](#names-and-closing).

## Screen broadcast (Adapter Stream)

Put **your own rendered screen** on a stage's *Adapter Stream* layer. The helper owns the
signalling choreography and **adds no dependency** — you inject one session per viewer,
built on whatever WebRTC stack you already use.

```swift
let broadcast = await startScreenBroadcast(
    client: client,
    createSession: { viewer, sendIceCandidate, iceServers, wireInput in
        // your WebRTC stack — pass iceServers into its configuration
        try await openSession(viewer, sendIceCandidate, iceServers, wireInput)
    },
    onRemoteInput: { pointer, viewer in inject(pointer) }   // optional
)
await broadcast.sourceChanged()   // when the capture starts or changes (not per frame) — viewers that got no answer offer again
await broadcast.viewerCount
await broadcast.stop()
```

The helper closes a viewer's session when the viewer leaves (`peer-left`). A viewer that left
while the adapter was reconnecting never produces a `peer-left` for it, so after every reconnect
the helper also closes the sessions of viewers missing from the server's `peers` reply to its
`ready`. It stops itself when the client's session ends, so a forgotten `stop()` does not keep
sessions or the hello keepalive alive.

Two rules the helper encodes because both fail **silently** — negotiation reports success
and the picture never appears:

1. **Announce on every connect** (`{"kind":"ready"}`, reconnects included). The server
   builds its peer list from sockets already in the room and only tells existing viewers
   about a late arrival when that arrival says `ready`. The helper re-announces on every
   `connected` **transition** (not the current-state value — a fast reconnect collapses
   into no re-announce if you watch the value).
2. **Attach the track to a stream** (`addTrack(track, stream)`) so the answer carries
   `msid`. That part is your session's job; without it the viewer's `ontrack` fires with
   an empty `streams` array.

The adapter only ever **answers** — the viewer sends the offer. Declare
`providesView = "stream"` at connect time and give the adapter a **stable display name**: the
server creates (or binds) the Adapter Stream layer for that name when you connect, and the layer
resolves your screen by name (the participant id changes every run). Print
`await client.me?.displayName` after connecting — `gauge (2)` means another adapter key holds
`gauge`, and the layer follows the name you got.

- If your factory returns `nil` (nothing to send yet), that viewer gets no answer; call
  `sourceChanged()` once the source exists. Call it when the source starts or changes, not per frame —
  calls are coalesced to at most one `ready` a second.
- Starting a broadcast without `providesView = "stream"` writes one `stream-undeclared` warning to
  the diagnostics ring — the server creates and binds no layer for such an adapter. It is judged from
  the `providesView` you connected with.
- The web keeps one receiver per (viewer page × adapter participant), each with its own
  `receiverId` — not one per layer instance; two receivers on one page replace each other — so a
  person with two tabs gets two sessions: your factory is called once per tab with
  the same viewer id, and `viewerCount` counts sessions. A new offer from the same tab replaces only
  that tab's session. A PIN or share-link guest's refreshed or closed tab leaves its session behind
  (the server reports such a guest leaving only when their last tab goes; team members get a new
  participant id per tab, so theirs is reported), so each person keeps at most 4 sessions — a fifth
  evicts first a session whose answer has not been sent yet, then their oldest. Report closures (next point) so stale receivers don't use the cap and push out
  a live tab. When the person leaves, all of their sessions close. Remote input is judged per person.
  Viewers that send no `receiverId` (older web) keep one session per person.
- Call `await broadcast.sessionClosed(session)` when a session's peer connection reaches `failed` or `closed`: the helper
  forgets it at once and calls its `close()` (keep that idempotent). It takes a class instance — sessions are matched by identity, so a
  value-type session does not compile. Optional — otherwise a
  dead session stays until the viewer offers again, leaves, or is evicted. A newer offer from the same
  tab that is still being negotiated is left alone.
- Viewer ICE that arrives early (before the session exists or has applied the offer, or even before the
  offer) is queued and handed over once the offer is applied. ICE that comes before any offer is held for
  10 s, for at most 4 receivers per viewer and 64 in total (the oldest is dropped; one `ice-queue`
  diagnostics note). If the ICE-server lookup fails or takes longer than 5 s, the helper answers with the
  last known servers (one `ice-servers` diagnostics note).
- If the adapter rejoins as a new participant (a new session after the stage reopened, or a
  long-lived adapter re-validating its key), the helper closes every viewer session from before —
  viewers address the adapter by participant id and offer again to the new one.
- `ensureStreamLayer()` is optional (idempotent). `renamed` means the server joined you under a
  different name (`gauge (2)`: another program on a different key uses `gauge`, or the `gauge` layer is
  bound to a key you rotated without revoking — rename one, or revoke the old key), so a layer placed
  for `gauge` will not show this screen. `nameConflict` is
  deprecated and equals `renamed`. For two instances of the same program that take over from each
  other, put both on one key and set `exclusiveName = true` (one active, the other standby). Errors are
  in the error table; cancelling the task throws `CancellationError`.

Pass **`onRemoteInput`** to the same call to accept an Editor's pointers. The
helper carries the holder gate, the direction whitelist, the `[0,1]` clamp, pointer-id
validation and cancel synthesis when the holder changes; **injecting the pointer into your
UI is your code** — what a pointer *means* is the app's decision.

## Diagnostics — `doctor` as a library call

```swift
let results = await runDoctor(credential: credential)   // [CheckResult]
print(formatDoctorReport(results))
```

Print the report when `connect` fails — it names the one step that is wrong:

```swift
let token = ProcessInfo.processInfo.environment["CONNECT_TOKEN"] ?? ""
do {
    let client = try await connect(ConnectOptions(secretKey: token, displayName: "Wall Sensor"))
    // …
    await client.close()
} catch let error as AdapterError {
    print("[\(error.code)] \(error.hint ?? error.message)")
    print(formatDoctorReport(await runDoctor(credential: token)))
}
```

Eight checks run in order and **stop at the first failure** (the point is to name one
cause, not list a cascade): credential · url shape · server reachable · adapter screens ·
remote input · stage session · socket join · screen adapters. *adapter screens* and *remote
input* only warn or skip — they never stop the run — and *screen adapters* appears only
after a successful join on a server that knows screens. Only ✖ lines need fixing (△ and ·
are information). The join step takes a guest seat for a moment (as `omni-adapter doctor`)
and leaves. The credential is masked in the report. HTTP and the connect step are
injectable, so you can unit-test the whole thing.

For a support ticket, `Report.build(credential:diagnostics:)` bundles the doctor run with
the client's diagnostics ring — pass `diagnostics: client.diagnostics()`, or the bundle's
list stays empty — and `Report.format(_:)` renders it (the credential appears only as a
fingerprint).

The terminal `omni-adapter` CLI stays TypeScript/Python: the same check from a terminal is
`npx --no-install omni-adapter doctor "$CONNECT_TOKEN"` in a Node project that has the
TypeScript adapter installed, or `python -m omni_adapter doctor "$CONNECT_TOKEN"` with the
Python one.

## Remote input

Pass `onRemoteInput` to `startScreenBroadcast` (above) and the helper accepts an Editor's
pointers — no registration code and no extra code on your side. Its holder gate stays permissive
until the server announces a holder. Standalone servers send lease-holder notices: input is accepted only from the Editor holding the
lease (key adapters are registered automatically when they join and are told the holder; the Connect
editor claims the lease on its first pointer-down), and PIN and share-link viewers never control.
Older servers that predate those notices never announce a holder, and the helper then accepts every
viewer's input.

The registration/lease API exists for when one-operator-at-a-time control returns
server-side; an adapter written today keeps working then, because the first holder notice
switches the gate to strict. What it carries is what the app cannot do itself:
registration (`client.submitRegistrationCode(code)` → `registrationId` + a succession
`deviceSecret` — a **secret**, redacted from `description`;
`client.resumeRegistration(deviceSecret:)` re-binds after reconnects) and the
operation-lease holder notice (`client.onRemoteInputHolder { … }`). The holder
listener runs **synchronously on the socket thread** to preserve notice order —
hop to your UI yourself, accept input only from the announced participant, and
expire it locally after the notice's `ttlMs`.

## Examples

The three examples are the **six-language common scenarios** — each shows the same thing as the
same-named example in every other language (declared in `docs/parity.json` → `commonExamples`;
the CI `parity` job checks that they exist).

```bash
cd swift
swift run Quickstart   "cst_…"   # connect · first acked round trip · receive
swift run RosterBridge "cst_…"   # bidirectional + roster (members)
swift run Resilience   "cst_…"   # reconnect · offline queue · auto-rejoin
# or: CONNECT_TOKEN=cst_… swift run Quickstart
```

`swift build` compiles the examples too, so the code in this README cannot silently drift from
the API.

## Testing

```bash
cd swift && swift test     # unit tests, no server needed
```

`ProtocolDriftTests` pins the wire constants against the sibling TypeScript
adapter sources in this repo (which in turn drift-check against the server's
protocol package); the check skips itself in a standalone checkout.

## Testing without a server — `MemoryStage`

```swift
let stage = MemoryStage()
let sensor = try await stage.join(MemoryJoinOptions(displayName: "sensor", sends: ["sensor.temperature"]))
let panel = try await stage.join(MemoryJoinOptions(displayName: "panel", receives: ["sensor.*"]))
_ = await panel.on("sensor.*") { print($0.value.jsonValue) }
try await sensor.send("sensor.temperature", .double(23.5), ack: true)   // panel receives it; sensor does not (no echo)

try stage.inject("ui.tap", .int(1))          // as if a prototype sent it
try await stage.disconnect(panel); try await stage.reconnect(panel)   // drop + recover
stage.end("host closed the stage")           // everyone gets .ended
```

Emulates the wire rules in-process (no self-echo, strict delivery, history ring,
`exclusiveName` standby, `listeners(_:)`); `rejectPublish:` injects failures. A harness,
not a server — no doctor, no REST, no media, and no 60-messages-a-second pacing (its
transport sets `bypassRateLimit`). `ensureStreamLayer()` answers with a stable fake layer per name
(`memory-layer:<name>`; `stage.streamLayers()` lists them). Delivery hops through the client actor,
so `await` a short settle before asserting.

## Testing without hardware — `replay` · `simulate`

```swift
let records = try parseRecording(String(contentsOfFile: "run.ndjson"))   // written by `omni-adapter record`
try await replay(client: client, records: records, options: ReplayOptions(speed: 2))
try await simulate(client: client, channel: "sensor.temperature",
                   options: SimulateOptions(waveform: "sine", min: 18, max: 26, hz: 2, durationMs: 30_000))
```

Both publish through `send` (rate limit, queue, standby and backoff unchanged), wait for the
queue to drain (giving up after 15 s without progress) before returning, stop on `.ended` or Task cancellation, and return
the partial count. `wait`/`random` are injectable for deterministic tests. They only pace the sends — the 15 s no-progress limit runs on the real clock, so a no-op wait does not make it give up early.

## History is state, not triggers

`history()`/`latest(_:)` are re-seeded from the server snapshot on every (re)join whose joined payload carries one (the `stage:joined` fallback re-seeds nothing) — but the snapshot is **never** replayed into `on(_:_:)` callbacks. A command published while this adapter was disconnected is lost as a trigger and survives only as a last value: read `latest(_:)` when `.stateChange(.connected)` arrives for state channels; treat trigger channels as at-most-once across a disconnect. Deliberate — replaying into callbacks would re-fire a stale command minutes late.
