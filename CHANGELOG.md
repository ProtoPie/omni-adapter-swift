# Changelog — OmniAdapter (Swift)

All notable changes to the Swift adapter. Versioning follows
[SemVer](https://semver.org); release tags use the `swift-v*` prefix
(independent of the other language packages). See `/VERSIONING.md` for the
policy and `/docs/COMPATIBILITY.md` for the server requirement matrix.

## 0.1.0-beta.0 (2026-10-29)

First public release of the Swift adapter for **Connect Omni** stages: an async/await
`ConnectClient` actor, distributed through SwiftPM, as a beta. SwiftPM does not match a
prerelease with `from: "0.1.0"`, so depend on `from: "0.1.0-beta.0"` (or `exact:`); the API may
still change before 0.1.0.

### Highlights

- `connect()` joins a stage with a `cst_…` adapter token (or a bare `ck_…` key plus `url` and
  `stageId`) and returns a client that sends, receives, acks and reconnects on its own.
- Sending: `send()` / `sendJSON()` with optional server acks, an ordered offline queue, a
  per-connection rate limiter matched to the server's 60 messages a second, and `throttle()`.
- Receiving: per-channel and wildcard subscriptions, `latest()` / `history()`, the `members()`
  roster with presence events (and an `events()` stream), and `listeners(channel)`. Callbacks
  are delivered in arrival order.
- Declarations (`receives` · `sends` · `nodeType`) make the adapter a node in Backstage;
  `strictRouting` delivers only what it declared, and `exclusiveName` keeps one active instance
  per name with automatic standby.
- Session resume (`autoRejoin`, on by default) waits through a stage that closes and reopens and
  gives up only on failures that waiting cannot fix.
- Diagnostics: a bounded `diagnostics()` ring with an `onLog` sink, `runDoctor()`, `ping()`
  latency statistics and `Report.build` repro bundles — none of them ever record a credential.
- Testing without a server or hardware: `MemoryStage`, `replay()` and `simulate()`.
- Experimental: raw `stage:signaling`, the screen-broadcast helper (`startScreenBroadcast`,
  media injected by the app) and remote input.

### License

- Apache License 2.0 (`LICENSE`, `NOTICE`). Connecting to a stage still requires a Connect Omni
  subscription (an adapter token).

### Requirements

- iOS 13+, macOS 10.15+, tvOS 13+, watchOS 6+; a consumer package must declare these in its
  own `platforms:`. The only dependency is `socket.io-client-swift` (v16).
- Add the package from `https://github.com/ProtoPie/omni-adapter-swift`
  (product `OmniAdapter`).
- A sandboxed macOS app needs the *Outgoing Connections (Client)* entitlement; a plain
  `http://` server addressed by host name needs an App Transport Security exception.

### Server requirements

- Joining, sending, receiving, acks, history, the roster and reconnects work on any Connect Omni
  server.
- `ck_` adapter keys, declarations, `strictRouting`, `exclusiveName`, `providesView`, screen
  broadcast and remote input need an adapter-enabled server (or web build). Older servers strip
  the declarations without an error. See `docs/COMPATIBILITY.md` for the minimum server per
  feature.

### Known limitations

- Soft real-time: a few milliseconds on a LAN, hundreds over a WAN. Not a control bus.
- A live session must exist; an adapter starts one only with a key created with "Let adapters
  start a session". Each connection uses one guest seat.
- History is a replay ring (1000 messages by default) that re-seeds state on rejoin but never
  re-fires handlers. Trigger channels are at-most-once across a disconnect.
- Values are primitives (structured data as JSON strings); binary payloads are not supported.
- A source faster than 60 messages a second loses its oldest queued values; `close()` drops
  queued fire-and-forget sends.
- Under a backlog of more than 1000 waiting message callbacks the oldest are skipped
  (`latest()` stays exact), and the `events()` stream drops its oldest events under bursts.
- No terminal CLI; `runDoctor()` offers the same checks as a library call.

Detailed history: https://github.com/ProtoPie/omni-adapter/blob/main/docs/history/swift.md
