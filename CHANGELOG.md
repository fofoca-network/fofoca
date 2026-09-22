# Changelog

All notable changes to the fofoca workspace. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); all member crates
share one version and move together. A release is a git tag — nothing is
published to a registry; pin it with
`fofoca = { git = "https://github.com/fofoca-network/fofoca", tag = "v0.6.0" }`.

## [Unreleased]

### Added

- `fofoca-wasm`: the browser peer, the byte pipe as a wasm-bindgen class,
  with `packages/fofoca-wasm` as its JS backend and a driverless harness
  page. `cargo task wasm-peer` builds it.
- A custom relay ladder (`relay_urls` / `relayUrls` / `--relay-url`) and the
  per-node path switches (`paths.ip`, `paths.webrtc`; `disable_ip` /
  `disable_webrtc` in C) on every create surface. The ladder is mixed into a
  derived topic id, so every member must pass the same list.
- `fofoca_protocol::Lookup` and `Transport`, the entries of the `lookup` and
  `transport` lists every create surface takes, with `LookupSet::from_lookups`,
  `TransportPolicy::from_transports` and `MeshConfig::resolve` behind them, so
  a consumer parses the two lists and applies the cross-rules with no code of
  its own.
- `cargo task e2e --suite mesh` and `--suite chat`: a real native peer and a
  real browser tab on a local plain-HTTP relay, swept over the relay policy,
  the native transport set and the join mode. Behind the `mesh` feature of
  `tasks`, and local-only for now.
- `Session::request` on the pipe, and one config path for every topic.
- `fofoca-pipe`, the binary (`crates/fofoca-pipe-cli`): stdin to a mesh, the
  mesh to stdout. A bare run mints a public mesh and prints the id, and with
  `--web-url` the page URL with the id in its fragment. It waits for a peer
  the roster shows as `unicast` before it reads stdin, because a frame sent
  earlier is lost.
- `packages/fofoca-pipe-web`: the pipe's web app, a static build any host
  serves. It shows streams in order and registers its runtime as WebMCP tools
  (`pipe_send`, `pipe_send_eof`, `pipe_read`, `pipe_peers`, `pipe_status`,
  `pipe_state_get`, `pipe_state_merge`), with the same functions on
  `window.pipe` where there is no model context.
- `cargo task e2e --suite pipe`: the binary against the page over a local
  relay. A payload twice the send window lands byte-exact both ways, the tab
  shows its own send, and the binary exits clean on EOF.
- Flow control on the pipe: a receiver acks every 8 frames (`pipe_ack`) and
  the sender waits while more than `WINDOW` (64) frames are unacknowledged by
  any peer with a proven payload lane (`fofoca_pipe::Flow`, on
  `Session::flow`). A receiver silent for `STALL_TIMEOUT` stops being waited
  for. Without it a 1 MB stream overran every queue on the path: the gossip
  subscription (`Lagged`), the pipe's inbound queue, the data channel's send
  buffer. It arrived incomplete. Measured 1 MB browser to browser: 12 to 21 s
  with losses before, 1.2 s (850 KB/s) lossless after.
- `fofoca-pipe`, the binary (`crates/fofoca-pipe-cli`): stdin to a mesh, the
  mesh to stdout. A bare run mints a public mesh and prints the id, and with
  `--web-url` the page URL with the id in its fragment. It waits for a peer
  the roster shows as `unicast` before it reads stdin, because a frame sent
  earlier is lost.
- `packages/fofoca-pipe-web`: the pipe's web app, a static build any host
  serves. It shows streams in order and registers its runtime as WebMCP tools
  (`pipe_send`, `pipe_send_eof`, `pipe_read`, `pipe_peers`, `pipe_status`,
  `pipe_state_get`, `pipe_state_merge`), with the same functions on
  `window.pipe` where there is no model context.
- Flow control on the pipe: a receiver acks every 8 frames (`pipe_ack`) and
  the sender waits while more than `WINDOW` (64) frames are unacknowledged by
  any peer with a proven payload lane (`fofoca_pipe::Flow`, on
  `Session::flow`). A receiver silent for `STALL_TIMEOUT` stops being waited
  for. Without it a 1 MB stream overran every queue on the path: the gossip
  subscription (`Lagged`), the pipe's inbound queue, the data channel's send
  buffer. It arrived incomplete. Measured 1 MB browser to browser: 12 to 21 s
  with losses before, 1.2 s (850 KB/s) lossless after.
- `fofoca-pipe`, the binary (`crates/fofoca-pipe-cli`): stdin to a mesh, the
  mesh to stdout. A bare run mints a public mesh and prints the id, and with
  `--web-url` the page URL with the id in its fragment. It waits for a peer
  the roster shows as `unicast` before it reads stdin, because a frame sent
  earlier is lost.
- Flow control on the pipe: a receiver acks every 8 frames (`pipe_ack`) and
  the sender waits while more than `WINDOW` (64) frames are unacknowledged by
  any peer with a proven payload lane (`fofoca_pipe::Flow`, on
  `Session::flow`). A receiver silent for `STALL_TIMEOUT` stops being waited
  for. Without it a 1 MB stream overran every queue on the path: the gossip
  subscription (`Lagged`), the pipe's inbound queue, the data channel's send
  buffer. It arrived incomplete. Measured 1 MB browser to browser: 12 to 21 s
  with losses before, 1.2 s (850 KB/s) lossless after.
- Flow control on the pipe: a receiver acks every 8 frames (`pipe_ack`) and
  the sender waits while more than `WINDOW` (64) frames are unacknowledged by
  any peer with a proven payload lane (`fofoca_pipe::Flow`, on
  `Session::flow`). A receiver silent for `STALL_TIMEOUT` stops being waited
  for. Without it a 1 MB stream overran every queue on the path: the gossip
  subscription (`Lagged`), the pipe's inbound queue, the data channel's send
  buffer. It arrived incomplete. Measured 1 MB browser to browser: 12 to 21 s
  with losses before, 1.2 s (850 KB/s) lossless after.

### Fixed

- A hidden tab stopped rendering what it received. The per-frame batching
  added with the render fix runs on animation frames, and a hidden tab gets
  none, so its view stayed empty and its pending batch grew without bound.
  The page now flushes on a timer as well, and the batch is bounded per
  stream. The data was never lost: `pipe_read` returned it throughout.
- `pipe_read` consumed what it returned, so two readers on one tab split the
  stream between them, and a binary payload reached every reader as U+FFFD
  because the runtime kept only decoded text. The runtime now keeps a
  bounded log of entries with sequence numbers and their bytes. `pipe_read`
  takes a `cursor` and returns the next one, so a read takes nothing away
  and each reader follows the stream from its own position, and
  `encoding: "base64"` returns bytes that are not text, exactly. The log is
  bounded by bytes held (4 MB), not by what anyone has read; `pipe_status`
  reports `buffered`, `cursor` and `oldestCursor` in place of `unread`.
- A beacon holder sheds its rendezvous periodically to re-arbitrate with a
  possible same-id co-host. It now waits, up to three rounds, while a
  data-channel peer depends on that beacon: a browser reaches the mesh
  through the rendezvous and has no second path, so the shed emptied its
  roster mid-transfer.
- The pipe page wrote a chunk at a time and read `scrollHeight` for each,
  so every chunk forced a layout of the whole view, on the same thread the
  wasm engine runs on. Throughput fell about 5x over a session and came back
  on reload. Measured on a view holding 4.2 M characters, 50 chunks cost
  5069 ms that way against 106 ms as one batch. The page now writes once per
  animation frame and bounds each view.
- A node judged its own need for the `WebRTC` lane from its endpoint address,
  which is empty in a browser whenever the relay link is down; empty read as
  "has IP", so a tab that was the lower id skipped the lane for a native peer
  and stayed relay-only. The pair decision and the rendezvous offer now use
  the node's own transport set (`EventLoopState::local_ip_transport`).

### Fixed

- A hidden tab stopped rendering what it received. The per-frame batching
  added with the render fix runs on animation frames, and a hidden tab gets
  none, so its view stayed empty and its pending batch grew without bound.
  The page now flushes on a timer as well, and the batch is bounded per
  stream. The data was never lost: `pipe_read` returned it throughout.
- `pipe_read` consumed what it returned, so two readers on one tab split the
  stream between them, and a binary payload reached every reader as U+FFFD
  because the runtime kept only decoded text. The runtime now keeps a
  bounded log of entries with sequence numbers and their bytes. `pipe_read`
  takes a `cursor` and returns the next one, so a read takes nothing away
  and each reader follows the stream from its own position, and
  `encoding: "base64"` returns bytes that are not text, exactly. The log is
  bounded by bytes held (4 MB), not by what anyone has read; `pipe_status`
  reports `buffered`, `cursor` and `oldestCursor` in place of `unread`.
- A beacon holder sheds its rendezvous periodically to re-arbitrate with a
  possible same-id co-host. It now waits, up to three rounds, while a
  data-channel peer depends on that beacon: a browser reaches the mesh
  through the rendezvous and has no second path, so the shed emptied its
  roster mid-transfer.
- The pipe page wrote a chunk at a time and read `scrollHeight` for each,
  so every chunk forced a layout of the whole view, on the same thread the
  wasm engine runs on. Throughput fell about 5x over a session and came back
  on reload. Measured on a view holding 4.2 M characters, 50 chunks cost
  5069 ms that way against 106 ms as one batch. The page now writes once per
  animation frame and bounds each view.
- A node judged its own need for the `WebRTC` lane from its endpoint address,
  which is empty in a browser whenever the relay link is down; empty read as
  "has IP", so a tab that was the lower id skipped the lane for a native peer
  and stayed relay-only. The pair decision and the rendezvous offer now use
  the node's own transport set (`EventLoopState::local_ip_transport`).
- A pipe receiver held a whole stream behind one missing frame until 256
  later frames arrived. A hole older than `GAP_TIMEOUT` (3 s) is now skipped
  on both sides (`Reorder::expire`, `Streams::expire`; `GAP_TIMEOUT_MS` in
  `fofoca-api`).

### Fixed

- A hidden tab stopped rendering what it received. The per-frame batching
  added with the render fix runs on animation frames, and a hidden tab gets
  none, so its view stayed empty and its pending batch grew without bound.
  The page now flushes on a timer as well, and the batch is bounded per
  stream. The data was never lost: `pipe_read` returned it throughout.
- `pipe_read` consumed what it returned, so two readers on one tab split the
  stream between them, and a binary payload reached every reader as U+FFFD
  because the runtime kept only decoded text. The runtime now keeps a
  bounded log of entries with sequence numbers and their bytes. `pipe_read`
  takes a `cursor` and returns the next one, so a read takes nothing away
  and each reader follows the stream from its own position, and
  `encoding: "base64"` returns bytes that are not text, exactly. The log is
  bounded by bytes held (4 MB), not by what anyone has read; `pipe_status`
  reports `buffered`, `cursor` and `oldestCursor` in place of `unread`.
- A beacon holder sheds its rendezvous periodically to re-arbitrate with a
  possible same-id co-host. It now waits, up to three rounds, while a
  data-channel peer depends on that beacon: a browser reaches the mesh
  through the rendezvous and has no second path, so the shed emptied its
  roster mid-transfer.
- The pipe page wrote a chunk at a time and read `scrollHeight` for each,
  so every chunk forced a layout of the whole view, on the same thread the
  wasm engine runs on. Throughput fell about 5x over a session and came back
  on reload. Measured on a view holding 4.2 M characters, 50 chunks cost
  5069 ms that way against 106 ms as one batch. The page now writes once per
  animation frame and bounds each view.
- A node judged its own need for the `WebRTC` lane from its endpoint address,
  which is empty in a browser whenever the relay link is down; empty read as
  "has IP", so a tab that was the lower id skipped the lane for a native peer
  and stayed relay-only. The pair decision and the rendezvous offer now use
  the node's own transport set (`EventLoopState::local_ip_transport`).
- A pipe receiver held a whole stream behind one missing frame until 256
  later frames arrived. A hole older than `GAP_TIMEOUT` (3 s) is now skipped
  on both sides (`Reorder::expire`, `Streams::expire`; `GAP_TIMEOUT_MS` in
  `fofoca-api`).

### Fixed

- A hidden tab stopped rendering what it received. The per-frame batching
  added with the render fix runs on animation frames, and a hidden tab gets
  none, so its view stayed empty and its pending batch grew without bound.
  The page now flushes on a timer as well, and the batch is bounded per
  stream. The data was never lost: `pipe_read` returned it throughout.
- `pipe_read` consumed what it returned, so two readers on one tab split the
  stream between them, and a binary payload reached every reader as U+FFFD
  because the runtime kept only decoded text. The runtime now keeps a
  bounded log of entries with sequence numbers and their bytes. `pipe_read`
  takes a `cursor` and returns the next one, so a read takes nothing away
  and each reader follows the stream from its own position, and
  `encoding: "base64"` returns bytes that are not text, exactly. The log is
  bounded by bytes held (4 MB), not by what anyone has read; `pipe_status`
  reports `buffered`, `cursor` and `oldestCursor` in place of `unread`.
- A beacon holder sheds its rendezvous periodically to re-arbitrate with a
  possible same-id co-host. It now waits, up to three rounds, while a
  data-channel peer depends on that beacon: a browser reaches the mesh
  through the rendezvous and has no second path, so the shed emptied its
  roster mid-transfer.
- The pipe page wrote a chunk at a time and read `scrollHeight` for each,
  so every chunk forced a layout of the whole view, on the same thread the
  wasm engine runs on. Throughput fell about 5x over a session and came back
  on reload. Measured on a view holding 4.2 M characters, 50 chunks cost
  5069 ms that way against 106 ms as one batch. The page now writes once per
  animation frame and bounds each view.
- A node judged its own need for the `WebRTC` lane from its endpoint address,
  which is empty in a browser whenever the relay link is down; empty read as
  "has IP", so a tab that was the lower id skipped the lane for a native peer
  and stayed relay-only. The pair decision and the rendezvous offer now use
  the node's own transport set (`EventLoopState::local_ip_transport`).
- A pipe receiver held a whole stream behind one missing frame until 256
  later frames arrived. A hole older than `GAP_TIMEOUT` (3 s) is now skipped
  on both sides (`Reorder::expire`, `Streams::expire`; `GAP_TIMEOUT_MS` in
  `fofoca-api`).

### Fixed

- A hidden tab stopped rendering what it received. The per-frame batching
  added with the render fix runs on animation frames, and a hidden tab gets
  none, so its view stayed empty and its pending batch grew without bound.
  The page now flushes on a timer as well, and the batch is bounded per
  stream. The data was never lost: `pipe_read` returned it throughout.
- `pipe_read` consumed what it returned, so two readers on one tab split the
  stream between them, and a binary payload reached every reader as U+FFFD
  because the runtime kept only decoded text. The runtime now keeps a
  bounded log of entries with sequence numbers and their bytes. `pipe_read`
  takes a `cursor` and returns the next one, so a read takes nothing away
  and each reader follows the stream from its own position, and
  `encoding: "base64"` returns bytes that are not text, exactly. The log is
  bounded by bytes held (4 MB), not by what anyone has read; `pipe_status`
  reports `buffered`, `cursor` and `oldestCursor` in place of `unread`.
- A beacon holder sheds its rendezvous periodically to re-arbitrate with a
  possible same-id co-host. It now waits, up to three rounds, while a
  data-channel peer depends on that beacon: a browser reaches the mesh
  through the rendezvous and has no second path, so the shed emptied its
  roster mid-transfer.
- The pipe page wrote a chunk at a time and read `scrollHeight` for each,
  so every chunk forced a layout of the whole view, on the same thread the
  wasm engine runs on. Throughput fell about 5x over a session and came back
  on reload. Measured on a view holding 4.2 M characters, 50 chunks cost
  5069 ms that way against 106 ms as one batch. The page now writes once per
  animation frame and bounds each view.
- A node judged its own need for the `WebRTC` lane from its endpoint address,
  which is empty in a browser whenever the relay link is down; empty read as
  "has IP", so a tab that was the lower id skipped the lane for a native peer
  and stayed relay-only. The pair decision and the rendezvous offer now use
  the node's own transport set (`EventLoopState::local_ip_transport`).
- A pipe receiver held a whole stream behind one missing frame until 256
  later frames arrived. A hole older than `GAP_TIMEOUT` (3 s) is now skipped
  on both sides (`Reorder::expire`, `Streams::expire`; `GAP_TIMEOUT_MS` in
  `fofoca-api`).

### Changed

- **Breaking (pipe wire):** a `pipe_data` body is now `<seq>:<base64>`, a
  `pipe_eof` body carries the stream's frame count, and `pipe_ack` is a new
  tag. Gossip keeps no order, so each frame names its position in its
  (author, addressee) stream and receivers reorder (`fofoca_pipe::Streams`,
  `Streams` in `fofoca-api`). A peer on the old wire drops the new frames as
  undecodable, and vice versa. The frame budget shrank from 2112 to 2094
  bytes to hold the prefix.
- **Breaking (C ABI):** `fofoca_frame` gained `seq` and grew from 80 to 88
  bytes. `fofoca_recv` writes it; a consumer compiled against the old header
  hands over a buffer eight bytes too small.
- **Breaking (C ABI):** `fofoca_opts` is now 72 bytes: the five discovery
  ints (`is_public`, `mdns`, `dht`, `relay_lookup`, `relay_transport`) are
  replaced by two comma-list strings, `lookup` and `transport`, ahead of
  `relay_urls`, and `disable_ip` / `disable_webrtc` follow. A consumer
  compiled against the old header keeps passing the old struct and the
  engine reads it wrong — there is no version field to catch that, so relink
  against the new `include/fofoca.h`. The layout is pinned by a compile-time
  assert in `fofoca-ffi` and by `packages/fofoca-ffi`'s encoder test.
- **Breaking:** every create surface names three mesh-wide choices apart,
  one concept each. `lookup` (`lookup: ['mdns', 'dht', 'relay']`, any
  subset) is how members find each other. `transport` (`['p2p']` or
  `['p2p', 'relay']`) is what payload may ride. `relay_urls` is which relay.
  The `public`, `mdns`, `dht`, `relay_lookup` and `relay_transport` booleans
  are gone; `public: true` is spelled `lookup: ['mdns', 'dht', 'relay']`, and
  naming no lookup is a loopback mesh. A ladder no longer implies the relay
  lookup: both it and `'relay'` in `transport` need `'relay'` in `lookup`,
  and a config that breaks either rule is rejected before any network. The
  per-node switches are `paths` (was `transports`), so "transport" means
  only the mesh-wide policy. In the wasm JSON every old field is an error,
  not a silent no-op. `fofoca_protocol::resolve_lookups` lost its `public`
  parameter. `TransportOpts.relay` keeps its name — it is per-node
  capability, not the mesh policy.
- `MeshConfig::validate` now also rejects a custom relay ladder that would
  not survive the wire (more than 16 rungs, or a URL over 512 bytes). A
  caller-supplied ladder reached the encoder unbounded before: past 255
  rungs it panicked, and between 17 and 255 it minted an id no member could
  decode.

### Fixed

- A negotiated WebRTC session is registered as a transport address, so a
  bare-id dial migrates onto it instead of being refused on the relay.
- An offer from a peer we already hold a session with detaches the old
  session and answers fresh.
- Every `joined` re-floods our `PeerInfo` behind a per-endpoint cooldown, so
  a newcomer or a rejoin across a beacon epoch is not unreachable forever.
- A public rendezvous claim needs two consecutive free probes, so a probe
  inside a live beacon's release window no longer stands up a rival copy.
- A fresh unicast dial waits up to five seconds for a direct path before its
  first frame, which the pool used to refuse as relayed.
- A digest window is bounded by the extent of its slice rather than by its
  first and last entries; the log is in arrival order, so the ends bounded
  nothing and a holder answered "nothing missing" for a gap.
- On a lookup-only mesh the debug census no longer demotes a proven peer on
  a relayed `conn_path` reading, which stopped every payload lane to it.

### Removed

- **Breaking:** the `fofoca-blobs` crate, and with it the workspace's only
  OPFS store backend. `fofoca-chunks` is the store: chunks prove content,
  where blobs' bao outboards proved placement. No known consumer imported
  `fofoca_blobs` at removal time; the v0.6.0 tag keeps the crate.

## [0.6.0] - 2026-08-30

The first tagged release. Everything since the extraction from mallorca.

### Added

- A WebRTC transport for iroh: QUIC datagrams over one unreliable data
  channel, with a native (str0m) and a browser (`RTCPeerConnection`)
  backend, signalled over the iroh relay with no signaling server
  (`fofoca-iroh-webrtc-transport`).
- A multihop transport: source-routed QUIC relaying through peers
  (`fofoca-iroh-multihop-transport`).
- The engine runs in the browser: `--no-default-features` leaves a portable
  core for wasm32, guarded by CI.
- `fofoca-blobs`: a BLAKE3/bao store of verification metadata for bytes the
  caller already owns, with fs, memory, OPFS, and IndexedDB backends.
- `fofoca-chunks`: the content-addressed chunk store, moved in from
  agent-share. Chunks prove content; `fofoca-blobs` outboards prove
  placement. It is meant to replace `fofoca-blobs` eventually.
- The `blob` feature: a point-to-point side channel for oversize payloads,
  with bearer-secret tickets.
- A C ABI (`fofoca-ffi`) and one JS API over it for Bun, Deno, and Node
  (`packages/fofoca-api`, `fofoca-pipe`).
- `fofoca-netplay`: GGPO-style rollback netcode for peer-to-peer games, and
  the light-cycles example that proves it in CI.
- The relay transport policy lives in the mesh id: a mesh can declare its
  relay lookup-only, and members prove a direct path before payload flows.

### Changed

- **Breaking:** `MeshConfig` gained a `transport` section, and the default
  relay policy is lookup-only: the relay carries bootstrap, signaling, and
  NAT traversal — no payload. A gossip graft waits for a proven non-relay
  path.
- `blake3` is a workspace dependency with `default-features = false` as the
  floor; crates opt in.

### Fixed

- A browser WebRTC session whose ICE died under an open data channel stayed
  registered forever, blocking every re-dial; the hub now watches
  `connectionState` and evicts (`failed`/`closed` at once, `disconnected`
  after a 10 s grace).
- `swarm-discovery` 0.6.3 span every tokio thread retrying a send to a gone
  mDNS updater; the iroh-address-lookups fork now pins the unreleased
  upstream fix.
- Bounded what a ping, a digest, a blob serve, and the reassembly buffers
  can cost the receive path; bounded the channel orphan buffer; evictions
  come from the fullest author.
- Directed frames no longer reach peers they are not addressed to; documents
  sync over a rendezvous-only link; multihop cells relay only when the route
  names this node.
- Many WebRTC/JSEP hardening fixes: session slots reserved before their
  driver spawns, duplicate sessions refused without killing the survivor,
  STUN replies matched by transaction id, stray datagrams tolerated during
  the handshake, ICE candidates classified by `typ`.

## [0.5.0] - 2026-07-31

The state of the engine at its extraction from the mallorca repo, as
`feat: extract fofoca from the mallorca repo` (86bd79d). Provenance and the
recorded fork changes live in [FORKED.md](FORKED.md).

[0.6.0]: https://github.com/fofoca-network/fofoca/compare/86bd79d...v0.6.0
[0.5.0]: https://github.com/fofoca-network/fofoca/commit/86bd79d
