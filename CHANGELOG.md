# Changelog

All notable changes to the fofoca workspace. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); all member crates
share one version and move together. A release is a git tag — nothing is
published to a registry; pin it with
`fofoca = { git = "https://github.com/fofoca-network/fofoca", tag = "v0.6.0" }`.

## [Unreleased]

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
