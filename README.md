# fofoca

A serverless gossip-network engine. Peers find each other over mDNS, the mainline
DHT or a relay, form a mesh, and exchange messages and a shared CRDT document —
with no server in the middle.

It is built on [iroh](https://github.com/n0-computer/iroh) for transport and
[automerge](https://automerge.org) for shared state, and it is embeddable: the
event loop runs on a tokio runtime inside the calling process, so joining a mesh
costs a function call rather than a daemon.

Its first non-Rust consumer is [mallorca](https://github.com/dviramontes/mallorca),
an Odin application that links [`fofoca-ffi`](crates/fofoca-ffi) as a static
library and joins a mesh from its own process.

## The crates

Dependencies point strictly downward.

```
fofoca-util          host helpers, no deps of consequence   (13 crates resolved)
  └── fofoca-protocol    wire vocabulary, + iroh-base      (138 crates)
        ├── fofoca-doc          shared-state CRDT channels
        ├── fofoca-logging      tracing sink + filter
        ├── fofoca-reassembly   multipart body reassembly
        ├── fofoca-directory    discovery advertisement codec
        └── fofoca         the engine, + iroh, iroh-gossip (436 crates)
              └── fofoca-ffi     the C ABI

fofoca-blobs                      verified byte ranges, + bao-tree, blake3
fofoca-iroh-webrtc-transport      QUIC over a WebRTC data channel, + iroh
fofoca-iroh-multihop-transport    QUIC relayed through peers, + iroh
```

The bottom three are standalone: they depend on nothing else here.

The load-bearing property: **only `fofoca` names `iroh`**. `fofoca-protocol`
builds on `iroh-base` alone and pulls no tokio, QUIC, TLS or DNS; `-doc`,
`-logging`, `-reassembly` and `-directory` inherit that.

[`fofoca-blobs`](crates/fofoca-blobs) is a BLAKE3/bao store of verification
metadata — outboards, root bindings, which ranges are held — for bytes that live
wherever the caller already keeps them, so a peer can serve verified ranges of a
file it did not have to copy first.

[`fofoca-iroh-webrtc-transport`](crates/fofoca-iroh-webrtc-transport) carries
QUIC datagrams over a WebRTC data channel as an iroh custom transport. It is how
a browser reaches a peer at all: a tab has no UDP socket, so iroh's own paths do
not exist there. One crate, two mutually exclusive backends behind features —
`native` (sans-io str0m on tokio) and `web` (the browser's own
`RTCPeerConnection`) — sharing one protocol half, because two peers that
disagree about the transport id or the envelope shape fail to connect with no
useful error.

[`fofoca-iroh-multihop-transport`](crates/fofoca-iroh-multihop-transport) is the
other custom transport: source-routed relaying through intermediate peers, for
when no direct path exists at all.

None of the three knows what a mesh is, and the engine takes only the multihop
one. The other two meet it in a consumer.

That is the whole reason the split exists. Cargo features cannot be selected
per-consumer across a dependency edge, so a consumer that wants the wire
vocabulary without the network stack needs a crate boundary, not a feature flag.
[`docs/mesh-slimming.md`](docs/mesh-slimming.md) has the measurements and the
p2panda-derived rules the split follows — the engine was 39.4 MiB of mallorca's
40.7 MiB release binary before it.

The crates are versioned in lockstep from `[workspace.package]`. They were carved
out of one engine to control the dependency closure, not to be released on
separate cadences.

## Related repos

- [`iroh`](https://github.com/fofoca-network/iroh) and
  [`iroh-gossip`](https://github.com/fofoca-network/iroh-gossip) — forks carrying
  two unreleased fixes, pinned by rev in `[patch.crates-io]`. See
  **Patch pins — do not drop** in [`FORKED.md`](FORKED.md).

## Build and test

```bash
cargo check --workspace
cargo test  --workspace          # 29 suites, 518 tests
```

The `mdns` and `dht` features (default on) gate iroh's discovery closure, and
`async-io` on `fofoca-util` gates its only tokio use, so the off positions are
worth checking too:

```bash
cargo check --workspace --no-default-features
cargo check --workspace --all-features
```

## The browser

The engine runs in a tab. `--no-default-features` drops `host` and leaves the
portable half — gossip, the CRDT documents, the protocol and identity types,
address lookup, and the whole node runtime — so a browser peer is the same peer
a CLI runs, not a reduced stand-in. What it loses is the control socket, the
session state file, the process helpers and the log sink, none of which have a
wasm32 equivalent.

Three crates reach that target and CI checks each:

```bash
rustup target add wasm32-unknown-unknown
cargo check --target wasm32-unknown-unknown -p fofoca --no-default-features
cargo check --target wasm32-unknown-unknown -p fofoca-blobs --all-targets
cargo check --target wasm32-unknown-unknown -p fofoca-iroh-webrtc-transport --features web
```

`cargo check` is not enough on its own, which is why
[`crates/fofoca/tests/wasm_runtime.rs`](crates/fofoca/tests/wasm_runtime.rs)
exists: `std::time::Instant::now()`, `tokio::time` and `tokio::spawn` all
compile for wasm32 and then panic at runtime. Running it needs a wasm-capable
clang for `ring`'s C core, so it is a compile check in CI and a real run
locally:

```bash
CC=$(brew --prefix llvm)/bin/clang CC_wasm32_unknown_unknown=$(brew --prefix llvm)/bin/clang \
  cargo test -p fofoca --no-default-features --target wasm32-unknown-unknown
```

`fofoca-blobs`'s eight OPFS tests need a real browser and are not in CI:
`wasm-pack test --headless --chrome crates/fofoca-blobs`.

To build the C ABI as a static library:

```bash
cargo build --release -p fofoca-ffi   # -> target/release/libfofoca_ffi.a
```

[`crates/fofoca-ffi/include/fofoca.h`](crates/fofoca-ffi/include/fofoca.h) is the
hand-written declaration a C caller compiles against, and the counterpart of
`crates/fofoca-ffi/src/ffi.rs`. Change one, change the other; CI asserts the
archive actually exports everything the header declares.

## Provenance

A hard fork of [agent-habilis/agent-gossip](https://github.com/agent-habilis/agent-gossip).
[`FORKED.md`](FORKED.md) records every divergence and maps upstream paths to
their homes here; [`VENDORED.md`](VENDORED.md) is the superseded original
vendoring contract, kept for the record.

## License

MIT — see [`LICENSE`](LICENSE).
