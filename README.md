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

Dependencies point strictly downward, except `fofoca-blobs`, which sits *above*
the engine because it needs endpoint construction.

```
fofoca-util          host helpers, no deps of consequence   (13 crates resolved)
  └── fofoca-protocol    wire vocabulary, + iroh-base      (138 crates)
        ├── fofoca-doc          shared-state CRDT channels
        ├── fofoca-logging      tracing sink + filter
        ├── fofoca-reassembly   multipart body reassembly
        ├── fofoca-directory    discovery advertisement codec
        └── fofoca         the engine, + iroh, iroh-gossip (436 crates)
              ├── fofoca-ffi     the C ABI
              └── fofoca-blobs   out-of-band content-addressed transfer
```

The load-bearing property: **only `fofoca` and `fofoca-blobs` name `iroh`**.
`fofoca-protocol` builds on `iroh-base` alone and pulls no tokio, QUIC, TLS or
DNS; `-doc`, `-logging`, `-reassembly` and `-directory` inherit that.

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

- [`iroh-multihop-transport`](https://github.com/fofoca-network/iroh-multihop-transport)
  — source-routed multi-hop iroh transport. Split out because it has no fofoca
  dependency; consumed here as a git dependency.
- [`iroh`](https://github.com/fofoca-network/iroh) and
  [`iroh-gossip`](https://github.com/fofoca-network/iroh-gossip) — forks carrying
  two unreleased fixes, pinned by rev in `[patch.crates-io]`. See
  **Patch pins — do not drop** in [`FORKED.md`](FORKED.md).

## Build and test

```bash
cargo check --workspace
cargo test  --workspace          # 22 suites
```

The `mdns` and `dht` features (default on) gate iroh's discovery closure, and
`async-io` on `fofoca-util` gates its only tokio use, so the off positions are
worth checking too:

```bash
cargo check --workspace --no-default-features
cargo check --workspace --all-features
```

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
