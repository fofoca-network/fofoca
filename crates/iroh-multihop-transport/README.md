# iroh-multihop-transport

A source-routed, multi-hop [iroh](https://github.com/n0-computer/iroh) **custom
transport**. It lets a peer reach a destination it cannot dial directly by
relaying the destination's QUIC packets through intermediate peers.

Registered on an iroh endpoint via the `unstable-custom-transports` seam, iroh
runs its full QUIC state machine end-to-end, so the two endpoints share a real
`iroh::endpoint::Connection` — streams, congestion control, path migration, and
QUIC-TLS end-to-end secrecy. Intermediate relays only forward opaque,
already-encrypted packets.

## Shape

- `Topology` — the routing brain: a metric-weighted link-state graph fed by
  peers' `LinkVector`s, producing node-disjoint source routes.
- A computed route is packed into a `CustomAddr` and travels with the
  connection; the crate's address lookup resolves a target endpoint id to one.
- `MultihopHandle` — owns a dedicated **underlay** iroh endpoint that carries
  packets hop-by-hop, and wires the transport onto an application endpoint via
  its `Preset` impl.

## Status

Reachability-first (v1): relays forward encrypted QUIC packets; per-hop onion
anonymity is a future addition. End-to-end confidentiality comes from QUIC-TLS.

## Provenance

It briefly had its own repo, on the reasoning that it has no fofoca dependency
and its audience is any iroh user. That is still true, and it still names only
crates.io versions of `iroh`/`iroh-base` — but a second repo bought nothing and
cost a rev pin to bump on every change, so it lives here again. Nothing in this
workspace is allowed to leak into it.

A consumer that patches `iroh` to a fork must patch `iroh-base` to the same repo
and rev, or the graph ends up with two `iroh_base` versions and `CustomAddr`
stops unifying (E0308). The `1.0.1` floor in `Cargo.toml` exists so that a
consumer patching to a 1.0.1 fork still gets its patch applied; see the comment
there before raising it.
