# fofoca-iroh-webrtc-transport

An iroh custom transport carrying QUIC datagrams over a WebRTC data channel —
on the host and in the browser. One data channel per remote peer, one QUIC
datagram per binary SCTP message, no extra framing. The channel is negotiated
unreliable + unordered (`maxRetransmits: 0`): QUIC above it owns loss recovery
and congestion control, so the channel stays a plain datagram pipe instead of
stacking a second retransmission loop.

Structure follows `iroh-multihop-transport` and the upstream
[iroh-tor-transport](https://github.com/n0-computer/iroh-tor-transport): a
transport factory, a per-endpoint receiver, a registry-backed sender, and a
per-session driver pumping the connection.

## One crate, two backends

`str0m` and `tokio` do not target `wasm32`; `web-sys` does not exist off the
browser. The *drivers* therefore cannot be one implementation. But the protocol
— the JSEP envelope, the transport id, the address convention — is identical,
and it is exactly the part that must not drift: two peers disagreeing about the
transport id or the envelope shape fail to connect with no useful error.

So the protocol half lives at the crate root, always compiled and free of host-
and browser-only dependencies, and each backend sits behind a feature. Neither
is on by default.

| | `native` | `web` |
|---|---|---|
| ICE / DTLS / SCTP | str0m (sans-io) | browser built-in |
| driver | tokio task | `spawn_local` pump |
| candidate gathering | `stun` module | browser ICE agent |
| backpressure | mpsc queue | `bufferedAmount` |

```bash
cargo build -p fofoca-iroh-webrtc-transport --features native
cargo build -p fofoca-iroh-webrtc-transport --features web --target wasm32-unknown-unknown
```

In the experiment this was ported from, `SignalEnvelope` and
`WEBRTC_TRANSPORT_ID` were written out twice — once per crate — under a comment
asking the next reader to keep them in lockstep by hand. Deleting that
duplication is the point of this layout, and `grep -rn 0x5752_5443` returning a
single hit is the test.

## NAT traversal

`str0m` gathers no candidates: it owns no sockets, so discovery is the caller's
job. Skipping it means advertising a single host candidate on a private
interface, which connects on one LAN and nowhere else.

`native::stun` closes that with an RFC 5389 Binding Request out of **the same
socket the media will use** — a NAT maps per source port, so a mapping learned
on any other socket describes a hole the data will not arrive through. The
result becomes a `server_reflexive` candidate alongside the host one. Configure
with `IceConfig`; `IceConfig::host_only()` keeps the tests offline.

The browser needs none of this: `IceServers` goes into the `RtcConfiguration`
and the platform's ICE agent gathers for free. Prefer
`IceServers::default()` — STUN only. **TURN is refused** by `accept_ice_uri`:
this project relays through its own iroh relay, so a peer that cannot pair
directly falls back there rather than to a second relay at the ICE layer.

## Building for wasm on macOS

`ring`'s C core cannot be compiled for `wasm32` by Apple clang. Use Homebrew
LLVM:

```bash
CC=/opt/homebrew/opt/llvm/bin/clang \
CC_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/clang \
AR=/opt/homebrew/opt/llvm/bin/llvm-ar \
cargo check -p fofoca-iroh-webrtc-transport --features web --target wasm32-unknown-unknown
```

