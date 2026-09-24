# Transport throughput

`cargo task benchmark` moves one bulk transfer over one QUIC bi-stream
between two iroh endpoints, in every pairing the WebRTC lane has, and reads
the result against two ceilings: plain iroh on UDP, and a bare data channel
with no QUIC in it. The question it was built to answer: is the
iroh↔WebRTC integration the bottleneck, and is the double encryption (DTLS
outside, QUIC TLS inside) the reason?

The short answer: **no, and no**. Browser↔browser, fofoca reaches the data
channel's own per-message ceiling. Native↔native over str0m, the cost is one
UDP syscall per packet in the driver. The inner encryption is under 2% of
native CPU, and about a quarter of the browser tab's wall time — half of its
busy time — but the tab is half idle, so it does not set the throughput.

## Method

- One transfer per round, on a fresh connection (the server serves one
  stream, then parks on `closed()`). One warm-up round is discarded because
  the first transfer pays the congestion-window ramp (measured: 34 Mbit/s on
  the raw channel's first round, 460 Mbit/s after).
- Timed on the receiving side only, from stream open to the last verified
  byte. The JSEP round is timed separately (`JSEP ms`) and is not in the
  window.
- Every cell asserts the path that carried it (`webrtc` / `ip` /
  `data-channel`), so a cell cannot quietly measure the wrong lane.
- Browser cells run in two separate Chrome-for-Testing processes, driven
  over CDP; this runner ferries the SDP between them. Host-only ICE, no
  STUN.
- Protocol and pages: `fofoca_iroh_webrtc_transport::bench`,
  `crates/fofoca-bench-wasm`. Runner: `tasks/src/bench.rs`.

```sh
cargo task benchmark                         # the matrix, 8 MiB × 5 rounds
cargo task benchmark --only native --rounds 20 --json target/bench.json
cargo task benchmark --direction up          # or `both`
```

## Results

Apple M5, macOS 27.0, rustc 1.95.0, Chrome for Testing 152.0.7977.42,
commit `1ce7fb1` plus this branch. 8 MiB downloads, 5 timed rounds, two
runs. `Mbit/s` is decimal megabits per second, the median of the rounds;
the range is min–max across both runs.

| cell | path | Mbit/s run 1 | Mbit/s run 2 | range | JSEP ms |
|---|---|---|---|---|---|
| fofoca web-web | webrtc | 242 | 234 | 216–247 | ~1000 |
| fofoca web-native | webrtc | 163 | 149 | 147–170 | ~630 |
| fofoca native-native | ip | 1787 | 1703 | 1216–1812 | 0 |
| fofoca native-native (webrtc-only) | webrtc | 135 | 182 | 120–247 | 5 |
| iroh native-native (baseline) | ip | 1778 | 1787 | 1753–1804 | 0 |
| webrtc web-web (raw, 64 KiB msgs) | data-channel | 474 | 477 | 461–487 | ~680 |
| webrtc web-web (raw, 1200 B msgs) | data-channel | — | 257 | 226–285 | ~690 |

Notes on the cells:

- `fofoca native-native` is the engine's wiring for two native peers: the
  WebRTC transport is registered beside UDP and the selector prefers direct
  IP. It equals the iroh baseline, so registering the transport costs
  nothing on the UDP path.
- `fofoca native-native (webrtc-only)` is str0m at both ends. It is the
  only native cell that goes through the data channel, and it has the
  widest spread.
- The raw channel is ordered and reliable (the default). The transport's
  channel is unordered and unreliable, with QUIC doing the recovery. The
  64 KiB row is what the browser can move; the 1200 B row is the same
  channel used the way the transport uses it, one QUIC datagram per SCTP
  message. The 1200 B raw row was added after run 1.

## Reading

**Browser↔browser.** fofoca (234–242 Mbit/s) equals the raw channel at
1200-byte messages (240–257 Mbit/s). The integration adds nothing
measurable on top of the channel's per-message cost. The channel at
64 KiB messages is twice as fast, so the lever is the number of messages,
not what is in them: larger QUIC datagrams on the WebRTC path would halve the
message count per byte. That is an MTU question for the transport and
iroh's path config, not a crypto one.

**Native (str0m).** 135–182 Mbit/s, below the browser, with the widest spread.
A 15 s `sample` of the runner during the webrtc-only cell (80 rounds)
puts the driver task's busy time at roughly: `sendto` 70%, `recvfrom` 12%,
`str0m::Rtc::poll_output` 7%, and both AEADs together (ring `aes_gcm_*` for
QUIC, aws-lc `aesv8_gcm_*` for DTLS) under 2%. The cost is one blocking
`UdpSocket::send_to` per SCTP packet in `native/driver.rs`; batching sends
is the lever there.

**Browser CPU.** A 12 s Chrome sampling profile of the client tab during
web-web (attached over one long-lived CDP session; `agent-browse cdp` is
one-shot and cannot hold profiler state):

| share of wall time | what |
|---|---|
| 50% | idle |
| 27% | QUIC packet protection: `aes_nohw_encrypt_batch`, `aes_nohw_sub_bytes`, `gcm_mul64_nohw` |
| 15% | other wasm (5% is `curve25519` handshakes, an artefact of the fresh connection per round) |
| 5% | `(program)` |
| 3% | JS glue |

`ring` has no hardware AES on wasm32, so the inner layer is bit-sliced
software AES-GCM. It is about half of the tab's busy time. Removing it
would cut renderer CPU, which matters under main-thread pressure and on
slow devices, but on this machine the tab is half idle and the number does
not move: the ceiling is in Chrome's network process, where SCTP and DTLS
run, and the tab profile does not see it.

## What this says about removing the inner encryption

- It is not what limits throughput in any cell.
- On native it is noise (<2%).
- In the browser it is real CPU (~27% of wall time on the receiving tab)
  but not the bottleneck. If it is done, the gain to claim is CPU, not
  Mbit/s, and it must be measured under pressure (`cargo task e2e` has the
  pressure axis) rather than here.
- The cheaper wins this table points at: larger datagrams on the WebRTC
  path (browser and native), and batched sends in the str0m driver
  (native).
