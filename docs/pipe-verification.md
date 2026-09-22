# fofoca-pipe web support — verification matrix

The pipe has three surfaces: the `fofoca-pipe` binary
(`crates/fofoca-pipe-cli`), the web app (`packages/fofoca-pipe-web`) and the
`seq` wire under both (`crates/fofoca-pipe`). The automated proof is one
scenario. This document is the rest: every combination worth checking, the
exact commands, the expected result, and what happened on the last run.

Rows the machine already proves say so in the **Automated** column. A person
spends time where the machine cannot go: a TTY, SIGINT, a second browser, a
real WebMCP browser, a deployed host.

## 0. Setup

```sh
cargo build -p fofoca-pipe-cli            # target/debug/fofoca-pipe
cargo task wasm-peer                      # the wasm glue, once
bun run --cwd packages/fofoca-pipe-web serve   # http://127.0.0.1:3020/
export FOFOCA_PIPE_WEB=http://127.0.0.1:3020/
```

You need two terminals (T1 sends, T2 receives) and one or two tabs (A, B).
A bare `fofoca-pipe` mints a public mesh on the default relay ladder, so a
manual run needs no local relay.

How to read the page: `#status` shows `<nick> · <mesh name>` once open;
`#webmcp` says whether the tools registered; `#streams` holds one `<pre>`
per (sender, directed) stream — `(direct)` marks a directed stream, `(you)`
your own composer sends, `⏹ end of stream` a completed one. In DevTools,
`window.pipe` has the same functions the WebMCP tools wrap.

**Pipe whenever you like.** The CLI waits for a peer the roster shows as
`unicast` before it reads stdin, and says so (`* <nick> can receive;
streaming stdin`). On a public mesh that takes 40–130 s. `--no-wait` skips
the wait, and then the opening frames go to nobody.

### Automated coverage

| Suite | Proves |
| --- | --- |
| `cargo test -p fofoca-pipe` | the `<seq>:<base64>` and `pipe_ack` codecs at every alignment, the old body refused, the reorder (shuffled, duplicate, early EOF, post-EOF continuation, gap skip at 256 frames and at 3 s), the send window (slowest receiver, directed stream, peer left, giving up on a silent one) |
| `cargo test -p fofoca-pipe-cli` | bare = public, selectors turn it off, `--mesh`/`--topic` exclusive, the fragment URL and its percent-encoding, which roster lanes count as "can receive" |
| `bun test packages` | the TS reorder twin, `PipeRuntime` (order, consume-once, multibyte straddle, long-poll, abort, cap), the seven tools' `execute` |
| `cargo task e2e --suite pipe` | CLI → page: three frames with an emoji across a frame boundary, byte-exact; `pipe.read` as one item; page → CLI stdout; EOF → `data-complete`; exit 0 |
| `cargo task e2e --suite mesh --quick` | relay lookup-only vs transport, join by topic vs id, native transport sets, all lanes both ways |

## 1. Matrix

Result column: ✅ pass, ❌ fail, ➖ not run, with a note. Last run: see §4.

### P — payload shapes

T1 sends, tab A shows.

| ID | Steps | Expected | Automated | Result |
| --- | --- | --- | --- | --- |
| P1 | `fofoca-pipe --no-wait </dev/null` | page shows an empty `<pre>` already complete (`received: 0, complete: true` in `pipe.status()`); CLI exits 0 | — | ✅ |
| P2 | `printf x \| fofoca-pipe` | `x`, then complete | pipe e2e (larger) | ➖ covered by P4 |
| P3 | `yes \| head -c 2094 \| fofoca-pipe` (exactly one frame) | 2094 bytes | — | ✅ |
| P4 | `seq 1 30000 \| fofoca-pipe` (~81 frames) | the `<pre>` text equals the file, byte for byte | — | ✅ 168,894 bytes byte-exact, 83 frames, `pipe_read` returned it as one item |
| P5 | `head -c 5000 /dev/urandom \| fofoca-pipe` | U+FFFD noise on the page (a text surface); the stream stays usable afterwards | — | ✅ noise, then P4-style text after it landed intact |
| P6 | tab A: `pipe.send('a')`, `sendEof()`, `send('b')` | T2 in receive mode gets `a` and exits; T2 with `--stay` gets `ab` | runtime.test (TS) | ✅ both |
| P7 | multibyte character across a frame boundary | whole character | pipe e2e | ✅ suite green |

### B — browsers as runtimes

| ID | Steps | Expected | Automated | Result |
| --- | --- | --- | --- | --- |
| B1 | tab B: `await pipe.read(20000)`; tab A: `pipe.send('hi')` | B resolves with one item from A; the next `read(0)` is empty | runtime.test | ✅ consume-once seen live (a `pipe_read` through WebMCP emptied the buffer for `window.pipe.read`) |
| B2 | A: `pipe.send('psst', '<nick>')` | only the addressee shows `(direct)`; nobody else sees it | mesh e2e (directed lane) | ✅ to a `unicast` peer; to a `relay-only` one the frame is parked by the mesh until a path forms (documented in the tool description) — see F2b |
| B3 | A, B and T1 all send | one `<pre>` per sender on every tab, texts never interleave | — | ✅ `terminal` and `chrome-a` streams separate on Safari |
| B4 | `pipe.status()` | `streams` lists each sender with `received`/`complete`; `unread` matches the next `read` | runtime.test | ✅ `unread: 83` before the read, 0 after |
| B5 | A: `pipe.stateMerge({k:1})`; B: `pipe.stateGet()` | converges within seconds | mesh e2e (state lane) | ✅ Chrome → Safari in < 8 s, even relay-only |
| B6 | `read(0)` with nothing pending; `read(3000)` | at once `{items: []}`; after ~3 s `{items: []}` | runtime.test | ✅ 3.36 s |

### J — join modes and relay policy

| ID | Steps | Expected | Automated | Result |
| --- | --- | --- | --- | --- |
| J1 | open the printed `#mesh=<id>` URL | `#status` shows the CLI's mesh name | pipe e2e | ✅ |
| J2 | `?mesh=<id>` (query fallback) | same | — | ➖ |
| J3 | `fofoca-pipe --topic t` and `#topic=t` | one mesh, data flows | mesh e2e (topic) | ✅ two CLIs and Chrome on one topic |
| J4 | `--topic "tea time&more" --web-url …` | prints `#topic=tea%20time%26more`; the page joins | args test | ✅ printed; join ➖ |
| J5 | `printf x \| fofoca-pipe --no-wait`, open the page after | page ready, no `x` — frames are not stored; later frames appear within `GAP_TIMEOUT` | — | ✅ nothing arrived; with F3 fixed the rest no longer stalls |
| J6 | `--topic t --relay-transport` with `#topic=t&relayTransport=1` | meets; without the query flag, two meshes | mesh e2e (policy) | ➖ |
| J7 | `--relay-url <url>` with `?relay=<url>` | meets over the custom relay | pipe e2e (local relay) | ✅ suite green |

### C — CLI lifecycle

| ID | Steps | Expected | Automated | Result |
| --- | --- | --- | --- | --- |
| C1 | send mode, stdin ends | EOF marker out, `⏹ end of stream` on the page, exit 0 | pipe e2e | ✅ |
| C2 | `--stay` | keeps running after EOF; page sends still land on stdout | — | ✅ |
| C3 | T2: `fofoca-pipe --topic t > out.txt` (TTY stdin = receive mode) | exits 0 after the page's `sendEof()`; `out.txt` holds the text | — | ✅ (run under `script -q /dev/null …` for the pty) |
| C4 | Ctrl-C | departs, exit 130 | — | ✅ `kill -INT` → 130 |
| C5 | `fofoca-pipe --mesh a --topic b` | usage error, exit 2 | args test | ✅ `the argument '--mesh <MESH>' cannot be used with '--topic <TOPIC>'`, exit 2 |
| C6 | `--relay-url not-a-url` | error naming the URL, exit 1 | — | ✅ `invalid relay URL "not-a-url": Failed to parse relay URL`, exit 1 |
| C7 | `--robot` | first stderr line `{"kind":"open","mesh","nick","url"}`, then engine events verbatim | pipe e2e | ✅ |
| C8 | no `--web-url`, no `FOFOCA_PIPE_WEB` | `mesh <id>` plus the `#mesh=` hint, no URL line | — | ✅ |
| C9 | stdout closed early: `fofoca-pipe --stay </dev/null \| head -c 1`, then the page sends twice | the CLI exits non-zero with a stdout error, not a hang | — | ✅ `Error: writing to stdout / Broken pipe (os error 32)`, exit 1 |

### W — WebMCP

| ID | Steps | Expected | Automated | Result |
| --- | --- | --- | --- | --- |
| W1 | a browser without a model context (Safari 27, Firefox) | `#webmcp` reads `no WebMCP in this browser — window.pipe only`; `window.pipe` works | webmcp.test | ✅ Safari 27.0 |
| W2 | Chrome for Testing 152 (has `document.modelContext` on by default) | `#webmcp` reads `WebMCP tools registered`; `await document.modelContext.getTools()` lists the seven | webmcp.test (fake) | ✅ CfT 152.0.7977.42; `navigator.modelContext` is absent there |
| W3 | in that Chrome: `mc.executeTool(tool, JSON.stringify({waitMs: 0}))` where `tool` is from `getTools()` | the tool's result | — | ✅ `pipe_status`, `pipe_read`, `pipe_peers`, `pipe_state_get` returned; note the argument is a JSON **string** and the result comes back as a JSON string of `{content:[…]}` |
| W4 | a browser agent asked to "read the pipe" | it calls `pipe_read`; its text matches the `<pre>` | — | ➖ no agent client available here |
| W5 | scripted: Puppeteer ≥ 24.41 listing and calling the tools | seven tools; status JSON | — | ➖ |

### D — deployed

| ID | Steps | Expected | Automated | Result |
| --- | --- | --- | --- | --- |
| D1 | `bun run --cwd packages/fofoca-pipe-web build`; `python3 -m http.server -d packages/fofoca-pipe-web/dist 8080`; open `http://127.0.0.1:8080/#topic=t` | page opens, `wasm/fofoca_wasm.js` and `wasm/fofoca_wasm_bg.wasm` load from `dist/wasm/` | — | ✅ |
| D2 | the same over HTTPS on the real host | joins and links from a non-localhost origin | — | ➖ no host yet |

## 2. Findings

All fixed on 2026-09-21 except where noted; each names its test.

**F1 — the CLI read stdin at `joined`, before frames could reach the tab.
Fixed.** `seq 1 30000 | fofoca-pipe` with a tab: the CLI logged `joined`,
streamed all 81 frames plus EOF, exited 0, and the tab received nothing.
`joined` is presence in the gossip overlay, which arrives long before a
payload path. The CLI now polls the roster every 500 ms and starts stdin
only when a peer shows `transport: "unicast"` — `multihop` is the engine's
guess before a path is proven, and a stream sent against it was still lost
(measured: a `multihop` peer for 90 s before `unicast`). Tests:
`payload_ready_peer` in `crates/fofoca-pipe-cli/src/main.rs`; the pipe e2e
now writes stdin **before** the link, the way a person pipes.

**F2 — a browser judged its own need for the WebRTC lane from its endpoint
address. Fixed.** A tab's address is empty whenever its relay link is down,
and `needs_webrtc_lane` reads empty as "unknown, so not a browser" — correct
for a remote peer, wrong for oneself. A tab that was the lower id therefore
skipped the lane against a native peer and stayed `relay-only` for good
(seen in Safari 27: 5 minutes, every probe `no direct path within the probe
deadline`). The pair decision and the rendezvous offer now read the node's
own transport set (`EventLoopState::local_ip_transport`); the retry pass
deliberately still reads the address, because judging it by the node there
made a tab re-offer to every native each tick and collide with the native's
own dial (the chat e2e never linked). Test:
`a_node_without_ip_needs_the_lane_even_while_its_own_address_is_empty` in
`crates/fofoca/src/transport/webrtc.rs`.

F2 is a correctness fix against the code's own documented invariant
("either end lacking IP is enough"), proven by a test that fails on the old
rule. It did **not** change any observed outcome, so treat it as a
correctness fix, not a cure for F2b.

**F2b — Safari still does not form a direct path.** Open, and not the same
bug: with F2 fixed, Safari 27.0 offers the lane and still ends `relay-only`
against both a terminal and a Chrome tab on the same machine. Chrome for
Testing 152 links in 40–130 s on the same setup. Broadcasts and state still
flow over gossip; directed frames do not. Use Chrome until this is found.

**F8 — the chat e2e suite is failing on this machine for reasons outside
the pipe.** Open, pre-existing. `cargo task e2e --suite chat --quick` went
2/2 green early in the session and 0/5 later the same evening, with the
browser's roster empty (`page peers: []`) and the terminal cycling `beacon
rival re-check: releasing the rendezvous to re-probe for a same-id
co-host`. **Unmodified `main` fails the same way, 0/2, in the same
conditions** — so it is the machine or the local beacon arbitration, not
this branch. The mesh and pipe suites (same relay, same browser) stay green
throughout. Anyone bisecting a chat failure should run `main` first.

**F3 — a missed opening frame stalled a stream until 256 later frames.
Fixed.** A hole older than `GAP_TIMEOUT` (3 s) is now skipped and the frames
behind it are released, on both sides: `Reorder::expire` / `Streams::expire`
in `crates/fofoca-pipe/src/reorder.rs`, and `GAP_TIMEOUT_MS` in
`packages/fofoca-api/src/reorder.ts`. The CLI ticks it every 500 ms; the
browser runtime does the same. Tests: four in each reorder module, plus
`a stream joined late is released once its hole is old` in
`packages/fofoca-pipe-web/src/runtime.test.ts`.

**F4 — `window.pipe.send` did not echo into the DOM. Fixed.** Every send
through the runtime — composer, `window.pipe` or a WebMCP tool — now lands
in the `(you)` stream (`PipeRuntime.onSent` / `onSentEof`). Tests:
`onSent reports every send this runtime made`, and the pipe e2e asserts the
tab shows its own send.

**F5 — CfT 152 ships `document.modelContext` on by default**, with
`getTools`, `executeTool`, `registerTool`, `ontoolchange`. Two API facts,
now in the package README: `executeTool` takes the `RegisteredTool` object
(a name string is a `TypeError`) and a JSON **string** of arguments (an
object is `Failed to parse input arguments`).

**F6 — a stream overran every queue on the path. Fixed.** Found while
measuring: 1 MB browser→browser arrived in 12–21 s with frames missing at
`Lagged` (the gossip subscription), the pipe's 256-slot inbound queue, and
the data channel's 1 MB send buffer (`dropping outbound datagrams`). The
pipe now paces itself: a receiver acks every 8 frames and at EOF
(`pipe_ack`), and a sender waits while more than 64 frames are
unacknowledged by any peer with a proven lane (`fofoca_pipe::Flow`, exposed
as `Session::flow`; all three senders — CLI, wasm, C ABI — wait). A receiver
silent for 2 s stops being waited for, so a dead or old-wire peer cannot
stall a stream. Same transfer after: 1.2 s, lossless. Tests: five in
`crates/fofoca-pipe/src/flow.rs`; the pipe e2e payload is now twice the
window.

**F7 — a tab's throughput degrades with sustained transfers.** Open. On one
mesh, 1 MB browser→browser measured 848 KB/s, then 163 KB/s on the third
run of the session; reloading both tabs restored 866 KB/s on the same mesh
with the same peers. So the loss is per-session state in the tab (the peer
connection or the wasm peer), not the mesh. A 5 MB transfer left the
sessions detaching (`peer connection Failed after grace`, `silent
partition`). Worth a look before anyone streams continuously.

## 3. Throughput

Measured 2026-09-21 on one machine (macOS 27, M-series), public topic mesh
over the default relay ladder, all peers `unicast`. Chrome for Testing
152 tabs; the native peer is `fofoca-pipe --stay`. Each number is one
transfer of 1 MB of ASCII text, lossless (the receiver's byte count equals
the sender's). "Stream" is first chunk to EOF at the receiver.

| Path | Stream | Throughput | Notes |
| --- | --- | --- | --- |
| browser → browser (`window.pipe`) | 1.18 s | **866 KB/s** | 502 `read` calls, 513 frames |
| native → browser (CLI stdin → tab) | 1.41 s | **726 KB/s** | end-to-end 2.4 s including the file write |
| browser → native (tab → CLI stdout) | 2.16 s | **473 KB/s** | directed send; stdout byte-exact |
| browser → browser **through WebMCP** | 3.97 s | **258 KB/s** | 16 `pipe_send` calls of 64 KB, driven from a native process over CDP; the receiver read through `pipe_read` |

The WebMCP path costs about 3× the direct one: every call crosses the
browser's tool boundary with its arguments and result as JSON strings, and
`pipe_read` returns the text of everything buffered since the last call.
Chunk size barely matters (1 call of 1 MB measured the same as 16 of 64 KB)
— the cost is the JSON round trip per call, not the number of calls.

Smaller transfers are latency-bound, not throughput-bound: 100 KB
browser→browser took 42 ms (2.4 MB/s) on a fresh session.

See F7: repeat runs within one tab session fall well below these numbers.

## 4. Run record

| | |
| --- | --- |
| Date | 2026-09-21 |
| Commit | d9434ee + the uncommitted pipe branch |
| OS | macOS 27.0, arm64 |
| Browsers | Chrome for Testing 152.0.7977.42 (via agent-browse), Safari 27.0 |
| Mesh | public preset on the default relay ladder (bare `fofoca-pipe`, `--topic`) |
| Not run | J2, J6, W4, W5, D2, and the join half of J4 |
| Re-run after the fixes | P4, B1, B4, C1, C7, W2, W3 — all pass; `cargo task ci` (1028 tests), `bun test packages` (144), and the pipe and mesh e2e suites are green. The chat suite fails here, and so does unmodified `main` — see F8. |
| Throughput | §3, measured on the fixed build |

Rows run: P1 P3 P4 P5 P6 · B1 B2 B3 B4 B5 B6 · J1 J3 J4 J5 · C1–C9 ·
W1 W2 W3 · D1. Fails: B2 to Safari (F2). Everything else passed, three
of them only after the workaround for F1 (write stdin after the link).
