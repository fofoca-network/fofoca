# Slimming fofoca

[`ffi-cost.md`](https://github.com/dviramontes/mallorca/blob/main/docs/ffi-cost.md) measured what the mesh costs mallorca: **39.4 MiB of binary** (96.7% of
it), **+23 MB RSS and +10 threads** to open a room, **~1.15 ms of render-thread time per frame**, and a
**~5 s freeze when joining**. It measured those, but explained none of them.

This document does the explaining, and proposes a crate structure that lets mallorca — or any
embedder — take the mesh without the parts it doesn't use.

**Part 2 has since been implemented** — the engine is now eight crates, and
`fofoca/FORKED.md` records the layout. See [Status](#status-the-split-has-been-done)
for what it measured. Part 1's runtime fixes are not done; they remain the highest-value work.

Every claim below is tagged **[measured]** (from `ffi-cost.md`, scenario named) or **[source]** (read
out of the vendored tree, with `file:line` so you can check it). Nothing read from source is presented
as a measurement.

Paths are relative to `fofoca/crates/` unless stated. Verified against the tree at
mallorca `fa15f0b`, tokio 1.53.1, iroh-gossip fork `c779c06`.

## The short version

Four fixes, all small and all in the FFI shim we already maintain locally, address most of the runtime
cost:

1. `try_recv()` instead of `timeout(Duration::ZERO, ..)` — recovers ~11% of render-thread wall clock.
2. Pass `cohost: Some(Never)` — removes the 5 s join freeze and a whole second iroh endpoint.
3. Cap the tokio runtime at 2 workers — removes ~8 threads and their stacks.
4. Delete two dead deps, feature-gate mDNS/DHT/IPC.

The binary size is a separate problem with a separate answer: **the mesh engine mallorca actually calls
is 0.75 MiB of `__text`; the other ~18 MiB is its dependency closure.** Shrinking that means splitting
the crate so the closure is opt-in, which is Part 2.

---

## Part 1 — Runtime cost, root-caused

### 1.1 The ~1 ms per-call tax

**[measured]** `p2p_poll` costs ~1.15 ms on every frame, with zero peers connected and nothing to read
(scenario C; mean 1146–1186 µs across all mesh scenarios, p50 in the 1279 µs bucket). At ~100 fps that
is ~11% of wall clock.

**[source]** It is not the mesh. It is tokio's timer granularity. `pipe.rs:416-425`:

```rust
pub fn recv(&mut self, timeout: Duration) -> Result<Option<Inbound>> {
    let inbound = &mut self.inbound;
    self.runtime.block_on(async move {
        match tokio::time::timeout(timeout, inbound.recv()).await {
            Err(_elapsed) => Ok(None), ...
```

`ffi.rs:345` maps `timeout_ms = 0` to `Duration::ZERO`. `tokio::time::timeout` builds a `Sleep` from
`Instant::now() + duration` (`tokio-1.53.1/src/time/timeout.rs:92-96`), and every deadline goes through
`TimeSource::deadline_to_tick` (`tokio-1.53.1/src/runtime/time/source.rs:17-20`):

```rust
pub(crate) fn deadline_to_tick(&self, t: Instant) -> u64 {
    // Round up to the end of a ms
    self.instant_to_tick(t + Duration::from_nanos(999_999))
}
```

There is no zero-duration short-circuit. So a "non-blocking poll" registers a timer entry in the wheel,
returns `Pending`, and the calling thread — a *foreign* thread, not a runtime thread — parks on a
condvar until a worker thread fires the timer and unparks it cross-thread. That is the 1.15 ms: ~1 ms of
wheel granularity plus the cross-thread wakeup. **`timeout_ms = 0` and `timeout_ms = 1` cost exactly the
same**, which is a useful thing to know about this API.

**Fix.** `tokio::sync::mpsc::Receiver::try_recv` exists
(`tokio-1.53.1/src/sync/mpsc/bounded.rs:364`) and needs no runtime, no timer, and no `block_on`:

```rust
pub fn recv(&mut self, timeout: Duration) -> Result<Option<Inbound>> {
    if timeout.is_zero() {
        return match self.inbound.try_recv() {
            Ok(frame) => Ok(Some(frame)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(anyhow!("mesh event loop has stopped")),
        };
    }
    // ... existing timeout path
}
```

Expected: ~1.15 ms → sub-microsecond. This is the highest value-per-line change in the study, it lives
entirely in the shim we already diverge from upstream in, and `docs/p2p-protocol.md` §8 already
specifies that frames are drained at a 0 ms timeout — so the fast path is the *only* path mallorca uses.

**Related [source]:** every other FFI call pays **two** `block_on` park/unpark round-trips, one to push
the request (`pipe.rs:490-495`) and one to await the oneshot reply (`pipe.rs:497-501`). No timer is
involved, so these are microseconds when the event loop is idle — but they are also why a busy event
loop shows up directly as caller latency, which is what §1.2 is about.

### 1.2 The 5 s join freeze

**[measured]** Every joining peer blocks for 4.80–5.38 s exactly once, in 13 of 13 runs, in whichever
mesh call it makes first (`p2p_roster_tick` when paused, `p2p_send_own` when playing). The host never
sees it.

**[source]** `beacon/mod.rs:193-215`, the probe-before-claim:

```rust
let budget = Duration::from_secs(HEAL_PROBE_SECS.min(heal_interval_secs()));  // min(5, 15) = 5s
let found_rival = match build_endpoint(&lookups, None, None, Vec::new(), None).await {
    Ok(prober) => {
        let found = probe_connect(&prober, EndpointAddr::new(params.id), budget).await;
```

`HEAL_PROBE_SECS = 5` (`util/tuning.rs:299`). `probe_connect` (`lookup/mod.rs:206-241`) is
`timeout(budget, endpoint.connect(..))`. When nobody is serving the rendezvous — the normal case for the
first peers of a fresh room — the full 5 s elapses. Note it also binds a **throwaway iroh endpoint** per
probe just to make the attempt.

How a joiner gets there: `probes_before_claim` (`event_loop.rs:923`) is true for every policy except
`Eager` (which is `create`), so every joiner probes. `maybe_reclaim` (`event_loop.rs:1354`) is driven by
a **400 ms tick** (`RECLAIM_INTERVAL_MS`, `util/tuning.rs:349`; arm at `event_loop.rs:587`) for a 6 s
window armed by any `NeighborDown` — which is exactly what a joiner sees while the overlay converges.

The probe is `.await`ed **inline on the single event loop**, so it head-of-line blocks `external_req_rx`
(`event_loop.rs:618-626`). That is why the stall is not tied to one call: whichever FFI request is
sitting in the queue eats it. The measurement and the source agree precisely on this point.

**Fix 1 — wire the knob that already exists.** `SetupParams.cohost: Option<CoHostPolicy>` is a real
field (`daemon/setup.rs:231`), applied as `cohost_override.unwrap_or(cohost)` (`daemon/setup.rs:396`).
`pipe.rs:326` hardcodes `cohost: None`. Passing `Some(CoHostPolicy::Never)` makes `maybe_cohost`
(`event_loop.rs:1327`) and `maybe_reclaim` never call `beacon::ensure` — removing the 5 s stall *and*
the second complete `Endpoint` + `Gossip` + `Router` that a beacon stands up (`beacon/mod.rs:216-224`,
`:298-302`).

The cost: that node never serves as a bootstrap beacon for others. For a GUI application embedding the
mesh, that is the right default — mallorca is a client, not infrastructure — but it should be an FFI
option rather than a hardcoded choice, since a long-lived peer might legitimately want to co-host.

**Fix 2 — move the probe off the event loop** into a spawned task that reports its verdict back. Correct
independently of Fix 1, because it removes a whole class of head-of-line blocking rather than one
instance.

**[source] Other inline blocks on the same event loop**, worth knowing about even though the
measurements didn't hit them:

- `transport/pool.rs:24` — `DIAL_TIMEOUT = 3 s` on a directed send to an unreachable peer.
- `mesh_close` costs ≥1.25 s: `node.rs:117` waits up to 3 s, plus `pipe.rs:485` `DEPARTURE_GRACE = 750
  ms` and `event_loop.rs:676` a 500 ms sleep.
- `daemon/setup.rs:107-126` — startup relay-rung confirmation binds **one full extra `Endpoint` per
  rung**, up to 5 rungs × 10 s. Detached, but it overlaps the first seconds of a node's life, which is
  where `ffi-cost.md`'s peak-RSS number comes from.

### 1.3 Threads and memory

**[measured]** Opening a room costs +22.9 MB RSS and +10 threads (C vs B). Merely linking the archive
costs nothing (B ≈ A). A connected peer adds ~2 MB more.

**[source]** Where it goes:

| item | `file:line` | finding |
|---|---|---|
| tokio runtime | `pipe.rs:304-307` | `new_multi_thread().enable_all()` with **no `worker_threads` and no `thread_stack_size`** ⇒ one worker per core (10 here), 2 MiB default stack each, plus a timer wheel and run queue per worker. **There is no knob** — `Opts` (`pipe.rs:248-265`) exposes nothing about the runtime, and `util/tuning.rs:214-225` replaced env vars with compile-time consts that only the CLI's `tuning::init()` can override, which the FFI never calls. |
| reclaim tick | `util/tuning.rs:349`, `event_loop.rs:587` | 400 ms interval armed forever — 2.5 wakeups/s even when `reclaim_until` is `None` and the arm does nothing but compare two `Instant`s. |
| message log | `util/consts.rs:152`, `daemon/message_log.rs:43` | `VecDeque::with_capacity(1000)` of `Message` (~250–350 B each) ⇒ **~250–350 KB allocated at startup, empty**. The only true preallocation in the engine — every other bounded container (`BoundedQueue`, `BoundedIdSet`, `BoundedFifoSet`) starts empty and grows. |
| gossip view | `util/consts.rs:324` → `lookup/mod.rs:258-266` | `GOSSIP_ACTIVE_VIEW_CAPACITY = 64`, passive view 128. **iroh-gossip's own default is 5** (`hyparview.rs:202`). A ceiling, not a floor — HyParView views are `Vec`s that grow — but the crate's own comment at `consts.rs:310-324` says "~0.5 MB resident per link … a fully-meshed node runs ~50 MB". For a jam of under ten people this authorises about 8× more than will ever be used. |
| doc frame retention | `doc/mod.rs:96` | `frames: HashMap<ChangeHash, Message>` retains **every signed change frame forever** so anti-entropy can re-serve it. Unbounded growth over a long session — the one plausible leak in the engine. |
| reassembly budgets | `util/consts.rs:59,78-88` | `MAX_LOGICAL_BODY_BYTES = 64 MiB` ⇒ 128 MiB per group, 192 MiB per author, **384 MiB total**. Nothing is allocated until shards arrive, but there is no low ceiling for an embedder whose largest message is a 1.5 KB grid snapshot. |

The idle +23 MB is therefore mostly tokio worker stacks, the iroh endpoint's rustls/QUIC/netwatch/
portmapper/relay-client state, hickory+moka DNS caching, the `n0-mainline` DHT's `lru` routing table,
and the `swarm-discovery`/`acto` mDNS actor — the last two present only because `src/p2p.odin:127-139`
passes `mdns = 1, dht = 1`.

**[measured] and worth restating:** turning discovery off (`--no-mdns --no-dht --no-relay`, scenario F)
recovered nothing — 7.4 → 7.2 pp CPU, inside the IQR, and RSS did not drop. Those subsystems are not
where the cost is; the runtime and the endpoint are.

---

## Part 2 — Crate split

### Why the size number is what it is

**[measured]** The mesh engine's own code is **0.75 MiB of `__text`**. The other ~18 MiB is its
dependency closure: Rust core/alloc/std 3.47, tokio+futures 2.15, HTTP/relay 1.61, TLS+crypto 1.57,
automerge 1.51, iroh 1.50, DNS 0.91, QUIC 0.73, tracing+regex 0.72, NAT/DHT/mDNS 0.56 (from
`bin/measure/families.tsv`).

You cannot shrink that by writing less code. You shrink it by making the closure opt-in — which means
crate boundaries, because cargo features cannot be selected per-consumer across a dependency edge.

### The reference: p2panda

p2panda solved this problem for the same stack (iroh + gossip + a CRDT + discovery + blobs). Their
answer is 11 crates in which **exactly one — `p2panda-net` — is allowed to name `iroh`**, and where the
CRDT (`p2panda-auth`), the crypto (`p2panda-encryption`), the sync protocols (`p2panda-sync`) and the
discovery protocol (`p2panda-discovery`) do not depend on it. `p2panda-core` has no async runtime at
all; `p2panda-discovery` makes even tokio optional.

The five rules worth copying, each verified in their tree:

1. **One crate names `iroh`.** Everything else speaks `Sink<Message>` / `Stream<Item = Result<Message,
   _>>`. In `p2panda-net/Cargo.toml`, `iroh` is `optional` behind the `iroh_endpoint` feature and every
   heavy feature uses `dep:`.
2. **The CRDT/sync crate must not depend on the net crate.** `p2panda-net` takes the sync manager as a
   *generic parameter* — `SyncManager::<TopicSyncManager<..>>::spawn(..)` — so the CRDT never enters the
   network layer's dependency graph. The proof this works: Reflection (the GNOME collaborative editor)
   swapped its CRDT from Automerge to Loro without changes to p2panda's networking.
3. **A protocol trait must be runnable over an in-memory channel.** `p2panda-sync::Protocol::run(self,
   sink, stream)` takes no connection, no socket, nothing iroh-shaped; `p2panda-net` adapts a QUIC
   bi-stream into that pair separately. If your trait takes a `Connection`, you have not made a seam.
4. **Split discovery three ways** — peer-sampling strategy, wire exchange protocol, and
   transport-specific lookup (`DiscoveryStrategy` / `DiscoveryProtocol` / `p2panda-net::iroh_mdns`).
   Only the third is heavy, and only the third is feature-gated.
5. **Feature = backend or module; crate = domain with its own dependency tree.** And run `cargo-hack`
   over the feature powerset in CI — p2panda shipped two separate bugfix PRs for broken feature
   combinations before adding that gate.

One thing not to copy: they kept their store traits but deleted the in-memory implementation, leaving
one impl and an unvalidated abstraction. Keep a second implementation alive for each seam, as the thing
that proves the seam is real.

### What is actually separable here

The engine's real internal dependency graph, with doc-comment references filtered out (a naive grep
inflates this badly — `protocol → beacon/gossip/daemon/transport`, `util → daemon/lookup` and `blob →
gossip` are all doc links, not code):

```
util → (leaf)                protocol → util              doc → protocol
directory → protocol,util    reassembly → protocol,util   invite/resolver → protocol,util
logging → protocol,util      blob → protocol,util,lookup  lookup → protocol,transport
beacon → lookup,protocol,util

daemon ⇄ gossip ⇄ lifecycle ⇄ transport      ← one strongly-connected blob, ~10.4k LOC
```

**Clean cuts.** One-way dependencies, few call sites:

| new crate / feature | from | LOC | sheds | rewiring |
|---|---|---:|---|---|
| `fofoca-doc` | `doc/` | 1169 | **automerge + hexane, 1.51 MiB** | 6 call sites: `daemon/{state,config,setup}.rs`, `gossip/{broadcast,recv}.rs`, plus `embed`/`ops` re-exports. **`automerge::` is imported in exactly one file, `doc/mod.rs:31-32`** — every other file that mentions it does so in a comment. |
| `fofoca-logging` | `logging/` | 516 | **tracing-subscriber + EnvFilter's regex, ~0.72 MiB** | 5 `log_{in,out}` calls in `gossip/` |
| feature `ipc` | `transport/ipc.rs` | 609 | `interprocess` | 2: `event_loop.rs:853-866` (already behind `if disabled { return None }`) and `runtime::ipc` |
| features `mdns`, `dht` | `lookup/{mdns,dht}.rs` | 51 | swarm-discovery+acto, n0-mainline+lru, ~0.56 MiB | pure gate, no restructuring — the two files are 25 and 26 lines |
| `fofoca-blobs` | `blob/` | 1324 | — | **zero engine callers**; reachable only via `ops::blob` |
| `fofoca-directory` | `directory/` | 402 | — | **zero engine callers**; reachable only via `ops::directory` |
| delete | `anstyle`, `anstream` | — | 2 dead direct deps | **zero references anywhere in `src/` or `build.rs`** — leftovers from the CLI that was not vendored |

**Blocked without trait inversion.** `daemon` + `gossip` + `lifecycle` + `transport` ≈ 10.4k LOC sharing
`EventLoopState` and `HandlerCtx` bidirectionally: every `gossip::{recv,broadcast,antientropy,heal}`
entry point takes `&mut EventLoopState` plus a `HandlerCtx` from `daemon`, and `daemon::event_loop`
calls back into `gossip`. Splitting these means inverting the god-object into traits first.

`protocol` (5798 LOC) looks like the natural bottom crate — it depends only on `util` — but it drags
`iroh` and `iroh-gossip` in through four files (`protocol/{identity,crypto,peer_addr,mesh}`). A
pure-wire `fofoca-protocol` means moving those four out first.

### The seam that matters most for mallorca

`doc` is the cleanest extraction in the tree and the closest analogue to `p2panda-sync`: 1169 LOC, one
heavy dependency confined to a single file, a one-way dependency on `protocol`, six call sites.

Modelled on `p2panda-sync::Protocol`, the shared-state channel becomes a trait, with automerge as one
implementation behind a feature:

```rust
pub trait SharedState {
    type Change: Serialize + DeserializeOwned;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Apply an RFC 7386 merge patch locally; returns the change to gossip, if any.
    fn merge_local(&mut self, patch: &serde_json::Value) -> Result<Option<Self::Change>, Self::Error>;
    /// Apply a remote change. Out-of-order changes are the impl's problem, not the caller's.
    fn ingest(&mut self, change: Self::Change, from: &Nickname) -> Result<Ingested, Self::Error>;
    fn snapshot(&self) -> Result<serde_json::Value, Self::Error>;
}
```

The existing `MeshDoc` satisfies this shape already — `merge_local`/`ingest`/`snapshot` map onto what
`gossip/broadcast.rs:142,194-200` and `gossip/recv.rs:713-750` do today. What it buys, concretely:
**`docs/p2p-protocol.md` §6 uses the shared doc for `bpm` and `playing` — two scalars — and mallorca
pays 1.51 MiB of CRDT for them.** A last-write-wins implementation behind the same trait is a few
hundred lines and no dependency.

Keeping the automerge impl in-tree as the second implementation is what keeps the trait honest, per the
p2panda lesson above.

### What forking costs

We are restructuring the vendored copy rather than tracking upstream, so this should be recorded
plainly: `VENDORED.md`'s re-copy procedure stops working, and upstream fixes must be ported by hand.

Two things must survive the fork regardless of structure — the `[patch.crates-io]` pins in the workspace
root, `fofoca-network/iroh` rev `dcbdc152…` (mapped_addrs eviction fix) and `fofoca-network/iroh-gossip`
rev `c779c066…` (connection-churn leak fix). Whatever workspace root ends up owning the dependency graph
has to keep them.

Recommendation: replace `VENDORED.md` with a `FORKED.md` recording the divergence commit
(`f81b0529ee1b66725295998f15292df1e1ca191c`), the patch pins, and each carried change — starting with
the ones already made locally (the `name`/`mesh_name()` FFI extension, the `staticlib` crate-type, the
thin-LTO profile, and the deliberate absence of `panic = "abort"`).

---

## Status: the split has been done

Items 4 and 6 of the backlog below, plus the whole of Part 2, are **implemented**.
`fofoca/FORKED.md` records the new layout and every carried change.

| crate | owns | resolved deps |
|---|---|---:|
| `fofoca-util` | runtime dirs, clock, tuning dials, bounded containers, build stamp | 13 |
| `fofoca-protocol` | messages, mesh ids, nicknames, identity, sealing, invites, multipart reassembly, the directory ad codec | 138 |
| `fofoca-doc` | the CRDT channels — **the only crate that names automerge** | |
| `fofoca-logging` | the tracing sink — **the only crate that names tracing-subscriber** | |
| `fofoca` | daemon, gossip, lifecycle, transport, lookup, beacon | 436 |

`fofoca-reassembly` and `fofoca-directory` were folded back into
`fofoca-protocol`. The split's stated rationale is dependency closure, and it
measurably did not apply to those two: every name each used was already one of
`fofoca-protocol`'s, so `cargo tree -p fofoca --no-default-features` resolves
the identical external set either way. They cost a manifest, a `host` and an
`adversarial` feature copied verbatim, and a lib target, and they bought
nothing — both were aliased straight back in at `fofoca/src/lib.rs`, so the
split did not even buy a path distinction.

The blob-transfer crate this table originally listed has since been deleted —
it had no consumer here, and the `fofoca-blobs` name now belongs to an unrelated
crate that depends on nothing in this workspace. See `FORKED.md`.

The load-bearing property, p2panda's rule 1: **only `fofoca` names `iroh`.**
`fofoca-protocol` builds on `iroh-base` alone and
resolves 138 crates against the engine's 436, with **no tokio, QUIC, TLS or DNS**
anywhere in its tree. Getting there took three changes: a local 32-byte
`TopicId` newtype so the wire crate need not name iroh-gossip, moving the
`MAX_MESSAGE_SIZE` compile-time tripwire into the engine, and gating `util`'s
one async module behind `async-io`.

Measured effect on mallorca's release binary:

| build | bytes | vs pre-split |
|---|---:|---:|
| pre-split baseline | 42,699,056 | |
| after the split, default features | 41,504,616 | **−1,194,440 (−1.14 MiB)** |
| with `--no-default-features` (no mDNS/DHT) | 39,011,128 | **−3,687,928 (−3.52 MiB)** |

The first row's saving is `blob` leaving the engine's dependency graph; the
second adds the 12 crates the `mdns`/`dht` gates drop (swarm-discovery, acto,
n0-mainline, lru, serde_bencode, …). The staticlib falls from 233.7 MB to
219.5 MB with discovery off.

What the split does *not* do on its own is remove automerge or
tracing-subscriber from mallorca's binary — the engine still depends on both
crates unconditionally. It makes that a one-line change in one manifest rather
than a refactor, which is backlog item 5.

## Part 3 — Backlog

Ranked by value ÷ effort. Items 1–4 are all in the FFI shim or `Cargo.toml`, need no restructuring, and
between them address most of the measured runtime cost.

| # | change | win | effort | risk |
|---|---|---|---|---|
| 1 | `try_recv` fast path for zero timeout (`pipe.rs:416`) | ~11% of render-thread wall clock | hours | very low — shim-local, and 0 ms is mallorca's only timeout |
| 2 | Expose `cohost`, default `Never` for embedded pipes (`pipe.rs:326`) | removes the 5 s join freeze and a second iroh endpoint | hours | low — the knob already exists at `setup.rs:231/396` |
| 3 | `worker_threads(2)` + smaller `thread_stack_size` (`pipe.rs:304`) | ~8 threads and their stacks | hours | low |
| 4 | ~~Delete `anstyle`/`anstream`; feature-gate `mdns`/`dht`~~ **done** | 12 crates dropped, 3.52 MiB with discovery off | hours | very low |
| 5 | ~~Extract `fofoca-doc`~~ **done**; still to do: the `SharedState` trait + an LWW impl, then drop the automerge dep | 1.51 MiB | days | medium |
| 6 | ~~Extract `fofoca-logging`~~ **done**; still to do: make it optional for embedders | 0.72 MiB | ~1 day | low |
| 7 | Right-size `GOSSIP_ACTIVE_VIEW_CAPACITY`, message log, reassembly budgets | RAM at scale | ~1 day | medium — tuning changes churn behaviour |
| 8 | Bound `MeshDoc::frames` (`doc/mod.rs:96`) | fixes unbounded growth over a long session | days | medium — anti-entropy correctness |
| 9 | Move `beacon::ensure`'s probe off the event loop | removes a class of head-of-line blocking | days | medium |
| 10 | Invert `EventLoopState`/`HandlerCtx` into traits | enables splitting the 10.4k-LOC core | weeks | high |

### How to confirm each one

For items 1–4, `just measure runtime` before and after, compared against the baselines already in
`bin/measure/runtime.tsv`:

| item | metric | current baseline |
|---|---|---|
| 1 | `poll_mean_us`, scenarios C–F | 1142–1186 µs |
| 2 | `roster_max_us` / `send_max_us`, D-join and E-join | 4.9–5.0 s |
| 3 | `threads` and `rss_mb`, scenario C | 17 threads, 107.5 MB |
| 4 | `bin/measure/size.tsv` delta, and `crates.tsv` for the shed families | 41,301,024 B delta |

For items 5–6, `just measure size`: the `automerge`/`hexane` and `tracing`/`regex` rows of
`bin/measure/families.tsv` should go to zero for a build that doesn't enable them.

### Open question

`src/fofoca_worker.odin` (untracked, in progress) moves every FFI call onto a worker thread. That changes
*who* pays the ~1 ms tax, not that it is paid — at its `MESH_RECV_TIMEOUT_MS = 16` poll interval the
worker would pay it ~62 times a second on its own thread instead of once per frame on the render thread.
Item 1 removes the tax rather than relocating it, so the two are complementary; but the numbers in
`ffi-cost.md` describe the inline path, and should be re-measured against the worker once it lands.

## See also

- [`ffi-cost.md`](https://github.com/dviramontes/mallorca/blob/main/docs/ffi-cost.md) — the measurements this document explains
- [`p2p-protocol.md`](https://github.com/dviramontes/mallorca/blob/main/docs/p2p-protocol.md) — the wire protocol, including the §6 shared-state doc that
  motivates the `SharedState` seam
- [p2panda](https://github.com/p2panda/p2panda) — the reference architecture;
  [`p2panda-sync/src/traits.rs`](https://github.com/p2panda/p2panda/blob/main/p2panda-sync/src/traits.rs)
  and [`p2panda-net/Cargo.toml`](https://github.com/p2panda/p2panda/blob/main/p2panda-net/Cargo.toml)
  are the two files worth reading in full
