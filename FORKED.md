# Forked from agent-habilis/agent-gossip

This workspace began as a verbatim vendoring of a subset of
`agent-habilis/agent-gossip` (see `VENDORED.md` for that original contract). It
is now a **fork**: the engine has been split into separate crates, so the
"re-copy the crate directories" update procedure no longer applies.

- **Upstream URL**: https://github.com/agent-habilis/agent-gossip
- **Divergence commit**: `f81b0529ee1b66725295998f15292df1e1ca191c`

Taking an upstream change now means porting it by hand into whichever crate
owns that code. The table under "Where things moved" maps upstream paths to
their new homes.

## Why

`docs/ffi-cost.md` in the mallorca repo measured the engine at **39.4 MiB of
mallorca's 40.7 MiB release binary**, with the engine's own code accounting for
0.75 MiB of `__text` and the rest being its dependency closure. Cargo features
cannot be selected per-consumer across a dependency edge, so making that closure
optional required crate boundaries. `docs/mesh-slimming.md` has the analysis and
the p2panda-derived rules the split follows.

## Crate layout

Dependencies point strictly downward.

```
fofoca-util          no deps of consequence          (13 crates resolved)
  └── fofoca-protocol    + iroh-base                 (138 crates)
        ├── fofoca-doc          + automerge
        ├── fofoca-logging      + tracing-subscriber
        ├── fofoca-reassembly
        ├── fofoca-directory
        └── fofoca         + iroh, iroh-gossip  (436 crates)
              └── fofoca-ffi

fofoca-blobs                    + bao-tree, blake3   (standalone)
fofoca-iroh-webrtc-transport    + iroh, str0m        (standalone)
```

The bottom two are off the tree entirely: neither depends on anything in this
workspace. See carried changes 18 and 19.

The load-bearing property: **only `fofoca` names `iroh`**. `fofoca-protocol`
builds on `iroh-base` alone and pulls no tokio, QUIC, TLS or DNS; `-doc` and
`-logging` inherit that.

## Where things moved

The left column is the path upstream still uses; the right is ours. The whole
workspace was also renamed from `agent-habilis-mesh/` to `fofoca/`.

| upstream path | now |
|---|---|
| `agent-habilis-mesh/src/util/` | `fofoca-util/src/` |
| `agent-habilis-mesh/src/protocol/` | `fofoca-protocol/src/` |
| `agent-habilis-mesh/src/{invite,resolver}/` | `fofoca-protocol/src/{invite,resolver}/` |
| `agent-habilis-mesh/src/doc/` | `fofoca-doc/src/` |
| `agent-habilis-mesh/src/logging/` | `fofoca-logging/src/` |
| `agent-habilis-mesh/src/reassembly/` | `fofoca-reassembly/src/` |
| `agent-habilis-mesh/src/directory/` | `fofoca-directory/src/` |
| `agent-habilis-mesh/src/blob/` | deleted — see carried change 18 |
| `agent-habilis-mesh-ffi` | `fofoca-ffi` |
| everything else | unchanged in `fofoca` (was `agent-habilis-mesh`) |

`fofoca` re-exports `util`, `protocol`, `doc`, `logging`,
`reassembly` and `directory` under their old module paths, so engine-internal
code and the `embed`/`net`/`ops`/`runtime` facades read as before.

## Carried changes

Divergences from upstream, in the order they were made.

**From the original vendoring** (all still in force):

1. Virtual workspace root `Cargo.toml` (upstream has a root package). `resolver
   = "3"` is load-bearing: it stops the `test-fixtures` dev-dep feature folding
   into the shipped binary.
2. `[profile.release]` uses `lto = "thin"`, `codegen-units = 16`,
   `strip = "debuginfo"` rather than upstream's fat-LTO / 1-CGU, for dev-loop
   speed.
3. Upstream's `panic = "abort"` deliberately **not** carried: `ffi.rs` relies on
   `catch_unwind` at the C boundary.
4. FFI extension: a `name` field alongside `nick`, plus `mesh_name()`, across
   `pipe.rs` / `ffi.rs` / `include/mesh.h` / `tests/ffi_smoke.rs`; and the
   `staticlib` crate-type.
5. `tests/c_suite.rs` dropped (it hardcodes paths into upstream `examples/`).

**From the split:**

6. The crate split above, with `pub(crate)` items promoted to `pub` where a
   crate boundary now sits between definition and use.
7. `protocol` no longer depends on `iroh-gossip`. Its `TopicId` is a local
   32-byte newtype (`fofoca-protocol/src/topic.rs`) with a hex `Debug`
   matching iroh-gossip's; `daemon::setup` converts at the two `gossip.subscribe`
   call sites. The `MAX_MESSAGE_SIZE` compile-time tripwire moved to
   `fofoca/src/gossip/mod.rs`, which can still name
   `DEFAULT_MAX_MESSAGE_SIZE`.
8. New cargo features: `mdns` and `dht` on `fofoca` (default on,
   forwarded by `fofoca-ffi`), and `async-io` on
   `fofoca-util` gating `bounded_read`, which is the only tokio user
   below the engine.
9. Dropped the unused `anstyle` / `anstream` dependencies (zero references).
10. `ops::blob` removed — the blob-transfer crate depended on the engine, so
    the engine could not re-export it. Superseded by carried change 18, which
    deleted that crate.
11. `public-surface.txt` moved to the workspace root and regenerated, since it
    now spans eight crates.

**From the rename:**

12. Everything named for the upstream org was renamed to **fofoca**: the
    directory, all eight crates (`agent-habilis-mesh` → `fofoca`,
    `agent-habilis-mesh-ffi` → `fofoca-ffi`, `agent-habilis-<x>` →
    `fofoca-<x>`), their lib names, the 89 `tracing` targets and the matching
    `log_filter` directives, the C ABI (`mesh_*` → `fofoca_*`, types
    `mesh_{pipe,opts,frame}` → `fofoca_*`), the header (`include/mesh.h` →
    `include/fofoca.h`, guard `FOFOCA_H`), the staticlib
    (`libfofoca_ffi.a`), the blob ALPN (`habilis-mesh/blob/1` →
    `fofoca/blob/1`) and the default mesh name (`"mesh-ffi"` → `"fofoca"`).
    The `github.com/agent-habilis/*` URLs above are upstream repositories and
    are deliberately untouched.

**From the extraction** (moving out of the mallorca repo into its own):

13. `iroh-multihop-transport` left the workspace for
    [its own repo](https://github.com/fofoca-network/iroh-multihop-transport).
    It has no fofoca dependency and its audience is any iroh user. It is now a
    git dependency pinned by rev in `[workspace.dependencies]`. Note that
    `[patch.crates-io]` below still governs it — patch applies from the
    top-level workspace root across the whole graph, git dependencies included.
14. The `iroh` / `iroh-gossip` forks were re-homed from `agent-habilis` to
    `fofoca-network` so this workspace owns its entire pin surface. The commits
    were pushed unchanged, so **the rev SHAs are identical** — only the URLs in
    `[patch.crates-io]` moved.
15. `public-surface.txt` was **deleted**. Nothing in the tree ever generated or
    checked it, so it silently rotted: by the time of the extraction it was
    ~530 entries behind and still listed `iroh-multihop-transport`, which no
    longer lives here. It cannot be faithfully regenerated by grepping for
    `pub` — it tracked the *reachable* API, so `pub` items inside private
    modules were correctly absent. Reinstating it means a real API-extraction
    tool (`cargo public-api`) plus a CI diff, not a shell script.
16. Added the things a standalone repo needs and the vendored copy lacked:
    `LICENSE` (every crate already declared MIT), a root `README.md`, and CI.
    `crates/*/version` moved to `version.workspace = true` — `fofoca-ffi` had
    drifted to `0.0.0` while the rest sat at `0.5.0`.
17. `docs/mesh-slimming.md` came along from mallorca: it is the rationale for
    the crate split, so it belongs beside the crates. `docs/ffi-cost.md` and
    `scripts/measure-ffi-cost.sh` stayed behind — they measure mallorca's
    binary, not this workspace.

**From reclaiming the `fofoca-blobs` name:**

18. The blob-transfer crate — upstream's `agent-habilis-mesh/src/blob/`, carved
    out by change 6 — was **deleted**, and the name reassigned to an unrelated
    crate brought in from `agent-habilis/agent-share`.

    It was dead code here: no `use fofoca_blobs::` anywhere in the workspace, no
    reverse edge in `Cargo.lock`, and `fofoca-ffi` — mallorca's only entry point
    — never depended on it. It is recoverable from history if a consumer ever
    wants it back; upstream `agent-gossip` still carries it under `src/blob/`.

    What took the name is a BLAKE3/bao verified-byte-range store: outboards,
    chunk availability, and a `BlobStore` seam over bytes the caller already
    owns, with in-memory, filesystem, OPFS and `IndexedDB` backends. It shares
    no code, no wire format and no dependency with what it replaced — the two
    crates only ever shared a name.

    Two invariants got stronger as a result. "Only `fofoca` and `-blobs` name
    `iroh`" became **only `fofoca` names `iroh`**, and the dependency graph lost
    its one upward edge: the new crate depends on nothing in this workspace, so
    it sits beside the tree rather than above the engine. Its own
    `tests/isolation.rs` is what keeps that true.

19. `fofoca-iroh-webrtc-transport` arrived from the same repo. It is an iroh
    custom transport carrying QUIC datagrams over a WebRTC data channel, and it
    is what lets a browser reach a peer at all — a tab has no UDP socket, so
    iroh's own paths do not exist there. Like `fofoca-blobs` it depends on
    nothing else here, so it sits beside the tree.

    Its two backends were renamed on the way in: `host` → **`native`** and
    `web` stayed, along with `src/host/` → `src/native/`. `host` collided with
    the engine's own `host` feature, which means something related but not the
    same, and the pair now says plainly which of two mutually exclusive
    implementations gets compiled.

    The `iroh` requirement stays at `1.0.1` rather than moving to this
    workspace's `1.0.2`. The crate is still consumed from `agent-share`, whose
    patch table supplies 1.0.1; a `1.0.2` floor would be unsatisfiable there.
    Raise it once both sides pin the same fork.

    CI grew four steps for it. Neither backend is on by default, so every
    existing job built neither, and the `web` half had never been linted at all
    — `agent-share` only ever ran `cargo check` against wasm32, never clippy.
    Its first clippy pass produced 18 findings, all fixed here.

## Patch pins — do not drop

`[patch.crates-io]` in the workspace root pins `iroh`, `iroh-base` and
`iroh-dns` to `fofoca-network/iroh` rev `f9cb1f4fd1b69e904770516029fa0afac0fd3ce4`
(mapped_addrs eviction + relay teardown fixes) and `iroh-gossip` to
`fofoca-network/iroh-gossip` rev
`c779c0661fc9429e86852570be9bdc00fb47fdd9` (connection-churn leak fix).

`iroh-base` must stay pinned to the **same repo and rev as `iroh`**: the forked
`iroh` uses its workspace-local copy, and mixing that with the crates.io release
puts two `iroh_base` versions in the graph, which makes types from
iroh-gossip and the address-lookup crates fail to unify (E0308).

## Verifying a change

```
cargo check --workspace            # also: --no-default-features, --all-features
cargo test --workspace             # 19 suites, 411 tests
```

From a [mallorca](https://github.com/dviramontes/mallorca) checkout, `just check`
and `just test` build the staticlib and the Odin app against it — the real check
that the C ABI is unchanged. mallorca pins this repo by rev in its `Justfile`
(`fofoca_rev`) and clones it under `fofoca/`, so the loop is: edit here, run
`just check` there, then bump `fofoca_rev` once the change is pushed.
