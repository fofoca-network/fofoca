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

Dependencies point strictly downward, except `fofoca-blobs`, which sits
*above* the engine because it needs endpoint construction.

```
fofoca-util          no deps of consequence          (13 crates resolved)
  └── fofoca-protocol    + iroh-base                 (138 crates)
        ├── fofoca-doc          + automerge
        ├── fofoca-logging      + tracing-subscriber
        ├── fofoca-reassembly
        ├── fofoca-directory
        └── fofoca         + iroh, iroh-gossip  (436 crates)
              ├── fofoca-ffi
              └── fofoca-blobs
```

The load-bearing property: **only `fofoca` and `-blobs` name
`iroh`**. `fofoca-protocol` builds on `iroh-base` alone and pulls no
tokio, QUIC, TLS or DNS; `-doc` and `-logging` inherit that.

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
| `agent-habilis-mesh/src/blob/` | `fofoca-blobs/src/` |
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
10. `ops::blob` removed — `fofoca-blobs` depends on the engine, so the
    engine cannot re-export it. Consumers take the crate directly.
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

## Patch pins — do not drop

`[patch.crates-io]` in the workspace root pins `iroh`, `iroh-base` and
`iroh-dns` to `fofoca-network/iroh` rev `dcbdc1521caf680d483b2d8eec669a7997e9edf3`
(mapped_addrs eviction fix) and `iroh-gossip` to `fofoca-network/iroh-gossip` rev
`c779c0661fc9429e86852570be9bdc00fb47fdd9` (connection-churn leak fix).

`iroh-base` must stay pinned to the **same repo and rev as `iroh`**: the forked
`iroh` uses its workspace-local copy, and mixing that with the crates.io release
puts two `iroh_base` versions in the graph, which makes types from
iroh-gossip and the address-lookup crates fail to unify (E0308).

## Verifying a change

```
cargo check --workspace            # also: --no-default-features, --all-features
cargo test --workspace             # 22 suites
```

From a [mallorca](https://github.com/dviramontes/mallorca) checkout, `just check`
and `just test` build the staticlib and the Odin app against it — the real check
that the C ABI is unchanged. mallorca pins this repo by rev in its `Justfile`
(`fofoca_rev`) and clones it under `fofoca/`, so the loop is: edit here, run
`just check` there, then bump `fofoca_rev` once the change is pushed.
