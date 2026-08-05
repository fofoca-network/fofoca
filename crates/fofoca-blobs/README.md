# fofoca-blobs

Verified byte ranges over data you already own.

A store of BLAKE3/bao verification metadata — outboards, root bindings, and
which ranges are held — for bytes that live somewhere this crate does not
control.

## Why not `iroh-blobs`

`iroh-blobs` is good and this crate is much smaller than it. Two things made it
unusable here, and both are structural rather than missing features.

**It owns its data.** You hand it bytes and it stores them under a hash, so it
cannot serve a blob until it has read every byte and built the tree. Listing a
large share becomes a full read of the share, an editor save re-hashes the file,
and browsing a 500 GB tree while reading three files out of it is impossible.
Here the blob *is* the caller's file, wherever it already is; this crate holds
only the sidecar metadata beside it. That is the inversion the whole design
turns on, and it is why this is not a fork.

**Its wasm store is memory-only** (n0-computer/iroh-blobs#84). A browser tab is
a first-class seeder in this system, so a store that cannot persist in one would
mean holding an entire share in JS heap.

What it does *not* mean is rejecting the good part: verification here is
`bao-tree`, the same crate `iroh-blobs` uses. Only the storage seam is ours.

## What it is not

No transport, no ALPN, no framing. No discovery. **No download scheduler** —
that needs peer budgets and connection state, and making it generic is precisely
how `iroh-blobs` gets rebuilt by accident. No GC, no tags, no collections: the
caller's own directory listing is the collection.

It also does not know what a mesh is, and depends on nothing else in this
workspace — no `fofoca`, no `fofoca-protocol`, no `iroh`. `tests/isolation.rs`
fails if it learns.

## Two concepts

**A root is bound to a version.** `(key, size, mtime) -> root`. Mutable files
are supported rather than treated as a violation: a file whose size or mtime
moved since its outboard was built is *unbound*, and an unbound file is refused
rather than served wrongly.

**Never answer short.** A caller cannot distinguish a truncated answer from a
small file, so a store that does not hold a range says so. Returning what it has
is how a half-written mirror silently loses the tail of everything.

## Backends

`MemStore` and `FsStore` on the host; `OpfsStore` and `IdbStore` in a browser.
Each re-runs `tests/conformance.rs` unchanged — that suite is the reason
`BlobStore` is a trait at all. The backends differ in every way except the
behaviour it asserts, and the expensive failures are all behaviour.

## Browser builds

Enable `wasm-simd` and pass `-C target-feature=+simd128`. Browsers have no
native BLAKE3 — `crypto.subtle` does not offer it — so wasm is the only
implementation and SIMD is the only lever; measured at 1.80× on hashing and
1.72× on outboard construction, for +12 KB of module. The feature is *assumed*
rather than detected, so enabling it without the flag produces a module that
fails validation.

The OPFS backend needs a Worker: `FileSystemSyncAccessHandle` is the only OPFS
API with read and write at an arbitrary offset, and it does not exist in a
window scope. `IdbStore` is the main-thread answer — slower, but reachable from
anywhere.
