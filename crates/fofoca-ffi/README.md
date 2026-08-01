# fofoca-ffi

A C ABI over the [`fofoca`](../fofoca) engine — the first consumer that is not
written in Rust. A C program links one library, calls a dozen functions, and is a
full member of a gossip mesh: it creates or joins, exchanges broadcast and
directed messages, and reads and writes the shared JSON state document. No
daemon, no socket, no subprocess — the event loop runs on a tokio runtime inside
the caller's own process.

The consumer that drives it is [mallorca](https://github.com/dviramontes/mallorca),
an Odin application that links the staticlib and joins a mesh from its own
process.

## Why it is a separate crate

The engine cannot expose this itself. `cdylib` and `staticlib` crate-types cannot
be switched on per-feature, so declaring them on `fofoca` would make every build
of the engine pay for libraries nobody links. The ABI also wants a lint posture
the engine does not: the workspace denies `unsafe_code`, and this crate is
nothing *but* raw pointers from a foreign caller.

It depends on the engine and nothing else.

## The surface

[`include/fofoca.h`](include/fofoca.h) is the hand-written, committed
declaration — the source of truth for a C caller, and the counterpart of
[`src/ffi.rs`](src/ffi.rs). Change one, change the other.

```c
fofoca_pipe *pipe = fofoca_open(&(fofoca_opts){ .is_public = 1 });
printf("joined %s as %s\n", fofoca_id(pipe), fofoca_nickname(pipe));

while (fofoca_peer_count(pipe) < 1) { /* wait for company */ }

fofoca_send(pipe, NULL, (const uint8_t *)"hello", 5);   /* broadcast */
fofoca_send(pipe, "bob", (const uint8_t *)"psst", 4);   /* just bob */
fofoca_state_merge(pipe, "{\"phase\":\"ready\"}");        /* shared JSON state */

fofoca_frame frame;
if (fofoca_recv(pipe, buf, fofoca_max_chunk(), 5000, &frame) > 0) { /* … */ }

fofoca_close(pipe);
```

Three conventions hold across the whole surface:

- **Failures are quiet in the return value, loud in the error slot.** A
  pointer-returning call yields NULL, an `int` call returns `-1`, and
  `fofoca_last_error()` says why. A call clears the slot on entry, so it only ever
  holds the most recent outcome. The slot is `thread_local!`, so it reports only
  errors from calls made on *that* thread — a caller that drives the mesh from a
  worker must capture its own failures there.
- **Buffers are sized by asking.** `fofoca_state_json` / `fofoca_peers_json` return
  the length the document needs; pass a NULL buffer to ask, then call again with
  one that fits. `fofoca_recv` refuses a buffer smaller than the frame rather than
  truncating — size it with `fofoca_max_chunk()`.
- **A handle belongs to one thread.** Distinct handles are fully independent, so
  one process can hold several members of the same mesh.

`fofoca_peer_count` is worth one note: it counts peers **other than you**, so a
lone member reads `0`, while the roster JSON's own `count` field includes you and
reads `1`. The loop above is why it exists — nothing is retained, so a sender
with no peers is not early, it is throwing bytes away.

## Two things that will bite you

- **No signal handlers.** `src/pipe.rs` passes `handle_signals: false` to
  `Node::spawn`, the one deliberate divergence from a standalone binary. A
  library that installs process-wide ctrl-c / SIGTERM listeners hijacks its
  host's own handling. A C caller traps signals itself and calls `fofoca_close`.
- **Panics must not cross the boundary.** `src/ffi.rs` wraps every entry point in
  `catch_unwind`, because unwinding past `extern "C"` is undefined behaviour.
  This is why the workspace's `[profile.release]` deliberately omits
  `panic = "abort"` — with it, any engine panic would kill the host process
  instead of returning an error code.

## Test it

```bash
cargo test -p fofoca-ffi
```

`tests/ffi_smoke.rs` drives the `extern "C"` functions from Rust through the
crate's `rlib`. It is about the boundary — NULL handling, the error slot, the
buffer conventions — and needs no C compiler, so the ABI stays covered on a host
with no toolchain for the calling language.

CI additionally builds the staticlib and asserts that every function declared in
`include/fofoca.h` is actually exported by the archive, so a header that drifts
ahead of the implementation fails here rather than at a downstream link.

## What it is not

Not a published crate (`publish = false`) and not a stable ABI. This is the
*engine*: raw frames and a shared JSON document, with no higher-level agent
semantics layered on top.
