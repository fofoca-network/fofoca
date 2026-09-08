# fofoca-ffi

A C ABI over the [`fofoca`](../fofoca) engine. A C program links one
library, calls a dozen functions, and is a full mesh member. The event
loop runs inside the caller's process. No daemon, no socket.

[`include/fofoca.h`](include/fofoca.h) is the committed declaration and
the counterpart of [`src/ffi.rs`](src/ffi.rs). If you change one, change
the other.

- A failed call returns NULL or `-1`. `fofoca_last_error()` says why. The
  error slot is thread-local.
- Buffers are sized by asking: pass a NULL buffer to get the length, then
  call again with one that fits.
- A handle belongs to one thread. Distinct handles are independent.
- The crate installs no signal handlers. Trap signals yourself and call
  `fofoca_close`.
- Every entry point catches panics, so an engine panic returns an error
  code instead of unwinding across `extern "C"`.
- `fofoca_opts` names the relay's two roles apart: `relay_lookup` finds peers
  through it, `relay_transport` lets payload ride it. Leave
  `relay_transport` at 0 and every byte of data goes peer to peer.
  `relay_urls` swaps in a custom ladder (comma-separated, NULL for the
  default), and `disable_ip` / `disable_webrtc` switch transports off.

Test with `cargo test -p fofoca-ffi`. CI builds the staticlib and makes
sure that every function in the header is exported.

Not a stable ABI, and not published. See `src/lib.rs` for the API.
