//! The portable half of the byte-pipe embedding: the frame taxonomy, the engine
//! driver that implements it, and the one setup ritual that stands it up.
//!
//! The frame taxonomy — `pipe_data` / `pipe_eof`, a base64 body, the
//! [`AppClass`](fofoca::embed::AppClass) flags, the chunk budget — is this
//! crate's own wire contract. Every peer on a pipe mesh must agree on it bit for
//! bit, so treat a change here as a wire break.
//!
//! It is a crate rather than a module for exactly that reason. A browser tab
//! (`packages/fofoca-wasm`) and a terminal (`fofoca-ffi`, and
//! `packages/fofoca-ffi` above it) are two builds of one contract, and the only
//! way two builds cannot drift is for there to be one copy. That covers more
//! than the bytes: [`join`] owns the *resolution* too, because a tab and a
//! terminal land on the same mesh only if they derive the same id from the same
//! selectors — and the derivation mixes the lookups in, so "same string" is not
//! enough.
//!
//! What stayed behind in `fofoca-ffi` is only what cannot cross to wasm32: an
//! owned `tokio::runtime::Runtime` and a `block_on` per call.

mod app;
mod event;
mod session;
mod wire;

pub use app::{Inbound, PipeApp, Request};
pub use event::{PipeEvent, json_sink};
pub use session::{Opts, Session, depart, join, resolve_kind};
pub use wire::{
    DEPARTURE_GRACE, INBOUND_CAP, data_body, data_tag, decode_data, default_chunk, eof_body,
    eof_tag, parse_to, tag,
};
