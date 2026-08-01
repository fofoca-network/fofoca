//! `fofoca` — the serverless gossip-network engine.
//!
//! This workspace-internal crate holds the transport-and-protocol engine
//! decoupled from any one application (its data model, its
//! CLI/MCP bindings, the library `api`). It is never published
//! (`publish = false`); a consumer crate depends on it and re-exports
//! the curated public surface.

// ── Role-based public facade ──────────────────────────────────────────────
//
// The modules below are grouped by what a consumer needs, not by the engine's
// internal topology: `protocol` (value types), `embed` (the seams you
// implement), `runtime` (start/stop a node), `ops` (what a hook may do), `net`
// (the quarantined iroh corner), `util` (host helpers). The implementation
// modules they re-export from are crate-private.
pub mod embed;
pub mod net;
pub mod ops;
pub mod runtime;

pub(crate) mod beacon;
pub(crate) mod daemon;
pub(crate) mod gossip;
pub(crate) mod lifecycle;
pub(crate) mod lookup;

// Extracted leaf crates. Each owns one dependency the engine would otherwise
// carry unconditionally — automerge, tracing-subscriber — and each depends only
// on `fofoca-protocol`/`-util`, never back on this crate. Aliased so
// engine code keeps its `crate::doc::…` paths. See docs/mesh-slimming.md.
pub(crate) use fofoca_directory as directory;
pub(crate) use fofoca_doc as doc;
pub(crate) use fofoca_logging as logging;

/// The wire vocabulary, re-exported from the leaf crate
/// `fofoca-protocol`. That crate builds on `iroh-base` alone — no
/// iroh, no iroh-gossip, no tokio — so a consumer that only needs to speak
/// the protocol does not pull in a network stack.
pub mod protocol {
    pub use fofoca_protocol::*;
}

/// Creator-minted invites. The redeem + decode primitives back
/// `resolver::JoinTarget::Invite`; `mint` is re-exported for consumers
/// through [`ops::invite`](crate::ops::invite).
pub(crate) use fofoca_protocol::invite;
pub(crate) use fofoca_protocol::resolver;

pub(crate) use fofoca_reassembly as reassembly;

pub(crate) mod transport;

/// Host helpers. The bulk lives in the leaf crate `fofoca-util`, which
/// has no engine or iroh dependency; it is re-exported here so both consumers
/// and engine code keep reaching it as `util::…`. The `logging` facade stays
/// on this side because it fronts the engine's own log sink.
pub mod util {
    pub use fofoca_util::*;

    /// Deferred `tracing` sink and the pinned directive filter.
    pub mod logging {
        pub use crate::logging::messages::log_out;
        pub use crate::logging::{LogSink, attach, flush_pending_to_stderr, install, log_filter};
    }
}

// Re-exported at the crate root so engine code (and the app's re-export) can
// reach the build version stamp as `crate::VERSION`.
pub use util::version::VERSION;

// The `NodeApp` / `NodeDriver` seams are `#[async_trait]`, so any consumer
// implementing them needs the same macro. Re-export it so a downstream crate
// annotates its impls with `#[fofoca::async_trait]` instead of
// taking its own (version-matched) `async-trait` dependency.
pub use async_trait::async_trait;

// Shared config for the crate's `proptest!` blocks. Overrides the default
// failure-persistence path so regression seeds land in
// `tests/proptest-regressions` rather than a `proptest-regressions` folder
// at the crate root. Every `proptest!` block opts in with
// `#![proptest_config(crate::proptest_support::config())]`. Kept last so it
// does not trip `clippy::items_after_test_module`.
#[cfg(test)]
pub(crate) mod proptest_support {
    use proptest::test_runner::{Config, FileFailurePersistence};

    pub(crate) fn config() -> Config {
        Config {
            failure_persistence: Some(Box::new(FileFailurePersistence::SourceParallel(
                "tests/proptest-regressions",
            ))),
            ..Config::default()
        }
    }
}
