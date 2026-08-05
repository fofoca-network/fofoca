//! The kill-gate on the seam: this crate must not know what a mesh is.
//!
//! `fofoca-blobs` never sees a manifest, a ticket, a mesh or an ALPN. It takes a
//! key, a size, an mtime and byte ranges. If the trait needs any of those to be
//! useful, the seam is in the wrong place and the crate has quietly become part
//! of the network layer rather than a thing the network layer uses.
//!
//! It is also the workspace's `only fofoca names iroh` property, enforced from
//! this side. This is the one crate that sits off the dependency tree entirely,
//! and that is worth something only while it stays true.
//!
//! A compile-time check would be better than reading the manifest, but a crate
//! cannot ask "am I linked against X" from inside itself. Reading the manifest
//! is the honest approximation, and it fails loudly the moment someone adds the
//! dependency that would make everything easier and the seam meaningless.

use std::path::Path;

/// Dependency-name prefixes that would mean the seam has collapsed. Prefixes,
/// not exact names: `fofoca` covers every sibling crate in this workspace and
/// `iroh` covers `iroh-base` / `iroh-gossip` / `iroh-io` alike.
const FORBIDDEN: &[&str] = &["fofoca", "iroh"];

#[test]
fn the_crate_does_not_know_about_the_network_layer() {
    let manifest =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
            .expect("this crate has a Cargo.toml");

    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        for forbidden in FORBIDDEN {
            assert!(
                !line.starts_with(forbidden),
                "fofoca-blobs must not depend on {forbidden}: found `{line}`.\n\
                 The store takes a key, a size, an mtime and byte ranges. If it \
                 needs a mesh to be useful, move the seam rather than the \
                 dependency."
            );
        }
    }
}

/// The other half of the rule, in the direction people forget: the crate must
/// stay buildable for the browser, because the browser runs the same store.
///
/// Not a substitute for `cargo check --target wasm32-unknown-unknown`, which CI
/// runs — this only catches a dependency that is *obviously* host-only, before
/// someone waits for a wasm build to tell them.
#[test]
fn no_obviously_host_only_dependency_crept_in() {
    let manifest =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
            .expect("this crate has a Cargo.toml");

    // `tokio` is the one that would slip in most naturally, via someone reaching
    // for `tokio::fs` in a backend. Backends own their I/O; this crate does not.
    for host_only in ["tokio", "interprocess", "memmap2"] {
        for line in manifest.lines() {
            let line = line.trim();
            if line.starts_with('#') {
                continue;
            }
            assert!(
                !line.starts_with(host_only),
                "fofoca-blobs must build for wasm32; `{host_only}` is host-only.\n\
                 If a backend needs it, the backend is the place for it."
            );
        }
    }
}
