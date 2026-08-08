//! `OpfsStore` against a real browser.
//!
//! Separate from `conformance.rs` for a reason that is not laziness: that suite
//! drives futures with a `block_on` that polls **once**, because every other
//! backend resolves without yielding. Every call here parks on a JS Promise, so
//! it needs a real executor — and OPFS does not exist under node, so it needs a
//! real browser too.
//!
//! ```sh
//! # Chrome or Safari; node will not do.
//! wasm-pack test --headless --chrome crates/fofoca-blobs
//! ```
//!
//! Only the assertion that has **no** counterpart in `conformance.rs` lives
//! here. Everything else OPFS must satisfy is in that suite, which now runs
//! this backend directly — the duplicated copies that used to sit in this file
//! were removed once `conformance.rs` learned to drive a parking future.
//!
//! Also worth knowing: sync access handles are **Worker-only**, so
//! `wasm_bindgen_test_configure!(run_in_browser)` alone is not enough if the
//! harness runs tests on the main thread. Where it does, these fail at
//! `create_sync_access_handle` with a `TypeError` — which is the platform
//! telling the truth, not a bug here.

#![cfg(target_arch = "wasm32")]

use bao_tree::ChunkRanges;
use fofoca_blobs::{BlobStore, FileId, OpfsStore, build_outboard, encode_ranges};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn data(len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| u8::try_from(index % 251).expect("bounded by 251"))
        .collect()
}

/// A fresh store per test. OPFS persists across runs by design, so tests that
/// shared a directory would see each other's leftovers — which is exactly the
/// property being relied on in production and a hazard in a test.
fn store(tag: &str) -> OpfsStore {
    OpfsStore::new(format!("fofoca-blobs-test/{tag}"))
}

fn file(dir: &str, name: &str, size: u64) -> FileId {
    FileId {
        key: format!("fofoca-blobs-test/{dir}/data/{name}"),
        size,
        mtime: 1_700_000_000,
    }
}

/// The one assertion with no counterpart in `conformance.rs`, because no other
/// backend can lose data underneath itself: OPFS is **evictable**. Reading the
/// range set back from storage rather than caching it in memory is what stops
/// this peer advertising bytes the browser has reclaimed.
#[wasm_bindgen_test]
async fn availability_is_read_back_from_storage_not_remembered() {
    let bytes = data(1 << 18);
    let (root, _) = build_outboard(&bytes);
    let all = ChunkRanges::all();
    let encoded = encode_ranges(&bytes, &all).expect("encode");

    let dest = file("reopen", "a.bin", bytes.len() as u64);
    store("reopen")
        .write_verified(&dest, root, &encoded, &all)
        .await
        .expect("write");

    // A second store over the same directory is what a reload looks like.
    let reopened = store("reopen");
    assert!(
        !reopened.present(root).await.expect("present").is_empty(),
        "a fresh store must see what the previous one persisted"
    );
    assert_eq!(
        reopened.bind(&dest).await.expect("bind"),
        Some(root),
        "and must still know which file that content belongs to"
    );
}
