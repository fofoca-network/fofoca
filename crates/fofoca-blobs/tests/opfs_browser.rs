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
//! **These assertions are the same behaviours `conformance.rs` asserts**, in the
//! same order, deliberately duplicated rather than shared. Sharing them would
//! mean making the whole suite generic over an executor to serve one backend,
//! and the duplication is what keeps that suite simple for the two backends that
//! do not need it. If they drift, this file is wrong.
//!
//! Also worth knowing: sync access handles are **Worker-only**, so
//! `wasm_bindgen_test_configure!(run_in_browser)` alone is not enough if the
//! harness runs tests on the main thread. Where it does, these fail at
//! `create_sync_access_handle` with a `TypeError` — which is the platform
//! telling the truth, not a bug here.

#![cfg(target_arch = "wasm32")]

use bao_tree::{ChunkNum, ChunkRanges};
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

#[wasm_bindgen_test]
async fn an_unknown_root_is_empty_not_an_error() {
    let store = store("unknown");
    let missing = [0xAB; 32];
    assert!(store.present(missing).await.expect("present").is_empty());
    assert!(store.outboard(missing).await.expect("outboard").is_none());
}

#[wasm_bindgen_test]
async fn a_complete_file_reports_everything_present() {
    let store = store("complete");
    let bytes = data(1 << 18);
    let id = file("complete", "a.bin", bytes.len() as u64);

    // The caller's file has to exist before the store can describe it — the
    // store never creates the data it records.
    store
        .write_verified(
            &id,
            build_outboard(&bytes).0,
            &encode_ranges(&bytes, &ChunkRanges::all()).expect("encode"),
            &ChunkRanges::all(),
        )
        .await
        .expect("materialize");

    let root = store.insert_complete(&id, &bytes).await.expect("insert");
    assert!(!store.present(root).await.expect("present").is_empty());
    assert_eq!(store.bind(&id).await.expect("bind"), Some(root));
}

/// **The version gate**, the property every backend shares.
#[wasm_bindgen_test]
async fn a_file_that_changed_is_unbound() {
    let store = store("versioned");
    let bytes = data(4096);
    let id = file("versioned", "a.bin", bytes.len() as u64);
    store.insert_complete(&id, &bytes).await.expect("insert");

    let grown = FileId {
        size: id.size + 1,
        ..id.clone()
    };
    assert_eq!(store.bind(&grown).await.expect("bind"), None, "size moved");

    let touched = FileId {
        mtime: id.mtime + 1,
        ..id.clone()
    };
    assert_eq!(
        store.bind(&touched).await.expect("bind"),
        None,
        "mtime moved"
    );
    assert!(store.bind(&id).await.expect("bind").is_some());
}

/// Re-seeding: take verified bytes from a third party, serve them onward.
#[wasm_bindgen_test]
async fn verified_bytes_become_servable() {
    let bytes = data(1 << 18);
    let (root, _) = build_outboard(&bytes);
    let all = ChunkRanges::all();
    let encoded = encode_ranges(&bytes, &all).expect("encode");

    let store = store("reseed");
    let dest = file("reseed", "mirrored.bin", bytes.len() as u64);
    let held = store
        .write_verified(&dest, root, &encoded, &all)
        .await
        .expect("write verified");
    assert!(!held.is_empty());

    let onward = store.read_ranges(&dest, &all).await.expect("re-serve");
    let mut target = Vec::new();
    // A final consumer, so the outboard is discarded.
    let mut outboard = Vec::new();
    fofoca_blobs::decode_into(
        root,
        bytes.len() as u64,
        &onward,
        &all,
        &mut target,
        &mut outboard,
    )
    .expect("the mirror's bytes must verify");
    assert_eq!(target, bytes, "a re-seeder must serve the same bytes");
}

/// A peer sending garbage costs its own bandwidth and nothing else.
#[wasm_bindgen_test]
async fn a_tampered_write_is_rejected_and_changes_nothing() {
    let bytes = data(1 << 18);
    let (root, _) = build_outboard(&bytes);
    let all = ChunkRanges::all();
    let mut encoded = encode_ranges(&bytes, &all).expect("encode");
    let last = encoded.len() - 1;
    encoded[last] ^= 0x01;

    let store = store("tampered");
    let dest = file("tampered", "bad.bin", bytes.len() as u64);
    assert!(
        store
            .write_verified(&dest, root, &encoded, &all)
            .await
            .is_err(),
        "a flipped bit must not be accepted"
    );
    assert!(
        store.present(root).await.expect("present").is_empty(),
        "a rejected write must leave nothing behind"
    );
}

/// **Never answer short.**
#[wasm_bindgen_test]
async fn reading_a_range_we_do_not_hold_fails_rather_than_truncating() {
    let bytes = data(1 << 18);
    let (root, _) = build_outboard(&bytes);
    let head = ChunkRanges::from(ChunkNum(0)..ChunkNum(64));
    let partial = encode_ranges(&bytes, &head).expect("encode head");

    let store = store("partial");
    let dest = file("partial", "part.bin", bytes.len() as u64);
    store
        .write_verified(&dest, root, &partial, &head)
        .await
        .expect("write head");

    assert!(
        store.read_ranges(&dest, &ChunkRanges::all()).await.is_err(),
        "asking for the whole file from a partial store must fail"
    );
    assert!(
        store.read_ranges(&dest, &head).await.is_ok(),
        "the part it does hold is still servable"
    );
}

/// **A partial mirror must serve proofs against the real root.** A store that
/// rebuilds the tree from its own file hashes the holes too, so its proofs are
/// against a root nobody else has and every receiver rejects them. Decoding the
/// serve is the only way to see it — locally the bytes look fine.
#[wasm_bindgen_test]
async fn a_partially_held_file_is_served_against_the_real_root() {
    let bytes = data(1 << 18);
    let (root, _) = build_outboard(&bytes);
    let head = ChunkRanges::from(ChunkNum(0)..ChunkNum(64));
    let partial = encode_ranges(&bytes, &head).expect("encode head");

    let store = store("partial-serve");
    let dest = file("partial-serve", "part.bin", bytes.len() as u64);
    store
        .write_verified(&dest, root, &partial, &head)
        .await
        .expect("write head");

    let onward = store
        .read_ranges(&dest, &head)
        .await
        .expect("the part it holds is servable");
    let mut target = Vec::new();
    let mut outboard = Vec::new();
    fofoca_blobs::decode_into(
        root,
        bytes.len() as u64,
        &onward,
        &head,
        &mut target,
        &mut outboard,
    )
    .expect("a partial mirror's proofs must verify against the original root");
}

/// Sparse writes accumulate, or a mirror forgets every range but its last.
#[wasm_bindgen_test]
async fn ranges_accumulate_across_writes() {
    let bytes = data(1 << 18);
    let (root, _) = build_outboard(&bytes);
    let store = store("accum");
    let dest = file("accum", "accum.bin", bytes.len() as u64);

    let first = ChunkRanges::from(ChunkNum(0)..ChunkNum(64));
    let second = ChunkRanges::from(ChunkNum(64)..ChunkNum(128));
    for window in [&first, &second] {
        let encoded = encode_ranges(&bytes, window).expect("encode");
        store
            .write_verified(&dest, root, &encoded, window)
            .await
            .expect("write");
    }

    let held = store.present(root).await.expect("present");
    assert!(first.is_subset(&held), "the first window survived");
    assert!(second.is_subset(&held), "the second landed");
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
