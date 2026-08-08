//! The conformance suite every backend must pass.
//!
//! Written once against the trait, run per backend. That is the whole reason
//! `BlobStore` is a trait at all: `FsStore` and `OpfsStore` differ in every way
//! except the behaviour asserted here, and the expensive failures — serving
//! bytes that were never verified, answering short instead of refusing, serving
//! a file whose content moved — are behaviour, not implementation.
//!
//! Add a backend by implementing [`Harness`] and adding one `backends!` line.
//! Anything that only passes for one backend is in the wrong file.

use std::future::Future;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

use bao_tree::{ChunkNum, ChunkRanges};
use fofoca_blobs::{BlobStore, FileId, MemStore, build_outboard, encode_ranges};

fn data(len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| u8::try_from(index % 251).expect("bounded by 251"))
        .collect()
}

/// What a backend supplies beyond the trait.
///
/// One thing, and it is exactly what the trait keeps opaque: **where a file
/// lives**. `MemStore` accepts any key; `FsStore` needs a path it can open. A
/// suite that hardcoded either would be testing one backend.
trait Harness {
    type Store: BlobStore;
    fn store(&self) -> &Self::Store;
    /// A key this backend can actually use for a file called `name`.
    fn key(&self, name: &str) -> String;
    /// Put `bytes` where this backend reads them from. A no-op for a store that
    /// holds its own; a real write for one that borrows the caller's file.
    ///
    /// Async because the browser backends materialize through the store itself,
    /// on a JS Promise. The host backends resolve immediately.
    fn place(&self, file: &FileId, bytes: &[u8]) -> impl Future<Output = ()>;

    fn file(&self, name: &str, size: u64) -> FileId {
        FileId {
            key: self.key(name),
            size,
            mtime: 1_700_000_000,
        }
    }
}

/// One conformance case, written once and run on both targets.
///
/// The two differ only in how a future gets driven, and that difference is a
/// property of the *target*, not of the backend: off wasm every future here
/// resolves without yielding, so a one-shot poll is enough; in a browser every
/// call parks on a JS Promise, so the case has to be an `async fn` that
/// `wasm-bindgen-test` drives.
///
/// Writing that split per case is what pushed the browser backend into its own
/// file with its own copies of these assertions. It costs one macro instead.
macro_rules! case {
    ($(#[$meta:meta])* $name:ident, $body:block) => {
        #[cfg(not(target_arch = "wasm32"))]
        $(#[$meta])*
        #[test]
        fn $name() {
            const CASE: &str = stringify!($name);
            block_on(async $body);
        }

        #[cfg(target_arch = "wasm32")]
        $(#[$meta])*
        #[wasm_bindgen_test::wasm_bindgen_test]
        async fn $name() {
            const CASE: &str = stringify!($name);
            $body
        }
    };
}

/// Run one async body on a plain executor.
///
/// Hand-rolled rather than `#[tokio::test]`: the trait's futures are `?Send`, so
/// binding this to a tokio runtime would bake in the one thing it must stay
/// free of. Off wasm only — a browser gets a real executor from
/// `wasm-bindgen-test`.
#[cfg(not(target_arch = "wasm32"))]
fn block_on<F: Future>(future: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWake;
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    // Polled once, deliberately: every future here resolves without yielding. A
    // backend that introduces real I/O will park, and should bring its own
    // driver rather than have this one silently spin.
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("a conformance future parked; this suite has no reactor"),
    }
}

struct MemHarness(MemStore);

impl Harness for MemHarness {
    type Store = MemStore;
    fn store(&self) -> &MemStore {
        &self.0
    }
    fn key(&self, name: &str) -> String {
        name.to_owned()
    }
    async fn place(&self, _file: &FileId, _bytes: &[u8]) {}
}

/// A throwaway directory, removed on drop. Hand-rolled rather than pulling
/// `tempfile` in: a dev-dependency is still a dependency to audit, and this is
/// eight lines.
///
/// Native-only, along with the `FsStore` harness below: a wasm build has no
/// `std::fs`, and the browser's backend is exercised in `opfs_browser.rs`
/// instead — it needs a real executor and a real browser, which this suite has
/// neither of.
#[cfg(not(target_arch = "wasm32"))]
struct TempDir(PathBuf);

#[cfg(not(target_arch = "wasm32"))]
impl TempDir {
    fn new(tag: &str) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "fofoca-blobs-{}-{tag}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self(path)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct FsHarness {
    store: fofoca_blobs::FsStore,
    /// Where *data* lives — deliberately not the store's own directory. The
    /// point of this backend is that the caller's files are somewhere else.
    data: TempDir,
    _meta: TempDir,
}

#[cfg(not(target_arch = "wasm32"))]
impl FsHarness {
    fn new() -> Self {
        let meta = TempDir::new("meta");
        let data = TempDir::new("data");
        let store = fofoca_blobs::FsStore::open(&meta.0).expect("open store");
        Self {
            store,
            data,
            _meta: meta,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Harness for FsHarness {
    type Store = fofoca_blobs::FsStore;
    fn store(&self) -> &fofoca_blobs::FsStore {
        &self.store
    }
    fn key(&self, name: &str) -> String {
        self.data.0.join(name).to_string_lossy().into_owned()
    }
    async fn place(&self, file: &FileId, bytes: &[u8]) {
        std::fs::write(&file.key, bytes).expect("place the caller's file");
    }
}

macro_rules! backends {
    ($($name:ident => |$tag:ident| $ctor:expr),* $(,)?) => {
        $(
            mod $name {
                use super::*;

                // `async` because a browser store opens over a Promise, and it
                // may name itself after `CASE` — the enclosing case's name,
                // which `case!` puts in scope. OPFS and IndexedDB persist
                // across runs by design, so two cases sharing a store name
                // would see each other's leftovers.
                async fn harness($tag: &str) -> impl Harness {
                    $ctor
                }

                case!(an_unknown_root_is_empty_not_an_error, {
                    let harness = harness(CASE).await;
                    let missing = [0xAB; 32];
                    assert!(
                        harness.store().present(missing).await.expect("present").is_empty(),
                        "a root never seen holds nothing"
                    );
                    assert!(
                        harness.store().outboard(missing).await.expect("outboard").is_none()
                    );
                });

                case!(a_complete_file_reports_everything_present, {
                    let harness = harness(CASE).await;
                    let bytes = data(1 << 20);
                    let id = harness.file("a.bin", bytes.len() as u64);
                    harness.place(&id, &bytes).await;
                    let root = harness.store().insert_complete(&id, &bytes).await.expect("insert");

                    assert!(!harness.store().present(root).await.expect("present").is_empty());
                    assert_eq!(harness.store().bind(&id).await.expect("bind"), Some(root));
                });

                case!(
                    /// **The version gate.** A file whose bytes moved is unbound, not
                    /// stale-but-usable: serving it would answer with one file's
                    /// content under another's name.
                    a_file_that_changed_is_unbound,
                    {
                    let harness = harness(CASE).await;
                    let bytes = data(4096);
                    let id = harness.file("a.bin", bytes.len() as u64);
                    harness.place(&id, &bytes).await;
                    harness.store().insert_complete(&id, &bytes).await.expect("insert");

                    let grown = FileId { size: id.size + 1, ..id.clone() };
                    assert_eq!(
                        harness.store().bind(&grown).await.expect("bind"), None, "size moved"
                    );

                    let touched = FileId { mtime: id.mtime + 1, ..id.clone() };
                    assert_eq!(
                        harness.store().bind(&touched).await.expect("bind"), None, "mtime moved"
                    );

                    assert!(
                        harness.store().bind(&id).await.expect("bind").is_some(),
                        "unchanged still binds"
                    );
                });

                case!(
                    /// **Stage 2b's kill-gate.** A file edited after it was hashed
                    /// must be refused, never served from an outboard describing
                    /// content that is gone.
                    a_file_edited_after_hashing_is_refused_not_mis_served,
                    {
                    let harness = harness(CASE).await;
                    let bytes = data(1 << 20);
                    let id = harness.file("mutable.bin", bytes.len() as u64);
                    harness.place(&id, &bytes).await;
                    harness.store().insert_complete(&id, &bytes).await.expect("insert");

                    // The caller looks again and sees different metadata,
                    // exactly as a rescan would.
                    let edited = FileId { size: id.size + 10, ..id.clone() };
                    assert!(
                        harness.store().read_ranges(&edited, &ChunkRanges::all()).await.is_err(),
                        "an edited file must not be served under its old root"
                    );
                });

                case!(an_unbound_key_is_none_rather_than_an_error, {
                    let harness = harness(CASE).await;
                    let unseen = harness.file("never-seen", 10);
                    assert_eq!(harness.store().bind(&unseen).await.expect("bind"), None);
                });

                case!(
                    /// A peer takes verified bytes from a third party and can then
                    /// serve them onward. Re-seeding, in one test.
                    verified_bytes_become_servable,
                    {
                    let origin = harness(CASE).await;
                    let bytes = data(1 << 20);
                    let id = origin.file("a.bin", bytes.len() as u64);
                    origin.place(&id, &bytes).await;
                    let root = origin.store().insert_complete(&id, &bytes).await.expect("insert");

                    let all = ChunkRanges::all();
                    let encoded = origin.store().read_ranges(&id, &all).await.expect("read");

                    let mirror = harness(CASE).await;
                    let dest = mirror.file("mirrored.bin", bytes.len() as u64);
                    let held = mirror
                        .store()
                        .write_verified(&dest, root, &encoded, &all)
                        .await
                        .expect("write verified");
                    assert!(!held.is_empty());

                    let onward = mirror.store().read_ranges(&dest, &all).await.expect("re-serve");
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
                });

                case!(
                    /// Tampered bytes must leave the store exactly as it was.
                    a_tampered_write_is_rejected_and_changes_nothing,
                    {
                    let bytes = data(1 << 20);
                    let (root, _) = build_outboard(&bytes);
                    let all = ChunkRanges::all();
                    let mut encoded = encode_ranges(&bytes, &all).expect("encode");
                    let last = encoded.len() - 1;
                    encoded[last] ^= 0x01;

                    let harness = harness(CASE).await;
                    let dest = harness.file("tampered.bin", bytes.len() as u64);
                    assert!(
                        harness.store().write_verified(&dest, root, &encoded, &all).await.is_err(),
                        "a flipped bit must not be accepted"
                    );
                    assert!(
                        harness.store().present(root).await.expect("present").is_empty(),
                        "a rejected write must leave nothing behind"
                    );
                });

                case!(
                    /// **Never answer short.** A caller cannot tell a truncated
                    /// answer from a small file, so a store that does not hold a
                    /// range must say so.
                    reading_a_range_we_do_not_hold_fails_rather_than_truncating,
                    {
                    let bytes = data(1 << 20);
                    let (root, _) = build_outboard(&bytes);
                    let head = ChunkRanges::from(ChunkNum(0)..ChunkNum(64));
                    let partial = encode_ranges(&bytes, &head).expect("encode head");

                    let harness = harness(CASE).await;
                    let dest = harness.file("partial.bin", bytes.len() as u64);
                    harness
                        .store()
                        .write_verified(&dest, root, &partial, &head)
                        .await
                        .expect("write head");

                    assert!(
                        harness.store().read_ranges(&dest, &ChunkRanges::all()).await.is_err(),
                        "asking for the whole file from a partial store must fail"
                    );
                    assert!(
                        harness.store().read_ranges(&dest, &head).await.is_ok(),
                        "the part it does hold is still servable"
                    );
                });

                case!(
                    /// **A partial mirror must serve proofs against the real root.**
                    /// A store that rebuilds the tree from its own file computes it
                    /// over the holes too, so the proofs it emits are against a root
                    /// nobody else has — and the receiver rejects every one. The
                    /// bytes look fine locally; the failure only shows up a hop away,
                    /// which is why this decodes the serve instead of trusting `is_ok`.
                    a_partially_held_file_is_served_against_the_real_root,
                    {
                    let bytes = data(1 << 20);
                    let (root, _) = build_outboard(&bytes);
                    let head = ChunkRanges::from(ChunkNum(0)..ChunkNum(64));
                    let partial = encode_ranges(&bytes, &head).expect("encode head");

                    let harness = harness(CASE).await;
                    let dest = harness.file("partial-serve.bin", bytes.len() as u64);
                    harness
                        .store()
                        .write_verified(&dest, root, &partial, &head)
                        .await
                        .expect("write head");

                    let onward = harness
                        .store()
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
                });

                case!(
                    /// Partial availability accumulates rather than replacing, or a
                    /// mirror would forget every range but its last.
                    ranges_accumulate_across_writes,
                    {
                    let bytes = data(1 << 20);
                    let (root, _) = build_outboard(&bytes);
                    let harness = harness(CASE).await;
                    let dest = harness.file("accum.bin", bytes.len() as u64);

                    let first = ChunkRanges::from(ChunkNum(0)..ChunkNum(64));
                    let second = ChunkRanges::from(ChunkNum(64)..ChunkNum(128));
                    for window in [&first, &second] {
                        let encoded = encode_ranges(&bytes, window).expect("encode");
                        harness
                            .store()
                            .write_verified(&dest, root, &encoded, window)
                            .await
                            .expect("write");
                    }

                    let held = harness.store().present(root).await.expect("present");
                    assert!(first.is_subset(&held), "the first window survived");
                    assert!(second.is_subset(&held), "the second landed");

                    // Read the first window back, not just its bookkeeping. A
                    // store that writes each arrival over the whole file keeps
                    // advertising the earlier window while serving the zeroes
                    // that replaced it, and `present` alone cannot see that.
                    let onward = harness
                        .store()
                        .read_ranges(&dest, &first)
                        .await
                        .expect("the first window is still servable");
                    let mut target = Vec::new();
                    let mut outboard = Vec::new();
                    fofoca_blobs::decode_into(
                        root,
                        bytes.len() as u64,
                        &onward,
                        &first,
                        &mut target,
                        &mut outboard,
                    )
                    .expect("an earlier window must survive a later write");
                    let end = usize::try_from(ChunkNum(64).to_bytes()).expect("fits");
                    assert_eq!(
                        &target[..end],
                        &bytes[..end],
                        "the first window must still be its own bytes, not zeroes"
                    );
                });

                case!(an_outboard_round_trips, {
                    let bytes = data(1 << 20);
                    let (root, outboard) = build_outboard(&bytes);
                    let harness = harness(CASE).await;
                    harness
                        .store()
                        .put_outboard(root, outboard.clone(), ChunkRanges::all())
                        .await
                        .expect("put");
                    assert_eq!(harness.store().outboard(root).await.expect("get"), Some(outboard));
                });

                case!(
                    /// Zero-length files exist and must not be a special case that
                    /// panics somewhere downstream.
                    an_empty_file_is_ordinary,
                    {
                    let harness = harness(CASE).await;
                    let id = harness.file("empty.bin", 0);
                    harness.place(&id, &[]).await;
                    let root = harness.store().insert_complete(&id, &[]).await.expect("insert");
                    assert_eq!(harness.store().bind(&id).await.expect("bind"), Some(root));
                });
            }
        )*
    };
}

/// A browser-backed harness. Both browser stores hold the bytes themselves, so
/// `place` materializes through the store rather than writing a file beside it.
///
/// `dir`/`db` are scoped by the case name: OPFS and IndexedDB persist across
/// runs, so a shared name would let one case read another's leftovers.
#[cfg(target_arch = "wasm32")]
struct OpfsHarness {
    store: fofoca_blobs::OpfsStore,
    dir: String,
}

#[cfg(target_arch = "wasm32")]
impl OpfsHarness {
    fn new(tag: &str) -> Self {
        let dir = format!("fofoca-blobs-test/{tag}");
        Self {
            store: fofoca_blobs::OpfsStore::new(dir.clone()),
            dir,
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl Harness for OpfsHarness {
    type Store = fofoca_blobs::OpfsStore;
    fn store(&self) -> &Self::Store {
        &self.store
    }
    fn key(&self, name: &str) -> String {
        format!("{}/data/{name}", self.dir)
    }
    async fn place(&self, file: &FileId, bytes: &[u8]) {
        materialize(&self.store, file, bytes).await;
    }
}

#[cfg(target_arch = "wasm32")]
struct IdbHarness {
    store: fofoca_blobs::IdbStore,
}

#[cfg(target_arch = "wasm32")]
impl IdbHarness {
    async fn open(tag: &str) -> Self {
        let store = fofoca_blobs::IdbStore::open(&format!("fofoca-blobs-test-{tag}"))
            .await
            .expect("open IndexedDB");
        Self { store }
    }
}

#[cfg(target_arch = "wasm32")]
impl Harness for IdbHarness {
    type Store = fofoca_blobs::IdbStore;
    fn store(&self) -> &Self::Store {
        &self.store
    }
    fn key(&self, name: &str) -> String {
        name.to_owned()
    }
    async fn place(&self, file: &FileId, bytes: &[u8]) {
        materialize(&self.store, file, bytes).await;
    }
}

/// Write `bytes` into a store that owns its own data, verified, so a later read
/// has something to serve. The host backends instead write a real file and let
/// the store describe it.
#[cfg(target_arch = "wasm32")]
async fn materialize(store: &impl BlobStore, file: &FileId, bytes: &[u8]) {
    if bytes.is_empty() {
        return;
    }
    store
        .write_verified(
            file,
            build_outboard(bytes).0,
            &encode_ranges(bytes, &ChunkRanges::all()).expect("encode"),
            &ChunkRanges::all(),
        )
        .await
        .expect("materialize the caller's bytes");
}

backends!(mem => |_tag| MemHarness(MemStore::new()));

#[cfg(not(target_arch = "wasm32"))]
backends!(fs => |_tag| FsHarness::new());

#[cfg(target_arch = "wasm32")]
backends!(opfs => |tag| OpfsHarness::new(tag));

#[cfg(target_arch = "wasm32")]
backends!(idb => |tag| IdbHarness::open(tag).await);
