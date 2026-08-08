//! Verified byte ranges over data you already own.
//!
//! A store of BLAKE3/bao verification metadata — outboards, root bindings, and
//! which ranges are held — for bytes that live somewhere this crate does not
//! control. That inversion is the whole design:
//!
//! > **`iroh-blobs` owns its data. This does not.**
//!
//! `iroh-blobs` takes bytes and stores them under a hash, so it cannot serve a
//! blob until it has read every byte and built its tree. That turns listing a
//! large share from a metadata walk into a full read, re-hashes on every editor
//! save, and makes browsing a 500 GB tree while reading three files impossible.
//! Here the blob *is* the caller's file, wherever it already is; the store holds
//! only the sidecar metadata beside it.
//!
//! The second first-class concept, which a content-addressed store has no room
//! for:
//!
//! > **A root is bound to a version: `(size, mtime) -> root`.**
//!
//! Mutable files are supported, not a violation. A file whose size or mtime has
//! moved since its outboard was built is *unbound*, and an unbound file is not
//! served rather than being served wrongly.
//!
//! # What this crate is not
//!
//! No transport, no ALPN, no framing. No discovery. No download scheduler — that
//! needs peer budgets and connection state, and making it generic is how
//! `iroh-blobs` gets rebuilt by accident. No GC, no tags, no collections: the
//! caller's own directory listing is the collection.
//!
//! It also does not know what a mesh is, and depends on nothing else in this
//! workspace. It takes a key, a size, an mtime and byte ranges. See
//! `tests/isolation.rs`.
//!
//! # Both runtimes
//!
//! The same store runs under tokio and in a browser, so every future here is
//! `?Send`: a `MemStore` in wasm holds `Rc`-flavoured state and an OPFS backend
//! is pinned to one Worker thread. Callers that need `Send` get it from their
//! own backend, not from this trait.

use std::future::Future;

use anyhow::{Context as _, Result, bail};
use bao_tree::BlockSize;

/// The filesystem backend.
///
/// Host-only, and gated on the target rather than a feature so it cannot be
/// misconfigured: a wasm build simply does not have it, and `OpfsStore` is what
/// a browser uses instead. `std::fs` is not a dependency, so the crate stays
/// wasm-clean either way.
#[cfg(not(target_arch = "wasm32"))]
pub mod fs;
/// The browser backend that needs no Worker. See the module docs.
#[cfg(target_arch = "wasm32")]
pub mod idb;
/// The JS-error adapter both browser backends share.
#[cfg(target_arch = "wasm32")]
mod js;
pub mod mem;
/// The browser backend. Worker-only; see the module docs.
#[cfg(target_arch = "wasm32")]
pub mod opfs;
mod sidecar;
pub mod sparse;
mod verify;

#[cfg(not(target_arch = "wasm32"))]
pub use fs::FsStore;
#[cfg(target_arch = "wasm32")]
pub use idb::IdbStore;
pub use mem::MemStore;
#[cfg(target_arch = "wasm32")]
pub use opfs::OpfsStore;
pub use sparse::SparseBlocks;
pub use verify::{
    Outboard, Root, build_outboard, decode_into, decode_sparse, encode_from_outboard, encode_ranges,
};

/// Re-exported so a caller can name a range set without depending on
/// `bao-tree` directly.
///
/// The alternative is every consumer taking the dependency to spell one type,
/// which would put the verification format in their manifests and make swapping
/// it a change to all of them rather than to this crate.
pub use bao_tree::{ChunkNum, ChunkRanges};

/// Chunk group size: 64 `KiB`, i.e. 2^6 chunks of 1 `KiB`.
///
/// Measured rather than assumed. Against 16 `KiB` this is **four
/// times smaller outboards** — 0.097 % of file size against 0.390 % — at
/// indistinguishable construction speed, and it lines up with the 128 `KiB` reads
/// the kernel issues through NFS. The cost is coarser partial-seed granularity
/// and more bytes discarded when a range fails to verify.
pub const BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(6);

/// Bytes per chunk group, derived so the two can never disagree.
pub const CHUNK_GROUP_BYTES: u64 = 1024 << 6;

/// What a caller knows about a file, independent of where the bytes live.
///
/// The store never opens this itself — that is the backend's job — and never
/// interprets `key`. Native backends use a path; a browser backend uses an OPFS
/// name. Keeping it opaque is what stops this crate learning about filesystems.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileId {
    /// Opaque to this crate. Whatever the backend can reopen the bytes with.
    pub key: String,
    /// Size at the moment the caller looked.
    pub size: u64,
    /// Seconds since the epoch; `0` when unknown.
    pub mtime: i64,
}

/// The chunk ranges a file of `size` bytes actually occupies.
///
/// Load-bearing, and the source of a bug worth naming: **`ChunkRanges::all()` is
/// unbounded** — `0..∞`, not `0..len`. So a request for "everything" is never a
/// subset of what any store holds, and a completeness check written the obvious
/// way rejects a store that is in fact complete. Clamp a caller's request to
/// this before comparing it with anything.
#[must_use]
pub fn extent_of(size: u64) -> ChunkRanges {
    ChunkRanges::from(..ChunkNum::chunks(size))
}

impl FileId {
    /// Whether `other` describes the same file *version* as this one.
    ///
    /// The comparison guarding every read. Two files at one key with different
    /// sizes or mtimes are different content, and serving one under the other's
    /// root is the silent-corruption case this exists to prevent.
    #[must_use]
    pub fn same_version(&self, other: &Self) -> bool {
        self.key == other.key && self.size == other.size && self.mtime == other.mtime
    }
}

/// A store of verification metadata for bytes held elsewhere.
///
/// Futures are `?Send` on purpose — see the module docs. Implementors are used
/// through generics rather than `dyn`, so a backend never pays for type erasure
/// it does not need.
pub trait BlobStore {
    /// Adopt content this peer already holds at `file.key`: hash it, bind it,
    /// and mark it fully servable.
    ///
    /// What an origin does the first time someone wants to pull one of its own
    /// files from a third party.
    ///
    /// **Required rather than provided, and that is the design showing
    /// through.** A default written in terms of [`Self::write_verified`] would
    /// force every backend to *copy* the bytes into itself, which is exactly
    /// the ownership model this crate exists to avoid. A filesystem backend
    /// implements this by recording metadata beside a file it never reads
    /// again; an in-memory one has nowhere else to put the bytes and does copy.
    /// The difference is the point, so the trait asks rather than assumes.
    fn insert_complete(&self, file: &FileId, bytes: &[u8]) -> impl Future<Output = Result<Root>>;

    /// The root this key is bound to, if the file still matches the version the
    /// binding was made for.
    ///
    /// `None` means *do not serve this*: either nothing was ever bound, or the
    /// file moved underneath and the outboard describes content that is gone.
    /// Both are ordinary, neither is an error.
    fn bind(&self, file: &FileId) -> impl Future<Output = Result<Option<Root>>>;
    /// Record that `file` hashes to `root`, replacing any previous binding.
    fn set_bind(&self, file: &FileId, root: Root) -> impl Future<Output = Result<()>>;

    /// Which chunk ranges of `root` this store can serve right now.
    ///
    /// Empty for a root it has never seen. This is the answer to "what can I
    /// seed", and the source for both the availability grid and a scheduler.
    fn present(&self, root: Root) -> impl Future<Output = Result<ChunkRanges>>;

    /// The outboard for `root`, if one has been built.
    fn outboard(&self, root: Root) -> impl Future<Output = Result<Option<Outboard>>>;
    /// Store an outboard, making its ranges servable.
    fn put_outboard(
        &self,
        root: Root,
        outboard: Outboard,
        present: ChunkRanges,
    ) -> impl Future<Output = Result<()>>;

    /// Read verified bytes for `ranges` of the file at `file`.
    ///
    /// Takes a [`FileId`] rather than a bare [`Root`] because a store does not
    /// necessarily hold the bytes: a filesystem backend reads them back out of
    /// the caller's own file, and needs to know which one. The root is resolved
    /// through [`Self::bind`], so **the version gate applies to every read** —
    /// a file that moved under the store is refused rather than served from a
    /// stale outboard.
    ///
    /// Errors rather than returning short when a requested range is absent: a
    /// short read is how a half-written mirror silently truncates a file, and
    /// distinguishing "EOF" from "I do not have this" is the difference.
    fn read_ranges(
        &self,
        file: &FileId,
        ranges: &ChunkRanges,
    ) -> impl Future<Output = Result<Vec<u8>>>;

    /// Take bytes from a third party, verify them against `root`, and keep what
    /// verifies — materializing them at `file`.
    ///
    /// `file` is the destination, and naming it here is what preserves the
    /// crate's central property: **the store never chooses where bytes live.**
    /// A mirror decides that; the store verifies and records. `file.size` is the
    /// full length of the content, not of this write.
    ///
    /// Returns the ranges now held. A failure leaves the store unchanged, so a
    /// peer that sends garbage costs its own bytes and nothing else.
    fn write_verified(
        &self,
        file: &FileId,
        root: Root,
        encoded: &[u8],
        ranges: &ChunkRanges,
    ) -> impl Future<Output = Result<ChunkRanges>>;
}

/// Everything [`BlobStore::read_ranges`] must establish before it can encode:
/// the root this version is bound to, the outboard that proves it, and the
/// clamped ranges the caller may actually be answered for.
///
/// Shared rather than repeated per backend because two of the three steps are
/// refusals, and a backend that forgets one serves wrongly instead of failing.
/// The outboard comes from storage and is never rebuilt from the file: a file
/// held only in part is still full length, its gaps left as the zeroes the
/// backend's own sizing call wrote, so rehashing yields a root no peer shares
/// and every proof is rejected by whoever asked.
pub(crate) async fn resolve_read<S: BlobStore>(
    store: &S,
    file: &FileId,
    ranges: &ChunkRanges,
) -> Result<(Root, Outboard, ChunkRanges)> {
    let root = store
        .bind(file)
        .await?
        .context("this file is unbound: it was never hashed, or it changed since it was")?;

    // Clamp before comparing: `ChunkRanges::all()` is `0..∞`, so an unclamped
    // subset check calls every store incomplete, including a complete one.
    let wanted = ranges.clone() & extent_of(file.size);
    if !wanted.is_subset(&store.present(root).await?) {
        // Refuse rather than answer short. A caller cannot tell a truncated
        // answer from a small file, which is how a mirror silently loses tails.
        bail!("requested ranges are not all held");
    }

    let outboard = store
        .outboard(root)
        .await?
        .context("bound to a root whose outboard is gone")?;
    Ok((root, outboard, wanted))
}
