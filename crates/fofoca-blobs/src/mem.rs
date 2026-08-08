//! An in-memory store.
//!
//! Exists to answer the shape questions — does one `?Send` trait serve tokio and
//! a browser, does verify-in-place hold together, is the range bookkeeping
//! right — with none of the I/O that would obscure them. It is also the
//! reference the other backends are checked against: the conformance suite in
//! `tests/` is written once here and re-run per backend.
//!
//! Unlike a real backend it *does* hold the bytes, which is the one way it is
//! unrepresentative. That is deliberate: a test double that reopens files would
//! be testing the filesystem.

use std::cell::RefCell;
use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use bao_tree::ChunkRanges;
use range_collections::range_set::RangeSetRange;

use crate::{BlobStore, FileId, Outboard, Root, decode_into, encode_from_outboard, extent_of};

/// What the store knows about one root.
#[derive(Debug, Clone, Default)]
struct Entry {
    outboard: Option<Outboard>,
    /// The verified bytes held so far, sparse-filled. Indexed absolutely, so a
    /// range written at the end does not move a range written at the start.
    data: Vec<u8>,
    present: ChunkRanges,
}

/// A store with everything in memory. Not `Sync`: one browser Worker owns it.
#[derive(Debug, Default)]
pub struct MemStore {
    entries: RefCell<HashMap<Root, Entry>>,
    /// `(size, mtime) -> root`, keyed by the caller's opaque key. The whole
    /// version binding: a key alone is not enough to serve by.
    binds: RefCell<HashMap<String, (u64, i64, Root)>>,
}

impl MemStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl BlobStore for MemStore {
    /// Copies, because memory is the only place this backend can put bytes.
    /// A filesystem backend records metadata instead and leaves the file be —
    /// see the trait's note on why this is not a provided method.
    async fn insert_complete(&self, file: &FileId, bytes: &[u8]) -> Result<Root> {
        let (root, outboard) = crate::build_outboard(bytes);
        self.entries.borrow_mut().insert(
            root,
            Entry {
                outboard: Some(outboard),
                data: bytes.to_vec(),
                present: ChunkRanges::all(),
            },
        );
        self.binds
            .borrow_mut()
            .insert(file.key.clone(), (file.size, file.mtime, root));
        Ok(root)
    }

    async fn bind(&self, file: &FileId) -> Result<Option<Root>> {
        Ok(crate::sidecar::gate(
            self.binds.borrow().get(&file.key),
            file,
        ))
    }

    async fn set_bind(&self, file: &FileId, root: Root) -> Result<()> {
        self.binds
            .borrow_mut()
            .insert(file.key.clone(), (file.size, file.mtime, root));
        Ok(())
    }

    async fn present(&self, root: Root) -> Result<ChunkRanges> {
        Ok(self
            .entries
            .borrow()
            .get(&root)
            .map(|entry| entry.present.clone())
            .unwrap_or_default())
    }

    async fn outboard(&self, root: Root) -> Result<Option<Outboard>> {
        Ok(self
            .entries
            .borrow()
            .get(&root)
            .and_then(|entry| entry.outboard.clone()))
    }

    async fn put_outboard(
        &self,
        root: Root,
        outboard: Outboard,
        present: ChunkRanges,
    ) -> Result<()> {
        let mut entries = self.entries.borrow_mut();
        let entry = entries.entry(root).or_default();
        entry.outboard = Some(outboard);
        entry.present = present;
        Ok(())
    }

    async fn read_ranges(&self, file: &FileId, ranges: &ChunkRanges) -> Result<Vec<u8>> {
        // Through the binding, so a file that moved is refused here too rather
        // than served from an outboard describing content that is gone.
        let root = self
            .bind(file)
            .await?
            .context("no binding for this file version")?;
        let entries = self.entries.borrow();
        let entry = entries.get(&root).context("no such root in this store")?;
        // Clamp before comparing: `ChunkRanges::all()` is `0..∞`, so an
        // unclamped subset check calls every store incomplete, including one
        // holding the whole file.
        let wanted = ranges.clone() & extent_of(entry.data.len() as u64);
        // Refuse rather than return short. A caller cannot tell a truncated
        // answer from a small file, so answering partially is how a mirror
        // silently loses the tail of everything it holds.
        if !wanted.is_subset(&entry.present) {
            bail!("requested ranges are not all held");
        }
        // From the stored outboard, never rebuilt from `data`: the gaps between
        // held ranges are zeroes, and hashing those produces a root no peer
        // shares, so every proof would be rejected by whoever asked for it.
        let outboard = entry
            .outboard
            .clone()
            .context("bound to a root whose outboard is gone")?;
        encode_from_outboard(root, file.size, outboard, entry.data.as_slice(), &wanted)
    }

    async fn write_verified(
        &self,
        file: &FileId,
        root: Root,
        encoded: &[u8],
        ranges: &ChunkRanges,
    ) -> Result<ChunkRanges> {
        // Verify into a scratch buffer first. Committing before the proof
        // checks out is what would let one bad peer corrupt a store.
        let mut entries = self.entries.borrow_mut();
        let mut outboard = entries
            .get(&root)
            .and_then(|entry| entry.outboard.clone())
            .unwrap_or_default();
        let mut target = Vec::new();
        let verified = decode_into(root, file.size, encoded, ranges, &mut target, &mut outboard)?;

        let entry = entries.entry(root).or_default();
        let wanted = usize::try_from(file.size).context("file too large for this target")?;
        if entry.data.len() < wanted {
            entry.data.resize(wanted, 0);
        }
        // Copy only what verified. `decode_into` sizes its target to the whole
        // file and zero-fills the rest, so copying it wholesale would erase every
        // range an earlier write had put there while `present` went on claiming
        // them — bytes advertised as held and served back as zeroes.
        for range in verified.iter() {
            if let RangeSetRange::Range(range) = range {
                let start = range.start.to_bytes();
                let end = range.end.to_bytes().min(file.size);
                let (Ok(from), Ok(to)) = (usize::try_from(start), usize::try_from(end)) else {
                    bail!("range does not fit this target");
                };
                let to = to.min(target.len()).min(entry.data.len());
                entry.data[from..to].copy_from_slice(&target[from..to]);
            }
        }
        entry.outboard = Some(outboard);
        entry.present |= verified;
        let held = entry.present.clone();
        drop(entries);
        // Bind on the way out: a mirror that verified bytes into place should be
        // able to serve them without a separate call it might forget to make.
        self.binds
            .borrow_mut()
            .insert(file.key.clone(), (file.size, file.mtime, root));
        Ok(held)
    }
}
