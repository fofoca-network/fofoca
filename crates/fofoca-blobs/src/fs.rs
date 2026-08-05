//! A filesystem store: metadata beside files it does not own.
//!
//! This is the backend the whole design is shaped around. It **never copies the
//! caller's data**. An origin's file stays exactly where the user put it; all
//! this holds is an outboard, a version binding and a range set, in a directory
//! of its own. That is what keeps `serve` a `stat` walk over a 500 GB tree
//! instead of a full read of it.
//!
//! The one case where bytes do land here is a *mirror*, and even then the store
//! does not choose the destination: [`BlobStore::write_verified`] is told which
//! file to materialize, and the caller owns that decision.
//!
//! # Layout
//!
//! ```text
//! <root>/
//!   <root-hex>.obao      the outboard
//!   <root-hex>.ranges    which chunk ranges are held
//!   binds.tsv            key -> (size, mtime, root)
//! ```
//!
//! Sidecars keyed by root rather than by path, because two paths holding the
//! same content share one outboard — and because a rename must not orphan the
//! work of hashing.
//!
//! # Crash consistency
//!
//! A range set that outlives its data is the dangerous direction: it makes this
//! peer advertise bytes it cannot serve, which to everyone else looks like a
//! peer that lies. So data is flushed before the range set that describes it is
//! written, and a binding is re-checked against the file's current size and
//! mtime on every read. The reverse — data present but ranges lost — costs a
//! re-fetch and nothing else.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result, bail};
use bao_tree::ChunkRanges;
use range_collections::range_set::RangeSetRange;

use crate::sidecar::{format_binds, format_ranges, hex, parse_binds, parse_ranges};
use crate::{BlobStore, FileId, Outboard, Root, decode_into, encode_ranges, extent_of};

/// A store whose metadata lives under one directory and whose data does not.
///
/// `Mutex` rather than `RefCell`, and that is the `?Send` trait's bargain being
/// settled here rather than in the trait: futures are `?Send` so a browser can
/// implement one at all, and a caller that needs `Send` — the native producer
/// does, since it answers each request in a spawned task — gets it from the
/// backend. Every lock below is taken and released without an await in between.
#[derive(Debug)]
pub struct FsStore {
    root: PathBuf,
    /// `key -> (size, mtime, root)`, mirrored in memory and written through.
    binds: Mutex<HashMap<String, (u64, i64, Root)>>,
}

impl FsStore {
    /// Open (creating if absent) a store keeping its metadata under `dir`.
    ///
    /// A bind table that cannot be read is not an error: it is rebuilt as files
    /// are hashed again. Losing bindings costs re-hashing, never correctness.
    ///
    /// # Errors
    /// `dir` cannot be created.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let root = dir.as_ref().to_path_buf();
        fs::create_dir_all(&root)
            .with_context(|| format!("creating the store directory {}", root.display()))?;
        let store = Self {
            root,
            binds: Mutex::new(HashMap::new()),
        };
        store.load_binds();
        Ok(store)
    }

    fn obao_path(&self, root: Root) -> PathBuf {
        self.root.join(format!("{}.obao", hex(root)))
    }

    fn ranges_path(&self, root: Root) -> PathBuf {
        self.root.join(format!("{}.ranges", hex(root)))
    }

    fn binds_path(&self) -> PathBuf {
        self.root.join("binds.tsv")
    }

    fn load_binds(&self) {
        let Ok(text) = fs::read_to_string(self.binds_path()) else {
            return;
        };
        let mut binds = self
            .binds
            .lock()
            .expect("the bind table lock is never held across an await");
        for (key, size, mtime, root) in parse_binds(&text) {
            binds.insert(key, (size, mtime, root));
        }
    }

    fn save_binds(&self) -> Result<()> {
        let binds: Vec<_> = self
            .binds
            .lock()
            .expect("the bind table lock is never held across an await")
            .iter()
            .map(|(key, (size, mtime, root))| (key.clone(), *size, *mtime, *root))
            .collect();
        write_durably(&self.binds_path(), format_binds(&binds)?.as_bytes())
    }

    fn load_ranges(&self, root: Root) -> ChunkRanges {
        let Ok(text) = fs::read_to_string(self.ranges_path(root)) else {
            return ChunkRanges::empty();
        };
        parse_ranges(&text)
    }

    fn save_ranges(&self, root: Root, ranges: &ChunkRanges) -> Result<()> {
        write_durably(&self.ranges_path(root), format_ranges(ranges).as_bytes())
    }

    /// Read the caller's file.
    ///
    /// Takes no `self` and that is the design visible in a signature: the store
    /// holds nothing to read *from*. It borrows the caller's bytes for the
    /// length of one call and keeps none of them.
    fn read_data(file: &FileId) -> Result<Vec<u8>> {
        let mut handle = fs::File::open(&file.key)
            .with_context(|| format!("opening {} to read its bytes", file.key))?;
        let mut bytes = Vec::new();
        handle
            .read_to_end(&mut bytes)
            .with_context(|| format!("reading {}", file.key))?;
        Ok(bytes)
    }
}

/// Write, flush, and fsync before returning.
///
/// The flush is the point. Recording that a range is held before its bytes are
/// on disk is what turns a power cut into a peer that advertises what it cannot
/// serve.
fn write_durably(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut handle =
        fs::File::create(path).with_context(|| format!("creating {}", path.display()))?;
    handle
        .write_all(bytes)
        .with_context(|| format!("writing {}", path.display()))?;
    handle
        .sync_all()
        .with_context(|| format!("flushing {}", path.display()))?;
    Ok(())
}

impl BlobStore for FsStore {
    /// Records metadata and **does not copy the file**.
    ///
    /// The difference from `MemStore`, and the reason this method is required
    /// rather than provided: an origin's bytes stay where the user put them.
    async fn insert_complete(&self, file: &FileId, bytes: &[u8]) -> Result<Root> {
        let (root, outboard) = crate::build_outboard(bytes);
        write_durably(&self.obao_path(root), &outboard)?;
        self.save_ranges(root, &extent_of(file.size))?;
        self.binds
            .lock()
            .expect("the bind table lock is never held across an await")
            .insert(file.key.clone(), (file.size, file.mtime, root));
        self.save_binds()?;
        Ok(root)
    }

    async fn bind(&self, file: &FileId) -> Result<Option<Root>> {
        let binds = self
            .binds
            .lock()
            .expect("the bind table lock is never held across an await");
        let Some(&(size, mtime, root)) = binds.get(&file.key) else {
            return Ok(None);
        };
        if size != file.size || mtime != file.mtime {
            return Ok(None);
        }
        Ok(Some(root))
    }

    async fn set_bind(&self, file: &FileId, root: Root) -> Result<()> {
        self.binds
            .lock()
            .expect("the bind table lock is never held across an await")
            .insert(file.key.clone(), (file.size, file.mtime, root));
        self.save_binds()
    }

    async fn present(&self, root: Root) -> Result<ChunkRanges> {
        Ok(self.load_ranges(root))
    }

    async fn outboard(&self, root: Root) -> Result<Option<Outboard>> {
        Ok(fs::read(self.obao_path(root)).ok())
    }

    async fn put_outboard(
        &self,
        root: Root,
        outboard: Outboard,
        present: ChunkRanges,
    ) -> Result<()> {
        write_durably(&self.obao_path(root), &outboard)?;
        self.save_ranges(root, &present)
    }

    async fn read_ranges(&self, file: &FileId, ranges: &ChunkRanges) -> Result<Vec<u8>> {
        // **The version gate, on the read path.** The file on disk may have
        // moved since it was bound; an outboard describing content that is gone
        // would happily encode the new bytes and produce proofs nobody can
        // verify. Refuse instead.
        let root = self
            .bind(file)
            .await?
            .context("this file is unbound: it was never hashed, or it changed since it was")?;

        let held = self.load_ranges(root);
        let wanted = ranges.clone() & extent_of(file.size);
        if !wanted.is_subset(&held) {
            bail!("requested ranges are not all held");
        }

        let bytes = Self::read_data(file)?;
        // Re-check after reading, not just before: a file can change *during*
        // the read, and encoding those bytes under the old root would emit a
        // proof that fails on the far side for no visible reason.
        if bytes.len() as u64 != file.size {
            bail!(
                "{} changed while it was being read: {} bytes, expected {}",
                file.key,
                bytes.len(),
                file.size
            );
        }
        encode_ranges(&bytes, &wanted)
    }

    async fn write_verified(
        &self,
        file: &FileId,
        root: Root,
        encoded: &[u8],
        ranges: &ChunkRanges,
    ) -> Result<ChunkRanges> {
        // Verify before touching the destination. A peer that sends garbage
        // must not be able to leave a partly-written file behind.
        let mut target = Vec::new();
        let verified = decode_into(root, file.size, encoded, ranges, &mut target)?;

        // Sparse write: only the chunks that verified, at their real offsets,
        // so a mirror filling a large file from several peers does not rewrite
        // what it already has.
        let path = Path::new(&file.key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        let mut handle = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(path)
            .with_context(|| format!("opening {} to materialize it", file.key))?;
        handle
            .set_len(file.size)
            .with_context(|| format!("sizing {}", file.key))?;
        for range in verified.iter() {
            if let RangeSetRange::Range(range) = range {
                let start = range.start.to_bytes();
                let end = range.end.to_bytes().min(file.size);
                let (Ok(from), Ok(to)) = (usize::try_from(start), usize::try_from(end)) else {
                    bail!("range does not fit this target");
                };
                handle
                    .seek(SeekFrom::Start(start))
                    .with_context(|| format!("seeking in {}", file.key))?;
                handle
                    .write_all(&target[from..to.min(target.len())])
                    .with_context(|| format!("writing {}", file.key))?;
            }
        }
        // Data durable *before* the range set that advertises it. The other
        // order is what makes a peer claim bytes a crash took away.
        handle
            .sync_all()
            .with_context(|| format!("flushing {}", file.key))?;

        let held = self.load_ranges(root) | verified;
        self.save_ranges(root, &held)?;
        self.binds
            .lock()
            .expect("the bind table lock is never held across an await")
            .insert(file.key.clone(), (file.size, file.mtime, root));
        self.save_binds()?;
        Ok(held)
    }
}
