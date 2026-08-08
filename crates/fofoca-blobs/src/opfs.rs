//! The browser store, on the origin-private filesystem.
//!
//! The same shape as [`crate::fs`]: metadata in a directory of its own, data
//! wherever the caller says. The differences are all imposed by the platform.
//!
//! # Why a Worker
//!
//! `FileSystemSyncAccessHandle` is the only OPFS API with read and write at an
//! arbitrary offset, and it exists **only inside a Worker** — the window-scope
//! probe returns `false` (confirmed in Safari). That looked like a
//! tax and is not: hashing a multi-gigabyte mirror would jank the main thread
//! anyway, so one Worker owns both the handles and the hashing, and the
//! constraint and the requirement have the same solution.
//!
//! # Why the store is not `Send`
//!
//! Everything here is a `JsValue`, which is pinned to one thread. That is the
//! reason [`crate::BlobStore`]'s futures are `?Send` rather than `Send`: a trait
//! that demanded `Send` would have excluded the browser, which is the peer this
//! whole design exists to make into a seeder.
//!
//! # Persistence
//!
//! OPFS survives a reload — measured, not assumed. It is still
//! *evictable* under storage pressure, and a peer that advertises ranges the
//! browser later reclaims looks to everyone else like a peer that lies. See the
//! note on `present`.

use anyhow::{Context, Result, bail};
use bao_tree::ChunkRanges;
use range_collections::range_set::RangeSetRange;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    DedicatedWorkerGlobalScope, FileSystemDirectoryHandle, FileSystemFileHandle,
    FileSystemGetDirectoryOptions, FileSystemGetFileOptions, FileSystemReadWriteOptions,
    FileSystemSyncAccessHandle,
};

use crate::js::js_err;
use crate::sidecar;
use crate::{BlobStore, FileId, Outboard, Root, decode_into, encode_from_outboard, extent_of};

/// A store on the origin-private filesystem. Worker-only; not `Send`.
#[derive(Debug)]
pub struct OpfsStore {
    /// Directory holding this store's metadata, relative to the OPFS root.
    meta: String,
}

impl OpfsStore {
    /// Open a store keeping its metadata under `dir` in the OPFS root.
    ///
    /// Nothing is touched until the first operation: OPFS handles are per-call,
    /// because holding one open across an await is how a second caller gets
    /// `NoModificationAllowedError` on a file this one is not using.
    #[must_use]
    pub fn new(dir: impl Into<String>) -> Self {
        Self { meta: dir.into() }
    }

    /// The OPFS root, from inside a Worker.
    async fn root() -> Result<FileSystemDirectoryHandle> {
        let scope: DedicatedWorkerGlobalScope = js_sys::global().unchecked_into();
        JsFuture::from(scope.navigator().storage().get_directory())
            .await
            .map_err(|error| js_err("reaching the OPFS root", &error))?
            .dyn_into()
            .map_err(|value| js_err("OPFS root was not a directory handle", &value))
    }

    /// Walk (creating) a `/`-separated path to its parent directory, returning
    /// that directory and the final component.
    ///
    /// Real nesting rather than flattening the key: a mirror of a tree has paths
    /// like `docs/guide.md`, and flattening them to `docs_guide.md` would let two
    /// distinct files collide onto one — silently serving one under the other's
    /// name, which is the class of bug this crate exists to prevent.
    async fn walk(path: &str) -> Result<(FileSystemDirectoryHandle, String)> {
        let mut dir = Self::root().await?;
        let mut parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
        let name = parts
            .pop()
            .context("an empty key names no file")?
            .to_owned();
        let options = FileSystemGetDirectoryOptions::new();
        options.set_create(true);
        for part in parts {
            dir = JsFuture::from(dir.get_directory_handle_with_options(part, &options))
                .await
                .map_err(|error| js_err(&format!("opening directory {part}"), &error))?
                .dyn_into()
                .map_err(|value| js_err("not a directory handle", &value))?;
        }
        Ok((dir, name))
    }

    /// A synchronous access handle for `path`, creating the file if absent.
    async fn handle(path: &str) -> Result<FileSystemSyncAccessHandle> {
        let (dir, name) = Self::walk(path).await?;
        let options = FileSystemGetFileOptions::new();
        options.set_create(true);
        let file: FileSystemFileHandle =
            JsFuture::from(dir.get_file_handle_with_options(&name, &options))
                .await
                .map_err(|error| js_err(&format!("opening {path}"), &error))?
                .dyn_into()
                .map_err(|value| js_err("not a file handle", &value))?;
        JsFuture::from(file.create_sync_access_handle())
            .await
            .map_err(|error| js_err(&format!("taking a sync handle on {path}"), &error))?
            .dyn_into()
            .map_err(|value| js_err("not a sync access handle", &value))
    }

    /// Read a whole OPFS file, or `None` when it does not exist.
    async fn read_all(path: &str) -> Result<Option<Vec<u8>>> {
        let Ok(handle) = Self::handle(path).await else {
            return Ok(None);
        };
        let size = handle
            .get_size()
            .map_err(|error| js_err(&format!("sizing {path}"), &error))?;
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "OPFS reports a size in f64; a file past usize::MAX cannot be held anyway"
        )]
        let mut bytes = vec![0u8; size as usize];
        if !bytes.is_empty() {
            handle
                .read_with_u8_array(&mut bytes)
                .map_err(|error| js_err(&format!("reading {path}"), &error))?;
        }
        handle.close();
        Ok(Some(bytes))
    }

    /// Replace an OPFS file's contents, flushing before returning.
    async fn write_all(path: &str, bytes: &[u8]) -> Result<()> {
        let handle = Self::handle(path).await?;
        handle
            .truncate_with_f64(0.0)
            .map_err(|error| js_err(&format!("truncating {path}"), &error))?;
        handle
            .write_with_u8_array(bytes)
            .map_err(|error| js_err(&format!("writing {path}"), &error))?;
        // Before the caller can record that these bytes exist.
        handle
            .flush()
            .map_err(|error| js_err(&format!("flushing {path}"), &error))?;
        handle.close();
        Ok(())
    }

    fn obao_path(&self, root: Root) -> String {
        format!("{}/{}", self.meta, sidecar::obao_name(root))
    }

    fn ranges_path(&self, root: Root) -> String {
        format!("{}/{}", self.meta, sidecar::ranges_name(root))
    }

    fn binds_path(&self) -> String {
        format!("{}/{}", self.meta, sidecar::BINDS)
    }

    async fn load_binds(&self) -> Vec<sidecar::Bind> {
        let Ok(Some(bytes)) = Self::read_all(&self.binds_path()).await else {
            return Vec::new();
        };
        sidecar::parse_binds(&String::from_utf8_lossy(&bytes))
    }

    async fn put_bind(&self, file: &FileId, root: Root) -> Result<()> {
        let mut binds = self.load_binds().await;
        sidecar::upsert(&mut binds, file, root);
        let text = sidecar::format_binds(&binds)?;
        Self::write_all(&self.binds_path(), text.as_bytes()).await
    }
}

impl BlobStore for OpfsStore {
    /// Records metadata; **does not copy the file**, exactly as the native
    /// backend does not.
    async fn insert_complete(&self, file: &FileId, bytes: &[u8]) -> Result<Root> {
        let (root, outboard) = crate::build_outboard(bytes);
        Self::write_all(&self.obao_path(root), &outboard).await?;
        Self::write_all(
            &self.ranges_path(root),
            sidecar::format_ranges(&extent_of(file.size)).as_bytes(),
        )
        .await?;
        self.put_bind(file, root).await?;
        Ok(root)
    }

    async fn bind(&self, file: &FileId) -> Result<Option<Root>> {
        Ok(sidecar::bound_root(&self.load_binds().await, file))
    }

    async fn set_bind(&self, file: &FileId, root: Root) -> Result<()> {
        self.put_bind(file, root).await
    }

    /// What this peer can serve.
    ///
    /// Read back from storage rather than cached in memory, deliberately: OPFS
    /// is evictable, and a range set held only in RAM would keep advertising
    /// bytes the browser has reclaimed.
    async fn present(&self, root: Root) -> Result<ChunkRanges> {
        let Some(bytes) = Self::read_all(&self.ranges_path(root)).await? else {
            return Ok(ChunkRanges::empty());
        };
        Ok(sidecar::parse_ranges(&String::from_utf8_lossy(&bytes)))
    }

    async fn outboard(&self, root: Root) -> Result<Option<Outboard>> {
        Self::read_all(&self.obao_path(root)).await
    }

    async fn put_outboard(
        &self,
        root: Root,
        outboard: Outboard,
        present: ChunkRanges,
    ) -> Result<()> {
        Self::write_all(&self.obao_path(root), &outboard).await?;
        Self::write_all(
            &self.ranges_path(root),
            sidecar::format_ranges(&present).as_bytes(),
        )
        .await
    }

    async fn read_ranges(&self, file: &FileId, ranges: &ChunkRanges) -> Result<Vec<u8>> {
        let (root, outboard, wanted) = crate::resolve_read(self, file, ranges).await?;

        let bytes = Self::read_all(&file.key)
            .await?
            .context("the file this store bound is gone")?;
        // Re-check after reading: a file can change mid-read, and encoding those
        // bytes under the old root emits a proof that fails on the far side for
        // no visible reason.
        if bytes.len() as u64 != file.size {
            bail!(
                "{} changed while it was being read: {} bytes, expected {}",
                file.key,
                bytes.len(),
                file.size
            );
        }
        encode_from_outboard(root, file.size, outboard, bytes, &wanted)
    }

    async fn write_verified(
        &self,
        file: &FileId,
        root: Root,
        encoded: &[u8],
        ranges: &ChunkRanges,
    ) -> Result<ChunkRanges> {
        // Verify before touching the destination, so a peer sending garbage
        // cannot leave a partly-written file behind.
        //
        // The outboard is carried in and handed back filled: the nodes a stream
        // proves are the only ones this store will ever have for those ranges,
        // since a file with holes cannot reproduce them. See `decode_into`.
        let mut target = Vec::new();
        let mut outboard = self.outboard(root).await?.unwrap_or_default();
        let verified = decode_into(root, file.size, encoded, ranges, &mut target, &mut outboard)?;

        let handle = Self::handle(&file.key).await?;
        #[expect(
            clippy::cast_precision_loss,
            reason = "OPFS takes an f64 offset; a file past 2^53 bytes is not reachable here"
        )]
        handle
            .truncate_with_f64(file.size as f64)
            .map_err(|error| js_err("sizing the destination", &error))?;

        // Sparse: only the chunks that verified, at their real offsets, so a
        // mirror filling a large file from several peers does not rewrite what
        // it already holds.
        for range in verified.iter() {
            let RangeSetRange::Range(range) = range else {
                continue;
            };
            let start = range.start.to_bytes();
            let end = range.end.to_bytes().min(file.size);
            let (Ok(from), Ok(to)) = (usize::try_from(start), usize::try_from(end)) else {
                bail!("range does not fit this target");
            };
            let options = FileSystemReadWriteOptions::new();
            #[expect(
                clippy::cast_precision_loss,
                reason = "OPFS takes an f64 offset; see above"
            )]
            options.set_at(start as f64);
            handle
                .write_with_u8_array_and_options(&target[from..to.min(target.len())], &options)
                .map_err(|error| js_err("writing a verified range", &error))?;
        }
        // Data durable *before* the range set that advertises it.
        handle
            .flush()
            .map_err(|error| js_err("flushing the destination", &error))?;
        handle.close();

        // Outboard before the range set, for the same reason the data goes
        // first: a range advertised without the nodes that prove it is a range
        // this store cannot actually answer for.
        Self::write_all(&self.obao_path(root), &outboard).await?;
        let held = self.present(root).await? | verified;
        Self::write_all(
            &self.ranges_path(root),
            sidecar::format_ranges(&held).as_bytes(),
        )
        .await?;
        self.put_bind(file, root).await?;
        Ok(held)
    }
}
