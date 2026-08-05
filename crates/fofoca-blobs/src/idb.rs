//! The browser store that needs no Worker: `IndexedDB`, addressed in blocks.
//!
//! [`crate::opfs`] is faster and holds real files, but it is Worker-only —
//! `FileSystemSyncAccessHandle` does not exist in a window scope, and the
//! main-thread alternative charges a whole-file copy per opened writer, which
//! turns filling a file into O(n²). `IndexedDB` is reachable from
//! *any* scope and addresses records rather than offsets, so a 64 `KiB` block
//! lands in one record however large the file is.
//!
//! Measured in Safari: **64 `MiB`/s write, 362
//! `MiB`/s read**, random access, linear in both directions. That is roughly
//! 24× slower than a Worker's sync handles and still 5–50× faster than a
//! realistic `WebRTC` data channel, so on the seeding path it is not the
//! bottleneck. It is the local operations — hashing a whole file, exporting one
//! — where the Worker would win.
//!
//! # Shape
//!
//! Two object stores, and the split is the crate's usual one:
//!
//! - `meta` — the same sidecar text [`crate::fs`] and [`crate::opfs`] write, one
//!   record per file name (`<root>.obao`, `<root>.ranges`, `binds.tsv`). Sharing
//!   the format rather than inventing a third means a bug in it is one bug.
//! - `blocks` — the data, keyed `<file key>\0<block index>`.
//!
//! # What is different here, and why
//!
//! Every other backend leaves the bytes where the caller put them and records
//! only metadata. This one **holds the data**, because in a browser there is no
//! "where the caller put them" — there is no path to hand back. That is the one
//! place this store departs from the crate's central inversion, and it departs
//! because the platform leaves no alternative, not because it is convenient.
//!
//! # Eviction
//!
//! `IndexedDB` is evictable under storage pressure like OPFS. A peer that
//! advertises ranges the browser later reclaims looks to everyone else like a
//! peer that lies, so [`BlobStore::present`] reads the range set back from
//! storage rather than caching it in memory.

use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow, bail};
use bao_tree::ChunkRanges;
use js_sys::Uint8Array;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{IdbDatabase, IdbFactory, IdbRequest, IdbTransaction, IdbTransactionMode};

use crate::sparse::SparseBlocks;
use crate::{
    BlobStore, CHUNK_GROUP_BYTES, FileId, Outboard, Root, decode_sparse, encode_from_outboard,
    extent_of,
};

/// Object store holding sidecar text, one record per name.
const META: &str = "meta";
/// Object store holding file data, one record per block.
const BLOCKS: &str = "blocks";

/// JS errors are not `std::error::Error`, so they cannot ride `?` into
/// `anyhow`. Rendering them at the boundary keeps every signature in the crate
/// identical across backends.
fn js_err(context: &str, error: &JsValue) -> anyhow::Error {
    anyhow!("{context}: {error:?}")
}

/// The block key for `index` of the content named by `root`.
///
/// **Keyed by root, not by file key, and that is load-bearing.** The range set
/// this store advertises is keyed by root — `present(root)` is the trait's
/// shape — so if the blocks were keyed by file, two files with identical
/// content would share a range set while holding separate bytes. Completing one
/// would make the other advertise ranges whose blocks do not exist, and the
/// first read would fail with a missing block on a file the store had just
/// claimed was complete.
///
/// Keying both by root makes them agree by construction, and identical files
/// deduplicate as a side effect. NUL separates because it cannot appear in the
/// hex of a root.
fn block_key(root: Root, index: u64) -> String {
    format!("{}\u{0}{index:016x}", crate::sidecar::hex(root))
}

/// The success/error closure pair kept alive for the duration of one await.
type Handlers = Option<(Closure<dyn FnMut()>, Closure<dyn FnMut()>)>;

/// Await an `IndexedDB` request.
///
/// `IndexedDB` predates promises, so every call is an event pair. The closures
/// are dropped after the await rather than `forget`-ed: this runs once per
/// block, and leaking a pair per block would be a leak proportional to the data
/// transferred.
async fn done(request: &IdbRequest) -> Result<JsValue> {
    let mut keep: Handlers = None;
    let promise = js_sys::Promise::new(&mut |resolve, reject| {
        let ok_request = request.clone();
        let ok = Closure::<dyn FnMut()>::new(move || {
            let value = ok_request.result().unwrap_or(JsValue::UNDEFINED);
            let _ = resolve.call1(&JsValue::NULL, &value);
        });
        let bad_request = request.clone();
        let bad = Closure::<dyn FnMut()>::new(move || {
            let error = bad_request
                .error()
                .ok()
                .flatten()
                .map_or_else(|| JsValue::from_str("request failed"), JsValue::from);
            let _ = reject.call1(&JsValue::NULL, &error);
        });
        request.set_onsuccess(Some(ok.as_ref().unchecked_ref()));
        request.set_onerror(Some(bad.as_ref().unchecked_ref()));
        keep = Some((ok, bad));
    });
    let outcome = JsFuture::from(promise).await;
    drop(keep);
    outcome.map_err(|error| js_err("an IndexedDB request", &error))
}

/// Await a transaction's completion.
///
/// Distinct from awaiting the last request in it: a request can succeed and the
/// transaction still abort, and only `oncomplete` means the writes are durable.
/// Recording ranges before that would advertise bytes that never landed.
async fn committed(tx: &IdbTransaction) -> Result<()> {
    let mut keep: Handlers = None;
    let promise = js_sys::Promise::new(&mut |resolve, reject| {
        let ok = Closure::<dyn FnMut()>::new(move || {
            let _ = resolve.call1(&JsValue::NULL, &JsValue::UNDEFINED);
        });
        let bad = Closure::<dyn FnMut()>::new(move || {
            let _ = reject.call1(&JsValue::NULL, &JsValue::from_str("transaction aborted"));
        });
        tx.set_oncomplete(Some(ok.as_ref().unchecked_ref()));
        tx.set_onabort(Some(bad.as_ref().unchecked_ref()));
        keep = Some((ok, bad));
    });
    let outcome = JsFuture::from(promise).await;
    drop(keep);
    outcome
        .map(|_| ())
        .map_err(|error| js_err("an IndexedDB transaction", &error))
}

/// A store backed by `IndexedDB`. Reachable from any scope; not `Send`.
#[derive(Debug)]
pub struct IdbStore {
    db: IdbDatabase,
}

impl IdbStore {
    /// Open (creating) the database named `name`.
    ///
    /// # Errors
    /// `IndexedDB` is unreachable — a browser in private mode may refuse it —
    /// or the schema upgrade fails.
    pub async fn open(name: &str) -> Result<Self> {
        // Read off the global rather than through `Window`, so this works
        // unchanged if the store later moves into a Worker. That migration is
        // the whole point of keeping the backend behind `BlobStore`.
        let factory: IdbFactory = js_sys::Reflect::get(&js_sys::global(), &"indexedDB".into())
            .map_err(|error| js_err("reaching indexedDB", &error))?
            .dyn_into()
            .map_err(|value| js_err("indexedDB was not a factory", &value))?;

        let request = factory
            .open_with_u32(name, 1)
            .map_err(|error| js_err(&format!("opening database {name}"), &error))?;

        // The schema is created inside `onupgradeneeded`, which fires before
        // `onsuccess`. Registering it after the open request would be too late.
        let upgrade_target = request.clone();
        let upgrade = Closure::<dyn FnMut()>::new(move || {
            let Ok(value) = upgrade_target.result() else {
                return;
            };
            let Ok(db) = value.dyn_into::<IdbDatabase>() else {
                return;
            };
            for store in [META, BLOCKS] {
                // Out-of-line keys: every key is supplied at `put` time rather
                // than read out of the value, because the values are raw bytes
                // with nowhere to put one.
                let _ = db.create_object_store(store);
            }
        });
        request.set_onupgradeneeded(Some(upgrade.as_ref().unchecked_ref()));

        let value = done(request.as_ref()).await?;
        drop(upgrade);
        let db: IdbDatabase = value
            .dyn_into()
            .map_err(|value| js_err("open did not yield a database", &value))?;
        Ok(Self { db })
    }

    fn store(&self, name: &str, mode: IdbTransactionMode) -> Result<(IdbTransaction, JsValue)> {
        let tx = self
            .db
            .transaction_with_str_and_mode(name, mode)
            .map_err(|error| js_err(&format!("opening a transaction on {name}"), &error))?;
        let store = tx
            .object_store(name)
            .map_err(|error| js_err(&format!("reaching object store {name}"), &error))?;
        Ok((tx, store.into()))
    }

    /// One record, or `None` when absent.
    async fn get(&self, name: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let (_tx, store) = self.store(name, IdbTransactionMode::Readonly)?;
        let store: web_sys::IdbObjectStore = store.unchecked_into();
        let request = store
            .get(&JsValue::from_str(key))
            .map_err(|error| js_err(&format!("reading {key}"), &error))?;
        let value = done(&request).await?;
        if value.is_undefined() || value.is_null() {
            return Ok(None);
        }
        Ok(Some(Uint8Array::new(&value).to_vec()))
    }

    /// Write records, all in one transaction.
    ///
    /// One transaction rather than one each: `IndexedDB`'s per-transaction
    /// overhead dominates a 64 `KiB` put, and batching is most of the difference
    /// between the measured 64 `MiB`/s and something far worse.
    async fn put_many(&self, name: &str, entries: &[(String, Vec<u8>)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let (tx, store) = self.store(name, IdbTransactionMode::Readwrite)?;
        let store: web_sys::IdbObjectStore = store.unchecked_into();
        for (key, bytes) in entries {
            let value = Uint8Array::from(bytes.as_slice());
            store
                .put_with_key(value.as_ref(), &JsValue::from_str(key))
                .map_err(|error| js_err(&format!("writing {key}"), &error))?;
        }
        committed(&tx).await
    }

    fn obao_key(root: Root) -> String {
        format!("{}.obao", crate::sidecar::hex(root))
    }

    fn ranges_key(root: Root) -> String {
        format!("{}.ranges", crate::sidecar::hex(root))
    }

    async fn load_binds(&self) -> Vec<(String, u64, i64, Root)> {
        let Ok(Some(bytes)) = self.get(META, "binds.tsv").await else {
            return Vec::new();
        };
        crate::sidecar::parse_binds(&String::from_utf8_lossy(&bytes))
    }

    async fn put_bind(&self, file: &FileId, root: Root) -> Result<()> {
        let mut binds = self.load_binds().await;
        binds.retain(|(key, ..)| key != &file.key);
        binds.push((file.key.clone(), file.size, file.mtime, root));
        let text = crate::sidecar::format_binds(&binds)?;
        self.put_many(META, &[("binds.tsv".to_owned(), text.into_bytes())])
            .await
    }

    /// Load the blocks at `indices` of `root` into a sparse view.
    async fn load_blocks(&self, root: Root, size: u64, indices: &[u64]) -> Result<SparseBlocks> {
        let mut blocks = BTreeMap::new();
        for &index in indices {
            if let Some(bytes) = self.get(BLOCKS, &block_key(root, index)).await? {
                blocks.insert(index, bytes);
            }
        }
        Ok(SparseBlocks::from_blocks(size, CHUNK_GROUP_BYTES, blocks))
    }

    async fn store_blocks(&self, root: Root, blocks: &BTreeMap<u64, Vec<u8>>) -> Result<()> {
        let entries: Vec<(String, Vec<u8>)> = blocks
            .iter()
            .map(|(index, bytes)| (block_key(root, *index), bytes.clone()))
            .collect();
        self.put_many(BLOCKS, &entries).await
    }
}

impl BlobStore for IdbStore {
    /// Unlike every other backend, this **does** copy the bytes in — see the
    /// module docs. A browser has no path to hand back later.
    async fn insert_complete(&self, file: &FileId, bytes: &[u8]) -> Result<Root> {
        if bytes.len() as u64 != file.size {
            bail!(
                "{} is {} bytes, but was described as {}",
                file.key,
                bytes.len(),
                file.size
            );
        }
        let (root, outboard) = crate::build_outboard(bytes);

        let mut blocks = BTreeMap::new();
        let block = usize::try_from(CHUNK_GROUP_BYTES).context("block size fits usize")?;
        for (index, piece) in bytes.chunks(block).enumerate() {
            blocks.insert(index as u64, piece.to_vec());
        }
        self.store_blocks(root, &blocks).await?;

        // Metadata last: data durable before anything advertises it.
        self.put_many(
            META,
            &[
                (Self::obao_key(root), outboard),
                (
                    Self::ranges_key(root),
                    crate::sidecar::format_ranges(&extent_of(file.size)).into_bytes(),
                ),
            ],
        )
        .await?;
        self.put_bind(file, root).await?;
        Ok(root)
    }

    async fn bind(&self, file: &FileId) -> Result<Option<Root>> {
        let Some((_, size, mtime, root)) = self
            .load_binds()
            .await
            .into_iter()
            .find(|(key, ..)| key == &file.key)
        else {
            return Ok(None);
        };
        if size != file.size || mtime != file.mtime {
            return Ok(None);
        }
        Ok(Some(root))
    }

    async fn set_bind(&self, file: &FileId, root: Root) -> Result<()> {
        self.put_bind(file, root).await
    }

    /// Read back from storage rather than cached: `IndexedDB` is evictable, and
    /// a range set held only in RAM would keep advertising reclaimed bytes.
    async fn present(&self, root: Root) -> Result<ChunkRanges> {
        let Some(bytes) = self.get(META, &Self::ranges_key(root)).await? else {
            return Ok(ChunkRanges::empty());
        };
        Ok(crate::sidecar::parse_ranges(&String::from_utf8_lossy(
            &bytes,
        )))
    }

    async fn outboard(&self, root: Root) -> Result<Option<Outboard>> {
        self.get(META, &Self::obao_key(root)).await
    }

    async fn put_outboard(
        &self,
        root: Root,
        outboard: Outboard,
        present: ChunkRanges,
    ) -> Result<()> {
        self.put_many(
            META,
            &[
                (Self::obao_key(root), outboard),
                (
                    Self::ranges_key(root),
                    crate::sidecar::format_ranges(&present).into_bytes(),
                ),
            ],
        )
        .await
    }

    /// Serve `ranges`, reading only the blocks that cover them.
    ///
    /// The linear path: [`encode_from_outboard`] reads through the sparse view,
    /// so answering for one 64 `KiB` range of a gigabyte touches one record.
    async fn read_ranges(&self, file: &FileId, ranges: &ChunkRanges) -> Result<Vec<u8>> {
        let root = self
            .bind(file)
            .await?
            .context("this file is unbound: it was never hashed, or it changed since it was")?;

        let held = self.present(root).await?;
        let wanted = ranges.clone() & extent_of(file.size);
        if !wanted.is_subset(&held) {
            bail!("requested ranges are not all held");
        }
        let outboard = self
            .outboard(root)
            .await?
            .context("bound to a root whose outboard is gone")?;

        let indices = SparseBlocks::blocks_covering(&wanted, file.size, CHUNK_GROUP_BYTES);
        let data = self.load_blocks(root, file.size, &indices).await?;
        encode_from_outboard(root, file.size, outboard, data, &wanted)
    }

    async fn write_verified(
        &self,
        file: &FileId,
        root: Root,
        encoded: &[u8],
        ranges: &ChunkRanges,
    ) -> Result<ChunkRanges> {
        // Preload the blocks this write lands in. A range need not cover a whole
        // block, and decoding into an empty view would replace a block with just
        // the part that arrived — losing bytes already held rather than adding
        // to them.
        let touched = SparseBlocks::blocks_covering(ranges, file.size, CHUNK_GROUP_BYTES);
        let mut target = self.load_blocks(root, file.size, &touched).await?;

        // Carried across calls, because the nodes a stream proves are the only
        // ones this store will ever have — unlike the backends that re-read the
        // whole file and rebuild. Dropping them would leave a store holding
        // bytes it can never serve. See `decode_sparse`.
        let mut outboard = self.outboard(root).await?.unwrap_or_default();

        // Verify before anything is written: a peer sending garbage must cost
        // its own bandwidth and nothing else.
        let verified = decode_sparse(root, file.size, encoded, ranges, &mut target, &mut outboard)?;

        self.store_blocks(root, target.blocks()).await?;

        let held = self.present(root).await? | verified;
        // Outboard and range set together: a range advertised without the nodes
        // that prove it is a range this store cannot actually answer for.
        self.put_many(
            META,
            &[
                (Self::obao_key(root), outboard),
                (
                    Self::ranges_key(root),
                    crate::sidecar::format_ranges(&held).into_bytes(),
                ),
            ],
        )
        .await?;
        self.put_bind(file, root).await?;
        Ok(held)
    }
}
