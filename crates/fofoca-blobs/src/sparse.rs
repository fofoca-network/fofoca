//! A file held as aligned blocks, without ever materializing the file.
//!
//! The backing shape for stores that cannot patch bytes in place. A browser is
//! the case that forced it: `IndexedDB` addresses records, not offsets, and OPFS
//! on the main thread charges a whole-file copy per opened writer. Both want the
//! same thing — keep the pieces, not the file.
//!
//! # Why this exists at all: the quadratic
//!
//! The obvious way to accept a range is to size a buffer to the whole file and
//! decode into it at absolute offsets, which is what [`crate::decode_into`]
//! does. That is fine when a file arrives in one call and ruinous when it
//! arrives in *n* pieces, because each piece pays for the whole file:
//!
//! ```text
//!   pieces        n / 64 KiB
//!   cost each     O(n)          allocate + zero the file
//!   total         O(n²)
//! ```
//!
//! At 1 `GiB` that is 16384 pieces each touching a gigabyte. Measured against
//! main-thread OPFS the same shape costs about nine hours.
//! `bao-tree` hands us the way out: `decode_ranges` writes through
//! [`WriteAt`], and `encode_ranges` reads through [`ReadAt`], so neither side
//! needs the file to exist contiguously. This type is that seam.
//!
//! # Holes are errors, not short reads
//!
//! [`ReadAt::read_at`] over a block this does not hold returns an error rather
//! than zeroes or a short count. A short read is indistinguishable from EOF to
//! the caller, and answering a range we do not hold with zeroes would produce a
//! proof that fails on the far side for no visible reason — the peer would look
//! hostile rather than incomplete. Refusing names the real problem.

use std::collections::BTreeMap;
use std::io;

use bao_tree::io::sync::{ReadAt, Size, WriteAt};
use bao_tree::{ChunkNum, ChunkRanges};

/// Bytes per chunk, fixed by BLAKE3. Byte offsets become chunk numbers through
/// this and nothing else, so it is named once.
const CHUNK_BYTES: u64 = 1024;

/// A file's bytes held as a sparse map of fixed-size blocks.
///
/// Block size is a parameter rather than [`crate::CHUNK_GROUP_BYTES`] so tests
/// can use a size small enough to reason about; real stores pass the constant.
#[derive(Debug, Clone)]
pub struct SparseBlocks {
    size: u64,
    block: u64,
    blocks: BTreeMap<u64, Vec<u8>>,
    /// Chunk ranges written through [`WriteAt`] since this was built.
    ///
    /// Recorded as it happens rather than re-derived afterwards, because the
    /// two answers differ in the case that matters: a peer that sends *less*
    /// than was asked for. What was written is what verified — `decode_ranges`
    /// only emits a leaf once its hash checks out — so this is the exact answer
    /// rather than a re-scan that has to guess at the difference between an
    /// absent chunk and a chunk of zeroes.
    written: ChunkRanges,
}

impl SparseBlocks {
    /// An empty file of `size` bytes, holding nothing.
    ///
    /// # Panics
    /// `block` is zero, which would address nothing.
    #[must_use]
    pub fn empty(size: u64, block: u64) -> Self {
        assert!(block > 0, "a zero block size addresses nothing");
        Self {
            size,
            block,
            blocks: BTreeMap::new(),
            written: ChunkRanges::empty(),
        }
    }

    /// A file of `size` bytes holding the given `(block index, bytes)` pairs.
    ///
    /// The read path: a store loads just the blocks covering what was asked
    /// for, and hands them here for the encoder to read through.
    pub fn from_blocks(
        size: u64,
        block: u64,
        blocks: impl IntoIterator<Item = (u64, Vec<u8>)>,
    ) -> Self {
        let mut sparse = Self::empty(size, block);
        sparse.blocks = blocks.into_iter().collect();
        sparse
    }

    /// Blocks held, by index, consuming the map.
    #[must_use]
    pub fn into_blocks(self) -> BTreeMap<u64, Vec<u8>> {
        self.blocks
    }

    /// Blocks held, by index.
    #[must_use]
    pub fn blocks(&self) -> &BTreeMap<u64, Vec<u8>> {
        &self.blocks
    }

    /// Chunk ranges written into this since it was built.
    #[must_use]
    pub fn written(&self) -> &ChunkRanges {
        &self.written
    }

    /// Which block indices cover `ranges` of a file of this size.
    ///
    /// What a store loads before serving: the encoder reads only the requested
    /// ranges, so only these blocks need to come off storage.
    #[must_use]
    pub fn blocks_covering(ranges: &ChunkRanges, size: u64, block: u64) -> Vec<u64> {
        use range_collections::range_set::RangeSetRange;

        let mut indices = Vec::new();
        let clamped = ranges.clone() & crate::extent_of(size);
        for range in clamped.iter() {
            let RangeSetRange::Range(range) = range else {
                continue;
            };
            let start = range.start.to_bytes();
            // `end` is exclusive in chunks; the last byte is one before it, and
            // clamping to `size` keeps a range that runs past the tail from
            // naming a block the file does not reach.
            let end = range.end.to_bytes().min(size);
            if end <= start {
                continue;
            }
            for index in (start / block)..=((end - 1) / block) {
                if indices.last() != Some(&index) {
                    indices.push(index);
                }
            }
        }
        indices.dedup();
        indices
    }

    /// Trim every block to the file's length, dropping any past the end.
    ///
    /// The final block of a file is usually short. A writer that filled a whole
    /// block would otherwise leave trailing bytes that belong to no file, and a
    /// later read would serve them.
    fn trim_tail(&mut self) {
        let block = self.block;
        self.blocks.retain(|index, _| index * block < self.size);
        if let Some((&index, bytes)) = self.blocks.iter_mut().next_back() {
            let start = index * block;
            let want = usize::try_from((self.size - start).min(block)).unwrap_or(usize::MAX);
            if bytes.len() > want {
                bytes.truncate(want);
            }
        }
    }
}

impl Size for SparseBlocks {
    fn size(&self) -> io::Result<Option<u64>> {
        Ok(Some(self.size))
    }
}

impl ReadAt for SparseBlocks {
    /// Read from `pos`, never crossing a block boundary in one call.
    ///
    /// Returning a short count is how `positioned_io::ReadAt` expects a
    /// segmented source to behave; `read_exact_at` loops. But a *hole* is an
    /// error rather than a short read — see the module docs.
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> io::Result<usize> {
        if pos >= self.size || buf.is_empty() {
            return Ok(0);
        }
        let index = pos / self.block;
        let Some(bytes) = self.blocks.get(&index) else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("block {index} is not held"),
            ));
        };
        let within = usize::try_from(pos - index * self.block).unwrap_or(usize::MAX);
        if within >= bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("block {index} is held but ends before offset {pos}"),
            ));
        }
        let available = &bytes[within..];
        let taken = available.len().min(buf.len());
        buf[..taken].copy_from_slice(&available[..taken]);
        Ok(taken)
    }
}

impl WriteAt for SparseBlocks {
    /// Write at `pos`, allocating only the blocks touched.
    ///
    /// Splits across block boundaries, so a caller never has to know the
    /// blocking. Blocks are zero-filled up to the written span; the tail is
    /// trimmed to the file's length by [`Self::trim_tail`] on flush.
    fn write_at(&mut self, pos: u64, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let index = pos / self.block;
        let within = usize::try_from(pos - index * self.block).unwrap_or(usize::MAX);
        let room = usize::try_from(self.block).unwrap_or(usize::MAX) - within;
        let taken = buf.len().min(room);

        let start = index * self.block;
        let capacity = usize::try_from(self.block.min(self.size.saturating_sub(start)))
            .unwrap_or(usize::MAX)
            .max(within + taken);
        let block = self
            .blocks
            .entry(index)
            .or_insert_with(|| Vec::with_capacity(capacity));
        if block.len() < within + taken {
            block.resize(within + taken, 0);
        }
        block[within..within + taken].copy_from_slice(&buf[..taken]);

        // Byte span to chunk span. Leaves arrive chunk-aligned, and the final
        // partial chunk still counts as one whole chunk, so round the end up.
        let end = pos + taken as u64;
        let first = ChunkNum(pos / CHUNK_BYTES);
        let last = ChunkNum(end.div_ceil(CHUNK_BYTES));
        if last > first {
            self.written |= ChunkRanges::from(first..last);
        }
        Ok(taken)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.trim_tail();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SparseBlocks;
    use bao_tree::io::sync::{ReadAt, Size, WriteAt};
    use bao_tree::{ChunkNum, ChunkRanges};

    #[test]
    fn a_write_lands_in_one_block_and_reads_back() {
        let mut sparse = SparseBlocks::empty(4096, 1024);
        sparse.write_all_at(1024, &[7u8; 1024]).expect("write");
        let mut buf = [0u8; 1024];
        sparse.read_exact_at(1024, &mut buf).expect("read");
        assert_eq!(buf, [7u8; 1024]);
        assert_eq!(sparse.blocks().keys().copied().collect::<Vec<_>>(), vec![1]);
    }

    /// The property the whole type exists for: touching one piece of a large
    /// file allocates one piece, not the file.
    #[test]
    fn writing_one_block_of_a_huge_file_allocates_one_block() {
        let mut sparse = SparseBlocks::empty(1 << 40, 65536);
        sparse
            .write_all_at(1 << 30, &vec![1u8; 65536])
            .expect("write");
        assert_eq!(sparse.blocks().len(), 1, "one block, not a terabyte");
        assert_eq!(sparse.size().expect("size"), Some(1 << 40));
    }

    #[test]
    fn a_write_spanning_blocks_is_split() {
        let mut sparse = SparseBlocks::empty(4096, 1024);
        sparse.write_all_at(512, &[3u8; 2048]).expect("write");
        assert_eq!(
            sparse.blocks().keys().copied().collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        let mut buf = [0u8; 2048];
        sparse.read_exact_at(512, &mut buf).expect("read");
        assert_eq!(buf, [3u8; 2048]);
    }

    /// A hole must not read as zeroes. Serving zeroes for a range we do not
    /// hold produces a proof that fails on the far side, which makes an
    /// incomplete peer look like a hostile one.
    #[test]
    fn reading_a_hole_is_an_error_not_zeroes() {
        let sparse = SparseBlocks::empty(4096, 1024);
        let mut buf = [0u8; 16];
        assert!(sparse.read_at(2048, &mut buf).is_err());
    }

    #[test]
    fn reading_past_the_end_is_eof_not_an_error() {
        let sparse = SparseBlocks::empty(4096, 1024);
        let mut buf = [0u8; 16];
        assert_eq!(sparse.read_at(4096, &mut buf).expect("eof"), 0);
    }

    #[test]
    fn written_ranges_track_what_landed() {
        let mut sparse = SparseBlocks::empty(8192, 1024);
        sparse.write_all_at(0, &[1u8; 2048]).expect("write");
        assert_eq!(
            *sparse.written(),
            ChunkRanges::from(ChunkNum(0)..ChunkNum(2))
        );
        sparse.write_all_at(4096, &[1u8; 1024]).expect("write");
        assert_eq!(
            *sparse.written(),
            ChunkRanges::from(ChunkNum(0)..ChunkNum(2))
                | ChunkRanges::from(ChunkNum(4)..ChunkNum(5))
        );
    }

    /// A final partial chunk is still a whole chunk as far as ranges go.
    #[test]
    fn a_short_tail_still_counts_as_a_chunk() {
        let mut sparse = SparseBlocks::empty(1500, 1024);
        sparse.write_all_at(1024, &[9u8; 476]).expect("write");
        assert_eq!(
            *sparse.written(),
            ChunkRanges::from(ChunkNum(1)..ChunkNum(2))
        );
    }

    /// A writer that filled a whole final block would otherwise leave bytes
    /// belonging to no file, and a later read would serve them.
    #[test]
    fn flush_trims_the_tail_to_the_files_length() {
        let mut sparse = SparseBlocks::empty(1500, 1024);
        sparse.write_all_at(1024, &[9u8; 1024]).expect("write");
        sparse.flush().expect("flush");
        assert_eq!(sparse.blocks()[&1].len(), 476, "1500 - 1024");
    }

    #[test]
    fn blocks_covering_names_only_the_blocks_a_range_touches() {
        // 1 MiB file, 64 KiB blocks: chunk 64 starts at byte 65536, block 1.
        let ranges = ChunkRanges::from(ChunkNum(64)..ChunkNum(128));
        assert_eq!(
            SparseBlocks::blocks_covering(&ranges, 1 << 20, 65536),
            vec![1]
        );
    }

    /// `ChunkRanges::all()` is unbounded, so a store asking "which blocks is
    /// that" must get the file's blocks rather than an infinite list.
    #[test]
    fn blocks_covering_clamps_an_unbounded_request() {
        let covering = SparseBlocks::blocks_covering(&ChunkRanges::all(), 1 << 20, 65536);
        assert_eq!(covering, (0..16).collect::<Vec<_>>());
    }

    #[test]
    fn blocks_covering_an_empty_file_names_nothing() {
        assert!(SparseBlocks::blocks_covering(&ChunkRanges::all(), 0, 65536).is_empty());
    }
}
