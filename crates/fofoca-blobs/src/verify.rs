//! The bao layer: build an outboard, encode a range, verify a range.
//!
//! Deliberately thin. `bao-tree` does the work; this exists so the rest of the
//! crate names one shape instead of four, and so the block size cannot be
//! passed inconsistently at two call sites — which would produce outboards that
//! silently fail to verify against each other.

use anyhow::{Context, Result};
use bao_tree::io::outboard::PreOrderMemOutboard;
use bao_tree::io::sync::{
    ReadAt, Size, WriteAt, decode_ranges, encode_ranges_validated, valid_ranges,
};
use bao_tree::{BaoTree, ChunkRanges};

use crate::BLOCK_SIZE;
use crate::sparse::SparseBlocks;

/// A BLAKE3 root: the name of some content, and what every range verifies
/// against.
pub type Root = [u8; 32];

/// A pre-order outboard — the hash tree that rides *beside* the data rather
/// than inside it. The reason a store can verify bytes it does not own.
pub type Outboard = Vec<u8>;

/// Hash `data`, returning its root and outboard.
///
/// The one-time cost a caller pays before a file can be served to a third
/// party. Measured at ~2.2 GiB/s natively and ~2.1 GiB/s in wasm with SIMD, so
/// roughly half a second per gigabyte — cheap enough to do lazily, on first
/// interest, which is the point.
#[must_use]
pub fn build_outboard(data: &[u8]) -> (Root, Outboard) {
    let outboard = PreOrderMemOutboard::create(data, BLOCK_SIZE);
    (*outboard.root.as_bytes(), outboard.data)
}

/// Encode `ranges` of `data` for a peer, with proofs.
///
/// # Errors
/// The ranges do not fit the data.
pub fn encode_ranges(data: &[u8], ranges: &ChunkRanges) -> Result<Vec<u8>> {
    let outboard = PreOrderMemOutboard::create(data, BLOCK_SIZE);
    let mut encoded = Vec::new();
    encode_ranges_validated(data, &outboard, ranges, &mut encoded)
        .context("encoding verified ranges")?;
    Ok(encoded)
}

/// Rebuild an outboard structure around stored outboard `bytes`.
///
/// The tree geometry is derived from `size` and [`BLOCK_SIZE`] rather than
/// stored, so a mismatched block size shows up as a verification failure
/// instead of silently reading the wrong node.
fn outboard_for(root: Root, size: u64, bytes: Vec<u8>) -> PreOrderMemOutboard {
    PreOrderMemOutboard {
        root: blake3::Hash::from(root),
        tree: BaoTree::new(size, BLOCK_SIZE),
        data: bytes,
    }
}

/// Encode `ranges` using an outboard already stored, reading only those ranges.
///
/// The difference from [`encode_ranges`] is asymptotic, not stylistic.
/// `encode_ranges` rebuilds the outboard, so it must read every byte of the
/// file to serve any part of it — turning "seed one 64 `KiB` range" into a
/// whole-file read, and a peer that walks a file range by range into O(n²).
/// This reads through [`ReadAt`], so a store backed by blocks touches only the
/// blocks asked for.
///
/// The trade is that this **trusts local storage**. `encode_ranges` re-hashes
/// as it goes and would catch bytes that rotted underneath us; this does not,
/// so corruption here surfaces as a proof the *receiver* rejects. That is the
/// right place for the check to live anyway — the receiver verifies against the
/// root regardless of what any peer claims — and it is the only version that
/// does not read the whole file to answer for a piece of it.
///
/// # Errors
/// The data source cannot answer for `ranges`, or the outboard does not match.
pub fn encode_from_outboard<D: ReadAt + Size>(
    root: Root,
    size: u64,
    outboard: Vec<u8>,
    data: D,
    ranges: &ChunkRanges,
) -> Result<Vec<u8>> {
    let outboard = outboard_for(root, size, outboard);
    let mut encoded = Vec::new();
    bao_tree::io::sync::encode_ranges(data, &outboard, ranges, &mut encoded)
        .context("encoding verified ranges from a stored outboard")?;
    Ok(encoded)
}

/// Verify `encoded` against `root` into sparse blocks, without materializing
/// the file.
///
/// The counterpart to [`encode_from_outboard`] on the write side, and the
/// reason [`crate::sparse`] exists: [`decode_into`] sizes its target to the
/// whole file on every call, so accepting a file in *n* pieces costs O(n²).
/// `decode_ranges` writes through [`WriteAt`], so the pieces can go straight
/// into the blocks they belong to.
///
/// Returns the ranges that verified, taken from what was actually written
/// rather than from what was requested — a peer that sends less than it was
/// asked for must not leave the store advertising the difference.
///
/// `outboard` is grown and filled in place. **It must be carried across calls
/// and persisted**, because a verified stream is the only place its nodes come
/// from: the stream proves its own hash path, and a store that throws those
/// nodes away can never serve the range back. Backends that rebuild the
/// outboard from the whole file on every read do not need this — a store that
/// reads only the blocks it was asked for has nothing to rebuild from.
///
/// # Errors
/// The stream does not verify against `root`, or is malformed. On failure
/// `target` may hold partial blocks; callers discard it rather than commit it.
pub fn decode_sparse(
    root: Root,
    size: u64,
    encoded: &[u8],
    ranges: &ChunkRanges,
    target: &mut SparseBlocks,
    outboard: &mut Outboard,
) -> Result<ChunkRanges> {
    let want = usize::try_from(BaoTree::new(size, BLOCK_SIZE).outboard_size())
        .context("outboard too large")?;
    if outboard.len() < want {
        outboard.resize(want, 0);
    }
    let mut outboard_mem = outboard_for(root, size, std::mem::take(outboard));
    let outcome = decode_ranges(
        std::io::Cursor::new(encoded),
        ranges,
        &mut *target,
        &mut outboard_mem,
    );
    // Hand the buffer back either way, so a caller that retries does not start
    // from an empty outboard and lose the nodes an earlier call proved.
    *outboard = outboard_mem.data;
    outcome.context("verifying ranges against the root")?;
    target.flush().context("closing out the written blocks")?;
    Ok(target.written().clone())
}

/// Verify `encoded` against `root` and write what it proves into `target`.
///
/// Returns the chunk ranges that verified. **A tampered byte fails here**, which
/// is what lets a consumer accept bytes from a peer it has no reason to trust:
/// the worst a hostile peer can do is waste its own bandwidth.
///
/// `outboard` is grown and filled in place, exactly as in [`decode_sparse`].
/// **A store that will serve these bytes onward must persist it**: the stream
/// proves its own hash path, and those nodes exist nowhere else. Rebuilding the
/// tree from a partly-filled file instead hashes the holes too, yielding a root
/// no peer shares and proofs every receiver rejects — the bytes look right
/// locally and fail one hop away. It is an out-parameter rather than a return
/// value so that throwing it away has to be written down.
///
/// A caller that is the final consumer of these bytes has nothing to serve and
/// can discard it.
///
/// # Errors
/// The stream does not verify against `root`, or is malformed.
pub fn decode_into(
    root: Root,
    size: u64,
    encoded: &[u8],
    ranges: &ChunkRanges,
    target: &mut Vec<u8>,
    outboard: &mut Outboard,
) -> Result<ChunkRanges> {
    let tree = BaoTree::new(size, BLOCK_SIZE);
    let want = usize::try_from(tree.outboard_size()).context("outboard too large")?;
    if outboard.len() < want {
        outboard.resize(want, 0);
    }
    let mut outboard_mem = outboard_for(root, size, std::mem::take(outboard));

    // Size the target to the *whole* file before decoding, even for a window in
    // the middle of it. `decode_ranges` writes at absolute offsets, so a target
    // grown only as far as the window ends leaves the scan below reading past
    // its end — which surfaces as "failed to fill whole buffer" and looks like a
    // verification failure rather than a too-small buffer.
    target.resize(
        usize::try_from(size).context("file too large for this target")?,
        0,
    );
    let outcome = decode_ranges(
        std::io::Cursor::new(encoded),
        ranges,
        &mut *target,
        &mut outboard_mem,
    );

    // Scan only where bytes were expected. Scanning everything would ask about
    // chunks this call never touched, whose zeroes are not a verification
    // failure — they are simply absent.
    //
    // And scan the *answer*, not the request: the two differ when a peer sends
    // part of what was asked for, and believing the request is how a store comes
    // to advertise bytes it does not hold.
    let scan = ranges.clone() & crate::extent_of(size);
    let held = outcome
        .context("verifying ranges against the root")
        .and_then(|()| {
            let mut held = ChunkRanges::empty();
            for range in valid_ranges(&outboard_mem, &target[..], &scan) {
                held |= ChunkRanges::from(range.context("scanning verified ranges")?);
            }
            Ok(held)
        });

    // Hand the buffer back either way, so a caller that retries does not start
    // from an empty outboard and lose the nodes an earlier call proved.
    *outboard = outboard_mem.data;
    held
}

#[cfg(test)]
mod tests {
    use super::{build_outboard, decode_into, decode_sparse, encode_from_outboard, encode_ranges};
    use crate::CHUNK_GROUP_BYTES;
    use crate::sparse::SparseBlocks;
    use bao_tree::{ChunkNum, ChunkRanges};

    fn data(len: usize) -> Vec<u8> {
        (0..len)
            .map(|index| u8::try_from(index % 251).expect("bounded by 251"))
            .collect()
    }

    #[test]
    fn a_full_round_trip_verifies() {
        let bytes = data(1 << 20);
        let (root, _) = build_outboard(&bytes);
        let all = ChunkRanges::all();
        let encoded = encode_ranges(&bytes, &all).expect("encode");

        let mut target = Vec::new();
        let held = decode_into(
            root,
            bytes.len() as u64,
            &encoded,
            &all,
            &mut target,
            &mut Vec::new(),
        )
        .expect("decode");
        assert_eq!(target, bytes);
        assert!(!held.is_empty());
    }

    /// The property partial seeding rests on: a peer holding part of a file can
    /// serve that part, provably, against the same root as the whole.
    #[test]
    fn a_partial_range_verifies_against_the_whole_files_root() {
        let bytes = data(1 << 20);
        let (root, _) = build_outboard(&bytes);
        // One 64 KiB chunk group is 64 chunks of 1 KiB.
        let window = ChunkRanges::from(ChunkNum(64)..ChunkNum(128));
        let encoded = encode_ranges(&bytes, &window).expect("encode");

        assert!(
            encoded.len() < bytes.len() / 4,
            "a partial encode must be far smaller than the file: {} of {}",
            encoded.len(),
            bytes.len()
        );

        let mut target = Vec::new();
        let held = decode_into(
            root,
            bytes.len() as u64,
            &encoded,
            &window,
            &mut target,
            &mut Vec::new(),
        )
        .expect("decode");
        assert!(!held.is_empty());
        assert_eq!(
            &target[64 * 1024..128 * 1024],
            &bytes[64 * 1024..128 * 1024]
        );
    }

    /// **The claim that makes a block store viable: serving a piece reads a
    /// piece.**
    ///
    /// Only the blocks covering the request are present — every other block is
    /// a hole, and `SparseBlocks` errors on a hole rather than reading zeroes.
    /// So this passing means the encoder genuinely never touched them. With
    /// `encode_ranges` instead, which rebuilds the outboard from the whole
    /// file, it could not.
    #[test]
    fn a_range_is_served_from_only_the_blocks_that_cover_it() {
        let bytes = data(1 << 20);
        let size = bytes.len() as u64;
        let (root, outboard) = build_outboard(&bytes);
        let window = ChunkRanges::from(ChunkNum(64)..ChunkNum(128));

        let covering = SparseBlocks::blocks_covering(&window, size, CHUNK_GROUP_BYTES);
        assert_eq!(covering.len(), 1, "one 64 KiB block of a 1 MiB file");
        let held = SparseBlocks::from_blocks(
            size,
            CHUNK_GROUP_BYTES,
            covering.iter().map(|index| {
                let start = usize::try_from(index * CHUNK_GROUP_BYTES).expect("fits");
                let end =
                    (start + usize::try_from(CHUNK_GROUP_BYTES).expect("fits")).min(bytes.len());
                (*index, bytes[start..end].to_vec())
            }),
        );

        let encoded = encode_from_outboard(root, size, outboard, held, &window).expect("encode");

        // And it verifies against the same root as the whole file.
        let mut target = Vec::new();
        decode_into(root, size, &encoded, &window, &mut target, &mut Vec::new()).expect("decode");
        assert_eq!(&target[65_536..131_072], &bytes[65_536..131_072]);
    }

    /// **The quadratic, closed.** A file arriving in pieces must cost its own
    /// size in total, not its size *per piece*.
    ///
    /// `decode_into` would size a buffer to the whole file on each of these
    /// calls. This asserts the sparse path allocates only what it was handed:
    /// after k pieces the store holds exactly k blocks, whatever the file's
    /// size.
    #[test]
    fn a_file_arriving_in_pieces_allocates_only_the_pieces() {
        let bytes = data(1 << 20);
        let size = bytes.len() as u64;
        let (root, _) = build_outboard(&bytes);

        let mut sparse = SparseBlocks::empty(size, CHUNK_GROUP_BYTES);
        let mut outboard = Vec::new();
        let mut all = ChunkRanges::empty();
        // 1 MiB in 64 KiB pieces, deliberately out of order: pieces come back
        // from several peers at once, and nothing may depend on their order.
        for step in [0u64, 8, 4, 12, 2, 10, 6, 14, 1, 9, 5, 13, 3, 11, 7, 15] {
            let window = ChunkRanges::from(ChunkNum(step * 64)..ChunkNum(step * 64 + 64));
            let encoded = encode_ranges(&bytes, &window).expect("encode");
            let held = decode_sparse(root, size, &encoded, &window, &mut sparse, &mut outboard)
                .expect("decode");
            all |= window;
            assert_eq!(held, all, "held must track exactly what has landed");
        }
        assert_eq!(sparse.blocks().len(), 16, "one block per piece, no more");

        // And the reassembled file is the original, byte for byte.
        let mut rebuilt = Vec::new();
        for index in 0..16u64 {
            rebuilt.extend_from_slice(&sparse.blocks()[&index]);
        }
        assert_eq!(rebuilt, bytes);
    }

    /// A peer that sends less than it was asked for must leave the store
    /// advertising what arrived, not what was requested. Believing the request
    /// is how a store comes to claim bytes it does not hold.
    #[test]
    fn a_short_answer_records_only_what_arrived() {
        let bytes = data(1 << 20);
        let size = bytes.len() as u64;
        let (root, _) = build_outboard(&bytes);

        let asked = ChunkRanges::from(ChunkNum(0)..ChunkNum(128));
        let sent = ChunkRanges::from(ChunkNum(0)..ChunkNum(64));
        let encoded = encode_ranges(&bytes, &sent).expect("encode");

        let mut sparse = SparseBlocks::empty(size, CHUNK_GROUP_BYTES);
        let mut outboard = Vec::new();
        let held =
            decode_sparse(root, size, &encoded, &sent, &mut sparse, &mut outboard).expect("decode");
        assert_eq!(held, sent);
        assert!(
            !asked.is_subset(&held),
            "the store must not claim the half it never received"
        );
    }

    /// The trust boundary, restated for the sparse path: `encode_from_outboard`
    /// does not re-hash, so a corrupted local block produces a proof the
    /// *receiver* rejects. The bytes must never be accepted as good.
    #[test]
    fn a_corrupted_local_block_fails_on_the_receiver() {
        let bytes = data(1 << 20);
        let size = bytes.len() as u64;
        let (root, outboard) = build_outboard(&bytes);
        let window = ChunkRanges::from(ChunkNum(0)..ChunkNum(64));

        let mut block = bytes[..65_536].to_vec();
        block[17] ^= 0xff;
        let rotted = SparseBlocks::from_blocks(size, CHUNK_GROUP_BYTES, [(0u64, block)]);

        let encoded =
            encode_from_outboard(root, size, outboard, rotted, &window).expect("encodes happily");
        let mut target = Vec::new();
        assert!(
            decode_into(root, size, &encoded, &window, &mut target, &mut Vec::new()).is_err(),
            "the receiver must reject bytes that do not match the root"
        );
    }

    /// The whole trust argument in one test. Without this a peer could serve
    /// anything and be believed.
    #[test]
    fn a_tampered_byte_is_rejected() {
        let bytes = data(1 << 20);
        let (root, _) = build_outboard(&bytes);
        let all = ChunkRanges::all();
        let mut encoded = encode_ranges(&bytes, &all).expect("encode");
        let last = encoded.len() - 1;
        encoded[last] ^= 0x01;

        let mut target = Vec::new();
        assert!(
            decode_into(
                root,
                bytes.len() as u64,
                &encoded,
                &all,
                &mut target,
                &mut Vec::new()
            )
            .is_err(),
            "a flipped bit must not verify"
        );
    }

    /// A root from *different* content must not accept these bytes, even though
    /// they are internally consistent.
    #[test]
    fn bytes_do_not_verify_against_another_files_root() {
        let mine = data(1 << 20);
        let theirs = data((1 << 20) + 1);
        let (their_root, _) = build_outboard(&theirs);
        let all = ChunkRanges::all();
        let encoded = encode_ranges(&mine, &all).expect("encode");

        let mut target = Vec::new();
        assert!(
            decode_into(
                their_root,
                mine.len() as u64,
                &encoded,
                &all,
                &mut target,
                &mut Vec::new()
            )
            .is_err(),
            "content must not verify against an unrelated root"
        );
    }

    #[test]
    fn an_empty_file_has_a_stable_root() {
        let (root, outboard) = build_outboard(&[]);
        assert_eq!(root, build_outboard(&[]).0, "not deterministic");
        assert!(outboard.is_empty(), "nothing to prove about no bytes");
    }

    /// Outboard overhead is the tax on every seedable file, so pin it.
    #[test]
    fn outboard_overhead_stays_under_a_tenth_of_a_percent() {
        let bytes = data(16 << 20);
        let (_, outboard) = build_outboard(&bytes);
        // Integer comparison rather than a float ratio: `outboard * 1000 <
        // data` is exactly "under 0.1%", with no rounding to argue about.
        assert!(
            outboard.len() * 1000 < bytes.len(),
            "64 KiB chunk groups should cost ~0.097%, got {} bytes over {}",
            outboard.len(),
            bytes.len()
        );
    }
}
