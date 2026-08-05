//! The on-storage format, shared by every persistent backend.
//!
//! `FsStore` and `OpfsStore` differ entirely in *how* they read and write —
//! `std::fs` against a `FileSystemSyncAccessHandle` — and not at all in *what*.
//! Keeping the format here rather than in either of them means a store written
//! by the CLI is readable by the browser, and that the two cannot drift into
//! disagreeing about what a range set says.
//!
//! Text, not a binary encoding. These files are small, they are read once at
//! open, and being able to `cat` one while diagnosing a peer that claims the
//! wrong ranges is worth more than the bytes.

use anyhow::{Result, bail};
use bao_tree::{ChunkNum, ChunkRanges};
use range_collections::range_set::RangeSetRange;

use crate::Root;

/// Lowercase hex, as every sidecar filename is keyed.
#[must_use]
pub(crate) fn hex(root: Root) -> String {
    use std::fmt::Write as _;
    root.iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Parse a 64-character lowercase hex root.
#[must_use]
pub(crate) fn parse_hex(text: &str) -> Option<Root> {
    if text.len() != 64 {
        return None;
    }
    let mut root = [0u8; 32];
    for (index, byte) in root.iter_mut().enumerate() {
        *byte = u8::from_str_radix(text.get(index * 2..index * 2 + 2)?, 16).ok()?;
    }
    Some(root)
}

/// `start-end` per line, half-open, in chunk units.
#[must_use]
pub(crate) fn format_ranges(ranges: &ChunkRanges) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for range in ranges.iter() {
        // `RangeFrom` is skipped deliberately: an unbounded tail is not
        // something a store can hold, and writing one would read back as a
        // claim to infinite bytes.
        if let RangeSetRange::Range(range) = range {
            let _ = writeln!(out, "{}-{}", range.start.0, range.end.0);
        }
    }
    out
}

/// Parse a range set, skipping anything malformed.
///
/// Tolerant on purpose. These files are rewritten wholesale, so a torn tail is
/// the expected shape after a crash — and losing a range costs a re-fetch,
/// whereas refusing the whole file would cost every range in it.
#[must_use]
pub(crate) fn parse_ranges(text: &str) -> ChunkRanges {
    let mut ranges = ChunkRanges::empty();
    for line in text.lines() {
        let Some((start, end)) = line.split_once('-') else {
            continue;
        };
        let (Ok(start), Ok(end)) = (start.parse::<u64>(), end.parse::<u64>()) else {
            continue;
        };
        ranges |= ChunkRanges::from(ChunkNum(start)..ChunkNum(end));
    }
    ranges
}

/// One binding per line: `key \t size \t mtime \t root`.
pub(crate) fn format_binds(binds: &[(String, u64, i64, Root)]) -> Result<String> {
    use std::fmt::Write as _;
    let mut out = String::new();
    for (key, size, mtime, root) in binds {
        // A key containing the separator would read back as a different
        // binding. Paths may legally contain a tab, so refuse rather than write
        // a file that lies on reload.
        if key.contains('\t') || key.contains('\n') {
            bail!("a store key may not contain a tab or newline: {key:?}");
        }
        let _ = writeln!(out, "{key}\t{size}\t{mtime}\t{}", hex(*root));
    }
    Ok(out)
}

/// Parse the bind table, skipping anything malformed — see [`parse_ranges`].
#[must_use]
pub(crate) fn parse_binds(text: &str) -> Vec<(String, u64, i64, Root)> {
    let mut binds = Vec::new();
    for line in text.lines() {
        let mut fields = line.split('\t');
        let (Some(key), Some(size), Some(mtime), Some(root)) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let (Ok(size), Ok(mtime)) = (size.parse::<u64>(), mtime.parse::<i64>()) else {
            continue;
        };
        let Some(root) = parse_hex(root) else {
            continue;
        };
        binds.push((key.to_owned(), size, mtime, root));
    }
    binds
}

#[cfg(test)]
mod tests {
    use super::{format_binds, format_ranges, hex, parse_binds, parse_hex, parse_ranges};
    use bao_tree::{ChunkNum, ChunkRanges};

    #[test]
    fn a_root_round_trips_through_hex() {
        let root = [0xABu8; 32];
        assert_eq!(parse_hex(&hex(root)), Some(root));
        assert_eq!(parse_hex("short"), None);
        assert_eq!(parse_hex(&"z".repeat(64)), None);
    }

    #[test]
    fn ranges_round_trip() {
        let mut ranges = ChunkRanges::from(ChunkNum(0)..ChunkNum(64));
        ranges |= ChunkRanges::from(ChunkNum(128)..ChunkNum(192));
        assert_eq!(parse_ranges(&format_ranges(&ranges)), ranges);
    }

    /// The whole reason `format_ranges` filters: `all()` is `0..∞`, and a store
    /// cannot hold an unbounded tail.
    #[test]
    fn an_unbounded_range_is_not_written() {
        assert_eq!(format_ranges(&ChunkRanges::all()), "");
    }

    #[test]
    fn binds_round_trip() {
        let binds = vec![
            ("a/b.txt".to_owned(), 10, 20, [1u8; 32]),
            ("c.bin".to_owned(), 0, -1, [2u8; 32]),
        ];
        let text = format_binds(&binds).expect("format");
        assert_eq!(parse_binds(&text), binds);
    }

    /// A separator in a key would read back as a different binding, so it is
    /// refused at write time rather than corrupting the table.
    #[test]
    fn a_key_with_a_separator_is_refused() {
        let binds = vec![("has\ttab".to_owned(), 1, 1, [0u8; 32])];
        assert!(format_binds(&binds).is_err());
    }

    /// A torn tail is the expected shape after a crash mid-rewrite. Losing one
    /// line must not cost the lines before it.
    #[test]
    fn a_truncated_table_keeps_what_it_can() {
        let binds = vec![
            ("first".to_owned(), 1, 1, [7u8; 32]),
            ("second".to_owned(), 2, 2, [8u8; 32]),
        ];
        let text = format_binds(&binds).expect("format");
        let torn = &text[..text.len() - 20];
        let parsed = parse_binds(torn);
        assert_eq!(parsed.len(), 1, "the intact line survives");
        assert_eq!(parsed[0].0, "first");
    }
}
