//! Stream order over an unordered transport: the sender's counter and the
//! receiver's reassembly.
//!
//! Gossip makes no ordering promise, so a byte stream cut into frames can
//! land shuffled. Each `pipe_data` frame carries its position (`seq`) in its
//! stream, and a stream is one (author, addressee) pair: a directed frame to
//! someone else never reaches this peer, so a counter per author alone would
//! leave permanent gaps in what it does see. `pipe_eof` carries the stream's
//! frame count, which is what tells a receiver it holds everything.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use fofoca::protocol::Nickname;
use n0_future::time::Instant;

use crate::app::Inbound;
use crate::wire::INBOUND_CAP;

/// How long a hole in a stream may stay open before the frames behind it are
/// released without it. Gossip delivers a late frame within well under a
/// second; a hole older than this is a frame that never came — the opening of
/// a stream this peer joined late, or a frame lost on the way — and a stream
/// frozen behind it helps nobody.
pub const GAP_TIMEOUT: Duration = Duration::from_secs(3);

/// The sender's side: one counter per addressee (`None` is the broadcast
/// stream).
#[derive(Debug, Default)]
pub struct StreamSeq {
    counters: HashMap<Option<Nickname>, u64>,
}

impl StreamSeq {
    /// The `seq` for the next frame to `to`, and advance.
    pub fn next(&mut self, to: &Option<Nickname>) -> u64 {
        let counter = self.counters.entry(to.clone()).or_default();
        let seq = *counter;
        *counter += 1;
        seq
    }

    /// How many frames went to `to` so far — the `count` a `pipe_eof` carries.
    #[must_use]
    pub fn count(&self, to: &Option<Nickname>) -> u64 {
        self.counters.get(to).copied().unwrap_or_default()
    }
}

/// The receiver's side of one stream.
///
/// The counter never resets: a sender that keeps writing after an EOF simply
/// continues its numbering, so only the EOF mark clears on completion.
///
/// A hole is given up two ways: after [`INBOUND_CAP`] later frames (a buffer
/// that only grows is worse than a hole) or after [`GAP_TIMEOUT`] (a stream
/// frozen behind one lost frame is worse too). The second needs a clock, so
/// a consumer calls [`Reorder::expire`] on a timer; nothing else does.
#[derive(Debug, Default)]
pub struct Reorder {
    next: u64,
    pending: BTreeMap<u64, Vec<u8>>,
    eof_at: Option<u64>,
    /// When the hole in front of `pending` opened; `None` while there is none.
    gap_since: Option<Instant>,
}

impl Reorder {
    /// Take the frame at `seq`; return every chunk now deliverable, in order.
    /// Empty on a gap or a duplicate.
    pub fn push(&mut self, seq: u64, bytes: Vec<u8>) -> Vec<Vec<u8>> {
        self.push_at(seq, bytes, Instant::now())
    }

    /// Release the frames behind a hole older than [`GAP_TIMEOUT`], in order.
    /// Empty when there is no such hole.
    pub fn expire(&mut self) -> Vec<Vec<u8>> {
        self.expire_at(Instant::now())
    }

    pub(crate) fn push_at(&mut self, seq: u64, bytes: Vec<u8>, now: Instant) -> Vec<Vec<u8>> {
        if seq < self.next || self.pending.contains_key(&seq) {
            return Vec::new();
        }
        self.pending.insert(seq, bytes);
        if self.pending.len() > INBOUND_CAP {
            self.skip_hole("pipe stream gap never filled; skipping");
        }
        self.drain(now)
    }

    pub(crate) fn expire_at(&mut self, now: Instant) -> Vec<Vec<u8>> {
        if self
            .gap_since
            .is_some_and(|since| now.saturating_duration_since(since) >= GAP_TIMEOUT)
        {
            self.skip_hole("pipe stream gap open too long; skipping");
        }
        self.drain(now)
    }

    fn skip_hole(&mut self, why: &'static str) {
        if let Some((&lowest, _)) = self.pending.first_key_value() {
            tracing::warn!(
                target: "fofoca::messages",
                skipped = lowest - self.next,
                why
            );
            self.next = lowest;
        }
    }

    /// Deliver everything contiguous from `next`, then note whether a hole
    /// remains and since when.
    fn drain(&mut self, now: Instant) -> Vec<Vec<u8>> {
        let mut delivered = Vec::new();
        while let Some(ready) = self.pending.remove(&self.next) {
            delivered.push(ready);
            self.next += 1;
        }
        self.gap_since = if self.pending.is_empty() {
            None
        } else if !delivered.is_empty() {
            // Progress was made, so whatever hole is left is a new one.
            Some(now)
        } else {
            self.gap_since.or(Some(now))
        };
        delivered
    }

    /// Note the stream's end at `count` frames; `true` when the stream is
    /// complete right now. Once complete, the mark clears so a later stream
    /// on the same pair can end again.
    pub fn eof(&mut self, count: u64) -> bool {
        self.eof_at = Some(count);
        self.settle()
    }

    /// Everything up to the EOF mark has been delivered.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.eof_at == Some(self.next)
    }

    fn settle(&mut self) -> bool {
        if !self.is_complete() {
            return false;
        }
        self.eof_at = None;
        true
    }
}

/// What one inbound frame released.
#[derive(Debug)]
pub struct Delivered {
    pub nick: String,
    pub directed: bool,
    /// In stream order; empty when the frame only filled a buffer.
    pub chunks: Vec<Vec<u8>>,
    /// The frame completed its stream (with or without chunks).
    pub complete: bool,
}

/// Every stream this peer receives, keyed by (author, directed).
#[derive(Debug, Default)]
pub struct Streams(HashMap<(String, bool), Reorder>);

impl Streams {
    /// Release, on every stream, the frames behind a hole older than
    /// [`GAP_TIMEOUT`]. Call it on a timer; each entry that delivered
    /// something (or completed) is returned.
    pub fn expire(&mut self) -> Vec<Delivered> {
        let now = Instant::now();
        let mut released = Vec::new();
        for ((nick, directed), stream) in &mut self.0 {
            let chunks = stream.expire_at(now);
            let complete = stream.settle();
            if !chunks.is_empty() || complete {
                released.push(Delivered {
                    nick: nick.clone(),
                    directed: *directed,
                    chunks,
                    complete,
                });
            }
        }
        released
    }

    /// Route `frame` to its stream.
    pub fn push(&mut self, frame: Inbound) -> Delivered {
        let Inbound {
            nick,
            directed,
            eof,
            seq,
            bytes,
        } = frame;
        let stream = self.0.entry((nick.clone(), directed)).or_default();
        let (chunks, complete) = if eof {
            (Vec::new(), stream.eof(seq))
        } else {
            let chunks = stream.push(seq, bytes);
            (chunks, stream.settle())
        };
        Delivered {
            nick,
            directed,
            chunks,
            complete,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(seq: u64) -> Vec<u8> {
        vec![u8::try_from(seq).unwrap()]
    }

    fn frame(nick: &str, directed: bool, seq: u64) -> Inbound {
        Inbound {
            nick: nick.to_owned(),
            directed,
            eof: false,
            seq,
            bytes: chunk(seq),
        }
    }

    fn eof(nick: &str, directed: bool, count: u64) -> Inbound {
        Inbound {
            nick: nick.to_owned(),
            directed,
            eof: true,
            seq: count,
            bytes: Vec::new(),
        }
    }

    #[test]
    fn the_sender_counts_per_addressee() {
        let mut seq = StreamSeq::default();
        let ana = Some(Nickname::new("ana").unwrap());
        assert_eq!(seq.next(&None), 0);
        assert_eq!(seq.next(&None), 1);
        assert_eq!(seq.next(&ana), 0);
        assert_eq!(seq.count(&None), 2);
        assert_eq!(seq.count(&ana), 1);
        assert_eq!(seq.count(&Some(Nickname::new("bo").unwrap())), 0);
    }

    #[test]
    fn in_order_frames_pass_straight_through() {
        let mut stream = Reorder::default();
        for seq in 0..3 {
            assert_eq!(stream.push(seq, chunk(seq)), vec![chunk(seq)]);
        }
    }

    #[test]
    fn shuffled_frames_come_out_in_order() {
        let mut stream = Reorder::default();
        assert_eq!(stream.push(2, chunk(2)), Vec::<Vec<u8>>::new());
        assert_eq!(stream.push(0, chunk(0)), vec![chunk(0)]);
        assert_eq!(stream.push(1, chunk(1)), vec![chunk(1), chunk(2)]);
    }

    #[test]
    fn a_duplicate_is_dropped() {
        let mut stream = Reorder::default();
        assert_eq!(stream.push(0, chunk(0)), vec![chunk(0)]);
        assert_eq!(stream.push(0, chunk(0)), Vec::<Vec<u8>>::new());
        assert_eq!(stream.push(2, chunk(2)), Vec::<Vec<u8>>::new());
        assert_eq!(stream.push(2, chunk(2)), Vec::<Vec<u8>>::new());
        assert_eq!(stream.push(1, chunk(1)), vec![chunk(1), chunk(2)]);
    }

    #[test]
    fn an_early_eof_completes_when_the_last_chunk_lands() {
        let mut stream = Reorder::default();
        stream.push(0, chunk(0));
        assert!(!stream.eof(2));
        assert!(!stream.is_complete());
        stream.push(1, chunk(1));
        assert!(stream.is_complete());
    }

    #[test]
    fn an_empty_stream_completes_on_its_eof() {
        let mut stream = Reorder::default();
        assert!(stream.eof(0));
    }

    #[test]
    fn a_stream_continues_past_its_eof_without_resetting() {
        let mut stream = Reorder::default();
        stream.push(0, chunk(0));
        assert!(stream.eof(1));
        assert!(!stream.is_complete(), "the mark clears once reported");
        assert_eq!(stream.push(1, chunk(1)), vec![chunk(1)]);
        assert!(stream.eof(2));
    }

    #[test]
    fn a_gap_that_never_fills_is_skipped_at_the_cap() {
        let mut stream = Reorder::default();
        let start = 1;
        let end = start + u64::try_from(INBOUND_CAP).unwrap();
        for seq in start..end {
            assert!(stream.push(seq, chunk(seq % 256)).is_empty(), "seq {seq}");
        }
        let released = stream.push(end, chunk(end % 256));
        assert_eq!(released.len(), INBOUND_CAP + 1);
        assert_eq!(released[0], chunk(1));
    }

    #[test]
    fn a_hole_older_than_the_timeout_is_skipped_on_expire() {
        let mut stream = Reorder::default();
        let opened = Instant::now();
        assert!(stream.push_at(1, chunk(1), opened).is_empty());
        assert!(stream.push_at(2, chunk(2), opened).is_empty());
        // Not yet: the hole is young.
        assert!(
            stream
                .expire_at(opened + GAP_TIMEOUT - Duration::from_millis(1))
                .is_empty()
        );
        assert_eq!(
            stream.expire_at(opened + GAP_TIMEOUT),
            vec![chunk(1), chunk(2)]
        );
        // The counter moved past the hole: the next frame flows.
        assert_eq!(stream.push_at(3, chunk(3), opened), vec![chunk(3)]);
    }

    #[test]
    fn a_hole_that_fills_in_time_is_not_a_gap() {
        let mut stream = Reorder::default();
        let opened = Instant::now();
        assert!(stream.push_at(1, chunk(1), opened).is_empty());
        assert_eq!(
            stream.push_at(0, chunk(0), opened + Duration::from_secs(1)),
            vec![chunk(0), chunk(1)]
        );
        assert!(stream.expire_at(opened + GAP_TIMEOUT * 2).is_empty());
    }

    #[test]
    fn a_new_hole_after_progress_gets_its_own_clock() {
        let mut stream = Reorder::default();
        let opened = Instant::now();
        stream.push_at(0, chunk(0), opened);
        // Hole at 1 opens now.
        assert!(stream.push_at(2, chunk(2), opened).is_empty());
        // It fills just in time, and a new hole (at 3) opens with this frame.
        let later = opened + GAP_TIMEOUT - Duration::from_millis(1);
        assert_eq!(stream.push_at(1, chunk(1), later), vec![chunk(1), chunk(2)]);
        assert!(stream.push_at(4, chunk(4), later).is_empty());
        // Measured from `later`, not from `opened`.
        assert!(stream.expire_at(opened + GAP_TIMEOUT).is_empty());
        assert_eq!(stream.expire_at(later + GAP_TIMEOUT), vec![chunk(4)]);
    }

    #[test]
    fn expire_on_streams_reports_completion_too() {
        let mut streams = Streams::default();
        assert!(!streams.push(eof("ana", false, 2)).complete);
        assert!(streams.push(frame("ana", false, 1)).chunks.is_empty());
        // Force the hole's age without a real clock: the stream's clock is
        // private, so push it past the timeout through the pub(crate) seam.
        let stream = streams.0.get_mut(&("ana".to_owned(), false)).unwrap();
        stream.gap_since = Some(Instant::now() - GAP_TIMEOUT * 2);
        let released = streams.expire();
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].chunks, vec![chunk(1)]);
        assert!(released[0].complete);
        assert!(streams.expire().is_empty());
    }

    #[test]
    fn streams_are_keyed_by_author_and_direction() {
        let mut streams = Streams::default();
        let gap = streams.push(frame("ana", false, 1));
        assert!(gap.chunks.is_empty() && !gap.complete);
        let directed = streams.push(frame("ana", true, 0));
        assert_eq!(directed.chunks, vec![chunk(0)]);
        assert!(directed.directed);
        let other = streams.push(frame("bo", false, 0));
        assert_eq!((other.nick.as_str(), other.chunks), ("bo", vec![chunk(0)]));
        let filled = streams.push(frame("ana", false, 0));
        assert_eq!(filled.chunks, vec![chunk(0), chunk(1)]);
        assert!(!filled.complete);
        let ended = streams.push(eof("ana", false, 2));
        assert!(ended.chunks.is_empty() && ended.complete);
    }

    #[test]
    fn a_late_chunk_reports_completion_itself() {
        let mut streams = Streams::default();
        assert!(!streams.push(eof("ana", false, 1)).complete);
        let last = streams.push(frame("ana", false, 0));
        assert_eq!(last.chunks, vec![chunk(0)]);
        assert!(last.complete);
    }
}
