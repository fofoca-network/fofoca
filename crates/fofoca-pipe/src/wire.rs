//! The frame taxonomy: the tags, the body codec, and the budgets they imply.

use std::time::Duration;

use anyhow::Result;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use fofoca::protocol::{AppTag, MessageBody, Nickname};
use fofoca::util::consts::MAX_MESSAGE_SIZE;

/// The `App`-frame tags — the engine routes on the tag but never interprets it.
pub mod tag {
    pub const DATA: &str = "pipe_data";
    pub const EOF: &str = "pipe_eof";
    /// A receiver's count of an author's frames, sent back to the author.
    pub const ACK: &str = "pipe_ack";
}

/// Inbound frames buffered for a consumer that reads on its own schedule.
/// Bounded, because a peer that only ever *sends* still receives broadcasts: an
/// unbounded queue would grow for the process's lifetime.
pub const INBOUND_CAP: usize = 256;

/// Post-EOF wait before leaving, so in-flight frames land first.
pub const DEPARTURE_GRACE: Duration = Duration::from_millis(750);

/// The tag on a data frame.
#[must_use]
pub fn data_tag() -> AppTag {
    AppTag::from(tag::DATA)
}

/// The tag on an end-of-stream marker.
#[must_use]
pub fn eof_tag() -> AppTag {
    AppTag::from(tag::EOF)
}

/// The tag on an acknowledgement.
#[must_use]
pub fn ack_tag() -> AppTag {
    AppTag::from(tag::ACK)
}

/// One `pipe_data` body: `<seq>:<base64 of slice>`.
///
/// `seq` is the frame's position in its stream — one counter per (author,
/// addressee), starting at 0 — and rides in the body because the engine keeps
/// no slot for it: `corr` is the pending-call id, and `Message::seq` is only
/// stamped on chained frames, which a pipe frame deliberately is not. `:` is
/// not a base64 character, so the split is unambiguous.
///
/// `slice` should be no longer than [`default_chunk`]. A longer one still
/// encodes here and is refused by the engine at [`MAX_MESSAGE_SIZE`], which is a
/// worse place to find out.
///
/// # Errors
/// The encoded body is not a valid [`MessageBody`]. Unreachable for a number
/// and base64, which are control-character-free by construction, but the type
/// demands it.
pub fn data_body(seq: u64, slice: &[u8]) -> Result<MessageBody> {
    MessageBody::new(format!("{seq}:{}", BASE64.encode(slice)))
        .map_err(|error| anyhow::anyhow!("{error}"))
}

/// The body of a `pipe_eof` frame: `count`, the number of `pipe_data` frames
/// the stream carried, so a receiver knows when it holds all of them.
///
/// # Errors
/// As [`data_body`].
pub fn eof_body(count: u64) -> Result<MessageBody> {
    MessageBody::new(count.to_string()).map_err(|error| anyhow::anyhow!("{error}"))
}

/// The `(seq, bytes)` behind a `pipe_data` body, or `None` when it does not
/// decode — a pre-`seq` body included, which is the wire break announcing
/// itself.
///
/// The inverse of [`data_body`], written here rather than inline in the receive
/// path so both directions of the codec sit one screen apart and one test away.
#[must_use]
pub fn decode_data(body: &MessageBody) -> Option<(u64, Vec<u8>)> {
    let (seq, encoded) = body.as_str().split_once(':')?;
    let seq = seq.parse().ok()?;
    let bytes = BASE64.decode(encoded).ok()?;
    Some((seq, bytes))
}

/// The stream count behind a `pipe_eof` body, or `None` when it does not
/// decode.
#[must_use]
pub fn decode_eof(body: &MessageBody) -> Option<u64> {
    body.as_str().parse().ok()
}

/// The body of a `pipe_ack`: `b:<n>` for the author's broadcast stream,
/// `d:<n>` for the stream the author directs at us — `n` frames received
/// so far. Directed back to the author; see [`crate::Flow`].
///
/// # Errors
/// As [`data_body`].
pub fn ack_body(directed: bool, received: u64) -> Result<MessageBody> {
    let kind = if directed { 'd' } else { 'b' };
    MessageBody::new(format!("{kind}:{received}")).map_err(|error| anyhow::anyhow!("{error}"))
}

/// The `(directed, received)` behind a `pipe_ack` body, or `None`.
#[must_use]
pub fn decode_ack(body: &MessageBody) -> Option<(bool, u64)> {
    let (kind, received) = body.as_str().split_once(':')?;
    let directed = match kind {
        "b" => false,
        "d" => true,
        _ => return None,
    };
    Some((directed, received.parse().ok()?))
}

/// Parse an optional addressee.
///
/// # Errors
/// `to` is not a valid nickname.
pub fn parse_to(to: Option<&str>) -> Result<Option<Nickname>> {
    to.map(|nick| Nickname::new(nick.to_owned()))
        .transpose()
        .map_err(|error| anyhow::anyhow!("{error}"))
}

/// Headroom for the JSON envelope.
const ENVELOPE_RESERVE: usize = 1024;

/// The widest `<seq>:` prefix: `u64::MAX` and the colon.
const SEQ_PREFIX_MAX: usize = 21;

/// The default raw-bytes-per-frame budget: a `pipe_data` frame is unsharded, so
/// the `<seq>:` prefix plus the base64-inflated body plus the JSON envelope
/// must fit [`MAX_MESSAGE_SIZE`]. Invert base64's 4/3 growth after reserving
/// the other two.
///
/// The result is a multiple of 3 by construction — `n / 4 * 3` is `3k` for any
/// `n` — which is what keeps base64 from emitting mid-stream padding. No
/// separate rounding step is needed for that.
///
/// Public because a consumer needs it to size its receive buffer: a frame larger
/// than the buffer handed to `fofoca_recv` is an error, not a truncation.
#[must_use]
pub fn default_chunk() -> usize {
    let body_budget = MAX_MESSAGE_SIZE.saturating_sub(ENVELOPE_RESERVE + SEQ_PREFIX_MAX);
    body_budget / 4 * 3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_chunk_never_makes_base64_pad_mid_stream() {
        assert_eq!(default_chunk() % 3, 0);
    }

    #[test]
    fn the_widest_prefix_is_budgeted() {
        assert_eq!(format!("{}:", u64::MAX).len(), SEQ_PREFIX_MAX);
    }

    #[test]
    fn a_prefixed_chunk_still_leaves_the_envelope_its_reserve() {
        let body = data_body(u64::MAX, &vec![0xAB; default_chunk()]).expect("valid body");
        assert!(
            body.as_str().len() + ENVELOPE_RESERVE <= MAX_MESSAGE_SIZE,
            "{} + {ENVELOPE_RESERVE} > {MAX_MESSAGE_SIZE}",
            body.as_str().len()
        );
    }

    #[test]
    fn the_codec_round_trips_at_every_alignment_and_seq() {
        for seq in [0, 1, u64::MAX] {
            for len in [0_usize, 1, 2, 3, 4, 5, default_chunk()] {
                let bytes: Vec<u8> = (0..len)
                    .map(|byte| u8::try_from(byte % 256).unwrap())
                    .collect();
                let body = data_body(seq, &bytes).expect("base64 is a valid body");
                assert_eq!(
                    decode_data(&body),
                    Some((seq, bytes)),
                    "seq {seq} len {len}"
                );
            }
        }
    }

    #[test]
    fn an_undecodable_body_is_none_rather_than_a_panic() {
        for text in ["not base64!!", "1:not base64!!", "x:aGk=", ":aGk=", "1"] {
            let body = MessageBody::new(text.to_owned()).expect("plain text is a valid body");
            assert_eq!(decode_data(&body), None, "{text:?}");
        }
    }

    #[test]
    fn a_pre_seq_body_is_refused_rather_than_misread() {
        let body = MessageBody::new(BASE64.encode(b"hello")).expect("base64 is a valid body");
        assert_eq!(decode_data(&body), None);
    }

    #[test]
    fn an_ack_body_names_the_stream_and_the_count() {
        for (directed, received) in [(false, 0), (true, 8), (false, u64::MAX)] {
            let body = ack_body(directed, received).expect("valid body");
            assert_eq!(decode_ack(&body), Some((directed, received)));
        }
        for text in ["x:1", "b:", "b:x", "8"] {
            let body = MessageBody::new(text.to_owned()).expect("plain text is a valid body");
            assert_eq!(decode_ack(&body), None, "{text:?}");
        }
    }

    #[test]
    fn an_eof_body_carries_the_stream_count() {
        for count in [0, 1, 7, u64::MAX] {
            let body = eof_body(count).expect("a number is a valid body");
            assert_eq!(decode_eof(&body), Some(count));
        }
        let old = MessageBody::new(String::new()).expect("empty is a valid body");
        assert_eq!(decode_eof(&old), None);
    }
}
