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

/// One `pipe_data` body: `slice`, base64-encoded.
///
/// `slice` should be no longer than [`default_chunk`]. A longer one still
/// encodes here and is refused by the engine at [`MAX_MESSAGE_SIZE`], which is a
/// worse place to find out.
///
/// # Errors
/// The encoded body is not a valid [`MessageBody`]. Unreachable for base64,
/// which is control-character-free by construction, but the type demands it.
pub fn data_body(slice: &[u8]) -> Result<MessageBody> {
    MessageBody::new(BASE64.encode(slice)).map_err(|error| anyhow::anyhow!("{error}"))
}

/// The empty body of a `pipe_eof` frame.
///
/// # Errors
/// As [`data_body`].
pub fn eof_body() -> Result<MessageBody> {
    MessageBody::new(String::new()).map_err(|error| anyhow::anyhow!("{error}"))
}

/// The bytes behind a `pipe_data` body, or `None` when it does not decode.
///
/// The inverse of [`data_body`], written here rather than inline in the receive
/// path so both directions of the codec sit one screen apart and one test away.
#[must_use]
pub fn decode_data(body: &MessageBody) -> Option<Vec<u8>> {
    BASE64.decode(body.as_str()).ok()
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

/// The default raw-bytes-per-frame budget: a `pipe_data` frame is unsharded, so
/// the base64-inflated body plus the JSON envelope must fit
/// [`MAX_MESSAGE_SIZE`]. Invert base64's 4/3 growth after reserving envelope
/// headroom.
///
/// The result is a multiple of 3 by construction — `n / 4 * 3` is `3k` for any
/// `n` — which is what keeps base64 from emitting mid-stream padding. No
/// separate rounding step is needed for that.
///
/// Public because a consumer needs it to size its receive buffer: a frame larger
/// than the buffer handed to `fofoca_recv` is an error, not a truncation.
#[must_use]
pub fn default_chunk() -> usize {
    const ENVELOPE_RESERVE: usize = 1024;
    let body_budget = MAX_MESSAGE_SIZE.saturating_sub(ENVELOPE_RESERVE);
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
    fn an_encoded_chunk_still_fits_one_frame() {
        let encoded = default_chunk().div_ceil(3) * 4;
        assert!(
            encoded <= MAX_MESSAGE_SIZE,
            "{encoded} > {MAX_MESSAGE_SIZE}"
        );
    }

    #[test]
    fn the_codec_round_trips_at_every_alignment() {
        for len in [0_usize, 1, 2, 3, 4, 5, default_chunk()] {
            let bytes: Vec<u8> = (0..len)
                .map(|byte| u8::try_from(byte % 256).unwrap())
                .collect();
            let body = data_body(&bytes).expect("base64 is a valid body");
            assert_eq!(
                decode_data(&body).as_deref(),
                Some(bytes.as_slice()),
                "len {len}"
            );
        }
    }

    #[test]
    fn an_undecodable_body_is_none_rather_than_a_panic() {
        let body = MessageBody::new("not base64!!".to_owned()).expect("plain text is a valid body");
        assert_eq!(decode_data(&body), None);
    }

    #[test]
    fn an_eof_body_is_genuinely_empty() {
        assert_eq!(eof_body().expect("empty is a valid body").as_str(), "");
    }
}
