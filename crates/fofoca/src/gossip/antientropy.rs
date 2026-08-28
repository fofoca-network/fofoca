//! Anti-entropy: periodic digest broadcast + gap-fill resend. Recovers
//! messages a node missed while partitioned/asleep/just-joined.
//!
//! The digest advertises a **window** of the message log: an inclusive
//! `[lo, hi]` timestamp range plus the compact ids the sender holds in it
//! (raw 16-byte UUIDs, Base58-packed, so ~10× more fit one gossip message
//! than the old 36-char strings). A log larger than one window is swept
//! across rounds via a rolling cursor; the `[lo, hi]` bounds let a receiver
//! re-send only **in-window** gaps, so advertising a sub-window never makes
//! peers perpetually re-broadcast the out-of-window remainder.
//!
//! A resend rides the same plane the original send chose: broadcast content
//! goes back on gossip, and a directed frame goes point-to-point to its
//! addressee — never to the peer that merely asked. Both follow from routing
//! every resend through [`crate::transport::deliver`].

use std::collections::HashSet;

use crate::transport::MeshSender;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::daemon::ctx::HandlerCtx;
use crate::daemon::message_log::{DigestWindow, MissingQuery, WindowRange};
use crate::daemon::state::EventLoopState;
use crate::protocol::{Channel, MeshId, Message, Nickname};
use crate::util::tuning::{ANTIENTROPY_DIGEST_WINDOW_IDS, antientropy_max_resend};

use super::broadcast_msg;

/// One window on the wire: its `[lo, hi]` ts bounds (`hi == i64::MAX` ⇒
/// open-ended) plus its ids packed as raw 16-byte UUIDs, Base58-encoded.
#[derive(Serialize, Deserialize)]
struct WireWindow {
    lo: i64,
    hi: i64,
    ids: String,
}

impl WireWindow {
    fn encode(window: &DigestWindow) -> Self {
        let mut packed = Vec::with_capacity(window.ids.len() * 16);
        for id in &window.ids {
            packed.extend_from_slice(id);
        }
        WireWindow {
            lo: window.lo,
            hi: window.hi,
            ids: bs58::encode(packed).into_string(),
        }
    }

    /// Decode the packed ids into a set, or `None` if the Base58 / length
    /// is malformed.
    fn decode_ids(&self) -> Option<HashSet<[u8; 16]>> {
        let raw = bs58::decode(&self.ids).into_vec().ok()?;
        if raw.len() % 16 != 0 {
            return None;
        }
        Some(
            raw.chunks_exact(16)
                .map(|chunk| {
                    let mut id = [0u8; 16];
                    id.copy_from_slice(chunk);
                    id
                })
                .collect(),
        )
    }
}

/// The on-the-wire digest body: up to two windows (open-ended newest +
/// rolling closed older).
#[derive(Serialize, Deserialize)]
struct DigestBody {
    windows: Vec<WireWindow>,
}

/// The state/meta anti-entropy digest body: this channel's automerge heads
/// (Base58 change hashes). Heads compactly represent the whole causal frontier,
/// so a holder computes exactly what the sender is missing in one step — no
/// windowing. Replaces the windowed [`DigestBody`] for these two channels.
#[derive(Serialize, Deserialize)]
struct HeadsBody {
    heads: Vec<String>,
}

/// Broadcast an anti-entropy digest: an **open-ended newest** window (so
/// holders re-send every newer message we lack — reconnect recovery) plus,
/// when the log is larger than one window, a rolling **closed** older
/// window (deep interior reconcile, swept across rounds via
/// `digest_cursor`). A node that missed messages while
/// partitioned/asleep/just-joined recovers. Like `PeerInfo`, never logged.
pub(crate) async fn broadcast_digest(
    state: &mut EventLoopState,
    sender: &MeshSender,
    mesh: &MeshId,
    author: &Nickname,
) {
    // No real peer has ever linked — a digest would broadcast into the
    // void (mirrors `tick_heal`/`tick_alive` no-peer guards).
    if !state.meshed {
        return;
    }
    let recent = ANTIENTROPY_DIGEST_WINDOW_IDS;
    let Some(newest) = state.message_log.recent_window(recent) else {
        return; // empty log
    };
    let mut windows = vec![WireWindow::encode(&newest)];

    let older_len = state.message_log.older_len(recent);
    if older_len == 0 {
        state.digest_cursor = 0;
    } else {
        let start = state.digest_cursor % older_len;
        if let Some(older) = state.message_log.older_window(recent, start, recent) {
            state.digest_cursor = (start + older.ids.len()) % older_len;
            windows.push(WireWindow::encode(&older));
        }
    }

    let total_ids: usize = windows.iter().map(|window| window.ids.len()).sum();
    let Some(body) = super::json_body(&DigestBody { windows }) else {
        return;
    };
    tracing::trace!(ids = total_ids, "anti-entropy digest broadcast");
    state.idle.broadcasts += 1;
    broadcast_msg(
        sender,
        &Message::new_digest(mesh, author, body).signed(&state.identity),
    )
    .await;
}

/// Handle a received anti-entropy digest: for each advertised window, re-send
/// our logged messages the sender lacks **within that window** (open-ended
/// newest ⇒ everything newer; closed older ⇒ that slice only), newest-first, up
/// to `antientropy_max_resend()` total. Receivers that already have them drop
/// the repeat (dedup); the sender (and anyone else who missed them) recovers.
/// Never logged.
///
/// Each resend goes through [`crate::transport::deliver`] rather than straight
/// onto gossip, so it takes the plane its addressing dictates — the same
/// decision the original send made. Paired with `missing_in_window`'s own
/// addressee gate, a directed frame is re-sent point-to-point to its addressee
/// and to nobody else, so backfill can't put on the gossip flood what the send
/// path structurally kept off it.
///
/// The windows' `have` sets are **unioned** before the diff (as in
/// [`handle_state_digest`]): the open-ended newest (`[lo, MAX]`) and closed
/// older (`[lo, hi]`) windows' ranges overlap at one-second-equal timestamps,
/// so a per-window `have` would re-send a message the sender holds but listed
/// under the *other* window — wasting the shared resend budget on messages the
/// peer already has and starving the genuinely-missing tail.
pub(crate) async fn handle_digest(
    message: &Message,
    state: &mut EventLoopState,
    ctx: &HandlerCtx<'_>,
) {
    let Ok(body) = serde_json::from_str::<DigestBody>(message.body.as_str()) else {
        return;
    };
    // One serve per author per window. Answering costs up to
    // `ANTIENTROPY_MAX_RESEND` mesh-wide broadcasts, so ungated this turns one
    // small frame into that much flooding from every member that hears it —
    // the cost scaling with the mesh rather than with the sender.
    if !state.admit_digest(&message.pubkey, crate::util::clock::Instant::now()) {
        tracing::debug!(
            target: "fofoca::gossip",
            author = %message.author,
            "digest ignored: this peer was served within the window"
        );
        return;
    }
    let mut have: HashSet<[u8; 16]> = HashSet::new();
    for window in &body.windows {
        if let Some(ids) = window.decode_ids() {
            have.extend(ids);
        }
    }
    let mut budget = antientropy_max_resend();
    let mut resent = 0usize;
    for window in &body.windows {
        if budget == 0 {
            break;
        }
        for msg in state.message_log.missing_in_window(MissingQuery {
            range: WindowRange {
                lo: window.lo,
                hi: window.hi,
            },
            have: &have,
            max: budget,
            requester: &message.author,
        }) {
            if !resend_one(&msg, state, ctx).await {
                continue;
            }
            // Mark it sent so the next (overlapping) window doesn't re-send it
            // and waste budget — equal-timestamp ranges overlap heavily.
            // Charged even when the send failed: an addressee with no endpoint
            // yet must not be retried inside this digest; it retries next round.
            have.insert(msg.dedup_key());
            resent += 1;
            budget -= 1;
        }
    }
    if resent > 0 {
        tracing::debug!(resent, "anti-entropy: resent messages a peer was missing");
    }
}

/// Re-send one message on the plane its addressing dictates. Returns whether it
/// was attempted at all — `false` only for the unserializable frame we somehow
/// hold, which must not be charged to the round's budget.
async fn resend_one(msg: &Message, state: &EventLoopState, ctx: &HandlerCtx<'_>) -> bool {
    let Ok(bytes) = msg.serialize() else {
        return false;
    };
    if let Err(error) = crate::transport::deliver(msg, Bytes::from(bytes), state, ctx.sender).await
    {
        tracing::debug!(target: "fofoca::gossip", %error, "anti-entropy resend failed");
    }
    true
}

/// Sweep both shared-state channels' anti-entropy digests (one tick).
///
/// Each digest advertises the channel's automerge heads — see
/// [`HeadsBody`] and [`handle_state_digest`]. Broadcast whenever the overlay
/// is reachable (see [`state_digest`]), even on an empty document, so a fresh
/// joiner advertises its empty frontier and gets backfilled.
pub(crate) async fn broadcast_state_digests(
    state: &mut EventLoopState,
    sender: &MeshSender,
    mesh: &MeshId,
    author: &Nickname,
) {
    let origin = DigestOrigin { mesh, author };
    broadcast_state_digest(state, sender, origin, Channel::State).await;
    broadcast_state_digest(state, sender, origin, Channel::Meta).await;
}

#[derive(Clone, Copy)]
struct DigestOrigin<'a> {
    mesh: &'a MeshId,
    author: &'a Nickname,
}

async fn broadcast_state_digest(
    state: &mut EventLoopState,
    sender: &MeshSender,
    origin: DigestOrigin<'_>,
    channel: Channel,
) {
    let Some(digest) = state_digest(state, origin, channel) else {
        return;
    };
    state.idle.broadcasts += 1;
    broadcast_msg(sender, &digest).await;
}

/// The signed heads digest for `channel`, or `None` while no gossip path can
/// carry it. Unlike the chat digest this does not wait for `meshed`: a node
/// whose only neighbor is the rendezvous relay still exchanges presence over
/// that link, so its documents must travel the same way. Gating on `meshed`
/// left two peers behind one relay with converged rosters and documents that
/// never met — a card published before anyone joined sat unadvertised until a
/// real-peer link happened to form, minutes later or never.
fn state_digest(
    state: &EventLoopState,
    origin: DigestOrigin<'_>,
    channel: Channel,
) -> Option<Message> {
    if !state.overlay_reachable() {
        return None;
    }
    let heads = state.doc(channel).heads();
    let body = super::json_body(&HeadsBody { heads })?;
    Some(
        Message::new_channel_digest(origin.mesh, origin.author, body, channel)
            .signed(&state.identity),
    )
}

/// Handle a received state digest: the sender advertised its automerge heads, so
/// re-broadcast the signed change frames it is missing (`changes_since`), up to
/// an own resend budget (separate from the chat digest's, so a busy chat log
/// can't starve state backfill). automerge's DAG collapses "what's missing" into
/// one query — no windows, no cursor. A late joiner advertising an empty (or
/// genesis-only) frontier pulls the whole history over successive rounds as its
/// heads advance.
pub(crate) async fn handle_state_digest(
    channel: Channel,
    message: &Message,
    state: &EventLoopState,
    ctx: &HandlerCtx<'_>,
) {
    let mut resent = 0usize;
    for frame in missing_frames(channel, message, state) {
        if let Ok(bytes) = frame.serialize() {
            let _ = ctx.sender.broadcast(Bytes::from(bytes)).await;
            resent += 1;
        }
    }
    if resent > 0 {
        tracing::debug!(resent, "state anti-entropy: resent frames a peer lacked");
    }
}

/// The signed change frames the author of `digest` lacks on `channel`, up to
/// the resend budget. Empty for an undecodable digest body.
fn missing_frames(channel: Channel, digest: &Message, state: &EventLoopState) -> Vec<Message> {
    let Ok(body) = serde_json::from_str::<HeadsBody>(digest.body.as_str()) else {
        return Vec::new();
    };
    state
        .doc(channel)
        .changes_since(&body.heads, antientropy_max_resend())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{
        ANTIENTROPY_DIGEST_WINDOW_IDS, DigestBody, DigestOrigin, HeadsBody, WireWindow,
        missing_frames, state_digest,
    };
    use crate::daemon::message_log::{DigestWindow, MessageLog};
    use crate::daemon::state::EventLoopState;
    use crate::doc::Ingested;
    use crate::protocol::{Channel, MeshId, Message, MessageBody, Nickname};
    use crate::testing::{fresh_state, nick};

    /// A node whose only gossip neighbor is the rendezvous relay: linked to it,
    /// never to a real peer.
    fn rendezvous_only() -> EventLoopState {
        let mut state = fresh_state();
        state.meshed = false;
        state.rendezvous_linked = true;
        state
    }

    /// Apply a meta merge locally the way `broadcast_state_merge` does, minus
    /// the wire: build, sign, ingest. What the doc holds afterwards is exactly
    /// what anti-entropy can re-serve.
    fn publish_meta(
        state: &mut EventLoopState,
        mesh: &MeshId,
        author: &Nickname,
        merge: &serde_json::Value,
    ) {
        let seed = *state.identity.public().as_bytes();
        let change = state
            .doc(Channel::Meta)
            .build_change(merge, &seed)
            .expect("a JSON object merges")
            .expect("a non-empty merge yields a change");
        let (wire, _plain) = state
            .doc(Channel::Meta)
            .compose_wire_body(&change, None)
            .expect("compose the wire body");
        let frame =
            Message::new_channel_event(mesh, author, wire, Channel::Meta).signed(&state.identity);
        assert!(
            matches!(
                state.doc_mut(Channel::Meta).ingest(&frame),
                Ingested::Applied { .. }
            ),
            "a locally-built change applies"
        );
    }

    /// One anti-entropy round on the meta channel, from `joiner`'s side:
    /// `joiner` advertises its heads, `holder` re-serves every change frame
    /// the digest shows missing, `joiner` ingests them. Returns how many frames
    /// travelled, or `None` when `joiner` sent no digest at all.
    fn round(
        joiner: &mut EventLoopState,
        joiner_nick: &Nickname,
        holder: &EventLoopState,
        mesh: &MeshId,
    ) -> Option<usize> {
        let origin = DigestOrigin {
            mesh,
            author: joiner_nick,
        };
        let digest = state_digest(joiner, origin, Channel::Meta)?;
        let frames = missing_frames(Channel::Meta, &digest, holder);
        for frame in &frames {
            joiner.doc_mut(Channel::Meta).ingest(frame);
        }
        Some(frames.len())
    }

    /// The scenario behind the fix: A publishes its meta card while alone on
    /// the mesh, B joins later, and the two only ever share the rendezvous
    /// relay as a gossip neighbor (`meshed` never flips on either side). B's
    /// meta doc must converge to A's within two anti-entropy rounds.
    #[test]
    fn late_joiner_converges_meta_over_a_rendezvous_only_link() {
        let mesh = MeshId::from("test");
        let alice_nick = nick("alice");
        let bob_nick = nick("bob");
        let mut alice = rendezvous_only();
        publish_meta(
            &mut alice,
            &mesh,
            &alice_nick,
            &serde_json::json!({"peers": {"alice": {"status": "idle", "model": "m"}}}),
        );
        let mut bob = rendezvous_only();
        assert_ne!(
            bob.doc(Channel::Meta).heads(),
            alice.doc(Channel::Meta).heads(),
            "bob joins knowing nothing"
        );

        // Round 1: bob's empty frontier pulls alice's change; alice's digest
        // finds nothing bob holds that she lacks.
        let served = round(&mut bob, &bob_nick, &alice, &mesh)
            .expect("a rendezvous-only joiner still advertises its heads");
        assert_eq!(served, 1, "bob lacked exactly alice's one change");
        assert_eq!(
            round(&mut alice, &alice_nick, &bob, &mesh),
            Some(0),
            "alice lacks nothing"
        );
        assert_eq!(
            bob.doc(Channel::Meta).heads(),
            alice.doc(Channel::Meta).heads(),
            "converged within one round"
        );
        assert_eq!(
            bob.doc(Channel::Meta).to_json(),
            alice.doc(Channel::Meta).to_json()
        );

        // Round 2 is a no-op: nothing left to serve either way.
        assert_eq!(round(&mut bob, &bob_nick, &alice, &mesh), Some(0));
        assert_eq!(round(&mut alice, &alice_nick, &bob, &mesh), Some(0));
    }

    /// The gate itself: a digest goes out when a real peer *or* the rendezvous
    /// relay is linked, and stays silent with no neighbor at all — or while
    /// degraded, when the overlay is suspected dead.
    #[test]
    fn state_digest_needs_some_gossip_path() {
        let mesh = MeshId::from("test");
        let author = nick("alice");
        let origin = DigestOrigin {
            mesh: &mesh,
            author: &author,
        };
        let mut state = fresh_state();
        state.meshed = false;
        state.rendezvous_linked = false;
        assert!(
            state_digest(&state, origin, Channel::Meta).is_none(),
            "no neighbor: nobody to advertise to"
        );

        state.rendezvous_linked = true;
        assert!(
            state_digest(&state, origin, Channel::Meta).is_some(),
            "a rendezvous-only link carries the digest"
        );

        state.note_degraded();
        assert!(
            state_digest(&state, origin, Channel::Meta).is_none(),
            "degraded: the relay link is no proof the overlay is alive"
        );

        state.rendezvous_linked = false;
        state.meshed = true;
        state.degraded = false;
        assert!(
            state_digest(&state, origin, Channel::State).is_some(),
            "meshed, as before"
        );
    }

    /// A full two-window digest must serialize within the gossip message
    /// cap — the regression guard for the former overflow (200 UUID
    /// *strings* ≈ 8 KB, ~2× over `MAX_MESSAGE_SIZE`, silently dropped by
    /// gossip).
    #[test]
    fn digest_fits_gossip_cap() {
        // Enough messages that both a newest and a rolling older window are
        // full (each `ANTIENTROPY_DIGEST_WINDOW_IDS` ids).
        let total = ANTIENTROPY_DIGEST_WINDOW_IDS * 3;
        let mut log = MessageLog::new(total);
        for index in 0..total {
            let mut message = Message::new_app(
                &MeshId::from("test"),
                &Nickname::from("author"),
                crate::protocol::message::AppFrameParams {
                    tag: crate::protocol::AppTag::from("app_msg"),
                    to: None,
                    corr: None,
                    body: MessageBody::from(format!("m{index}").as_str()),
                },
            );
            message.timestamp = 1_700_000_000 + i64::try_from(index).unwrap();
            log.push(message);
        }
        let newest = log.recent_window(ANTIENTROPY_DIGEST_WINDOW_IDS).unwrap();
        let older = log
            .older_window(
                ANTIENTROPY_DIGEST_WINDOW_IDS,
                0,
                ANTIENTROPY_DIGEST_WINDOW_IDS,
            )
            .unwrap();
        let digest_body = DigestBody {
            windows: vec![WireWindow::encode(&newest), WireWindow::encode(&older)],
        };
        let json = serde_json::to_string(&digest_body).unwrap();
        let body = MessageBody::new(json).expect("digest body has no control chars");

        // Worst-case envelope: a realistically long mesh id.
        let mesh = MeshId::from(
            "6bLvZNPGxuqnsbaPVGwf277NyTp8cYPCMiBxXED8d6TyBZpDDzZADkKHL7tTB1EjFagbCXYZ",
        );
        let digest = Message::new_digest(&mesh, &Nickname::from("a-fairly-long-nickname"), body);
        let wire = digest.serialize().expect("serialize digest");
        assert!(
            wire.len() <= crate::util::consts::MAX_MESSAGE_SIZE,
            "digest is {} bytes, over the {}-byte gossip cap",
            wire.len(),
            crate::util::consts::MAX_MESSAGE_SIZE
        );
    }

    /// The packed-id wire codec must round-trip exactly — a regression here
    /// would silently break cross-node reconciliation.
    #[test]
    fn wire_window_round_trips_ids() {
        let ids: Vec<[u8; 16]> = (0..5u8).map(|seed| [seed; 16]).collect();
        let window = DigestWindow {
            lo: 1,
            hi: i64::MAX,
            ids: ids.clone(),
        };
        let wire = WireWindow::encode(&window);
        let decoded = wire.decode_ids().expect("valid window decodes");
        assert_eq!(decoded, ids.into_iter().collect::<HashSet<_>>());
    }

    /// A malformed digest body must decode to `None` (so `handle_digest`
    /// skips it) rather than panic or yield garbage ids.
    #[test]
    fn decode_ids_rejects_malformed() {
        // Not valid Base58 (`0`, `O`, `I`, `l`, space are outside the alphabet).
        let bad_alphabet = WireWindow {
            lo: 0,
            hi: 0,
            ids: "0OIl not base58".to_string(),
        };
        assert!(bad_alphabet.decode_ids().is_none(), "bad Base58 ⇒ None");

        // Valid Base58 but not a whole number of 16-byte ids (5 bytes).
        let odd_length = WireWindow {
            lo: 0,
            hi: 0,
            ids: bs58::encode([1u8; 5]).into_string(),
        };
        assert!(odd_length.decode_ids().is_none(), "non-16-multiple ⇒ None");

        // Empty is well-formed: zero ids.
        let empty = WireWindow {
            lo: 0,
            hi: 0,
            ids: String::new(),
        };
        assert_eq!(empty.decode_ids().map(|set| set.len()), Some(0));
    }

    /// A state/meta heads digest round-trips and stays tiny — automerge heads are
    /// a bounded frontier (a handful of 32-byte hashes), so unlike the windowed
    /// chat digest there is no overflow risk as the doc's history grows.
    #[test]
    fn state_heads_digest_round_trips_and_is_small() {
        let mesh = MeshId::from(
            "6bLvZNPGxuqnsbaPVGwf277NyTp8cYPCMiBxXED8d6TyBZpDDzZADkKHL7tTB1EjFagbCXYZ",
        );
        let author = Nickname::from("a-fairly-long-nickname");
        let heads: Vec<String> = (0..4u8)
            .map(|seed| bs58::encode([seed; 32]).into_string())
            .collect();
        let json = serde_json::to_string(&HeadsBody {
            heads: heads.clone(),
        })
        .expect("serialize heads body");
        let back: HeadsBody = serde_json::from_str(&json).expect("round-trip");
        assert_eq!(back.heads, heads);

        let body = MessageBody::new(json).expect("heads body has no control chars");
        let digest = Message::new_state_digest(&mesh, &author, body);
        let wire = digest.serialize().expect("serialize state digest");
        assert!(
            wire.len() <= crate::util::consts::MAX_MESSAGE_SIZE,
            "heads digest is {} bytes, over the {}-byte gossip cap",
            wire.len(),
            crate::util::consts::MAX_MESSAGE_SIZE
        );
    }

    /// The full digest body survives a serde round-trip, preserving the
    /// open-ended (`i64::MAX`) upper bound that drives reconnect recovery.
    #[test]
    fn digest_body_serde_round_trips() {
        let body = DigestBody {
            windows: vec![
                WireWindow {
                    lo: 100,
                    hi: i64::MAX,
                    ids: bs58::encode([7u8; 16]).into_string(),
                },
                WireWindow {
                    lo: 10,
                    hi: 50,
                    // Two *distinct* 16-byte ids (identical halves would
                    // dedup to one in the decoded set).
                    ids: {
                        let mut raw = [0u8; 32];
                        raw[16..].fill(1);
                        bs58::encode(raw).into_string()
                    },
                },
            ],
        };
        let json = serde_json::to_string(&body).unwrap();
        let back: DigestBody = serde_json::from_str(&json).unwrap();
        assert_eq!(back.windows.len(), 2);
        assert_eq!(back.windows[0].hi, i64::MAX, "open-ended bound preserved");
        assert_eq!(back.windows[0].decode_ids().unwrap().len(), 1);
        assert_eq!(back.windows[1].decode_ids().unwrap().len(), 2);
    }
}
