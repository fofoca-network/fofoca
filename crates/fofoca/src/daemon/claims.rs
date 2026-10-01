use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use n0_future::time::Instant as TokioInstant;

use crate::protocol::{Message, MessageKind, Nickname, PresenceSubtype};
use crate::util::clock::Instant;
use crate::util::tuning::{NICKNAME_ALONE_SECS, NICKNAME_ANSWER_SECS, alive_timeout_secs};

/// Where a member's nickname came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NicknameSource {
    /// Minted at random; no one else can be expected to hold it.
    Minted,
    /// Chosen by the caller, who may be rejoining under it or may have named a
    /// peer's.
    Chosen,
}

/// Who holds a nickname that this member asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NicknameHolder {
    /// Another key, on the mesh, holds it.
    Peer {
        /// The holder's signing key, lowercase hex.
        pubkey: String,
    },
    /// Another daemon on this machine answers on the control socket of that
    /// nickname.
    Local,
}

/// This member asked for a nickname that is held, and was refused it before it
/// reported ready. Typed so a caller can print one line and exit non-zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NicknameTaken {
    pub nickname: Nickname,
    pub holder: NicknameHolder,
}

impl fmt::Display for NicknameTaken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.holder {
            NicknameHolder::Peer { pubkey } => write!(
                formatter,
                "the nickname `{}` is held by another peer (key {})",
                self.nickname,
                pubkey.get(..8).unwrap_or(pubkey)
            ),
            NicknameHolder::Local => write!(
                formatter,
                "the nickname `{}` is held by another daemon on this machine",
                self.nickname
            ),
        }
    }
}

impl std::error::Error for NicknameTaken {}

/// Whether `ready` waits for the mesh to confirm the nickname.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NicknameCheck {
    /// Report ready as soon as the node can serve.
    Skip,
    /// Hold ready until a peer has had the time to answer.
    Confirm,
}

/// `ready` is held until the mesh has had time to say the nickname is taken.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReadyHold {
    pub(crate) deadline: TokioInstant,
    linked: bool,
}

impl ReadyHold {
    pub(crate) fn new(now: TokioInstant) -> Self {
        Self {
            deadline: now + Duration::from_secs(NICKNAME_ALONE_SECS),
            linked: false,
        }
    }

    /// The first link to a real peer: now a holder can answer, so the wait is
    /// one answer window from here.
    pub(crate) fn on_first_link(&mut self, now: TokioInstant) {
        if !self.linked {
            self.linked = true;
            self.deadline = now + Duration::from_secs(NICKNAME_ANSWER_SECS);
        }
    }
}

/// Who holds each nickname we have heard, as read off fresh frames.
///
/// A claim lasts while its key keeps sending, for the alive timeout, and ends
/// with its `left`. While it lasts, frames from any other key under that
/// nickname change nothing: not the roster, not what is shown, and above all
/// not the holder's own departure.
#[derive(Debug, Default)]
pub(crate) struct Claims {
    held: HashMap<Nickname, Claim>,
    rivals: HashMap<String, Rival>,
}

#[derive(Debug)]
struct Claim {
    pubkey: String,
    last_fresh: Instant,
}

/// A key other than ours that sends under our nickname.
#[derive(Debug)]
struct Rival {
    first_ts: i64,
    announced: bool,
    reported: bool,
}

/// What to do about a rival's frame.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RivalStep {
    /// First sighting of this rival: answer with a fresh `joined` of our own,
    /// so a joiner that is waiting to hear from the holder hears.
    pub(crate) announce: bool,
    /// The rival has kept sending past the answer window, so it is a holder
    /// too and not a joiner about to give up: report the conflict.
    pub(crate) report: bool,
}

/// Most claims and rivals kept. A flood of nicknames can only cost the
/// protection, never memory.
const CLAIMS_CAP: usize = 4096;
const RIVALS_CAP: usize = 64;

impl Claims {
    /// Whether a frame from `message.author` is let through to the roster and
    /// the surfaces. `fresh` says the frame is from after this process started.
    /// The caller exempts the channel kinds, which are never dropped.
    pub(crate) fn admit(&mut self, message: &Message, fresh: bool, now: Instant) -> bool {
        let alive = Duration::from_secs(alive_timeout_secs());
        let author = &message.author;
        if let Some(claim) = self.held.get_mut(author) {
            if claim.pubkey == message.pubkey {
                if fresh {
                    claim.last_fresh = now;
                }
                if is_left(&message.kind) {
                    self.held.remove(author);
                }
                return true;
            }
            if now.duration_since(claim.last_fresh) <= alive {
                return false;
            }
            // The holder went quiet past the timeout: the nickname is free.
            self.held.remove(author);
        }
        if fresh && !is_left(&message.kind) {
            if self.held.len() >= CLAIMS_CAP {
                self.held
                    .retain(|_, claim| now.duration_since(claim.last_fresh) <= alive);
                if self.held.len() >= CLAIMS_CAP {
                    self.held.clear();
                }
            }
            self.held.insert(
                author.clone(),
                Claim {
                    pubkey: message.pubkey.clone(),
                    last_fresh: now,
                },
            );
        }
        true
    }

    /// Note a fresh frame from another key under our own nickname.
    pub(crate) fn note_rival(&mut self, message: &Message) -> RivalStep {
        if self.rivals.len() >= RIVALS_CAP && !self.rivals.contains_key(&message.pubkey) {
            self.rivals.clear();
        }
        let rival = self.rivals.entry(message.pubkey.clone()).or_insert(Rival {
            first_ts: message.timestamp,
            announced: false,
            reported: false,
        });
        let mut step = RivalStep::default();
        if !rival.announced {
            rival.announced = true;
            step.announce = true;
        }
        let settled = message.timestamp
            >= rival
                .first_ts
                .saturating_add(i64::try_from(NICKNAME_ANSWER_SECS).unwrap_or(i64::MAX));
        if settled && !rival.reported {
            rival.reported = true;
            step.report = true;
        }
        step
    }
}

fn is_left(kind: &MessageKind) -> bool {
    matches!(
        kind,
        MessageKind::Presence {
            subtype: PresenceSubtype::Left
        }
    )
}

/// Whether the key `ours` keeps a nickname against `theirs` after a partition
/// heals: the lower key keeps it.
#[must_use]
pub(crate) fn we_lose_the_nickname(ours: &str, theirs: &str) -> bool {
    ours > theirs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::MeshId;
    use crate::protocol::identity::{Identity, encode_pubkey};

    const KEY_A: &str = "aa";
    const KEY_B: &str = "bb";

    fn frame(author: &str, pubkey: &str) -> Message {
        let mut message = Message::new_joined(&MeshId::from("test"), &Nickname::from(author));
        message.pubkey = pubkey.to_owned();
        message
    }

    fn left(author: &str, pubkey: &str) -> Message {
        let mut message = Message::new_left(&MeshId::from("test"), &Nickname::from(author));
        message.pubkey = pubkey.to_owned();
        message
    }

    #[test]
    fn the_first_fresh_frame_claims_the_nickname_and_another_key_is_then_dropped() {
        let mut claims = Claims::default();
        let now = Instant::now();
        assert!(claims.admit(&frame("alice", KEY_A), true, now));
        assert!(
            !claims.admit(&frame("alice", KEY_B), true, now),
            "second key"
        );
        assert!(
            !claims.admit(&frame("alice", KEY_B), false, now),
            "an old frame of the second key fares no better"
        );
        assert!(
            claims.admit(&frame("alice", KEY_A), true, now),
            "the holder"
        );
        assert!(
            claims.admit(&frame("bob", KEY_B), true, now),
            "another nickname"
        );
    }

    #[test]
    fn a_frame_from_before_we_started_claims_nothing() {
        let mut claims = Claims::default();
        let now = Instant::now();
        assert!(
            claims.admit(&frame("alice", KEY_A), false, now),
            "processed"
        );
        assert!(
            claims.admit(&frame("alice", KEY_B), true, now),
            "backlog held no claim, so a fresh key may take the nickname"
        );
        assert!(!claims.admit(&frame("alice", KEY_A), true, now));
    }

    #[test]
    fn the_holders_left_frees_the_nickname_and_anothers_left_does_not() {
        let mut claims = Claims::default();
        let now = Instant::now();
        assert!(claims.admit(&frame("alice", KEY_A), true, now));
        assert!(
            !claims.admit(&left("alice", KEY_B), true, now),
            "another key cannot send the holder away"
        );
        assert!(
            claims.admit(&frame("alice", KEY_A), true, now),
            "still held"
        );
        assert!(
            claims.admit(&left("alice", KEY_A), true, now),
            "the holder leaves"
        );
        assert!(
            claims.admit(&frame("alice", KEY_B), true, now),
            "the nickname is free"
        );
    }

    #[test]
    fn a_claim_lapses_when_its_key_goes_quiet_past_the_timeout() {
        let mut claims = Claims::default();
        let start = Instant::now();
        assert!(claims.admit(&frame("alice", KEY_A), true, start));
        let later = start + Duration::from_secs(alive_timeout_secs() + 1);
        assert!(
            claims.admit(&frame("alice", KEY_B), true, later),
            "the holder was silent past the timeout"
        );
        assert!(!claims.admit(&frame("alice", KEY_A), true, later));
    }

    #[test]
    fn a_rival_is_answered_once_and_reported_once_after_the_answer_window() {
        let mut claims = Claims::default();
        let mut first = frame("alice", KEY_B);
        first.timestamp = 1_000;
        assert_eq!(
            claims.note_rival(&first),
            RivalStep {
                announce: true,
                report: false
            },
            "a newcomer is answered, not yet a conflict"
        );
        let mut early = frame("alice", KEY_B);
        early.timestamp = 1_001;
        assert_eq!(claims.note_rival(&early), RivalStep::default());
        let mut settled = frame("alice", KEY_B);
        settled.timestamp = 1_000 + i64::try_from(NICKNAME_ANSWER_SECS).expect("small");
        assert_eq!(
            claims.note_rival(&settled),
            RivalStep {
                announce: false,
                report: true
            },
            "it kept sending past the window, so it is a holder too"
        );
        assert_eq!(claims.note_rival(&settled), RivalStep::default(), "once");
    }

    #[test]
    fn a_rival_that_goes_away_inside_the_window_is_never_reported() {
        let mut claims = Claims::default();
        let mut only = frame("alice", KEY_B);
        only.timestamp = 1_000;
        let step = claims.note_rival(&only);
        assert!(step.announce && !step.report);
    }

    #[test]
    fn the_lower_key_keeps_the_nickname() {
        let (low, high) = (
            encode_pubkey(&Identity::from_secret_bytes([1; 32]).public()),
            encode_pubkey(&Identity::from_secret_bytes([2; 32]).public()),
        );
        let (low, high) = if low < high { (low, high) } else { (high, low) };
        assert!(!we_lose_the_nickname(&low, &high));
        assert!(we_lose_the_nickname(&high, &low));
    }

    #[test]
    fn the_ready_hold_runs_to_the_alone_deadline_until_a_first_link() {
        let start = TokioInstant::now();
        let mut hold = ReadyHold::new(start);
        assert_eq!(
            hold.deadline,
            start + Duration::from_secs(NICKNAME_ALONE_SECS)
        );
        let linked = start + Duration::from_secs(2);
        hold.on_first_link(linked);
        assert_eq!(
            hold.deadline,
            linked + Duration::from_secs(NICKNAME_ANSWER_SECS)
        );
        hold.on_first_link(linked + Duration::from_secs(1));
        assert_eq!(
            hold.deadline,
            linked + Duration::from_secs(NICKNAME_ANSWER_SECS),
            "only the first link counts"
        );
    }

    #[test]
    fn the_refusal_names_the_nickname_and_the_holder_in_one_line() {
        let peer = NicknameTaken {
            nickname: Nickname::from("alice"),
            holder: NicknameHolder::Peer {
                pubkey: "0123456789abcdef".to_owned(),
            },
        };
        assert_eq!(
            peer.to_string(),
            "the nickname `alice` is held by another peer (key 01234567)"
        );
        let local = NicknameTaken {
            nickname: Nickname::from("alice"),
            holder: NicknameHolder::Local,
        };
        assert!(local.to_string().contains("another daemon on this machine"));
        assert!(!local.to_string().contains('\n'));
    }
}
