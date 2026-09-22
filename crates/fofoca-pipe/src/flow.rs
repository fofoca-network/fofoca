//! Flow control: a sender does not outrun its receivers.
//!
//! Gossip is fire-and-forget and every hop has a queue that drops when full
//! — the gossip subscription (`Lagged`), the pipe's own inbound queue
//! ([`INBOUND_CAP`](crate::INBOUND_CAP)), the data channel's send buffer.
//! A 1 MB stream fired at a browser tab lost frames at every one of them.
//! So a receiver acknowledges what it has received (a `pipe_ack` frame back
//! to the author every [`ACK_EVERY`] frames and at EOF), and a sender waits
//! while more than [`WINDOW`] frames are unacknowledged by any receiver it
//! expects an ack from.
//!
//! Expected receivers are the peers with a proven payload lane at the time
//! of the send — the ones that can send a directed ack at all. A receiver
//! that goes silent for [`STALL_TIMEOUT`] is dropped from the expectation
//! until it acks again, so a dead or old-wire peer cannot stall a stream.
//!
//! The driver keeps the counters; consumers call [`Flow::wait_for_window`]
//! before each frame. Nothing here touches the event loop's timing.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use fofoca::protocol::Nickname;
use tokio::sync::watch;

/// Frames a sender may have in flight, unacknowledged, per receiver.
pub const WINDOW: u64 = 64;
/// A receiver acks every this many frames of a stream (and at its EOF).
pub const ACK_EVERY: u64 = 8;
/// How long a sender waits for ack progress before it stops expecting acks
/// from the receivers that stayed silent.
pub const STALL_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Default)]
struct StreamFlow {
    sent: u64,
    /// Frames each receiver has reported, by nickname.
    acked: HashMap<Nickname, u64>,
    /// Who the last send expected an ack from.
    expected: Vec<Nickname>,
    /// Receivers that stalled; not expected until they ack again.
    silent: HashSet<Nickname>,
}

impl StreamFlow {
    /// Frames sent beyond what every expected receiver has acked.
    fn in_flight(&self) -> u64 {
        let floor = self
            .expected
            .iter()
            .filter(|nick| !self.silent.contains(*nick))
            .map(|nick| self.acked.get(nick).copied().unwrap_or(0))
            .min()
            .unwrap_or(self.sent);
        self.sent.saturating_sub(floor)
    }
}

/// The per-stream counters, shared between the driver and the consumers.
#[derive(Debug)]
pub struct Flow {
    streams: Mutex<HashMap<Option<Nickname>, StreamFlow>>,
    /// Bumped on every change a waiter could care about.
    changed: watch::Sender<u64>,
}

impl Flow {
    #[must_use]
    pub fn new() -> Arc<Self> {
        let (changed, _) = watch::channel(0);
        Arc::new(Self {
            streams: Mutex::new(HashMap::new()),
            changed,
        })
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<Option<Nickname>, StreamFlow>> {
        self.streams.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn bump(&self) {
        self.changed.send_modify(|epoch| *epoch += 1);
    }

    /// A data frame went out on the stream to `to`; `expected` are the
    /// receivers with a payload lane right now.
    pub(crate) fn note_sent(&self, to: Option<&Nickname>, expected: Vec<Nickname>) {
        let mut streams = self.lock();
        let stream = streams.entry(to.cloned()).or_default();
        stream.sent += 1;
        stream.expected = expected;
    }

    /// `from` has received `received` frames of our stream — the broadcast
    /// one, or the one directed at them.
    pub(crate) fn note_ack(&self, from: &Nickname, directed: bool, received: u64) {
        let key = directed.then(|| from.clone());
        {
            let mut streams = self.lock();
            let stream = streams.entry(key).or_default();
            let acked = stream.acked.entry(from.clone()).or_default();
            *acked = (*acked).max(received);
            stream.silent.remove(from);
        }
        self.bump();
    }

    /// `nick` left: nothing is expected from them any more.
    pub(crate) fn note_left(&self, nick: &Nickname) {
        {
            let mut streams = self.lock();
            for stream in streams.values_mut() {
                stream.acked.remove(nick);
                stream.expected.retain(|expected| expected != nick);
                stream.silent.remove(nick);
            }
        }
        self.bump();
    }

    /// Frames in flight on the stream to `to`.
    #[must_use]
    pub fn in_flight(&self, to: &Option<Nickname>) -> u64 {
        self.lock().get(to).map_or(0, StreamFlow::in_flight)
    }

    /// Stop expecting acks from the receivers of `to` that have not acked
    /// past the current floor. Returns how many were silenced.
    fn give_up(&self, to: Option<&Nickname>) -> usize {
        let mut streams = self.lock();
        let Some(stream) = streams.get_mut(&to.cloned()) else {
            return 0;
        };
        let floor = stream.sent.saturating_sub(WINDOW);
        let mut silenced = 0;
        for nick in &stream.expected {
            if stream.acked.get(nick).copied().unwrap_or(0) <= floor
                && stream.silent.insert(nick.clone())
            {
                silenced += 1;
            }
        }
        silenced
    }

    /// Wait until fewer than [`WINDOW`] frames are in flight on the stream to
    /// `to`. `false` when the wait ended by giving up on silent receivers
    /// instead — the send may go on, unpaced against them.
    pub async fn wait_for_window(&self, to: &Option<Nickname>) -> bool {
        loop {
            let mut changed = self.changed.subscribe();
            changed.mark_unchanged();
            if self.in_flight(to) < WINDOW {
                return true;
            }
            let progressed = n0_future::time::timeout(STALL_TIMEOUT, changed.changed()).await;
            if progressed.is_err() && self.in_flight(to) >= WINDOW {
                let silenced = self.give_up(to.as_ref());
                if silenced > 0 {
                    tracing::warn!(
                        target: "fofoca::messages",
                        silenced,
                        "pipe receivers stopped acking; streaming on without them"
                    );
                }
                return false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nick(name: &str) -> Nickname {
        Nickname::new(name.to_owned()).unwrap()
    }

    #[test]
    fn nothing_expected_means_nothing_in_flight() {
        let flow = Flow::new();
        for _ in 0..3 {
            flow.note_sent(None, Vec::new());
        }
        assert_eq!(flow.in_flight(&None), 0);
    }

    #[test]
    fn the_slowest_expected_receiver_sets_the_floor() {
        let flow = Flow::new();
        let (ana, bo) = (nick("ana"), nick("bo"));
        for _ in 0..10 {
            flow.note_sent(None, vec![ana.clone(), bo.clone()]);
        }
        assert_eq!(flow.in_flight(&None), 10);
        flow.note_ack(&ana, false, 8);
        assert_eq!(flow.in_flight(&None), 10, "bo has acked nothing");
        flow.note_ack(&bo, false, 4);
        assert_eq!(flow.in_flight(&None), 6);
        flow.note_ack(&bo, false, 2);
        assert_eq!(
            flow.in_flight(&None),
            6,
            "an older ack never lowers a count"
        );
    }

    #[test]
    fn a_directed_stream_is_paced_by_its_addressee_alone() {
        let flow = Flow::new();
        let (ana, bo) = (nick("ana"), nick("bo"));
        for _ in 0..5 {
            flow.note_sent(Some(&ana), vec![ana.clone()]);
        }
        flow.note_ack(&bo, true, 5);
        assert_eq!(
            flow.in_flight(&Some(ana.clone())),
            5,
            "bo's ack is for bo's stream"
        );
        flow.note_ack(&ana, true, 5);
        assert_eq!(flow.in_flight(&Some(ana)), 0);
        assert_eq!(
            flow.in_flight(&None),
            0,
            "the broadcast stream is untouched"
        );
    }

    #[test]
    fn a_receiver_that_left_is_no_longer_expected() {
        let flow = Flow::new();
        let (ana, bo) = (nick("ana"), nick("bo"));
        for _ in 0..4 {
            flow.note_sent(None, vec![ana.clone(), bo.clone()]);
        }
        flow.note_ack(&ana, false, 4);
        assert_eq!(flow.in_flight(&None), 4);
        flow.note_left(&bo);
        assert_eq!(flow.in_flight(&None), 0);
    }

    #[test]
    fn giving_up_silences_only_the_receivers_behind_the_window() {
        let flow = Flow::new();
        let (ana, bo) = (nick("ana"), nick("bo"));
        for _ in 0..(WINDOW + 10) {
            flow.note_sent(None, vec![ana.clone(), bo.clone()]);
        }
        flow.note_ack(&ana, false, WINDOW + 8);
        assert_eq!(flow.in_flight(&None), WINDOW + 10);
        assert_eq!(flow.give_up(None), 1);
        assert_eq!(flow.in_flight(&None), 2, "paced by ana alone now");
        // bo acks again and is expected again.
        flow.note_ack(&bo, false, 3);
        assert_eq!(flow.in_flight(&None), WINDOW + 7);
    }
}
