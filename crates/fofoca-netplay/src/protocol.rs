//! One remote peer: handshake, input exchange, liveness.
//!
//! A session owns one of these per remote player. It is a pure state
//! machine — it never touches the transport itself, it produces outgoing
//! [`Message`]s for the session to send and consumes incoming ones. That
//! keeps it testable without a socket and keeps all the I/O in one place.
//!
//! Lifecycle: [`PeerState::Syncing`] until a handshake completes in both
//! directions, then [`PeerState::Running`] for the match, then
//! [`PeerState::Disconnected`] if the peer goes quiet past the timeout.

use crate::config::Config;
use crate::frame::{Frame, NULL_FRAME};
use crate::time_sync::{TimeSync, frame_advantage};
use crate::transport::{Body, InputPacket, Message};

/// Handshake round-trips before a peer is considered reachable. More than
/// one because a single reply only proves the path worked once.
const SYNC_ROUNDTRIPS: u32 = 5;

/// Ticks without hearing anything before a peer is declared gone.
///
/// Generous on purpose: a browser tab that is backgrounded has its
/// `requestAnimationFrame` suspended entirely, so a peer can legitimately go
/// silent for a long time and come back. Dropping them eagerly would end
/// matches that were merely paused.
const DISCONNECT_TIMEOUT_TICKS: u32 = 600;

/// Ticks between quality reports.
const QUALITY_REPORT_INTERVAL: u32 = 60;

/// Where a peer is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerState {
    /// Handshaking. No inputs are exchanged yet.
    Syncing,
    /// Handshake done; exchanging inputs.
    Running,
    /// Silent past the timeout.
    Disconnected,
}

/// Something worth telling the application about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PeerEvent {
    /// The handshake completed and this peer is now exchanging inputs.
    Synchronized,
    /// The peer went silent past the timeout.
    Disconnected,
    /// The peer's checksum for a frame disagrees with ours.
    Desync {
        /// The frame that disagreed.
        frame: Frame,
        /// Our checksum.
        local: u128,
        /// Theirs.
        remote: u128,
    },
}

/// One remote peer's connection.
pub(crate) struct PeerProtocol<T: Config> {
    pub(crate) addr: T::Address,
    /// Which player handle this peer drives.
    pub(crate) handle: usize,
    state: PeerState,
    /// Our session nonce, stamped on everything we send.
    magic: u32,
    /// Theirs, learned from the first packet; anything else is stale.
    remote_magic: Option<u32>,
    /// Handshake round-trips still owed.
    roundtrips_left: u32,
    /// Token echoed in the current outstanding sync request.
    sync_token: u32,
    /// Highest frame we have a real input from this peer for.
    last_received_frame: Frame,
    /// Highest frame *they* have acked from us — everything past this is
    /// resent for redundancy.
    last_acked_frame: Frame,
    /// Our own inputs not yet known to have arrived, oldest first.
    pending: Vec<(Frame, T::Input)>,
    /// Ticks since we last heard anything.
    silent_ticks: u32,
    /// Ticks since the last quality report went out.
    since_quality_report: u32,
    /// Their self-reported frame advantage.
    remote_advantage: i32,
    time_sync: TimeSync,
    /// Checksums we have advertised, so a mismatch can be attributed.
    local_checksums: Vec<(Frame, u128)>,
}

impl<T: Config> std::fmt::Debug for PeerProtocol<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PeerProtocol")
            .field("addr", &self.addr)
            .field("handle", &self.handle)
            .field("state", &self.state)
            .field("last_received_frame", &self.last_received_frame)
            .field("last_acked_frame", &self.last_acked_frame)
            .finish_non_exhaustive()
    }
}

impl<T: Config> PeerProtocol<T> {
    pub(crate) fn new(addr: T::Address, handle: usize, magic: u32) -> Self {
        Self {
            addr,
            handle,
            state: PeerState::Syncing,
            magic,
            remote_magic: None,
            roundtrips_left: SYNC_ROUNDTRIPS,
            // Derived from our own nonce so the first request is answerable
            // without any shared randomness.
            sync_token: magic ^ 0x5bf0_3635,
            last_received_frame: NULL_FRAME,
            last_acked_frame: NULL_FRAME,
            pending: Vec::new(),
            silent_ticks: 0,
            since_quality_report: 0,
            remote_advantage: 0,
            time_sync: TimeSync::default(),
            local_checksums: Vec::new(),
        }
    }

    pub(crate) fn state(&self) -> PeerState {
        self.state
    }

    pub(crate) fn last_received_frame(&self) -> Frame {
        self.last_received_frame
    }

    /// How many frames to skip so this peer can catch up; `0` to keep going.
    pub(crate) fn recommend_skip(&self) -> u32 {
        if self.state == PeerState::Running {
            self.time_sync.recommend_skip()
        } else {
            0
        }
    }

    /// Queue one of our own inputs for delivery.
    pub(crate) fn send_input(&mut self, frame: Frame, input: T::Input) {
        self.pending.push((frame, input));
    }

    /// Record a confirmed-state checksum and build the packet advertising
    /// it, so the peer can compare against its own.
    ///
    /// Records as well as sends: the comparison happens on whichever side
    /// receives the *other's* value, so both need their own on file.
    pub(crate) fn checksum_message(&mut self, frame: Frame, checksum: u128) -> Message<T::Input> {
        self.local_checksums.push((frame, checksum));
        // Only recent frames can still be contested.
        if self.local_checksums.len() > 64 {
            self.local_checksums.remove(0);
        }
        self.wrap(Body::Checksum { frame, checksum })
    }

    /// Everything to send this tick, and any events for the application.
    pub(crate) fn tick(
        &mut self,
        current_frame: Frame,
    ) -> (Vec<Message<T::Input>>, Vec<PeerEvent>) {
        let mut out = Vec::new();
        let mut events = Vec::new();

        self.silent_ticks = self.silent_ticks.saturating_add(1);
        if self.state != PeerState::Disconnected && self.silent_ticks > DISCONNECT_TIMEOUT_TICKS {
            self.state = PeerState::Disconnected;
            events.push(PeerEvent::Disconnected);
            return (out, events);
        }

        match self.state {
            PeerState::Syncing => out.push(self.wrap(Body::SyncRequest {
                token: self.sync_token,
            })),
            PeerState::Running => {
                out.extend(self.input_message());
                self.since_quality_report = self.since_quality_report.saturating_add(1);
                if self.since_quality_report >= QUALITY_REPORT_INTERVAL {
                    self.since_quality_report = 0;
                    let advantage = frame_advantage(current_frame, self.last_received_frame)
                        .clamp(i32::from(i8::MIN), i32::from(i8::MAX));
                    out.push(self.wrap(Body::QualityReport {
                        // Clamped to i8's range immediately above, so this
                        // conversion cannot fail.
                        frame_advantage: i8::try_from(advantage).unwrap_or(0),
                        ping: u64::from(self.silent_ticks),
                    }));
                }
                self.time_sync.advance(
                    frame_advantage(current_frame, self.last_received_frame),
                    self.remote_advantage,
                );
            }
            PeerState::Disconnected => {}
        }
        (out, events)
    }

    /// The input packet for this tick, if there is anything unacked.
    fn input_message(&mut self) -> Option<Message<T::Input>> {
        // Drop what the peer has confirmed; resend everything else.
        self.pending
            .retain(|(frame, _)| *frame > self.last_acked_frame);
        let start_frame = self.pending.first().map(|(frame, _)| *frame)?;
        let inputs: Vec<T::Input> = self.pending.iter().map(|(_, input)| *input).collect();
        Some(self.wrap(Body::Input(InputPacket {
            start_frame,
            ack_frame: self.last_received_frame,
            inputs,
        })))
    }

    fn wrap(&self, body: Body<T::Input>) -> Message<T::Input> {
        Message {
            magic: self.magic,
            body,
        }
    }

    /// Handle a packet from this peer.
    ///
    /// Returns any inputs to feed the rollback engine, as
    /// `(frame, input)` pairs, plus events for the application. Stale
    /// packets — wrong nonce — are dropped silently.
    pub(crate) fn on_message(
        &mut self,
        message: &Message<T::Input>,
        replies: &mut Vec<Message<T::Input>>,
    ) -> (Vec<(Frame, T::Input)>, Vec<PeerEvent>) {
        let mut inputs = Vec::new();
        let mut events = Vec::new();

        match self.remote_magic {
            Some(known) if known != message.magic => return (inputs, events),
            Some(_) => {}
            None => self.remote_magic = Some(message.magic),
        }
        self.silent_ticks = 0;

        match &message.body {
            Body::SyncRequest { token } => {
                replies.push(self.wrap(Body::SyncReply { token: *token }));
            }
            Body::SyncReply { token } => {
                if self.state == PeerState::Syncing && *token == self.sync_token {
                    self.roundtrips_left = self.roundtrips_left.saturating_sub(1);
                    // Fresh token each round so a duplicated reply cannot
                    // advance the handshake twice.
                    self.sync_token = self.sync_token.wrapping_mul(1_664_525).wrapping_add(1);
                    if self.roundtrips_left == 0 {
                        self.state = PeerState::Running;
                        events.push(PeerEvent::Synchronized);
                    }
                }
            }
            Body::Input(packet) => {
                self.last_acked_frame = self.last_acked_frame.max(packet.ack_frame);
                // A run that starts past the next frame we need would leave a
                // hole, and the rollback engine requires contiguity. Drop it
                // and wait — the sender resends everything unacked, so the
                // missing frames are already on their way.
                //
                // The *first* packet is exempt. A sender with input delay
                // files its first real input at frame `delay`, having filled
                // the frames below it with the default input, so its first
                // packet legitimately starts above zero. That is not a hole:
                // our own queue fills the same frames the same way through
                // the same code path, so both sides agree on them.
                if self.last_received_frame != NULL_FRAME
                    && packet.start_frame > self.last_received_frame + 1
                {
                    return (inputs, events);
                }
                for (offset, input) in packet.inputs.iter().enumerate() {
                    let frame = packet.start_frame + Frame::try_from(offset).unwrap_or(Frame::MAX);
                    // Only what we have not already filed — resends are the
                    // norm, not an anomaly.
                    if frame > self.last_received_frame {
                        inputs.push((frame, *input));
                    }
                }
                if let Some((frame, _)) = inputs.last() {
                    self.last_received_frame = *frame;
                }
                replies.push(self.wrap(Body::InputAck {
                    ack_frame: self.last_received_frame,
                }));
            }
            Body::InputAck { ack_frame } => {
                self.last_acked_frame = self.last_acked_frame.max(*ack_frame);
            }
            Body::QualityReport {
                frame_advantage,
                ping,
            } => {
                self.remote_advantage = i32::from(*frame_advantage);
                replies.push(self.wrap(Body::QualityReply { pong: *ping }));
            }
            Body::QualityReply { .. } | Body::KeepAlive => {}
            Body::Checksum { frame, checksum } => {
                if let Some((_, ours)) = self.local_checksums.iter().find(|(seen, _)| seen == frame)
                    && ours != checksum
                {
                    events.push(PeerEvent::Desync {
                        frame: *frame,
                        local: *ours,
                        remote: *checksum,
                    });
                }
            }
        }
        (inputs, events)
    }
}
