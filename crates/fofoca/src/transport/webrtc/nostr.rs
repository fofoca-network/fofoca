//! JSEP over Nostr: the same offer and answer as the signal ALPN, carried as
//! sealed, signed events on public relays instead of over an iroh connection.
//!
//! It exists for the mesh with no iroh relay, where the ALPN has nothing to
//! ride on: two peers that have never connected cannot open a connection to
//! exchange the SDP that would let them connect.
//!
//! Identity is the one thing the ALPN got for free and Nostr does not: there
//! the remote id came from the TLS handshake, here it comes from `from`, which
//! [`Opener::open`] has checked against the sender's endpoint signature before
//! anything below reads it.
//!
//! Who offers is the caller's rule, as for the ALPN (the lower id, in
//! `negotiate_session`). Here two peers offering to each other at once each
//! refuse the other as `InFlight` and both rounds run to their deadline.
#![expect(
    dead_code,
    reason = "the discovery service wires this into the event loop; drop this then"
)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::protocol::nostr::{NostrKeys, Opener, Signal, SignalKind, seal};
use anyhow::{Context as _, Result};
use fofoca_iroh_webrtc_transport::{SignalEnvelope, WebRtcHandle};
use fofoca_nostr::Pool;
use iroh::{Endpoint, EndpointId, SecretKey};
use tokio::sync::{mpsc, oneshot};
use url::Url;

use super::super::LOG_TARGET;
use super::super::admission::{Refusal, SignalAdmission};
use super::{
    CapRefused, IceProfile, SignalDeadlines, build_answer, build_offer, register_session_addr,
};

/// Relays a node holds while it has no link, so a newcomer's first events
/// reach the relays incumbents listen on.
pub(crate) const JOINING_WIDTH: usize = 3;

/// The Nostr signalling carrier for one mesh.
///
/// Dropping the last handle does not cut an answer short: each answer task
/// holds the carrier until its round ends, bounded by the round deadline and
/// abortable through admission.
#[derive(Clone)]
pub(crate) struct NostrSignal {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for NostrSignal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NostrSignal")
            .field("local", &self.inner.local)
            .finish_non_exhaustive()
    }
}

struct Inner {
    pool: Pool,
    keys: NostrKeys,
    secret: SecretKey,
    local: EndpointId,
    endpoint: Endpoint,
    handle: WebRtcHandle,
    admission: SignalAdmission,
    ice: IceProfile,
    deadlines: SignalDeadlines,
    /// Offer rounds waiting for their answer.
    replies: Replies,
    _dispatch: n0_future::task::AbortOnDropHandle<()>,
}

/// What a [`NostrSignal`] needs from the node it serves.
pub(crate) struct NostrSignalParts {
    pub(crate) endpoint: Endpoint,
    pub(crate) handle: WebRtcHandle,
    pub(crate) admission: SignalAdmission,
    pub(crate) ice: IceProfile,
    pub(crate) keys: NostrKeys,
    /// Ranked the same way by every member (`NostrKeys::ranked_relays`).
    pub(crate) relays: Vec<Url>,
}

impl NostrSignal {
    /// Subscribe to the room and to our own tag, and start answering offers.
    /// `Hello`s are handed to the returned receiver: discovery is the caller's.
    pub(crate) fn start(parts: NostrSignalParts) -> (Self, mpsc::UnboundedReceiver<Signal>) {
        Self::start_with(parts, SignalDeadlines::DEFAULT)
    }

    pub(crate) fn start_with(
        parts: NostrSignalParts,
        deadlines: SignalDeadlines,
    ) -> (Self, mpsc::UnboundedReceiver<Signal>) {
        let local = parts.endpoint.id();
        let secret = parts.endpoint.secret_key().clone();
        let (pool, events) = Pool::open(
            parts.relays,
            JOINING_WIDTH,
            &[parts.keys.room_tag(), parts.keys.self_tag(&local)],
        );
        let (hellos_tx, hellos) = mpsc::unbounded_channel();
        let inner = Arc::new_cyclic(|weak: &std::sync::Weak<Inner>| {
            let dispatch = n0_future::task::spawn(dispatch(
                weak.clone(),
                events,
                Opener::new(parts.keys.clone(), local),
                hellos_tx,
            ));
            Inner {
                pool,
                keys: parts.keys,
                secret,
                local,
                endpoint: parts.endpoint,
                handle: parts.handle,
                admission: parts.admission,
                ice: parts.ice,
                deadlines,
                replies: Replies::default(),
                _dispatch: n0_future::task::AbortOnDropHandle::new(dispatch),
            }
        });
        (Self { inner }, hellos)
    }

    /// Publish a signal from us. `to` set ⇒ on the addressee's own tag,
    /// otherwise on the room tag.
    pub(crate) fn send(&self, signal: &Signal) {
        let tag = signal.to.map_or_else(
            || self.inner.keys.room_tag(),
            |to| self.inner.keys.self_tag(&to),
        );
        self.inner
            .pool
            .publish(&tag, seal(&self.inner.keys, &self.inner.secret, signal));
    }

    /// A signal from us, stamped now.
    pub(crate) fn signal(&self, kind: SignalKind) -> Signal {
        Signal::new(kind, self.inner.local, now())
    }

    /// How many relays to hold; see `Pool::set_width`.
    pub(crate) fn set_width(&self, width: usize) {
        self.inner.pool.set_width(width);
    }

    /// Offer a session to `peer` over Nostr and attach it. The caller holds
    /// the admission slot for the round, as for the ALPN.
    ///
    /// # Errors
    /// No answer before the deadline, a refusal ([`CapRefused`]), or a
    /// negotiation that does not complete.
    pub(crate) async fn offer(&self, peer: EndpointId) -> Result<()> {
        let inner = &self.inner;
        match n0_future::time::timeout(inner.deadlines.round, Box::pin(self.offer_round(peer)))
            .await
        {
            Ok(result) => result,
            Err(_elapsed) => {
                anyhow::bail!(
                    "nostr signalling with {peer} exceeded {:?}",
                    inner.deadlines.round
                )
            }
        }
    }

    async fn offer_round(&self, peer: EndpointId) -> Result<()> {
        let inner = &self.inner;
        let offer = build_offer(inner.local, &inner.handle, inner.ice).await?;
        let mut signal = self.signal(SignalKind::Offer);
        signal.to = Some(peer);
        signal.envelope = Some(serde_json::to_string(offer.envelope())?);
        // Registered before the offer goes out, so the answer cannot beat it.
        let (_pending, reply) = expect_reply(&inner.replies, signal.nonce, peer);
        self.send(&signal);
        let reply = n0_future::time::timeout(inner.deadlines.exchange, reply)
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "no answer from {peer} within {:?}",
                    inner.deadlines.exchange
                )
            })?
            .context("offer round dropped")?;
        if reply.kind == SignalKind::Refuse {
            return Err(anyhow::Error::new(CapRefused(peer)));
        }
        let answer: SignalEnvelope = serde_json::from_str(
            reply
                .envelope
                .as_deref()
                .context("an answer with no envelope")?,
        )
        .context("parse signal answer")?;
        offer
            .with_answer(answer)
            .complete(peer, &inner.handle)
            .await?;
        register_session_addr(&inner.endpoint, peer);
        tracing::debug!(target: LOG_TARGET, %peer, "webrtc session attached (offerer, nostr)");
        Ok(())
    }
}

/// Offer rounds waiting for their reply: the offer's nonce, the peer it went
/// to, and where to hand the reply.
type Replies = Arc<Mutex<HashMap<[u8; 16], (EndpointId, oneshot::Sender<Signal>)>>>;

/// A registered wait. Dropping it forgets the wait, however the round ends,
/// including when the round's own deadline cancels it mid-wait.
struct Pending {
    replies: Replies,
    nonce: [u8; 16],
}

impl Drop for Pending {
    fn drop(&mut self) {
        self.replies
            .lock()
            .expect("replies map poisoned")
            .remove(&self.nonce);
    }
}

fn expect_reply(
    replies: &Replies,
    nonce: [u8; 16],
    peer: EndpointId,
) -> (Pending, oneshot::Receiver<Signal>) {
    let (sender, receiver) = oneshot::channel();
    replies
        .lock()
        .expect("replies map poisoned")
        .insert(nonce, (peer, sender));
    let pending = Pending {
        replies: Arc::clone(replies),
        nonce,
    };
    (pending, receiver)
}

/// Hand a reply to the round waiting on its `re`, if it came from the peer
/// that round offered to. Checked before the wait is taken: every member can
/// read an offer's nonce off the addressee's tag, and a wait consumed by the
/// wrong sender would end the round before the real answer lands.
fn deliver_reply(replies: &Replies, reply: Signal) {
    let Some(nonce) = reply.re else { return };
    let mut waiting = replies.lock().expect("replies map poisoned");
    if waiting
        .get(&nonce)
        .is_none_or(|(peer, _)| *peer != reply.from)
    {
        return;
    }
    if let Some((_peer, sender)) = waiting.remove(&nonce) {
        let _ = sender.send(reply);
    }
}

fn now() -> u64 {
    u64::try_from(crate::util::clock::unix_secs()).unwrap_or(0)
}

/// Open every event the pool delivers and route it: replies to the round
/// waiting on them, offers to an answer, hellos to the caller.
async fn dispatch(
    inner: std::sync::Weak<Inner>,
    mut events: mpsc::UnboundedReceiver<fofoca_nostr::Event>,
    mut opener: Opener,
    hellos: mpsc::UnboundedSender<Signal>,
) {
    while let Some(event) = events.recv().await {
        let signal = match opener.open(&event.content, now()) {
            Ok(signal) => signal,
            Err(error) => {
                tracing::debug!(target: LOG_TARGET, %error, "dropped a nostr signal");
                continue;
            }
        };
        let Some(inner) = inner.upgrade() else { return };
        match signal.kind {
            SignalKind::Hello => {
                let _ = hellos.send(signal);
            }
            SignalKind::Answer | SignalKind::Refuse => {
                deliver_reply(&inner.replies, signal);
            }
            SignalKind::Offer => answer(&inner, signal),
        }
    }
}

/// Answer an offer: the ALPN acceptor's rules, with `from` in place of the
/// TLS-proven id.
fn answer(inner: &Arc<Inner>, offer: Signal) {
    let remote = offer.from;
    // A signed re-offer from a peer we hold a session with means its half is
    // gone; see `WebRtcSignalAcceptor::accept`. Safe on the signature alone.
    if inner.handle.has_session(&remote) && inner.handle.detach(&remote) {
        tracing::debug!(target: LOG_TARGET, %remote, "detached a half-dead session on a fresh nostr offer");
    }
    let guard = match inner.admission.try_admit(remote, &inner.handle) {
        Ok(guard) => guard,
        Err(reason) => {
            // Self-limiting, though it looks like an amplifier: the offerer
            // notes the refusal and cools off, so one offer earns one Refuse.
            if reason == Refusal::AtCap {
                let mut refuse = Signal::new(SignalKind::Refuse, inner.local, now());
                refuse.to = Some(remote);
                refuse.re = Some(offer.nonce);
                publish(inner, &refuse);
            }
            tracing::debug!(target: LOG_TARGET, %remote, ?reason, "refused a nostr offer");
            return;
        }
    };
    let this = Arc::clone(inner);
    let task = n0_future::task::spawn(async move {
        let _guard = guard;
        let round = async {
            let envelope: SignalEnvelope = serde_json::from_str(
                offer
                    .envelope
                    .as_deref()
                    .context("an offer with no envelope")?,
            )
            .context("parse signal offer")?;
            let answer = build_answer(this.local, &envelope, this.ice).await?;
            let mut reply = Signal::new(SignalKind::Answer, this.local, now());
            reply.to = Some(remote);
            reply.re = Some(offer.nonce);
            reply.envelope = Some(serde_json::to_string(answer.envelope())?);
            // The answer goes out before we complete: the offerer cannot
            // finish ICE without our SDP. The same rule as `answer_one`, whose
            // tail this mirrors; change the two together.
            publish(&this, &reply);
            answer.complete(remote, &this.handle).await
        };
        match n0_future::time::timeout(this.deadlines.round, Box::pin(round)).await {
            Ok(Ok(())) => {
                register_session_addr(&this.endpoint, remote);
                tracing::debug!(target: LOG_TARGET, %remote, "webrtc session attached (answerer, nostr)");
            }
            Ok(Err(error)) => {
                tracing::debug!(target: LOG_TARGET, %remote, %error, "nostr answer failed");
            }
            Err(_elapsed) => {
                tracing::debug!(target: LOG_TARGET, %remote, "nostr answer timed out");
            }
        }
    });
    inner.admission.track(remote, task.abort_handle());
}

fn publish(inner: &Inner, signal: &Signal) {
    let tag = signal
        .to
        .map_or_else(|| inner.keys.room_tag(), |to| inner.keys.self_tag(&to));
    inner
        .pool
        .publish(&tag, seal(&inner.keys, &inner.secret, signal));
}

// Host-only: real endpoints with only the WebRTC transport (no IP, no relay)
// and an in-process Nostr relay.
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fofoca_iroh_webrtc_transport::WebRtcTransport;
    use fofoca_nostr::test_relay::TestRelay;
    use iroh::endpoint::presets;
    use iroh::{EndpointAddr, RelayMode, TransportAddr};

    use super::*;
    use crate::protocol::TopicId;
    use crate::transport::MAX_DIRECT_PEERS;

    const ECHO_ALPN: &[u8] = b"test/nostr-echo";

    fn quick() -> SignalDeadlines {
        SignalDeadlines {
            exchange: Duration::from_secs(8),
            round: Duration::from_secs(20),
        }
    }

    /// An endpoint that can reach nothing but a `WebRTC` session: no IP, no
    /// relay, no address lookup.
    async fn webrtc_only() -> (Endpoint, WebRtcHandle) {
        let key = SecretKey::generate();
        let handle = WebRtcHandle::new(WebRtcTransport::new(key.public()));
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(key)
            .relay_mode(RelayMode::Disabled)
            .clear_ip_transports()
            .clear_address_lookup()
            .add_custom_transport(handle.transport())
            .alpns(vec![ECHO_ALPN.to_vec()])
            .bind()
            .await
            .expect("bind a webrtc-only endpoint");
        (endpoint, handle)
    }

    struct Node {
        endpoint: Endpoint,
        handle: WebRtcHandle,
        admission: SignalAdmission,
        signal: NostrSignal,
    }

    async fn node(relay: &TestRelay, topic: [u8; 32], cap: usize) -> Node {
        let (endpoint, handle) = webrtc_only().await;
        let admission = SignalAdmission::new(cap);
        let (signal, _hellos) = NostrSignal::start_with(
            NostrSignalParts {
                endpoint: endpoint.clone(),
                handle: handle.clone(),
                admission: admission.clone(),
                ice: IceProfile { host_only: true },
                keys: NostrKeys::derive(&TopicId::from_bytes(topic)),
                relays: vec![relay.url()],
            },
            quick(),
        );
        Node {
            endpoint,
            handle,
            admission,
            signal,
        }
    }

    /// Wait until both pools hold the relay, so the first offer is not
    /// published before the answerer subscribes.
    async fn settled(relay: &TestRelay) {
        for _ in 0..100 {
            if relay.connections() >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    async fn offer(from: &Node, to: &Node) -> Result<()> {
        let peer = to.endpoint.id();
        let _guard = from
            .admission
            .try_admit(peer, &from.handle)
            .map_err(|reason| anyhow::anyhow!("not admitted: {reason:?}"))?;
        from.signal.offer(peer).await
    }

    /// Bytes over QUIC on the session, the only path either endpoint has.
    async fn echo(from: &Node, to: &Node) {
        let server = to.endpoint.clone();
        let accept = tokio::spawn(async move {
            let conn = server
                .accept()
                .await
                .expect("incoming")
                .await
                .expect("accept");
            let (mut send, mut recv) = conn.accept_bi().await.expect("stream");
            let bytes = recv.read_to_end(64).await.expect("read");
            send.write_all(&bytes).await.expect("write");
            send.finish().expect("finish");
            conn.closed().await;
        });
        let addr = EndpointAddr::from_parts(
            to.endpoint.id(),
            [TransportAddr::Custom(
                fofoca_iroh_webrtc_transport::custom_addr(to.endpoint.id()),
            )],
        );
        let conn = from
            .endpoint
            .connect(addr, ECHO_ALPN)
            .await
            .expect("connect over webrtc");
        let (mut send, mut recv) = conn.open_bi().await.expect("open");
        send.write_all(b"over nostr-signalled webrtc")
            .await
            .expect("write");
        send.finish().expect("finish");
        assert_eq!(
            recv.read_to_end(64).await.expect("read"),
            b"over nostr-signalled webrtc"
        );
        conn.close(0u32.into(), b"done");
        accept.abort();
    }

    fn reply(kind: SignalKind, from: EndpointId, re: [u8; 16]) -> Signal {
        let mut signal = Signal::new(kind, from, now());
        signal.re = Some(re);
        signal
    }

    /// Every member can read an offer's nonce off the addressee's tag. A reply
    /// from anyone but the addressee must not consume the wait, or one signed
    /// reply from any member ends any round.
    #[test]
    fn a_reply_from_a_third_member_does_not_take_the_wait() {
        let replies = Replies::default();
        let bob = SecretKey::generate().public();
        let mallory = SecretKey::generate().public();
        let (_pending, mut reply_rx) = expect_reply(&replies, [7u8; 16], bob);

        deliver_reply(&replies, reply(SignalKind::Answer, mallory, [7u8; 16]));
        assert!(reply_rx.try_recv().is_err(), "mallory's reply is ignored");

        deliver_reply(&replies, reply(SignalKind::Answer, bob, [7u8; 16]));
        assert_eq!(reply_rx.try_recv().expect("bob's reply lands").from, bob);
    }

    /// However the round ends, its wait is forgotten.
    #[test]
    fn a_dropped_wait_is_forgotten() {
        let replies = Replies::default();
        let (pending, _reply_rx) =
            expect_reply(&replies, [8u8; 16], SecretKey::generate().public());
        drop(pending);
        assert!(replies.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_nostr_round_attaches_a_session_with_no_iroh_path() {
        let relay = TestRelay::spawn().await.unwrap();
        let alice = node(&relay, [1u8; 32], MAX_DIRECT_PEERS).await;
        let bob = node(&relay, [1u8; 32], MAX_DIRECT_PEERS).await;
        settled(&relay).await;

        offer(&alice, &bob).await.expect("the nostr round attaches");
        assert!(alice.handle.has_session(&bob.endpoint.id()));
        for _ in 0..50 {
            if bob.handle.has_session(&alice.endpoint.id()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(bob.handle.has_session(&alice.endpoint.id()));
        echo(&alice, &bob).await;
    }

    /// Alice lost her half; her signed re-offer must replace Bob's stale half
    /// rather than be refused as "already have a session".
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_signed_reoffer_replaces_a_half_dead_session() {
        let relay = TestRelay::spawn().await.unwrap();
        let alice = node(&relay, [2u8; 32], MAX_DIRECT_PEERS).await;
        let bob = node(&relay, [2u8; 32], MAX_DIRECT_PEERS).await;
        settled(&relay).await;
        offer(&alice, &bob).await.expect("first round");
        for _ in 0..50 {
            if bob.handle.has_session(&alice.endpoint.id()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        assert!(alice.handle.detach(&bob.endpoint.id()));
        offer(&alice, &bob).await.expect("the re-offer attaches");
        echo(&alice, &bob).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_answerer_at_its_cap_refuses() {
        let relay = TestRelay::spawn().await.unwrap();
        let alice = node(&relay, [3u8; 32], MAX_DIRECT_PEERS).await;
        let bob = node(&relay, [3u8; 32], 0).await;
        settled(&relay).await;
        let error = offer(&alice, &bob).await.expect_err("bob is at his cap");
        assert!(super::super::is_cap_refusal(&error), "{error:#}");
    }

    /// The dispatcher runs every event through the opener: an offer sealed for
    /// another mesh is dropped before it reaches admission.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_offer_from_another_mesh_is_dropped_before_admission() {
        let relay = TestRelay::spawn().await.unwrap();
        let bob = node(&relay, [4u8; 32], MAX_DIRECT_PEERS).await;
        let stranger = node(&relay, [5u8; 32], MAX_DIRECT_PEERS).await;
        settled(&relay).await;
        // The stranger's keys, Bob's tag: the seal cannot open.
        let mut forged = stranger.signal.signal(SignalKind::Offer);
        forged.to = Some(bob.endpoint.id());
        forged.envelope = Some("{}".into());
        let content = seal(
            &stranger.signal.inner.keys,
            &stranger.signal.inner.secret,
            &forged,
        );
        let bobs_tag = bob.signal.inner.keys.self_tag(&bob.endpoint.id());
        for _ in 0..5 {
            stranger
                .signal
                .inner
                .pool
                .publish(&bobs_tag, content.clone());
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(bob.admission.in_flight(), 0);
    }
}
