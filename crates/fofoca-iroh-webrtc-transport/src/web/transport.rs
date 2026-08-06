//! Multi-session browser `WebRTC` transport.
//!
//! The consumer and the producer share one shape: a hub that owns zero or more
//! data channels, keyed by remote [`EndpointId`]. The consumer attaches one
//! session after offering; the producer attaches each peer that signals in.

use std::io;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::SinkExt as _;
use futures::StreamExt as _;
use futures::channel::mpsc;
use iroh::EndpointId;
use iroh::endpoint::transports::{
    CustomEndpoint, CustomSender, CustomTransport, RecvInfo, Transmit,
};
use iroh_base::CustomAddr;
use js_sys::Reflect;
use n0_watcher::Watchable;
use wasm_bindgen::JsCast as _;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{MessageEvent, RtcDataChannel, RtcPeerConnection};

use crate::custom_addr;
use crate::registry::Registry;

/// Bound on queued outbound datagrams per session.
const OUT_QUEUE: usize = 256;
/// Bound on inbound datagrams from every session toward QUIC.
pub(crate) const IN_QUEUE: usize = 512;
/// Stop queueing into the channel above this much buffered data.
const BUFFER_CAP: u32 = 1 << 20;
/// The suffix an mDNS candidate address carries, which the browser hands out
/// in place of a private IP once it has anonymized the candidate.
const MDNS_SUFFIX: &str = ".local";

pub(crate) struct InboundPacket {
    pub(crate) from: EndpointId,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug)]
struct SessionHandle {
    out_tx: mpsc::Sender<Vec<u8>>,
    /// Keeps the peer connection and data channel alive.
    keepalive: SessionKeepalive,
}

/// Opaque hold on browser handles that must outlive the QUIC path.
#[derive(Debug)]
struct SessionKeepalive {
    peer_connection: RtcPeerConnection,
    data_channel: RtcDataChannel,
    _callbacks: Vec<JsValue>,
}

impl Drop for SessionKeepalive {
    fn drop(&mut self) {
        // Clear the handlers before the closures in `callbacks` drop, so a
        // late browser event cannot invoke a destroyed closure.
        self.data_channel.set_onmessage(None);
        self.data_channel.set_onclose(None);
        self.data_channel.set_onerror(None);
        self.data_channel.close();
        self.peer_connection.close();
    }
}

type SessionMap = Arc<Registry<SessionHandle>>;

/// Why [`BrowserHubTransport::attach`] refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachError {
    /// Someone already holds this peer's slot — a live session, or another
    /// negotiation partway through attaching one.
    ///
    /// Not necessarily a fault. The mount lane and the mesh lane share one
    /// registry, so either can reach a peer first; the loser should use the
    /// winner's session rather than treat this as a failure.
    ///
    /// The handles passed to `attach` have already been closed.
    AlreadyAttached(EndpointId),
}

impl std::fmt::Display for AttachError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyAttached(remote) => {
                write!(formatter, "a WebRTC session for {remote} already exists")
            }
        }
    }
}

impl std::error::Error for AttachError {}

/// A session that is attached but not yet proven to carry traffic.
///
/// Attaching happens *before* the data channel opens — deliberately, because
/// the browser has no inbound buffer and a QUIC Initial that lands before
/// `onmessage` is installed is lost. This guard is what makes that early attach
/// safe: dropping it without [`Self::commit`] removes the session and closes
/// the peer connection, so a negotiation that never gets its channel open
/// leaves nothing behind.
#[derive(Debug)]
#[must_use = "an uncommitted session guard tears the session down when dropped"]
pub struct BrowserSessionGuard(crate::registry::SessionGuard<SessionHandle>);

impl BrowserSessionGuard {
    /// The peer this session is for.
    #[must_use]
    pub fn remote(&self) -> EndpointId {
        self.0.remote()
    }

    /// The channel is open and carrying traffic: keep the session.
    pub fn commit(self) {
        self.0.commit();
    }
}

/// Factory for the browser `WebRTC` datagram lane of one iroh endpoint.
pub struct BrowserHubTransport {
    local_id: EndpointId,
    sessions: SessionMap,
    inbound_tx: mpsc::Sender<InboundPacket>,
    inbound_rx: Mutex<Option<mpsc::Receiver<InboundPacket>>>,
    local_addrs: Watchable<Vec<CustomAddr>>,
}

impl BrowserHubTransport {
    #[must_use]
    pub fn new(local_id: EndpointId) -> Arc<Self> {
        let (inbound_tx, inbound_rx) = mpsc::channel(IN_QUEUE);
        Arc::new(Self {
            local_id,
            sessions: Registry::new(),
            inbound_tx,
            inbound_rx: Mutex::new(Some(inbound_rx)),
            local_addrs: Watchable::new(vec![custom_addr(local_id)]),
        })
    }

    #[must_use]
    pub fn local_id(&self) -> EndpointId {
        self.local_id
    }

    /// Adopt an open data channel for `remote` and start its pumps.
    ///
    /// Returns a guard, not `()`. The session is in the registry the moment
    /// this returns — it has to be, so no inbound QUIC Initial is lost — but it
    /// is not yet *proven*, and the caller must [`BrowserSessionGuard::commit`]
    /// once the channel opens. Dropping the guard instead removes the session
    /// and closes the handles.
    ///
    /// # Errors
    /// [`AttachError::AlreadyAttached`] when someone already holds this peer's
    /// slot. The handles passed in are closed before returning: `attach` took
    /// ownership of them, so the caller has no way to.
    pub fn attach(
        &self,
        remote: EndpointId,
        peer_connection: RtcPeerConnection,
        data_channel: RtcDataChannel,
        mut callbacks: Vec<JsValue>,
    ) -> Result<BrowserSessionGuard, AttachError> {
        // Claim the slot first, before a single handler is installed.
        //
        // The old order was install-then-check, and the refused path was a trap:
        // `Closure::into_js_value` forgets each closure into JS, so dropping the
        // local `callbacks` vec detached nothing. A duplicate left behind an
        // unclosed peer connection, an inbound pump still injecting packets
        // tagged as this remote, and — worst — a live `onclose` hook that would
        // later remove the *surviving* session, killing a working data path
        // mid-transfer.
        //
        // Nothing between here and `fulfil` below awaits or re-enters the
        // registry (`spawn_local` only queues), so the reserved slot is never
        // observable from outside this function.
        let Some(reservation) = self.sessions.reserve(remote) else {
            data_channel.close();
            peer_connection.close();
            return Err(AttachError::AlreadyAttached(remote));
        };
        let generation = reservation.generation();

        let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(OUT_QUEUE);
        let (mut inbound_sender, mut inbound_receiver) = mpsc::channel::<Vec<u8>>(IN_QUEUE);

        let onmessage = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
            let Some(bytes) = message_bytes(&event) else {
                return;
            };
            let _ = inbound_sender.try_send(bytes);
        });
        data_channel.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        callbacks.push(onmessage.into_js_value());

        // Self-remove when the channel dies, so a reconnecting peer is not
        // refused with "a live session already exists" and the map cannot
        // grow without bound. Removal is deferred to a task: it drops this
        // very closure (it lives in the session's keepalive), which must not
        // happen while the closure is executing.
        //
        // Scoped to `generation`, so a hook can only ever remove the session it
        // was born into. An unscoped hook on a channel that outlived its
        // session — an orphan from a refused duplicate, or a channel whose
        // close event arrives after the peer reconnected — would take out the
        // replacement instead.
        for event in ["close", "error"] {
            let sessions = Arc::clone(&self.sessions);
            let hook = Closure::<dyn FnMut()>::new(move || {
                let sessions = Arc::clone(&sessions);
                wasm_bindgen_futures::spawn_local(async move {
                    if sessions.remove_if_generation(&remote, generation) {
                        web_sys::console::log_1(&JsValue::from_str(&format!(
                            "[fofoca webrtc] session for {remote} detached (channel closed)"
                        )));
                    }
                });
            });
            match event {
                "close" => data_channel.set_onclose(Some(hook.as_ref().unchecked_ref())),
                _ => data_channel.set_onerror(Some(hook.as_ref().unchecked_ref())),
            }
            callbacks.push(hook.into_js_value());
        }

        {
            let mut inbound_tx = self.inbound_tx.clone();
            wasm_bindgen_futures::spawn_local(async move {
                while let Some(payload) = inbound_receiver.next().await {
                    // Await rather than try_send: a single full queue used to
                    // tear down the whole pump and kill the WebRTC lane.
                    if inbound_tx
                        .send(InboundPacket {
                            from: remote,
                            payload,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }

        {
            let data_channel = data_channel.clone();
            wasm_bindgen_futures::spawn_local(async move {
                // Rate-limited visibility for the lossy gate: without it a
                // congested channel is indistinguishable from a broken one.
                let mut dropped: u64 = 0;
                while let Some(datagram) = out_rx.next().await {
                    if data_channel.buffered_amount() < BUFFER_CAP {
                        let _ = data_channel.send_with_u8_array(&datagram);
                    } else {
                        dropped += 1;
                        if dropped == 1 || dropped.is_multiple_of(256) {
                            web_sys::console::warn_1(&JsValue::from_str(&format!(
                                "[fofoca webrtc] dropping outbound datagrams \
                                 (bufferedAmount over cap); total {dropped} for {remote}"
                            )));
                        }
                    }
                }
                if dropped > 0 {
                    web_sys::console::log_1(&JsValue::from_str(&format!(
                        "[fofoca webrtc] session for {remote} dropped \
                         {dropped} outbound datagrams over its lifetime"
                    )));
                }
            });
        }

        Ok(BrowserSessionGuard(reservation.fulfil(SessionHandle {
            out_tx,
            keepalive: SessionKeepalive {
                peer_connection,
                data_channel,
                _callbacks: callbacks,
            },
        })))
    }

    /// Whether a *usable* session for `remote` exists.
    ///
    /// The mirror of `WebRtcTransport::has_session` on the host side. A session
    /// manager needs this to answer "have I already negotiated with this peer?"
    /// without attempting a duplicate `attach` and reading the error.
    ///
    /// A session still waiting for its channel to open does not count. It is
    /// not yet a path anything can be sent over, and reporting it as one is how
    /// a failed ICE run used to pin a pair to the relay forever.
    #[must_use]
    pub fn has_session(&self, remote: &EndpointId) -> bool {
        self.sessions.is_live(remote)
    }

    /// How many live sessions this hub holds — the tab's direct-peer count.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.live_len()
    }

    /// Endpoint ids of every live session.
    #[must_use]
    pub fn live_peer_ids(&self) -> Vec<EndpointId> {
        self.sessions.live_ids()
    }

    /// Selected ICE remote candidate for `remote`, if a live session exists.
    ///
    /// Returns `(address, candidate_type)` where `candidate_type` is the
    /// browser's string (`host` / `srflx` / `relay` / `prflx`). Address may be
    /// an mDNS `.local` name — browsers redact LAN IPs that way.
    pub async fn selected_remote_candidate(&self, remote: &EndpointId) -> Option<(String, String)> {
        let peer_connection = self
            .sessions
            .with_live(remote, |handle| handle.keepalive.peer_connection.clone())?;
        selected_candidate_from_stats(&peer_connection, "remoteCandidateId").await
    }

    /// Counters and round-trip time on the selected candidate pair.
    ///
    /// Returns `(bytes_sent, bytes_received, rtt_seconds)`. Wire bytes, not
    /// payload: SCTP, DTLS and STUN framing are included, so this is what
    /// actually crossed the network rather than what the application handed
    /// over — the number that says where traffic really went.
    ///
    /// The counters are cumulative; a rate is the caller's job, from two
    /// samples. WebRTC exposes no instantaneous throughput for a data channel
    /// (`availableOutgoingBitrate` is media-bandwidth-estimation driven and is
    /// absent here), so differencing is the only route.
    ///
    /// `rtt` is `None` when the pair has not been measured yet, rather than
    /// zero — a real 0 ms and "not known" should not render alike.
    pub async fn selected_pair_stats(
        &self,
        remote: &EndpointId,
    ) -> Option<(f64, f64, Option<f64>)> {
        let peer_connection = self
            .sessions
            .with_live(remote, |handle| handle.keepalive.peer_connection.clone())?;
        let (_, pair) = selected_pair_from_stats(&peer_connection).await?;
        let read = |key: &str| {
            Reflect::get(&pair, &JsValue::from_str(key))
                .ok()
                .and_then(|value| value.as_f64())
        };
        Some((
            read("bytesSent").unwrap_or(0.0),
            read("bytesReceived").unwrap_or(0.0),
            read("currentRoundTripTime"),
        ))
    }

    /// Selected ICE **local** candidate for a live session, if any.
    ///
    /// The other half of [`Self::selected_remote_candidate`]. Without it a tab
    /// can name every peer's address but not its own, so the roster shows the
    /// local row with no ip at all — the one row where the answer is always
    /// available, since it comes from our own `getStats`.
    pub async fn selected_local_candidate(&self, remote: &EndpointId) -> Option<(String, String)> {
        let peer_connection = self
            .sessions
            .with_live(remote, |handle| handle.keepalive.peer_connection.clone())?;
        selected_candidate_from_stats(&peer_connection, "localCandidateId").await
    }

    /// Tear down the session for `remote`, if any.
    pub fn detach(&self, remote: &EndpointId) -> bool {
        self.sessions.remove(remote)
    }

    /// Tear down every live session (producer shutdown). Returns how many
    /// sessions were closed.
    pub fn detach_all(&self) -> usize {
        self.sessions.clear()
    }
}

impl std::fmt::Debug for BrowserHubTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BrowserHubTransport")
            .field("local_id", &self.local_id)
            .field("sessions", &self.sessions.live_len())
            .finish_non_exhaustive()
    }
}

/// Read the selected ICE pair's remote candidate via `RTCPeerConnection.getStats`.
/// Address + candidate type for one side of the selected candidate pair.
///
/// `side` is the stats field naming the candidate to follow —
/// `remoteCandidateId` for the peer, `localCandidateId` for us. Both sides come
/// from the same `getStats` walk, so the pair a row reports is the pair that is
/// actually carrying traffic.
/// Index a `getStats` report by id, and find the selected candidate pair.
///
/// Shared so the candidate reader and the byte reader agree on *which* pair
/// they are describing: an address from one pair and counters from another
/// would be a plausible-looking lie.
async fn selected_pair_from_stats(
    peer_connection: &RtcPeerConnection,
) -> Option<(
    std::collections::HashMap<String, js_sys::Object>,
    js_sys::Object,
)> {
    let report = JsFuture::from(peer_connection.get_stats()).await.ok()?;
    let mut by_id: std::collections::HashMap<String, js_sys::Object> =
        std::collections::HashMap::new();
    let iter = js_sys::try_iter(&report).ok().flatten()?;
    for entry in iter.flatten() {
        let Ok(pair) = entry.dyn_into::<js_sys::Array>() else {
            continue;
        };
        if pair.length() < 2 {
            continue;
        }
        let Ok(obj) = pair.get(1).dyn_into::<js_sys::Object>() else {
            continue;
        };
        let id = pair.get(0).as_string().unwrap_or_default();
        by_id.insert(id, obj);
    }

    let mut selected_pair_id = None;
    for stats in by_id.values() {
        let ty = Reflect::get(stats, &JsValue::from_str("type"))
            .ok()
            .and_then(|value| value.as_string())
            .unwrap_or_default();
        if ty == "transport" {
            selected_pair_id = Reflect::get(stats, &JsValue::from_str("selectedCandidatePairId"))
                .ok()
                .and_then(|value| value.as_string());
            if selected_pair_id.is_some() {
                break;
            }
        }
        if ty == "candidate-pair"
            && Reflect::get(stats, &JsValue::from_str("selected"))
                .ok()
                .and_then(|value| value.as_bool())
                == Some(true)
        {
            selected_pair_id = Reflect::get(stats, &JsValue::from_str("id"))
                .ok()
                .and_then(|value| value.as_string());
            break;
        }
    }
    let pair_id = selected_pair_id?;
    let pair = by_id.get(&pair_id)?.clone();
    Some((by_id, pair))
}

/// Address + candidate type for one side of the selected pair.
///
/// `side` is the stats field naming the candidate to follow —
/// `remoteCandidateId` for the peer, `localCandidateId` for us.
///
/// **The address may be empty, and a caller must render that case.** A browser
/// only reports a candidate's address in `getStats` when that address has
/// already gone out on the wire; anything it considers private it withholds,
/// leaving `address` and `ip` both null. Measured: Safari withholds it for
/// `host` *and* `prflx` and names only `srflx`; Chrome withholds it for
/// `prflx`. The candidate **type** is reported in every one of those cases, and
/// "we paired on a host candidate whose address the browser will not name" is a
/// far more useful answer than the `None` this used to return — which a caller
/// cannot tell apart from "there is no session".
async fn selected_candidate_from_stats(
    peer_connection: &RtcPeerConnection,
    side: &str,
) -> Option<(String, String)> {
    let (by_id, pair) = selected_pair_from_stats(peer_connection).await?;
    let pair = &pair;
    let remote_id = Reflect::get(pair, &JsValue::from_str(side))
        .ok()
        .and_then(|value| value.as_string())?;
    let remote = by_id.get(&remote_id)?;
    let address = Reflect::get(remote, &JsValue::from_str("address"))
        .ok()
        .and_then(|value| value.as_string())
        .or_else(|| {
            Reflect::get(remote, &JsValue::from_str("ip"))
                .ok()
                .and_then(|value| value.as_string())
        })
        .unwrap_or_default();
    let kind = Reflect::get(remote, &JsValue::from_str("candidateType"))
        .ok()
        .and_then(|value| value.as_string())
        .unwrap_or_else(|| "unknown".to_owned());
    // Suffix-compared case-insensitively: DNS labels are case-insensitive by
    // spec, and going through `str::ends_with` with a dotted literal reads to
    // clippy as a file-extension test, which this is not.
    let is_mdns = address
        .len()
        .checked_sub(MDNS_SUFFIX.len())
        .is_some_and(|start| address[start..].eq_ignore_ascii_case(MDNS_SUFFIX));
    let kind = if is_mdns { "mdns".to_owned() } else { kind };
    Some((address, kind))
}

impl CustomTransport for BrowserHubTransport {
    fn bind(&self) -> io::Result<Box<dyn CustomEndpoint>> {
        let receiver = self
            .inbound_rx
            .lock()
            .expect("inbound receiver mutex poisoned")
            .take()
            .ok_or_else(|| io::Error::other("BrowserHubTransport is already bound"))?;
        Ok(Box::new(BrowserHubEndpoint {
            local_addrs: self.local_addrs.clone(),
            receiver,
            sessions: Arc::clone(&self.sessions),
        }))
    }
}

struct BrowserHubEndpoint {
    local_addrs: Watchable<Vec<CustomAddr>>,
    receiver: mpsc::Receiver<InboundPacket>,
    sessions: SessionMap,
}

impl std::fmt::Debug for BrowserHubEndpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BrowserHubEndpoint")
            .finish_non_exhaustive()
    }
}

impl CustomEndpoint for BrowserHubEndpoint {
    fn watch_local_addrs(&self) -> n0_watcher::Direct<Vec<CustomAddr>> {
        self.local_addrs.watch()
    }

    fn create_sender(&self) -> Arc<dyn CustomSender> {
        Arc::new(BrowserHubSender {
            sessions: Arc::clone(&self.sessions),
        })
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
        metas: &mut [noq_udp::RecvMeta],
        recv_infos: &mut [RecvInfo],
    ) -> Poll<io::Result<usize>> {
        if bufs.is_empty() || metas.is_empty() || recv_infos.is_empty() {
            return Poll::Ready(Ok(0));
        }
        loop {
            match self.receiver.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    return Poll::Ready(Err(io::Error::other("inbound packet channel closed")));
                }
                Poll::Ready(Some(packet)) => {
                    if bufs[0].len() < packet.payload.len() {
                        continue;
                    }
                    bufs[0][..packet.payload.len()].copy_from_slice(&packet.payload);
                    metas[0].len = packet.payload.len();
                    metas[0].stride = packet.payload.len();
                    // Match the original single-session transport: leave local
                    // unset. Some iroh paths treat a Custom local addr oddly
                    // when the path was learned only from the data channel.
                    recv_infos[0] = RecvInfo::new(custom_addr(packet.from), None);
                    return Poll::Ready(Ok(1));
                }
            }
        }
    }
}

fn message_bytes(event: &MessageEvent) -> Option<Vec<u8>> {
    let data = event.data();
    if let Ok(buffer) = data.clone().dyn_into::<js_sys::ArrayBuffer>() {
        return Some(js_sys::Uint8Array::new(&buffer).to_vec());
    }
    if let Ok(array) = data.dyn_into::<js_sys::Uint8Array>() {
        return Some(array.to_vec());
    }
    None
}

struct BrowserHubSender {
    sessions: SessionMap,
}

impl std::fmt::Debug for BrowserHubSender {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BrowserHubSender")
            .finish_non_exhaustive()
    }
}

impl CustomSender for BrowserHubSender {
    /// Live sessions only. A reserved slot has no pump behind it, so calling it
    /// a valid address would have QUIC write into a channel that is not open —
    /// which is exactly what a stuck phantom entry used to do, silently.
    fn is_valid_send_addr(&self, addr: &CustomAddr) -> bool {
        let Ok(remote) = crate::parse_custom_addr(addr) else {
            return false;
        };
        self.sessions.is_live(&remote)
    }

    fn poll_send(
        &self,
        _cx: &mut Context<'_>,
        dst: &CustomAddr,
        _src: Option<&CustomAddr>,
        transmit: &Transmit<'_>,
    ) -> Poll<io::Result<()>> {
        let Ok(remote) = crate::parse_custom_addr(dst) else {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        };
        let chunk_size = transmit
            .segment_size
            .unwrap_or_else(|| transmit.contents.len().max(1));
        // Clone the sender out under the lock, then queue outside it.
        let Some(mut out_tx) = self
            .sessions
            .with_live(&remote, |handle| handle.out_tx.clone())
        else {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        };
        for chunk in transmit.contents.chunks(chunk_size) {
            let _ = out_tx.try_send(chunk.to_vec());
        }
        Poll::Ready(Ok(()))
    }
}

/// Backward-compatible name used by the consume path and older docs.
pub type BrowserRtcTransport = BrowserHubTransport;
