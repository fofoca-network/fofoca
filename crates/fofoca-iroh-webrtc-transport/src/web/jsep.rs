//! Browser-side JSEP: drive `RTCPeerConnection` to a negotiated data channel.
//!
//! Deliberately carrier-agnostic, exactly like the host side. [`offer`] /
//! [`answer`] hand back a [`SignalEnvelope`] and the caller carries it however
//! it likes. Nothing here knows how the remote is reached.
//!
//! Vanilla ICE: candidates ride inside the SDP, so gathering must complete
//! *before* the envelope goes out. There is no trickle message to add them
//! later.

use wasm_bindgen::JsCast as _;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    RtcConfiguration, RtcDataChannel, RtcDataChannelEvent, RtcIceConnectionState,
    RtcIceGatheringState, RtcPeerConnection, RtcSdpType, RtcSessionDescriptionInit,
};

use crate::{DATA_CHANNEL_LABEL, SIGNAL_VERSION, SignalEnvelope, accept_ice_uri};

use super::transport::BrowserHubTransport;
use iroh_base::EndpointId;

/// How long to wait for the data channel after the answer is applied.
const CHANNEL_OPEN_DEADLINE_MS: f64 = 60_000.0;
/// The same deadline for `sleep_ms`, which takes the browser's own `i32`
/// milliseconds. Written out rather than cast so the two cannot drift and no
/// truncation is implied.
const CHANNEL_OPEN_DEADLINE_MS_I32: i32 = 60_000;
/// How long to let gathering run before settling for the candidates in hand.
/// Host is instant and srflx costs one STUN round trip, so this is generous —
/// it was sized for TURN allocate, which no longer happens.
const ICE_GATHERING_DEADLINE_MS: f64 = 10_000.0;
const POLL_MS: i32 = 50;

/// One `RTCIceServer` entry.
///
/// No `username`/`credential`: those exist only for TURN, which this crate
/// refuses (see [`crate::accept_ice_uri`]). Dropping the fields means a
/// credentialed server cannot be expressed, let alone configured.
#[derive(Debug, Clone)]
pub struct IceServer {
    pub urls: Vec<String>,
}

/// ICE servers for the browser's own agent.
#[derive(Debug, Clone)]
pub struct IceServers(pub Vec<IceServer>);

impl Default for IceServers {
    fn default() -> Self {
        Self(vec![IceServer {
            urls: vec![
                // `stun1`, not the bare `stun.l.google.com`: blocklists name the
                // latter explicitly and null-route it to 0.0.0.0, which is worse
                // than NXDOMAIN — the agent waits out a timeout on an
                // unroutable address instead of failing fast, spending part of
                // `ICE_GATHERING_DEADLINE_MS` on a server that cannot answer.
                // Measured behind an AdGuard resolver: `stun.l.google.com` →
                // 0.0.0.0, `stun1..4.l.google.com` → 74.125.250.129.
                //
                // Two servers, deliberately, and from two *operators* — that is
                // the redundancy that counts. `stun2/3/4` share one address with
                // `stun1`, so they would be redundancy in name only, and every
                // extra server costs a srflx candidate per local interface.
                "stun:stun1.l.google.com:19302".to_owned(),
                "stun:stun.cloudflare.com:3478".to_owned(),
            ],
        }])
    }
}

impl IceServers {
    #[must_use]
    pub fn host_only() -> Self {
        Self(Vec::new())
    }

    fn to_configuration(&self) -> RtcConfiguration {
        let config = RtcConfiguration::new();
        let servers = js_sys::Array::new();
        for server in &self.0 {
            let entry = js_sys::Object::new();
            let urls = js_sys::Array::new();
            // The enforcement point: anything `accept_ice_uri` refuses never
            // reaches the browser. TURN cannot be configured here even by
            // mistake, and a query string Safari would throw on is stripped
            // before it can take down the whole peer connection.
            for url in server.urls.iter().filter_map(|url| accept_ice_uri(url)) {
                urls.push(&JsValue::from_str(&url));
            }
            if urls.length() == 0 {
                continue;
            }
            let _ = js_sys::Reflect::set(&entry, &JsValue::from_str("urls"), &urls);
            servers.push(&entry);
        }
        config.set_ice_servers(&servers);
        config
    }
}

/// Build an `RTCPeerConnection`, degrading rather than failing when the browser
/// refuses a server entry.
///
/// The constructor validates every ICE URL and throws on the first bad one, so
/// a single unusable entry costs the whole connection. [`accept_ice_uri`]
/// filters what we control, but the configuration can still be rejected for a
/// reason we have not met yet — and a peer connection with no ICE servers still
/// gathers host candidates, which is strictly better than none at all.
fn new_peer_connection(ice: &IceServers) -> Result<RtcPeerConnection, JsValue> {
    match RtcPeerConnection::new_with_configuration(&ice.to_configuration()) {
        Ok(peer_connection) => Ok(peer_connection),
        Err(error) => {
            web_sys::console::warn_1(&JsValue::from_str(&format!(
                "[fofoca webrtc] the browser refused the ICE configuration \
                 ({error:?}); retrying with no ICE servers (host candidates only)"
            )));
            RtcPeerConnection::new_with_configuration(&IceServers::host_only().to_configuration())
                .map_err(|retry_error| js_err("RTCPeerConnection", &retry_error))
        }
    }
}

/// A negotiated browser session attached into a [`BrowserHubTransport`].
pub struct BrowserSession {
    pub remote: EndpointId,
}

impl std::fmt::Debug for BrowserSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BrowserSession")
            .field("remote", &self.remote)
            .finish()
    }
}

/// An `RTCPeerConnection` that closes itself unless it is released.
///
/// Every early return on the way to a session used to drop an open peer
/// connection without closing it, leaving a live ICE agent — and, when the
/// public TURN fallback is in play, a live TURN allocation — for as long as the
/// tab lives. Failures here are routine by design (ICE fails, a peer vanishes
/// mid-handshake, the offerer never opens its channel), so "routine" was
/// leaking.
///
/// The newtype goes on the *field* rather than the outer struct because `Drop`
/// and destructuring `let Self { .. } = self` cannot coexist.
struct OpenPeerConnection {
    inner: RtcPeerConnection,
    armed: bool,
}

impl OpenPeerConnection {
    fn new(inner: RtcPeerConnection) -> Self {
        Self { inner, armed: true }
    }

    /// Hand the connection to whoever owns its lifetime from here — in
    /// practice the hub, whose session keepalive closes it on drop.
    fn release(mut self) -> RtcPeerConnection {
        self.armed = false;
        self.inner.clone()
    }
}

impl std::ops::Deref for OpenPeerConnection {
    type Target = RtcPeerConnection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Drop for OpenPeerConnection {
    fn drop(&mut self) {
        if self.armed {
            self.inner.close();
        }
    }
}

impl std::fmt::Debug for OpenPeerConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenPeerConnection")
            .field("armed", &self.armed)
            .finish_non_exhaustive()
    }
}

/// Offerer state between producing the offer and applying the answer.
pub struct PendingOffer {
    peer_connection: OpenPeerConnection,
    data_channel: RtcDataChannel,
    callbacks: Vec<JsValue>,
}

impl std::fmt::Debug for PendingOffer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingOffer")
            .finish_non_exhaustive()
    }
}

/// Answerer state between sending the answer and the channel opening.
pub struct PendingAnswer {
    peer_connection: OpenPeerConnection,
    /// Filled by `ondatachannel` when the remote opens the channel.
    channel_rx: futures::channel::oneshot::Receiver<RtcDataChannel>,
    callbacks: Vec<JsValue>,
}

impl std::fmt::Debug for PendingAnswer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingAnswer")
            .finish_non_exhaustive()
    }
}

/// Start a negotiation as the offerer.
///
/// # Errors
/// The peer connection cannot be created, SDP generation fails, or ICE
/// gathers no candidates.
pub async fn offer(
    local: EndpointId,
    ice: &IceServers,
) -> Result<(PendingOffer, SignalEnvelope), JsValue> {
    let peer_connection = OpenPeerConnection::new(new_peer_connection(ice)?);
    // Unreliable + unordered: the channel carries QUIC datagrams, and QUIC
    // already owns loss recovery and congestion control. Reliable ordered
    // SCTP underneath it would stack a second retransmission loop and
    // head-of-line-block unrelated QUIC streams. The answerer adopts this
    // config from DCEP, so the offerer is the only place it is declared.
    let channel_init = web_sys::RtcDataChannelInit::new();
    channel_init.set_ordered(false);
    channel_init.set_max_retransmits(0);
    let data_channel = peer_connection
        .create_data_channel_with_data_channel_dict(DATA_CHANNEL_LABEL, &channel_init);
    data_channel.set_binary_type(web_sys::RtcDataChannelType::Arraybuffer);
    // Do not install onmessage yet — `complete` attaches first so the hub
    // handler is live before the channel opens (avoids dropping QUIC Initials).

    let sdp_offer = JsFuture::from(peer_connection.create_offer())
        .await
        .map_err(|error| js_err("createOffer", &error))?;
    let offer_init: RtcSessionDescriptionInit = sdp_offer.unchecked_into();
    JsFuture::from(peer_connection.set_local_description(&offer_init))
        .await
        .map_err(|error| js_err("setLocalDescription", &error))?;
    wait_ice_complete(&peer_connection).await;
    let local_sdp = peer_connection
        .local_description()
        .ok_or_else(|| JsValue::from_str("no local description after gathering"))?
        .sdp();
    require_candidates("offer", &local_sdp)?;

    let envelope = SignalEnvelope::Offer {
        version: SIGNAL_VERSION,
        endpoint_id: local.to_string(),
        sdp: local_sdp,
    };
    Ok((
        PendingOffer {
            peer_connection,
            data_channel,
            callbacks: Vec::new(),
        },
        envelope,
    ))
}

impl PendingOffer {
    /// Apply the remote answer, wait for the channel, attach into `hub`.
    ///
    /// # Errors
    /// A non-answer envelope, a bad SDP, the channel never opening, or attach.
    pub async fn complete(
        self,
        hub: &BrowserHubTransport,
        answer: &SignalEnvelope,
    ) -> Result<BrowserSession, JsValue> {
        let Self {
            peer_connection,
            data_channel,
            callbacks,
        } = self;

        let remote = answer
            .claimed_endpoint()
            .map_err(|error| any_err("answer endpoint id", error))?;
        let SignalEnvelope::Answer { sdp, .. } = answer else {
            return Err(JsValue::from_str("expected an answer envelope"));
        };

        let answer_init = RtcSessionDescriptionInit::new(RtcSdpType::Answer);
        answer_init.set_sdp(sdp);
        JsFuture::from(peer_connection.set_remote_description(&answer_init))
            .await
            .map_err(|error| js_err("setRemoteDescription", &error))?;

        // Attach before Open so inbound QUIC Initials are not lost between
        // the channel opening and the onmessage handler being installed.
        //
        // The guard is what makes that early attach safe. If the channel never
        // opens — ICE failed, or the deadline passed while it was still
        // Connecting — dropping the guard on the way out removes the session
        // and closes the handles. Without it the entry stayed forever, because
        // a channel that never opened never fires `onclose` either: it counted
        // toward the direct-peer total, answered `has_session` so the pair was
        // never retried, and told QUIC it was a valid send address.
        let pc = peer_connection.release();
        let guard = hub
            .attach(remote, pc.clone(), data_channel.clone(), callbacks)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        wait_channel_open(&data_channel, &pc).await?;
        guard.commit();
        Ok(BrowserSession { remote })
    }
}

/// Answer a remote offer.
///
/// # Errors
/// A non-offer envelope, SDP / peer-connection setup fails, or ICE gathers
/// no candidates.
pub async fn answer(
    local: EndpointId,
    offer: &SignalEnvelope,
    ice: &IceServers,
) -> Result<(PendingAnswer, SignalEnvelope), JsValue> {
    let SignalEnvelope::Offer { sdp, .. } = offer else {
        return Err(JsValue::from_str("expected an offer envelope"));
    };

    let peer_connection = OpenPeerConnection::new(new_peer_connection(ice)?);

    let (channel_tx, channel_rx) = futures::channel::oneshot::channel::<RtcDataChannel>();
    let channel_tx = std::cell::RefCell::new(Some(channel_tx));
    let ondatachannel =
        Closure::<dyn FnMut(RtcDataChannelEvent)>::new(move |event: RtcDataChannelEvent| {
            let channel = event.channel();
            channel.set_binary_type(web_sys::RtcDataChannelType::Arraybuffer);
            if let Some(tx) = channel_tx.borrow_mut().take() {
                let _ = tx.send(channel);
            }
        });
    peer_connection.set_ondatachannel(Some(ondatachannel.as_ref().unchecked_ref()));

    let offer_init = RtcSessionDescriptionInit::new(RtcSdpType::Offer);
    offer_init.set_sdp(sdp);
    JsFuture::from(peer_connection.set_remote_description(&offer_init))
        .await
        .map_err(|error| js_err("setRemoteDescription", &error))?;

    let sdp_answer = JsFuture::from(peer_connection.create_answer())
        .await
        .map_err(|error| js_err("createAnswer", &error))?;
    let answer_init: RtcSessionDescriptionInit = sdp_answer.unchecked_into();
    JsFuture::from(peer_connection.set_local_description(&answer_init))
        .await
        .map_err(|error| js_err("setLocalDescription", &error))?;
    wait_ice_complete(&peer_connection).await;
    let local_sdp = peer_connection
        .local_description()
        .ok_or_else(|| JsValue::from_str("no local description after gathering"))?
        .sdp();
    require_candidates("answer", &local_sdp)?;

    let envelope = SignalEnvelope::Answer {
        version: SIGNAL_VERSION,
        endpoint_id: local.to_string(),
        sdp: local_sdp,
    };
    Ok((
        PendingAnswer {
            peer_connection,
            channel_rx,
            callbacks: vec![ondatachannel.into_js_value()],
        },
        envelope,
    ))
}

impl PendingAnswer {
    /// Wait for the offerer's data channel, then attach into `hub`.
    ///
    /// `remote` is the authenticated peer id (prefer `connection.remote_id()`
    /// over the envelope claim).
    ///
    /// # Errors
    /// The channel never arrives or attach fails.
    pub async fn complete(
        self,
        hub: &BrowserHubTransport,
        remote: EndpointId,
    ) -> Result<BrowserSession, JsValue> {
        let Self {
            peer_connection,
            channel_rx,
            callbacks,
        } = self;

        let data_channel = match futures::future::select(
            channel_rx,
            Box::pin(async {
                sleep_ms(CHANNEL_OPEN_DEADLINE_MS_I32).await;
            }),
        )
        .await
        {
            futures::future::Either::Left((Ok(channel), _)) => channel,
            futures::future::Either::Left((Err(_), _)) => {
                return Err(JsValue::from_str("data channel sender dropped"));
            }
            futures::future::Either::Right(((), _)) => {
                return Err(JsValue::from_str("timed out waiting for ondatachannel"));
            }
        };

        // Attach before Open — same race as the offerer path: the peer may
        // dial the mount ALPN the instant its channel is open. Same guard, for
        // the same reason; see `PendingOffer::complete`.
        let pc = peer_connection.release();
        let guard = hub
            .attach(remote, pc.clone(), data_channel.clone(), callbacks)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        wait_channel_open(&data_channel, &pc).await?;
        guard.commit();
        Ok(BrowserSession { remote })
    }
}

/// Log local/remote SDP snippets when a dial fails after envelopes were swapped.
pub fn log_signal_sdps(role: &str, local: &SignalEnvelope, remote: &SignalEnvelope) {
    let local_sdp = envelope_sdp(local).unwrap_or("");
    let remote_sdp = envelope_sdp(remote).unwrap_or("");
    web_sys::console::log_1(&JsValue::from_str(&format!(
        "[fofoca webrtc] {role} local candidates: {}",
        format_candidate_counts(local_sdp)
    )));
    web_sys::console::log_1(&JsValue::from_str(&format!(
        "[fofoca webrtc] {role} remote candidates: {}",
        format_candidate_counts(remote_sdp)
    )));
    web_sys::console::log_1(&JsValue::from_str(&format!(
        "[fofoca webrtc] {role} local SDP:\n{local_sdp}"
    )));
    web_sys::console::log_1(&JsValue::from_str(&format!(
        "[fofoca webrtc] {role} remote SDP:\n{remote_sdp}"
    )));
}

fn envelope_sdp(envelope: &SignalEnvelope) -> Option<&str> {
    match envelope {
        SignalEnvelope::Offer { sdp, .. } | SignalEnvelope::Answer { sdp, .. } => Some(sdp),
        SignalEnvelope::Error { .. } => None,
    }
}

fn js_err(context: &str, error: &JsValue) -> JsValue {
    JsValue::from_str(&format!("{context}: {error:?}"))
}

fn any_err(context: &str, error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&format!("{context}: {error}"))
}

async fn wait_ice_complete(peer_connection: &RtcPeerConnection) {
    let deadline = now_ms() + ICE_GATHERING_DEADLINE_MS;
    loop {
        if peer_connection.ice_gathering_state() == RtcIceGatheringState::Complete {
            return;
        }
        // Always proceed after the deadline — previously we looped forever when
        // gathering stalled with zero `a=candidate` lines. Callers then check
        // for candidates and hard-fail if none landed.
        if now_ms() >= deadline {
            return;
        }
        sleep_ms(POLL_MS).await;
    }
}

fn now_ms() -> f64 {
    js_sys::Date::now()
}

/// Count ICE candidates in `sdp` and refuse a candidate-less envelope.
fn require_candidates(role: &str, sdp: &str) -> Result<(), JsValue> {
    let (host, mdns, srflx, relay, other) = count_candidates(sdp);
    let total = host + mdns + srflx + relay + other;
    web_sys::console::log_1(&JsValue::from_str(&format!(
        "[fofoca webrtc] {role} ICE candidates: host={host} mdns={mdns} srflx={srflx} relay={relay} other={other}"
    )));
    if total == 0 {
        return Err(JsValue::from_str(
            "browser gathered no ICE candidates — WebRTC/UDP blocked (Local Network permission, VPN, or policy)",
        ));
    }
    Ok(())
}

fn count_candidates(sdp: &str) -> (usize, usize, usize, usize, usize) {
    let mut host = 0;
    let mut mdns = 0;
    let mut srflx = 0;
    let mut relay = 0;
    let mut other = 0;
    for line in sdp.lines() {
        if !line.starts_with("a=candidate:") {
            continue;
        }
        if line.contains(".local") {
            mdns += 1;
        } else if line.contains(" typ host") {
            host += 1;
        } else if line.contains(" typ srflx") {
            srflx += 1;
        } else if line.contains(" typ relay") {
            relay += 1;
        } else {
            other += 1;
        }
    }
    (host, mdns, srflx, relay, other)
}

fn format_candidate_counts(sdp: &str) -> String {
    let (host, mdns, srflx, relay, other) = count_candidates(sdp);
    format!("host={host} mdns={mdns} srflx={srflx} relay={relay} other={other}")
}

fn ice_state_label(state: RtcIceConnectionState) -> &'static str {
    match state {
        RtcIceConnectionState::New => "new",
        RtcIceConnectionState::Checking => "checking",
        RtcIceConnectionState::Connected => "connected",
        RtcIceConnectionState::Completed => "completed",
        RtcIceConnectionState::Failed => "failed",
        RtcIceConnectionState::Disconnected => "disconnected",
        RtcIceConnectionState::Closed => "closed",
        _ => "unknown",
    }
}

fn gather_state_label(state: RtcIceGatheringState) -> &'static str {
    match state {
        RtcIceGatheringState::New => "new",
        RtcIceGatheringState::Gathering => "gathering",
        RtcIceGatheringState::Complete => "complete",
        _ => "unknown",
    }
}

fn channel_state_label(state: web_sys::RtcDataChannelState) -> &'static str {
    match state {
        web_sys::RtcDataChannelState::Connecting => "connecting",
        web_sys::RtcDataChannelState::Open => "open",
        web_sys::RtcDataChannelState::Closing => "closing",
        web_sys::RtcDataChannelState::Closed => "closed",
        _ => "unknown",
    }
}

async fn wait_channel_open(
    data_channel: &RtcDataChannel,
    peer_connection: &RtcPeerConnection,
) -> Result<(), JsValue> {
    let deadline = now_ms() + CHANNEL_OPEN_DEADLINE_MS;
    loop {
        let ice = peer_connection.ice_connection_state();
        if ice == RtcIceConnectionState::Failed {
            return Err(JsValue::from_str(
                "ICE failed — no route between the peers (host/mDNS blocked and TURN did not connect)",
            ));
        }
        match data_channel.ready_state() {
            web_sys::RtcDataChannelState::Open => return Ok(()),
            web_sys::RtcDataChannelState::Closing | web_sys::RtcDataChannelState::Closed => {
                return Err(JsValue::from_str(&format!(
                    "data channel closed during setup (ready_state={}, ice_connection_state={}, ice_gathering_state={})",
                    channel_state_label(data_channel.ready_state()),
                    ice_state_label(ice),
                    gather_state_label(peer_connection.ice_gathering_state()),
                )));
            }
            // Still negotiating. `_` rides along because `RtcDataChannelState`
            // is a generated web-sys enum that can grow a variant without us
            // touching anything, and "keep waiting" is the right answer to a
            // state we have not heard of.
            web_sys::RtcDataChannelState::Connecting | _ => {}
        }
        if now_ms() >= deadline {
            return Err(JsValue::from_str(&format!(
                "data channel never opened (ready_state={}, ice_connection_state={}, ice_gathering_state={})",
                channel_state_label(data_channel.ready_state()),
                ice_state_label(ice),
                gather_state_label(peer_connection.ice_gathering_state()),
            )));
        }
        sleep_ms(POLL_MS).await;
    }
}

async fn sleep_ms(millis: i32) {
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        if let Some(window) = web_sys::window() {
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, millis);
        } else {
            // Avoid hanging forever outside a Window (e.g. some worker contexts).
            let _ = resolve.call0(&JsValue::NULL);
        }
    });
    let _ = JsFuture::from(promise).await;
}
