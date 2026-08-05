//! QUIC datagrams over a `WebRTC` data channel, as an iroh custom transport —
//! on the host and in the browser.
//!
//! One data channel per remote peer, one QUIC datagram per binary SCTP
//! message, no extra framing. The channel is negotiated **unreliable and
//! unordered** (`maxRetransmits: 0`): QUIC above it owns loss recovery and
//! congestion control, so reliable ordered SCTP underneath would stack a
//! second retransmission loop on every loss. Structure follows
//! [iroh-multihop-transport](https://github.com/fofoca-network/iroh-multihop-transport)
//! and the upstream
//! [iroh-tor-transport](https://github.com/n0-computer/iroh-tor-transport):
//! a transport factory, a per-endpoint receiver, a registry-backed sender, and
//! a per-session driver pumping the connection.
//!
//! # Why one crate with two backends
//!
//! `str0m` and `tokio` do not target `wasm32`; `web-sys` does not exist off the
//! browser. The *drivers* therefore cannot be one implementation. But the
//! protocol — the JSEP envelope, the transport id, the address convention — is
//! identical, and it is exactly the part that must not drift: two peers that
//! disagree about the transport id or the envelope shape fail to connect with
//! no useful error.
//!
//! So the protocol half lives at the crate root, always compiled and free of
//! host- and browser-only dependencies, and each backend sits behind a feature:
//!
//! - `native` — sans-io [`str0m`] driven by tokio, plus STUN gathering ([`stun`]).
//! - `web` — the browser's own `RTCPeerConnection` through `web-sys`.
//!
//! Neither is on by default; a consumer enables the one matching its target.
//!
//! # NAT traversal
//!
//! `str0m` gathers no candidates — it owns no sockets, so discovery is the
//! caller's job. Skipping it (as the experiment this was ported from did) means
//! advertising a single host candidate on a private interface, which connects
//! on one LAN and nowhere else. [`stun`] closes that on the host side; the
//! browser's ICE agent does it natively once given `iceServers`.
//!
//! The iroh relay carries the SDP exchange. **TURN is refused** — see
//! [`accept_ice_uri`]: a consumer that cannot pair directly falls back to that
//! same iroh relay rather than to a second relay at the ICE layer. Neither
//! backend has a TURN client, and neither should grow one.

mod addr;
mod ice_uri;
// Consumed by the browser backend. The host backend has its own equivalent in
// `native::session`, built the same way for the same reasons — a generation per
// session, and teardown on drop rather than on a cleanup branch.
//
// Compiled in every configuration even so, because this is the only place that
// logic is *provable*: the browser backend cannot be tested at all (no
// `RTCPeerConnection` in node, no webdriver in CI), so its decisions were
// pulled out into a payload-generic type that `cargo test` reaches whatever
// features are on.
#[cfg_attr(
    not(feature = "web"),
    expect(
        dead_code,
        reason = "only the browser backend consumes it; its tests still run"
    )
)]
mod registry;
#[cfg(any(feature = "native", feature = "web"))]
mod selector;
mod signaling;

pub use addr::{WEBRTC_TRANSPORT_ID, custom_addr, parse_custom_addr};
pub use ice_uri::accept_ice_uri;
pub use signaling::{MAX_ENVELOPE_BYTES, SIGNAL_VERSION, SignalEnvelope};

/// Label of the single data channel each session carries. Both ends must use
/// the same string or the channel never opens.
pub const DATA_CHANNEL_LABEL: &str = "iroh";

#[cfg(feature = "native")]
mod native;

#[cfg(feature = "native")]
pub use native::{
    IceConfig, NegotiatedSession, PendingAnswer, PendingOffer, WebRtcTransport, answer,
    answer_with, offer, offer_with, stun,
};

#[cfg(feature = "web")]
mod web;

#[cfg(feature = "web")]
pub use web::{
    AttachError, BrowserHubTransport, BrowserRtcTransport, BrowserSession, BrowserSessionGuard,
    IceServer, IceServers, PendingAnswer as BrowserPendingAnswer,
    PendingOffer as BrowserPendingOffer, answer as browser_answer, log_signal_sdps,
    offer as browser_offer,
};

/// A registered `WebRTC` transport, ready to hand to an iroh endpoint builder.
///
/// The same name on both targets so a consumer's wiring code is written once:
/// which backend it wraps is decided by the feature, not by the caller. Cheap
/// to clone — it is a handle, and every clone shares one session registry.
#[cfg(any(feature = "native", feature = "web"))]
#[derive(Debug, Clone)]
pub struct WebRtcHandle {
    #[cfg(feature = "native")]
    inner: std::sync::Arc<WebRtcTransport>,
    #[cfg(all(feature = "web", not(feature = "native")))]
    inner: std::sync::Arc<BrowserHubTransport>,
}

#[cfg(feature = "native")]
impl WebRtcHandle {
    /// Wrap a host transport.
    #[must_use]
    pub fn new(transport: std::sync::Arc<WebRtcTransport>) -> Self {
        Self { inner: transport }
    }

    /// The transport to register with `Builder::add_custom_transport`.
    ///
    /// Registration is deliberately additive rather than a `Preset`: a preset
    /// would make `WebRTC` the endpoint's *only* transport, which is right for a
    /// browser and wrong for a native peer that should still prefer iroh's own
    /// hole-punched paths.
    #[must_use]
    pub fn transport(&self) -> std::sync::Arc<WebRtcTransport> {
        std::sync::Arc::clone(&self.inner)
    }

    /// The path selector that keeps the relay a rendezvous, for
    /// `Builder::path_selector`. Install it wherever [`Self::transport`] is
    /// registered — see [`crate::selector`] for why registering the transport
    /// alone is not enough.
    #[must_use]
    pub fn path_selector(&self) -> std::sync::Arc<dyn iroh::endpoint::transports::PathSelector> {
        std::sync::Arc::new(selector::WebRtcPreferred)
    }

    /// Attach a negotiated session for `remote`.
    ///
    /// # Errors
    /// The registry rejects the session (already attached).
    pub fn attach(
        &self,
        remote: iroh_base::EndpointId,
        session: NegotiatedSession,
    ) -> anyhow::Result<()> {
        self.inner.attach(remote, session)
    }

    /// Whether a usable session for `remote` exists.
    #[must_use]
    pub fn has_session(&self, remote: &iroh_base::EndpointId) -> bool {
        self.inner.has_session(remote)
    }

    /// How many usable sessions this handle's registry holds.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.inner.session_count()
    }

    /// Tear down the session for `remote`, if any.
    #[must_use]
    pub fn detach(&self, remote: &iroh_base::EndpointId) -> bool {
        self.inner.detach(remote)
    }
}

#[cfg(all(feature = "web", not(feature = "native")))]
impl WebRtcHandle {
    /// Wrap a browser hub transport (consumer or producer).
    #[must_use]
    pub fn new(transport: std::sync::Arc<BrowserHubTransport>) -> Self {
        Self { inner: transport }
    }

    /// Empty hub for `local`, ready to register and later [`Self::attach`].
    #[must_use]
    pub fn hub(local: iroh_base::EndpointId) -> Self {
        Self::new(BrowserHubTransport::new(local))
    }

    /// The transport to register with `Builder::add_custom_transport`.
    #[must_use]
    pub fn transport(&self) -> std::sync::Arc<BrowserHubTransport> {
        std::sync::Arc::clone(&self.inner)
    }

    /// The path selector that keeps the relay a rendezvous, for
    /// `Builder::path_selector`. Install it wherever [`Self::transport`] is
    /// registered — see [`crate::selector`] for why registering the transport
    /// alone is not enough.
    #[must_use]
    pub fn path_selector(&self) -> std::sync::Arc<dyn iroh::endpoint::transports::PathSelector> {
        std::sync::Arc::new(selector::WebRtcPreferred)
    }

    /// Attach a negotiated browser session for `remote`.
    ///
    /// Prefer calling [`BrowserPendingOffer::complete`] /
    /// [`BrowserPendingAnswer::complete`], which attach themselves; this is
    /// the escape hatch when the session pieces are already in hand.
    ///
    /// # Errors
    /// The peer's slot is already claimed. The handles passed in are closed
    /// before returning.
    pub fn attach_parts(
        &self,
        remote: iroh_base::EndpointId,
        peer_connection: web_sys::RtcPeerConnection,
        data_channel: web_sys::RtcDataChannel,
        callbacks: Vec<wasm_bindgen::JsValue>,
    ) -> Result<BrowserSessionGuard, AttachError> {
        self.inner
            .attach(remote, peer_connection, data_channel, callbacks)
    }

    /// Whether a usable session for `remote` exists.
    #[must_use]
    pub fn has_session(&self, remote: &iroh_base::EndpointId) -> bool {
        self.inner.has_session(remote)
    }

    /// How many usable sessions this handle's registry holds.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.inner.session_count()
    }

    /// Endpoint ids of every live session.
    #[must_use]
    pub fn live_peer_ids(&self) -> Vec<iroh_base::EndpointId> {
        self.inner.live_peer_ids()
    }

    /// Selected ICE remote candidate for a live session, if any.
    pub async fn selected_remote_candidate(
        &self,
        remote: &iroh_base::EndpointId,
    ) -> Option<(String, String)> {
        self.inner.selected_remote_candidate(remote).await
    }

    /// Tear down the session for `remote`, if any.
    #[must_use]
    pub fn detach(&self, remote: &iroh_base::EndpointId) -> bool {
        self.inner.detach(remote)
    }
}
