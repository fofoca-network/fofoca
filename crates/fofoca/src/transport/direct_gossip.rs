//! The gossip accept gate: on a mesh whose relay is lookup only, an inbound
//! `GOSSIP_ALPN` connection is handed to iroh-gossip only once iroh has
//! selected a direct path on it. Nothing is read from the connection while
//! it is held, so no gossip frame — membership or payload — crosses the
//! relay. The rendezvous link is gated like every other: it starts on the
//! relay by construction and punches a direct path inside the connection
//! like any peer link. iroh-gossip's dialer has no handshake timeout, so the
//! far side simply waits.

use fofoca_iroh_webrtc_transport::WebRtcHandle;
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh_gossip::net::Gossip;

use super::path::{GOSSIP_RELAY_REFUSED_CODE, PROBE_DEADLINE, refuse_unless_direct};

#[derive(Debug, Clone)]
pub(crate) struct DirectOnlyGossip {
    inner: Gossip,
    relay_transport: bool,
    /// Set on a node with no UDP path, whose only direct path to anyone is a
    /// `WebRTC` session. See [`DirectOnlyGossip::accept`].
    session_gate: Option<(WebRtcHandle, super::SignalAdmission)>,
}

impl DirectOnlyGossip {
    pub(crate) fn new(
        inner: Gossip,
        relay_transport: bool,
        session_gate: Option<(WebRtcHandle, super::SignalAdmission)>,
    ) -> Self {
        Self {
            inner,
            relay_transport,
            session_gate,
        }
    }
}

impl ProtocolHandler for DirectOnlyGossip {
    /// On a node without UDP, a connection from a peer it holds no session
    /// with, and is not negotiating one with, is refused at once rather than
    /// held. It can only go direct once a session attaches, and the session's
    /// offerer grafts the pair over it the moment it does. A round in flight
    /// is held as before: the offerer attaches first and may dial before this
    /// side has. Held, it would be a second connection for the same
    /// pair: iroh-gossip dedups a pair by closing the connection each side
    /// dialed, and the HyParView reply already sent on the closed one is
    /// lost, which leaves the pair half-linked until a heal re-grafts it.
    /// This connection is typically the far side's `ForwardJoin` dial, sent
    /// through the rendezvous before any session exists.
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let remote = conn.remote_id();
        if !self.relay_transport
            && let Some((webrtc, admission)) = &self.session_gate
            && refuse_before_session(webrtc.has_session(&remote), admission.negotiating(remote))
        {
            tracing::debug!(target: super::LOG_TARGET, remote = %conn.remote_id(), "gossip dial before its webrtc session refused");
            conn.close(GOSSIP_RELAY_REFUSED_CODE.into(), b"no webrtc session yet");
            return Ok(());
        }
        if !refuse_unless_direct(
            &conn,
            self.relay_transport,
            PROBE_DEADLINE,
            GOSSIP_RELAY_REFUSED_CODE,
        )
        .await
        {
            return Ok(());
        }
        self.inner
            .handle_connection(conn)
            .await
            .map_err(AcceptError::from_err)
    }

    async fn shutdown(&self) {
        if let Err(error) = self.inner.shutdown().await {
            tracing::warn!(target: super::LOG_TARGET, %error, "error while shutting down gossip");
        }
    }
}

/// Whether the gate refuses a gossip dial at once rather than holding it,
/// from what this node knows about the dialer's `WebRTC` session.
fn refuse_before_session(has_session: bool, negotiating: bool) -> bool {
    !has_session && !negotiating
}

#[cfg(test)]
mod tests {
    use super::refuse_before_session;

    // The offerer attaches one DTLS trip before the answerer does, and grafts
    // the moment it attaches, so its dial can reach the answerer mid-round.
    #[test]
    fn a_dial_during_the_round_is_held_not_refused() {
        assert!(refuse_before_session(false, false), "no session, no round");
        assert!(
            !refuse_before_session(false, true),
            "the round is finishing"
        );
        assert!(!refuse_before_session(true, false), "the session is up");
    }
}
