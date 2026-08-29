//! The gossip accept gate: on a mesh whose relay is lookup only, an inbound
//! `GOSSIP_ALPN` connection is handed to iroh-gossip only once iroh has
//! selected a direct path on it. Nothing is read from the connection while
//! it is held, so no gossip frame — membership or payload — crosses the
//! relay. The rendezvous link is gated like every other: it starts on the
//! relay by construction and punches a direct path inside the connection
//! like any peer link. iroh-gossip's dialer has no handshake timeout, so the
//! far side simply waits.

use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh_gossip::net::Gossip;

use super::path::{GOSSIP_RELAY_REFUSED_CODE, PROBE_DEADLINE, refuse_unless_direct};

#[derive(Debug, Clone)]
pub(crate) struct DirectOnlyGossip {
    inner: Gossip,
    relay_transport: bool,
}

impl DirectOnlyGossip {
    pub(crate) fn new(inner: Gossip, relay_transport: bool) -> Self {
        Self {
            inner,
            relay_transport,
        }
    }
}

impl ProtocolHandler for DirectOnlyGossip {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
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
