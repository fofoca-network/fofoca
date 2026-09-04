//! The dial side of the unicast plane: a per-peer connection pool that dials
//! lazily, reuses a warm QUIC connection across messages, and re-dials after a
//! close. Modeled on the application bridge's shared connection, but keyed per endpoint
//! and with a short dial budget so a send to an unreachable peer fails fast
//! rather than stalling the event loop.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Result, bail};
use bytes::Bytes;
use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointAddr, EndpointId};
use tokio::sync::Mutex;

use super::path::wait_direct;
use super::{LOG_TARGET, RELAY_REFUSED, UNICAST_ALPN, payload_allowed_on};

use crate::util::clock::Instant;
use crate::util::cooldown::Cooldown;

/// How long an inline dial keeps trying before giving up. Deliberately short —
/// far under the application's 90s discovery deadline — because the dial blocks the send,
/// and an unreachable peer should surface as an error now, not stall the
/// caller. The addressee's `EndpointAddr` is already registered with the
/// endpoint (`add_peer_addr`), so a reachable peer resolves well inside this.
const DIAL_TIMEOUT: Duration = Duration::from_secs(3);

/// After a failed dial the endpoint goes on cooldown: further cold sends to it
/// error immediately instead of re-dialing. The dial blocks the event loop, so
/// without this a burst of directed frames to one dead peer (a 16-shard repair
/// reply, a flushed backlog) serializes a [`DIAL_TIMEOUT`] stall per frame —
/// the cooldown bounds that to one stall per window per peer.
const DIAL_FAILURE_COOLDOWN: Duration = Duration::from_secs(10);

/// How long a *fresh* inline dial may wait for iroh to select a non-relay
/// path before the send is refused. Shorter than the accept side's
/// `PROBE_DEADLINE`: this wait blocks the event loop exactly like the dial
/// itself, so it gets a dial-sized budget, not an accept-sized one — and a
/// punch or a WebRTC path-open lands in single-digit seconds when it lands
/// at all.
const PATH_SELECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct UnicastPool {
    inner: Arc<PoolInner>,
}

impl std::fmt::Debug for UnicastPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnicastPool").finish_non_exhaustive()
    }
}

struct PoolInner {
    /// `None` for a detached pool (unit-test states / pre-wiring default): every
    /// operation is inert, so a directed send simply errors instead of dialing.
    endpoint: Option<Endpoint>,
    conns: Mutex<HashMap<EndpointId, Connection>>,
    /// When each endpoint's last dial failed, for the
    /// [`DIAL_FAILURE_COOLDOWN`] gate. An entry clears on a successful dial or
    /// a graceful `Left`; expired ones are pruned on the next `note`.
    dial_failures: Mutex<Cooldown<EndpointId>>,
    /// Times the inline-dial path was entered. Counted on entry rather than at
    /// the dial itself, because the question a caller on the event loop needs
    /// answered is whether it *could* have waited here — a detached pool bails
    /// before dialing, and a warm hit inside never dials, but both mean the
    /// caller was willing to.
    dial_attempts: AtomicU64,
    /// `TransportPolicy::relay`. Off, a send on a connection whose selected
    /// path is the relay is refused; the connection stays pooled, since iroh
    /// may still punch a direct path on it.
    relay_transport: bool,
}

/// What [`UnicastPool::send_if_warm`] did with the frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WarmSend {
    /// Handed to a warm connection.
    Sent,
    /// No warm connection: the caller dials inline.
    Cold,
    /// A warm connection exists, but its selected path is the relay and the
    /// relay is lookup only. Dialing would find the same connection.
    Refused,
}

impl UnicastPool {
    /// A pool wired to `endpoint`, able to dial and carry unicast traffic.
    pub(crate) fn new(endpoint: Endpoint, relay_transport: bool) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                endpoint: Some(endpoint),
                conns: Mutex::new(HashMap::new()),
                dial_failures: Mutex::new(Cooldown::new(DIAL_FAILURE_COOLDOWN)),
                dial_attempts: AtomicU64::new(0),
                relay_transport,
            }),
        }
    }

    /// A detached pool with no endpoint — every operation is a no-op. The
    /// default an [`EventLoopState`](crate::daemon::state::EventLoopState) holds
    /// until the real loop installs an endpoint-backed pool; also what the
    /// lower-level unit tests (which never run the transport) get.
    pub(crate) fn disconnected() -> Self {
        Self {
            inner: Arc::new(PoolInner {
                endpoint: None,
                conns: Mutex::new(HashMap::new()),
                dial_failures: Mutex::new(Cooldown::new(DIAL_FAILURE_COOLDOWN)),
                dial_attempts: AtomicU64::new(0),
                relay_transport: false,
            }),
        }
    }

    /// Send `bytes` over a warm connection to `eid` if one exists, returning
    /// `true` on a fire-and-forget handoff. `false` means no warm connection —
    /// the caller then dials inline via [`Self::dial_and_send`]. The actual
    /// stream write is spawned so the event loop never blocks on QUIC I/O
    /// (awaiting it wouldn't buy delivery truth anyway — QUIC buffers locally,
    /// so a write "succeeds" before the path is proven). A write that fails on
    /// a half-dead pooled connection evicts it and redials-and-resends once in
    /// the background, so the detectable failure mode recovers instead of
    /// silently losing the frame; a path that dies *after* buffering remains
    /// at-most-once (an app-level concern: waiter timeouts, and anti-entropy,
    /// which re-sends a directed frame point-to-point to its addressee).
    pub(crate) async fn send_if_warm(&self, eid: EndpointId, bytes: Bytes) -> WarmSend {
        let Some(conn) = self.warm(eid).await else {
            return WarmSend::Cold;
        };
        if !payload_allowed_on(&conn, self.inner.relay_transport) {
            return WarmSend::Refused;
        }
        let pool = self.clone();
        n0_future::task::spawn(async move {
            let Err(error) = send_one(&conn, &bytes).await else {
                return;
            };
            tracing::debug!(target: LOG_TARGET, %error, "unicast send failed; dropping connection and redialing");
            pool.inner.conns.lock().await.remove(&eid);
            if let Err(redial_error) = pool.dial_and_send(eid, bytes).await {
                tracing::debug!(target: LOG_TARGET, %redial_error, "redial after a failed warm send also failed; frame dropped");
            }
        });
        WarmSend::Sent
    }

    /// The pooled connection to `eid`, if one is open.
    async fn warm(&self, eid: EndpointId) -> Option<Connection> {
        let conns = self.inner.conns.lock().await;
        conns
            .get(&eid)
            .filter(|conn| conn.close_reason().is_none())
            .cloned()
    }

    /// How many times the inline-dial path was entered, for tests asserting a
    /// caller stayed off it.
    #[cfg(test)]
    pub(crate) fn dial_attempts(&self) -> u64 {
        self.inner.dial_attempts.load(Ordering::Relaxed)
    }

    /// Ensure a connection to `eid` (reusing a warm one or dialing inline) and
    /// send `bytes` over it, awaiting the handoff. The cold path for every
    /// directed send — there is no other transport to carry the first message.
    ///
    /// # Errors
    /// A detached pool, an endpoint on failed-dial cooldown, a dial that
    /// fails/times out, or a stream write error.
    pub(crate) async fn dial_and_send(&self, eid: EndpointId, bytes: Bytes) -> Result<()> {
        self.inner.dial_attempts.fetch_add(1, Ordering::Relaxed);
        let conn = self.warm_or_dial(eid).await?;
        // A connection this call just dialed is milliseconds old: its
        // non-relay path (the punch, or a WebRTC path opened right after
        // `AddConnection`) is still forming, and the synchronous check read
        // "no path selected yet" as relay — refusing the *first* directed
        // frame to every peer without a warm connection. Wait like the
        // accept side does, on a dial-sized budget.
        if !self.inner.relay_transport && !wait_direct(&conn, PATH_SELECT_TIMEOUT).await {
            bail!("{RELAY_REFUSED}");
        }
        if let Err(error) = send_one(&conn, &bytes).await {
            self.inner.conns.lock().await.remove(&eid);
            return Err(error);
        }
        Ok(())
    }

    /// The pooled connection to `eid`, dialing one if none is warm. The
    /// direct-path probe uses this so its connection *is* the one later
    /// unicast rides — one handshake, and a punch landed here serves both.
    ///
    /// # Errors
    /// See [`Self::dial_and_send`].
    pub(crate) async fn warm_or_dial(&self, eid: EndpointId) -> Result<Connection> {
        let Some(endpoint) = self.inner.endpoint.clone() else {
            bail!("unicast pool has no endpoint");
        };
        if let Some(conn) = self.warm(eid).await {
            return Ok(conn);
        }
        if self
            .inner
            .dial_failures
            .lock()
            .await
            .on_cooldown(&eid, Instant::now())
        {
            bail!("unicast dial on cooldown after a recent failure");
        }
        match dial(&endpoint, eid).await {
            Ok(conn) => {
                self.inner.dial_failures.lock().await.forget(&eid);
                self.inner.conns.lock().await.insert(eid, conn.clone());
                Ok(conn)
            }
            Err(error) => {
                self.inner
                    .dial_failures
                    .lock()
                    .await
                    .note(eid, Instant::now());
                Err(error)
            }
        }
    }

    /// Drop `eid`'s pooled connection and dial-failure cooldown: the peer left
    /// gracefully, so neither may serve or block a future dial (a rejoin
    /// re-dials cold). The connection is closed explicitly — `Connection` is a
    /// cheap clone handle, and in-flight spawned writers hold clones, so just
    /// dropping the map entry would leave the QUIC connection open until every
    /// clone drops.
    pub(crate) async fn forget(&self, eid: EndpointId) {
        if let Some(conn) = self.inner.conns.lock().await.remove(&eid) {
            conn.close(0u32.into(), b"peer left");
        }
        self.inner.dial_failures.lock().await.forget(&eid);
    }
}

/// Dial `eid` on the unicast ALPN within [`DIAL_TIMEOUT`]. The endpoint already
/// knows the peer's address (registered via `add_peer_addr`), so a bare-id
/// `EndpointAddr` resolves through the endpoint's address book + lookups.
async fn dial(endpoint: &Endpoint, eid: EndpointId) -> Result<Connection> {
    match n0_future::time::timeout(
        DIAL_TIMEOUT,
        endpoint.connect(EndpointAddr::new(eid), UNICAST_ALPN),
    )
    .await
    {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(error)) => Err(anyhow::anyhow!("{error}")),
        Err(_) => bail!("unicast dial timed out after {DIAL_TIMEOUT:?}"),
    }
}

/// One message per unidirectional stream: open, write the whole frame, finish.
/// No length framing is needed — the accept side reads the stream to EOF and
/// gets exactly one serialized `Message`. The finished stream keeps flushing
/// after it is dropped because the connection stays pooled, so there is no need
/// to await `stopped()` (which would block the event loop on the inline
/// unicast-only path).
async fn send_one(conn: &Connection, bytes: &[u8]) -> Result<()> {
    let mut stream = conn.open_uni().await?;
    stream.write_all(bytes).await?;
    stream.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::testing::endpoint_id;
    use crate::util::clock::Instant;
    use std::time::Duration;

    use super::{Cooldown, DIAL_FAILURE_COOLDOWN};

    #[test]
    fn a_fresh_failure_puts_the_endpoint_on_cooldown() {
        let mut failures = Cooldown::new(DIAL_FAILURE_COOLDOWN);
        let bob = endpoint_id(1);
        let now = Instant::now();
        failures.note(bob, now);
        assert!(failures.on_cooldown(&bob, now));
        // Another endpoint is unaffected.
        assert!(!failures.on_cooldown(&endpoint_id(2), now));
    }

    #[test]
    fn an_expired_failure_allows_the_dial_and_is_pruned() {
        let mut failures = Cooldown::new(DIAL_FAILURE_COOLDOWN);
        let bob = endpoint_id(1);
        let failed_at = Instant::now();
        failures.note(bob, failed_at);
        let later = failed_at + DIAL_FAILURE_COOLDOWN + Duration::from_millis(1);
        assert!(!failures.on_cooldown(&bob, later));
        // Pruning happens on the next write rather than on the read, so the
        // table still cannot grow without bound.
        failures.note(endpoint_id(2), later);
        assert_eq!(
            failures.len(),
            1,
            "the stale entry is dropped, not retained"
        );
    }

    /// A graceful `Left` forgets the peer's pool slots: the dial cooldown is
    /// cleared so a rejoin dials immediately, and forgetting an unknown
    /// endpoint is a no-op. (Closing a warm `Connection` needs a live
    /// endpoint pair; that path is covered by the network suite.)
    #[tokio::test]
    async fn forget_clears_the_cooldown_and_tolerates_absence() {
        let pool = super::UnicastPool::disconnected();
        let bob = endpoint_id(1);
        pool.inner
            .dial_failures
            .lock()
            .await
            .note(bob, Instant::now());

        pool.forget(bob).await;

        assert!(pool.inner.dial_failures.lock().await.is_empty());
        assert!(pool.inner.conns.lock().await.is_empty());
        // Absent endpoint: nothing to drop, nothing panics.
        pool.forget(endpoint_id(2)).await;
    }
}
