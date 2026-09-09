//! Probe, then graft: on a mesh whose relay is lookup only, a peer joins our
//! gossip overlay only once a direct path to it is proven.
//!
//! iroh opens the first connection to a peer behind NAT over the relay and
//! punches a direct path *inside* it; a gossip link opened on that connection
//! would carry every frame relayed until the punch lands — and, for a `WebRTC`
//! peer, for the life of the link, since a custom-transport path is never
//! added to a live connection. So the graft waits: an IP peer is probed with
//! the unicast pool's own connection until iroh selects a non-relay path, a
//! browser peer until its `WebRTC` session is attached. Both bounded — a
//! peer that never proves a direct path stays `RelayOnly`, retried on the
//! alive tick, and is never grafted through the relay.

use iroh::EndpointId;

use super::path::{PROBE_DEADLINE, wait_direct};
use super::webrtc::needs_webrtc_lane;
use crate::daemon::ctx::HandlerCtx;
use crate::daemon::state::{DirectState, EventLoopState};
use crate::util::clock::Instant;

/// A probe's verdict, reported back to the event loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DirectOutcome {
    pub(crate) peer: EndpointId,
    pub(crate) direct: bool,
}

/// Pure: may a peer be grafted, from what is known right now? A `WebRTC`
/// pair needs its session attached; an IP pair needs a proven direct path.
pub(crate) fn may_graft(known_direct: bool, has_session: bool, needs_webrtc: bool) -> bool {
    if needs_webrtc {
        has_session
    } else {
        known_direct
    }
}

/// Whether `peer` may be grafted right now. `false` means a probe is in
/// flight or a `WebRTC` session is still being negotiated; the loop grafts on
/// the [`DirectOutcome`] that follows, or the alive tick retries.
pub(crate) fn ensure_direct(
    state: &mut EventLoopState,
    ctx: &HandlerCtx<'_>,
    peer: EndpointId,
    peer_addr: &iroh::EndpointAddr,
) -> bool {
    if state.relay_transport {
        return true;
    }
    let needs_webrtc = needs_webrtc_lane(peer_addr) || needs_webrtc_lane(&ctx.endpoint.addr());
    let has_session = state
        .webrtc
        .as_ref()
        .is_some_and(|handle| handle.has_session(&peer));
    let known = state.direct.get(&peer).copied();
    if may_graft(
        known == Some(DirectState::Direct),
        has_session,
        needs_webrtc,
    ) {
        state.direct.insert(peer, DirectState::Direct);
        return true;
    }
    if needs_webrtc || known == Some(DirectState::Pending) {
        // A browser peer proves itself through `negotiate_session`; an IP
        // peer's probe is already running.
        state.direct.entry(peer).or_insert(DirectState::Pending);
        return false;
    }
    state.direct.insert(peer, DirectState::Pending);
    let pool = state.unicast_pool.clone();
    let tx = state.direct_proven.clone();
    n0_future::task::spawn(async move {
        let direct = match pool.warm_or_dial(peer).await {
            Ok(conn) => wait_direct(&conn, PROBE_DEADLINE).await,
            Err(error) => {
                tracing::debug!(target: super::LOG_TARGET, %peer, %error, "direct-path probe could not connect");
                false
            }
        };
        let _ = tx.send(DirectOutcome { peer, direct });
    });
    false
}

/// Apply a probe's verdict: a proven peer is grafted, an unproven one is
/// recorded `RelayOnly` for the alive tick to retry, unless it proved itself
/// another way while the probe ran.
pub(crate) async fn on_outcome(
    outcome: DirectOutcome,
    state: &mut EventLoopState,
    ctx: &HandlerCtx<'_>,
) {
    let DirectOutcome { peer, direct } = outcome;
    if direct {
        graft_proven(state, ctx, peer).await;
    } else if state.demote_unproven(peer) {
        tracing::info!(target: super::LOG_TARGET, %peer, "no direct path within the probe deadline; peer stays relay-only");
    } else {
        tracing::debug!(target: super::LOG_TARGET, %peer, "late probe verdict ignored; the peer is no longer pending");
    }
}

/// Record `peer` as `Direct`, graft it if it is not linked already and there
/// is room, and flush any frame parked for it.
pub(crate) async fn graft_proven(
    state: &mut EventLoopState,
    ctx: &HandlerCtx<'_>,
    peer: EndpointId,
) {
    state.direct.insert(peer, DirectState::Direct);
    if !state.linked_endpoints.contains(&peer) && state.linked_endpoints.len() < ctx.max_peers {
        state.note_relink(peer, Instant::now());
        if let Err(error) = ctx.sender.join_peers(vec![peer]).await {
            tracing::warn!(target: super::LOG_TARGET, %peer, %error, "graft request failed");
        }
    }
    if state.meshed && !state.pending_outbound.is_empty() {
        crate::gossip::flush_pending(state, ctx, "direct path proven").await;
    }
}

/// The alive-tick retry: every known peer that is neither linked nor mid-probe
/// gets another `ensure_direct`, subject to the relink cooldown. A no-op while
/// the relay may carry payload. The rendezvous is skipped: it accepts no
/// unicast, so it cannot be probed; its link is gated on the beacon's side.
///
/// With `distrust_links` (the re-bridge after a resume or starvation) every
/// link and proven path is stale by definition, so linked peers are retried
/// too and their `Direct` verdicts are forgotten first, or `ensure_direct`
/// would trust the pre-sleep answer.
pub(crate) async fn retry_direct(
    state: &mut EventLoopState,
    ctx: &HandlerCtx<'_>,
    distrust_links: bool,
) {
    if state.relay_transport {
        return;
    }
    for addr in retry_candidates(state, ctx.rendezvous_id, distrust_links) {
        if distrust_links {
            state.direct.remove(&addr.id);
        }
        if ensure_direct(state, ctx, addr.id, &addr) {
            graft_proven(state, ctx, addr.id).await;
        }
    }
}

/// The peers one `retry_direct` pass probes, in a fixed order.
fn retry_candidates(
    state: &EventLoopState,
    rendezvous_id: EndpointId,
    distrust_links: bool,
) -> Vec<iroh::EndpointAddr> {
    let now = Instant::now();
    let mut peers: Vec<iroh::EndpointAddr> = state
        .peer_endpoints
        .values()
        .filter(|addr| addr.id != rendezvous_id)
        .filter(|addr| distrust_links || !state.linked_endpoints.contains(&addr.id))
        .filter(|addr| {
            state.direct.get(&addr.id) != Some(&DirectState::Pending)
                || (needs_webrtc_lane(addr)
                    && state
                        .webrtc
                        .as_ref()
                        .is_some_and(|handle| handle.has_session(&addr.id)))
        })
        .filter(|addr| !state.relink_on_cooldown(addr.id, now))
        .cloned()
        .collect();
    peers.sort_unstable_by_key(|addr| addr.id);
    peers
}

#[cfg(test)]
mod tests {
    use iroh::EndpointAddr;

    use super::{may_graft, retry_candidates};
    use crate::testing::{endpoint_id, fresh_state, nick};

    // `linked_endpoints` is not cleared on the resume edge, so a re-bridge
    // that trusted it skipped exactly the peers it exists to re-dial.
    #[test]
    fn a_re_bridge_keeps_linked_peers_as_candidates() {
        let mut state = fresh_state();
        state.relay_transport = false;
        let rendezvous = endpoint_id(1);
        let linked = endpoint_id(2);
        let unlinked = endpoint_id(3);
        for (name, id) in [
            ("beacon", rendezvous),
            ("linked", linked),
            ("unlinked", unlinked),
        ] {
            state
                .peer_endpoints
                .insert(nick(name), EndpointAddr::new(id));
        }
        state.linked_endpoints.insert(linked);
        let ids = |distrust_links: bool| -> Vec<_> {
            retry_candidates(&state, rendezvous, distrust_links)
                .into_iter()
                .map(|addr| addr.id)
                .collect()
        };

        assert_eq!(
            ids(false),
            [unlinked],
            "the alive tick leaves a linked peer alone"
        );
        let mut expected = [linked, unlinked];
        expected.sort_unstable();
        assert_eq!(
            ids(true),
            expected,
            "the re-bridge re-dials the linked peer too"
        );
    }

    #[test]
    fn ip_peer_needs_a_proven_direct_path() {
        assert!(may_graft(true, false, false));
        assert!(!may_graft(false, true, false));
    }

    #[test]
    fn webrtc_peer_needs_its_session_not_a_path() {
        assert!(may_graft(false, true, true));
        assert!(!may_graft(true, false, true));
    }
}
