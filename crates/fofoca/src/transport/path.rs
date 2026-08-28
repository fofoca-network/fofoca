//! Which kind of path a connection's data is on right now, and whether the
//! mesh's transport policy lets payload ride it.
//!
//! iroh keeps every path it has to a peer open and picks one to carry data;
//! the relay path is never closed, only demoted. So "is this peer relayed" is
//! a question about the *selected* path, not about which paths exist —
//! `conn_path` in `gossip` answers the latter, for diagnostics.

use iroh::TransportAddr;
use iroh::endpoint::Connection;

/// The kind of path a connection's selected path is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathKind {
    /// A direct UDP path, hole-punched or not.
    Ip,
    /// The iroh relay server.
    Relay,
    /// A custom transport (`WebRTC`, multihop): peer to peer as far as the
    /// relay is concerned.
    Custom,
}

/// The selected path's kind, or `None` while iroh has not selected one.
pub(crate) fn selected_kind(conn: &Connection) -> Option<PathKind> {
    conn.paths()
        .iter()
        .find(iroh::endpoint::Path::is_selected)
        .map(|path| kind_of(path.remote_addr()))
}

fn kind_of(addr: &TransportAddr) -> PathKind {
    // `TransportAddr` is `#[non_exhaustive]`; anything iroh adds later is by
    // definition not the relay.
    match addr {
        TransportAddr::Ip(_) => PathKind::Ip,
        TransportAddr::Relay(_) => PathKind::Relay,
        TransportAddr::Custom(_) | _ => PathKind::Custom,
    }
}

/// Whether payload may go out on a path of kind `selected` under the mesh's
/// transport policy. With the relay allowed as a transport, anything goes,
/// even an as-yet-unselected path. With the relay lookup only, the path must
/// be a proven non-relay one: "not selected yet" is refused, not trusted.
pub(crate) fn payload_allowed(selected: Option<PathKind>, relay_transport: bool) -> bool {
    relay_transport || matches!(selected, Some(PathKind::Ip | PathKind::Custom))
}

/// [`payload_allowed`] for a live connection.
pub(crate) fn payload_allowed_on(conn: &Connection, relay_transport: bool) -> bool {
    payload_allowed(selected_kind(conn), relay_transport)
}

/// The refusal every payload lane reports when the relay is lookup only and
/// the only path is the relay. One string, so a log reader can grep for it.
pub(crate) const RELAY_REFUSED: &str =
    "relay-only path refused: the relay is lookup only on this mesh";

#[cfg(test)]
mod tests {
    use super::{PathKind, payload_allowed};

    #[test]
    fn relay_as_transport_allows_every_path() {
        for selected in [
            None,
            Some(PathKind::Ip),
            Some(PathKind::Relay),
            Some(PathKind::Custom),
        ] {
            assert!(payload_allowed(selected, true), "{selected:?}");
        }
    }

    #[test]
    fn relay_lookup_only_needs_a_proven_direct_path() {
        assert!(payload_allowed(Some(PathKind::Ip), false));
        assert!(payload_allowed(Some(PathKind::Custom), false));
        assert!(!payload_allowed(Some(PathKind::Relay), false));
        assert!(!payload_allowed(None, false), "unselected is not direct");
    }
}
