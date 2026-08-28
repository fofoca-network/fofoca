//! Whether a connection's data is on a direct path right now, and whether the
//! mesh's transport policy lets payload ride it.
//!
//! iroh keeps every path it has to a peer open and picks one to carry data;
//! the relay path is never closed, only demoted. So "is this peer relayed" is
//! a question about the *selected* path, not about which paths exist —
//! `gossip::conn_path` answers the latter, for diagnostics.

use iroh::endpoint::Connection;

/// Whether iroh's selected path to the remote is not the relay: a direct UDP
/// path, or a custom transport (`WebRTC`, multihop), which is peer to peer as
/// far as the relay is concerned. `false` while no path is selected yet.
pub(crate) fn selected_is_direct(conn: &Connection) -> bool {
    conn.paths()
        .iter()
        .find(iroh::endpoint::Path::is_selected)
        .is_some_and(|path| !path.is_relay())
}

/// Whether payload may go out on `conn` under the mesh's transport policy.
/// With the relay allowed as a transport, anything goes — decided before the
/// path snapshot, which locks and clones. With the relay lookup only, the
/// selected path must be a proven non-relay one: "not selected yet" is
/// refused, not trusted.
pub(crate) fn payload_allowed_on(conn: &Connection, relay_transport: bool) -> bool {
    relay_transport || selected_is_direct(conn)
}

/// The refusal every payload lane reports when the relay is lookup only and
/// the only path is the relay. One string, so a log reader can grep for it.
pub(crate) const RELAY_REFUSED: &str =
    "relay-only path refused: the relay is lookup only on this mesh";

/// [`payload_allowed_on`] as a guard: on refusal, close `conn` with
/// `close_code` so the other end reads the cause, and return the error.
///
/// # Errors
/// The relay is lookup only and `conn`'s selected path is the relay.
#[cfg(feature = "blob")]
pub(crate) fn refuse_relayed(
    conn: &Connection,
    relay_transport: bool,
    close_code: u32,
) -> anyhow::Result<()> {
    if payload_allowed_on(conn, relay_transport) {
        return Ok(());
    }
    conn.close(close_code.into(), b"relay path refused");
    anyhow::bail!("{RELAY_REFUSED}")
}
