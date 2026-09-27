//! The mesh-wide transport policy carried in the mesh id: which paths may
//! carry **payload**. How members find each other is the lookup side
//! (`lookup.rs`); which relay servers exist is the ladder inside it. This
//! module names neither: the one rule that needs both — letting the relay
//! carry payload needs a relay lookup — lives in `MeshConfig`.

use std::fmt;
use std::str::FromStr;

use anyhow::{Result, bail};
use serde::Deserialize;

use super::ChoiceError;

/// One path a mesh's members may carry payload over — an entry of the
/// `transport` list a create names. Mesh-wide: every joiner inherits it from
/// the mesh id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    /// Direct paths only: hole-punched IP or a `WebRTC` session. Always on; a
    /// list that leaves it out is an error, because the engine has no
    /// relay-only mode.
    P2p,
    /// Let payload also fall back to the relay when no direct path exists.
    Relay,
}

impl Transport {
    const NAMES: &[&str] = &["p2p", "relay"];

    /// The name the list spells this transport by.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::P2p => "p2p",
            Self::Relay => "relay",
        }
    }
}

impl FromStr for Transport {
    type Err = ChoiceError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "p2p" => Ok(Self::P2p),
            "relay" => Ok(Self::Relay),
            other => Err(ChoiceError::new("transport", other, Self::NAMES)),
        }
    }
}

impl fmt::Display for Transport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Which transports may carry mesh **payload**, baked into the mesh id beside
/// [`LookupOpts`](crate::LookupOpts). Lookups say how members find each
/// other; this says what their traffic may ride once they have. Only mesh-wide
/// policy lives here — what a given node *can* do (`ip`, `webrtc`, `multihop`)
/// is per node, in the engine's `TransportOpts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransportPolicy {
    /// Whether the iroh relay may carry payload. Off by default: the relay is
    /// kept for lookup alone — the bootstrap dial, JSEP signalling and the
    /// NAT-traversal frames of a freshly opened connection still cross it —
    /// and every payload lane refuses to send while the relay is the only
    /// path to a peer. A pair that cannot hole-punch and has no `WebRTC`
    /// session stays unlinked for payload rather than relayed. `true` lets
    /// payload fall back to the relay; meaningless without a relay lookup, so
    /// rejected together with [`RelayChoice::Disabled`](crate::RelayChoice).
    pub relay_transport: bool,
}

impl TransportPolicy {
    /// The policy a `transport` list names. Empty ⇒ the default, direct paths
    /// only.
    ///
    /// # Errors
    /// The list is non-empty and leaves `p2p` out.
    pub fn from_transports(transports: &[Transport]) -> Result<Self> {
        if transports.is_empty() {
            return Ok(Self::default());
        }
        if !transports.contains(&Transport::P2p) {
            bail!("transport `p2p` cannot be disabled: name `p2p` or `p2p,relay`");
        }
        Ok(Self {
            relay_transport: transports.contains(&Transport::Relay),
        })
    }
}
