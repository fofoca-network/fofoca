//! Fixtures every unit-test module in this crate was writing for itself.
//!
//! `fresh_state` in particular was copy-pasted into six modules, which is what
//! made [`StateInit`] painful to extend: one new field meant editing six test
//! modules that had no reason to know about it. One home instead.

use std::sync::Arc;

use iroh::{EndpointId, SecretKey};

use crate::daemon::state::{EventLoopState, MeshSecrets, StateInit};
use crate::protocol::Nickname;
use crate::protocol::identity::Identity;
use crate::util::clock::Instant;

/// A bare state with a fresh identity: no state file, no secrets, no per-peer
/// gate. The starting point for a test that only cares about one field.
pub(crate) fn fresh_state() -> EventLoopState {
    EventLoopState::new(
        StateInit {
            state_file: None,
            identity: Arc::new(Identity::generate()),
            secrets: MeshSecrets::default(),
            per_peer_gate: None,
        },
        Instant::now(),
    )
}

/// A nickname, panicking on an invalid one — a test fixture, so a bad literal
/// should fail loudly at the assertion rather than be handled.
pub(crate) fn nick(name: &str) -> Nickname {
    Nickname::new(name.to_owned()).expect("valid test nickname")
}

/// A deterministic endpoint id from `seed`, so a test can name the same peer
/// twice and assert on identity.
pub(crate) fn endpoint_id(seed: u8) -> EndpointId {
    SecretKey::from_bytes(&[seed; 32]).public()
}
