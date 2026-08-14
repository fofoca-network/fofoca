//! Selecting a mesh, standing it up, and leaving it — the one ritual both
//! consumers run.

use std::sync::Arc;

use anyhow::{Context, Result};
use fofoca::embed::NodeSink;
use fofoca::net::TransportOpts;
use fofoca::protocol::JoinTarget;
use fofoca::protocol::Nickname;
use fofoca::protocol::{
    DirectorySelection, LookupSet, MeshConfig, MeshName, RelaySelection, resolve_lookups,
};
use fofoca::runtime::{CreateParams, JoinParams, Node, Resolved, TopicParams};
use fofoca::runtime::{SetupKind, SetupParams, setup_mesh};
use fofoca::util::tuning::GOSSIP_ACTIVE_VIEW_CAPACITY;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::app::{Inbound, PipeApp};
use crate::wire::{DEPARTURE_GRACE, INBOUND_CAP};

/// How the caller selects a mesh — an id to join, a shared string to derive one
/// from, or a create over these lookups.
///
/// `Deserialize` so a browser tab can hand its constructor a plain object and
/// have it land here, which is what stops the browser and the C caller growing
/// two different option sets. `deny_unknown_fields` so a typo is an error rather
/// than a silently loopback mesh; `default` so every field stays optional.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
#[expect(
    clippy::struct_excessive_bools,
    reason = "four independent discovery choices (public/mdns/dht/relay); they are flat inputs, not a state machine to model as an enum"
)]
pub struct Opts {
    /// A `mesh id` to join.
    pub mesh: Option<String>,
    /// A shared string both sides derive the same public mesh from.
    pub topic: Option<String>,
    /// Local nickname; `None` mints a random one.
    pub nick: Option<String>,
    /// Mesh name to create with; `None` falls back to `"fofoca"`. Ignored
    /// when joining (the name travels with the id/topic instead).
    pub name: Option<String>,
    /// Create a public mesh (the all-on discovery preset).
    pub public: bool,
    pub mdns: bool,
    pub dht: bool,
    pub relay: bool,
    /// Active-view cap; `0` takes the engine default.
    pub max_peers: usize,
}

/// One live membership's moving parts.
#[expect(
    missing_debug_implementations,
    reason = "Node's Debug is manual and says nothing a caller wants; the identity a reader would look for is on `node` already"
)]
pub struct Session {
    pub node: Node<PipeApp>,
    /// Inbound `pipe_*` frames in arrival order, bounded at
    /// [`INBOUND_CAP`](crate::wire::INBOUND_CAP).
    pub inbound: mpsc::Receiver<Inbound>,
}

/// Resolve `opts`, stand the mesh up, and spawn the event loop.
///
/// The single setup ritual both consumers run. A browser and a terminal reach
/// each other only if they agree on the mesh id, the transports and the spawn
/// flags, so none of those is written twice. The one thing that differs is
/// `sink`: the C caller passes `SilentSink` because it can only poll, and the
/// browser passes [`json_sink`](crate::event::json_sink)'s because it has a
/// callback.
///
/// # Errors
/// An unparseable id/topic/nickname, conflicting selectors, a mesh no peer on
/// this target could reach, or a failure standing up the endpoint and overlay.
pub async fn join(opts: &Opts, sink: Arc<dyn NodeSink>) -> Result<Session> {
    let nickname = opts
        .nick
        .clone()
        .map(Nickname::new)
        .transpose()
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let (kind, author) = resolve_kind(opts, nickname)?;
    let max_peers = if opts.max_peers == 0 {
        GOSSIP_ACTIVE_VIEW_CAPACITY
    } else {
        opts.max_peers
    };

    // A browser has no UDP socket, so the loopback ladder a selector-less create
    // resolves to is a mesh of exactly one, forever — and it fails *silently*:
    // the node comes up healthy, the roster stays empty, and nothing says why.
    // Refused where the target makes it impossible rather than where the caller
    // happens to be, the same reasoning that splits the WebRTC backend by target
    // rather than by feature.
    #[cfg(target_arch = "wasm32")]
    {
        let loopback = match &kind {
            SetupKind::Create { config, .. } => config.lookups.is_loopback(),
            SetupKind::Join { mesh, .. } | SetupKind::Topic { mesh, .. } => mesh.is_loopback(),
        };
        anyhow::ensure!(
            !loopback,
            "a browser peer cannot reach a loopback mesh: pass `public`, or a topic / mesh id"
        );
    }

    let (inbound_tx, inbound) = mpsc::channel(INBOUND_CAP);
    let config = setup_mesh(
        kind,
        SetupParams {
            author,
            max_peers,
            // An embedded library writes no files and binds no control
            // socket, so it claims no /tmp root of its own.
            runtime_base: None,
            state_file: None,
            sink,
            // The engine binds its own endpoint and serves no extra
            // ALPNs here: injecting either is for a consumer that
            // already owns an iroh endpoint, which a byte pipe does
            // not.
            endpoint: None,
            protocols: Vec::new(),
            // Everything this target has. Off wasm32 that keeps IP paths
            // alongside the relay; in a browser the WebRTC lane is the only one
            // that exists, and the engine attaches it per target.
            transports: TransportOpts::default(),
            multihop: false,
            // A byte pipe publishes no per-peer identity, so `meta`
            // stays free-form.
            per_peer_gate: None,
            cohost: None,
            live_count: None,
        },
    )
    .await
    .context("setting up the mesh")?;

    // `handle_signals: false` — this is a library inside somebody else's
    // process, unlike a CLI that owns its own: installing process-wide ctrl-c /
    // SIGTERM listeners would hijack the host's own handling. A foreign caller
    // traps signals itself and closes the handle.
    let node = Node::spawn(
        config,
        PipeApp::new(inbound_tx),
        /* push */ None,
        /* handle_signals */ false,
    );

    Ok(Session { node, inbound })
}

/// Broadcast `Left` and wind the loop down, after a brief grace period: a gossip
/// broadcast is fire-and-forget, so leaving the instant after a send could race
/// the frames out of existence.
///
/// `n0_future::time::sleep`, not `tokio::time`: off wasm32 it *is*
/// `tokio::time::sleep` verbatim, and in a browser tokio has no timer driver and
/// panics. The grace is part of what a remote peer sees, so it belongs here
/// rather than in either consumer.
///
/// # Errors
/// The event loop returned an error or panicked.
pub async fn depart(node: Node<PipeApp>) -> Result<()> {
    n0_future::time::sleep(DEPARTURE_GRACE).await;
    node.leave().await
}

/// Resolve the selectors into a [`SetupKind`] plus our nickname. Exactly one
/// source: an id, a topic string, or a create over the lookup flags (no
/// selector at all ⇒ a loopback create).
///
/// Public because it is the one function that decides *which mesh you land in*,
/// and a tab and a terminal meeting depends on both running it.
///
/// # Errors
/// Both `mesh` and `topic` set, an unparseable id or topic, an invalid mesh
/// name, or a create whose advertise target is unreachable.
pub fn resolve_kind(opts: &Opts, nickname: Option<Nickname>) -> Result<(SetupKind, Nickname)> {
    match (&opts.mesh, &opts.topic) {
        (Some(_), Some(_)) => anyhow::bail!("pass only one of mesh / topic"),
        (Some(id), None) => {
            let target: JoinTarget = id.parse().map_err(|error| anyhow::anyhow!("{error}"))?;
            let Resolved { kind, author, .. } = JoinParams {
                target,
                nickname,
                password: None,
            }
            .resolve()
            .context("resolving the mesh id")?;
            Ok((kind, author))
        }
        (None, Some(string)) => {
            // Note what this ignores: the discovery flags. `TopicParams::resolve`
            // always derives through the public preset, so a topic mesh is
            // always mDNS + DHT + the pinned relay ladder. That is what lets a
            // tab and a terminal derive the same id from the same string —
            // `derive_topic_mesh_with` mixes the lookups into the derivation, so
            // two reaches over one string are two different meshes.
            let Resolved { kind, author, .. } = TopicParams {
                string: string.clone(),
                nickname,
            }
            .resolve()
            .context("resolving the topic string")?;
            Ok((kind, author))
        }
        (None, None) => {
            let lookups = LookupSet {
                mdns: opts.mdns,
                dht: opts.dht,
                relay: if opts.relay {
                    RelaySelection::Default
                } else {
                    RelaySelection::Unset
                },
            };
            let config = MeshConfig {
                lookups: resolve_lookups(opts.public, lookups),
                password: None,
                issuer_pubkey: None,
            };
            let name = MeshName::new(opts.name.clone().unwrap_or_else(|| "fofoca".to_string()))
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            let Resolved { kind, author, .. } = CreateParams {
                name,
                nickname,
                config,
                advertise: DirectorySelection::Unset,
                password: None,
                invite_only: false,
            }
            .resolve()
            .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok((kind, author))
        }
    }
}

#[cfg(test)]
mod tests {
    use fofoca::runtime::derive_topic_mesh_with;

    use super::*;

    fn opts() -> Opts {
        Opts::default()
    }

    /// The interop assertion. A browser resolving `topic` and a terminal calling
    /// `derive_topic_mesh_with(s, public_preset())` must land on the same id:
    /// the derivation mixes the lookups in, so this is not a formality.
    #[test]
    fn a_topic_derives_the_public_preset_mesh() {
        let (kind, _author) = resolve_kind(
            &Opts {
                topic: Some("standup".to_owned()),
                ..opts()
            },
            None,
        )
        .expect("a plain topic string resolves");
        let expected =
            derive_topic_mesh_with("standup", fofoca::protocol::LookupOpts::public_preset())
                .expect("the same string derives");
        match kind {
            SetupKind::Topic { mesh, topic_string } => {
                // Both, because they are what a peer meets another peer on: the
                // gossip topic it subscribes to, and the pseudo-node it dials to
                // find the swarm in the first place.
                assert_eq!(mesh.topic_id(), expected.topic_id());
                assert_eq!(mesh.rendezvous_id(), expected.rendezvous_id());
                assert_eq!(topic_string, "standup");
            }
            SetupKind::Create { .. } | SetupKind::Join { .. } => {
                panic!("a topic string must resolve to SetupKind::Topic")
            }
        }
    }

    #[test]
    fn naming_both_selectors_is_an_error_rather_than_a_precedence_rule() {
        let error = resolve_kind(
            &Opts {
                mesh: Some("whatever".to_owned()),
                topic: Some("standup".to_owned()),
                ..opts()
            },
            None,
        )
        .expect_err("mesh and topic together");
        assert!(error.to_string().contains("only one"), "{error}");
    }

    #[test]
    fn a_bare_create_is_loopback_and_naming_public_is_not() {
        let (bare, _) = resolve_kind(&opts(), None).expect("a bare create resolves");
        let (public, _) = resolve_kind(
            &Opts {
                public: true,
                ..opts()
            },
            None,
        )
        .expect("a public create resolves");
        for (kind, loopback) in [(bare, true), (public, false)] {
            match kind {
                SetupKind::Create { config, .. } => {
                    assert_eq!(config.lookups.is_loopback(), loopback);
                }
                SetupKind::Join { .. } | SetupKind::Topic { .. } => {
                    panic!("no selector must resolve to SetupKind::Create")
                }
            }
        }
    }

    #[test]
    fn naming_one_leg_selects_only_that_leg() {
        let (kind, _) = resolve_kind(
            &Opts {
                mdns: true,
                ..opts()
            },
            None,
        )
        .expect("an mdns-only create resolves");
        match kind {
            SetupKind::Create { config, .. } => {
                assert!(config.lookups.mdns);
                assert!(!config.lookups.dht);
                assert!(!config.lookups.is_loopback());
            }
            SetupKind::Join { .. } | SetupKind::Topic { .. } => {
                panic!("no selector must resolve to SetupKind::Create")
            }
        }
    }
}
