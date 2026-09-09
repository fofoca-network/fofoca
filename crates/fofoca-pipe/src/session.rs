//! Selecting a mesh, standing it up, and leaving it — the one ritual both
//! consumers run.

use std::sync::Arc;

use anyhow::{Context, Result};
use fofoca::embed::NodeSink;
use fofoca::iroh::RelayUrl;
use fofoca::net::TransportOpts;
use fofoca::protocol::JoinTarget;
use fofoca::protocol::Nickname;
use fofoca::protocol::{
    DirectorySelection, LookupOpts, LookupSet, MeshConfig, MeshName, RelayChoice, RelaySelection,
    TransportPolicy, resolve_lookups,
};
use fofoca::runtime::{CreateParams, JoinParams, Node, Resolved};
use fofoca::runtime::{SetupKind, SetupParams, derive_topic_mesh_config, setup_mesh};
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
    reason = "four independent discovery choices (public/mdns/dht/relay lookup) plus the relay's transport role; they are flat inputs, not a state machine to model as an enum"
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
    /// The relay as a **lookup**: members find each other through it.
    pub relay_lookup: bool,
    /// The relay as a **transport**: payload may fall back to it. Off by
    /// default, so all data is peer to peer and the relay serves lookup
    /// alone. Needs `relay_lookup` (or `public`); baked into the mesh id, so a
    /// joiner inherits it. Ignored when joining by id.
    pub relay_transport: bool,
    /// A custom relay **ladder** (ordered URLs, first preferred), replacing
    /// the default ladder. Implies the relay lookup. Part of the mesh id —
    /// with `topic`, every member must pass the same list or they derive
    /// different meshes. Empty ⇒ the default ladder. Ignored when joining
    /// by id.
    pub relay_urls: Vec<String>,
    /// Which of this node's transports may carry data. Per node, not in the
    /// id; the default is everything the target has.
    pub transports: TransportFlags,
    /// Active-view cap; `0` takes the engine default.
    pub max_peers: usize,
}

/// Per-node transport switches, mirrored from the engine's `TransportOpts`.
/// Only the two a consumer plausibly turns off are exposed: the relay stays
/// (it is the rendezvous) and multihop stays an engine concern.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct TransportFlags {
    /// Direct UDP and hole-punched paths.
    pub ip: bool,
    /// QUIC over a `WebRTC` data channel.
    pub webrtc: bool,
}

impl Default for TransportFlags {
    fn default() -> Self {
        Self {
            ip: true,
            webrtc: true,
        }
    }
}

/// The relay leg these options ask for: a custom ladder wins over the
/// default/off switch, and a bad URL is an error rather than a shrunk ladder.
fn relay_choice(urls: &[String], default_on: bool) -> Result<RelayChoice> {
    if urls.is_empty() {
        return Ok(if default_on {
            RelayChoice::Pinned
        } else {
            RelayChoice::Disabled
        });
    }
    let ladder = urls
        .iter()
        .map(|url| {
            url.trim()
                .parse::<RelayUrl>()
                .map_err(|error| anyhow::anyhow!("invalid relay URL {url:?}: {error}"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RelayChoice::Custom(ladder))
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
            // The caller's switches over everything this target has. In a
            // browser the WebRTC lane is the only one that exists, and the
            // engine attaches it per target.
            transports: TransportOpts {
                ip: opts.transports.ip,
                webrtc: opts.transports.webrtc,
                ..TransportOpts::default()
            },
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

impl Session {
    /// Push a request into the event loop and await its reply — the dance
    /// every consumer of the pipe (the wasm peer, the task runner's native
    /// side, the chat example) had re-implemented on its own.
    ///
    /// # Errors
    /// The event loop stopped, or dropped the reply.
    pub async fn request<T>(
        &self,
        build: impl FnOnce(tokio::sync::oneshot::Sender<T>) -> crate::Request,
    ) -> Result<T, String> {
        let (reply, answer) = tokio::sync::oneshot::channel();
        self.node
            .sender()
            .send(build(reply))
            .await
            .map_err(|_| "the event loop stopped".to_owned())?;
        answer
            .await
            .map_err(|_| "the event loop dropped the reply".to_owned())
    }
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
            // the lookups are mixed into the derivation, so two reaches over
            // one string are two different meshes. `relay_urls` and
            // `relay_transport` are the two knobs that *do* change the id,
            // and every member must pass the same values.
            let config = MeshConfig {
                lookups: LookupOpts {
                    mdns: true,
                    dht: true,
                    relay_lookup: relay_choice(&opts.relay_urls, true)?,
                },
                password: None,
                issuer_pubkey: None,
                transport: TransportPolicy {
                    relay_transport: opts.relay_transport,
                },
            };
            let mesh =
                derive_topic_mesh_config(string, config).context("resolving the topic string")?;
            Ok((
                SetupKind::Topic {
                    mesh,
                    topic_string: string.clone(),
                },
                nickname.unwrap_or_else(Nickname::random),
            ))
        }
        (None, None) => {
            let lookups = LookupSet {
                mdns: opts.mdns,
                dht: opts.dht,
                relay_lookup: if opts.relay_lookup {
                    RelaySelection::Default
                } else {
                    RelaySelection::Unset
                },
            };
            let mut resolved_lookups = resolve_lookups(opts.public, lookups);
            if !opts.relay_urls.is_empty() {
                resolved_lookups.relay_lookup = relay_choice(&opts.relay_urls, true)?;
            }
            let config = MeshConfig {
                lookups: resolved_lookups,
                password: None,
                issuer_pubkey: None,
                transport: TransportPolicy {
                    relay_transport: opts.relay_transport,
                },
            };
            config
                .validate()
                .map_err(|error| anyhow::anyhow!("{error}"))?;
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
    use fofoca::protocol::RelayChoice;
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
        let expected = derive_topic_mesh_with("standup", LookupOpts::public_preset())
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

    /// The relay's two roles are two options: `relay_lookup` finds peers through
    /// it, `relay_transport` lets payload ride it. The second is off unless
    /// named, and meaningless without the first.
    #[test]
    fn relay_transport_is_off_unless_named_and_needs_the_relay_lookup() {
        let (kind, _) = resolve_kind(
            &Opts {
                public: true,
                ..opts()
            },
            None,
        )
        .expect("a public create resolves");
        let SetupKind::Create { config, .. } = kind else {
            panic!("no selector must resolve to SetupKind::Create")
        };
        assert!(!config.transport.relay_transport);

        let (relayed, _) = resolve_kind(
            &Opts {
                relay_lookup: true,
                relay_transport: true,
                ..opts()
            },
            None,
        )
        .expect("a relay-transport create resolves");
        let SetupKind::Create {
            config: relayed_config,
            ..
        } = relayed
        else {
            panic!("no selector must resolve to SetupKind::Create")
        };
        assert!(relayed_config.transport.relay_transport);

        assert!(
            resolve_kind(
                &Opts {
                    relay_transport: true,
                    ..opts()
                },
                None,
            )
            .is_err(),
            "relay transport without a relay lookup is an error, not a loopback mesh"
        );
    }

    /// `relayUrls` swaps the default ladder for the caller's, on both the
    /// create path and the topic path — and on a topic it changes the id,
    /// because the lookups are mixed into the derivation.
    #[test]
    fn relay_urls_replace_the_ladder_and_change_a_topic_id() {
        let urls = vec!["http://127.0.0.1:3340/".to_owned()];
        let (kind, _) = resolve_kind(
            &Opts {
                relay_urls: urls.clone(),
                ..opts()
            },
            None,
        )
        .expect("a custom-ladder create resolves");
        let SetupKind::Create { config, .. } = kind else {
            panic!("no selector must resolve to SetupKind::Create")
        };
        let RelayChoice::Custom(ladder) = &config.lookups.relay_lookup else {
            panic!("relayUrls must resolve to a custom ladder")
        };
        assert_eq!(ladder.len(), 1);

        let (plain, _) = resolve_kind(
            &Opts {
                topic: Some("standup".to_owned()),
                ..opts()
            },
            None,
        )
        .expect("a plain topic resolves");
        let (laddered, _) = resolve_kind(
            &Opts {
                topic: Some("standup".to_owned()),
                relay_urls: urls,
                ..opts()
            },
            None,
        )
        .expect("a topic with a custom ladder resolves");
        let (SetupKind::Topic { mesh: plain, .. }, SetupKind::Topic { mesh: laddered, .. }) =
            (plain, laddered)
        else {
            panic!("a topic selector must resolve to SetupKind::Topic")
        };
        assert_ne!(
            plain.to_string(),
            laddered.to_string(),
            "the ladder is mixed into the id, so both sides must name the same one"
        );

        assert!(
            resolve_kind(
                &Opts {
                    relay_urls: vec!["not a url".to_owned()],
                    ..opts()
                },
                None,
            )
            .is_err(),
            "a bad URL is an error, not a shrunk ladder"
        );
    }

    /// `relay_urls` is caller-supplied — the `relay_urls` C field splits an
    /// arbitrary string on `,` — so an oversize ladder must be an error here,
    /// at the boundary. Past 255 rungs the wire count no longer fits its `u8`
    /// and the encoder panics; a wasm or in-process caller has no guard to
    /// catch that, and an FFI caller gets a bare "a panic crossed the FFI
    /// boundary" instead of a diagnostic.
    #[test]
    fn an_oversize_relay_ladder_is_an_error_not_a_panic() {
        let urls: Vec<String> = (0..300)
            .map(|index| format!("https://r{index}.example"))
            .collect();
        for kind in [None, Some("standup".to_owned())] {
            let error = resolve_kind(
                &Opts {
                    topic: kind.clone(),
                    relay_urls: urls.clone(),
                    ..opts()
                },
                None,
            )
            .expect_err("300 rungs must not reach the encoder");
            // `{:#}` so the topic path's context does not hide the cause.
            assert!(
                format!("{error:#}").contains("relay ladder too long"),
                "got: {error:#}"
            );
        }
    }

    /// A topic with the relay allowed as transport derives a different mesh
    /// than the plain public-preset topic — and inherits the policy.
    #[test]
    fn relay_transport_changes_a_topic_id() {
        let (plain, _) = resolve_kind(
            &Opts {
                topic: Some("standup".to_owned()),
                ..opts()
            },
            None,
        )
        .expect("a plain topic resolves");
        let (relayed, _) = resolve_kind(
            &Opts {
                topic: Some("standup".to_owned()),
                relay_transport: true,
                ..opts()
            },
            None,
        )
        .expect("a relay-transport topic resolves");
        let (SetupKind::Topic { mesh: plain, .. }, SetupKind::Topic { mesh: relayed, .. }) =
            (plain, relayed)
        else {
            panic!("a topic selector must resolve to SetupKind::Topic")
        };
        assert_ne!(plain.to_string(), relayed.to_string());
        assert!(relayed.transport().relay_transport);
        assert!(!plain.transport().relay_transport);
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

#[cfg(test)]
mod resolve_tests {
    use super::*;
    use fofoca::runtime::TopicParams;

    /// The collapse of the bare-topic early return rests on this identity: a
    /// tab and a terminal meet only if the no-override arm keeps deriving the
    /// exact id `TopicParams::resolve` always produced.
    #[test]
    fn a_bare_topic_still_derives_the_public_preset_mesh() {
        let opts = Opts {
            topic: Some("standup".to_owned()),
            ..Opts::default()
        };
        let (kind, _author) = resolve_kind(&opts, None).expect("resolve");
        let SetupKind::Topic { mesh, .. } = kind else {
            panic!("a topic resolves to a topic kind");
        };
        let Resolved { kind: baseline, .. } = TopicParams {
            string: "standup".to_owned(),
            nickname: None,
        }
        .resolve()
        .expect("baseline");
        let SetupKind::Topic { mesh: expected, .. } = baseline else {
            panic!("the baseline is a topic kind");
        };
        assert_eq!(mesh.to_string(), expected.to_string());
    }
}
