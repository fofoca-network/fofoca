//! After the node that holds the beacon leaves a loopback mesh, the members
//! left behind must link to whoever claims the rendezvous next, so a peer that
//! joins afterwards reaches them.
//!
//! A loopback beacon binds a port and answers no `WebRTC` offer, so an
//! IP-capable member that lost its rendezvous link must go on grafting it on
//! the heal timer. Holding that graft for a session that can never attach
//! partitioned every later joiner (the agent-gossip creator-death tests).

#![cfg(feature = "host")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use fofoca::embed::{
    AppClass, EventLoopState, HandlerCtx, InboundApp, NodeApp, NodeDriver, NodeEvent, NodeSink,
};
use fofoca::net::TransportOpts;
use fofoca::protocol::{LookupOpts, Message, MessageKind, Nickname, PresenceSubtype};
use fofoca::runtime::{Node, SetupKind, SetupParams, derive_topic_mesh_with, setup_mesh};
use tokio::sync::Notify;

/// The do-nothing application: presence alone forms a mesh.
struct Probe;

#[fofoca::async_trait]
impl NodeApp for Probe {
    fn classify(&self, _message: &Message) -> AppClass {
        AppClass {
            loggable: false,
            beat: true,
            valid: true,
            chained: false,
            sealed: false,
        }
    }

    async fn on_app_frame(
        &mut self,
        _frame: InboundApp<'_>,
        _state: &mut EventLoopState,
        _ctx: &HandlerCtx<'_>,
    ) -> bool {
        false
    }
}

#[fofoca::async_trait]
impl NodeDriver for Probe {
    type Session = ();
    type Http = ();
    type Ipc = serde_json::Value;
}

/// Records every `joined` presence the engine surfaces.
#[derive(Default)]
struct Joined {
    peers: Mutex<Vec<Nickname>>,
    changed: Notify,
}

impl NodeSink for Joined {
    fn emit(&self, event: NodeEvent) {
        if let NodeEvent::Presence { msg } = event
            && let MessageKind::Presence {
                subtype: PresenceSubtype::Joined,
            } = msg.kind
        {
            self.peers
                .lock()
                .expect("no poison")
                .push(msg.author.clone());
            self.changed.notify_waiters();
        }
    }
}

impl Joined {
    async fn wait_for(&self, nick: &Nickname, deadline: Duration) -> bool {
        let seen = || self.peers.lock().expect("no poison").contains(nick);
        let wait = async {
            loop {
                let changed = self.changed.notified();
                if seen() {
                    return;
                }
                changed.await;
            }
        };
        tokio::time::timeout(deadline, wait).await.is_ok() || seen()
    }
}

async fn spawn(topic: &str, nick: &str, sink: Arc<Joined>) -> Node<Probe> {
    let mesh =
        derive_topic_mesh_with(topic, LookupOpts::loopback()).expect("derive a loopback mesh");
    assert!(
        !mesh.transport().relay_transport,
        "the default policy keeps the relay for lookup only"
    );
    assert!(
        TransportOpts::default().webrtc,
        "the WebRTC lane is on, as in agent-gossip"
    );
    let author = Nickname::new(nick).expect("valid nickname");
    let config = setup_mesh(
        SetupKind::Topic {
            mesh,
            topic_string: topic.to_owned(),
        },
        SetupParams {
            author,
            max_peers: 16,
            endpoint: None,
            protocols: Vec::new(),
            transports: TransportOpts::default(),
            runtime_base: None,
            state_file: None,
            sink,
            multihop: false,
            per_peer_gate: None,
            cohost: None,
            live_count: None,
        },
    )
    .await
    .expect("setup_mesh on a loopback mesh must not touch the network");
    Node::spawn(config, Probe, None, false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joiner_after_the_beacon_holder_leaves_reaches_the_survivors() {
    let topic = format!("beacon-departure-{}", rand::random::<u64>());
    let nick = |name: &str| Nickname::new(name).expect("valid");
    let bob_saw = Arc::new(Joined::default());
    let carol_saw = Arc::new(Joined::default());
    // First up, so it claims the free loopback rendezvous port.
    let alice = spawn(&topic, "alice", Arc::new(Joined::default())).await;
    let bob = spawn(&topic, "bob", Arc::clone(&bob_saw)).await;
    let carol = spawn(&topic, "carol", Arc::clone(&carol_saw)).await;

    let meshed = Duration::from_secs(45);
    assert!(
        bob_saw.wait_for(&nick("carol"), meshed).await,
        "bob never saw carol"
    );
    assert!(
        carol_saw.wait_for(&nick("bob"), meshed).await,
        "carol never saw bob"
    );

    alice.leave().await.expect("alice leaves");
    let dave = spawn(&topic, "dave", Arc::new(Joined::default())).await;

    // A few heal intervals: the survivors must relink the rendezvous on the
    // timer, since no WebRTC session to a loopback beacon ever attaches.
    let relinked = Duration::from_secs(90);
    assert!(
        bob_saw.wait_for(&nick("dave"), relinked).await,
        "bob never saw dave join after the beacon holder left"
    );
    assert!(
        carol_saw.wait_for(&nick("dave"), relinked).await,
        "carol never saw dave join after the beacon holder left"
    );

    for node in [bob, carol, dave] {
        node.leave().await.expect("leave");
    }
}
