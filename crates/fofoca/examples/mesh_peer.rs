//! A minimal mesh peer, for interop testing against a browser peer.
//!
//! ```text
//! cargo run -p fofoca --example mesh_peer              # create, print the id
//! cargo run -p fofoca --example mesh_peer -- <id>    # join that mesh
//! MESH_TRANSPORT=webrtc cargo run … --example mesh_peer -- <id>  # WebRTC-only data plane
//! ```
//!
//! `MESH_TRANSPORT=webrtc` clears IP transports, so any data path that is not
//! the `WebRTC` one is a failure rather than a silent fallback. The relay stays —
//! it is the rendezvous, and the JSEP exchange rides it.
//!
//! Prints one line per second: the gossip roster count and the number of live
//! `WebRTC` sessions. Those two numbers are the whole point — they are what a
//! browser peer must be able to move.
//!
//! This is also the smallest possible answer to "what does embedding the engine
//! take": a driver with two required methods and three associated types.
//! Everything else on `NodeDriver` has a default.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use fofoca::embed::{
    AppClass, EventLoopState, HandlerCtx, InboundApp, NodeApp, NodeDriver, SilentSink,
};
use fofoca::net::TransportOpts;
use fofoca::protocol::{DirectorySelection, JoinTarget, LookupOpts, MeshConfig, MeshName, Message};
use fofoca::runtime::{CreateParams, JoinParams, Node, Resolved, SetupParams, setup_mesh};

/// The do-nothing application. A peer that only carries presence still forms a
/// mesh — payload handling is a separate concern from membership.
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "agent_habilis_mesh=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    // Env var rather than a flag: this example takes exactly one positional
    // argument and adding a parser for one knob is not worth it. The real CLI
    // surface is `--transport`, on `agent-share`.
    let transports = match std::env::var("MESH_TRANSPORT").as_deref() {
        Ok("webrtc") => TransportOpts::webrtc_only(),
        Ok(other) if !other.is_empty() => {
            anyhow::bail!("unknown MESH_TRANSPORT {other:?}; expected `webrtc`")
        }
        _ => TransportOpts::default(),
    };

    let arg = std::env::args().nth(1);
    let Resolved { kind, author, .. } = match arg {
        Some(id) => JoinParams {
            target: id.parse::<JoinTarget>()?,
            nickname: None,
            password: None,
        }
        .resolve()?,
        None => CreateParams {
            name: MeshName::random(),
            nickname: None,
            // Public preset: mDNS + DHT + the pinned relay ladder. The relay
            // rung is the part that matters here — it is the only leg a browser
            // peer has, and the rendezvous homes on it.
            config: MeshConfig {
                lookups: LookupOpts::public_preset(),
                password: None,
                issuer_pubkey: None,
            },
            advertise: DirectorySelection::Unset,
            password: None,
            invite_only: false,
        }
        .resolve()?,
    };

    // The live peer count, mirrored out of the loop on every roster change and
    // on the periodic refresh. Lock-free, so reading it per second costs
    // nothing and needs no request/response hop into the loop.
    let live = Arc::new(AtomicUsize::new(0));
    let config = setup_mesh(
        kind,
        SetupParams {
            author: author.clone(),
            max_peers: 16,
            endpoint: None,
            protocols: Vec::new(),
            transports,
            // No files and no control socket: this is an embedded peer, not a
            // daemon something else drives.
            runtime_base: None,
            state_file: None,
            sink: Arc::new(SilentSink),
            multihop: false,
            per_peer_gate: None,
            cohost: None,
            live_count: Some(Arc::clone(&live)),
        },
    )
    .await?;

    println!("mesh    {}", config.mesh_id().as_str());
    println!("nick    {author}");
    println!(
        "transport {}",
        if transports.ip { "all" } else { "webrtc-only" }
    );

    // `handle_signals: false` — registering tokio's signal handlers suppresses
    // the OS default-terminate for the whole process, permanently. This example
    // wants ctrl-c to keep working.
    let node: Node<Probe> = Node::spawn(config, Probe, None, false);

    let ticker = async {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            println!("peers   {}", live.load(Ordering::Relaxed));
        }
    };
    tokio::select! {
        () = ticker => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    node.leave().await?;
    Ok(())
}
