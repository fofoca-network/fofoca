//! The `select!` event loop itself — the bulk of the daemon.
//!
//! [`run`] sets up per-session state then drives [`event_loop`], which
//! multiplexes external inputs (stdin / IPC / gossip), time-driven
//! maintenance (heartbeat, sweep, heal, anti-entropy, reclaim), and
//! shutdown. The orchestration lives here; the behavioral subsystems are
//! crate-root siblings (`crate::{gossip,lifecycle,beacon,lookup}`), and
//! the daemon-internal plumbing (`config`/`ctx`/`ipc`/`state`/`timers`/
//! `setup`) are siblings under `super`.

use std::time::Duration;

use anyhow::Result;
use futures_util::StreamExt;
use iroh::{Endpoint, EndpointId, RelayUrl};
use iroh_gossip::api::{GossipReceiver, GossipSender};
use tokio::sync::{broadcast, mpsc, watch};

#[cfg(feature = "host")]
use crate::daemon::state_file::StateFile;
use crate::gossip::event::{NodeEvent, NodeSink};
use crate::protocol::mesh::MeshName;
use crate::protocol::{MeshId, Message, Nickname};
use crate::transport::IpcMessage;
use crate::transport::MeshSender;
use crate::util::clock::Instant;
// The timer-driver clock, distinct from `clock::Instant` off wasm32 (there it is
// `tokio::time::Instant`). Aliased rather than path-qualified, matching
// `daemon::state` / `daemon::app`.
use crate::util::tuning::{
    ALIVE_INTERVAL_SECS, LINKSTATE_INTERVAL_SECS, RECLAIM_INTERVAL_MS, RECLAIM_WINDOW_SECS,
    RESUBSCRIBE_MAX_ATTEMPTS, STATE_REFRESH_SECS, antientropy_interval_secs, heal_interval_secs,
    heal_stall_threshold_secs, sweep_interval_secs,
};
use n0_future::time::Instant as TokioInstant;
// Gated with `spawn_orphan_watch`, its only caller.
#[cfg(all(unix, feature = "host"))]
use crate::util::tuning::ppid_watch_interval_ms;
use crate::{beacon, gossip, lifecycle, lookup};

use super::app::NodeDriver;
use super::config::{CoHostPolicy, DriverMode, EventLoopConfig};
use super::ctx::HandlerCtx;
use super::state::{EventLoopState, MeshSecrets};
use super::{setup, timers};
use crate::gossip::app::NodeApp;

/// Never returns normally — exits the process on ctrl-c / SIGTERM.
///
/// The engine loop is generic over the application: `app` implements
/// [`NodeDriver`] (the shipped application supplies the only production impl), and its
/// two typed driver inputs — the in-process `session_rx` (in-process) and the
/// localhost binding's `http_rx` — are threaded here so the
/// loop never names an application payload type.
///
/// # Errors
/// Propagates a fatal setup/transport error that terminates the event loop.
#[expect(
    clippy::too_many_lines,
    reason = "session setup + secrets + sender/receiver wiring before handing off to event_loop; each step is a single call, and splitting further would only scatter sequential setup across helpers"
)]
pub async fn run<A: NodeDriver>(
    cfg: EventLoopConfig,
    app: A,
    session_rx: Option<mpsc::Receiver<A::Session>>,
    http_rx: Option<mpsc::Receiver<A::Http>>,
) -> Result<()> {
    let EventLoopConfig {
        topic,
        gossip,
        author,
        identity,
        mesh: mesh_str,
        name: mesh_name,
        topic_string,
        mesh_password,
        mesh_key,
        sink,
        mint_mesh,
        endpoint,
        router: _router,
        max_peers,
        rendezvous_params,
        rung_rx,
        cohost,
        runtime_base,
        state_file,
        #[cfg(feature = "host")]
        multihop,
        webrtc,
        webrtc_admission,
        webrtc_ice,
        unicast_rx,
        live_count,
        driver,
        per_peer_gate,
    } = cfg;

    // Every driver-derived fact in one place. Only the CLI exits the
    // process on quit and binds the unix socket; in-process drivers
    // (in-process: library API or MCP) take typed requests on `session_rx` instead.
    let (external_quit_rx, external_msg_tx, ipc_listener_disabled, exit_on_quit, handle_signals) =
        match driver {
            DriverMode::Cli => (None, None, false, true, true),
            DriverMode::InProcess {
                msg_tx,
                quit_rx,
                handle_signals,
            } => (Some(quit_rx), msg_tx, true, false, handle_signals),
        };
    let external_req_rx = session_rx;

    let started = Instant::now();
    // CLI `create`/`join` daemons default their state file into the mesh's
    // runtime folder (`<prefix>/<nick>.state.json`, beside the socket + log)
    // when no `--state-file` override is given. In-process in-process sessions
    // (`!exit_on_quit`) keep writing nothing.
    // Host-only: a browser node has no filesystem to write the session file to.
    // The path is still carried on the config (a `PathBuf` costs nothing off
    // wasm), it is only the `StateFile` that cannot exist.
    #[cfg(feature = "host")]
    let state_file = state_file
        .or_else(|| {
            let base = runtime_base.as_deref()?;
            exit_on_quit.then(|| {
                crate::util::mesh_runtime_dir(base, mesh_str.as_str())
                    .join(format!("{author}.state.json"))
            })
        })
        .map(|path| {
            StateFile::new(path, &mesh_str, &author, &mesh_name)
                .with_base(runtime_base.clone())
                .with_topic(topic_string.as_deref())
        });
    #[cfg(not(feature = "host"))]
    drop(state_file);
    // The departure line's label: a topic gossip shows `topic` plus the raw
    // string it was derived from (the name is lossy, and the wording mirrors
    // the `joined topic …` startup line), every other gossip its `#name`.
    let leave_label = topic_string.map_or_else(
        || format!("#{mesh_name}"),
        |raw_topic| format!("topic {raw_topic}"),
    );
    // Seed the state file with the application's own discovery fields before
    // readiness is advertised — the local client reads them from this mode-600
    // file. What those fields are is the app's business; the engine only writes.
    #[cfg(feature = "host")]
    app.init_state_file(state_file.as_ref());
    let mut state = EventLoopState::new(
        crate::daemon::state::StateInit {
            #[cfg(feature = "host")]
            state_file,
            identity,
            secrets: MeshSecrets {
                password: mesh_password,
                key: mesh_key,
            },
            per_peer_gate,
        },
        started,
    );
    state.mint_mesh = mint_mesh; // creator-only: backs the `invite` command
    #[cfg(feature = "host")]
    {
        state.multihop = multihop; // `--multihop`: the registered transport's handle
    }
    state.webrtc = Some(webrtc); // the direct-path transport the session manager fills
    // The *same* table the Router's signal acceptor holds, not a fresh one:
    // the cap only means anything if both roles count against it together.
    state.webrtc_admission = webrtc_admission;
    state.webrtc_ice = webrtc_ice;
    wire_session_state(
        &mut state,
        &endpoint,
        SessionWireParams {
            live_count,
            rendezvous_id: rendezvous_params.id,
        },
    );

    // An eager member co-hosts from t=0 so a beacon exists before any
    // joiner subscribes; everyone else defers to the heal gate
    // (`may_cohost`). `Eager` skips the probe (a brand-new mesh has no
    // peers to self-collide with); `EagerProbed` probes first, so several
    // advertisers sharing one directory `rendezvous_id` don't bind
    // duplicate copies. Why: `EventLoopConfig::cohost`.
    let mut rendezvous: Option<beacon::Rendezvous> = None;
    // The outstanding probe-before-claim, if any. Owned here beside the
    // beacon it decides, because a probe outlives the tick that started it:
    // its verdict arrives on the loop's own arm, up to `HEAL_PROBE_SECS`
    // later.
    let mut rival_probe: Option<beacon::RivalProbe> = None;
    if claims_at_startup(cohost) {
        let claimed = beacon::ensure(
            &rendezvous_params,
            &endpoint,
            &mut rendezvous,
            probes_before_claim(cohost),
            &mut rival_probe,
        )
        .await;
        if claimed {
            schedule_rival_recheck(&mut state, cohost, &rendezvous_params, &endpoint);
        }
    }

    let (gossip_sender, receiver) = topic.split();

    let sender = MeshSender::new(gossip_sender);

    #[cfg(feature = "host")]
    let ipc_rx = spawn_ipc_rx::<A::Ipc>(
        &IpcBinding {
            disabled: ipc_listener_disabled,
            runtime_base: runtime_base.as_deref(),
            mesh: &mesh_str,
            author: &author,
        },
        &sink,
    );
    // A browser binds no control socket. The loop keeps its IPC `select!` arm —
    // `IpcMessage` is portable — and the arm simply never fires.
    #[cfg(not(feature = "host"))]
    let ipc_rx: Option<mpsc::Receiver<IpcMessage<A::Ipc>>> = {
        drop(runtime_base);
        None
    };

    // Arrival announce is deferred to the first `NeighborUp` — see
    // `gossip::handle_gossip_event`.

    let intervals = build_maintenance_intervals().await;
    // A session inside a foreground command that owns its own lifetime
    // (a `--advertise` transfer, a directory browse) must not register
    // process-wide signal handlers — doing so suppresses the OS
    // default-terminate forever and the host command stops dying on
    // ctrl-c. Give the loop a quit channel that never fires instead;
    // shutdown comes from `external_quit_rx` / drop. A browser is always this
    // case: there are no process signals to listen for.
    #[cfg(feature = "host")]
    let quit_rx = if handle_signals {
        spawn_quit_signal_tasks(exit_on_quit)
    } else {
        never_quit()
    };
    #[cfg(not(feature = "host"))]
    let quit_rx = {
        let _ = handle_signals;
        never_quit()
    };

    // Flip `ready` to `true` only once the daemon can actually serve, then
    // re-write the state file (the earlier write reported `ready: false`) and
    // emit the `ready` event.
    //
    // "Serving" means: in CLI mode the IPC socket is bound — `spawn_ipc_rx`
    // binds *synchronously*, so a `Some` receiver proves an accepting socket
    // exists and a gate that observes the flag is guaranteed a subsequent
    // `poll`/`msg` connect succeeds; a `None` here is a bind failure, and we
    // must NOT advertise readiness (the daemon still gossips, but has no IPC).
    // In-process mode (in-process) has no socket by design (`req_rx` drives it)
    // and no `--state-file`, so it is always considered serving.
    //
    // The event is emitted *here*, beside the flag, rather than in `setup`:
    // there it announced a socket that had not been bound, so a client acting
    // on `ready` could race the listener. The two readiness signals — the
    // stdout event and the state-file flag a readiness gate polls — now
    // have one source and cannot disagree.
    if ipc_listener_disabled || ipc_rx.is_some() {
        state.ready = true;
        state.write_peer_count();
        sink.emit(NodeEvent::Ready {
            mesh: mesh_str.clone(),
            name: mesh_name.clone(),
            nickname: author.clone(),
        });
    }

    // `_router` stays owned in this scope so its accept loop outlives
    // the event loop below (dropping it makes the daemon unreachable
    // to new peers).
    //
    // `Box::pin` keeps the event-loop future off `run`'s stack frame, so
    // `run` — and every caller that awaits it up through `cli::dispatch`
    // — stays under clippy's `large_futures` threshold. The future's
    // size is target-dependent (it crosses the limit on x86_64-linux but
    // not aarch64-macOS), so boxing the single await is more robust than
    // shaving struct fields.
    Box::pin(event_loop(EventLoop {
        sender,
        receiver,
        gossip,
        endpoint,
        mesh: mesh_str,
        name: mesh_name,
        leave_label,
        author,
        sink,
        max_peers,
        state,
        app,
        ipc_rx,
        intervals,
        rendezvous,
        rival_probe,
        rendezvous_params,
        rung_rx,
        cohost,
        started,
        external_quit_rx,
        external_req_rx,
        external_msg_tx,
        quit_rx,
        exit_on_quit,
        http_rx,
        unicast_rx: Some(unicast_rx),
    }))
    .await
}

/// The 1-minute housekeeping arm: the memory warn + reassembly sweep, then
/// ask authors to re-send what our stalled big shard groups are missing —
/// Wire the just-built state to this session's endpoint + config: the real
/// unicast pool (the default is detached), the advertise counter (set before
/// the first write so the initial ad carries a real count), and the
/// rendezvous id — then publish the initial count.
fn wire_session_state(state: &mut EventLoopState, endpoint: &Endpoint, wiring: SessionWireParams) {
    state.unicast_pool = crate::transport::UnicastPool::new(endpoint.clone());
    state.live_count = wiring.live_count;
    state.rendezvous_id = Some(wiring.rendezvous_id);
    state.write_peer_count();
}

/// The per-session value cluster [`wire_session_state`] folds into a fresh
/// [`EventLoopState`]: the shared advertise counter (if advertising) and the
/// well-known rendezvous endpoint id.
struct SessionWireParams {
    live_count: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    rendezvous_id: EndpointId,
}

/// The alive tick: note the gap, then broadcast the keepalive presence.
async fn alive_arm(anchors: &mut TickAnchors, state: &mut EventLoopState, ctx: &HandlerCtx<'_>) {
    timers::note_tick_gap(
        "alive",
        &mut anchors.alive,
        &mut anchors.alive_wall,
        Duration::from_secs(ALIVE_INTERVAL_SECS),
    );
    lifecycle::heartbeat::tick_alive(state, ctx.sender, ctx.mesh, ctx.author).await;
}

/// The anti-entropy tick: note the gap, advertise the chat digest, then both
/// channel digests, so peers can request anything we hold that they miss.
async fn antientropy_arm(
    anchors: &mut TickAnchors,
    state: &mut EventLoopState,
    ctx: &HandlerCtx<'_>,
) {
    timers::note_tick_gap(
        "antientropy",
        &mut anchors.antientropy,
        &mut anchors.antientropy_wall,
        Duration::from_secs(antientropy_interval_secs()),
    );
    gossip::antientropy::broadcast_digest(state, ctx.sender, ctx.mesh, ctx.author).await;
    gossip::antientropy::broadcast_state_digests(state, ctx.sender, ctx.mesh, ctx.author).await;
}

/// Default per-link routing cost we advertise for our own neighbours until live
/// telemetry (RTT / delivery) is wired into the multihop metric.
#[cfg(feature = "host")]
const MULTIHOP_LINK_COST: u32 = 10;

/// The multihop link-state tick: re-broadcast our own links (one per direct
/// neighbour, carrying our underlay dial address) so every peer keeps a fresh
/// routing graph for the multihop transport. No-op until meshed, or when the
/// multihop transport is off — a vector with no consumer helps no one.
/// Off a host the multihop transport does not exist, so the tick has nothing to
/// broadcast. A no-op stub rather than a `cfg` at the `select!` arm, so the loop
/// body reads the same on both targets.
#[cfg(not(feature = "host"))]
#[expect(
    clippy::unused_async,
    reason = "the `host` body awaits; the signatures must match so the select! arm reads the same on both targets"
)]
async fn linkstate_arm(state: &mut EventLoopState, ctx: &HandlerCtx<'_>) {
    let _ = (state, ctx);
}

#[cfg(feature = "host")]
async fn linkstate_arm(state: &mut EventLoopState, ctx: &HandlerCtx<'_>) {
    if !state.meshed || state.multihop.is_none() {
        return;
    }
    state.link_state_seq += 1;
    let seq = state.link_state_seq;
    let links: Vec<_> = state
        .linked_endpoints
        .iter()
        .map(|eid| (*eid, MULTIHOP_LINK_COST))
        .collect();
    let handle = state.multihop.as_ref().expect("checked above");
    let vector = handle.link_vector(seq, links);
    // Fold our own vector into our own routing table: gossip never loops a
    // broadcast back, and without our outbound edges the local graph can't
    // source a route (`route_to(self, …)` would always be empty).
    handle.feed_topology(vector.clone());
    let Ok(json) = serde_json::to_string(&vector) else {
        return;
    };
    let Ok(body) = crate::protocol::MessageBody::new(json) else {
        return;
    };
    state.idle.broadcasts += 1;
    // Retained locally for the same reason the vector is fed into our own
    // routing table above: gossip never loops a broadcast back. Every tick
    // mints a fresh `seq`, so an unretained vector is one more message our
    // peers re-send to us on every anti-entropy round for as long as the log
    // holds it (see `gossip::recv::retain_own_broadcast`).
    let vector_msg = Message::new_link_state(ctx.mesh, ctx.author, body).signed(&state.identity);
    gossip::broadcast_msg(ctx.sender, &vector_msg).await;
    gossip::retain_own_broadcast(state, &vector_msg);
}

/// The sweep-tick arm: note the gap, then evict silent peers. The app's own
/// timers that ride this cadence run via [`NodeDriver::on_tick`] right after.
fn sweep_arm(anchors: &mut TickAnchors, state: &mut EventLoopState, sink: &dyn NodeSink) {
    timers::note_tick_gap(
        "sweep",
        &mut anchors.sweep,
        &mut anchors.sweep_wall,
        Duration::from_secs(sweep_interval_secs()),
    );
    lifecycle::heartbeat::tick_sweep(state, sink);
}

/// Owned working set for [`event_loop`]. `run` does setup, fills this,
/// and hands it over; the loop destructures it back into the same
/// locals the orchestrator used to hold inline. Splitting the 11-arm
/// `select!` out keeps both functions within the readability budget
/// (clippy `too_many_lines`) without an `#[allow]`.
struct EventLoop<A: NodeDriver> {
    sender: MeshSender,
    receiver: GossipReceiver,
    /// The gossip frontend, kept so the loop can re-subscribe the topic
    /// after the stream terminally ends (see the heal arm) — without it
    /// a closed subscription (e.g. lag-evicted by the actor) would
    /// leave the daemon permanently deaf.
    gossip: iroh_gossip::net::Gossip,
    endpoint: Endpoint,
    mesh: MeshId,
    name: MeshName,
    /// The departure line's label: the raw topic string for a topic
    /// gossip (the derived name is lossy), `#name` otherwise.
    leave_label: String,
    author: Nickname,
    sink: std::sync::Arc<dyn NodeSink>,
    max_peers: usize,
    state: EventLoopState,
    /// Application state behind the [`NodeDriver`] seam, disjoint from the mesh
    /// `state` and threaded as its own `&mut` through the app callbacks.
    app: A,
    ipc_rx: Option<mpsc::Receiver<IpcMessage<A::Ipc>>>,
    intervals: MaintenanceIntervals,
    rendezvous: Option<beacon::Rendezvous>,
    /// The outstanding probe-before-claim, whose verdict the loop applies on
    /// its own arm (`beacon::probe_verdict`). Off-loop by construction: a
    /// free rendezvous is only provably free once the dial exhausts its
    /// budget, and paying that inline froze the whole loop for ~5s a tick.
    rival_probe: Option<beacon::RivalProbe>,
    rendezvous_params: beacon::RendezvousParams,
    /// Bootstrap rung chosen off-loop (startup probe + beacon
    /// self-monitor); the loop applies changes via the rung-update arm.
    rung_rx: watch::Receiver<Option<RelayUrl>>,
    /// When this member may serve the rendezvous (see [`CoHostPolicy`]).
    cohost: CoHostPolicy,
    /// Event-loop start, for the unmeshed-joiner co-host grace.
    started: Instant,
    external_quit_rx: Option<mpsc::Receiver<()>>,
    /// The in-process typed session channel (in-process: library API or MCP); `None` on the CLI.
    external_req_rx: Option<mpsc::Receiver<A::Session>>,
    external_msg_tx: Option<broadcast::Sender<Message>>,
    quit_rx: mpsc::Receiver<()>,
    exit_on_quit: bool,
    /// The application's localhost HTTP binding request channel; `None` off.
    http_rx: Option<mpsc::Receiver<A::Http>>,
    /// Inbound unicast frames from the `UNICAST_ALPN` acceptor, drained into
    /// `gossip::ingest` (same validation + dedup path as gossip). `Option` so
    /// the `select!` arm can disable itself if the channel ever closes.
    unicast_rx: Option<mpsc::Receiver<bytes::Bytes>>,
}

/// The daemon's `select!` loop. Never returns normally on the CLI
/// path (ctrl-c / SIGTERM `std::process::exit`s); in-process drivers
/// break out via their external quit channel and get `Ok(())`.
/// Log the one per-daemon build-stamp line into the always-on file (one log
/// file == one process == one build). The `ready` JSON event carries the same
/// `version`; this is the file-log counterpart. Explicit pinned target so it
/// survives a release build's `error` base.
fn log_daemon_start(author: &Nickname) {
    tracing::info!(
        target: "fofoca::lifecycle",
        version = crate::util::version::build_version(),
        nickname = %author,
        "daemon starting"
    );
}

#[expect(
    clippy::too_many_lines,
    reason = "the daemon's central select! loop: one arm per event source (ipc, http, gossip, the maintenance ticks, quit); each arm delegates to a helper, but the arm list itself is irreducibly long"
)]
async fn event_loop<A: NodeDriver>(loop_state: EventLoop<A>) -> Result<()> {
    let EventLoop {
        mut sender,
        mut receiver,
        gossip,
        endpoint,
        mesh: mesh_str,
        name: mesh_name,
        leave_label,
        author,
        sink,
        max_peers,
        mut state,
        mut app,
        mut ipc_rx,
        mut intervals,
        mut rendezvous,
        mut rival_probe,
        mut rendezvous_params,
        mut rung_rx,
        cohost,
        started,
        mut external_quit_rx,
        mut external_req_rx,
        external_msg_tx,
        mut quit_rx,
        exit_on_quit,
        mut http_rx,
        mut unicast_rx,
    } = loop_state;

    log_daemon_start(&author);

    // The surfaced-events ring (`poll`/`fetch` history) lives app-side, fed by
    // the tap the caller attached to `output` before handing it in (both this
    // `output` and the app's own `Output` are clones of that one tapped sink,
    // so events interleave in surfacing order). The loop drives the ring
    // through the app's `drain_surfaced` / poll-deadline hooks — the engine
    // never names the ring's element type.

    let mut anchors = TickAnchors::now();

    // Owned Arc clone so the per-arm ctx can borrow it without colliding with `&mut state`.
    let identity = state.identity.clone();
    // Our own pubkey hex, computed once for the per-message self-echo compare.
    let our_pubkey = crate::protocol::identity::encode_pubkey(&identity.public());
    // Everything a HandlerCtx needs *except* the sender. The ctx itself
    // is built per-arm (`parts.ctx(&sender)`) rather than once out here:
    // a loop-lifetime ctx would borrow `sender` forever, and the
    // resubscribe path must replace `sender`/`receiver` when the gossip
    // stream ends.
    let parts = CtxParts {
        endpoint: &endpoint,
        mesh: &mesh_str,
        author: &author,
        identity: identity.as_ref(),
        our_pubkey: &our_pubkey,
        max_peers,
        rendezvous_id: rendezvous_params.id,
        external_msg_tx: external_msg_tx.as_ref(),
        sink: sink.as_ref(),
    };

    // Consecutive failed resubscribe attempts (reset on success); at
    // `RESUBSCRIBE_MAX_ATTEMPTS` the gossip actor itself is gone and the
    // daemon shuts down rather than pretend to be a live member.
    let mut resubscribe_attempts: u32 = 0;

    {
        let ctx = parts.ctx(&sender);
        app.on_startup(&mut state, &ctx).await;
    }

    loop {
        tokio::select! {
            () = sleep_until_opt(state.ping_round.as_ref().map(|round| round.deadline)) => {
                state.idle.external += 1;
                finalize_ping_round(&mut state, sink.as_ref());
            }
            () = sleep_until_opt(app.earliest_poll_deadline()) => {
                state.idle.external += 1;
                app.poll_deadline_elapsed();
            }
            () = sleep_until_opt(app.earliest_deadline()) => {
                state.idle.external += 1;
                app.expire_deadlines(TokioInstant::now());
            }
            ipc_msg = recv_opt(&mut ipc_rx) => match ipc_msg {
                None => ipc_rx = None,
                Some((cmd, resp_tx)) => {
                    state.idle.external += 1;
                    let ctx = parts.ctx(&sender);
                    let req = super::app::IpcRequest {
                        cmd,
                        resp: resp_tx,
                        name: &mesh_name,
                    };
                    if app.handle_ipc(req, &mut state, &ctx).await {
                        state.last_sent_at = Instant::now();
                    }
                }
            },
            http_req = recv_opt(&mut http_rx) => match http_req {
                None => http_rx = None,
                Some(req) => {
                    state.idle.external += 1;
                    let ctx = parts.ctx(&sender);
                    app.handle_http(req, &mut state, &ctx).await;
                }
            },
            event = receiver.next(), if state.gossip_open => {
                state.idle.external += 1;
                let ctx = parts.ctx(&sender);
                gossip::handle_gossip_event(event, &mut state, &mut app, &ctx).await;
            }
            // Inbound unicast rides the *same* validate + dedup path as gossip (`ingest`).
            frame = recv_opt(&mut unicast_rx) => match frame {
                Some(bytes) => {
                    state.idle.external += 1;
                    gossip::ingest(bytes, &mut state, &mut app, &parts.ctx(&sender)).await;
                }
                None => unicast_rx = None,
            },
            _ = intervals.prune.tick() => {
                state.idle.prune += 1;
                timers::tick_prune(&mut state, sink.as_ref());
            }
            _ = intervals.alive.tick() => {
                state.idle.alive += 1;
                let ctx = parts.ctx(&sender);
                alive_arm(&mut anchors, &mut state, &ctx).await;
                // Retry WebRTC negotiation for any peer we still have no
                // session with. Without this a pair gets exactly one attempt
                // ever: negotiation fires on `PeerInfo`, and once the pair is
                // linked, `PeerInfo` stops re-flooding — so a first attempt
                // lost to a transient (the peer not yet reachable, an ICE
                // hiccup) is never retried, and the pair stays relay-only for
                // the life of the link. Observed exactly that, CLI↔browser.
                crate::transport::webrtc::retry_sessions(&mut state, &ctx);
            }
            _ = intervals.sweep.tick() => {
                state.idle.sweep += 1;
                sweep_arm(&mut anchors, &mut state, sink.as_ref());
                let ctx = parts.ctx(&sender);
                app.on_tick(&mut state, &ctx).await;
            }
            _ = intervals.heal.tick() => {
                state.idle.heal += 1;
                let (mono_gap, wall_gap) = timers::note_tick_gap("heal", &mut anchors.heal, &mut anchors.heal_wall, Duration::from_secs(heal_interval_secs()));
                if state.gossip_open {
                    let ctx = parts.ctx(&sender);
                    heal_tick(&mut state, &ctx, HealTickParams {
                        gap: TickGap { mono: mono_gap, wall: wall_gap },
                        params: &rendezvous_params,
                        cohost,
                        started,
                    }, &mut rendezvous, &mut rival_probe).await;
                } else {
                    // Stream ended: resubscribe instead of healing a dead topic
                    // (see `resubscribe_tick`); the beacon keeps the mesh joinable.
                    // The loop's one error exit, and it must release the
                    // beacon on the way out for the same reason the normal
                    // one does — see `release_rendezvous`.
                    if let Err(error) = resubscribe_tick(
                        &ResubscribeEnv { gossip: &gossip, params: &rendezvous_params, parts: &parts, exit_on_quit },
                        &mut state,
                        &mut app,
                        GossipLink { sender: &mut sender, receiver: &mut receiver, attempts: &mut resubscribe_attempts },
                    ).await {
                        release_rendezvous(&mut rendezvous, &mut rival_probe).await;
                        return Err(error);
                    }
                    let ctx = parts.ctx(&sender);
                    maybe_cohost(&mut state, &ctx, &CohostArm { policy: cohost, params: &rendezvous_params, started }, &mut rendezvous, &mut rival_probe).await;
                }
            }
            // A bootstrap rung chosen off-loop (startup probe / beacon self-monitor); apply it cheaply.
            // `Ok(())` only: a closed channel (impossible while the beacon params live) disables the arm.
            // Counted `external`: like the probe verdict below it is beacon
            // work driven from off-loop, not by one of our maintenance tickers,
            // and it is silent on a settled daemon — so `external` still reads
            // 0 when idle while `wakeups` stays equal to the column sum.
            Ok(()) = rung_rx.changed() => {
                state.idle.external += 1;
                apply_rung_change(&mut rendezvous_params, &endpoint, &mut rendezvous, &rung_rx);
            }
            // The off-loop probe-before-claim answered. Its own arm rather
            // than a poll at the next heal tick: the probe already cost up to
            // `HEAL_PROBE_SECS`, and making a free rendezvous wait out another
            // 15s interval before anyone binds it would hand back the claim
            // latency this change was meant to leave untouched.
            found_rival = beacon::probe_verdict(&mut rival_probe) => {
                state.idle.external += 1;
                let claimed = beacon::claim_after_probe(&rendezvous_params, &endpoint, &mut rendezvous, found_rival).await;
                if claimed {
                    schedule_rival_recheck(&mut state, cohost, &rendezvous_params, &endpoint);
                }
            }
            _ = intervals.reclaim.tick() => {
                state.idle.reclaim += 1;
                let ctx = parts.ctx(&sender);
                let arm = CohostArm { policy: cohost, params: &rendezvous_params, started };
                // The rival re-check shed rides this ticker, NOT the heal tick:
                // simultaneous joiners' heal tickers align, and a shed deadline
                // quantized to a shared multi-second boundary re-synchronizes
                // the very sheds whose ms-scale phase offsets must differ for
                // one holder to catch the other still up (observed lockstep:
                // both shed within 5ms, both probe into the gap, both re-bind,
                // forever). At this cadence the offsets survive; the next tick
                // (~RECLAIM_INTERVAL_MS later, after the dropped endpoint has
                // unmapped) runs the re-probe via `maybe_reclaim`.
                if !shed_rival_beacon_if_due(&mut state, &arm, &mut rendezvous) {
                    maybe_reclaim(&mut state, &ctx, &arm, &mut rendezvous, &mut rival_probe).await;
                }
            }
            _ = intervals.antientropy.tick() => {
                state.idle.antientropy += 1;
                let ctx = parts.ctx(&sender);
                antientropy_arm(&mut anchors, &mut state, &ctx).await;
            }
            // Counted before the census reads them, so the tick that reports an
            // interval is itself in that interval's numbers.
            _ = intervals.state_refresh.tick() => {
                state.idle.state_refresh += 1;
                timers::tick_state_refresh(&mut state, &endpoint).await;
            }
            _ = intervals.linkstate.tick() => {
                state.idle.linkstate += 1;
                let ctx = parts.ctx(&sender);
                linkstate_arm(&mut state, &ctx).await;
            }
            _ = recv_opt(&mut external_quit_rx) => {
                // External quit is always in-process (MCP): never hard-exit (`false`).
                let ctx = parts.ctx(&sender);
                announce_and_maybe_exit(&mut state, &mut app, &ctx, QuitParams { name: &mesh_name, leave_label: &leave_label, exit_on_quit: false }).await;
                break;
            }
            req = recv_opt(&mut external_req_rx) => match req {
                None => external_req_rx = None,
                Some(req) => {
                    state.idle.external += 1;
                    let ctx = parts.ctx(&sender);
                    if app.handle_session(req, &mut state, &ctx).await {
                        state.last_sent_at = Instant::now();
                    }
                }
            },
            _ = quit_rx.recv() => {
                let ctx = parts.ctx(&sender);
                announce_and_maybe_exit(&mut state, &mut app, &ctx, QuitParams { name: &mesh_name, leave_label: &leave_label, exit_on_quit }).await;
                break;
            }
        }
        state.idle.wakeups += 1;
        app.drain_surfaced();
    }

    release_rendezvous(&mut rendezvous, &mut rival_probe).await;
    Ok(())
}

/// Close the co-hosted rendezvous endpoint before this loop's stack unwinds.
///
/// The `Rendezvous` is a loop local, and letting it merely *drop* aborts its
/// tasks while leaving the endpoint open — iroh then logs `Endpoint dropped
/// without calling Endpoint::close. Aborting ungracefully.` and tears the
/// socket down without the QUIC close. Every co-hosting member hits this on
/// every departure, which in a public mesh is every member.
///
/// A graceful close is not just quieter: it is the same courtesy
/// [`beacon::Rendezvous::shed`] pays mid-run, so peers holding a link to our
/// beacon see an immediate `NeighborDown` rather than waiting out the QUIC
/// idle timeout on a corpse.
///
/// Not in `shutdown()` — the loop owns the `Rendezvous`, and the CLI's
/// `exit_on_quit` path `process::exit`s from inside `shutdown` before any of
/// this could run (that path skips every destructor by design, so there is no
/// warning to silence there either).
///
/// An outstanding probe-before-claim goes the same way, and for the same
/// reason: its throwaway endpoint is just as capable of reaching `Drop` open.
async fn release_rendezvous(
    rendezvous: &mut Option<beacon::Rendezvous>,
    probe: &mut Option<beacon::RivalProbe>,
) {
    if let Some(rendezvous) = rendezvous.take() {
        rendezvous.shed_and_wait().await;
    }
    if let Some(probe) = probe.take() {
        probe.abort_and_close().await;
    }
}

/// Graceful shutdown: remove the statusline state file first, then
/// announce `Left` and give the broadcast a moment to reach peers.
/// Shared by both the external-quit and ctrl-c/SIGTERM/SIGHUP paths so
/// they can't drift apart.
///
/// The state-file removal is the time-critical step: an external reader
/// (the shell statusline) shows the mesh pill while the file is fresh,
/// so a leaver must clear it *immediately*. It runs before the
/// best-effort `Left` broadcast and its 500 ms propagation sleep so a
/// kill landing during that window can't strand the file with a still-fresh
/// `last_updated`, leaving a ghost pill on the statusline.
async fn shutdown<A: NodeDriver>(
    state: &mut EventLoopState,
    app: &mut A,
    ctx: &HandlerCtx<'_>,
    quit: &QuitParams<'_>,
) {
    // Release app-owned resources (fail parked app waiters, close the
    // blob-serving endpoint whose store spool is dropped with it).
    app.on_shutdown(state, ctx).await;
    #[cfg(feature = "host")]
    if let Some(sf) = state.state_file.as_ref() {
        sf.remove();
    }
    ctx.sink
        .emit(NodeEvent::Info(format!("left {}", quit.leave_label)));
    lifecycle::log_leaving(quit.name.as_str());
    // The `Left` rides gossip only — deliberately NOT mirrored over unicast
    // (unlike the meta retraction in `on_shutdown`): presence drives the
    // survivors' rendezvous fast-reclaim, and a `Left` that lands while a
    // survivor's gossip still holds the dying beacon's link re-stands the
    // beacon into a stale connection and stalls (iroh-gossip#10; see the
    // post-departure-join test's SIGKILL choreography).
    gossip::broadcast_msg(
        ctx.sender,
        &Message::new_left(ctx.mesh, ctx.author).signed(&state.identity),
    )
    .await;
    n0_future::time::sleep(Duration::from_millis(500)).await;
}

/// The mesh name (for the departure log line), the user-facing departure
/// label (the raw topic string for a topic gossip, `#name` otherwise), and
/// whether this quit should hard-exit the process — the plain values
/// [`announce_and_maybe_exit`] needs beyond the loop state and the shared
/// handler context.
struct QuitParams<'a> {
    name: &'a MeshName,
    leave_label: &'a str,
    exit_on_quit: bool,
}

/// Announce departure, then decide whether to hard-exit the process.
///
/// `exit_on_quit` is the CLI hard-exit: the CLI process exits immediately on
/// quit rather than tearing down its background tasks (advertiser,
/// localhost HTTP server, iroh) and unwinding. In-process quits pass `false`
/// and unwind cleanly instead. Under the `dhat-heap` profiling build we
/// *never* `process::exit` regardless — it skips destructors, so the heap
/// profiler would never flush `dhat-heap.json`; we fall through so `main`
/// unwinds and the profiler drops.
async fn announce_and_maybe_exit<A: NodeDriver>(
    state: &mut EventLoopState,
    app: &mut A,
    ctx: &HandlerCtx<'_>,
    quit: QuitParams<'_>,
) {
    // Empty out any parked long-poll waiters first, so a held call returns a
    // clean timeout (empty) rather than a dropped-channel error — and before
    // the `exit_on_quit` path below may `std::process::exit`. Other app-owned
    // waiters (app RPC calls) are failed in `on_shutdown` (inside `shutdown`).
    app.close_poll_waiters();
    shutdown(state, app, ctx, &quit).await;
    #[cfg(not(feature = "dhat-heap"))]
    if quit.exit_on_quit {
        std::process::exit(0);
    }
    #[cfg(feature = "dhat-heap")]
    let _ = quit.exit_on_quit;
}

/// Spawn ctrl-c (all platforms) plus SIGTERM/SIGHUP/SIGQUIT (unix)
/// listener tasks feeding a single internal quit channel.
/// `tokio::signal::ctrl_c()` inside a `select!` branch doesn't reliably
/// interrupt a blocking stdin read, so we offload signal listening to
/// dedicated tasks.
///
/// Every catchable termination signal routes through the graceful
/// `shutdown()` path so the statusline state file is removed. SIGHUP in
/// particular is what a closing parent (e.g. the Monitor that hosts the
/// daemon for a `/gossip-*` session) tends to send; without catching it
/// the default action terminated the daemon without cleanup, stranding a
/// ghost pill on the statusline. Only SIGKILL stays uncatchable.
#[cfg(feature = "host")]
fn spawn_quit_signal_tasks(exit_on_quit: bool) -> mpsc::Receiver<()> {
    let (quit_tx, quit_rx) = mpsc::channel::<()>(1);
    let ctrl_c_tx = quit_tx.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = ctrl_c_tx.send(()).await;
    });
    #[cfg(unix)]
    for kind in [
        tokio::signal::unix::SignalKind::terminate(),
        tokio::signal::unix::SignalKind::hangup(),
        tokio::signal::unix::SignalKind::quit(),
    ] {
        let signal_tx = quit_tx.clone();
        tokio::spawn(async move {
            let mut signal =
                tokio::signal::unix::signal(kind).expect("failed to register termination handler");
            signal.recv().await;
            let _ = signal_tx.send(()).await;
        });
    }
    // Only the CLI daemon owns a process to exit; the in-process driver runs
    // in-process with no parent of its own to lose, so it must never self-quit
    // on a host reparent.
    #[cfg(unix)]
    if exit_on_quit {
        spawn_orphan_watch(quit_tx);
    }
    quit_rx
}

/// Detect orphaning by the spawning agent and route it through the same quit
/// channel as a signal. A hard-killed parent (`kill -9`, a reinstall, an IDE
/// restart) can't run any cleanup, so the spawned daemon is reparented instead
/// of terminated and would otherwise linger in the mesh forever. The daemon
/// watches its *own* parent — the only mechanism that survives SIGKILL and is
/// identical on macOS and Linux (`PR_SET_PDEATHSIG` and kqueue `NOTE_EXIT` are
/// each platform-specific). When the parent vanishes we feed `quit_tx`, reusing
/// the SIGTERM path that broadcasts `left` and exits cleanly.
// `unix` alone is not enough: `libc` arrives with the `host` feature.
#[cfg(all(unix, feature = "host"))]
#[expect(
    unsafe_code,
    reason = "libc::getppid FFI; no safe wrapper, always succeeds"
)]
fn spawn_orphan_watch(quit_tx: mpsc::Sender<()>) {
    let original_ppid = unsafe { libc::getppid() };
    if !orphan_watch_warranted(original_ppid) {
        return;
    }
    let interval = Duration::from_millis(ppid_watch_interval_ms());
    tokio::spawn(async move {
        loop {
            n0_future::time::sleep(interval).await;
            let current_ppid = unsafe { libc::getppid() };
            if parent_lost(original_ppid, current_ppid) {
                let _ = quit_tx.send(()).await;
                return;
            }
        }
    });
}

/// Whether the orphan watch is worth running. Skip it when the daemon already
/// has no agent to lose — a parent pid of 1 means it was launched detached
/// straight from init/launchd, so it must never self-terminate.
#[cfg(unix)]
fn orphan_watch_warranted(original_ppid: i32) -> bool {
    original_ppid > 1
}

/// The orphaning test: the parent pid changed from the one captured at startup.
/// Comparing against the *original* (not against `1`) is what makes this correct
/// on both platforms — macOS reparents an orphan to launchd (1), but under
/// systemd Linux reparents to a subreaper at some other pid. Pid reuse can't
/// fool it: the reaper's pid won't coincidentally equal the original parent's.
#[cfg(unix)]
fn parent_lost(original_ppid: i32, current_ppid: i32) -> bool {
    original_ppid != current_ppid
}

/// A quit channel whose sender is deliberately leaked, so the receiver parks
/// forever. The loop's quit arm then only ever fires from `external_quit_rx`.
fn never_quit() -> mpsc::Receiver<()> {
    let (quit_tx, quit_rx) = mpsc::channel::<()>(1);
    std::mem::forget(quit_tx);
    quit_rx
}

/// Where [`spawn_ipc_rx`] would bind the control socket, and whether to at all.
/// Grouped rather than passed loose: the three are only ever used together, to
/// build one path.
#[cfg(feature = "host")]
#[derive(Clone, Copy)]
struct IpcBinding<'a> {
    /// In-process drivers (library API / MCP) use the typed `session_rx` and
    /// bind no socket.
    disabled: bool,
    runtime_base: Option<&'a std::path::Path>,
    mesh: &'a MeshId,
    author: &'a Nickname,
}

/// Resolve the IPC receiver: reuse a pre-wired channel (MCP / library API) or,
/// for the CLI, spawn the unix-socket listener and own the channel.
/// Returning `Option` keeps the loop's `select!` arm uniform.
#[cfg(feature = "host")]
fn spawn_ipc_rx<C: serde::de::DeserializeOwned + Send + 'static>(
    binding: &IpcBinding<'_>,
    sink: &std::sync::Arc<dyn NodeSink>,
) -> Option<mpsc::Receiver<IpcMessage<C>>> {
    let &IpcBinding {
        disabled,
        runtime_base,
        mesh,
        author,
    } = binding;
    // In-process mode (in-process: library API or MCP): no socket. Returning `None` leaves
    // the loop's IPC `select!` arm inert (it pends forever), so the
    // unix-socket listener is never bound — those drivers use the typed
    // `session_rx` instead.
    if disabled {
        return None;
    }
    // No base, no socket: an embedder that configured no runtime root has
    // nowhere to put one. Same inert `select!` arm as the in-process drivers.
    let base = runtime_base?;
    // Bind synchronously here, *before* the caller marks the session ready,
    // so an accepting socket always exists by the time the readiness flag
    // flips. Only the (always-running) accept loop is spawned. A bind
    // failure is non-fatal — the daemon still gossips; it just can't take
    // IPC — matching the prior best-effort behavior, only now observed
    // before readiness rather than racing it.
    let listener = match crate::transport::ipc::bind(base, mesh, author) {
        Ok(listener) => listener,
        Err(error) => {
            sink.emit(NodeEvent::Error(format!("IPC: {error}")));
            tracing::warn!(%error, "IPC: failed to bind socket");
            return None;
        }
    };
    let (ipc_tx, rx) = mpsc::channel::<IpcMessage<C>>(32);
    tokio::spawn(crate::transport::ipc::serve::<C>(
        listener,
        ipc_tx,
        std::sync::Arc::clone(sink),
    ));
    Some(rx)
}

/// Sleep until a ping round's deadline, or pend forever when no round
/// is active. Lets the event loop's `select!` carry a ping-finalize arm
/// that only fires while a round is in flight, without borrowing
/// `state` across the await (the deadline is copied out beforehand).
async fn sleep_until_opt(deadline: Option<TokioInstant>) {
    match deadline {
        Some(at) => n0_future::time::sleep_until(at).await,
        None => std::future::pending::<()>().await,
    }
}

/// Build and emit the `ping_report` for the elapsed round, then clear
/// it. RTT is each pong's local arrival minus the probe broadcast time.
fn finalize_ping_round(state: &mut EventLoopState, sink: &dyn NodeSink) {
    let Some(round) = state.ping_round.take() else {
        return;
    };
    // (Live per-neighbour RTT/delivery telemetry once fed the link metric; the
    // multihop transport currently advertises a flat link cost, so the ping round
    // only produces the user-facing report below. Re-wiring telemetry into the
    // multihop metric is a future enhancement.)
    let mut peers: Vec<(Nickname, u64)> = round
        .pongs
        .iter()
        .map(|(nickname, arrival)| {
            let rtt_ms =
                u64::try_from(arrival.duration_since(round.t1).as_millis()).unwrap_or(u64::MAX);
            (nickname.clone(), rtt_ms)
        })
        .collect();
    peers.sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
    // The in-process `ping` request waits on this channel (no event stream to
    // read the report from); the CLI/IPC path leaves it unset and consumes the
    // `ping_report` event below instead.
    if let Some(resp) = round.resp {
        let _ = resp.send(peers.clone());
    }
    // `known` must never be less than the number that responded: a peer can
    // pong and then leave the roster before this ~10s finalize, which would
    // otherwise report responded > known. Clamp so the count stays coherent.
    let known = state.peers.len().max(peers.len());
    sink.emit(NodeEvent::PingReport { peers, known });
}

/// Whether a co-hosting member probes the rendezvous before claiming it —
/// the single source of truth for the `probe_first` flag passed to
/// [`beacon::ensure`] from every claim site (startup, heal tick, reclaim
/// window). Only `Eager` (the mesh origin) skips the probe: a brand-new
/// mesh has no peers to self-collide with. Every other policy probes, so
/// it never binds a duplicate of a rendezvous a peer already serves — the
/// directory advertiser's shared `rendezvous_id` (`EagerProbed`) or a
/// survivor mid-failover (`Deferred`). Exhaustive on purpose: a new variant
/// must make this decision explicitly rather than defaulting to "probe".
fn probes_before_claim(cohost: CoHostPolicy) -> bool {
    match cohost {
        CoHostPolicy::Eager => false,
        CoHostPolicy::EagerProbed | CoHostPolicy::Deferred | CoHostPolicy::Never => true,
    }
}

/// Whether this member claims the rendezvous **at startup** (t=0) rather
/// than deferring to the heal gate ([`may_cohost`]) or never co-hosting.
/// The eager policies claim immediately so a beacon exists before any
/// joiner/discoverer subscribes; whether that claim probes first is the
/// orthogonal [`probes_before_claim`] axis.
fn claims_at_startup(cohost: CoHostPolicy) -> bool {
    match cohost {
        CoHostPolicy::Eager | CoHostPolicy::EagerProbed => true,
        CoHostPolicy::Deferred | CoHostPolicy::Never => false,
    }
}

/// May this member co-host the rendezvous yet? See [`CoHostPolicy`].
/// `Never` never co-hosts (a pure consumer); `Eager`/`EagerProbed` always
/// may; a `Deferred` member only once `meshed`, or after
/// `cohost_grace_secs` for an empty mesh (then probe-gated in
/// `beacon::ensure`). Pure + cheap; never blocks `ready`.
fn may_cohost(cohost: CoHostPolicy, meshed: bool, started: Instant) -> bool {
    match cohost {
        CoHostPolicy::Never => false,
        CoHostPolicy::Eager | CoHostPolicy::EagerProbed => true,
        CoHostPolicy::Deferred => {
            meshed || started.elapsed().as_secs() >= crate::util::tuning::cohost_grace_secs()
        }
    }
}

/// Monotonic `gap` past `stall_threshold`: the process was throttled
/// (but not fully frozen) between heal ticks (macOS App Nap / timer
/// coalescing) long enough that the mesh died of idle timeout.
fn is_resume(gap: Duration, stall_threshold: Duration) -> bool {
    gap > stall_threshold
}

/// The macOS-sleep signature the monotonic gap is blind to: the
/// monotonic clock pauses in lockstep with the frozen process, so a
/// day-long suspend shows only a few seconds of `mono_gap` while the
/// wall clock jumped the whole way. A `wall_gap` exceeding `mono_gap`
/// by more than `stall_threshold` means time elapsed that the process
/// could not observe — it was suspended and the mesh is dead.
fn is_wall_resume(wall_gap: Duration, mono_gap: Duration, stall_threshold: Duration) -> bool {
    wall_gap.saturating_sub(mono_gap) > stall_threshold
}

/// One heal tick (factored out of `event_loop` for the line budget).
/// On a resume edge the steady probe can't rebuild a mesh that fully
/// died while the timers were frozen, so re-enter cold-joiner mode,
/// re-assert the relay-homed rendezvous hint (the network changed),
/// and run the long re-bootstrap probe. Otherwise the normal probe.
///
/// A resume is either a monotonic stall (throttle) OR a wall-vs-
/// monotonic divergence (suspend/sleep) — the latter is the only
/// signal that survives a macOS sleep, which freezes the monotonic
/// clock with the process.
async fn run_heal(
    gap: TickGap,
    state: &mut EventLoopState,
    ctx: &HandlerCtx<'_>,
    params: &beacon::RendezvousParams,
) {
    let threshold = Duration::from_secs(heal_stall_threshold_secs());
    let hard_edge = is_resume(gap.mono, threshold) || is_wall_resume(gap.wall, gap.mono, threshold);
    if hard_edge {
        tracing::warn!(
            target: "fofoca::gossip",
            mono_gap_ms = u64::try_from(gap.mono.as_millis()).unwrap_or(u64::MAX),
            wall_gap_ms = u64::try_from(gap.wall.as_millis()).unwrap_or(u64::MAX),
            "heal: hard re-bootstrap edge"
        );
        state.note_degraded();
        // The frozen-era link view is stale by definition; clearing this
        // re-arms the regular tick's probe until a fresh NeighborUp.
        state.rendezvous_linked = false;
        // A rival re-check deadline that "matured" while the process was
        // frozen would shed the beacon into a mesh that is still
        // re-forming; push it out a steady interval so the re-bootstrap
        // settles first.
        if state.next_rival_recheck.is_some() {
            state.next_rival_recheck = Some(
                Instant::now() + Duration::from_secs(crate::util::tuning::rival_recheck_secs()),
            );
        }
        // Re-assert the rendezvous hint (the network changed). The rung
        // is re-validated off-loop by the beacon's liveness self-monitor,
        // so a rung that died during the freeze self-corrects — no inline
        // ladder walk on the event loop here.
        setup::register_rendezvous(ctx.endpoint, params);
        gossip::heal::tick_heal_hard(ctx.endpoint, params.id, ctx.sender).await;
    } else if state.rendezvous_linked {
        // A live rendezvous link has nothing to heal — and healing it
        // anyway is what flapped it once per tick (both heal legs dial
        // `GOSSIP_ALPN`, which the beacon's gossip adopts, superseding
        // the healthy link; see `tick_heal`). `NeighborDown` re-arms
        // this gate instantly.
        tracing::debug!(
            target: "fofoca::gossip",
            "heal tick: rendezvous linked; idle"
        );
    } else {
        gossip::heal::tick_heal(params.id, ctx.sender).await;
    }
    // Rendezvous-independent re-bridge. Fires on the hard (resume) edge —
    // where a reused endpoint id can be stuck behind a stale *accepted*
    // rendezvous connection (iroh-gossip#10), so the rendezvous re-graft
    // alone may not re-admit us — or on steady-state loss of every live
    // link (relay flap). Re-dials remembered peers directly. Skipped when
    // healthy (`hard_edge` false and links remain) and for a lone node
    // (nothing remembered), so it adds no churn. `linked_endpoints` is
    // not cleared on the resume edge, hence the explicit `hard_edge` arm.
    if (hard_edge || state.linked_endpoints.is_empty()) && !state.known_endpoints.is_empty() {
        gossip::heal::rebridge_known(ctx.sender, &state.known_endpoints).await;
    }
    // Starvation watchdog: links/heal can look busy while no traffic
    // flows (the roster-collapse signature), so the last word every heal
    // tick is a check on verified *inbound* silence.
    if state.starvation_due(
        Instant::now(),
        Duration::from_secs(crate::util::tuning::starvation_threshold_secs()),
    ) {
        gossip::heal::recover_from_starvation(state, ctx).await;
    }
}

/// A tick's elapsed-time gap on both clocks: monotonic (throttle-blind) and
/// wall (the only one a macOS suspend shows up on) — see [`is_resume`] /
/// [`is_wall_resume`].
#[derive(Clone, Copy)]
struct TickGap {
    mono: Duration,
    wall: Duration,
}

/// The value cluster [`heal_tick`] needs beyond the loop state and handler
/// context: the tick's gap, the rendezvous params to heal/claim under, which
/// co-host policy governs this session, and (for the unmeshed-joiner grace)
/// when the event loop started.
struct HealTickParams<'a> {
    gap: TickGap,
    params: &'a beacon::RendezvousParams,
    cohost: CoHostPolicy,
    started: Instant,
}

/// One heal tick: re-bootstrap/heal, then (re)claim the beacon if we
/// should co-host. Grouped so the event-loop arm stays a one-liner.
async fn heal_tick(
    state: &mut EventLoopState,
    ctx: &HandlerCtx<'_>,
    tick: HealTickParams<'_>,
    rendezvous: &mut Option<beacon::Rendezvous>,
    probe: &mut Option<beacon::RivalProbe>,
) {
    run_heal(tick.gap, state, ctx, tick.params).await;
    let arm = CohostArm {
        policy: tick.cohost,
        params: tick.params,
        started: tick.started,
    };
    maybe_cohost(state, ctx, &arm, rendezvous, probe).await;
}

/// Per-timer gap anchors; the heal gap also drives the resume-edge hard
/// re-bootstrap. Each timer carries a monotonic anchor AND a wall-clock
/// anchor: on macOS the monotonic clock pauses in lockstep with a
/// sleeping process, so only the wall gap reveals a suspend (see
/// `note_tick_gap` / `run_heal`).
struct TickAnchors {
    alive: Instant,
    sweep: Instant,
    heal: Instant,
    antientropy: Instant,
    alive_wall: i64,
    sweep_wall: i64,
    heal_wall: i64,
    antientropy_wall: i64,
}

impl TickAnchors {
    fn now() -> Self {
        let mono = Instant::now();
        let wall = crate::util::clock::unix_secs();
        Self {
            alive: mono,
            sweep: mono,
            heal: mono,
            antientropy: mono,
            alive_wall: wall,
            sweep_wall: wall,
            heal_wall: wall,
            antientropy_wall: wall,
        }
    }
}

/// Everything a [`HandlerCtx`] needs except the gossip sender. The loop
/// holds one of these and builds the ctx per-arm (`parts.ctx(&sender)`):
/// a loop-lifetime ctx would borrow `sender` forever, and the
/// resubscribe path must be able to replace it.
struct CtxParts<'a> {
    endpoint: &'a Endpoint,
    mesh: &'a MeshId,
    author: &'a Nickname,
    identity: &'a crate::protocol::identity::Identity,
    our_pubkey: &'a str,
    max_peers: usize,
    rendezvous_id: EndpointId,
    external_msg_tx: Option<&'a broadcast::Sender<Message>>,
    sink: &'a dyn NodeSink,
}

impl<'a> CtxParts<'a> {
    fn ctx<'b>(&'b self, sender: &'b MeshSender) -> HandlerCtx<'b>
    where
        'a: 'b,
    {
        HandlerCtx {
            sender,
            endpoint: self.endpoint,
            mesh: self.mesh,
            author: self.author,
            identity: self.identity,
            our_pubkey: self.our_pubkey,
            max_peers: self.max_peers,
            rendezvous_id: self.rendezvous_id,
            external_msg_tx: self.external_msg_tx,
            sink: self.sink,
        }
    }
}

/// Outcome of one resubscribe attempt (the heal arm drives one per
/// tick while the gossip stream is down).
enum Resubscribe {
    Restored(GossipSender, GossipReceiver),
    Pending,
    Fatal,
}

/// The resubscribe attempt's read-only environment: the gossip frontend to
/// re-subscribe through, the rendezvous params to bootstrap from, the shared
/// ctx-building parts (for its `sink` and to rebuild a [`HandlerCtx`] once the
/// sender is swapped), and the CLI hard-exit flag for the `Fatal` path.
struct ResubscribeEnv<'a> {
    gossip: &'a iroh_gossip::net::Gossip,
    params: &'a beacon::RendezvousParams,
    parts: &'a CtxParts<'a>,
    exit_on_quit: bool,
}

/// The live gossip link a resubscribe mutates: the sender/receiver pair
/// swapped in on success, and the consecutive-failure counter it resets or
/// increments.
struct GossipLink<'a> {
    sender: &'a mut MeshSender,
    receiver: &'a mut GossipReceiver,
    attempts: &'a mut u32,
}

/// One heal-tick turn while the gossip stream is down: attempt the
/// resubscribe and, on success, swap in the fresh sender/receiver,
/// drain the dead subscription's buffer (the actor counts those
/// messages as delivered — overlay dedup will never re-push them, and
/// anti-entropy resends of them are deduped too, so the buffer is the
/// only copy), then re-enter the overlay via the starvation-recovery
/// primitive (degraded mesh, throttles cleared, known peers re-dialed,
/// arrival re-announced). On `Fatal` (the actor itself is gone) the
/// daemon stops posing as a live member: statusline state file cleared
/// (a `Left` broadcast is pointless on a dead topic), `exit(1)` on the
/// CLI path, `Err` for in-process drivers.
async fn resubscribe_tick(
    env: &ResubscribeEnv<'_>,
    state: &mut EventLoopState,
    app: &mut dyn NodeApp,
    link: GossipLink<'_>,
) -> Result<()> {
    match try_resubscribe(env, state, link.attempts).await {
        Resubscribe::Restored(new_sender, new_receiver) => {
            let mut dead_receiver = std::mem::replace(link.receiver, new_receiver);
            link.sender.replace_gossip(new_sender);
            state.gossip_open = true;
            // The dead subscription's link view is void; the fresh one
            // emits its own NeighborUps (and re-arms the probe gate).
            state.rendezvous_linked = false;
            let ctx = env.parts.ctx(link.sender);
            gossip::drain_dead_receiver(&mut dead_receiver, state, app, &ctx).await;
            drop(dead_receiver);
            gossip::heal::recover_from_starvation(state, &ctx).await;
        }
        Resubscribe::Pending => {}
        Resubscribe::Fatal => {
            #[cfg(feature = "host")]
            if let Some(state_file) = state.state_file.as_ref() {
                state_file.remove();
            }
            env.parts.sink.emit(NodeEvent::Error(
                "gossip subscription unrecoverable; shutting down".to_owned(),
            ));
            #[cfg(not(feature = "dhat-heap"))]
            if env.exit_on_quit {
                std::process::exit(1);
            }
            #[cfg(feature = "dhat-heap")]
            let _ = env.exit_on_quit;
            anyhow::bail!("gossip subscription unrecoverable after repeated resubscribe attempts");
        }
    }
    Ok(())
}

/// Re-open the gossip topic after its stream terminally ended. The
/// designed-for remedy, not a workaround: iroh-gossip closes a lagging
/// subscriber outright and its docs instruct "close and re-open".
/// Bootstrap is the rendezvous plus every remembered peer so the fresh
/// subscription re-grafts without waiting for lookups. `Fatal` after
/// `RESUBSCRIBE_MAX_ATTEMPTS` consecutive failures: a subscribe error
/// means the gossip actor itself is gone (endpoint closed), which no
/// retry can fix.
async fn try_resubscribe(
    env: &ResubscribeEnv<'_>,
    state: &EventLoopState,
    attempts: &mut u32,
) -> Resubscribe {
    let mut bootstrap = vec![env.params.id];
    bootstrap.extend(state.known_endpoints.iter().copied());
    match env.gossip.subscribe(env.params.topic_id, bootstrap).await {
        Ok(topic) => {
            *attempts = 0;
            tracing::warn!(
                target: "fofoca::gossip",
                "gossip stream restored (resubscribed)"
            );
            env.parts.sink.emit(NodeEvent::Info(
                "gossip stream restored; rejoining the mesh".to_owned(),
            ));
            let (sender, receiver) = topic.split();
            Resubscribe::Restored(sender, receiver)
        }
        Err(error) => {
            *attempts += 1;
            tracing::warn!(
                target: "fofoca::gossip",
                %error,
                attempts = *attempts,
                "gossip resubscribe failed"
            );
            if *attempts >= RESUBSCRIBE_MAX_ATTEMPTS {
                Resubscribe::Fatal
            } else {
                Resubscribe::Pending
            }
        }
    }
}

/// Apply a bootstrap rung chosen **off the event loop** (the startup
/// confirmation probe or the beacon's liveness self-monitor publishing
/// through `rendezvous_params.rung_tx`). Cheap and non-blocking — the
/// ladder walk already ran in the background task. If the new rung
/// differs from the one we're homed on, re-pre-register `rendezvous_id`
/// at it and drop the beacon so `maybe_cohost` rebuilds it homed on the
/// new rung.
fn apply_rung_change(
    params: &mut beacon::RendezvousParams,
    endpoint: &Endpoint,
    rendezvous: &mut Option<beacon::Rendezvous>,
    rung_rx: &watch::Receiver<Option<RelayUrl>>,
) {
    let selected = rung_rx.borrow().clone();
    if let lookup::RungRefresh::Rehome(new) =
        lookup::plan_rung_refresh(params.bootstrap_relay.as_ref(), selected)
    {
        tracing::info!(
            target: "fofoca::beacon",
            old = ?params.bootstrap_relay,
            new = ?new,
            "bootstrap relay rung changed; re-registering rendezvous and re-homing the beacon"
        );
        params.bootstrap_relay = new;
        setup::register_rendezvous(endpoint, params);
        // Release the beacon so `maybe_cohost` → `beacon::ensure` rebuilds it
        // homed on the new rung at the next heal/reclaim tick — `shed`, not a
        // plain drop. The old endpoint is still open and still homed on the
        // rung we are abandoning; dropping it leaves iroh to tear the socket
        // down ungracefully and leaves peers linked to a corpse until the QUIC
        // idle timeout. This fires exactly when relays are flaky, which is
        // when a graceful handover matters most.
        if let Some(old) = rendezvous.take() {
            old.shed();
        }
    }
}

/// The co-host decision inputs a heal/reclaim tick needs: which policy
/// governs this session, the rendezvous params to (re)claim under, and
/// (for the unmeshed-joiner grace `maybe_cohost` alone reads) when the
/// event loop started.
struct CohostArm<'a> {
    policy: CoHostPolicy,
    params: &'a beacon::RendezvousParams,
    started: Instant,
}

/// Heal-tick co-host: stand up the beacon if this member may serve it
/// now (`may_cohost`). Claim-if-free in private; in public a non-`Eager`
/// member probes first (`beacon::ensure`) so it never registers a
/// duplicate rendezvous that would capture its own bootstrap dial.
async fn maybe_cohost(
    state: &mut EventLoopState,
    ctx: &HandlerCtx<'_>,
    arm: &CohostArm<'_>,
    current: &mut Option<beacon::Rendezvous>,
    probe: &mut Option<beacon::RivalProbe>,
) {
    if may_cohost(arm.policy, state.meshed, arm.started) {
        let claimed = beacon::ensure(
            arm.params,
            ctx.endpoint,
            current,
            probes_before_claim(arm.policy),
            probe,
        )
        .await;
        if claimed {
            schedule_rival_recheck(state, arm.policy, arm.params, ctx.endpoint);
        }
    }
}

/// Fast event-driven failover: while the post-`NeighborDown` reclaim
/// window is open, retry the rendezvous claim so a survivor takes the
/// freed port in ~1s instead of waiting for the 15s heal tick. A no-op
/// outside the window (just an `Instant` compare) and idempotent once
/// the rendezvous is held. `Never` consumers never reclaim; everyone
/// else probes first (`!Eager`) so a survivor that already took over
/// isn't displaced by a colliding duplicate.
async fn maybe_reclaim(
    state: &mut EventLoopState,
    ctx: &HandlerCtx<'_>,
    arm: &CohostArm<'_>,
    current: &mut Option<beacon::Rendezvous>,
    probe: &mut Option<beacon::RivalProbe>,
) {
    if arm.policy != CoHostPolicy::Never
        && state
            .reclaim_until
            .is_some_and(|deadline| Instant::now() < deadline)
    {
        let claimed = beacon::ensure(
            arm.params,
            ctx.endpoint,
            current,
            probes_before_claim(arm.policy),
            probe,
        )
        .await;
        if claimed {
            schedule_rival_recheck(state, arm.policy, arm.params, ctx.endpoint);
        }
    }
}

/// Whether this session's beacon is subject to the periodic rival
/// re-check shed: only a **public** `EagerProbed` co-host. `EagerProbed`
/// claimants share a `rendezvous_id` with concurrent peers (topic joiners,
/// directory advertisers) and can double-claim inside each other's probe
/// window; every other policy either owns the identity from t=0 (`Eager`
/// creator), meshes before claiming (`Deferred`), or never claims
/// (`Never`). A loopback mesh's port ladder arbitrates atomically at bind
/// time (`AddrInUse` + identity probe), so no split exists to fix there.
fn rival_recheck_applies(policy: CoHostPolicy, public: bool) -> bool {
    policy == CoHostPolicy::EagerProbed && public
}

/// When the next rival re-check shed should run, from the moment of a
/// claim. Round 0 (a startup claim) is the *fast first* check plus a
/// deterministic endpoint-id phase offset — the tie-break that orders
/// simultaneous claimants so the earlier one sheds, finds the later
/// one's still-held beacon, and yields. Later rounds run the steady
/// cadence (lone vs meshed tier) plus fresh random jitter, breaking the
/// residual both-shed-together collision geometrically.
fn next_recheck_delay(round: u32, meshed: bool, endpoint_id: EndpointId) -> Duration {
    use crate::util::consts::RIVAL_RECHECK_OFFSET_SPAN_SECS;
    use crate::util::tuning::{
        rival_recheck_first_secs, rival_recheck_meshed_secs, rival_recheck_secs,
    };

    if round == 0 {
        let mut prefix = [0u8; 8];
        prefix.copy_from_slice(&endpoint_id.as_bytes()[..8]);
        // Pubkey bytes are already uniform — no hashing needed.
        let offset_ms = u64::from_le_bytes(prefix) % (RIVAL_RECHECK_OFFSET_SPAN_SECS * 1000);
        return Duration::from_secs(rival_recheck_first_secs()) + Duration::from_millis(offset_ms);
    }
    let base_secs = if meshed {
        rival_recheck_meshed_secs()
    } else {
        rival_recheck_secs()
    };
    // Jitter spans the full base: two split holders re-jitter from
    // near-aligned schedules every round, and the wider the span the more
    // likely one's probe window lands while the other still holds.
    let jitter_ms = rand::Rng::random_range(&mut rand::rng(), 0..=base_secs.saturating_mul(1000));
    Duration::from_secs(base_secs) + Duration::from_millis(jitter_ms)
}

/// Arm the next rival re-check after a fresh claim (a `None` → live
/// `beacon::ensure` transition). No-op for sessions the shed doesn't
/// apply to, so every claim site can call it unconditionally.
fn schedule_rival_recheck(
    state: &mut EventLoopState,
    policy: CoHostPolicy,
    params: &beacon::RendezvousParams,
    endpoint: &Endpoint,
) {
    if !rival_recheck_applies(policy, params.bind_ports.is_empty()) {
        return;
    }
    let delay = next_recheck_delay(state.rival_recheck_rounds, state.meshed, endpoint.id());
    state.next_rival_recheck = Some(Instant::now() + delay);
}

/// The rival re-check itself: at the scheduled deadline, **release** the
/// held beacon and let probe-before-claim re-arbitrate on the reclaim
/// burst. Two `EagerProbed` members that claimed inside each other's
/// probe window both hold the same `rendezvous_id` and each captures its
/// own bootstrap dial — a split nothing else repairs, because a holder's
/// dial of the shared id preferentially reaches itself. Dropping our copy
/// first removes us from every resolution channel (relay registration,
/// mDNS record, pooled connection), so the re-probe's answer is finally
/// meaningful: *connects* ⇒ a rival serves it, stay a peer and let
/// the heal re-graft merge the overlays; *times out* ⇒ genuinely alone,
/// re-claim. Returns whether a shed happened (the caller then skips this
/// tick's synchronous re-claim).
fn shed_rival_beacon_if_due(
    state: &mut EventLoopState,
    arm: &CohostArm<'_>,
    rendezvous: &mut Option<beacon::Rendezvous>,
) -> bool {
    if rendezvous.is_none() || !rival_recheck_applies(arm.policy, arm.params.bind_ports.is_empty())
    {
        return false;
    }
    let due = state
        .next_rival_recheck
        .is_some_and(|deadline| Instant::now() >= deadline);
    if !due {
        return false;
    }
    tracing::info!(
        target: "fofoca::gossip",
        "beacon rival re-check: releasing the rendezvous to re-probe for a same-id co-host"
    );
    // `shed`, not a plain drop: the graceful endpoint close turns the
    // peer's link to its own dead beacon into an immediate
    // `NeighborDown` instead of a zombie that only dies at the QUIC idle
    // timeout — the zombie both stalls the post-shed re-graft and leaves
    // a poisoned pool entry under the shared id.
    if let Some(held) = rendezvous.take() {
        held.shed();
    }
    state.next_rival_recheck = None;
    state.rival_recheck_rounds = state.rival_recheck_rounds.saturating_add(1);
    // Don't wait for that `NeighborDown` either — clear the link flag now
    // (mirroring the hard resume edge), or a yielding node's heal ticks
    // idle on "rendezvous linked" instead of grafting the rival's beacon.
    state.rendezvous_linked = false;
    // Arm the fast burst explicitly rather than waiting for our own
    // beacon's `NeighborDown` to do it — the re-probe (and the re-claim
    // when no rival exists) then runs within ~RECLAIM_INTERVAL_MS.
    state.reclaim_until = Some(Instant::now() + Duration::from_secs(RECLAIM_WINDOW_SECS));
    true
}

/// The time-driven maintenance tickers.
struct MaintenanceIntervals {
    prune: n0_future::time::Interval,
    alive: n0_future::time::Interval,
    sweep: n0_future::time::Interval,
    heal: n0_future::time::Interval,
    /// Fast event-driven failover burst; only does work while
    /// `state.reclaim_until` is open (armed on `NeighborDown`).
    reclaim: n0_future::time::Interval,
    /// Periodic anti-entropy digest broadcast (recover messages missed
    /// while partitioned/asleep).
    antientropy: n0_future::time::Interval,
    state_refresh: n0_future::time::Interval,
    /// Periodic relay link-state re-broadcast (our own measured links), so peers
    /// keep a fresh routing graph.
    linkstate: n0_future::time::Interval,
}

/// Build the maintenance tickers, eating the first immediate tick on
/// the ones that must wait a full period (we just announced `Joined`).
///
/// Every ticker uses [`MissedTickBehavior::Skip`]: after the monotonic clock
/// jumps (an App Nap throttle, a SIGSTOP freeze), a default `Burst` ticker
/// fires a catch-up salvo — several anti-entropy digests back-to-back, a
/// heal immediately after the hard re-bootstrap it just ran, a prune replaying
/// its backlog — and poisons the tick-gap telemetry (the burst ticks report a
/// ~0 gap). Each tick here means "do the maintenance now", so a skipped tick is
/// free; `Skip` collapses the salvo to one tick on the next aligned boundary.
async fn build_maintenance_intervals() -> MaintenanceIntervals {
    use n0_future::time::MissedTickBehavior::Skip;

    let mut prune = n0_future::time::interval(Duration::from_mins(1));
    prune.set_missed_tick_behavior(Skip);
    let mut alive = n0_future::time::interval(Duration::from_secs(ALIVE_INTERVAL_SECS));
    alive.set_missed_tick_behavior(Skip);
    alive.tick().await;
    let mut sweep = n0_future::time::interval(Duration::from_secs(sweep_interval_secs()));
    sweep.set_missed_tick_behavior(Skip);
    sweep.tick().await;
    let mut heal = n0_future::time::interval(Duration::from_secs(heal_interval_secs()));
    heal.set_missed_tick_behavior(Skip);
    heal.tick().await;
    let mut reclaim = n0_future::time::interval(Duration::from_millis(RECLAIM_INTERVAL_MS));
    reclaim.set_missed_tick_behavior(Skip);
    reclaim.tick().await;
    let mut antientropy =
        n0_future::time::interval(Duration::from_secs(antientropy_interval_secs()));
    antientropy.set_missed_tick_behavior(Skip);
    antientropy.tick().await;
    let mut state_refresh = n0_future::time::interval(Duration::from_secs(STATE_REFRESH_SECS));
    state_refresh.set_missed_tick_behavior(Skip);
    state_refresh.tick().await;
    let mut linkstate = n0_future::time::interval(Duration::from_secs(LINKSTATE_INTERVAL_SECS));
    linkstate.set_missed_tick_behavior(Skip);
    linkstate.tick().await;
    MaintenanceIntervals {
        prune,
        alive,
        sweep,
        heal,
        reclaim,
        antientropy,
        state_refresh,
        linkstate,
    }
}

/// `select!`-friendly receive over an optional mpsc channel: yields
/// the next item, `None` once the channel closes, and pends forever
/// when the channel is absent so the arm stays inert for drivers that
/// don't wire it (CLI/MCP). Shared by the ipc / external-quit /
/// external-send arms so the idiom isn't written three ways.
async fn recv_opt<T>(rx: &mut Option<mpsc::Receiver<T>>) -> Option<T> {
    match rx.as_mut() {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        CoHostPolicy, claims_at_startup, is_resume, is_wall_resume, next_recheck_delay,
        orphan_watch_warranted, parent_lost, probes_before_claim, rival_recheck_applies,
    };

    #[test]
    fn directory_advertiser_claims_at_startup_with_probe() {
        // Regression for the duplicate-beacon directory bug: an advertiser
        // must co-host the shared rendezvous from t=0 *and* probe-first, so a
        // second advertiser into the same directory defers instead of binding
        // a duplicate (which partitioned the directory in public mode — only
        // one mesh was discoverable). The pre-fix policy was the no-probe
        // `Eager` (claims, doesn't probe), which the probe assertion guards.
        let advertiser = crate::daemon::config::DIRECTORY_ADVERTISER_COHOST;
        assert!(claims_at_startup(advertiser), "must claim at t=0");
        assert!(
            probes_before_claim(advertiser),
            "must probe before claiming"
        );

        // The mesh origin (`create`) claims at startup but skips the probe;
        // joiners and consumers don't claim at startup at all.
        assert!(claims_at_startup(CoHostPolicy::Eager));
        assert!(!probes_before_claim(CoHostPolicy::Eager));
        assert!(!claims_at_startup(CoHostPolicy::Deferred));
        assert!(!claims_at_startup(CoHostPolicy::Never));
    }

    #[test]
    fn rival_recheck_gates_on_eager_probed_and_public() {
        // The shed exists for shared-rendezvous claimants racing each other:
        // topic joiners and directory advertisers, both `EagerProbed` on a
        // public (ephemeral, address-lookup) rendezvous.
        assert!(rival_recheck_applies(CoHostPolicy::EagerProbed, true));
        // The loopback port ladder arbitrates atomically at bind time — no
        // split to fix, shedding would only churn the beacon.
        assert!(!rival_recheck_applies(CoHostPolicy::EagerProbed, false));
        // Every other policy either owns the identity from t=0, meshes
        // before claiming, or never claims.
        assert!(!rival_recheck_applies(CoHostPolicy::Eager, true));
        assert!(!rival_recheck_applies(CoHostPolicy::Deferred, true));
        assert!(!rival_recheck_applies(CoHostPolicy::Never, true));
    }

    #[test]
    fn first_recheck_delay_is_deterministic_and_bounded() {
        use crate::util::consts::{RIVAL_RECHECK_FIRST_SECS, RIVAL_RECHECK_OFFSET_SPAN_SECS};

        let id = |byte: u8| iroh::SecretKey::from_bytes(&[byte; 32]).public();

        // Round 0 must be a *deterministic* function of the endpoint id — it
        // is the tie-break ordering simultaneous claimants, so per-call
        // randomness would defeat it.
        assert_eq!(
            next_recheck_delay(0, false, id(1)),
            next_recheck_delay(0, false, id(1))
        );

        // Base + phase offset, offset strictly inside the span.
        let base = Duration::from_secs(RIVAL_RECHECK_FIRST_SECS);
        let span = Duration::from_secs(RIVAL_RECHECK_OFFSET_SPAN_SECS);
        for byte in 0..8u8 {
            let delay = next_recheck_delay(0, false, id(byte));
            assert!(
                delay >= base && delay < base + span,
                "out of bounds: {delay:?}"
            );
        }

        // Distinct ids must (in practice) spread across the span — all-equal
        // offsets would mean the tie-break never orders anyone.
        let all_equal = (1..8u8).all(|byte| {
            next_recheck_delay(0, false, id(byte)) == next_recheck_delay(0, false, id(0))
        });
        assert!(!all_equal, "phase offsets did not spread across ids");
    }

    #[test]
    fn steady_recheck_delay_is_jittered_within_bounds() {
        use crate::util::consts::{RIVAL_RECHECK_MESHED_SECS, RIVAL_RECHECK_SECS};

        let id = iroh::SecretKey::from_bytes(&[9; 32]).public();

        // Steady rounds: base by tier plus jitter in [0, base].
        let lone_base = Duration::from_secs(RIVAL_RECHECK_SECS);
        let meshed_base = Duration::from_secs(RIVAL_RECHECK_MESHED_SECS);
        for _ in 0..8 {
            let lone = next_recheck_delay(1, false, id);
            assert!(lone >= lone_base && lone <= lone_base * 2);
            let meshed = next_recheck_delay(1, true, id);
            assert!(meshed >= meshed_base && meshed <= meshed_base * 2);
        }
    }

    #[test]
    fn orphan_watch_fires_only_on_a_parent_change() {
        // The agent that spawned us is alive ⇒ same ppid ⇒ stay running.
        assert!(!parent_lost(4242, 4242));
        // The agent died ⇒ reparented to launchd (1) ⇒ orphaned, quit.
        assert!(parent_lost(4242, 1));
        // …or, under a systemd subreaper, to some other pid ⇒ still orphaned.
        assert!(parent_lost(4242, 990));
    }

    #[test]
    fn orphan_watch_skips_an_already_detached_daemon() {
        // Spawned by a normal agent ⇒ worth watching.
        assert!(orphan_watch_warranted(4242));
        // Launched detached straight from init/launchd ⇒ no parent to lose.
        assert!(!orphan_watch_warranted(1));
    }

    #[test]
    fn is_resume_only_past_threshold() {
        let threshold = Duration::from_mins(1);
        // A normal heal cadence (≤ ~15s) is never a resume.
        assert!(!is_resume(Duration::from_secs(0), threshold));
        assert!(!is_resume(Duration::from_secs(15), threshold));
        assert!(!is_resume(Duration::from_secs(59), threshold));
        // Exactly at the threshold is not yet a stall (strictly `>`).
        assert!(!is_resume(Duration::from_mins(1), threshold));
        // A multi-minute gap = the process was frozen → hard re-bootstrap.
        assert!(is_resume(Duration::from_secs(61), threshold));
        assert!(is_resume(Duration::from_hours(1), threshold));
    }

    #[test]
    fn is_resume_respects_injected_threshold() {
        // The subprocess stall regression shortens the threshold via
        // the env knob; the comparison must track whatever is passed.
        let short = Duration::from_secs(4);
        assert!(!is_resume(Duration::from_secs(3), short));
        assert!(is_resume(Duration::from_secs(5), short));
    }

    #[test]
    fn wall_resume_detects_macos_sleep_signature() {
        let threshold = Duration::from_mins(1);
        // macOS sleep: the monotonic clock froze (a few seconds of
        // real post-wake time) while the wall clock jumped a full day.
        // The monotonic gap alone misses it; the divergence catches it.
        let mono_gap = Duration::from_secs(3);
        let wall_gap = Duration::from_hours(24);
        assert!(!is_resume(mono_gap, threshold));
        assert!(is_wall_resume(wall_gap, mono_gap, threshold));
    }

    #[test]
    fn wall_resume_ignores_clocks_advancing_together() {
        let threshold = Duration::from_mins(1);
        // Steady operation: wall and monotonic advance in lockstep, so
        // their divergence is ~0 — never a resume, whatever the cadence.
        assert!(!is_wall_resume(
            Duration::from_secs(15),
            Duration::from_secs(15),
            threshold
        ));
        // A wall clock running slightly behind monotonic (NTP step
        // back) saturates to 0 divergence, not a spurious resume.
        assert!(!is_wall_resume(
            Duration::from_secs(10),
            Duration::from_secs(15),
            threshold
        ));
    }
}
