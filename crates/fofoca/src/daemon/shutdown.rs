//! Leaving the mesh: the graceful shutdown path and the signals that trigger it.
//!
//! Split out of `event_loop` because none of it is the loop — it is what runs
//! once, on the way out, and it was the largest block of the file that never
//! touched a tick.

use std::time::Duration;

use tokio::sync::mpsc;

use crate::{gossip, lifecycle};

use super::app::NodeDriver;
use super::ctx::HandlerCtx;
use super::state::EventLoopState;
use crate::gossip::event::NodeEvent;
use crate::protocol::{MeshName, Message};
#[cfg(all(unix, feature = "host"))]
use crate::util::tuning::ppid_watch_interval_ms;

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
pub(super) async fn shutdown<A: NodeDriver>(
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
pub(super) struct QuitParams<'a> {
    pub(super) name: &'a MeshName,
    pub(super) leave_label: &'a str,
    pub(super) exit_on_quit: bool,
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
pub(super) async fn announce_and_maybe_exit<A: NodeDriver>(
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
pub(super) fn spawn_quit_signal_tasks(exit_on_quit: bool) -> mpsc::Receiver<()> {
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
#[cfg(all(unix, feature = "host"))]
pub(super) fn orphan_watch_warranted(original_ppid: i32) -> bool {
    original_ppid > 1
}
/// The orphaning test: the parent pid changed from the one captured at startup.
/// Comparing against the *original* (not against `1`) is what makes this correct
/// on both platforms — macOS reparents an orphan to launchd (1), but under
/// systemd Linux reparents to a subreaper at some other pid. Pid reuse can't
/// fool it: the reaper's pid won't coincidentally equal the original parent's.
#[cfg(all(unix, feature = "host"))]
pub(super) fn parent_lost(original_ppid: i32, current_ppid: i32) -> bool {
    original_ppid != current_ppid
}
/// A quit channel whose sender is deliberately leaked, so the receiver parks
/// forever. The loop's quit arm then only ever fires from `external_quit_rx`.
pub(super) fn never_quit() -> mpsc::Receiver<()> {
    let (quit_tx, quit_rx) = mpsc::channel::<()>(1);
    std::mem::forget(quit_tx);
    quit_rx
}

#[cfg(all(unix, feature = "host"))]
#[cfg(test)]
mod tests {
    use super::{orphan_watch_warranted, parent_lost};

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
}
