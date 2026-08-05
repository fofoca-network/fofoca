use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use iroh::EndpointId;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Bound on queued outbound datagrams per session. Overflow drops the
/// datagram (QUIC above retransmits); blocking would stall iroh's shared
/// send loop across all transports.
pub(crate) const OUT_QUEUE: usize = 256;

/// Bound on inbound datagrams fanned in from all sessions to `poll_recv`.
pub(crate) const IN_QUEUE: usize = 512;

pub(crate) struct InboundPacket {
    pub(crate) from: EndpointId,
    pub(crate) payload: Bytes,
}

/// Rate-limited visibility for the lossy queues: without it a congested
/// channel is indistinguishable from a broken one. `total` is the counter
/// value *after* the increment; logs on the first drop and every 256th.
pub(crate) fn note_dropped(remote: &EndpointId, total: u64, context: &str) {
    if total == 1 || total.is_multiple_of(256) {
        tracing::warn!(%remote, total, "webrtc lane dropping datagrams ({context})");
    }
}

/// Aborts the session driver task when the handle leaves the registry, so
/// a `detach` (or dropping the whole transport) reliably stops the str0m
/// loop instead of detaching it.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) struct SessionHandle {
    pub(crate) out_tx: mpsc::Sender<Bytes>,
    pub(crate) dropped_tx: Arc<AtomicU64>,
    generation: u64,
    _task: AbortOnDrop,
}

#[derive(Default)]
pub(crate) struct SessionRegistry {
    sessions: Mutex<HashMap<EndpointId, SessionHandle>>,
    next_generation: AtomicU64,
}

impl SessionRegistry {
    /// Reserves a generation for a new session. Generations disambiguate a
    /// driver's self-removal from a newer session attached under the same
    /// remote id after a detach/re-attach cycle.
    pub(crate) fn next_generation(&self) -> u64 {
        self.next_generation.fetch_add(1, Ordering::Relaxed)
    }

    /// Inserts a session; fails if the remote already has a live one.
    pub(crate) fn insert(
        &self,
        remote: EndpointId,
        out_tx: mpsc::Sender<Bytes>,
        dropped_tx: Arc<AtomicU64>,
        generation: u64,
        task: JoinHandle<()>,
    ) -> anyhow::Result<()> {
        // Wrap before the exists-check so bailing aborts the freshly
        // spawned driver instead of detaching it.
        let task = AbortOnDrop(task);
        let mut sessions = self.sessions.lock().expect("session registry poisoned");
        if sessions.contains_key(&remote) {
            anyhow::bail!("a live WebRTC session for {remote} already exists");
        }
        sessions.insert(
            remote,
            SessionHandle {
                out_tx,
                dropped_tx,
                generation,
                _task: task,
            },
        );
        Ok(())
    }

    /// Driver self-cleanup: removes the session only if it is still the
    /// same generation (a re-attach may have replaced it).
    pub(crate) fn remove_if_generation(&self, remote: &EndpointId, generation: u64) {
        let mut sessions = self.sessions.lock().expect("session registry poisoned");
        if sessions
            .get(remote)
            .is_some_and(|handle| handle.generation == generation)
        {
            sessions.remove(remote);
        }
    }

    pub(crate) fn remove(&self, remote: &EndpointId) -> bool {
        self.sessions
            .lock()
            .expect("session registry poisoned")
            .remove(remote)
            .is_some()
    }

    pub(crate) fn contains(&self, remote: &EndpointId) -> bool {
        self.sessions
            .lock()
            .expect("session registry poisoned")
            .contains_key(remote)
    }

    pub(crate) fn len(&self) -> usize {
        self.sessions
            .lock()
            .expect("session registry poisoned")
            .len()
    }

    pub(crate) fn outbound_for(
        &self,
        remote: &EndpointId,
    ) -> Option<(mpsc::Sender<Bytes>, Arc<AtomicU64>)> {
        self.sessions
            .lock()
            .expect("session registry poisoned")
            .get(remote)
            .map(|handle| (handle.out_tx.clone(), handle.dropped_tx.clone()))
    }
}
