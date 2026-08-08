use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use iroh::EndpointId;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub(crate) struct InboundPacket {
    pub(crate) from: EndpointId,
    pub(crate) payload: Bytes,
}

/// Rate-limited visibility for the lossy queues. `total` is the counter value
/// *after* the increment.
pub(crate) fn note_dropped(remote: &EndpointId, total: u64, context: &str) {
    if crate::should_log_drop(total) {
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
    _task: AbortOnDrop,
}

/// One peer's slot: reserved while `value` is `None`, live once it is `Some`.
///
/// A reservation exists so a driver that dies before its session is recorded
/// has something to retire. Without it the removal lands on an empty registry
/// and is lost, and the record that follows is permanent — a session that
/// reports itself live while nothing pumps it.
///
/// Reserved is deliberately **not** live: `contains`, `len` and `outbound_for`
/// ignore a reservation, because handing QUIC a slot with no driver behind it
/// is worse than reporting none. Only claiming treats the two alike, since for
/// admission they answer the same question. This mirrors the browser's
/// [`crate::registry::Registry`], which had the reservation from the start.
struct Slot {
    generation: u64,
    value: Option<SessionHandle>,
}

#[derive(Default)]
pub(crate) struct SessionRegistry {
    sessions: Mutex<HashMap<EndpointId, Slot>>,
    next_generation: AtomicU64,
}

impl SessionRegistry {
    /// Claim `remote`'s slot before its driver is spawned, returning the
    /// generation to spawn under. `None` if anyone already holds the peer,
    /// reserved or live.
    pub(crate) fn reserve(&self, remote: EndpointId) -> Option<u64> {
        let mut sessions = self.sessions.lock().expect("session registry poisoned");
        if sessions.contains_key(&remote) {
            return None;
        }
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        sessions.insert(
            remote,
            Slot {
                generation,
                value: None,
            },
        );
        Some(generation)
    }

    /// Fill a reservation with its live session.
    ///
    /// Fails when the reservation is gone — which is exactly the case this
    /// design exists for: the driver failed and retired its own generation
    /// while the spawn was still returning. Failing drops `task` rather than
    /// recording a session nothing is driving.
    pub(crate) fn fulfil(
        &self,
        remote: EndpointId,
        out_tx: mpsc::Sender<Bytes>,
        dropped_tx: Arc<AtomicU64>,
        generation: u64,
        task: JoinHandle<()>,
    ) -> anyhow::Result<()> {
        // Wrap before the check so bailing aborts the freshly spawned driver
        // instead of detaching it.
        let task = AbortOnDrop(task);
        let mut sessions = self.sessions.lock().expect("session registry poisoned");
        let Some(slot) = sessions.get_mut(&remote) else {
            anyhow::bail!("the session for {remote} was retired before it was recorded");
        };
        if slot.generation != generation || slot.value.is_some() {
            anyhow::bail!("a newer WebRTC session for {remote} already holds the slot");
        }
        slot.value = Some(SessionHandle {
            out_tx,
            dropped_tx,
            _task: task,
        });
        Ok(())
    }

    /// Driver self-cleanup: removes the slot only if it is still the same
    /// generation (a re-attach may have replaced it). Retires a reservation as
    /// readily as a live session, so a driver that dies early leaves nothing.
    pub(crate) fn remove_if_generation(&self, remote: &EndpointId, generation: u64) {
        let mut sessions = self.sessions.lock().expect("session registry poisoned");
        if sessions
            .get(remote)
            .is_some_and(|slot| slot.generation == generation)
        {
            sessions.remove(remote);
        }
    }

    pub(crate) fn remove(&self, remote: &EndpointId) -> bool {
        self.sessions
            .lock()
            .expect("session registry poisoned")
            .remove(remote)
            .is_some_and(|slot| slot.value.is_some())
    }

    pub(crate) fn contains(&self, remote: &EndpointId) -> bool {
        self.sessions
            .lock()
            .expect("session registry poisoned")
            .get(remote)
            .is_some_and(|slot| slot.value.is_some())
    }

    pub(crate) fn len(&self) -> usize {
        self.sessions
            .lock()
            .expect("session registry poisoned")
            .values()
            .filter(|slot| slot.value.is_some())
            .count()
    }

    pub(crate) fn outbound_for(
        &self,
        remote: &EndpointId,
    ) -> Option<(mpsc::Sender<Bytes>, Arc<AtomicU64>)> {
        self.sessions
            .lock()
            .expect("session registry poisoned")
            .get(remote)
            .and_then(|slot| slot.value.as_ref())
            .map(|handle| (handle.out_tx.clone(), handle.dropped_tx.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::SessionRegistry;
    use iroh::{EndpointId, SecretKey};
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use tokio::sync::mpsc;

    fn eid(seed: u8) -> EndpointId {
        SecretKey::from_bytes(&[seed; 32]).public()
    }

    /// **A driver that dies before its slot is recorded must leave nothing.**
    ///
    /// `attach` spawns the driver and records the session afterwards, so a
    /// session that fails immediately retires its own generation against an
    /// empty registry — the removal finds nothing and is lost. The insert then
    /// lands on top, and the entry is permanent: `has_session` stays true,
    /// every send reports `NotConnected`, and re-attach is refused because a
    /// live session supposedly already exists.
    #[tokio::test]
    async fn a_driver_that_dies_before_its_insert_leaves_no_phantom() {
        let registry = SessionRegistry::default();
        let remote = eid(1);
        let generation = registry
            .reserve(remote)
            .expect("a fresh remote is claimable");

        // The driver's self-cleanup, arriving before `attach` recorded it.
        registry.remove_if_generation(&remote, generation);

        let (out_tx, _out_rx) = mpsc::channel(1);
        let task = tokio::spawn(async {});
        let _ = registry.fulfil(
            remote,
            out_tx,
            Arc::new(AtomicU64::new(0)),
            generation,
            task,
        );

        assert!(
            !registry.contains(&remote),
            "a retired generation must not be resurrected by its own late insert"
        );
    }

    /// The ordinary path must still work: reserve, insert, and the session is
    /// live until something removes it.
    #[tokio::test]
    async fn a_session_that_attaches_normally_is_live() {
        let registry = SessionRegistry::default();
        let remote = eid(2);
        let generation = registry
            .reserve(remote)
            .expect("a fresh remote is claimable");
        assert!(
            !registry.contains(&remote),
            "a reservation is not yet a usable session"
        );
        let (out_tx, _out_rx) = mpsc::channel(1);
        let task = tokio::spawn(async {});
        registry
            .fulfil(
                remote,
                out_tx,
                Arc::new(AtomicU64::new(0)),
                generation,
                task,
            )
            .expect("a fresh remote attaches");
        assert!(registry.contains(&remote));

        registry.remove_if_generation(&remote, generation);
        assert!(!registry.contains(&remote), "its own driver may retire it");
    }
}
