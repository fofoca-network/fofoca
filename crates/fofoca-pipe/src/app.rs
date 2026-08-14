//! The engine driver: what a `pipe_*` frame means on the way in, and what a
//! consumer's request means on the way out.

use anyhow::Result;
use fofoca::async_trait;
use fofoca::embed::{AppClass, EventLoopState, HandlerCtx, InboundApp, NodeApp, NodeDriver};
use fofoca::ops::{StateMergeParams, broadcast_state_merge, send_app};
use fofoca::protocol::{
    AppFrameParams, AppTag, Channel, Message, MessageBody, MessageKind, Nickname,
};
use tokio::sync::{mpsc, oneshot};

use crate::wire::{self, tag};

/// One surfaced inbound frame, flattened for a consumer that speaks bytes.
#[derive(Debug)]
pub struct Inbound {
    /// The frame's author nickname.
    pub nick: String,
    /// The frame was addressed to us specifically, not broadcast.
    pub directed: bool,
    /// A `pipe_eof` marker; `bytes` is then empty.
    pub eof: bool,
    pub bytes: Vec<u8>,
}

/// A request pushed into the event loop from the consumer's side. Every arm
/// carries its own reply channel: a foreign caller needs the outcome of a send
/// as a return code, not a log line.
#[expect(
    missing_debug_implementations,
    reason = "the oneshot reply senders carry no Debug bound worth adding for a value that never reaches a log line"
)]
pub enum Request {
    Send {
        tag: AppTag,
        to: Option<Nickname>,
        body: MessageBody,
        reply: oneshot::Sender<Result<(), String>>,
    },
    StateMerge {
        merge: serde_json::Value,
        reply: oneshot::Sender<Result<(), String>>,
    },
    StateJson {
        reply: oneshot::Sender<String>,
    },
    Peers {
        reply: oneshot::Sender<String>,
    },
    PeerCount {
        reply: oneshot::Sender<usize>,
    },
}

/// The engine seam. Every inbound `pipe_*` frame is queued for the consumer
/// instead of being written anywhere — this crate owns no stdio and holds no
/// callback: a browser's `js_sys::Function` is neither `Send` nor `Sync`, and
/// [`NodeApp`] is `Send`, so the driver could not keep one even if it wanted to.
/// Both consumers drain the queue on their own schedule.
#[derive(Debug)]
pub struct PipeApp {
    inbound: mpsc::Sender<Inbound>,
}

impl PipeApp {
    /// Frames land on `inbound`. See [`wire::INBOUND_CAP`] for the drop-on-full
    /// policy.
    #[must_use]
    pub fn new(inbound: mpsc::Sender<Inbound>) -> Self {
        Self { inbound }
    }
}

#[async_trait]
impl NodeApp for PipeApp {
    fn classify(&self, _message: &Message) -> AppClass {
        // Ephemeral stream bytes: never logged, never
        // a task beat, always valid (an opaque base64 body), no per-author hash
        // chain. `sealed: false` is load-bearing — this consumer publishes no
        // a2a card, so it can neither seal to a peer nor be sealed to, and the
        // addressee must pass a directed plaintext body straight through
        // instead of trying (and failing) to unseal it.
        AppClass {
            loggable: false,
            beat: false,
            valid: true,
            chained: false,
            sealed: false,
        }
    }

    async fn on_app_frame(
        &mut self,
        frame: InboundApp<'_>,
        _state: &mut EventLoopState,
        _ctx: &HandlerCtx<'_>,
    ) -> bool {
        let InboundApp {
            message,
            surfaceable: _,
        } = frame;
        // The engine only dispatches frames addressed to us or broadcast, so a
        // present `to` is us.
        let directed = matches!(message.kind, MessageKind::App { to: Some(_), .. });
        let queued = match message.kind.app_tag().map(AppTag::as_str) {
            Some(tag::DATA) => {
                if let Some(bytes) = wire::decode_data(&message.body) {
                    Some(Inbound {
                        nick: message.author.to_string(),
                        directed,
                        eof: false,
                        bytes,
                    })
                } else {
                    tracing::warn!(
                        target: "fofoca::messages",
                        "dropping undecodable pipe_data"
                    );
                    None
                }
            }
            Some(tag::EOF) => Some(Inbound {
                nick: message.author.to_string(),
                directed,
                eof: true,
                bytes: Vec::new(),
            }),
            Some(_) | None => None,
        };
        if let Some(inbound) = queued
            && self.inbound.try_send(inbound).is_err()
        {
            // Full or closed: the caller is not draining. Dropping is the only
            // option that neither blocks the event loop nor grows without bound.
            tracing::warn!(
                target: "fofoca::messages",
                "inbound queue full; dropped a pipe frame"
            );
        }
        // Never retained or indexed — the frame is fully handled here.
        false
    }
}

#[async_trait]
impl NodeDriver for PipeApp {
    type Session = Request;
    type Http = ();
    type Ipc = ();

    async fn handle_session(
        &mut self,
        req: Request,
        state: &mut EventLoopState,
        ctx: &HandlerCtx<'_>,
    ) -> bool {
        match req {
            Request::Send {
                tag,
                to,
                body,
                reply,
            } => {
                let sent = send_app(
                    state,
                    ctx,
                    AppFrameParams {
                        tag,
                        to,
                        corr: None,
                        body,
                    },
                )
                .await;
                let _ = reply.send(sent.map_err(|error| error.to_string()));
                true
            }
            Request::StateMerge { merge, reply } => {
                let merged = broadcast_state_merge(
                    state,
                    StateMergeParams {
                        mesh: ctx.mesh,
                        author: ctx.author,
                        merge,
                        sender: ctx.sender,
                        sink: ctx.sink,
                        channel: Channel::State,
                        surface: true,
                    },
                )
                .await;
                let _ = reply.send(merged.map(|_| ()).map_err(|error| error.to_string()));
                true
            }
            Request::StateJson { reply } => {
                let _ = reply.send(state.doc(Channel::State).to_json().to_string());
                false
            }
            Request::Peers { reply } => {
                let snapshot = state.roster_snapshot();
                let json = serde_json::to_string(&snapshot)
                    .unwrap_or_else(|error| format!("{{\"error\":\"{error}\"}}"));
                let _ = reply.send(json);
                false
            }
            Request::PeerCount { reply } => {
                // `peers.len()`, not the snapshot's `count` — that one includes
                // self, and a caller asking "is anyone else here?" wants zero
                // when it is alone.
                let _ = reply.send(state.roster_snapshot().peers.len());
                false
            }
        }
    }
}
