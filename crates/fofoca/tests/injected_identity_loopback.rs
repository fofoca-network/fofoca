//! A node handed its signing key speaks under it, and a restart under that key
//! is the same peer: its chain never reuses a seq (so no false fork), its own
//! re-served frames are kept without being shown, and with a resume point it is
//! shown what was said while it was down. Two conflicting messages at one seq
//! still fork.

#![cfg(feature = "host")]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fofoca::embed::{
    AppClass, EventLoopState, HandlerCtx, InboundApp, NodeApp, NodeDriver, NodeEvent, NodeSink,
};
use fofoca::net::TransportOpts;
use fofoca::ops::{StateMergeParams, broadcast_state_merge};
use fofoca::protocol::{
    AppFrameParams, AppTag, Channel, Identity, LookupOpts, Message, MessageBody, MessageKind,
    Nickname, PresenceSubtype, encode_pubkey,
};
use fofoca::runtime::{
    NicknameSource, Node, SetupKind, SetupParams, derive_topic_mesh_with, setup_mesh,
};
use tokio::sync::{Notify, oneshot};

const BUDGET: Duration = Duration::from_secs(45);

/// One chat frame to send. `seq` forces a position on the chain (and leaves the
/// chain where it was), the way a faulty author would; `None` takes the next
/// one, the way an app does.
struct Say {
    body: &'static str,
    seq: Option<u64>,
    done: oneshot::Sender<Message>,
}

/// What a test asks of a node's loop.
enum Request {
    Say(Say),
    /// Merge into the `meta` doc and gossip the change.
    Merge(serde_json::Value, oneshot::Sender<()>),
    /// Read the `meta` doc as JSON.
    ReadMeta(oneshot::Sender<serde_json::Value>),
    /// Whether the message log holds this frame.
    Holds(Box<Message>, oneshot::Sender<bool>),
}

/// What a node's sink and app saw, with one `Notify` for any change.
#[derive(Default)]
struct Seen {
    joined: Mutex<Vec<Nickname>>,
    lefts: Mutex<Vec<Nickname>>,
    ready_at: Mutex<Option<Instant>>,
    first_joined_at: Mutex<Option<Instant>>,
    conflicts: Mutex<Vec<(String, String, bool)>>,
    chat: Mutex<Vec<(String, String)>>,
    forks: Mutex<Vec<(String, u64)>>,
    changed: Notify,
}

impl NodeSink for Seen {
    fn emit(&self, event: NodeEvent) {
        match &event {
            NodeEvent::Presence { msg } => match msg.kind {
                MessageKind::Presence {
                    subtype: PresenceSubtype::Joined,
                } => {
                    self.joined
                        .lock()
                        .expect("no poison")
                        .push(msg.author.clone());
                    self.first_joined_at
                        .lock()
                        .expect("no poison")
                        .get_or_insert_with(Instant::now);
                }
                MessageKind::Presence {
                    subtype: PresenceSubtype::Left,
                } => self
                    .lefts
                    .lock()
                    .expect("no poison")
                    .push(msg.author.clone()),
                MessageKind::Presence { .. }
                | MessageKind::App { .. }
                | MessageKind::PeerInfo
                | MessageKind::Digest
                | MessageKind::Ping
                | MessageKind::Pong { .. }
                | MessageKind::State
                | MessageKind::StateDigest
                | MessageKind::Meta
                | MessageKind::MetaDigest
                | MessageKind::LinkState => return,
            },
            NodeEvent::Fork { pubkey, seq, .. } => {
                self.forks
                    .lock()
                    .expect("no poison")
                    .push((pubkey.clone(), *seq));
            }
            NodeEvent::Ready { .. } => {
                self.ready_at
                    .lock()
                    .expect("no poison")
                    .get_or_insert_with(Instant::now);
            }
            NodeEvent::NicknameConflict {
                ours, theirs, lost, ..
            } => self.conflicts.lock().expect("no poison").push((
                ours.clone(),
                theirs.clone(),
                *lost,
            )),
            NodeEvent::Info(_)
            | NodeEvent::Error(_)
            | NodeEvent::PeerTimeout { .. }
            | NodeEvent::PeerReturn { .. }
            | NodeEvent::PingReport { .. }
            | NodeEvent::StateChanged { .. } => return,
        }
        self.changed.notify_waiters();
    }
}

impl Seen {
    async fn until(&self, deadline: Duration, ready: impl Fn(&Seen) -> bool) -> bool {
        let wait = async {
            loop {
                let changed = self.changed.notified();
                if ready(self) {
                    return;
                }
                changed.await;
            }
        };
        tokio::time::timeout(deadline, wait).await.is_ok() || ready(self)
    }

    fn joined_count(&self, nick: &Nickname) -> usize {
        self.joined
            .lock()
            .expect("no poison")
            .iter()
            .filter(|joined| *joined == nick)
            .count()
    }

    fn bodies(&self) -> Vec<String> {
        self.chat
            .lock()
            .expect("no poison")
            .iter()
            .map(|(_, body)| body.clone())
            .collect()
    }

    fn forks(&self) -> Vec<(String, u64)> {
        self.forks.lock().expect("no poison").clone()
    }
}

struct Chat {
    seen: Arc<Seen>,
    /// A value to put in the `meta` doc's `card` when the engine says our own
    /// earlier changes just came back, as an app re-asserts its card.
    reassert: Option<&'static str>,
}

#[fofoca::async_trait]
impl NodeApp for Chat {
    fn classify(&self, _message: &Message) -> AppClass {
        AppClass {
            loggable: true,
            beat: false,
            valid: true,
            chained: true,
            sealed: false,
        }
    }

    async fn on_app_frame(
        &mut self,
        frame: InboundApp<'_>,
        _state: &mut EventLoopState,
        _ctx: &HandlerCtx<'_>,
    ) -> bool {
        if frame.surfaceable {
            self.seen.chat.lock().expect("no poison").push((
                frame.message.pubkey.clone(),
                frame.message.body.as_str().to_owned(),
            ));
            self.seen.changed.notify_waiters();
        }
        true
    }

    async fn on_own_channel_restored(
        &mut self,
        channel: Channel,
        state: &mut EventLoopState,
        ctx: &HandlerCtx<'_>,
    ) {
        let Some(card) = self.reassert else { return };
        if channel != Channel::Meta {
            return;
        }
        broadcast_state_merge(
            state,
            StateMergeParams {
                mesh: ctx.mesh,
                author: ctx.author,
                merge: serde_json::json!({ "card": card }),
                sender: ctx.sender,
                sink: ctx.sink,
                channel: Channel::Meta,
                surface: false,
            },
        )
        .await
        .expect("the card is rebuilt, signed and sent");
    }
}

#[fofoca::async_trait]
impl NodeDriver for Chat {
    type Session = Request;
    type Http = ();
    type Ipc = serde_json::Value;

    async fn handle_session(
        &mut self,
        request: Request,
        state: &mut EventLoopState,
        ctx: &HandlerCtx<'_>,
    ) -> bool {
        let say = match request {
            Request::Say(say) => say,
            Request::Merge(merge, done) => {
                broadcast_state_merge(
                    state,
                    StateMergeParams {
                        mesh: ctx.mesh,
                        author: ctx.author,
                        merge,
                        sender: ctx.sender,
                        sink: ctx.sink,
                        channel: Channel::Meta,
                        surface: false,
                    },
                )
                .await
                .expect("the merge is built, signed and sent");
                let _ = done.send(());
                return false;
            }
            Request::ReadMeta(reply) => {
                let _ = reply.send(state.doc(Channel::Meta).to_json());
                return false;
            }
            Request::Holds(frame, reply) => {
                let _ = reply.send(state.message_log_mut().holds(&frame));
                return false;
            }
        };
        let (head_seq, head_prev) = state.chain_head();
        let (seq, prev) = match say.seq {
            Some(seq) => (seq, None),
            None => (head_seq, head_prev.map(str::to_owned)),
        };
        let frame = Message::new_app(
            ctx.mesh,
            ctx.author,
            AppFrameParams {
                tag: AppTag::from("chat"),
                to: None,
                corr: None,
                body: MessageBody::new(format!("\"{}\"", say.body)).expect("a JSON body"),
            },
        )
        .with_chain(seq, prev)
        .signed(ctx.identity);
        let bytes = bytes::Bytes::from(frame.serialize().expect("serialize"));
        fofoca::ops::deliver(&frame, bytes, state, ctx.sender)
            .await
            .expect("a meshed node broadcasts");
        // The author keeps its own frame, as a chat app does, so a peer that
        // missed it can be answered by whoever else holds it.
        let _ = state.message_log_mut().push(frame.clone());
        if say.seq.is_none() {
            state.advance_chain(frame.content_hash_hex());
        }
        let _ = say.done.send(frame);
        false
    }
}

struct Run {
    identity: Option<Identity>,
    resume_from: Option<i64>,
    nickname_source: NicknameSource,
    reassert: Option<&'static str>,
}

impl Run {
    /// A fresh key and a nickname the caller chose: the joiner that must be told
    /// the nickname is free before it reports ready.
    fn chosen() -> Self {
        Run {
            nickname_source: NicknameSource::Chosen,
            ..Run::minted()
        }
    }

    fn minted() -> Self {
        Run {
            identity: None,
            resume_from: None,
            nickname_source: NicknameSource::Minted,
            reassert: None,
        }
    }

    fn with_saved_key() -> Self {
        Run {
            identity: Some(saved_identity()),
            resume_from: None,
            nickname_source: NicknameSource::Minted,
            reassert: None,
        }
    }
}

async fn spawn(topic: &str, nick: &str, seen: &Arc<Seen>, run: Run) -> Node<Chat> {
    let mesh =
        derive_topic_mesh_with(topic, LookupOpts::loopback()).expect("derive a loopback mesh");
    let config = setup_mesh(
        SetupKind::Topic {
            mesh,
            topic_string: topic.to_owned(),
        },
        SetupParams {
            author: Nickname::new(nick).expect("valid nickname"),
            max_peers: 16,
            endpoint: None,
            protocols: Vec::new(),
            transports: TransportOpts::default(),
            runtime_base: None,
            state_file: None,
            sink: Arc::clone(seen) as Arc<dyn NodeSink>,
            multihop: false,
            per_peer_gate: None,
            cohost: None,
            live_count: None,
            identity: run.identity,
            resume_from: run.resume_from,
            nickname_source: run.nickname_source,
        },
    )
    .await
    .expect("setup_mesh on a loopback mesh must not touch the network");
    Node::spawn(
        config,
        Chat {
            seen: Arc::clone(seen),
            reassert: run.reassert,
        },
        None,
        false,
    )
}

async fn say(node: &Node<Chat>, body: &'static str, seq: Option<u64>) -> Message {
    let (done, sent) = oneshot::channel();
    node.send(Request::Say(Say { body, seq, done }))
        .await
        .expect("the request reaches the loop");
    sent.await.expect("the frame was sent")
}

async fn holds(node: &Node<Chat>, frame: &Message) -> bool {
    let (reply, held) = oneshot::channel();
    node.send(Request::Holds(Box::new(frame.clone()), reply))
        .await
        .expect("the request reaches the loop");
    held.await.expect("the log was read")
}

async fn merge_meta(node: &Node<Chat>, merge: serde_json::Value) {
    let (done, merged) = oneshot::channel();
    node.send(Request::Merge(merge, done))
        .await
        .expect("the request reaches the loop");
    merged.await.expect("the change was sent");
}

async fn meta_of(node: &Node<Chat>) -> serde_json::Value {
    let (reply, doc) = oneshot::channel();
    node.send(Request::ReadMeta(reply))
        .await
        .expect("the request reaches the loop");
    doc.await.expect("the doc was read")
}

/// Poll `node`'s `meta` doc until it equals `want`, or the budget runs out.
async fn meta_becomes(node: &Node<Chat>, want: &serde_json::Value) -> bool {
    let deadline = tokio::time::Instant::now() + BUDGET;
    loop {
        if meta_of(node).await == *want {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The key a test hands to both incarnations of one peer.
fn saved_identity() -> Identity {
    Identity::from_secret_bytes([7; 32])
}

fn unix_secs() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("after 1970")
            .as_secs(),
    )
    .expect("fits")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_under_the_saved_key_is_the_same_peer_not_a_fork() {
    let topic = format!("same-key-restart-{}", rand::random::<u64>());
    let alice_nick = Nickname::new("alice").expect("valid");
    let key = encode_pubkey(&saved_identity().public());
    let bob_seen = Arc::new(Seen::default());
    let alice_seen = Arc::new(Seen::default());
    let bob = spawn(&topic, "bob", &bob_seen, Run::minted()).await;
    let alice = spawn(&topic, "alice", &alice_seen, Run::with_saved_key()).await;

    assert!(
        bob_seen
            .until(BUDGET, |seen| seen.joined_count(&alice_nick) >= 1)
            .await,
        "bob never saw alice join"
    );
    say(&alice, "before-the-crash", None).await;
    assert!(
        bob_seen
            .until(BUDGET, |seen| seen.bodies().len() == 1)
            .await,
        "bob never got the first message"
    );
    alice.leave().await.expect("alice's first run ends");

    // No pause, no wait for the new `joined` to land first: nothing about the
    // verdict depends on the order or on a announcement arriving at all.
    let alice_again = spawn(&topic, "alice", &alice_seen, Run::with_saved_key()).await;
    assert!(
        bob_seen
            .until(BUDGET, |seen| seen.joined_count(&alice_nick) >= 2)
            .await,
        "bob never saw the restarted alice join"
    );
    say(&alice_again, "after-the-restart", None).await;
    assert!(
        bob_seen
            .until(BUDGET, |seen| seen.bodies().len() == 2)
            .await,
        "bob never got the restarted peer's message"
    );
    // The fork check runs after the frame is handed to the app.
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(
        bob_seen
            .chat
            .lock()
            .expect("no poison")
            .iter()
            .all(|(pubkey, _)| *pubkey == key),
        "both runs must sign with the key they were handed"
    );
    assert!(
        bob_seen.forks().is_empty(),
        "a restart was reported as a fork: {:?}",
        bob_seen.forks()
    );
    assert!(
        alice_seen.bodies().is_empty(),
        "the restarted peer was shown its own earlier message: {:?}",
        alice_seen.bodies()
    );

    alice_again.leave().await.expect("alice leaves");
    bob.leave().await.expect("bob leaves");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn what_was_said_while_the_peer_was_down_is_shown_when_it_resumes() {
    let topic = format!("same-key-resume-{}", rand::random::<u64>());
    let alice_nick = Nickname::new("alice").expect("valid");
    let bob_seen = Arc::new(Seen::default());
    let alice_seen = Arc::new(Seen::default());
    let bob = spawn(&topic, "bob", &bob_seen, Run::minted()).await;
    let alice = spawn(&topic, "alice", &alice_seen, Run::with_saved_key()).await;
    assert!(
        bob_seen
            .until(BUDGET, |seen| seen.joined_count(&alice_nick) >= 1)
            .await,
        "bob never saw alice join"
    );

    let went_down = unix_secs();
    alice.leave().await.expect("alice's first run ends");
    say(&bob, "said-during-the-outage", None).await;
    // Whole-second stamps: the frame must predate the restart's own start, or
    // the plain join horizon would show it and the test would prove nothing.
    tokio::time::sleep(Duration::from_millis(1100)).await;

    let alice_again = spawn(
        &topic,
        "alice",
        &alice_seen,
        Run {
            identity: Some(saved_identity()),
            resume_from: Some(went_down),
            nickname_source: NicknameSource::Minted,
            reassert: None,
        },
    )
    .await;
    assert!(
        alice_seen
            .until(BUDGET, |seen| {
                seen.bodies() == ["\"said-during-the-outage\"".to_owned()]
            })
            .await,
        "the restarted peer was not shown what was said while it was down: {:?}",
        alice_seen.bodies()
    );

    alice_again.leave().await.expect("alice leaves");
    bob.leave().await.expect("bob leaves");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_messages_at_one_seq_are_still_a_fork() {
    let topic = format!("same-key-equivocation-{}", rand::random::<u64>());
    let alice_nick = Nickname::new("alice").expect("valid");
    let key = encode_pubkey(&saved_identity().public());
    let bob_seen = Arc::new(Seen::default());
    let alice_seen = Arc::new(Seen::default());
    let bob = spawn(&topic, "bob", &bob_seen, Run::minted()).await;
    let alice = spawn(&topic, "alice", &alice_seen, Run::with_saved_key()).await;

    assert!(
        bob_seen
            .until(BUDGET, |seen| seen.joined_count(&alice_nick) >= 1)
            .await,
        "bob never saw alice join"
    );
    say(&alice, "one-story", Some(3)).await;
    say(&alice, "another-story", Some(3)).await;
    assert!(
        bob_seen
            .until(BUDGET, |seen| !seen.forks().is_empty())
            .await,
        "equivocation must still fork"
    );
    assert_eq!(
        bob_seen.forks(),
        [(key, 3)],
        "the fork names the injected key and the reused seq"
    );

    alice.leave().await.expect("alice leaves");
    bob.leave().await.expect("bob leaves");
}

/// The restarted peer's document starts empty under a new actor. A peer's later
/// change builds on the peer's earlier one, so the restart has to be handed its
/// own earlier entry back, or that change waits for its parent for good.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_peer_relearns_its_own_meta_and_converges_with_a_dependent_write() {
    let topic = format!("same-key-meta-{}", rand::random::<u64>());
    let alice_nick = Nickname::new("alice").expect("valid");
    let bob_seen = Arc::new(Seen::default());
    let alice_seen = Arc::new(Seen::default());
    let bob = spawn(&topic, "bob", &bob_seen, Run::minted()).await;
    let alice = spawn(&topic, "alice", &alice_seen, Run::with_saved_key()).await;
    assert!(
        bob_seen
            .until(BUDGET, |seen| seen.joined_count(&alice_nick) >= 1)
            .await,
        "bob never saw alice join"
    );

    merge_meta(&alice, serde_json::json!({"before_the_crash": 1})).await;
    assert!(
        meta_becomes(&bob, &serde_json::json!({"before_the_crash": 1})).await,
        "bob never got alice's entry"
    );
    alice.leave().await.expect("alice's first run ends");

    // Written on top of alice's entry while she is down.
    merge_meta(&bob, serde_json::json!({"while_she_was_down": 2})).await;

    let alice_again = spawn(&topic, "alice", &alice_seen, Run::with_saved_key()).await;
    let both = serde_json::json!({"before_the_crash": 1, "while_she_was_down": 2});
    assert!(
        meta_becomes(&alice_again, &both).await,
        "the restarted peer's doc stalled: {}",
        meta_of(&alice_again).await
    );
    assert_eq!(meta_of(&bob).await, both, "bob's doc agrees");

    // And the restarted peer can still write, under its new actor.
    merge_meta(&alice_again, serde_json::json!({"after_the_restart": 3})).await;
    let all = serde_json::json!({
        "before_the_crash": 1,
        "while_she_was_down": 2,
        "after_the_restart": 3,
    });
    assert!(
        meta_becomes(&bob, &all).await,
        "bob never got the restarted peer's new write: {}",
        meta_of(&bob).await
    );

    alice_again.leave().await.expect("alice leaves");
    bob.leave().await.expect("bob leaves");
}

/// An app keeps its own card in the `meta` doc. The restarted run's doc is
/// handed the card it wrote before the crash, and the engine tells the app so
/// it can write this run's over it: the later write wins on every replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_app_re_asserts_its_card_over_the_one_handed_back() {
    let topic = format!("same-key-card-{}", rand::random::<u64>());
    let alice_nick = Nickname::new("alice").expect("valid");
    let bob_seen = Arc::new(Seen::default());
    let alice_seen = Arc::new(Seen::default());
    let bob = spawn(&topic, "bob", &bob_seen, Run::minted()).await;
    let alice = spawn(&topic, "alice", &alice_seen, Run::with_saved_key()).await;
    assert!(
        bob_seen
            .until(BUDGET, |seen| seen.joined_count(&alice_nick) >= 1)
            .await,
        "bob never saw alice join"
    );
    merge_meta(&alice, serde_json::json!({"card": "first run"})).await;
    assert!(
        meta_becomes(&bob, &serde_json::json!({"card": "first run"})).await,
        "bob never got the first card"
    );
    drop(alice);

    let alice_again = spawn(
        &topic,
        "alice",
        &alice_seen,
        Run {
            reassert: Some("second run"),
            ..Run::with_saved_key()
        },
    )
    .await;
    let want = serde_json::json!({"card": "second run"});
    assert!(
        meta_becomes(&alice_again, &want).await,
        "the restarted peer never settled on its new card: {}",
        meta_of(&alice_again).await
    );
    assert!(
        meta_becomes(&bob, &want).await,
        "bob still reads the old card: {}",
        meta_of(&bob).await
    );

    alice_again.leave().await.expect("alice leaves");
    bob.leave().await.expect("bob leaves");
}

/// No `Left`, no leave: the loop is aborted mid-run, as a crash does. The
/// restarted peer is handed back both what it said and what it wrote to the
/// doc, end to end through the engine's own-frame gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_a_crash_with_no_goodbye_the_restart_holds_its_old_frames() {
    let topic = format!("same-key-crash-{}", rand::random::<u64>());
    let alice_nick = Nickname::new("alice").expect("valid");
    let bob_seen = Arc::new(Seen::default());
    let alice_seen = Arc::new(Seen::default());
    let bob = spawn(&topic, "bob", &bob_seen, Run::minted()).await;
    let alice = spawn(&topic, "alice", &alice_seen, Run::with_saved_key()).await;
    assert!(
        bob_seen
            .until(BUDGET, |seen| seen.joined_count(&alice_nick) >= 1)
            .await,
        "bob never saw alice join"
    );
    let before_crash = say(&alice, "said-before-the-crash", None).await;
    merge_meta(&alice, serde_json::json!({"before_the_crash": 1})).await;
    assert!(
        bob_seen
            .until(BUDGET, |seen| seen.bodies().len() == 1)
            .await,
        "bob never got the chat"
    );
    assert!(
        meta_becomes(&bob, &serde_json::json!({"before_the_crash": 1})).await,
        "bob never got the doc change"
    );
    drop(alice);

    let alice_again = spawn(&topic, "alice", &alice_seen, Run::with_saved_key()).await;
    let deadline = tokio::time::Instant::now() + BUDGET;
    while !holds(&alice_again, &before_crash).await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the restart was never handed its own earlier message"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        meta_becomes(&alice_again, &serde_json::json!({"before_the_crash": 1})).await,
        "the restart's doc never relearned its own entry: {}",
        meta_of(&alice_again).await
    );
    assert!(
        alice_seen.bodies().is_empty(),
        "its own old message was shown to it: {:?}",
        alice_seen.bodies()
    );
    assert!(bob_seen.forks().is_empty(), "{:?}", bob_seen.forks());

    alice_again.leave().await.expect("alice leaves");
    bob.leave().await.expect("bob leaves");
}

fn lefts_of(seen: &Seen, nick: &Nickname) -> usize {
    seen.lefts
        .lock()
        .expect("no poison")
        .iter()
        .filter(|left| *left == nick)
        .count()
}

fn pubkeys_heard(seen: &Seen) -> std::collections::HashSet<String> {
    seen.chat
        .lock()
        .expect("no poison")
        .iter()
        .map(|(pubkey, _)| pubkey.clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_minted_nickname_is_ready_at_once_and_a_chosen_one_waits_to_be_told_it_is_free() {
    let topic = format!("nickname-ready-{}", rand::random::<u64>());
    let first_seen = Arc::new(Seen::default());
    let first = spawn(&topic, "first-peer", &first_seen, Run::minted()).await;

    let started = Instant::now();
    let minted_seen = Arc::new(Seen::default());
    let minted = spawn(&topic, "minted-one", &minted_seen, Run::minted()).await;
    assert!(
        minted_seen
            .until(Duration::from_secs(2), |seen| seen
                .ready_at
                .lock()
                .expect("no poison")
                .is_some())
            .await,
        "a nickname nobody chose reports ready as soon as it can serve"
    );

    let chosen_seen = Arc::new(Seen::default());
    let chosen_started = Instant::now();
    let chosen = spawn(&topic, "chosen-one", &chosen_seen, Run::chosen()).await;
    assert!(
        chosen_seen.ready_at.lock().expect("no poison").is_none(),
        "held at the start"
    );
    assert!(
        chosen_seen
            .until(BUDGET, |seen| seen
                .ready_at
                .lock()
                .expect("no poison")
                .is_some())
            .await,
        "ready never came"
    );
    let ready_after = chosen_seen
        .ready_at
        .lock()
        .expect("no poison")
        .expect("set")
        .duration_since(chosen_started);
    let linked_after = chosen_seen
        .first_joined_at
        .lock()
        .expect("no poison")
        .expect("it met its peers")
        .duration_since(chosen_started);
    assert!(
        ready_after >= linked_after + Duration::from_secs(2),
        "ready waited an answer window after the first link: ready at {ready_after:?}, \
         first peer seen at {linked_after:?}"
    );
    assert!(
        ready_after < Duration::from_secs(7),
        "and did not wait for the alone path: {ready_after:?}"
    );
    assert!(started.elapsed() > Duration::ZERO);

    chosen.leave().await.expect("leaves");
    minted.leave().await.expect("leaves");
    first.leave().await.expect("leaves");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chosen_nickname_with_nobody_to_ask_takes_the_alone_path() {
    let topic = format!("nickname-alone-{}", rand::random::<u64>());
    let seen = Arc::new(Seen::default());
    let started = Instant::now();
    let alone = spawn(&topic, "all-alone", &seen, Run::chosen()).await;

    assert!(
        seen.until(BUDGET, |seen| seen
            .ready_at
            .lock()
            .expect("no poison")
            .is_some())
            .await,
        "an unreachable mesh must not hold ready for good"
    );
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_secs(7) && waited < Duration::from_secs(15),
        "ready after the alone deadline, not before and not never: {waited:?}"
    );
    alone.leave().await.expect("leaves");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nickname_a_live_peer_holds_is_refused_before_ready() {
    let topic = format!("nickname-refused-{}", rand::random::<u64>());
    let holder_key = Identity::from_secret_bytes([11; 32]);
    let holder_pubkey = encode_pubkey(&holder_key.public());
    let holder_seen = Arc::new(Seen::default());
    let holder = spawn(
        &topic,
        "taken-name",
        &holder_seen,
        Run {
            identity: Some(holder_key),
            ..Run::minted()
        },
    )
    .await;

    let imposter_seen = Arc::new(Seen::default());
    let imposter = spawn(&topic, "taken-name", &imposter_seen, Run::chosen()).await;
    // Longer than the answer window, and shorter than the alone deadline.
    tokio::time::sleep(Duration::from_secs(6)).await;

    let error = imposter
        .leave()
        .await
        .expect_err("the nickname is taken, so the node ends with that");
    let taken = error
        .downcast_ref::<fofoca::runtime::NicknameTaken>()
        .unwrap_or_else(|| panic!("a typed refusal, not: {error:#}"));
    assert_eq!(taken.nickname, Nickname::new("taken-name").expect("valid"));
    assert_eq!(
        taken.holder,
        fofoca::runtime::NicknameHolder::Peer {
            pubkey: holder_pubkey
        }
    );
    assert!(
        imposter_seen.ready_at.lock().expect("no poison").is_none(),
        "it never reported ready"
    );
    assert!(
        holder_seen.conflicts.lock().expect("no poison").is_empty(),
        "a joiner that gave up inside the window is no conflict for the holder"
    );
    assert!(holder_seen.forks().is_empty());
    holder.leave().await.expect("the holder is untouched");
}

/// A partition heals and two working holders meet. Each is told; the lower key
/// keeps the nickname and the other is told it lost; nobody exits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_working_holders_that_meet_both_hear_of_it_and_the_lower_key_keeps_the_nickname() {
    let topic = format!("nickname-conflict-{}", rand::random::<u64>());
    let (first_key, second_key) = (
        Identity::from_secret_bytes([21; 32]),
        Identity::from_secret_bytes([22; 32]),
    );
    let (first_pubkey, second_pubkey) = (
        encode_pubkey(&first_key.public()),
        encode_pubkey(&second_key.public()),
    );
    let (first_seen, second_seen) = (Arc::new(Seen::default()), Arc::new(Seen::default()));
    let with_key = |key| Run {
        identity: Some(key),
        ..Run::minted()
    };
    let first = spawn(&topic, "one-name", &first_seen, with_key(first_key)).await;
    let second = spawn(&topic, "one-name", &second_seen, with_key(second_key)).await;

    // Each holds the nickname and each keeps talking past the answer window.
    tokio::time::sleep(Duration::from_secs(4)).await;
    say(&first, "still here", None).await;
    say(&second, "me too", None).await;
    assert!(
        first_seen
            .until(BUDGET, |seen| !seen
                .conflicts
                .lock()
                .expect("no poison")
                .is_empty())
            .await,
        "the first never heard of the conflict"
    );
    assert!(
        second_seen
            .until(BUDGET, |seen| !seen
                .conflicts
                .lock()
                .expect("no poison")
                .is_empty())
            .await,
        "the second never heard of the conflict"
    );
    let heard_first = first_seen.conflicts.lock().expect("no poison").clone();
    let heard_second = second_seen.conflicts.lock().expect("no poison").clone();
    assert_eq!(
        heard_first,
        [(
            first_pubkey.clone(),
            second_pubkey.clone(),
            first_pubkey > second_pubkey
        )]
    );
    assert_eq!(
        heard_second,
        [(
            second_pubkey.clone(),
            first_pubkey.clone(),
            second_pubkey > first_pubkey
        )]
    );
    assert_ne!(
        heard_first[0].2, heard_second[0].2,
        "exactly one of them lost"
    );

    first.leave().await.expect("nobody is evicted or exits");
    second.leave().await.expect("nobody is evicted or exits");
}

/// A third peer holds one key's claim on a nickname. A second key under it can
/// neither be heard nor send the holder away, though its doc changes land.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_key_under_a_held_nickname_cannot_speak_for_it_or_send_it_away() {
    let topic = format!("nickname-held-{}", rand::random::<u64>());
    let held = Nickname::new("held-name").expect("valid");
    let observer_seen = Arc::new(Seen::default());
    let observer = spawn(&topic, "observer", &observer_seen, Run::minted()).await;
    let holder_seen = Arc::new(Seen::default());
    let holder = spawn(&topic, "held-name", &holder_seen, Run::minted()).await;
    assert!(
        observer_seen
            .until(BUDGET, |seen| seen.joined_count(&held) >= 1)
            .await,
        "the observer never met the holder"
    );
    say(&holder, "from the holder", None).await;
    assert!(
        observer_seen
            .until(BUDGET, |seen| seen.bodies().len() == 1)
            .await,
        "the holder's message never arrived"
    );
    let holder_key = pubkeys_heard(&observer_seen);
    assert_eq!(holder_key.len(), 1);

    let usurper_seen = Arc::new(Seen::default());
    let usurper = spawn(&topic, "held-name", &usurper_seen, Run::minted()).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    say(&usurper, "from the usurper", None).await;
    merge_meta(&usurper, serde_json::json!({"written_by_the_usurper": 1})).await;
    assert!(
        meta_becomes(&observer, &serde_json::json!({"written_by_the_usurper": 1})).await,
        "a doc change is never dropped, whoever signed it"
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        observer_seen.bodies().len(),
        1,
        "the usurper's message must not be shown: {:?}",
        observer_seen.bodies()
    );
    assert_eq!(pubkeys_heard(&observer_seen), holder_key);

    usurper.leave().await.expect("the usurper leaves");
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        lefts_of(&observer_seen, &held),
        0,
        "the usurper's goodbye did not send the holder away"
    );
    assert_eq!(
        observer_seen.joined_count(&held),
        1,
        "and it never re-joined"
    );

    holder.leave().await.expect("leaves");
    observer.leave().await.expect("leaves");
}
