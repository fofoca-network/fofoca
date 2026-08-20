//! The highest-value test in this example: two independent bots on a
//! loopback-only mesh (no internet, no relay — `LookupOpts::public_preset()`
//! makes zero external network calls, so this runs offline and in CI)
//! play a whole match against each other and independently derive the
//! same outcome, having exchanged nothing but per-tick inputs.
//!
//! That is the central claim of the example — GGPO-style rollback over a
//! fofoca mesh, no authoritative peer, no state on the wire — checked
//! end-to-end rather than just in the simulation (`sim.rs`'s own tests
//! cover that half, and `sync_test.rs` covers determinism).

use std::time::Duration;

use fofoca::embed::SilentSink;
use fofoca::net::TransportOpts;
use fofoca::protocol::{LookupOpts, Nickname};
use fofoca::runtime::{Node, SetupKind, SetupParams, derive_topic_mesh_with, setup_mesh};
use fofoca_netplay::RollbackDriver;

use light_cycles_native::app::{Game, LightCycles};
use light_cycles_native::grid::Dir;
use light_cycles_native::sim::Outcome;

struct Bot {
    node: Node<RollbackDriver<LightCycles>>,
    game: Game,
}

async fn spawn_bot(topic: &str, nick: &str) -> Bot {
    let mesh =
        derive_topic_mesh_with(topic, LookupOpts::public_preset()).expect("derive a loopback mesh");
    let author = Nickname::new(nick).expect("valid nickname");
    let kind = SetupKind::Topic {
        mesh,
        topic_string: topic.to_string(),
    };

    let config = setup_mesh(
        kind,
        SetupParams {
            author,
            max_peers: 16,
            endpoint: None,
            protocols: Vec::new(),
            transports: TransportOpts::default(),
            runtime_base: None,
            state_file: None,
            sink: std::sync::Arc::new(SilentSink),
            multihop: false,
            per_peer_gate: None,
            cohost: None,
            live_count: None,
        },
    )
    .await
    .expect("setup_mesh on a loopback mesh must not touch the network");

    let (driver, pending) = RollbackDriver::<LightCycles>::new();
    let node: Node<RollbackDriver<LightCycles>> = Node::spawn(config, driver, None, false);
    let game = Game::new(
        pending.connect(node.sender()),
        topic.to_string(),
        nick.to_string(),
    );

    Bot { node, game }
}

/// Ticks both bots at the real frame rate until `condition` holds or
/// `timeout` elapses. Steering is scripted off the tick count so the two
/// bots drive differently and actually collide.
async fn drive(
    bots: &mut [Bot],
    timeout: Duration,
    mut condition: impl FnMut(&[Bot]) -> bool,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut tick: usize = 0;
    loop {
        for (index, bot) in bots.iter_mut().enumerate() {
            if tick.is_multiple_of(17) {
                let turns = [Dir::Up, Dir::Right, Dir::Down, Dir::Left];
                bot.game.steer(turns[(tick / 17 + index * 2) % turns.len()]);
            }
            bot.game.tick();
        }
        tick += 1;
        if condition(bots) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn public_path_probe() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let topic = format!("light-cycles-two-bots-test-{}", std::process::id());
    let mut bots = vec![
        spawn_bot(&topic, "alice").await,
        spawn_bot(&topic, "bob").await,
    ];

    // The mesh forms, the lobby agrees a match, and both bots build a
    // session for it. Nobody negotiates the roster: the lobby fixes it.
    let started = drive(&mut bots, Duration::from_secs(30), |bots| {
        bots.iter().all(|bot| bot.game.snapshot().roster.len() == 2)
    })
    .await;
    assert!(started, "the two bots never started a match together");

    let rosters: Vec<Vec<String>> = bots
        .iter()
        .map(|bot| {
            bot.game
                .snapshot()
                .roster
                .into_iter()
                .map(|(pubkey, _)| pubkey)
                .collect()
        })
        .collect();
    assert_eq!(
        rosters[0], rosters[1],
        "handles are positional, so a roster that differs by order is a \
         silent desync waiting to happen"
    );

    // They play it out. Two cycles turning across a small arena decide
    // well inside the tick cap.
    let decided = drive(&mut bots, Duration::from_secs(60), |bots| {
        bots.iter()
            .all(|bot| bot.game.outcome() != Outcome::InProgress)
    })
    .await;
    assert!(
        decided,
        "the match never reached a decided outcome on both sides: {:?}",
        bots.iter()
            .map(|bot| bot.game.snapshot().world.tick)
            .collect::<Vec<_>>()
    );

    // A match that ends before it starts would satisfy every assertion
    // above vacuously, so pin down that one was actually simulated.
    for bot in &bots {
        let snapshot = bot.game.snapshot();
        assert!(
            snapshot.world.tick > 5,
            "the match decided at tick {} — too early to have been played",
            snapshot.world.tick
        );
        assert_eq!(snapshot.match_number, 1);
    }

    // The actual claim under test.
    assert_eq!(
        bots[0].game.outcome(),
        bots[1].game.outcome(),
        "the two bots disagree about who won"
    );
    assert!(
        bots.iter().all(|bot| !bot.game.snapshot().desynced),
        "a bot reported a state checksum that disagreed with its peer's"
    );

    // And then they play another one. This is the lobby's epoch doing its
    // job: the second match's id is not ordered below the first's, so the
    // tie-break alone would have refused it and the two would have sat on
    // a finished game forever.
    let spawns = |bots: &[Bot]| -> Vec<(i16, i16)> {
        bots[0]
            .game
            .snapshot()
            .world
            .cycles
            .iter()
            .map(|cycle| (cycle.x, cycle.y))
            .collect()
    };
    let first_arena = spawns(&bots);

    let rematched = drive(&mut bots, Duration::from_secs(30), |bots| {
        bots.iter().all(|bot| bot.game.snapshot().match_number == 2)
    })
    .await;
    assert!(
        rematched,
        "no rematch was agreed: {:?}",
        bots.iter()
            .map(|bot| bot.game.snapshot().match_number)
            .collect::<Vec<_>>()
    );
    assert_ne!(
        spawns(&bots),
        first_arena,
        "a rematch reusing the previous arena means the session id was \
         reused, and with it the magic that rejects stale packets"
    );

    // Leave, rather than letting the runtime drop these mid-flight.
    // `iroh-gossip`'s actor loop `expect`s on its `JoinSet`, so a task
    // cancelled by runtime shutdown panics and fails a passing test.
    for bot in bots {
        bot.node.leave().await.expect("leave the mesh cleanly");
    }
}
