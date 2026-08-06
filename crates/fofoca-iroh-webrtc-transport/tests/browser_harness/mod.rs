//! Shared machinery for the two browser test targets: a negotiated
//! browser↔browser [`Pair`] and a bulk protocol to drive over it.
//!
//! Included by `browser_loopback.rs` (the fast regression suite) and
//! `browser_matrix.rs` (the slow configuration sweep) with `mod
//! browser_harness;`. It lives in a subdirectory because cargo auto-discovers
//! test targets only from `tests/*.rs`, so a shared module cannot sit beside
//! them without becoming a third, empty target.
//!
//! ## Running either target
//!
//! ```sh
//! export CC_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/clang
//! export AR_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/llvm-ar
//! CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
//! CHROMEDRIVER=/path/to/chromedriver \
//! WASM_BINDGEN_TEST_WEBDRIVER_JSON=/path/to/webdriver.json \
//!   cargo test --release --target wasm32-unknown-unknown \
//!     -p fofoca-iroh-webrtc-transport --features web --test browser_loopback
//! ```
//!
//! `cargo task e2e` does all of that for you, over several browsers, and
//! drives the Chromium ones through CDP instead of a webdriver. Four things
//! about the invocation are load-bearing, and each fails in a way that does not
//! name its cause:
//!
//! - **Homebrew clang.** `ring` compiles its C core for wasm32 and Apple clang
//!   has no wasm backend, so without this the link silently leaves 44
//!   `ring_core_*` symbols as imports from a module called `env`. The browser
//!   then refuses the module with `Failed to resolve module specifier "env"`,
//!   and the runner reports only `Failed to detect test as having been run`.
//!   `examples/chat-webrtc/build-wasm.sh` says the same thing.
//! - **`--release`.** The debug build is ~137 MB of wasm and the browser gives
//!   up loading it, with the same unhelpful message.
//! - **A `chromedriver` matching the Chrome it drives** — only on the webdriver
//!   path. A mismatch fails session creation with a bare HTTP 404. The CDP path
//!   `cargo task e2e` uses for Chromium needs no driver at all.
//! - **Stop the browser hiding local IPs behind mDNS**
//!   (`--disable-features=WebRtcHideLocalIpsWithMdns` on Chrome; every engine
//!   has its own switch). Otherwise the only host candidate is a `.local`
//!   name, and a headless
//!   browser has no mDNS responder to resolve its own — ICE never leaves `new`
//!   and the channel never opens. Real tabs are unaffected; this is a property
//!   of the harness, not of the transport.
//!
//! ## What a pass here does and does not mean
//!
//! Two `BrowserHubTransport`s and two iroh endpoints **in one tab**, JSEP'd in
//! memory exactly as `../tests/loopback.rs` does natively. One tab rather than
//! two is not a shortcut: both ends run the same `web::` code either way, and
//! the pair is what is under test, not the page they live on.
//!
//! It is, though, the *easiest* case that exists — same process, loopback
//! candidates, no NAT — so a pass does not retire a report from two separate
//! browsers on a real network. That gap is why `browser_matrix.rs` exists.

#![allow(
    dead_code,
    reason = "each test target compiles this whole module and uses a subset of it — the regression suite never asks for an upload, the matrix never calls `Pair::request`. Splitting it along the seam of who happens to call what would put the negotiation in one file and the exchange it negotiates for in another."
)]

use std::time::Duration;

use fofoca_iroh_webrtc_transport::iroh::endpoint::{Connection, presets};
use fofoca_iroh_webrtc_transport::iroh::protocol::{AcceptError, ProtocolHandler, Router};
use fofoca_iroh_webrtc_transport::iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayMode, SecretKey, TransportAddr,
};
use fofoca_iroh_webrtc_transport::{
    BrowserHubTransport, IceServers, WEBRTC_TRANSPORT_ID, WebRtcHandle, browser_answer,
    browser_offer, custom_addr,
};
use wasm_bindgen::JsCast as _;

pub(crate) const BENCH_ALPN: &[u8] = b"fofoca-webrtc/test-bench/0";

/// Bytes the plain bulk case asks for. Comfortably past one QUIC congestion
/// window and past the transport's own 256-datagram outbound queue, which is
/// the point: a burst that fits in the queue proves nothing about a burst that
/// does not.
pub(crate) const BULK_BYTES: usize = 1 << 20;

/// How long a transfer may take before we call it stalled. Generous for one
/// `MiB` over loopback ICE (the drills measured ~2 ms RTT); a stall is
/// indefinite, so anything in between is not a case we need to distinguish.
///
/// The matrix raises this per cell — an 8 `MiB` transfer under a 20× CPU
/// throttle is slow, not stalled, and a deadline that cannot tell them apart
/// would report the throttle as the bug.
pub(crate) const TRANSFER_DEADLINE: Duration = Duration::from_secs(20);

/// Which way the bulk flows in one exchange.
///
/// The consumer's stall was on `OP_READ` — a small request, a bulk reply — so
/// [`Self::Download`] is the shape that matters most and the one the regression
/// suite uses. The other two exist because "the send pump drops under load" and
/// "the receive path drops under load" are different claims, and an exchange
/// that is bulk in both directions cannot distinguish them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    /// Small request, bulk reply — the `OP_READ` shape.
    Download,
    /// Bulk request, small reply — stresses the initiator's send pump.
    Upload,
    /// Bulk both ways at once.
    Both,
}

impl Direction {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Download => "down",
            Self::Upload => "up",
            Self::Both => "both",
        }
    }

    /// Bytes crossing the wire in one exchange of `size`, for the in-flight
    /// budget the matrix enforces.
    pub(crate) fn bytes_for(self, size: usize) -> usize {
        match self {
            Self::Download | Self::Upload => size,
            Self::Both => size * 2,
        }
    }
}

/// One exchange's request header: the mode, then the byte count wanted back.
///
/// Five bytes rather than four so the server can be told which direction to
/// play without a second stream or a second ALPN. `Upload` sets `wanted` to a
/// token reply size; the bulk is what follows on the request stream.
const HEADER_LEN: usize = 5;

fn header(direction: Direction, wanted: u32) -> [u8; HEADER_LEN] {
    let mut bytes = [0u8; HEADER_LEN];
    bytes[0] = match direction {
        Direction::Download => 0,
        Direction::Upload => 1,
        Direction::Both => 2,
    };
    bytes[1..].copy_from_slice(&wanted.to_le_bytes());
    bytes
}

/// The bulk peer: reads a header, then plays whichever direction it names.
#[derive(Debug, Clone)]
pub(crate) struct Bench;

impl ProtocolHandler for Bench {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;

        let mut head = [0u8; HEADER_LEN];
        recv.read_exact(&mut head)
            .await
            .map_err(AcceptError::from_err)?;
        let wanted = u32::from_le_bytes([head[1], head[2], head[3], head[4]]) as usize;

        // Anything after the header on the request stream is the client's bulk
        // upload. Drained and verified before replying, so a corrupted upload
        // fails as an upload rather than as a mismatched reply.
        if matches!(head[0], 1 | 2) {
            let uploaded = recv
                .read_to_end(MAX_TRANSFER_BYTES)
                .await
                .map_err(AcceptError::from_err)?;
            if !is_payload(&uploaded) {
                return Err(AcceptError::from_err(std::io::Error::other(
                    "uploaded body did not match the expected pattern",
                )));
            }
        }

        // One write of the whole reply: handing QUIC the entire body at once is
        // what produces the burst the outbound pump has to survive. Feeding it
        // in small pieces would pace the sender for it and hide the defect.
        send.write_all(&payload(wanted))
            .await
            .map_err(AcceptError::from_err)?;
        send.finish().map_err(AcceptError::from_err)?;
        connection.closed().await;
        Ok(())
    }
}

/// Ceiling on one transfer, so `read_to_end` has a bound that is not a memory
/// policy in disguise. Above any cell the matrix runs.
pub(crate) const MAX_TRANSFER_BYTES: usize = 64 << 20;

/// A recognisable, position-dependent body, so a truncated or misordered
/// transfer fails on content rather than only on length.
pub(crate) fn payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| u8::try_from(index % 251).expect("bounded by 251"))
        .collect()
}

/// Verify a body against [`payload`] **without building it**.
///
/// The matrix runs 8 `MiB` cells at concurrency 4; materialising an expected
/// vector per check would double the tab's peak memory for no benefit, and an
/// OOM-killed tab is not a transport finding.
pub(crate) fn is_payload(body: &[u8]) -> bool {
    body.iter()
        .enumerate()
        .all(|(index, byte)| usize::from(*byte) == index % 251)
}

/// A negotiated browser↔browser pair, both ends live in this tab.
pub(crate) struct Pair {
    client: Endpoint,
    server_addr: EndpointAddr,
    pub(crate) client_hub: std::sync::Arc<BrowserHubTransport>,
    pub(crate) server_id: EndpointId,
    router: Router,
}

impl Pair {
    /// Stand up both endpoints, negotiate one data channel between them, and
    /// leave the server accepting [`BENCH_ALPN`].
    pub(crate) async fn negotiate() -> anyhow::Result<Self> {
        let key_client = SecretKey::generate();
        let key_server = SecretKey::generate();
        let id_client = key_client.public();
        let id_server = key_server.public();

        let client_hub = WebRtcHandle::hub(id_client);
        let server_hub = WebRtcHandle::hub(id_server);

        let client = browser_endpoint(key_client, &client_hub).await?;
        let server = browser_endpoint(key_server, &server_hub).await?;
        let router = Router::builder(server).accept(BENCH_ALPN, Bench).spawn();

        // Host-only ICE: both agents are in this tab, so loopback candidates
        // pair without a STUN round trip — and a test that reached a public
        // STUN server would be measuring the network, not the transport.
        let (pending_offer, offer_env) = browser_offer(id_client, &IceServers::host_only())
            .await
            .map_err(|error| js_error(&error))?;
        let (pending_answer, answer_env) =
            browser_answer(id_server, &offer_env, &IceServers::host_only())
                .await
                .map_err(|error| js_error(&error))?;

        // Concurrently, and this is load-bearing: the answerer is waiting for
        // `ondatachannel`, which only fires once the offerer applies the
        // answer. Awaiting them in sequence deadlocks.
        let (client_session, server_session) = n0_future::future::zip(
            pending_offer.complete(client_hub.transport().as_ref(), &answer_env),
            pending_answer.complete(server_hub.transport().as_ref(), id_client),
        )
        .await;
        client_session.map_err(|error| js_error(&error))?;
        server_session.map_err(|error| js_error(&error))?;

        anyhow::ensure!(client_hub.has_session(&id_server), "client session is live");
        anyhow::ensure!(server_hub.has_session(&id_client), "server session is live");

        Ok(Self {
            client,
            server_addr: EndpointAddr::from_parts(
                id_server,
                [TransportAddr::Custom(custom_addr(id_server))],
            ),
            client_hub: client_hub.transport(),
            server_id: id_server,
            router,
        })
    }

    /// A download of `wanted` bytes — the regression suite's exchange.
    pub(crate) async fn request(&self, wanted: usize) -> anyhow::Result<()> {
        self.exchange(Direction::Download, wanted, TRANSFER_DEADLINE)
            .await
    }

    /// One exchange in `direction`, sized `bulk`, bounded by `deadline`.
    ///
    /// A fresh connection per call for the same reason `loopback.rs` uses one:
    /// [`Bench`] serves a single bi-stream then parks on `closed()`. Callers
    /// that want many exchanges over *one* data channel get that for free —
    /// the channel is the session, the QUIC connection is not.
    pub(crate) async fn exchange(
        &self,
        direction: Direction,
        bulk: usize,
        deadline: Duration,
    ) -> anyhow::Result<()> {
        // A token reply, small enough that the `Upload` case is unambiguously
        // measuring one direction. Not zero: a reply of nothing would let a
        // completely dead return path pass.
        const TOKEN: usize = 32;

        let connection = self
            .client
            .connect(self.server_addr.clone(), BENCH_ALPN)
            .await?;

        // The only transport is WebRTC, but assert the selected path anyway so
        // a future regression can't silently reroute and call it a pass.
        let paths = connection.paths();
        let on_webrtc = paths.iter().any(|path| {
            matches!(
                path.remote_addr(),
                TransportAddr::Custom(addr) if addr.id() == WEBRTC_TRANSPORT_ID
            )
        });
        anyhow::ensure!(on_webrtc, "no WebRTC path on the connection: {paths:?}");

        let reply = match direction {
            Direction::Download | Direction::Both => bulk,
            Direction::Upload => TOKEN,
        };
        let upload = match direction {
            Direction::Upload | Direction::Both => bulk,
            Direction::Download => 0,
        };

        let (mut send, mut recv) = connection.open_bi().await?;

        // The whole exchange under one deadline, both legs inside it. A stall
        // is silent and indefinite — the harness would simply never finish — so
        // the failure has to be manufactured, and it has to cover the send leg
        // too: an upload that wedges in `write_all` is exactly the pump
        // starvation this is hunting.
        let run = async {
            send.write_all(&header(
                direction,
                u32::try_from(reply).expect("reply fits in u32"),
            ))
            .await?;
            if upload > 0 {
                send.write_all(&payload(upload)).await?;
            }
            send.finish()?;
            let body = recv.read_to_end(reply + 64).await?;
            anyhow::Ok(body)
        };

        let Ok(body) = n0_future::time::timeout(deadline, run).await else {
            let counters = self.client_hub.session_counters(&self.server_id);
            let (out_bytes, in_bytes) = self.channel_bytes().await;
            anyhow::bail!(
                "{} of {bulk} bytes stalled past {deadline:?}; \
                 data-channel bytes sent={out_bytes} received={in_bytes}; \
                 outbound lane = {counters:?}",
                direction.label()
            );
        };
        let body = body?;

        anyhow::ensure!(
            body.len() == reply,
            "short reply: wanted {reply} bytes, got {}",
            body.len()
        );
        anyhow::ensure!(is_payload(&body), "reply body did not match");
        connection.close(0u32.into(), b"done");
        Ok(())
    }

    /// Bytes this session's data channel has carried, both directions.
    ///
    /// Deliberately **not** `selected_pair_stats`: this test caught that reader
    /// reporting 7400 bytes and 28 packets on a connection that had just
    /// carried a verified megabyte, intermittently and on the transport row as
    /// well as the candidate pair. The `data-channel` row comes from the layer
    /// that actually carries our datagrams and names this channel by its label,
    /// so it cannot be describing someone else's traffic.
    ///
    /// An instantaneous sample, which is what a *failure* path wants — see
    /// [`Self::channel_bytes_settled`] for the one a report wants.
    pub(crate) async fn channel_bytes(&self) -> (f64, f64) {
        let (sent, received, _, _) = self
            .client_hub
            .data_channel_bytes(&self.server_id)
            .await
            .unwrap_or((0.0, 0.0, 0.0, 0.0));
        (sent, received)
    }

    /// [`Self::channel_bytes`], sampled until it stops moving.
    ///
    /// The browser updates these counters on its own cadence, not ours, so a
    /// read taken the instant a transfer's last byte is delivered can still be
    /// reporting the state before it. Measured on a passing 64 `KiB` cell: a
    /// verified transfer completed and the counter said zero.
    ///
    /// That is intolerable in a table built to hunt a stall, because "0 bytes
    /// received" is the exact signature being hunted — a sampling artefact
    /// dressed as the bug. So: poll until two consecutive reads agree, or give
    /// up and say so. `None` means "not observed", which a caller must render
    /// as unknown rather than as zero.
    pub(crate) async fn channel_bytes_settled(&self) -> Option<(f64, f64)> {
        const SETTLE_ATTEMPTS: usize = 60;
        const SETTLE_GAP: Duration = Duration::from_millis(25);

        let mut previous = self.channel_bytes().await;
        for _ in 0..SETTLE_ATTEMPTS {
            n0_future::time::sleep(SETTLE_GAP).await;
            let current = self.channel_bytes().await;
            // Stability is judged on *received* alone. QUIC keeps acknowledging
            // after the last payload byte lands, so the sent counter can still
            // be creeping when the received one is final — waiting for both to
            // hold still would sometimes wait out the whole budget and then
            // report "not observed" for a transfer that plainly finished.
            //
            // Non-zero as well as stable: two zeros in a row means the row has
            // not appeared yet, not that nothing crossed.
            if (current.1 - previous.1).abs() < f64::EPSILON && current.1 > 0.0 {
                return Some(current);
            }
            previous = current;
        }
        (previous.1 > 0.0).then_some(previous)
    }

    /// Every `transport`, `candidate-pair`, `data-channel` and
    /// `peer-connection` row `getStats` reports, verbatim.
    ///
    /// Only ever printed on a failure: when the byte counters disagree with
    /// what demonstrably crossed the connection, the question is which row the
    /// reader picked, and that cannot be answered from the two numbers it
    /// returned.
    pub(crate) async fn dump_stats(&self) -> String {
        let Some(peer_connection) = self.client_hub.peer_connection(&self.server_id) else {
            return "<no live session>".to_owned();
        };
        let Ok(report) = wasm_bindgen_futures::JsFuture::from(peer_connection.get_stats()).await
        else {
            return "<getStats failed>".to_owned();
        };
        let mut rows = Vec::new();
        let Some(iter) = js_sys::try_iter(&report).ok().flatten() else {
            return "<report not iterable>".to_owned();
        };
        for entry in iter.flatten() {
            let Ok(pair) = entry.dyn_into::<js_sys::Array>() else {
                continue;
            };
            let Ok(object) = pair.get(1).dyn_into::<js_sys::Object>() else {
                continue;
            };
            let kind = js_sys::Reflect::get(&object, &wasm_bindgen::JsValue::from_str("type"))
                .ok()
                .and_then(|value| value.as_string())
                .unwrap_or_default();
            if matches!(
                kind.as_str(),
                "transport" | "candidate-pair" | "data-channel" | "peer-connection"
            ) {
                rows.push(
                    js_sys::JSON::stringify(&object)
                        .map_or_else(|_| "<unstringifiable>".to_owned(), String::from),
                );
            }
        }
        rows.join("\n")
    }

    pub(crate) async fn shutdown(self) {
        let _ = self.router.shutdown().await;
        self.client.close().await;
    }
}

/// A browser peer: WebRTC is its only data path, and no relay, so a run that
/// claims to be browser↔browser can be shown to be one.
pub(crate) async fn browser_endpoint(
    key: SecretKey,
    hub: &WebRtcHandle,
) -> anyhow::Result<Endpoint> {
    Ok(Endpoint::builder(presets::Minimal)
        .secret_key(key)
        .relay_mode(RelayMode::Disabled)
        .add_custom_transport(hub.transport())
        .path_selector(hub.path_selector())
        .bind()
        .await?)
}

/// `JsValue` is not an `Error`, so it cannot cross `?` on its own.
pub(crate) fn js_error(error: &wasm_bindgen::JsValue) -> anyhow::Error {
    anyhow::anyhow!("{error:?}")
}
