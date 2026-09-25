//! A stream node: one endpoint that produces streams, opens them, or both.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use fofoca::iroh::Endpoint;
use fofoca::iroh::protocol::Router;
use fofoca::net::direct::{
    IceProfile, MAX_DIRECT_PEERS, MESH_WEBRTC_SIGNAL_ALPN, PROBE_DEADLINE, SignalAdmission,
    WebRtcHandle, WebRtcSignalAcceptor, build_peer_webrtc, dial_signal, pair_needs_lane,
    wait_direct,
};
use fofoca::net::{PathFlags, TransportOpts, add_peer_addr};
use fofoca::protocol::{Lookup, LookupOpts, MeshConfig, RelayLadder, Transport};
use rand::RngCore as _;
use serde::Deserialize;

use crate::code;
use crate::hash::{ID_LEN, SECRET_LEN, StreamHash};
use crate::produce::{Producer, Registry, StreamAcceptor};
use crate::read::{Reader, Refused};

/// How long `bind` waits for the endpoint to learn its own addresses, so the
/// first hash it mints is reachable. Best effort: it never blocks past this.
const ONLINE_WAIT: Duration = Duration::from_secs(5);

/// How a node finds peers and what may carry its bytes — the same three lists
/// a mesh create takes, so a producer and a tab agree on them.
///
/// `Deserialize` so a browser hands its constructor a plain object;
/// `deny_unknown_fields` so a typo is an error, not a silent loopback node.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct StreamOpts {
    /// How peers find each other: any of `mdns`, `dht`, `relay`. None is a
    /// loopback node.
    pub lookup: Vec<Lookup>,
    /// What may carry bytes: `p2p`, and `relay` if named.
    pub transport: Vec<Transport>,
    /// A custom relay ladder, first preferred. Empty is the default ladder.
    pub relay_urls: Vec<String>,
    /// Which of this node's paths may carry bytes.
    pub paths: PathFlags,
}

/// One endpoint serving and opening streams. It never joins a gossip mesh.
#[derive(Debug)]
pub struct StreamNode {
    endpoint: Endpoint,
    router: Router,
    webrtc: WebRtcHandle,
    registry: Arc<Registry>,
    lookups: LookupOpts,
    relay_transport: bool,
    paths: PathFlags,
    ice: IceProfile,
}

impl StreamNode {
    /// Stand up an endpoint with `opts`.
    ///
    /// # Errors
    /// Conflicting or invalid options, a loopback node in a browser, or an
    /// endpoint that fails to bind.
    pub async fn bind(opts: &StreamOpts) -> Result<Self> {
        let config = MeshConfig::resolve(
            &opts.lookup,
            relay_ladder(&opts.relay_urls)?,
            &opts.transport,
        )?;
        Self::bind_with(config.lookups, config.transport.relay_transport, opts.paths).await
    }

    /// Stand up an endpoint that can reach the producer of `hash`: the hash's
    /// own lookups and relay policy, and every path this target has.
    ///
    /// # Errors
    /// As [`bind`](Self::bind).
    pub async fn bind_for(hash: &StreamHash) -> Result<Self> {
        Self::bind_with(
            hash.lookups.clone(),
            hash.relay_transport,
            PathFlags::default(),
        )
        .await
    }

    async fn bind_with(
        lookups: LookupOpts,
        relay_transport: bool,
        paths: PathFlags,
    ) -> Result<Self> {
        // A browser has no UDP socket, so a loopback node there is one no peer
        // can ever reach, and it would fail silently.
        #[cfg(target_arch = "wasm32")]
        anyhow::ensure!(
            !lookups.is_loopback(),
            "a browser cannot reach a loopback stream: name a lookup (`relay`)"
        );
        let (endpoint, webrtc) = build_peer_webrtc(&lookups, TransportOpts::from(paths)).await?;
        let registry = Arc::new(Registry::default());
        let ice = IceProfile {
            host_only: lookups.is_loopback(),
        };
        let mut router = Router::builder(endpoint.clone()).accept(
            crate::STREAM_ALPN,
            StreamAcceptor {
                registry: Arc::clone(&registry),
                relay_transport,
            },
        );
        if paths.webrtc {
            router = router.accept(
                MESH_WEBRTC_SIGNAL_ALPN,
                WebRtcSignalAcceptor::new(
                    webrtc.clone(),
                    endpoint.clone(),
                    endpoint.id(),
                    SignalAdmission::new(MAX_DIRECT_PEERS),
                    ice,
                ),
            );
        }
        let router = router.spawn();
        if !lookups.is_loopback() {
            let _ = n0_future::time::timeout(ONLINE_WAIT, endpoint.online()).await;
        }
        Ok(Self {
            endpoint,
            router,
            webrtc,
            registry,
            lookups,
            relay_transport,
            paths,
            ice,
        })
    }

    /// Open a new stream and return its writing end. Hand its
    /// [`hash`](Producer::hash) to the one consumer.
    #[must_use]
    pub fn create(&self) -> Producer {
        let mut id = [0; ID_LEN];
        let mut secret = [0; SECRET_LEN];
        rand::rng().fill_bytes(&mut id);
        rand::rng().fill_bytes(&mut secret);
        Producer::new(
            StreamHash {
                addr: self.endpoint.addr(),
                lookups: self.lookups.clone(),
                relay_transport: self.relay_transport,
                id,
                secret,
            },
            Arc::clone(&self.registry),
        )
    }

    /// Take the consumer slot of the stream behind `hash`.
    ///
    /// # Errors
    /// The producer cannot be reached, the only path is a relay the stream
    /// refuses, or the producer refuses the hash (a [`Refused`]).
    pub async fn open(&self, hash: &StreamHash) -> Result<Reader> {
        add_peer_addr(&self.endpoint, hash.addr.clone())?;
        // The WebRTC lane first: iroh does not move a live connection onto a
        // transport attached after it, so a browser's connection must start on
        // the data channel.
        if self.paths.webrtc
            && pair_needs_lane(&hash.addr, self.paths.ip)
            && !self.webrtc.has_session(&hash.addr.id)
            && let Err(error) =
                dial_signal(&self.endpoint, hash.addr.clone(), &self.webrtc, self.ice).await
            && !hash.relay_transport
        {
            return Err(error.context("opening a WebRTC lane to the producer"));
        }
        let conn = self
            .endpoint
            .connect(hash.addr.clone(), crate::STREAM_ALPN)
            .await
            .context("connecting to the producer")?;
        if !hash.relay_transport && !wait_direct(&conn, PROBE_DEADLINE).await {
            conn.close(code::RELAY_REFUSED.into(), b"relay path refused");
            bail!(Refused::RelayRefused);
        }
        let (mut send, recv) = conn.open_bi().await.context("opening the stream")?;
        send.write_all(&hash.id).await?;
        send.write_all(&hash.secret).await?;
        send.finish()?;
        Ok(Reader::new(conn, recv))
    }

    /// Shut the node down. Every stream still open is abandoned.
    pub async fn close(self) {
        self.registry.clear();
        let _ = self.router.shutdown().await;
        self.endpoint.close().await;
    }
}

/// The ladder `urls` name, or `None` for the default. Parsed as one list, so an
/// empty or bad entry is an error rather than a shrunk ladder.
fn relay_ladder(urls: &[String]) -> Result<Option<RelayLadder>> {
    if urls.is_empty() {
        return Ok(None);
    }
    urls.join(",")
        .parse::<RelayLadder>()
        .map(Some)
        .map_err(|error| anyhow::anyhow!("{error}"))
}
