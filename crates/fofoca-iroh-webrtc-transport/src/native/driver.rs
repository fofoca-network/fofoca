use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use bytes::Bytes;
use iroh::EndpointId;
use str0m::channel::ChannelId;
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, IceConnectionState, Input, Output, Rtc};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use super::session::{InboundPacket, SessionRegistry};

/// A `WebRTC` data channel negotiated via JSEP and ready to carry datagrams —
/// the value handed to [`crate::WebRtcTransport::attach`].
pub struct NegotiatedSession {
    pub(crate) rtc: Rtc,
    pub(crate) socket: UdpSocket,
    pub(crate) channel_id: ChannelId,
    pub(crate) advertised: SocketAddr,
}

impl std::fmt::Debug for NegotiatedSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NegotiatedSession")
            .field("advertised", &self.advertised)
            .finish_non_exhaustive()
    }
}

pub(crate) fn build_rtc(now: Instant) -> Rtc {
    Rtc::builder()
        .set_crypto_provider(Arc::new(str0m_aws_lc_rs::default_provider()))
        .build(now)
}

/// Binds an ephemeral UDP socket for the str0m session. When the bind is
/// unspecified, advertise a non-loopback interface address so the single
/// (vanilla-ICE) host candidate is reachable beyond this machine; the
/// loopback fallback is what makes same-host tests work.
pub(crate) async fn bind_ephemeral_udp() -> std::io::Result<(UdpSocket, SocketAddr)> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
    let local = socket.local_addr()?;
    let ip = if local.ip().is_unspecified() {
        if_addrs::get_if_addrs()
            .ok()
            .into_iter()
            .flatten()
            .find(|iface| !iface.is_loopback() && iface.addr.ip().is_ipv4())
            .map_or(Ipv4Addr::LOCALHOST.into(), |iface| iface.addr.ip())
    } else {
        local.ip()
    };
    Ok((socket, SocketAddr::new(ip, local.port())))
}

pub(crate) fn add_host_candidate(rtc: &mut Rtc, advertised: SocketAddr) -> anyhow::Result<()> {
    let candidate = Candidate::host(advertised, "udp").context("ICE host candidate")?;
    rtc.add_local_candidate(candidate);
    Ok(())
}

/// Ask STUN what this socket looks like from outside and, if anything
/// answers, offer that as a server-reflexive candidate too.
///
/// This is what lifts the transport off the LAN. `str0m` gathers nothing on
/// its own, so without this the SDP carries a single host candidate on a
/// private interface and two peers behind different NATs never meet.
///
/// A failure here is logged and swallowed: the host candidate still stands,
/// and the relay fallback covers what ICE cannot reach.
pub(crate) async fn add_reflexive_candidate(
    rtc: &mut Rtc,
    socket: &UdpSocket,
    base: SocketAddr,
    config: &super::stun::IceConfig,
) -> bool {
    let Some(reflexive) = super::stun::gather_reflexive(socket, config).await else {
        return false;
    };
    // A NAT that does not translate reports the base back; adding it again
    // would just duplicate the host candidate.
    if reflexive == base {
        tracing::debug!(%reflexive, "reflexive address equals the base; not on a NAT");
        return false;
    }
    // str0m rejects a reflexive/base pair on different IP versions, so a v6
    // answer for a v4 socket surfaces here as an error rather than a candidate
    // that never pairs.
    match Candidate::server_reflexive(reflexive, base, "udp") {
        Ok(candidate) => {
            rtc.add_local_candidate(candidate);
            tracing::debug!(%reflexive, %base, "added a server-reflexive candidate");
            true
        }
        Err(error) => {
            tracing::debug!(%error, "str0m rejected the reflexive candidate");
            false
        }
    }
}

pub(crate) enum ChannelReadyTarget {
    Offerer(ChannelId),
    Answerer,
}

/// Drives ICE/DTLS/SCTP until the data channel opens. Returns the open
/// channel's id (the answerer learns it from the `ChannelOpen` event).
pub(crate) async fn drive_until_channel_ready(
    rtc: &mut Rtc,
    socket: &UdpSocket,
    advertised: SocketAddr,
    target: &ChannelReadyTarget,
    deadline: Duration,
) -> anyhow::Result<ChannelId> {
    let give_up = Instant::now() + deadline;
    let mut buf = vec![0u8; 2000];
    let mut next_wake = Instant::now();
    let mut opened: Option<ChannelId> = None;

    while Instant::now() < give_up {
        let sleep_for = next_wake
            .saturating_duration_since(Instant::now())
            .max(Duration::from_millis(1))
            .min(Duration::from_millis(50));

        tokio::select! {
            () = tokio::time::sleep(sleep_for) => {
                rtc.handle_input(Input::Timeout(Instant::now()))
                    .context("str0m timeout input")?;
            }
            received = socket.recv_from(&mut buf) => {
                let (len, src) = received.context("UDP recv during JSEP handshake")?;
                if len == 0 {
                    continue;
                }
                // The destination must be the advertised candidate address,
                // not the socket's 0.0.0.0 bind — str0m matches it against
                // its local ICE candidate.
                //
                // A datagram that does not parse is noise, not failure — the
                // same tolerance `drive_session` has. The media socket is
                // shared with the STUN gather, and the gather probes its
                // servers concurrently: the losing server's Binding Success
                // lands here after the winner returned, and str0m rejects it
                // ("no message integrity") because it is a plain RFC 5389
                // reply, not an ICE-authenticated one. Failing the whole
                // handshake for a stray reply killed real connects.
                let Ok(receive) = Receive::new(Protocol::Udp, src, advertised, &buf[..len]) else {
                    tracing::trace!(%src, "ignoring a datagram str0m cannot parse during the handshake");
                    continue;
                };
                rtc.handle_input(Input::Receive(Instant::now(), receive))
                    .context("str0m receive input")?;
            }
        }

        loop {
            match rtc.poll_output().context("str0m poll_output")? {
                Output::Timeout(wake) => {
                    next_wake = wake;
                    break;
                }
                Output::Transmit(transmit) => {
                    let _ = socket
                        .send_to(&transmit.contents, transmit.destination)
                        .await;
                }
                Output::Event(Event::ChannelOpen(id, _)) => match target {
                    ChannelReadyTarget::Offerer(expected) if id == *expected => {
                        opened = Some(id);
                    }
                    ChannelReadyTarget::Offerer(_) => {}
                    ChannelReadyTarget::Answerer => {
                        opened = Some(id);
                    }
                },
                Output::Event(_) => {}
            }
        }

        // Return only after the drain hit `Timeout`: quitting mid-drain
        // would swallow queued transmits (notably the DCEP ACK the remote
        // offerer is waiting on to open its side of the channel).
        if let Some(id) = opened {
            return Ok(id);
        }
    }

    anyhow::bail!("timed out waiting for the WebRTC data channel to open");
}

enum SessionEnd {
    Closed(&'static str),
    Failed(anyhow::Error),
}

/// Per-session driver: pumps the sans-io `Rtc` between its UDP socket, the
/// bounded outbound queue, and the shared inbound fan-in until the channel
/// closes or ICE gives up, then removes itself from the registry.
pub(crate) async fn drive_session(
    session: NegotiatedSession,
    remote: EndpointId,
    generation: u64,
    registry: Arc<SessionRegistry>,
    mut out_rx: mpsc::Receiver<Bytes>,
    in_tx: mpsc::Sender<InboundPacket>,
    dropped_rx: Arc<AtomicU64>,
) {
    let NegotiatedSession {
        mut rtc,
        socket,
        channel_id,
        advertised,
    } = session;
    let mut buf = vec![0u8; 2000];
    let mut next_wake = Instant::now();

    let end = loop {
        let sleep_for = next_wake
            .saturating_duration_since(Instant::now())
            .max(Duration::from_millis(1))
            .min(Duration::from_millis(100));

        tokio::select! {
            () = tokio::time::sleep(sleep_for) => {
                if let Err(error) = rtc.handle_input(Input::Timeout(Instant::now())) {
                    break SessionEnd::Failed(error.into());
                }
            }
            received = socket.recv_from(&mut buf) => {
                let Ok((len, src)) = received else {
                    break SessionEnd::Failed(anyhow::anyhow!("session UDP socket error"));
                };
                if len == 0 {
                    continue;
                }
                let Ok(receive) = Receive::new(Protocol::Udp, src, advertised, &buf[..len]) else {
                    continue;
                };
                if let Err(error) = rtc.handle_input(Input::Receive(Instant::now(), receive)) {
                    break SessionEnd::Failed(error.into());
                }
            }
            outbound = out_rx.recv() => {
                let Some(datagram) = outbound else {
                    break SessionEnd::Closed("transport dropped");
                };
                if let Some(mut channel) = rtc.channel(channel_id) {
                    // One QUIC datagram = one binary SCTP message; boundaries
                    // are preserved end to end, so no framing header.
                    if channel.write(true, &datagram).is_err() {
                        // The channel refused the write (e.g. buffer full or
                        // closing): drop the datagram, QUIC retransmits.
                        let total = dropped_rx.fetch_add(1, Ordering::Relaxed) + 1;
                        super::session::note_dropped(&remote, total, "channel write refused");
                    }
                }
            }
        }

        match pump_outputs(&mut rtc, &socket, channel_id, remote, &in_tx).await {
            Ok(wake) => next_wake = wake,
            Err(end) => break end,
        }
    };

    match end {
        SessionEnd::Closed(reason) => {
            tracing::debug!(%remote, reason, "webrtc session closed");
        }
        SessionEnd::Failed(error) => {
            tracing::warn!(%remote, "webrtc session failed: {error:#}");
        }
    }
    let dropped_total = dropped_rx.load(Ordering::Relaxed);
    if dropped_total > 0 {
        tracing::info!(%remote, dropped_total, "webrtc session dropped datagrams over its lifetime");
    }
    registry.remove_if_generation(&remote, generation);
}

async fn pump_outputs(
    rtc: &mut Rtc,
    socket: &UdpSocket,
    channel_id: ChannelId,
    remote: EndpointId,
    in_tx: &mpsc::Sender<InboundPacket>,
) -> Result<Instant, SessionEnd> {
    loop {
        let output = rtc
            .poll_output()
            .map_err(|error| SessionEnd::Failed(error.into()))?;
        match output {
            Output::Timeout(wake) => return Ok(wake),
            Output::Transmit(transmit) => {
                let _ = socket
                    .send_to(&transmit.contents, transmit.destination)
                    .await;
            }
            Output::Event(Event::ChannelData(data)) if data.id == channel_id => {
                let packet = InboundPacket {
                    from: remote,
                    payload: Bytes::from(data.data),
                };
                // Lossy on overflow, like the UDP it stands in for.
                let _ = in_tx.try_send(packet);
            }
            Output::Event(Event::ChannelClose(id)) if id == channel_id => {
                return Err(SessionEnd::Closed("data channel closed"));
            }
            Output::Event(Event::IceConnectionStateChange(IceConnectionState::Disconnected)) => {
                return Err(SessionEnd::Closed("ICE disconnected"));
            }
            Output::Event(_) => {}
        }
    }
}
