//! A minimal RFC 5389 STUN client — just enough to learn one
//! server-reflexive address.
//!
//! [`str0m`](https://docs.rs/str0m) deliberately does no candidate gathering:
//! it owns no sockets, so discovering addresses is the caller's job. Without
//! this the transport advertises a single `Candidate::host` on a LAN
//! interface, which is why the experiment this was forked from only ever
//! connected on one network.
//!
//! The one rule that matters: the Binding Request **must** go out of the very
//! socket the media will later use. A NAT maps per source port, so a mapping
//! learned on any other socket describes a hole that the data will not arrive
//! through.
//!
//! No TURN. A consumer whose ICE fails outright is expected to fall back to
//! its own ALPN over the iroh relay, which gives the same reachability without
//! a second relay protocol to implement and operate.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::net::UdpSocket;

/// STUN message type for a Binding Request.
const BINDING_REQUEST: u16 = 0x0001;
/// STUN message type for a Binding Success Response.
const BINDING_SUCCESS: u16 = 0x0101;
/// The fixed cookie every RFC 5389 message carries at bytes `4..8`. Also the
/// XOR mask for the port and (`IPv4`) address in `XOR-MAPPED-ADDRESS`.
const MAGIC_COOKIE: u32 = 0x2112_A442;
/// Attribute type `XOR-MAPPED-ADDRESS`.
const ATTR_XOR_MAPPED_ADDRESS: u16 = 0x0020;
/// Attribute type `MAPPED-ADDRESS` — the pre-5389 spelling, unXORed. Accepted
/// as a fallback because some deployed servers still answer with it.
const ATTR_MAPPED_ADDRESS: u16 = 0x0001;

/// Every STUN message begins with a 20-byte header.
const HEADER_LEN: usize = 20;
/// Length of the transaction id, which follows the magic cookie.
const TRANSACTION_ID_LEN: usize = 12;

/// Address families inside a `MAPPED-ADDRESS`-shaped attribute.
const FAMILY_IPV4: u8 = 0x01;
const FAMILY_IPV6: u8 = 0x02;

/// A Binding Request plus the transaction id that identifies its reply.
struct BindingRequest {
    message: [u8; HEADER_LEN],
    transaction_id: [u8; TRANSACTION_ID_LEN],
}

fn binding_request(transaction_id: [u8; TRANSACTION_ID_LEN]) -> BindingRequest {
    let mut message = [0u8; HEADER_LEN];
    message[0..2].copy_from_slice(&BINDING_REQUEST.to_be_bytes());
    // A plain Binding Request carries no attributes, so the body is empty.
    message[2..4].copy_from_slice(&0u16.to_be_bytes());
    message[4..8].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
    message[8..HEADER_LEN].copy_from_slice(&transaction_id);
    BindingRequest {
        message,
        transaction_id,
    }
}

/// Pull the reflexive address out of a Binding Success Response.
///
/// # Errors
/// A short or non-success message, a cookie/transaction-id mismatch (a stray
/// datagram, not our reply), or no address attribute we understand.
fn parse_binding_response(
    message: &[u8],
    transaction_id: &[u8; TRANSACTION_ID_LEN],
) -> Result<SocketAddr> {
    let header = message
        .get(..HEADER_LEN)
        .context("STUN response shorter than a header")?;
    let kind = u16::from_be_bytes([header[0], header[1]]);
    if kind != BINDING_SUCCESS {
        bail!("STUN response is not a Binding Success (type {kind:#06x})");
    }
    if u32::from_be_bytes([header[4], header[5], header[6], header[7]]) != MAGIC_COOKIE {
        bail!("STUN response has the wrong magic cookie");
    }
    if &header[8..HEADER_LEN] != transaction_id {
        bail!("STUN response is for another transaction");
    }

    let body_len = usize::from(u16::from_be_bytes([header[2], header[3]]));
    let body = message
        .get(HEADER_LEN..HEADER_LEN + body_len)
        .context("STUN response body is truncated")?;

    // Attributes are TLV, each padded to a 4-byte boundary. Prefer the XORed
    // form; remember a plain MAPPED-ADDRESS in case that is all we get.
    let mut fallback = None;
    let mut pos = 0usize;
    while pos + 4 <= body.len() {
        let attr_type = u16::from_be_bytes([body[pos], body[pos + 1]]);
        let attr_len = usize::from(u16::from_be_bytes([body[pos + 2], body[pos + 3]]));
        let value_start = pos + 4;
        let value = body
            .get(value_start..value_start + attr_len)
            .context("STUN attribute is truncated")?;
        match attr_type {
            ATTR_XOR_MAPPED_ADDRESS => return decode_address(value, transaction_id, true),
            ATTR_MAPPED_ADDRESS if fallback.is_none() => {
                fallback = decode_address(value, transaction_id, false).ok();
            }
            _ => {}
        }
        // Round the value length up to the next multiple of four.
        pos = value_start + attr_len.next_multiple_of(4);
    }
    fallback.context("STUN response carried no address attribute we understand")
}

/// Decode a `MAPPED-ADDRESS`-shaped value: `reserved(1) family(1) port(2) addr(4|16)`.
/// When `xored`, the port and address are masked per RFC 5389 §15.2.
fn decode_address(
    value: &[u8],
    transaction_id: &[u8; TRANSACTION_ID_LEN],
    xored: bool,
) -> Result<SocketAddr> {
    let family = *value.get(1).context("address attribute too short")?;
    let raw_port = u16::from_be_bytes([
        *value.get(2).context("address attribute too short")?,
        *value.get(3).context("address attribute too short")?,
    ]);
    // The port is masked with the top 16 bits of the cookie.
    let port = if xored {
        raw_port ^ u16::try_from(MAGIC_COOKIE >> 16).expect("the high half of a u32 fits a u16")
    } else {
        raw_port
    };

    match family {
        FAMILY_IPV4 => {
            let raw: [u8; 4] = value
                .get(4..8)
                .context("IPv4 address attribute too short")?
                .try_into()
                .expect("4 bytes");
            let octets = if xored {
                let mask = MAGIC_COOKIE.to_be_bytes();
                std::array::from_fn(|index| raw[index] ^ mask[index])
            } else {
                raw
            };
            Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(octets)), port))
        }
        FAMILY_IPV6 => {
            let raw: [u8; 16] = value
                .get(4..20)
                .context("IPv6 address attribute too short")?
                .try_into()
                .expect("16 bytes");
            let octets = if xored {
                // IPv6 masks with the cookie followed by the transaction id.
                let mut mask = [0u8; 16];
                mask[..4].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
                mask[4..].copy_from_slice(transaction_id);
                std::array::from_fn(|index| raw[index] ^ mask[index])
            } else {
                raw
            };
            Ok(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port))
        }
        other => bail!("unknown STUN address family {other:#04x}"),
    }
}

/// Which STUN servers to ask, and how long to wait.
///
/// Configurable rather than hardcoded because the useful answer differs by
/// deployment: a laptop on the open internet wants a public server, a private
/// network wants its own, and the test suite wants none at all.
#[derive(Debug, Clone)]
pub struct IceConfig {
    /// `host:port` STUN servers, tried in order until one answers. Resolved
    /// at gather time, so a hostname is fine.
    pub stun_servers: Vec<String>,
    /// How long to wait for any single server.
    pub stun_timeout: Duration,
}

impl IceConfig {
    /// No STUN: a host candidate only, which reaches the same machine and a
    /// flat LAN and nothing else. What the offline tests use.
    #[must_use]
    pub fn host_only() -> Self {
        Self {
            stun_servers: Vec::new(),
            stun_timeout: Duration::from_secs(2),
        }
    }
}

impl Default for IceConfig {
    fn default() -> Self {
        Self {
            // Two operators, so one being down is not an outage. These only
            // ever learn our public ip:port — no traffic flows through them.
            stun_servers: vec![
                // `stun1`, not the bare `stun.l.google.com` — see the note in
                // `web/jsep.rs`. Blocklists null-route the bare name to 0.0.0.0,
                // which costs a timeout per gather rather than failing fast.
                "stun1.l.google.com:19302".to_owned(),
                "stun.cloudflare.com:3478".to_owned(),
            ],
            stun_timeout: Duration::from_secs(2),
        }
    }
}

/// Learn this socket's reflexive address, if any server answers.
///
/// Returns `None` rather than erroring when STUN is disabled or every server
/// fails: losing the reflexive candidate costs reachability, not correctness,
/// and a share that still works on the LAN beats one that refuses to start.
/// The relay fallback covers the rest.
pub async fn gather_reflexive(socket: &UdpSocket, config: &IceConfig) -> Option<SocketAddr> {
    for server in &config.stun_servers {
        let resolved = match tokio::net::lookup_host(server).await {
            Ok(mut addrs) => addrs.next(),
            Err(error) => {
                tracing::debug!(%server, %error, "STUN server did not resolve");
                continue;
            }
        };
        let Some(addr) = resolved else {
            tracing::debug!(%server, "STUN server resolved to nothing");
            continue;
        };
        match reflexive_address(socket, addr, config.stun_timeout).await {
            Ok(reflexive) => {
                tracing::debug!(%server, %reflexive, "learned a reflexive address");
                return Some(reflexive);
            }
            Err(error) => tracing::debug!(%server, %error, "STUN request failed"),
        }
    }
    None
}

/// Ask `server` what address it sees `socket` coming from.
///
/// `socket` must be the socket the session will actually send media on — see
/// the module docs. Retries are the caller's business; a single unanswered
/// request times out rather than blocking the handshake.
///
/// # Errors
/// The send fails, no well-formed reply arrives before `timeout`, or the
/// server answers something we cannot parse.
pub async fn reflexive_address(
    socket: &UdpSocket,
    server: SocketAddr,
    timeout: Duration,
) -> Result<SocketAddr> {
    let mut transaction_id = [0u8; TRANSACTION_ID_LEN];
    rand::fill(&mut transaction_id);
    let request = binding_request(transaction_id);

    socket
        .send_to(&request.message, server)
        .await
        .with_context(|| format!("sending a STUN Binding Request to {server}"))?;

    // A STUN reply is small, but the socket may also be carrying ICE traffic
    // already; keep reading until one datagram parses as *our* response.
    let deadline = tokio::time::Instant::now() + timeout;
    let mut buffer = vec![0u8; 1500];
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            bail!("no STUN response from {server} within {timeout:?}");
        }
        let received = match tokio::time::timeout(remaining, socket.recv_from(&mut buffer)).await {
            Ok(Ok((len, from))) if from == server => len,
            // Someone else's datagram on a shared socket: ignore and keep waiting.
            Ok(Ok(_)) => continue,
            Ok(Err(error)) => {
                return Err(error).context("reading a STUN response");
            }
            Err(_elapsed) => bail!("no STUN response from {server} within {timeout:?}"),
        };
        match parse_binding_response(&buffer[..received], &request.transaction_id) {
            Ok(addr) => return Ok(addr),
            Err(error) => {
                tracing::debug!(%error, "ignoring a datagram that is not our STUN response");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ATTR_MAPPED_ADDRESS, ATTR_XOR_MAPPED_ADDRESS, BINDING_SUCCESS, FAMILY_IPV4, FAMILY_IPV6,
        HEADER_LEN, MAGIC_COOKIE, TRANSACTION_ID_LEN, binding_request, parse_binding_response,
    };
    use std::net::SocketAddr;

    const TXID: [u8; TRANSACTION_ID_LEN] = [9u8; TRANSACTION_ID_LEN];

    /// Build a Binding Success Response carrying one address attribute.
    fn response(attr_type: u16, value: &[u8], transaction_id: [u8; TRANSACTION_ID_LEN]) -> Vec<u8> {
        let mut attr = Vec::new();
        attr.extend_from_slice(&attr_type.to_be_bytes());
        attr.extend_from_slice(&u16::try_from(value.len()).expect("small").to_be_bytes());
        attr.extend_from_slice(value);
        while attr.len() % 4 != 0 {
            attr.push(0);
        }
        let mut message = Vec::new();
        message.extend_from_slice(&BINDING_SUCCESS.to_be_bytes());
        message.extend_from_slice(&u16::try_from(attr.len()).expect("small").to_be_bytes());
        message.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
        message.extend_from_slice(&transaction_id);
        message.extend_from_slice(&attr);
        message
    }

    /// `203.0.113.7:41234`, XOR-masked per RFC 5389 §15.2.
    fn xor_ipv4_value() -> Vec<u8> {
        let mask = MAGIC_COOKIE.to_be_bytes();
        let addr = [203u8, 0, 113, 7];
        let port = 0xA112u16 ^ u16::try_from(MAGIC_COOKIE >> 16).expect("fits"); // 41234
        let mut value = vec![0, FAMILY_IPV4];
        value.extend_from_slice(&port.to_be_bytes());
        value.extend(addr.iter().zip(mask).map(|(byte, mask)| byte ^ mask));
        value
    }

    #[test]
    fn request_is_a_well_formed_binding_request() {
        let request = binding_request(TXID);
        assert_eq!(request.message.len(), HEADER_LEN);
        assert_eq!(
            u16::from_be_bytes([request.message[0], request.message[1]]),
            0x0001
        );
        assert_eq!(
            u16::from_be_bytes([request.message[2], request.message[3]]),
            0
        );
        assert_eq!(&request.message[4..8], &MAGIC_COOKIE.to_be_bytes());
        assert_eq!(&request.message[8..], &TXID);
    }

    #[test]
    fn xor_mapped_ipv4_is_unmasked() {
        let message = response(ATTR_XOR_MAPPED_ADDRESS, &xor_ipv4_value(), TXID);
        let addr = parse_binding_response(&message, &TXID).expect("parse");
        assert_eq!(
            addr,
            "203.0.113.7:41234".parse::<SocketAddr>().expect("addr")
        );
    }

    #[test]
    fn plain_mapped_address_is_accepted_as_a_fallback() {
        // Older servers answer with the unXORed attribute; still usable.
        let mut value = vec![0, FAMILY_IPV4];
        value.extend_from_slice(&41234u16.to_be_bytes());
        value.extend_from_slice(&[203, 0, 113, 7]);
        let message = response(ATTR_MAPPED_ADDRESS, &value, TXID);
        let addr = parse_binding_response(&message, &TXID).expect("parse");
        assert_eq!(
            addr,
            "203.0.113.7:41234".parse::<SocketAddr>().expect("addr")
        );
    }

    #[test]
    fn xor_mapped_ipv6_masks_with_the_transaction_id() {
        let mut mask = [0u8; 16];
        mask[..4].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
        mask[4..].copy_from_slice(&TXID);
        let addr: std::net::Ipv6Addr = "2001:db8::1".parse().expect("addr");
        let port = 0x1388u16 ^ u16::try_from(MAGIC_COOKIE >> 16).expect("fits"); // 5000
        let mut value = vec![0, FAMILY_IPV6];
        value.extend_from_slice(&port.to_be_bytes());
        value.extend(
            addr.octets()
                .iter()
                .zip(mask)
                .map(|(byte, mask)| byte ^ mask),
        );
        let message = response(ATTR_XOR_MAPPED_ADDRESS, &value, TXID);
        let parsed = parse_binding_response(&message, &TXID).expect("parse");
        assert_eq!(parsed, SocketAddr::new(addr.into(), 5000));
    }

    #[test]
    fn a_reply_for_another_transaction_is_rejected() {
        // Two handshakes can share a socket; matching on the transaction id is
        // what keeps one from stealing the other's answer.
        let message = response(
            ATTR_XOR_MAPPED_ADDRESS,
            &xor_ipv4_value(),
            [1u8; TRANSACTION_ID_LEN],
        );
        assert!(parse_binding_response(&message, &TXID).is_err());
    }

    #[test]
    fn malformed_responses_are_rejected() {
        assert!(parse_binding_response(&[], &TXID).is_err(), "empty");
        assert!(
            parse_binding_response(&[0u8; HEADER_LEN], &TXID).is_err(),
            "not a success response"
        );

        // Right shape, wrong cookie.
        let mut bad_cookie = response(ATTR_XOR_MAPPED_ADDRESS, &xor_ipv4_value(), TXID);
        bad_cookie[4] ^= 0xFF;
        assert!(
            parse_binding_response(&bad_cookie, &TXID).is_err(),
            "cookie"
        );

        // A success response with no address attribute at all.
        let empty = response(0x8022, b"fofoca", TXID);
        assert!(parse_binding_response(&empty, &TXID).is_err(), "no address");
    }

    #[test]
    fn a_truncated_body_is_rejected_not_panicked_on() {
        let full = response(ATTR_XOR_MAPPED_ADDRESS, &xor_ipv4_value(), TXID);
        for len in 0..full.len() {
            // A hostile or damaged server must produce an error, never a panic.
            let _ = parse_binding_response(&full[..len], &TXID);
        }
    }
}
