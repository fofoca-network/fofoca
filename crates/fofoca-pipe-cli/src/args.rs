//! The command line, and what it resolves to: the pipe's `Opts` plus the
//! switches that are this binary's own.

use clap::Parser;
use fofoca_pipe::Opts;

/// Pipe bytes through a fofoca mesh: stdin goes out, what peers send comes to
/// stdout.
#[derive(Debug, Parser)]
#[command(name = "fofoca-pipe", version)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "command-line switches: each is an independent flag, not a state to model as an enum"
)]
pub(crate) struct Args {
    /// A shared string every member derives the same public mesh from.
    #[arg(long, conflicts_with = "mesh")]
    pub topic: Option<String>,
    /// A mesh id to join.
    #[arg(long)]
    pub mesh: Option<String>,
    /// This peer's nickname; minted when absent.
    #[arg(long)]
    pub nick: Option<String>,
    /// A custom relay ladder, first preferred. Part of a topic's mesh id, so
    /// every member must pass the same list.
    #[arg(long = "relay-url", value_name = "URL")]
    pub relay_urls: Vec<String>,
    /// Let payload fall back to the relay. Part of the mesh id.
    #[arg(long)]
    pub relay_transport: bool,
    /// Create a public mesh (mDNS, DHT and the relay ladder). The default
    /// when no selector and no other discovery switch is given.
    #[arg(long)]
    pub public: bool,
    /// Create a mesh discoverable over mDNS.
    #[arg(long)]
    pub mdns: bool,
    /// Create a mesh discoverable over the mainline DHT.
    #[arg(long)]
    pub dht: bool,
    /// The web page's address. Printed with the mesh in its fragment as the
    /// URL to open.
    #[arg(long, env = "FOFOCA_PIPE_WEB", value_name = "URL")]
    pub web_url: Option<String>,
    /// Read stdin at once instead of after the first peer joins. Frames are
    /// not stored, so a peer that joins later misses what went before.
    #[arg(long)]
    pub no_wait: bool,
    /// Keep the pipe open after stdin ends, or after the first remote stream
    /// completes, until interrupted.
    #[arg(long)]
    pub stay: bool,
    /// One JSON object per line on stderr instead of prose.
    #[arg(long)]
    pub robot: bool,
}

impl Args {
    /// The pipe's options. With no selector and no discovery switch the mesh
    /// is public: a bare `fofoca-pipe` must be reachable by the tab it prints
    /// the URL for, and a loopback mesh is not.
    pub(crate) fn opts(&self) -> Opts {
        let selected = self.topic.is_some() || self.mesh.is_some();
        let discovery = self.mdns || self.dht || !self.relay_urls.is_empty();
        Opts {
            mesh: self.mesh.clone(),
            topic: self.topic.clone(),
            nick: self.nick.clone(),
            public: self.public || (!selected && !discovery),
            mdns: self.mdns,
            dht: self.dht,
            relay_transport: self.relay_transport,
            relay_urls: self.relay_urls.clone(),
            ..Opts::default()
        }
    }

    /// The page URL for this mesh, when a web address is known. The selector
    /// rides in the fragment, which a browser never sends to the server.
    pub(crate) fn page_url(&self, mesh_id: &str) -> Option<String> {
        let base = self.web_url.as_deref()?;
        let fragment = match &self.topic {
            Some(topic) => format!("topic={}", percent_encode(topic)),
            None => format!("mesh={mesh_id}"),
        };
        Some(format!("{base}#{fragment}"))
    }
}

/// Percent-encode everything outside RFC 3986's unreserved set, so a topic
/// with spaces or `&` survives `URLSearchParams` on the other side.
fn percent_encode(text: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            out.push(char::from(byte));
        } else {
            // Writing to a String cannot fail.
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Args {
        Args::try_parse_from(std::iter::once("fofoca-pipe").chain(args.iter().copied()))
            .expect("valid args")
    }

    #[test]
    fn a_bare_invocation_creates_a_public_mesh() {
        let opts = parse(&[]).opts();
        assert!(opts.public);
        assert!(opts.mesh.is_none() && opts.topic.is_none());
    }

    #[test]
    fn a_selector_or_a_discovery_switch_turns_the_default_off() {
        assert!(!parse(&["--mesh", "abc"]).opts().public);
        assert!(!parse(&["--topic", "room"]).opts().public);
        assert!(
            !parse(&["--relay-url", "http://127.0.0.1:3340/"])
                .opts()
                .public
        );
        assert!(!parse(&["--mdns"]).opts().public);
        assert!(parse(&["--mdns", "--public"]).opts().public);
    }

    #[test]
    fn mesh_and_topic_are_mutually_exclusive() {
        assert!(Args::try_parse_from(["fofoca-pipe", "--mesh", "abc", "--topic", "room"]).is_err());
    }

    #[test]
    fn the_page_url_carries_the_selector_in_the_fragment() {
        let args = parse(&["--web-url", "http://127.0.0.1:3020/"]);
        assert_eq!(
            args.page_url("abc").as_deref(),
            Some("http://127.0.0.1:3020/#mesh=abc")
        );
        let topical = parse(&["--web-url", "https://x.test/", "--topic", "tea time&more"]);
        assert_eq!(
            topical.page_url("ignored").as_deref(),
            Some("https://x.test/#topic=tea%20time%26more")
        );
        assert_eq!(parse(&[]).page_url("abc"), None);
    }
}
