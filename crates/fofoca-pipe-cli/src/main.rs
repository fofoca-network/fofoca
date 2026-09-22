//! `fofoca-pipe`: stdin to a gossip mesh, the mesh to stdout.
//!
//! ```text
//! cat lorem.txt | fofoca-pipe --web-url http://127.0.0.1:3020/
//! fofoca-pipe --mesh <id> > received.txt
//! ```
//!
//! Stdout is the byte stream and nothing else: every word this binary has to
//! say — the mesh id, the URL to open, who joined — goes to stderr, as prose
//! or (`--robot`) as one JSON object per line.
//!
//! Two modes, decided by what stdin is. A pipe or a file is **send** mode:
//! bytes go out in numbered frames, an EOF marker follows, and the process
//! leaves once the marker is out. A terminal is **receive** mode: stdin is not
//! read, and the process leaves once the first remote stream completes.
//! `--stay` keeps either open until interrupted.
//!
//! Frames are not stored by the mesh, so send mode waits for a peer that can
//! receive before it reads stdin (`--no-wait` skips that). "Can receive"
//! means the roster shows a payload lane, not merely that a peer joined: a
//! tab is in the gossip overlay long before its data channel is up, and
//! frames sent in that window are gone.

mod args;

use std::io::{IsTerminal as _, Read as _, Write as _};

use anyhow::{Context as _, Result};
use clap::Parser as _;
use fofoca_pipe::{
    Delivered, Request, Session, StreamSeq, Streams, data_body, data_tag, default_chunk, depart,
    eof_body, eof_tag, join, json_sink,
};
use tokio::sync::mpsc;

use crate::args::Args;

/// What the stdin thread hands over.
enum Chunk {
    Data(Vec<u8>),
    Eof,
}

/// How the main loop ends.
enum Exit {
    Done,
    Interrupted,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "fofoca=warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    let (sink, mut events) = json_sink();
    let session = join(&args.opts(), sink).await?;
    let mesh_id = session.node.mesh_id().to_string();
    let url = args.page_url(&mesh_id);
    report_open(&args, &session, &mesh_id, url.as_deref());

    let send_mode = !std::io::stdin().is_terminal();
    let mut stdin: Option<mpsc::Receiver<Chunk>> = None;
    let mut waiting = send_mode && !args.no_wait;
    if send_mode && args.no_wait {
        stdin = Some(spawn_stdin());
    } else if waiting {
        note(&args, "waiting for a peer");
    }
    let mut roster_tick = tokio::time::interval(ROSTER_POLL);
    let mut gap_tick = tokio::time::interval(GAP_POLL);

    let mut seq = StreamSeq::default();
    let mut streams = Streams::default();
    let mut session = session;
    let exit = loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break Exit::Interrupted,
            chunk = next_chunk(&mut stdin) => {
                let Some(chunk) = chunk else { break Exit::Done };
                match chunk {
                    Chunk::Data(bytes) => send_data(&session, &mut seq, &bytes).await?,
                    Chunk::Eof => {
                        send_eof(&session, &seq).await?;
                        stdin = None;
                        if !args.stay {
                            break Exit::Done;
                        }
                    }
                }
            }
            maybe = session.inbound.recv() => {
                let Some(frame) = maybe else { break Exit::Done };
                let complete = deliver(&streams.push(frame))?;
                if complete && !send_mode && !args.stay {
                    break Exit::Done;
                }
            }
            _ = gap_tick.tick() => {
                let mut complete = false;
                for released in streams.expire() {
                    complete |= deliver(&released)?;
                }
                if complete && !send_mode && !args.stay {
                    break Exit::Done;
                }
            }
            maybe = events.recv() => {
                let Some(event) = maybe else { break Exit::Done };
                show_event(&args, &event);
            }
            _ = roster_tick.tick(), if waiting => {
                let roster = session
                    .request(|reply| Request::Peers { reply })
                    .await
                    .map_err(|error| anyhow::anyhow!(error))?;
                if let Some(nick) = payload_ready_peer(&roster) {
                    note(&args, &format!("{nick} can receive; streaming stdin"));
                    stdin = Some(spawn_stdin());
                    waiting = false;
                }
            }
        }
    };

    depart(session.node).await?;
    if matches!(exit, Exit::Interrupted) {
        std::process::exit(130);
    }
    Ok(())
}

/// Stdin on its own thread: the pipe owns no stdio, and tokio's `io-std`
/// stays off. A small bounded channel, so a slow mesh holds stdin back
/// instead of the process holding the whole input.
fn spawn_stdin() -> mpsc::Receiver<Chunk> {
    let (tx, rx) = mpsc::channel(4);
    std::thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        let mut buf = vec![0_u8; default_chunk()];
        loop {
            let chunk = match stdin.read(&mut buf) {
                Ok(0) | Err(_) => Chunk::Eof,
                Ok(len) => Chunk::Data(buf[..len].to_vec()),
            };
            let last = matches!(chunk, Chunk::Eof);
            if tx.blocking_send(chunk).is_err() || last {
                break;
            }
        }
    });
    rx
}

/// The next stdin chunk, or never while stdin is not being read.
async fn next_chunk(stdin: &mut Option<mpsc::Receiver<Chunk>>) -> Option<Chunk> {
    match stdin {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

async fn send_data(session: &Session, seq: &mut StreamSeq, bytes: &[u8]) -> Result<()> {
    session.flow.wait_for_window(&None).await;
    let body = data_body(seq.next(&None), bytes)?;
    request_send(session, data_tag(), body).await
}

async fn send_eof(session: &Session, seq: &StreamSeq) -> Result<()> {
    request_send(session, eof_tag(), eof_body(seq.count(&None))?).await
}

/// A refused frame is a hole in the stream, so it ends the run rather than
/// being skipped over.
async fn request_send(
    session: &Session,
    tag: fofoca::protocol::AppTag,
    body: fofoca::protocol::MessageBody,
) -> Result<()> {
    session
        .request(|reply| Request::Send {
            tag,
            to: None,
            body,
            reply,
        })
        .await
        .map_err(|error| anyhow::anyhow!(error))?
        .map_err(|refusal| anyhow::anyhow!("send failed: {refusal}"))
}

/// Write what a frame (or an expired hole) released to stdout, in order.
/// `true` when it completed its stream.
fn deliver(delivered: &Delivered) -> Result<bool> {
    if !delivered.chunks.is_empty() {
        let mut stdout = std::io::stdout().lock();
        for chunk in &delivered.chunks {
            stdout.write_all(chunk).context("writing to stdout")?;
        }
        stdout.flush().context("flushing stdout")?;
    }
    Ok(delivered.complete)
}

/// How often the roster is asked whether a peer can receive yet.
const ROSTER_POLL: std::time::Duration = std::time::Duration::from_millis(500);
/// How often open holes in the received streams are checked against
/// `fofoca_pipe::GAP_TIMEOUT`.
const GAP_POLL: std::time::Duration = std::time::Duration::from_millis(500);

/// The first peer in a roster snapshot with a proven payload lane, by
/// nickname. Only `unicast` counts: `multihop` is the engine's guess before a
/// direct path is proven, `relay-only` is a peer whose only path is a
/// lookup-only relay, and `unreachable` says what it says. A frame sent to a
/// tab before its lane is up sits in a queue until it is — or dies with this
/// process if it leaves first.
fn payload_ready_peer(roster_json: &str) -> Option<String> {
    let roster: serde_json::Value = serde_json::from_str(roster_json).ok()?;
    roster
        .get("peers")?
        .as_array()?
        .iter()
        .find(|peer| peer.get("transport").and_then(serde_json::Value::as_str) == Some("unicast"))
        .and_then(|peer| peer.get("nickname").and_then(serde_json::Value::as_str))
        .map(str::to_owned)
}

fn report_open(args: &Args, session: &Session, mesh_id: &str, url: Option<&str>) {
    if args.robot {
        eprintln!(
            "{}",
            serde_json::json!({
                "kind": "open",
                "mesh": mesh_id,
                "nick": session.node.nickname().to_string(),
                "url": url,
            })
        );
        return;
    }
    eprintln!("mesh {mesh_id}");
    match url {
        Some(url) => eprintln!("open {url}"),
        None => eprintln!("open the web page with #mesh={mesh_id} (or pass --web-url)"),
    }
}

fn note(args: &Args, text: &str) {
    if args.robot {
        eprintln!("{}", serde_json::json!({ "kind": "note", "text": text }));
    } else {
        eprintln!("* {text}");
    }
}

/// Engine events arrive as JSON already; the robot gets them verbatim and a
/// person gets the ones that matter as prose.
fn show_event(args: &Args, event: &str) {
    if args.robot {
        eprintln!("{event}");
        return;
    }
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(event) else {
        return;
    };
    match (
        parsed.get("kind").and_then(serde_json::Value::as_str),
        parsed.get("nick").and_then(serde_json::Value::as_str),
    ) {
        (Some(kind @ ("joined" | "left" | "returned")), Some(nick)) => eprintln!("* {nick} {kind}"),
        (Some("error"), _) => {
            if let Some(text) = parsed.get("text").and_then(serde_json::Value::as_str) {
                eprintln!("! {text}");
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::payload_ready_peer;

    #[test]
    fn a_joined_peer_on_the_relay_alone_cannot_receive_yet() {
        let relay_only = r#"{"count":2,"peers":[{"nickname":"tab","transport":"relay-only"}]}"#;
        assert_eq!(payload_ready_peer(relay_only), None);
        let unreachable = r#"{"count":2,"peers":[{"nickname":"tab","transport":"unreachable"}]}"#;
        assert_eq!(payload_ready_peer(unreachable), None);
        let guessed = r#"{"count":2,"peers":[{"nickname":"tab","transport":"multihop"}]}"#;
        assert_eq!(payload_ready_peer(guessed), None);
        assert_eq!(payload_ready_peer(r#"{"count":1,"peers":[]}"#), None);
        assert_eq!(payload_ready_peer("not json"), None);
    }

    #[test]
    fn a_peer_with_a_proven_lane_can() {
        let roster = r#"{"count":3,"peers":[
            {"nickname":"far","transport":"relay-only"},
            {"nickname":"hop","transport":"multihop"},
            {"nickname":"tab","transport":"unicast"}]}"#;
        assert_eq!(payload_ready_peer(roster).as_deref(), Some("tab"));
    }
}
