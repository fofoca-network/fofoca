//! `fofoca-stream`: stdin to one reader, or a stream's bytes to stdout.
//!
//! ```text
//! cat lorem.txt | fofoca-stream --web-url http://127.0.0.1:3020/
//! fofoca-stream <hash> > received.txt
//! ```
//!
//! Stdout is the byte stream and nothing else: every word this binary has to
//! say — the hash, the URL to open — goes to stderr, as prose or (`--robot`)
//! as one JSON object per line.
//!
//! With a hash it reads that stream to stdout and exits at its end. Without
//! one, stdin must be a pipe or a file: it creates a stream, prints its hash,
//! waits for the one reader, streams stdin to it, and ends the stream at EOF.
//! The bytes ride a direct path, never gossip, and the reader paces the
//! writer.

mod args;

use std::io::{IsTerminal as _, Read as _, Write as _};

use anyhow::{Context as _, Result, bail};
use clap::Parser as _;
use fofoca_stream::{Producer, StreamHash, StreamNode};
use tokio::sync::mpsc;

use crate::args::Args;

/// How much of stdin one write carries.
const CHUNK: usize = 64 * 1024;

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
    tokio::select! {
        outcome = run(&args) => outcome,
        _ = tokio::signal::ctrl_c() => std::process::exit(130),
    }
}

async fn run(args: &Args) -> Result<()> {
    if let Some(hash) = &args.hash {
        return read(args, &hash.parse()?).await;
    }
    if std::io::stdin().is_terminal() {
        bail!("pipe data in to stream it, or pass a hash to read one");
    }
    produce(args).await
}

/// Read the stream behind `hash` to stdout.
async fn read(args: &Args, hash: &StreamHash) -> Result<()> {
    let node = StreamNode::bind_for(hash).await?;
    let mut reader = node.open(hash).await?;
    note(args, "reading");
    let mut stdout = std::io::stdout().lock();
    while let Some(chunk) = reader.read().await? {
        stdout.write_all(&chunk).context("writing to stdout")?;
        stdout.flush().context("flushing stdout")?;
    }
    drop(stdout);
    note(args, "end of stream");
    node.close().await;
    Ok(())
}

/// Stream stdin to the one reader of a new stream.
async fn produce(args: &Args) -> Result<()> {
    let node = StreamNode::bind(&args.opts()).await?;
    let mut producer = node.create().await;
    report_ready(args, producer.hash());
    producer.attached().await?;
    note(args, "a reader attached; streaming stdin");
    pump(&mut producer, spawn_stdin()).await?;
    producer.close().await?;
    note(args, "stream closed");
    node.close().await;
    Ok(())
}

async fn pump(
    producer: &mut Producer,
    mut stdin: mpsc::Receiver<std::io::Result<Vec<u8>>>,
) -> Result<()> {
    while let Some(chunk) = stdin.recv().await {
        producer.write(&chunk.context("reading stdin")?).await?;
    }
    Ok(())
}

/// Stdin on its own thread, since tokio's `io-std` stays off. A small bounded
/// channel, so a slow reader holds stdin back instead of the process holding
/// the whole input.
fn spawn_stdin() -> mpsc::Receiver<std::io::Result<Vec<u8>>> {
    let (tx, rx) = mpsc::channel(4);
    std::thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        let mut buf = vec![0_u8; CHUNK];
        loop {
            let chunk = match stdin.read(&mut buf) {
                Ok(0) => break,
                Ok(len) => Ok(buf[..len].to_vec()),
                Err(error) => Err(error),
            };
            let failed = chunk.is_err();
            if tx.blocking_send(chunk).is_err() || failed {
                break;
            }
        }
    });
    rx
}

fn report_ready(args: &Args, hash: &StreamHash) {
    let hash = hash.encode();
    let url = args.page_url(&hash);
    if args.robot {
        eprintln!(
            "{}",
            serde_json::json!({ "kind": "ready", "hash": hash, "url": url })
        );
        return;
    }
    eprintln!("hash {hash}");
    match url {
        Some(url) => eprintln!("open {url}"),
        None => eprintln!("read it with: fofoca-stream {hash}"),
    }
    eprintln!("* waiting for a reader");
}

fn note(args: &Args, text: &str) {
    if args.robot {
        eprintln!("{}", serde_json::json!({ "kind": "note", "text": text }));
    } else {
        eprintln!("* {text}");
    }
}
