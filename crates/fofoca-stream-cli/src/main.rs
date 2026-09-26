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

use std::io::{IsTerminal as _, Write as _};

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

    run(&Args::parse()).await
}

/// The node outlives the stream work, and is closed after it however it
/// ended, Ctrl-C included: closing is what gets a dropped stream's
/// `ABANDONED` to the peer. Exiting straight from the signal lost that race
/// about one time in five, and the peer then waited out the idle timeout.
async fn run(args: &Args) -> Result<()> {
    let (node, outcome) = if let Some(hash) = &args.hash {
        let hash: StreamHash = hash.parse()?;
        let node = StreamNode::bind_for(&hash).await?;
        let outcome = until_interrupted(read(args, &node, &hash)).await;
        (node, outcome)
    } else {
        if std::io::stdin().is_terminal() {
            bail!("pipe data in to stream it, or pass a hash to read one");
        }
        let node = StreamNode::bind(&args.opts()).await?;
        let outcome = until_interrupted(produce(args, &node)).await;
        (node, outcome)
    };
    if outcome.is_none() {
        note(args, "interrupted");
    }
    // A second Ctrl-C during the close means leave now, flush or not.
    tokio::select! {
        () = node.close() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    outcome.unwrap_or_else(|| std::process::exit(130))
}

/// `work`'s outcome, or `None` at Ctrl-C. Either way `work` is dropped by the
/// time this returns.
async fn until_interrupted(work: impl Future<Output = Result<()>>) -> Option<Result<()>> {
    tokio::select! {
        outcome = work => Some(outcome),
        _ = tokio::signal::ctrl_c() => None,
    }
}

/// Read the stream behind `hash` to stdout.
async fn read(args: &Args, node: &StreamNode, hash: &StreamHash) -> Result<()> {
    let mut reader = node.open(hash).await?;
    note(args, "reading");
    let mut stdout = std::io::stdout().lock();
    while let Some(chunk) = reader.read().await? {
        stdout.write_all(&chunk).context("writing to stdout")?;
        stdout.flush().context("flushing stdout")?;
    }
    drop(stdout);
    note(args, "end of stream");
    Ok(())
}

/// Stream stdin to the one reader of a new stream.
async fn produce(args: &Args, node: &StreamNode) -> Result<()> {
    let mut producer = node.create().await;
    report_ready(args, producer.hash());
    producer.attached().await?;
    note(args, "a reader attached; streaming stdin");
    pump(&mut producer, spawn_stdin()).await?;
    producer.close().await?;
    note(args, "stream closed");
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
    std::thread::spawn(move || forward(std::io::stdin().lock(), &tx));
    rx
}

/// Send `input` down `tx` in chunks until its end, or its first error. An
/// `Interrupted` read is retried, not sent.
fn forward(mut input: impl std::io::Read, tx: &mpsc::Sender<std::io::Result<Vec<u8>>>) {
    let mut buf = vec![0_u8; CHUNK];
    loop {
        let chunk = match input.read(&mut buf) {
            Ok(0) => break,
            Ok(len) => Ok(buf[..len].to_vec()),
            // A signal landed mid-read; nothing was lost, so read again.
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => Err(error),
        };
        let failed = chunk.is_err();
        if tx.blocking_send(chunk).is_err() || failed {
            break;
        }
    }
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

#[cfg(test)]
mod tests {
    use std::io::{Error, ErrorKind, Read};

    use tokio::sync::mpsc;

    /// Returns `Interrupted` once, then `data`, then the end.
    struct InterruptedOnce {
        interrupted: bool,
        data: &'static [u8],
    }

    impl Read for InterruptedOnce {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if !self.interrupted {
                self.interrupted = true;
                return Err(Error::from(ErrorKind::Interrupted));
            }
            let len = self.data.len().min(buf.len());
            buf[..len].copy_from_slice(&self.data[..len]);
            self.data = &self.data[len..];
            Ok(len)
        }
    }

    /// `Interrupted` means "try again" (a signal landed mid-read), not a
    /// failed input: the stream must carry on, not end in an error.
    #[test]
    fn an_interrupted_read_is_retried() {
        let (tx, mut rx) = mpsc::channel(4);
        let input = InterruptedOnce {
            interrupted: false,
            data: b"after the signal",
        };
        std::thread::spawn(move || super::forward(input, &tx));
        let mut got = Vec::new();
        while let Some(chunk) = rx.blocking_recv() {
            got.extend(chunk.expect("an interrupted read is not an error"));
        }
        assert_eq!(got, b"after the signal");
    }

    /// Any other failure is sent once and ends the input: the producer then
    /// fails, and never closes the stream as if the input had ended.
    #[test]
    fn a_failed_read_ends_the_stream_as_an_error() {
        struct Failing;
        impl Read for Failing {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(Error::other("the disk went away"))
            }
        }
        let (tx, mut rx) = mpsc::channel(4);
        std::thread::spawn(move || super::forward(Failing, &tx));
        let first = rx.blocking_recv().expect("the error is sent");
        assert_eq!(
            first.expect_err("a failed read is an error").to_string(),
            "the disk went away"
        );
        assert!(rx.blocking_recv().is_none(), "nothing follows the error");
    }
}
