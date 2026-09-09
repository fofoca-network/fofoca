//! A terminal chat over the byte pipe — the chat example's native half
//! (`examples/chat/` holds the browser half and the README).
//!
//! ```text
//! cargo run -p fofoca-pipe --example chat -- --topic room
//! cargo run -p fofoca-pipe --example chat -- --topic room --nick ana --relay-url http://127.0.0.1:3340/
//! ```
//!
//! Type to broadcast; `/msg <nick> <text>` sends directed; `/peers` prints
//! the roster; `/quit` leaves. `--robot` swaps the prose for one JSON object
//! per line — the engine's own events pass through verbatim, frames become
//! `{"kind":"frame",...}` — which is what `cargo task e2e --suite chat`
//! reads. Tracing goes to stderr either way, so stdout *is* the chat.

use anyhow::{Context as _, Result, bail};
use fofoca_pipe::{Inbound, Opts, Request, data_body, data_tag, depart, join, json_sink, parse_to};
use tokio::sync::oneshot;

struct Args {
    opts: Opts,
    robot: bool,
}

fn parse_args() -> Result<Args> {
    let mut opts = Opts::default();
    let mut robot = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().with_context(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--topic" => opts.topic = Some(value("--topic")?),
            "--mesh" => opts.mesh = Some(value("--mesh")?),
            "--nick" => opts.nick = Some(value("--nick")?),
            // `--relay-url`, not `--relay`: the bare name reads as the boolean
            // lookup switch, which is the confusion `relay_lookup` was named
            // apart to end. Spelled as the bun-ffi client spells it.
            "--relay-url" => opts.relay_urls.push(value("--relay-url")?),
            "--relay-transport" => opts.relay_transport = true,
            "--robot" => robot = true,
            other => bail!(
                "unknown argument {other}\nusage: chat --topic <t> | --mesh <id> \
                 [--nick <n>] [--relay-url <url>]... [--relay-transport] [--robot]"
            ),
        }
    }
    Ok(Args { opts, robot })
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // Stderr, so stdout stays the chat surface (and the robot protocol).
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "fofoca=warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = parse_args()?;
    let (sink, mut events) = json_sink();
    let mut session = join(&args.opts, sink).await?;

    // Stdin on its own thread: the pipe owns no stdio, and tokio's own stdin
    // wants the `io-std` feature this crate deliberately leaves off.
    let (lines_tx, mut lines) = tokio::sync::mpsc::unbounded_channel::<String>();
    std::thread::spawn(move || {
        use std::io::BufRead as _;
        for line in std::io::stdin().lock().lines() {
            let Ok(line) = line else { break };
            if lines_tx.send(line).is_err() {
                break;
            }
        }
    });

    loop {
        tokio::select! {
            maybe = lines.recv() => {
                let Some(line) = maybe else { break };
                if !handle_line(&session, line.trim(), args.robot).await? {
                    break;
                }
            }
            maybe = session.inbound.recv() => {
                let Some(frame) = maybe else { break };
                show_frame(&frame, args.robot);
            }
            maybe = events.recv() => {
                let Some(event) = maybe else { break };
                show_event(&event, args.robot);
            }
        }
    }
    depart(session.node).await
}

/// One line of input. `false` ends the chat.
async fn handle_line(session: &fofoca_pipe::Session, line: &str, robot: bool) -> Result<bool> {
    if line.is_empty() {
        return Ok(true);
    }
    if line == "/quit" {
        return Ok(false);
    }
    if line == "/peers" {
        let roster = request(session, |reply| Request::Peers { reply }).await?;
        println!("{roster}");
        return Ok(true);
    }
    let (to, text) = if let Some(rest) = line.strip_prefix("/msg ") {
        let Some((nick, text)) = rest.split_once(' ') else {
            report_error(robot, "usage: /msg <nick> <text>");
            return Ok(true);
        };
        (Some(nick), text)
    } else {
        (None, line)
    };
    let to = parse_to(to)?;
    let body = data_body(text.as_bytes())?;
    let sent = request(session, |reply| Request::Send {
        tag: data_tag(),
        to,
        body,
        reply,
    })
    .await?;
    if let Err(refusal) = sent {
        report_error(robot, &format!("send failed: {refusal}"));
    }
    Ok(true)
}

async fn request<T>(
    session: &fofoca_pipe::Session,
    build: impl FnOnce(oneshot::Sender<T>) -> Request,
) -> Result<T> {
    session
        .request(build)
        .await
        .map_err(|error| anyhow::anyhow!(error))
}

fn show_frame(frame: &Inbound, robot: bool) {
    if robot {
        println!(
            "{}",
            serde_json::json!({
                "kind": "frame",
                "from": frame.nick,
                "text": String::from_utf8_lossy(&frame.bytes),
                "directed": frame.directed,
                "eof": frame.eof,
            })
        );
        return;
    }
    if frame.eof {
        return;
    }
    let mark = if frame.directed { " (direct)" } else { "" };
    println!(
        "{}{mark}: {}",
        frame.nick,
        String::from_utf8_lossy(&frame.bytes)
    );
}

/// Engine events arrive as JSON already; the robot passes them through and
/// the human reading gets the two that matter as prose.
fn show_event(event: &str, robot: bool) {
    if robot {
        println!("{event}");
        return;
    }
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(event) else {
        return;
    };
    match (
        parsed.get("kind").and_then(serde_json::Value::as_str),
        parsed.get("nick").and_then(serde_json::Value::as_str),
    ) {
        (Some("ready"), _) => println!("* connected — type to chat, /quit to leave"),
        (Some(kind @ ("joined" | "left" | "returned")), Some(nick)) => {
            println!("* {nick} {kind}");
        }
        _ => {}
    }
}

fn report_error(robot: bool, message: &str) {
    if robot {
        println!(
            "{}",
            serde_json::json!({ "kind": "error", "message": message })
        );
    } else {
        println!("! {message}");
    }
}
