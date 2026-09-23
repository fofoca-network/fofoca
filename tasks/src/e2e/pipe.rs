//! `cargo task e2e --suite pipe` — the pipe end to end: the real
//! `fofoca-pipe` binary and the real pipe web page, meeting on a mesh the
//! CLI mints over a local relay.
//!
//! The chat suite proves lines both ways; this proves *bytes in order*: a
//! payload wider than one frame, with a multibyte character straddling a
//! frame boundary, goes down the CLI's stdin and must come out of the page's
//! `<pre>` byte-exact; text sent from the page must land on the CLI's stdout
//! the same way. The page is opened at the URL the CLI printed, so the
//! fragment contract is on the line too.

use std::fmt::Write as _;
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use crate::TaskOutcome;
use crate::util::output;
use crate::util::{repo_root, wait_for};

use super::mesh::{BunServer, Page, call_page, launch_page, urlencode};
use super::{Args, Skip, build};

type Failure = Box<dyn std::error::Error>;

/// Beacon claim + JSEP + graft on a cold pair; generous like the mesh
/// suite's link budget, for the same reason.
const LINK_TIMEOUT: Duration = Duration::from_secs(150);
/// One payload hop on a linked pair.
const PAYLOAD_TIMEOUT: Duration = Duration::from_secs(20);
/// How long the tab's console is recorded: past the page's one-minute open
/// and the link wait, so a failure at either still has the whole console.
const CONSOLE_WINDOW: Duration =
    Duration::from_secs(60 + LINK_TIMEOUT.as_secs() + PAYLOAD_TIMEOUT.as_secs());
/// The CLI's departure grace plus the process winding down.
const EXIT_TIMEOUT: Duration = Duration::from_secs(10);

// ── the terminal half ───────────────────────────────────────────────────

/// The CLI as the suite drives it: stdin held open, stdout collected as
/// bytes (it is the received stream, not lines), stderr collected as robot
/// JSON where it parses and as the engine log where it does not.
struct Cli {
    child: Child,
    /// `None` once closed — closing stdin is the EOF the scenario sends.
    stdin: Option<ChildStdin>,
    stdout: mpsc::Receiver<Vec<u8>>,
    stderr_lines: mpsc::Receiver<String>,
    received: Vec<u8>,
    seen: Vec<serde_json::Value>,
    engine_log: String,
}

impl Drop for Cli {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Cli {
    fn spawn(binary: &PathBuf, relay_url: &str, web_url: &str) -> Result<Self, Skip> {
        let mut child = Command::new(binary)
            .args([
                "--nick",
                "terminal",
                "--lookup",
                "relay",
                "--relay-url",
                relay_url,
                "--web-url",
                web_url,
                "--robot",
            ])
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or_else(|_| "fofoca=info".to_owned()),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| Skip(format!("could not start fofoca-pipe: {error}")))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Skip("fofoca-pipe has no stdin pipe".to_owned()))?;
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| Skip("fofoca-pipe has no stdout pipe".to_owned()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| Skip("fofoca-pipe has no stderr pipe".to_owned()))?;
        let (bytes_tx, bytes) = mpsc::channel();
        std::thread::spawn(move || {
            let mut buf = [0_u8; 4096];
            while let Ok(len) = stdout.read(&mut buf)
                && len > 0
                && bytes_tx.send(buf[..len].to_vec()).is_ok()
            {}
        });
        let (stderr_tx, stderr_lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if stderr_tx.send(line).is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            child,
            stdin: Some(stdin),
            stdout: bytes,
            stderr_lines,
            received: Vec::new(),
            seen: Vec::new(),
            engine_log: String::new(),
        })
    }

    fn pump(&mut self) {
        while let Ok(chunk) = self.stdout.try_recv() {
            self.received.extend_from_slice(&chunk);
        }
        while let Ok(line) = self.stderr_lines.try_recv() {
            if let Ok(value) = serde_json::from_str(&line) {
                self.seen.push(value);
            } else {
                self.engine_log.push_str(&line);
                self.engine_log.push('\n');
            }
        }
    }

    fn saw(&mut self, pred: impl Fn(&serde_json::Value) -> bool) -> Option<serde_json::Value> {
        self.pump();
        self.seen.iter().find(|value| pred(value)).cloned()
    }

    fn saw_kind(&mut self, kind: &str) -> Option<serde_json::Value> {
        self.saw(|value| value.get("kind").and_then(serde_json::Value::as_str) == Some(kind))
    }

    fn engine_log_contains(&mut self, needle: &str) -> bool {
        self.pump();
        self.engine_log.contains(needle)
    }

    fn received(&mut self) -> &[u8] {
        self.pump();
        &self.received
    }

    fn write_stdin(&mut self, bytes: &[u8]) -> Result<(), String> {
        let stdin = self
            .stdin
            .as_mut()
            .ok_or("fofoca-pipe stdin already closed")?;
        stdin
            .write_all(bytes)
            .and_then(|()| stdin.flush())
            .map_err(|error| format!("fofoca-pipe stdin write failed: {error}"))
    }

    fn close_stdin(&mut self) {
        self.stdin = None;
    }

    fn exit_code(&mut self) -> Option<i32> {
        self.child
            .try_wait()
            .ok()
            .flatten()
            .and_then(|status| status.code())
    }

    fn transcript(&mut self) -> String {
        self.pump();
        let lines: Vec<String> = self.seen.iter().map(ToString::to_string).collect();
        format!(
            "\u{2500}\u{2500} cli robot lines \u{2500}\u{2500}\n{}\n\u{2500}\u{2500} cli stdout ({} bytes) \u{2500}\u{2500}\n{}\n\u{2500}\u{2500} cli engine log \u{2500}\u{2500}\n{}",
            lines.join("\n"),
            self.received.len(),
            String::from_utf8_lossy(&self.received),
            self.engine_log
        )
    }
}

// ── driving the page ────────────────────────────────────────────────────

fn page_text(page: &Page, id: &str) -> String {
    page.evaluate(&format!(
        "(document.getElementById('{id}')||{{}}).textContent||''"
    ))
}

fn stream_text(page: &Page, from: &str) -> String {
    page.evaluate(&format!(
        "(document.querySelector('#streams pre[data-from=\"{from}\"][data-self=\"false\"]')||{{}}).textContent||''"
    ))
}

fn stream_complete(page: &Page, from: &str) -> bool {
    page.evaluate(&format!(
        "(document.querySelector('#streams pre[data-from=\"{from}\"][data-self=\"false\"]')||{{dataset:{{}}}}).dataset.complete||''"
    )) == "true"
}

/// A payload wider than the send window, with a four-byte character placed
/// to straddle the first frame boundary, and no trailing newline — the shape
/// that catches a per-chunk decode, a lost last chunk, or a sender that does
/// not wait for its receiver's acks.
fn payload() -> String {
    let chunk = fofoca_pipe::default_chunk();
    let mut text = String::new();
    let mut line = 0;
    while text.len() < chunk - 2 {
        let _ = writeln!(
            text,
            "line {line:04} the quick brown fox jumps over the lazy dog"
        );
        line += 1;
    }
    text.truncate(chunk - 2);
    text.push('\u{1F30A}');
    while text.len() < chunk * 2 + 100 {
        let _ = writeln!(
            text,
            "line {line:04} pack my box with five dozen liquor jugs"
        );
        line += 1;
    }
    text.push_str("the end");
    text
}

// ── the scenario ────────────────────────────────────────────────────────

pub(super) fn run(args: &Args) -> TaskOutcome {
    if args.list {
        output::detail("one scenario: `fofoca-pipe` \u{2194} the pipe page on a minted mesh");
        return Ok(());
    }
    build::ensure_bun("the pipe suite serves its page with bun")?;
    build::build_browser_peer()?;
    output::status("Building", "fofoca-pipe");
    let cli_binary = build::build_binary("fofoca-pipe-cli", "fofoca-pipe")?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("no tokio runtime: {error}"))?;
    let (relay_url, _relay_server) = runtime
        .block_on(fofoca::net::test_relay::spawn_plain())
        .map_err(|error| format!("no local relay: {error:#}"))?;
    let server = BunServer::serve(
        &repo_root().join("packages/fofoca-pipe-web"),
        "serve.ts",
        "the pipe page",
    )
    .map_err(|Skip(reason)| reason)?;
    output::status(
        "Serving",
        &format!("relay {relay_url} · pipe {}", server.url),
    );

    let web_url = format!("{}/", server.url);
    let mut cli =
        Cli::spawn(&cli_binary, relay_url.as_str(), &web_url).map_err(|Skip(reason)| reason)?;
    let (page, sent) = link(&mut cli, &web_url, relay_url.as_str())?;
    exchange(&mut cli, &page, &sent)?;
    output::status(
        "ok",
        "fofoca-pipe \u{2194} the pipe page  linked, streamed both ways, exited clean",
    );
    Ok(())
}

/// Open the page at the URL the CLI printed and wait until the pair is
/// linked for payload.
fn link(cli: &mut Cli, web_url: &str, relay_url: &str) -> Result<(Page, String), Failure> {
    // The URL the CLI printed is the one a person would open.
    let Some(open) = wait_for(Duration::from_mins(1), Duration::from_millis(500), || {
        cli.saw_kind("open")
    }) else {
        return fail(cli, None, "the cli never reported its mesh");
    };
    let Some(url) = open.get("url").and_then(serde_json::Value::as_str) else {
        return fail(cli, None, "the cli reported no url");
    };
    if !url.starts_with(&format!("{web_url}#mesh=")) {
        return fail(cli, None, &format!("unexpected url {url}"));
    }

    // The stagger the mesh suite proved out: the terminal claims the beacon
    // before the tab even opens, so the pair never races the claim.
    if wait_for(Duration::from_mins(1), Duration::from_millis(500), || {
        cli.engine_log_contains("beacon role active").then_some(())
    })
    .is_none()
    {
        return fail(cli, None, "the terminal never claimed the beacon");
    }

    let page = launch_page("cft").map_err(|Skip(reason)| Failure::from(reason))?;
    // The relay and the nickname ride the query; the selector stays in the
    // fragment exactly as printed.
    let (base, fragment) = url.split_once('#').unwrap_or((url, ""));
    page.navigate_watching_console(
        &format!(
            "{base}?nick=browser&relay={}&log=fofoca=info#{fragment}",
            urlencode(relay_url),
        ),
        CONSOLE_WINDOW,
    );
    output::status("Running", "fofoca-pipe \u{2194} the pipe page");

    if wait_for(Duration::from_mins(1), Duration::from_millis(500), || {
        (!page_text(&page, "ready").is_empty()).then_some(())
    })
    .is_none()
    {
        let reason = format!(
            "the page never opened its mesh: {}",
            page_text(&page, "failed")
        );
        return fail(cli, Some(&page), &reason);
    }

    // The payload goes down stdin *now*, with the tab open but not yet
    // linked — the way a person pipes. Holding it back until the link would
    // hide the one thing the CLI must get right: it, not the caller, waits
    // for a peer that can receive.
    let sent = payload();
    cli.write_stdin(sent.as_bytes())?;

    // Linked: the page's roster shows the terminal on the unicast lane, and
    // the CLI saw the tab join.
    let linked = wait_for(LINK_TIMEOUT, Duration::from_secs(1), || {
        (page_text(&page, "peers").contains("\"unicast\"") && cli.saw_kind("joined").is_some())
            .then_some(())
    });
    if linked.is_none() {
        let reason = format!(
            "the pair never linked for payload (page peers: {})",
            page_text(&page, "peers")
        );
        return fail(cli, Some(&page), &reason);
    }

    Ok((page, sent))
}

/// Bytes both ways on the linked pair, then a clean end. `sent` is already
/// on the CLI's stdin.
fn exchange(cli: &mut Cli, page: &Page, sent: &str) -> Result<(), Failure> {
    // Terminal → page: the whole payload, byte-exact and in order.
    let landed = wait_for(PAYLOAD_TIMEOUT, Duration::from_millis(500), || {
        (stream_text(page, "terminal") == sent).then_some(())
    });
    if landed.is_none() {
        let got = stream_text(page, "terminal");
        let reason = format!(
            "the payload did not land whole on the page ({} of {} bytes, complete={})",
            got.len(),
            sent.len(),
            stream_complete(page, "terminal")
        );
        return fail(cli, Some(page), &reason);
    }
    if stream_complete(page, "terminal") {
        return fail(cli, Some(page), "the stream read complete before its eof");
    }

    // Page → terminal, through the same runtime the WebMCP tools call.
    call_page(page, "pipe", "send('hello-from-web')")
        .or_else(|error| fail(cli, Some(page), &error))?;
    call_page(page, "pipe", "sendEof()").or_else(|error| fail(cli, Some(page), &error))?;
    // The tab shows its own send, whoever asked for it.
    let own = page.evaluate(
        "(document.querySelector('#streams pre[data-self=\"true\"]')||{}).textContent||''",
    );
    if own != "hello-from-web" {
        let reason = format!("the tab did not show its own send (got {own:?})");
        return fail(cli, Some(page), &reason);
    }
    let echoed = wait_for(PAYLOAD_TIMEOUT, Duration::from_millis(500), || {
        (cli.received() == b"hello-from-web").then_some(())
    });
    if echoed.is_none() {
        let reason = format!(
            "the page's text did not land on stdout (got {:?})",
            String::from_utf8_lossy(cli.received())
        );
        return fail(cli, Some(page), &reason);
    }

    // `pipe_read` semantics on the tab: the payload, as one ordered item,
    // and a second read from the same cursor-free call sees it again — a
    // read takes nothing away.
    call_page(
        page,
        "pipe",
        "read({ waitMs: 0 }).then(r => { window.__read = JSON.stringify(r) })",
    )
    .or_else(|error| fail(cli, Some(page), &error))?;
    let read = page.evaluate("window.__read||''");
    let parsed: serde_json::Value = serde_json::from_str(&read)
        .map_err(|error| format!("pipe.read returned no JSON ({error}): {read}"))?;
    let items = parsed
        .get("items")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let first = items
        .first()
        .and_then(|item| item.get("text"))
        .and_then(serde_json::Value::as_str);
    if items.len() != 1 || first != Some(sent) {
        let reason = format!(
            "pipe.read did not return the payload as one item ({} items, first {} bytes)",
            items.len(),
            first.map_or(0, str::len)
        );
        return fail(cli, Some(page), &reason);
    }

    // EOF: closing stdin ends the stream on the page and the CLI exits clean.
    cli.close_stdin();
    let completed = wait_for(PAYLOAD_TIMEOUT, Duration::from_millis(500), || {
        stream_complete(page, "terminal").then_some(())
    });
    if completed.is_none() {
        return fail(cli, Some(page), "the page never saw the eof");
    }
    let exited = wait_for(EXIT_TIMEOUT, Duration::from_millis(250), || cli.exit_code());
    if exited != Some(0) {
        let reason = format!("the cli did not exit 0 after eof (exit {exited:?})");
        return fail(cli, Some(page), &reason);
    }

    let _ = call_page(page, "pipe", "close()");
    Ok(())
}

/// Dump both surfaces to a file and fail the suite.
fn fail<T>(cli: &mut Cli, page: Option<&Page>, reason: &str) -> Result<T, Failure> {
    // The page first, the console last among them: the console blocks until
    // its window closes, and the native transcript is read after it so both
    // logs cover the same stretch of time.
    let mut browser = String::new();
    if let Some(page) = page {
        for id in ["status", "webmcp", "streams", "peers", "events"] {
            let _ = write!(
                browser,
                "\n\u{2500}\u{2500} browser {id} \u{2500}\u{2500}\n{}\n",
                page_text(page, id)
            );
        }
        let _ = write!(
            browser,
            "\n\u{2500}\u{2500} browser console \u{2500}\u{2500}\n{}\n",
            page.console()
        );
    }
    let dump = cli.transcript() + &browser;
    let path = repo_root().join("target/pipe-e2e.log");
    let _ = std::fs::write(&path, dump);
    output::detail(&format!("full log: {}", path.display()));
    Err(format!("pipe e2e failed: {reason}").into())
}
