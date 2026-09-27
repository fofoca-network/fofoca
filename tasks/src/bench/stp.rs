//! The Safari Technology Preview backend: `safaridriver --mcp`, spoken over
//! its stdin and stdout as line-delimited JSON-RPC.
//!
//! Not the W3C `WebDriver` backend, because on macOS 27 classic `safaridriver`
//! never gets a session: for both Safari and STP, with remote automation
//! allowed and `safaridriver --enable` run, `POST /session` times out at
//! "Request creation of a new automation session". The MCP mode of the same
//! binary drives STP on the same machine. Each [`Browser`] is its own driver
//! process with its own tab. Whether two of them share state is untested: no
//! cell opens two, and `evaluate_javascript` takes no tab handle.

use std::io::{BufRead as _, BufReader, Write as _};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

use crate::util::{Skip, json_to_string, wait_for};

const DRIVER: &str = "/Applications/Safari Technology Preview.app/Contents/MacOS/safaridriver";

/// One tool call is one page action; a stalled STP must fail the cell rather
/// than hang the runner.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// One STP tab behind its own `safaridriver --mcp`, stopped when dropped.
pub(crate) struct Browser {
    child: Child,
    stdin: Mutex<ChildStdin>,
    replies: Mutex<mpsc::Receiver<serde_json::Value>>,
    next_id: AtomicU64,
}

impl Drop for Browser {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Browser {
    pub(crate) fn launch() -> Result<Self, Skip> {
        if !std::path::Path::new(DRIVER).is_file() {
            return Err(Skip(
                "Safari Technology Preview is not installed".to_owned(),
            ));
        }
        let mut child = Command::new(DRIVER)
            .arg("--mcp")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| Skip(format!("could not start safaridriver --mcp: {error}")))?;
        let stdin = child.stdin.take().expect("stdin was piped");
        let stdout = child.stdout.take().expect("stdout was piped");

        // A reader thread, so a reply that never comes is a timeout on the
        // channel rather than a `read_line` that blocks forever.
        let (sender, replies) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                let Ok(message) = serde_json::from_str::<serde_json::Value>(&line) else {
                    continue;
                };
                if sender.send(message).is_err() {
                    break;
                }
            }
        });

        let browser = Self {
            child,
            stdin: Mutex::new(stdin),
            replies: Mutex::new(replies),
            next_id: AtomicU64::new(0),
        };
        let unreachable = |error: String| {
            Skip(format!(
                "safaridriver --mcp did not answer ({error}) — STP needs Settings ▸ Developer ▸ Allow remote automation"
            ))
        };
        browser
            .request(
                "initialize",
                &serde_json::json!({
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": { "name": "cargo-task", "version": "0" },
                }),
            )
            .map_err(unreachable)?;
        browser
            .send(&serde_json::json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))
            .map_err(unreachable)?;
        // STP ends the previous cell's automation session in the background,
        // and a tab asked for while it does fails with "The remote session was
        // terminated". Asking again once it has finished succeeds. The 20 s
        // bounds those fast refusals; a stalled driver fails one try only after
        // `CALL_TIMEOUT`.
        let mut refused = String::new();
        wait_for(Duration::from_secs(20), Duration::from_secs(1), || {
            browser
                .tool("create_tab", &serde_json::json!({ "url": "about:blank" }))
                .map_err(|error| refused = error)
                .ok()
        })
        .ok_or_else(|| unreachable(refused))?;
        Ok(browser)
    }

    fn send(&self, message: &serde_json::Value) -> Result<(), String> {
        let mut stdin = self.stdin.lock().expect("stdin mutex poisoned");
        writeln!(stdin, "{message}")
            .and_then(|()| stdin.flush())
            .map_err(|error| format!("driver stdin: {error}"))
    }

    fn request(
        &self,
        method: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.send(
            &serde_json::json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }),
        )?;
        let replies = self.replies.lock().expect("reply mutex poisoned");
        let deadline = Instant::now() + CALL_TIMEOUT;
        loop {
            let reply = replies
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .map_err(|error| match error {
                    RecvTimeoutError::Timeout => {
                        format!("{method} got no reply within {CALL_TIMEOUT:?}")
                    }
                    RecvTimeoutError::Disconnected => format!("{method}: the driver exited"),
                })?;
            // A request from the server can reuse our id; only a reply lacks `method`.
            if reply.get("method").is_some()
                || reply.get("id").and_then(serde_json::Value::as_u64) != Some(id)
            {
                continue;
            }
            if let Some(error) = reply.get("error") {
                return Err(format!("{method} failed: {error}"));
            }
            return Ok(reply.get("result").cloned().unwrap_or_default());
        }
    }

    /// One MCP tool call, flattened to its first text block.
    fn tool(&self, name: &str, arguments: &serde_json::Value) -> Result<String, String> {
        let result = self.request(
            "tools/call",
            &serde_json::json!({ "name": name, "arguments": arguments }),
        )?;
        let text = result
            .pointer("/content/0/text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if result.get("isError").and_then(serde_json::Value::as_bool) == Some(true) {
            return Err(format!("{name}: {text}"));
        }
        Ok(text)
    }

    pub(crate) fn navigate(&self, url: &str) {
        let _ = self.tool("navigate_to_url", &serde_json::json!({ "url": url }));
    }

    /// Evaluate a JS expression (no `return`), reading its value as a string.
    pub(crate) fn evaluate(&self, expression: &str) -> String {
        self.tool(
            "evaluate_javascript",
            &serde_json::json!({ "expression": format!("return ({expression});") }),
        )
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
        .map(|value| json_to_string(&value))
        .unwrap_or_default()
    }

    /// The version is read from the page. The name comes from [`DRIVER`]: the
    /// user agent does not tell STP from Safari.
    pub(crate) fn version(&self) -> String {
        let agent = self.evaluate("navigator.userAgent");
        if agent.is_empty() {
            return "unknown".to_owned();
        }
        agent
            .split_whitespace()
            .find_map(|part| part.strip_prefix("Version/"))
            .map_or(agent.clone(), |version| {
                format!("Safari Technology Preview {version}")
            })
    }
}
