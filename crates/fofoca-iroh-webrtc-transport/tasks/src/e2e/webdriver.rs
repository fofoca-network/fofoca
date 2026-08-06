//! The `WebDriver` backend, for every browser CDP cannot reach.
//!
//! A minimal W3C client — open a page, poll for an element, read it. That is
//! all the harness signal needs, and far less than keeping a driver-matched
//! `wasm-bindgen-test-runner` run working costs.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use crate::util::wait_for;

use super::{Harvest, Skip, server};

/// A free port, taken fresh for each driver.
///
/// This used to be a fixed 9615, and the fixed port let someone else's driver
/// answer for ours. A run killed with SIGKILL leaves its driver behind — the
/// `Drop` guard below never runs — and the next run's readiness probe cannot
/// tell that corpse from the process it just spawned. A cell duly came back
/// reporting a browser it had never launched, which is only visible at all
/// because the version is read back from whatever answered rather than taken
/// from the cell's name.
///
/// Binding to port 0 and immediately releasing leaves a window where something
/// else could take it. That window is microseconds and the alternative is a
/// collision that lasts as long as the stale process does.
fn free_port() -> Result<u16, Skip> {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .map_err(|error| {
            Skip(format!(
                "could not find a free port for the driver: {error}"
            ))
        })
}

/// A running driver, killed when it goes out of scope.
#[derive(Debug)]
pub(super) struct Driver {
    child: Child,
    url: String,
}

impl Drop for Driver {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `$CHROMEDRIVER` before `$PATH`, so a driver downloaded next to the run works
/// without installing anything system-wide — the same courtesy the wasm-bindgen
/// runner extends via the same variable.
fn locate(env_var: &str, binary: &str) -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(env_var).map(PathBuf::from)
        && path.is_file()
    {
        return Some(path);
    }
    let found = Command::new("which").arg(binary).output().ok()?;
    found
        .status
        .success()
        .then(|| PathBuf::from(String::from_utf8_lossy(&found.stdout).trim().to_owned()))
}

impl Driver {
    pub(super) fn start(browser: &str) -> Result<Self, Skip> {
        let port = free_port()?;
        let mut command = match browser {
            "chrome-151" => {
                // A chromedriver whose version matches the Chrome it drives. A
                // mismatch fails session creation with a bare HTTP 404 and no
                // hint that the version is what is wrong.
                let driver = locate("CHROMEDRIVER", "chromedriver").ok_or_else(|| {
                    Skip(
                        "no chromedriver for Chrome 151 — set $CHROMEDRIVER to a matching build"
                            .to_owned(),
                    )
                })?;
                let mut command = Command::new(driver);
                command.arg(format!("--port={port}"));
                command
            }
            "safari" => {
                let driver = PathBuf::from("/usr/bin/safaridriver");
                if !driver.is_file() {
                    return Err(Skip("safaridriver is missing".to_owned()));
                }
                let mut command = Command::new(driver);
                command.args(["-p", &port.to_string()]);
                command
            }
            other => return Err(Skip(format!("no driver wired up for {other}"))),
        };

        let child = command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| Skip(format!("could not start the {browser} driver: {error}")))?;
        let mut driver = Self {
            child,
            url: format!("http://127.0.0.1:{port}"),
        };

        // Give up the moment the child dies, rather than polling a port it was
        // never going to open. Combined with the fresh port above, a `/status`
        // that answers is now necessarily *this* driver's.
        let status_url = format!("{}/status", driver.url);
        let ready = wait_for(Duration::from_secs(10), Duration::from_millis(250), || {
            if matches!(driver.child.try_wait(), Ok(Some(_))) {
                return Some(false);
            }
            server::reachable(&status_url).then_some(true)
        });
        match ready {
            Some(true) => Ok(driver),
            Some(false) => Err(Skip(format!(
                "the {browser} driver exited before it was ready"
            ))),
            None => Err(Skip(format!("the {browser} driver never became ready"))),
        }
    }

    fn post(&self, path: &str, body: &serde_json::Value) -> Result<serde_json::Value, String> {
        ureq::post(format!("{}{path}", self.url))
            .config()
            .timeout_global(Some(Duration::from_mins(1)))
            .build()
            .send_json(body)
            .map_err(|error| error.to_string())?
            .body_mut()
            .read_json::<serde_json::Value>()
            .map_err(|error| error.to_string())
    }
}

/// Every browser needs its own way of being told to stop hiding local IPs
/// behind mDNS; without it the only host candidate is a `.local` name a
/// headless browser cannot resolve, and ICE never leaves `new`. Real tabs are
/// unaffected — this is a property of the harness, not of the transport.
fn capabilities(browser: &str, binary: &str) -> serde_json::Value {
    match browser {
        "chrome-151" => serde_json::json!({ "capabilities": { "alwaysMatch": {
            "goog:chromeOptions": {
                "binary": binary,
                "args": [
                    "--headless=new",
                    "--disable-gpu",
                    "--disable-features=WebRtcHideLocalIpsWithMdns",
                ],
            },
        }}}),
        // Safari has no such switch and no headless mode either, so it runs
        // visibly and pairs on whatever it is willing to gather.
        _ => serde_json::json!({ "capabilities": { "alwaysMatch": {} } }),
    }
}

/// Run one cell and harvest what the page published.
pub(super) fn run(
    driver: &Driver,
    browser: &str,
    binary: &str,
    pressure: u32,
    timeout: Duration,
) -> Result<Harvest, Skip> {
    let created = driver
        .post("/session", &capabilities(browser, binary))
        .map_err(|error| session_refused(browser, &error))?;

    let session = created
        .pointer("/value/sessionId")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            let detail = created
                .pointer("/value/message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("no message");
            session_refused(browser, detail)
        })?;

    // Same reason the CDP path reads `Browser.getVersion`: the label is our
    // intent, this is what answered.
    let capability = |key: &str| {
        created
            .pointer(&format!("/value/capabilities/{key}"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?")
            .to_owned()
    };
    let version = format!(
        "{}/{}",
        capability("browserName"),
        capability("browserVersion")
    );

    let execute = |script: &str| -> String {
        driver
            .post(
                &format!("/session/{session}/execute/sync"),
                &serde_json::json!({ "script": script, "args": [] }),
            )
            .ok()
            .and_then(|reply| reply.get("value").map(super::json_to_string))
            .unwrap_or_default()
    };

    let url = format!("{}/?pressure={pressure}", server::URL);
    let _ = driver.post(
        &format!("/session/{session}/url"),
        &serde_json::json!({ "url": url }),
    );

    let published = wait_for(timeout, Duration::from_secs(3), || {
        (execute(super::DONE_EXPRESSION) == "1").then_some(())
    })
    .is_some();

    let harvest = Harvest {
        table: execute(super::TABLE_SCRIPT),
        failed: super::normalise_failed(&execute(super::FAILED_SCRIPT)),
        version,
        published,
    };

    let _ = ureq::delete(format!("{}/session/{session}", driver.url))
        .config()
        .timeout_global(Some(Duration::from_secs(30)))
        .build()
        .call();

    Ok(harvest)
}

/// A refusal and a hang want different fixes, so they get different messages —
/// and Safari's is neither a driver bug nor something a script can resolve.
fn session_refused(browser: &str, detail: &str) -> Skip {
    if browser == "safari" {
        return Skip(
            "safaridriver would not start a session — Safari needs Develop ▸ Allow Remote \
             Automation, a one-time GUI toggle no script can set"
                .to_owned(),
        );
    }
    Skip(format!("no session from the {browser} driver: {detail}"))
}
