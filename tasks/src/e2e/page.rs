//! One browser tab behind whichever driver reaches it, and the one way the
//! runner can wait on a page's promise: neither backend can await a JS
//! promise directly, so a call parks its outcome on `window` and the runner
//! polls for it.

#![cfg_attr(
    not(feature = "mesh"),
    allow(
        dead_code,
        reason = "the benchmark drives Chrome over CDP only; the WebDriver arm, the URL helper and the default-timeout call are the mesh and chat suites' — one module either way, so the two features do not each carry a copy"
    )
)]

use std::time::Duration;

use crate::util::wait_for;

use super::{cdp, webdriver};

/// How long a page call may take to settle before it is reported as never
/// having done so.
const CALL_TIMEOUT: Duration = Duration::from_secs(20);

/// One browser on a page, behind whichever driver reaches it.
pub(crate) enum Page {
    Cdp(cdp::Browser),
    WebDriver(webdriver::Session),
}

impl Page {
    pub(crate) fn navigate(&self, url: &str) {
        match self {
            Self::Cdp(browser) => browser.navigate(url),
            Self::WebDriver(session) => session.navigate(url),
        }
    }

    /// Evaluate a JS expression (no `return`), reading its value as a string.
    pub(crate) fn evaluate(&self, expression: &str) -> String {
        match self {
            Self::Cdp(browser) => browser.evaluate(expression),
            Self::WebDriver(session) => session.execute(&format!("return ({expression});")),
        }
    }

    pub(crate) fn version(&self) -> String {
        match self {
            Self::Cdp(browser) => browser.version(),
            Self::WebDriver(session) => session.version(),
        }
    }
}

/// A call on a page object that has started but not settled: the token
/// [`await_call`] reads the outcome from.
pub(crate) struct Started(String);

/// Start `window.{object}.{call}` without waiting for it.
///
/// Split from [`await_call`] because two pages sometimes have to be in flight
/// at once — a JSEP answerer waits on `ondatachannel`, which only fires once
/// the offerer applies the answer, so awaiting either side alone deadlocks.
/// `Promise.resolve` around the call lets a plain function ride the same path
/// as a promise-returning one.
pub(crate) fn start_call(page: &Page, object: &str, call: &str) -> Result<Started, String> {
    let token = format!("call{}", rand_token());
    let expression = format!(
        "window.{token}='pending',Promise.resolve().then(()=>window.{object}.{call}).then((v)=>window.{token}='ok:'+(v===undefined?'':String(v)),(e)=>window.{token}='error: '+e),'started'"
    );
    let started = page.evaluate(&expression);
    if started != "started" {
        return Err(format!("{object} call {call} did not start: {started:?}"));
    }
    Ok(Started(token))
}

/// Wait for a started call, handing back what it resolved to.
pub(crate) fn await_call(
    page: &Page,
    started: &Started,
    timeout: Duration,
) -> Result<String, String> {
    let Started(token) = started;
    let outcome = wait_for(timeout, Duration::from_millis(250), || {
        let state = page.evaluate(&format!("window.{token}"));
        (state != "pending").then_some(state)
    })
    .ok_or_else(|| format!("page call {token} never settled"))?;
    outcome
        .strip_prefix("ok:")
        .map(str::to_owned)
        .ok_or_else(|| format!("page call failed: {outcome}"))
}

/// A page object's promise-returning controls (`window.harness`,
/// `window.chat`, `window.bench`), awaited via a completion flag the driver
/// polls. Resolves to the call's value, rendered as a string.
pub(crate) fn call_page(page: &Page, object: &str, call: &str) -> Result<String, String> {
    call_page_within(page, object, call, CALL_TIMEOUT)
}

/// [`call_page`] with a caller-chosen `timeout`.
pub(crate) fn call_page_within(
    page: &Page,
    object: &str,
    call: &str,
    timeout: Duration,
) -> Result<String, String> {
    let started = start_call(page, object, call)?;
    await_call(page, &started, timeout).map_err(|error| format!("{object} call {call}: {error}"))
}

/// Wait for the page's `#ready` or a non-empty `#failed`, handing back the
/// failure's text. `None` if neither shows up within `timeout`.
pub(crate) fn wait_ready(
    page: &Page,
    timeout: Duration,
    poll: Duration,
) -> Option<Result<(), String>> {
    wait_for(timeout, poll, || {
        let failed = page.evaluate("(document.getElementById('failed')||{}).textContent||''");
        if !failed.is_empty() {
            return Some(Err(failed));
        }
        let ready = page.evaluate("document.getElementById('ready')?'1':'0'");
        (ready == "1").then_some(Ok(()))
    })
}

pub(crate) fn rand_token() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.subsec_nanos().into())
        .unwrap_or_default()
}

pub(crate) fn urlencode(raw: &str) -> String {
    raw.replace('%', "%25")
        .replace('&', "%26")
        .replace('+', "%2B")
        .replace('#', "%23")
}
