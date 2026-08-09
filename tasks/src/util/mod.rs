//! Shared helpers for the tasks in this runner.

pub(crate) mod output;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// The workspace root — `tasks/..`, resolved from the manifest directory
/// rather than the current one, so `cargo task` behaves the same wherever it is
/// invoked from inside the workspace.
pub(crate) fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the tasks crate always sits directly under the workspace root")
        .to_path_buf()
}

/// Poll `probe` until it yields a value, or give up.
///
/// Used for everything that comes up asynchronously — the harness server's
/// socket, a driver's `/status`, the page's `#matrix-done` — so each of them
/// gets a bounded wait instead of an unbounded one.
pub(crate) fn wait_for<T>(
    timeout: Duration,
    interval: Duration,
    mut probe: impl FnMut() -> Option<T>,
) -> Option<T> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = probe() {
            return Some(value);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(interval);
    }
}
