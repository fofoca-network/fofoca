//! Task runner for `fofoca-iroh-webrtc-transport`. Run `cargo task <task>`
//! from inside the crate.
//!
//! It exists for one reason: the crate's browser tests need a preamble that is
//! four things long, and every one of them fails in a way that does not name
//! its cause. Homebrew clang or `ring`'s C core silently does not link;
//! `--release` or the wasm is too big to load; a matching driver or session
//! creation 404s; the browser's mDNS switch or ICE never leaves `new`. A runner
//! that gets all four right is the difference between a suite anyone can run
//! and one only its author can.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod e2e;
mod util;

/// Task result; any `Err` is printed and turns into a non-zero exit.
pub(crate) type TaskOutcome = Result<(), Box<dyn std::error::Error>>;

#[derive(Parser)]
#[command(bin_name = "cargo task", about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    task: Task,
}

/// Variant doc comments *are* the `--help` text — no separate usage block to
/// drift out of step with the code.
#[derive(Subcommand)]
enum Task {
    /// Drive the browser tests against real browsers.
    ///
    /// The default `matrix` suite sweeps browser × build profile × main-thread
    /// pressure and prints one summary table; `--suite loopback` runs the fast
    /// four-test regression suite against a single browser instead.
    E2e(e2e::Args),
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let outcome = match cli.task {
        Task::E2e(args) => e2e::run(&args),
    };
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            util::output::failure("error", &error.to_string());
            ExitCode::FAILURE
        }
    }
}
