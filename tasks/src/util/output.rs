//! Cargo-style status output.
//!
//! A matrix sweep is minutes of mostly-silence punctuated by builds and browser
//! launches, sandwiched between the cargo invocations that produced it. Making
//! it read the same way — a right-aligned green verb, then the subject — is the
//! difference between a runner that looks like part of the toolchain and one
//! that looks like a shell script someone left running.

use std::io::Write as _;

use anstream::stdout;
use anstyle::{AnsiColor, Color, Style};

const HEADER: Style = Style::new()
    .bold()
    .fg_color(Some(Color::Ansi(AnsiColor::Green)));
const FAILURE: Style = Style::new()
    .bold()
    .fg_color(Some(Color::Ansi(AnsiColor::Red)));
const CAUTION: Style = Style::new()
    .bold()
    .fg_color(Some(Color::Ansi(AnsiColor::Yellow)));
const MUTED: Style = Style::new().dimmed();

/// `    Verb subject` — the shape `cargo build` prints.
pub(crate) fn status(verb: &str, subject: &str) {
    emit(HEADER, verb, subject);
}

/// The same, in red, for a cell that failed rather than a step that ran.
pub(crate) fn failure(verb: &str, subject: &str) {
    emit(FAILURE, verb, subject);
}

/// The same, in yellow, for a cell that was skipped — never silent, because a
/// matrix that quietly ran half its cells and printed "all green" is worse
/// than no matrix.
pub(crate) fn caution(verb: &str, subject: &str) {
    emit(CAUTION, verb, subject);
}

/// Secondary detail under a status line: an artifact path, a driver's excuse.
pub(crate) fn detail(text: &str) {
    line(&format!("{MUTED}{text}{MUTED:#}"));
}

/// Verbatim, unstyled — the results table the page produced, which owns its own
/// column layout and must not be re-wrapped or re-coloured on the way out.
pub(crate) fn verbatim(text: &str) {
    line(text);
}

fn emit(style: Style, verb: &str, subject: &str) {
    line(&format!("{style}{verb:>12}{style:#} {subject}"));
}

/// Every line is flushed as it is written.
///
/// A sweep is minutes of waiting on builds and browsers, and stdout is only
/// line-buffered when it is a terminal — pipe it into a file or a log and the
/// whole run goes silent until it exits. That is indistinguishable from a hang,
/// on exactly the kind of long run someone is most likely to redirect.
fn line(text: &str) {
    let mut out = stdout().lock();
    let _ = writeln!(out, "{text}");
    let _ = out.flush();
}
