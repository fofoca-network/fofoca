//! Prerequisite checks and the wasm build.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::TaskOutcome;
use crate::util::{output, repo_root};

use super::Profile;

/// Environment the wasm build needs, or an error naming what is missing.
///
/// `ring` compiles its C core for wasm32 and Apple clang has no wasm backend.
/// Without Homebrew's, the link silently leaves 44 `ring_core_*` symbols as
/// imports from a module called `env`, the browser refuses the module with
/// `Failed to resolve module specifier "env"`, and the test runner reports only
/// `Failed to detect test as having been run`. Four layers, none of which
/// mentions clang. `examples/chat-webrtc/build-wasm.sh` says the same thing for
/// the same reason.
pub(super) fn wasm_env() -> Result<BTreeMap<String, String>, String> {
    let clang = [
        "/opt/homebrew/opt/llvm/bin/clang",
        "/usr/local/opt/llvm/bin/clang",
    ]
    .into_iter()
    .map(Path::new)
    .find(|candidate| candidate.is_file());

    let Some(clang) = clang else {
        if cfg!(target_os = "macos") {
            return Err(concat!(
                "no Homebrew LLVM clang found, and Apple clang cannot emit wasm\n",
                "             brew install llvm\n",
                "             (without it `ring`'s C core does not link and the browser\n",
                "              refuses the module with 'Failed to resolve module specifier \"env\"')"
            )
            .to_owned());
        }
        // Every other platform ships a clang that can target wasm32.
        return Ok(BTreeMap::new());
    };

    let bin = clang.parent().expect("a clang path always has a directory");
    Ok(BTreeMap::from([
        (
            "CC_wasm32_unknown_unknown".to_owned(),
            clang.display().to_string(),
        ),
        (
            "AR_wasm32_unknown_unknown".to_owned(),
            bin.join("llvm-ar").display().to_string(),
        ),
    ]))
}

/// The tools the sweep shells out to.
pub(super) fn check_tooling() -> TaskOutcome {
    if Command::new("wasm-bindgen-test-runner")
        .arg("--version")
        .output()
        .is_err()
    {
        return Err(
            "wasm-bindgen-test-runner is not on PATH\n             cargo install wasm-bindgen-cli"
                .into(),
        );
    }
    let installed = Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .map_err(|error| format!("could not ask rustup for installed targets: {error}"))?;
    if !String::from_utf8_lossy(&installed.stdout)
        .lines()
        .any(|line| line.trim() == "wasm32-unknown-unknown")
    {
        return Err(
            "the wasm32-unknown-unknown target is not installed\n             rustup target add wasm32-unknown-unknown"
                .into(),
        );
    }
    Ok(())
}

/// Build one test target and hand back the `.wasm` cargo actually produced.
///
/// The path comes from cargo's own JSON rather than a glob of `deps/`: a stale
/// artefact from an earlier build is indistinguishable from a fresh one by
/// filename, and running the wrong binary is the kind of mistake that costs a
/// whole investigation rather than a run.
///
/// `--release` is not a flag anyone can turn off. The debug build of these
/// tests is ~137 MB of wasm and the browser gives up loading it, with the same
/// unhelpful message as every other failure on this path.
pub(super) fn build(
    test: &str,
    profile: Profile,
    env: &BTreeMap<String, String>,
) -> Result<PathBuf, String> {
    let mut command = Command::new("cargo");
    command
        .current_dir(repo_root())
        .args([
            "test",
            "--release",
            "--no-run",
            "--quiet",
            "--target",
            "wasm32-unknown-unknown",
            "-p",
            "fofoca-iroh-webrtc-transport",
            "--features",
            "web",
            "--test",
            test,
            "--message-format=json",
        ])
        .envs(env);

    if profile == Profile::ReleaseSlow {
        // Models the consumer's development build: a slower wasm holds the
        // shared JS task queue for longer, which is the outbound pump's
        // starvation hypothesis by another route. Not the actual debug build —
        // that one will not load, which is a harness limit and not a finding.
        command.env("CARGO_PROFILE_RELEASE_OPT_LEVEL", "1");
    }

    let built = command
        .output()
        .map_err(|error| format!("could not run cargo: {error}"))?;

    // Last wins: cargo emits one `compiler-artifact` per crate in the graph and
    // the test target is the last to link.
    let artifact = String::from_utf8_lossy(&built.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter_map(|message| {
            message
                .get("executable")
                .and_then(serde_json::Value::as_str)
                .map(PathBuf::from)
        })
        .next_back();

    match artifact {
        Some(path) if path.is_file() => Ok(path),
        _ => Err(format!(
            "could not build the {test} wasm for profile '{profile}':\n{}",
            String::from_utf8_lossy(&built.stderr).trim()
        )),
    }
}

/// Print the artifact that a cell is about to run, so a run that used a stale
/// or unexpected build says so in its own log.
pub(super) fn announce(profile: Profile, artifact: &Path) {
    output::status("Building", &format!("{profile} wasm"));
    output::detail(&format!("             {}", artifact.display()));
}
