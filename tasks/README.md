# tasks

The workspace task runner. Run `cargo task <task>` from anywhere in the
workspace.

Two jobs:

- The quality gate. Everything `.github/workflows/ci.yml` runs lives in
  `src/gate.rs` as data (`STEPS`), so you can run the gate locally with
  `cargo task ci` and narrow it to one crate with `-p`.
- `cargo task e2e`. The browser tests of the WebRTC crate need a long
  preamble, and each step fails without naming its cause. The runner does
  the preamble and names the causes.

If the gate and the workflow disagree, the workflow wins: CI is what
gates the branch.
