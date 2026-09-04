# chat · rust

The native Rust half of the chat example lives at
[`crates/fofoca-pipe/examples/chat.rs`](../../../crates/fofoca-pipe/examples/chat.rs),
not here. It stays a cargo example of `fofoca-pipe` on purpose: the e2e suite
builds it as `-p fofoca-pipe --example chat`, and its tokio dev-dependencies
are target-scoped there so the crate's wasm checks stay clean.

## Run

```sh
cargo run -p fofoca-pipe --example chat -- --topic room --nick terminal
```

Flags: `--topic <string>` or `--mesh <id>` selects the mesh; `--nick <name>`;
`--relay <url>` (repeatable) swaps in a custom relay ladder; `--robot` turns
the terminal chat into the NDJSON automation contract the e2e suite drives.

Commands once connected: type to broadcast, `/msg <nick> <text>`, `/peers`,
`/quit`.
