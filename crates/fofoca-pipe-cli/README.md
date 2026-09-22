# fofoca-pipe (the binary)

Stdin to a gossip mesh, the mesh to stdout.

```sh
cat lorem.txt | fofoca-pipe --web-url http://127.0.0.1:3020/
```

The CLI mints a public mesh, prints `mesh <id>` and `open <url>#mesh=<id>`
to stderr, waits for a peer that can receive (one the roster shows with a
payload lane, not merely one that joined — a tab is in the overlay long
before its data channel is up), then streams stdin out in numbered frames
and ends with an EOF marker. Open the URL in a browser
(`packages/fofoca-pipe-web`) to see the bytes land.

Stdout is the received stream and nothing else. Every word the binary has
to say goes to stderr — as prose, or with `--robot` as one JSON object per
line (`{"kind":"open",...}` first, then the engine's events verbatim).

Two modes, decided by what stdin is:

- A pipe or a file is **send** mode. After stdin ends, the EOF marker goes
  out and the process leaves.
- A terminal is **receive** mode. Stdin is not read, and the process leaves
  once the first remote stream completes: `fofoca-pipe --mesh <id> > out.txt`.

`--stay` keeps either mode open until Ctrl-C. `--no-wait` reads stdin at
once; frames are not stored by the mesh, so a peer that joins later misses
what went before — and what it does receive waits 3 s behind the hole
(`fofoca_pipe::GAP_TIMEOUT`) before it is written out without it.

Selectors: `--mesh <id>` joins, `--topic <string>` derives a public mesh
every member can derive from the same string, and nothing at all creates a
public mesh. `--relay-url` alone creates a relay-only mesh, which is what the
e2e suite (`cargo task e2e --suite pipe`) uses.

The crate is a sibling of `fofoca-pipe` rather than a `[[bin]]` in it: that
crate is built for wasm32 by the gate, and a binary needs the tokio runtime.
