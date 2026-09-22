# fofoca-pipe-web

The pipe's web app: a tab on the byte pipe. It shows every stream as it
arrives, in order, and exposes its runtime to whoever drives the tab — a
person at the composer, a browser agent through WebMCP, or a script through
`window.pipe`.

## Run

```sh
cargo task wasm-peer                         # the wasm glue, once
bun run --cwd packages/fofoca-pipe-web serve # http://127.0.0.1:3020/
```

Then, in a terminal:

```sh
cat lorem.txt | fofoca-pipe --web-url http://127.0.0.1:3020/
```

The CLI prints `open http://127.0.0.1:3020/#mesh=<id>`. Open that URL. The
mesh id rides in the fragment, which a browser never sends to the server.

## Deploy

```sh
bun run --cwd packages/fofoca-pipe-web build   # packages/fofoca-pipe-web/dist/
```

`dist/` is static: `index.html`, `main.js` and the wasm glue under `wasm/`.
Any static host serves it. Point the CLI at the host with `--web-url` or
`FOFOCA_PIPE_WEB`.

## URL

- `#mesh=<id>` or `#topic=<string>` — exactly one. The query string is read
  as a fallback for both.
- `?nick=` — this peer's nickname.
- `?relay=` — a custom relay URL (repeatable). With `topic` it is part of the
  derived id, so every member must pass the same.
- `?transport=p2p,relay` — let payload fall back to the relay (same rule).
- `?log=` — an `EnvFilter` for the engine's tracing, to the console.

## WebMCP

When the browser has `document.modelContext` (or the older
`navigator.modelContext`), the page registers seven tools. Without it, the
same functions are on `window.pipe`.

| Tool | Does |
| --- | --- |
| `pipe_send` | Send `text`, broadcast or directed with `to`. A directed send to a `relay-only` peer is parked until a direct path forms. |
| `pipe_send_eof` | End the stream you have been sending. |
| `pipe_read` | What has arrived, in order, plus a `cursor` to continue from. Reading takes nothing away, so two readers each follow the stream with their own cursor. When nothing waits, wait up to `waitMs` (at most 25000) for the first entry. `encoding: "base64"` returns bytes that are not text, exactly. |
| `pipe_peers` | The roster. |
| `pipe_status` | Mesh id, name, nickname, peer count, what the read log holds (`buffered`, `cursor`, `oldestCursor`), every stream seen. |
| `pipe_state_get` | The shared state document. |
| `pipe_state_merge` | Apply an RFC 7386 merge `patch` to it. |

WebMCP has no streaming: a tool is one promise. `pipe_read` is the closest
thing — an agent that wants to follow a stream calls it in a loop, passing
back the `cursor` it got. An item with `eof: true` closes that sender's
stream. To read only what arrives from now on, take `cursor` from
`pipe_status` first.

The read log is bounded by bytes held (4 MB), not by what anyone has read.
Past that the oldest entries age out, and `oldestCursor` in `pipe_status`
says where a reader that fell behind actually resumes.

Two facts about Chrome's implementation (Chrome for Testing 152 ships
`document.modelContext` on): `executeTool` takes the `RegisteredTool` object
from `getTools()`, not a name, and its arguments as a JSON **string**; the
result comes back as a JSON string of `{content: [...]}`.

Frames are reassembled per sender. A hole in a stream — the opening frames
of a stream this tab joined late, or a frame lost on the way — holds the
rest back for 3 s (`GAP_TIMEOUT_MS`), then the tab skips it and goes on.

## Files

- `src/runtime.ts` — `PipeRuntime`, DOM-free: reorders frames, decodes text
  per stream, buffers for `read`.
- `src/webmcp.ts` — the tool descriptors and the registration.
- `src/main.ts` — the page: URL, DOM, `window.pipe`.
- `serve.ts` — the dev server. `build.ts` — the static build.
