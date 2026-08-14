# `packages/` — fofoca from JavaScript

Three packages, so a JS or TS program can join a mesh, send messages, read the
roster and merge the shared document without writing any Rust.

| | |
|---|---|
| [`fofoca-api`](fofoca-api) | The contract, and the machinery both backends share. No runtime dependencies, and no way to join a mesh. |
| [`fofoca-wasm`](fofoca-wasm) | A browser tab. Runs the engine as WebAssembly over the WebRTC transport. |
| [`fofoca-ffi`](fofoca-ffi) | Bun, Deno and Node. `dlopen`s the C ABI in [`crates/fofoca-ffi`](../crates/fofoca-ffi) and gets mDNS, the mainline DHT and the relay ladder. |

Two backends because the two hosts reach the engine differently: a tab has no
UDP socket, a terminal has real ones. They meet on the mesh because both speak
[`crates/fofoca-pipe`](../crates/fofoca-pipe)'s wire contract, which exists as
one crate for exactly that reason.

```ts
import { join } from 'fofoca-ffi' // or 'fofoca-wasm'

await using mesh = await join({ topic: 'star-lake', nick: 'caio' })
await mesh.send('hello')

for await (const message of mesh.messages()) {
  console.log(`${message.from}: ${message.text}`)
}
```

## Two things that surprise people

**`join({ topic })` is always public.** A topic mesh is reached over mDNS, the
mainline DHT and the pinned relay ladder, and that is not a default you can
change. The engine mixes the lookup set into the mesh id, so two peers reaching
the same string over different discovery legs derive two different meshes and
never meet. `JoinOpts` therefore carries no discovery flags at all.

**`create({})` is machine-local.** Naming no discovery option is not "the
default set" — it resolves to a loopback mesh nothing off this machine can
reach. That is what makes the offline two-peer test possible, and it is
surprising everywhere else. Pass `public: true`, or name the legs you want.

## The workspace

The root `package.json` globs `packages/*` and `examples/chat/*`. It must never
glob `examples/*`: `examples/light-cycles/web` is its own Bun workspace root,
with `"workspaces": ["vendor/*"]`, and two overlapping workspaces make
`bun install` resolve the vendored packages twice. `package.json` cannot carry
comments, which is why that rule is written down here.

Every package is `private: true` and points its `exports` at `.ts` source rather
than built output — the shape `examples/light-cycles/web/vendor/*` already uses.
Publishing needs a build step, for a reason worth knowing: Node refuses to strip
types from a file whose real path is inside `node_modules`, and only the Bun
workspace symlink is what keeps `fofoca-ffi`'s worker loadable under Node today.
