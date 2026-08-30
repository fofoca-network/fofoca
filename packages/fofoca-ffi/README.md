# fofoca-ffi (package)

fofoca on Bun, Deno, and Node: `dlopen` over the C ABI of
[`crates/fofoca-ffi`](../../crates/fofoca-ffi), with mDNS, the mainline
DHT, and the relay ladder.

Status: the low-level modules are here and tested — `abi.ts` (the
foreign-function surface), `frame.ts`, `protocol.ts`, `query.ts`, and
`discover.ts` (loader discovery for the three runtimes). The `join` /
`create` entry point is not built yet, so the package is not importable
as a mesh client today.

`koffi` is an optional peer dependency; Node needs it, Bun and Deno use
their built-in FFI.

See [`../README.md`](../README.md) for the workspace rules.
