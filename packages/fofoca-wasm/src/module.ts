/**
 * The wasm-bindgen module's shape, as this package consumes it.
 *
 * Declared here rather than imported from the generated `.d.ts` because the
 * glue under `../wasm/` is a build artifact (`cargo task wasm-peer`), and the
 * package must type-check before it has been built. The contract's source of
 * truth is `crates/fofoca-wasm/src/lib.rs`; change one, change the other.
 */

export interface MeshPeerHandle {
  id(): string
  nick(): string
  name(): string
  maxChunk(): number
  /** Splits at `maxChunk()` on the Rust side; `to` absent = broadcast. */
  send(to: string | undefined, bytes: Uint8Array): Promise<void>
  sendEof(to: string | undefined): Promise<void>
  /**
   * The next inbound frame as JSON (`{nick, directed, eof, bytes: number[]}`),
   * or `undefined` once the mesh is gone. One in-flight call at a time is the
   * intended shape; a concurrent second call waits.
   */
  nextFrame(): Promise<string | undefined>
  /** The next `PipeEvent` as JSON, or `undefined` once the mesh is gone. */
  nextEvent(): Promise<string | undefined>
  peersJson(): Promise<string>
  peerCount(): Promise<number>
  stateJson(): Promise<string>
  /** Applies an RFC 7386 merge document; resolves with the resulting state. */
  stateMerge(patchJson: string): Promise<string>
  close(): Promise<void>
}

export interface FofocaWasmModule {
  /** wasm-bindgen's init — must resolve before anything else is touched. */
  default(input?: unknown): Promise<unknown>
  /** Route the engine's tracing lines to the console. One-shot. */
  initTracing(filter: string): void
  MeshPeer: {
    open(optsJson: string): Promise<MeshPeerHandle>
  }
}

let cached: Promise<FofocaWasmModule> | null = null

/**
 * Load and initialise the wasm module, once — the *promise* is cached, not
 * the module, so concurrent first calls share one init instead of racing
 * wasm-bindgen's one-shot `default()`.
 *
 * `glueUrl` overrides where the generated glue lives; the default expects
 * `wasm/fofoca_wasm.js` beside `src/`, which `cargo task wasm-peer` produces.
 */
export function loadWasm(glueUrl?: string): Promise<FofocaWasmModule> {
  cached ??= (async () => {
    const url = glueUrl ?? new URL('../wasm/fofoca_wasm.js', import.meta.url).href
    const loaded = (await import(url)) as unknown as FofocaWasmModule
    await loaded.default()
    return loaded
  })()
  return cached
}
