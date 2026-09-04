/**
 * fofoca in the browser.
 *
 * ```ts
 * import { join } from 'fofoca-wasm'
 * const mesh = await join({ topic: 'standup' })
 * ```
 *
 * The wasm glue must exist first: `cargo task wasm-peer` drops it under
 * `wasm/`.
 */

export * from 'fofoca-api'

import { openMesh } from 'fofoca-api'
import type { CreateOpts, JoinOpts, Mesh } from 'fofoca-api'

import { openWasm } from './backend.ts'
import { loadWasm } from './module.ts'

export { openWasm } from './backend.ts'
export { loadWasm } from './module.ts'
export type { FofocaWasmModule, MeshPeerHandle } from './module.ts'

/** Extras every open accepts, beside the mesh selectors. */
export interface WasmOpts {
  /**
   * An `EnvFilter` string routed to the browser console
   * (`fofoca=info,fofoca::lifecycle=debug`). Omit for `info`.
   */
  log?: string
  /** Where the generated wasm glue lives; see [`loadWasm`]. */
  glueUrl?: string
}

/** Join an existing mesh — by topic string or by id. */
export async function join(opts: JoinOpts & WasmOpts): Promise<Mesh> {
  return open(
    {
      mesh: opts.id,
      topic: opts.topic,
      nick: opts.nick,
      relayTransport: opts.relayTransport ?? false,
      relayUrls: opts.relayUrls ?? [],
      maxPeers: opts.maxPeers ?? 0,
    },
    opts,
  )
}

/** Create a new mesh. `create({})` is refused in a browser: a loopback mesh
 * is unreachable from a tab — pass `public: true` or name the legs. */
export async function create(opts: CreateOpts & WasmOpts): Promise<Mesh> {
  return open(
    {
      name: opts.name,
      nick: opts.nick,
      public: opts.public ?? false,
      mdns: opts.mdns ?? false,
      dht: opts.dht ?? false,
      relay: opts.relay ?? false,
      relayTransport: opts.relayTransport ?? false,
      relayUrls: opts.relayUrls ?? [],
      transports: opts.transports ?? {},
      maxPeers: opts.maxPeers ?? 0,
    },
    opts,
  )
}

async function open(pipeOpts: Record<string, unknown>, extras: WasmOpts): Promise<Mesh> {
  const module = await loadWasm(extras.glueUrl)
  module.initTracing(extras.log ?? 'info')
  return openMesh(openWasm(module, JSON.stringify(pipeOpts)))
}
