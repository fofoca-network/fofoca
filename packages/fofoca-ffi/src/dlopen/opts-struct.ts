/**
 * `fofoca_opts`, encoded by hand for the loaders whose FFI cannot marshal a
 * struct (`bun:ffi` and Deno's). koffi marshals a plain object itself and
 * never touches this file.
 *
 * ```c
 * typedef struct {
 *   const char *mesh;    // offset  0
 *   const char *topic;   // offset  8
 *   const char *nick;    // offset 16
 *   const char *name;    // offset 24
 *   int is_public;       // offset 32
 *   int mdns;            // offset 36
 *   int dht;             // offset 40
 *   int relay;           // offset 44
 *   size_t max_peers;    // offset 48
 * } fofoca_opts;         // 56 bytes
 * ```
 *
 * 64-bit little-endian only, the same scope `frame.ts` claims for the same
 * reason.
 */

import type { WireOpts } from '../protocol.ts'

const MESH_OFFSET = 0
const TOPIC_OFFSET = 8
const NICK_OFFSET = 16
const NAME_OFFSET = 24
const IS_PUBLIC_OFFSET = 32
const MDNS_OFFSET = 36
const DHT_OFFSET = 40
const RELAY_OFFSET = 44
const MAX_PEERS_OFFSET = 48

export const OPTS_BYTES = 56

const LITTLE_ENDIAN = new Uint8Array(new Uint32Array([1]).buffer)[0] === 1

const encoder = new TextEncoder()

export interface EncodedOpts {
  /** The struct itself, to pass as the `fofoca_open` argument. */
  readonly struct: Uint8Array
  /**
   * The NUL-terminated string buffers whose addresses the struct embeds.
   *
   * The caller MUST keep a reference to this array until `fofoca_open`
   * returns: nothing else roots these buffers, and a GC between encode and
   * call would leave the struct pointing at freed memory.
   */
  readonly keepAlive: readonly Uint8Array[]
}

function terminated(value: string): Uint8Array {
  const text = encoder.encode(value)
  const bytes = new Uint8Array(text.byteLength + 1)
  bytes.set(text, 0)
  return bytes
}

/**
 * Build the struct. `pointerOf` is injected because taking a buffer's address
 * is the one loader-specific step, and injecting it keeps the layout testable
 * without any FFI at all.
 */
export function encodeOpts(opts: WireOpts, pointerOf: (buffer: Uint8Array) => bigint): EncodedOpts {
  const struct = new Uint8Array(OPTS_BYTES)
  const view = new DataView(struct.buffer)
  const keepAlive: Uint8Array[] = []

  const field = (offset: number, value: string | null) => {
    if (value === null) {
      view.setBigUint64(offset, 0n, LITTLE_ENDIAN)
      return
    }
    const bytes = terminated(value)
    keepAlive.push(bytes)
    view.setBigUint64(offset, pointerOf(bytes), LITTLE_ENDIAN)
  }

  field(MESH_OFFSET, opts.mesh)
  field(TOPIC_OFFSET, opts.topic)
  field(NICK_OFFSET, opts.nick)
  field(NAME_OFFSET, opts.name)
  view.setInt32(IS_PUBLIC_OFFSET, opts.isPublic ? 1 : 0, LITTLE_ENDIAN)
  view.setInt32(MDNS_OFFSET, opts.mdns ? 1 : 0, LITTLE_ENDIAN)
  view.setInt32(DHT_OFFSET, opts.dht ? 1 : 0, LITTLE_ENDIAN)
  view.setInt32(RELAY_OFFSET, opts.relay ? 1 : 0, LITTLE_ENDIAN)
  view.setBigUint64(MAX_PEERS_OFFSET, BigInt(opts.maxPeers), LITTLE_ENDIAN)

  return { struct, keepAlive }
}
