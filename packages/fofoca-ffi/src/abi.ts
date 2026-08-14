/**
 * `crates/fofoca-ffi/include/fofoca.h`, transcribed once.
 *
 * The vocabulary is deliberately not any one runtime's type names: `bun:ffi`,
 * `Deno.dlopen` and koffi spell the same C types three different ways, and
 * translating this table into each of them is the whole job of a loader. One
 * table means a signature can be wrong once rather than three times, and
 * `abi.test.ts` holds it against the header.
 */

export type CType =
  /** C `int`. */
  | 'i32'
  /** C `long`. 8 bytes on every target here, so it crosses as a bigint. */
  | 'isize'
  /** C `size_t`. Likewise a bigint. */
  | 'usize'
  /** An opaque address in or out — a handle, or a returned `char *`. */
  | 'ptr'
  /** A caller-owned buffer the callee reads or writes in place. Never NULL. */
  | 'buf'
  /** A NUL-terminated string argument, or NULL. */
  | 'cstr'

export interface Signature {
  readonly args: readonly CType[]
  readonly returns: CType
}

export const ABI = {
  fofoca_open: { args: ['ptr'], returns: 'ptr' },

  fofoca_id: { args: ['ptr'], returns: 'ptr' },
  fofoca_name: { args: ['ptr'], returns: 'ptr' },
  fofoca_nickname: { args: ['ptr'], returns: 'ptr' },

  fofoca_send: { args: ['ptr', 'cstr', 'buf', 'usize'], returns: 'i32' },
  fofoca_send_eof: { args: ['ptr', 'cstr'], returns: 'i32' },
  /** 1 = a frame is in `out`, 0 = timeout, -1 = failure. EOF is a field, not a code. */
  fofoca_recv: { args: ['ptr', 'buf', 'usize', 'i32', 'buf'], returns: 'isize' },

  fofoca_state_merge: { args: ['ptr', 'cstr'], returns: 'i32' },
  fofoca_state_json: { args: ['ptr', 'buf', 'usize'], returns: 'isize' },
  fofoca_peers_json: { args: ['ptr', 'buf', 'usize'], returns: 'isize' },
  /**
   * Bound because the header declares it, and never called: `mesh.peers` needs
   * the whole roster anyway, and this one excludes self where the roster
   * document's own `count` includes it. Calling both would be two sources of
   * truth that disagree by one.
   */
  fofoca_peer_count: { args: ['ptr'], returns: 'isize' },

  fofoca_close: { args: ['ptr'], returns: 'i32' },
  fofoca_last_error: { args: [], returns: 'ptr' },
  fofoca_version: { args: [], returns: 'ptr' },
  fofoca_max_chunk: { args: [], returns: 'usize' },
} as const satisfies Record<string, Signature>

export type SymbolName = keyof typeof ABI
