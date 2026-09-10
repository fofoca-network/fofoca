import { describe, expect, test } from 'bun:test'
import type { WireOpts } from '../protocol.ts'
import { encodeOpts, OPTS_BYTES } from './opts-struct.ts'

const BASE: WireOpts = {
  mesh: null,
  topic: null,
  nick: null,
  name: null,
  isPublic: false,
  mdns: false,
  dht: false,
  relayLookup: false,
  relayTransport: false,
  relayUrls: null,
  disableIp: false,
  disableWebrtc: false,
  maxPeers: 0,
}

/** Hands out a distinct fake address per buffer and remembers which was which. */
function fakePointers() {
  const buffers: Uint8Array[] = []
  return {
    buffers,
    pointerOf: (buffer: Uint8Array): bigint => {
      buffers.push(buffer)
      return 0x1000n + BigInt(buffers.length - 1) * 0x100n
    },
  }
}

describe('encodeOpts', () => {
  // The literal, not OPTS_BYTES against itself. The other side of this number
  // is the layout assert on `FofocaOpts` in crates/fofoca-ffi/src/ffi.rs; a
  // linked C consumer keeps passing the old struct when it moves, so both
  // sides must be edited together.
  test('the struct is the 80 bytes the C header lays out', () => {
    expect(OPTS_BYTES).toBe(80)
  })

  test('null selectors encode as NULL pointers', () => {
    const { struct } = encodeOpts(BASE, fakePointers().pointerOf)
    expect(struct.byteLength).toBe(OPTS_BYTES)
    const view = new DataView(struct.buffer)
    for (const offset of [0, 8, 16, 24]) {
      expect(view.getBigUint64(offset, true)).toBe(0n)
    }
  })

  test('string fields land at their offsets, NUL-terminated', () => {
    const pointers = fakePointers()
    const { struct, keepAlive } = encodeOpts(
      { ...BASE, mesh: 'M', topic: 'tea-time', nick: 'ana', name: 'salon' },
      pointers.pointerOf,
    )
    const view = new DataView(struct.buffer)
    // One buffer per string, in field order, each address embedded in order.
    expect(keepAlive.length).toBe(4)
    expect(view.getBigUint64(0, true)).toBe(0x1000n)
    expect(view.getBigUint64(8, true)).toBe(0x1100n)
    expect(view.getBigUint64(16, true)).toBe(0x1200n)
    expect(view.getBigUint64(24, true)).toBe(0x1300n)
    const topic = pointers.buffers[1]
    expect(topic).toBeDefined()
    expect(Array.from(topic ?? [])).toEqual([...new TextEncoder().encode('tea-time'), 0])
  })

  test('flags and max_peers', () => {
    const { struct } = encodeOpts(
      { ...BASE, isPublic: true, dht: true, disableWebrtc: true, maxPeers: 12 },
      fakePointers().pointerOf,
    )
    const view = new DataView(struct.buffer)
    expect(view.getInt32(32, true)).toBe(1) // is_public
    expect(view.getInt32(36, true)).toBe(0) // mdns
    expect(view.getInt32(40, true)).toBe(1) // dht
    expect(view.getInt32(44, true)).toBe(0) // relay_lookup
    expect(view.getInt32(48, true)).toBe(0) // relay_transport
    expect(view.getBigUint64(56, true)).toBe(0n) // relay_urls
    expect(view.getInt32(64, true)).toBe(0) // disable_ip
    expect(view.getInt32(68, true)).toBe(1) // disable_webrtc
    expect(view.getBigUint64(72, true)).toBe(12n)
  })

  test('relay transport and a custom ladder', () => {
    const pointers = fakePointers()
    const { struct, keepAlive } = encodeOpts(
      { ...BASE, relayLookup: true, relayTransport: true, relayUrls: 'http://a/,http://b/' },
      pointers.pointerOf,
    )
    const view = new DataView(struct.buffer)
    expect(view.getInt32(44, true)).toBe(1) // relay_lookup
    expect(view.getInt32(48, true)).toBe(1) // relay_transport
    expect(view.getBigUint64(56, true)).toBe(0x1000n) // relay_urls
    expect(keepAlive.length).toBe(1)
    expect(Array.from(pointers.buffers[0] ?? [])).toEqual([
      ...new TextEncoder().encode('http://a/,http://b/'),
      0,
    ])
  })

  test('keepAlive roots every encoded string', () => {
    const { keepAlive } = encodeOpts({ ...BASE, nick: 'ana' }, fakePointers().pointerOf)
    expect(keepAlive.length).toBe(1)
    expect(keepAlive[0]?.at(-1)).toBe(0)
  })
})
