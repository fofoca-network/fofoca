import { describe, expect, test } from 'bun:test'

import { FRAME_BYTES, decodeFrame, encodeFrame } from './frame.ts'

describe('decodeFrame', () => {
  test('round-trips every flag combination', () => {
    for (const directed of [false, true]) {
      for (const eof of [false, true]) {
        const meta = { nick: 'ana', directed, eof, len: eof ? 0 : 17, seq: eof ? 3 : 2 }
        expect(decodeFrame(encodeFrame(meta))).toEqual(meta)
      }
    }
  })

  test('reads a nick at the 63-byte limit `write_nick` truncates to', () => {
    const nick = 'n'.repeat(63)
    expect(decodeFrame(encodeFrame({ nick, directed: false, eof: false, len: 1, seq: 0 })).nick).toBe(nick)
  })

  test('reads a len at the frame budget', () => {
    const len = 2094
    expect(decodeFrame(encodeFrame({ nick: 'ana', directed: false, eof: false, len, seq: 0 })).len).toBe(len)
  })

  test('reads a seq past the 32-bit line', () => {
    const seq = 2 ** 40
    expect(decodeFrame(encodeFrame({ nick: 'ana', directed: false, eof: false, len: 1, seq })).seq).toBe(seq)
  })

  test('an empty nick decodes as empty rather than as the whole field', () => {
    expect(decodeFrame(encodeFrame({ nick: '', directed: false, eof: true, len: 0, seq: 0 })).nick).toBe('')
  })

  test('stops at the NUL rather than trailing the rest of the field', () => {
    const bytes = encodeFrame({ nick: 'ana', directed: false, eof: false, len: 3, seq: 0 })
    // Garbage after the terminator, as a reused buffer would hold.
    bytes.set([0x7a, 0x7a], 8)

    expect(decodeFrame(bytes).nick).toBe('ana')
  })

  test('an unterminated nick is an error, not a read into the integers', () => {
    const bytes = encodeFrame({ nick: 'ana', directed: false, eof: false, len: 3, seq: 0 })
    bytes.fill(0x61, 0, 64)

    expect(() => decodeFrame(bytes)).toThrow(/NUL-terminated/)
  })

  test('a short buffer is an error rather than a garbage frame', () => {
    expect(() => decodeFrame(new Uint8Array(FRAME_BYTES - 1))).toThrow(RangeError)
  })

  test('decodes at a non-zero byte offset, as a subarray of a scratch buffer is', () => {
    const scratch = new Uint8Array(FRAME_BYTES * 2)
    const meta = { nick: 'bo', directed: true, eof: false, len: 5, seq: 1 }
    scratch.set(encodeFrame(meta), FRAME_BYTES)

    expect(decodeFrame(scratch.subarray(FRAME_BYTES))).toEqual(meta)
  })

  test('the struct is 88 bytes, which is what the C layout says', () => {
    expect(FRAME_BYTES).toBe(88)
    expect(encodeFrame({ nick: '', directed: false, eof: false, len: 0, seq: 0 }).byteLength).toBe(88)
  })
})
