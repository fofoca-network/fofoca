import { describe, expect, test } from 'bun:test'

import { GAP_TIMEOUT_MS, REORDER_CAP, Reorder, Streams } from './reorder.ts'

const chunk = (seq: number) => new Uint8Array([seq % 256])

const frame = (from: string, directed: boolean, seq: number) => ({
  from,
  directed,
  eof: false,
  seq,
  bytes: chunk(seq),
})

const eof = (from: string, directed: boolean, count: number) => ({
  from,
  directed,
  eof: true,
  seq: count,
  bytes: new Uint8Array(),
})

describe('Reorder', () => {
  test('in-order frames pass straight through', () => {
    const stream = new Reorder()
    for (let seq = 0; seq < 3; seq += 1) {
      expect(stream.push(seq, chunk(seq))).toEqual([chunk(seq)])
    }
  })

  test('shuffled frames come out in order', () => {
    const stream = new Reorder()
    expect(stream.push(2, chunk(2))).toEqual([])
    expect(stream.push(0, chunk(0))).toEqual([chunk(0)])
    expect(stream.push(1, chunk(1))).toEqual([chunk(1), chunk(2)])
  })

  test('a duplicate is dropped', () => {
    const stream = new Reorder()
    expect(stream.push(0, chunk(0))).toEqual([chunk(0)])
    expect(stream.push(0, chunk(0))).toEqual([])
    expect(stream.push(2, chunk(2))).toEqual([])
    expect(stream.push(2, chunk(2))).toEqual([])
    expect(stream.push(1, chunk(1))).toEqual([chunk(1), chunk(2)])
  })

  test('an early eof completes when the last chunk lands', () => {
    const stream = new Reorder()
    stream.push(0, chunk(0))
    expect(stream.eof(2)).toBe(false)
    expect(stream.isComplete()).toBe(false)
    stream.push(1, chunk(1))
    expect(stream.isComplete()).toBe(true)
  })

  test('an empty stream completes on its eof', () => {
    expect(new Reorder().eof(0)).toBe(true)
  })

  test('a stream continues past its eof without resetting', () => {
    const stream = new Reorder()
    stream.push(0, chunk(0))
    expect(stream.eof(1)).toBe(true)
    expect(stream.isComplete()).toBe(false)
    expect(stream.push(1, chunk(1))).toEqual([chunk(1)])
    expect(stream.eof(2)).toBe(true)
  })

  test('a gap that never fills is skipped at the cap', () => {
    const stream = new Reorder()
    const start = 1
    const end = start + REORDER_CAP
    for (let seq = start; seq < end; seq += 1) {
      expect(stream.push(seq, chunk(seq))).toEqual([])
    }
    const released = stream.push(end, chunk(end))
    expect(released).toHaveLength(REORDER_CAP + 1)
    expect(released[0]).toEqual(chunk(1))
  })
})

describe('Reorder gap timeout', () => {
  test('a hole older than the timeout is skipped on expire', () => {
    const stream = new Reorder()
    const opened = 1000
    expect(stream.push(1, chunk(1), opened)).toEqual([])
    expect(stream.push(2, chunk(2), opened)).toEqual([])
    expect(stream.expire(opened + GAP_TIMEOUT_MS - 1)).toEqual([])
    expect(stream.expire(opened + GAP_TIMEOUT_MS)).toEqual([chunk(1), chunk(2)])
    expect(stream.push(3, chunk(3), opened)).toEqual([chunk(3)])
  })

  test('a hole that fills in time is not a gap', () => {
    const stream = new Reorder()
    expect(stream.push(1, chunk(1), 0)).toEqual([])
    expect(stream.push(0, chunk(0), 1000)).toEqual([chunk(0), chunk(1)])
    expect(stream.expire(GAP_TIMEOUT_MS * 2)).toEqual([])
  })

  test('a new hole after progress gets its own clock', () => {
    const stream = new Reorder()
    stream.push(0, chunk(0), 0)
    expect(stream.push(2, chunk(2), 0)).toEqual([])
    const later = GAP_TIMEOUT_MS - 1
    expect(stream.push(1, chunk(1), later)).toEqual([chunk(1), chunk(2)])
    expect(stream.push(4, chunk(4), later)).toEqual([])
    expect(stream.expire(GAP_TIMEOUT_MS)).toEqual([])
    expect(stream.expire(later + GAP_TIMEOUT_MS)).toEqual([chunk(4)])
  })

  test('expire on streams reports completion too', () => {
    const streams = new Streams()
    expect(streams.push(eof('ana', false, 2)).complete).toBe(false)
    expect(streams.push(frame('ana', false, 1), 0).chunks).toEqual([])
    expect(streams.expire(GAP_TIMEOUT_MS - 1)).toEqual([])
    const released = streams.expire(GAP_TIMEOUT_MS)
    expect(released).toEqual([{ from: 'ana', directed: false, chunks: [chunk(1)], complete: true }])
    expect(streams.expire(GAP_TIMEOUT_MS * 2)).toEqual([])
  })
})

describe('Streams', () => {
  test('streams are keyed by author and direction', () => {
    const streams = new Streams()
    const gap = streams.push(frame('ana', false, 1))
    expect(gap.chunks).toEqual([])
    expect(gap.complete).toBe(false)
    const directed = streams.push(frame('ana', true, 0))
    expect(directed.chunks).toEqual([chunk(0)])
    expect(directed.directed).toBe(true)
    const other = streams.push(frame('bo', false, 0))
    expect([other.from, other.chunks]).toEqual(['bo', [chunk(0)]])
    const filled = streams.push(frame('ana', false, 0))
    expect(filled.chunks).toEqual([chunk(0), chunk(1)])
    expect(filled.complete).toBe(false)
    const ended = streams.push(eof('ana', false, 2))
    expect(ended.chunks).toEqual([])
    expect(ended.complete).toBe(true)
  })

  test('a late chunk reports completion itself', () => {
    const streams = new Streams()
    expect(streams.push(eof('ana', false, 1)).complete).toBe(false)
    const last = streams.push(frame('ana', false, 0))
    expect(last.chunks).toEqual([chunk(0)])
    expect(last.complete).toBe(true)
  })
})
