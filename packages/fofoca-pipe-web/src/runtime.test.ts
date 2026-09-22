import { describe, expect, test } from 'bun:test'

import { eof, fakeMesh, frame, settle } from './fake-mesh.ts'
import { GAP_TIMEOUT_MS } from 'fofoca-api'

import type { Chunk } from './runtime.ts'
import { MAX_WAIT_MS, PipeRuntime } from './runtime.ts'

describe('PipeRuntime', () => {
  test('shuffled frames read back in order, merged into one item', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    fake.push(frame('ana', 2, 'c'))
    fake.push(frame('ana', 0, 'a'))
    fake.push(frame('ana', 1, 'b'))
    await settle()

    const { items } = await runtime.read({ waitMs: 0 })
    expect(items).toEqual([{ from: 'ana', directed: false, text: 'abc', eof: false }])
  })

  test('a read takes nothing away: two readers see the same stream', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    fake.push(frame('ana', 0, 'a'))
    await settle()

    const first = await runtime.read({ waitMs: 0 })
    const second = await runtime.read({ waitMs: 0 })
    expect(first.items).toEqual(second.items)
    expect(first.cursor).toBe(second.cursor)
  })

  test('a cursor reads only what came after it', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    fake.push(frame('ana', 0, 'a'))
    await settle()
    const { cursor } = await runtime.read({ waitMs: 0 })

    fake.push(frame('ana', 1, 'b'))
    await settle()
    const next = await runtime.read({ waitMs: 0, cursor })
    expect(next.items).toEqual([{ from: 'ana', directed: false, eof: false, text: 'b' }])
    expect(next.cursor).toBe(cursor + 1)
  })

  test('a cursor at the end reads nothing and stays put', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    fake.push(frame('ana', 0, 'a'))
    await settle()
    const { cursor } = await runtime.read({ waitMs: 0 })

    const empty = await runtime.read({ waitMs: 0, cursor })
    expect(empty.items).toEqual([])
    expect(empty.cursor).toBe(cursor)
  })

  test('two readers keep independent cursors', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    fake.push(frame('ana', 0, 'a'))
    await settle()
    const slow = (await runtime.read({ waitMs: 0 })).cursor
    fake.push(frame('ana', 1, 'b'))
    fake.push(frame('ana', 2, 'c'))
    await settle()

    // The fast reader takes everything; the slow one still gets b and c.
    const fast = await runtime.read({ waitMs: 0 })
    expect(fast.items[0]?.text).toBe('abc')
    const rest = await runtime.read({ waitMs: 0, cursor: slow })
    expect(rest.items[0]?.text).toBe('bc')
  })

  test('an eof is its own item and closes the merge', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    fake.push(frame('ana', 0, 'a'))
    fake.push(eof('ana', 1))
    fake.push(frame('ana', 1, 'b'))
    await settle()

    const { items } = await runtime.read({ waitMs: 0 })
    expect(items).toEqual([
      { from: 'ana', directed: false, text: 'a', eof: false },
      { from: 'ana', directed: false, text: '', eof: true },
      { from: 'ana', directed: false, text: 'b', eof: false },
    ])
  })

  test('a multibyte character split across frames decodes whole', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    const bytes = new TextEncoder().encode('é')
    fake.push({ from: 'ana', bytes: bytes.subarray(0, 1), directed: false, eof: false, seq: 0 })
    fake.push({ from: 'ana', bytes: bytes.subarray(1), directed: false, eof: false, seq: 1 })
    await settle()

    const { items } = await runtime.read({ waitMs: 0 })
    expect(items).toEqual([{ from: 'ana', directed: false, text: 'é', eof: false }])
  })

  test('streams from two authors, and directed from one, stay apart', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    fake.push(frame('ana', 0, 'a'))
    fake.push(frame('bo', 0, 'b'))
    fake.push(frame('ana', 0, 'd', true))
    await settle()

    const { items } = await runtime.read({ waitMs: 0 })
    expect(items).toEqual([
      { from: 'ana', directed: false, text: 'a', eof: false },
      { from: 'bo', directed: false, text: 'b', eof: false },
      { from: 'ana', directed: true, text: 'd', eof: false },
    ])
  })

  test('read waits for the first chunk, then returns it', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    const pending = runtime.read({ waitMs: MAX_WAIT_MS })
    await settle()
    fake.push(frame('ana', 0, 'late'))

    const { items } = await pending
    expect(items).toEqual([{ from: 'ana', directed: false, text: 'late', eof: false }])
  })

  test('read gives up after waitMs with nothing', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    const started = performance.now()

    expect((await runtime.read({ waitMs: 20 })).items).toEqual([])
    expect(performance.now() - started).toBeGreaterThanOrEqual(15)
  })

  test('read returns early when its signal aborts', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    const controller = new AbortController()
    const pending = runtime.read({ waitMs: MAX_WAIT_MS }, controller.signal)
    controller.abort()

    expect((await pending).items).toEqual([])
  })

  test('two waiting reads both wake, and both get the chunk', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    const first = runtime.read({ waitMs: MAX_WAIT_MS })
    const second = runtime.read({ waitMs: MAX_WAIT_MS })
    await settle()
    fake.push(frame('ana', 0, 'x'))

    const [a, b] = await Promise.all([first, second])
    expect(a.items).toEqual([{ from: 'ana', directed: false, eof: false, text: 'x' }])
    expect(b.items).toEqual(a.items)
  })

  test('the log drops its oldest past the byte cap', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh, { maxBytes: 4 })
    fake.push(frame('ana', 0, 'aaa'))
    fake.push(frame('bo', 0, 'bbb'))
    fake.push(frame('cy', 0, 'ccc'))
    await settle()

    const { items } = await runtime.read({ waitMs: 0 })
    expect(items.map((item) => item.from)).toEqual(['cy'])
  })

  test('a cursor older than the log serves what is left', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh, { maxBytes: 4 })
    fake.push(frame('ana', 0, 'aaa'))
    await settle()
    const stale = (await runtime.read({ waitMs: 0 })).cursor - 1
    fake.push(frame('bo', 0, 'bbb'))
    await settle()

    const { items } = await runtime.read({ waitMs: 0, cursor: stale })
    expect(items.map((item) => item.from)).toEqual(['bo'])
  })

  test('base64 carries bytes that are not text', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    const bytes = new Uint8Array([0x00, 0xff, 0xfe, 0x41])
    fake.push({ from: 'ana', bytes, directed: false, eof: false, seq: 0 })
    await settle()

    const { items } = await runtime.read({ waitMs: 0, encoding: 'base64' })
    expect(items).toEqual([{ from: 'ana', directed: false, eof: false, base64: 'AP/+QQ==' }])
    // The text view of the same bytes is lossy, which is why base64 exists.
    const asText = await runtime.read({ waitMs: 0 })
    expect(asText.items[0]?.text).not.toBe('\u0000\u00ff\u00feA')
  })

  test('base64 joins bytes before encoding, across chunks', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    fake.push({ from: 'ana', bytes: new Uint8Array([1]), directed: false, eof: false, seq: 0 })
    fake.push({ from: 'ana', bytes: new Uint8Array([2, 3]), directed: false, eof: false, seq: 1 })
    await settle()

    const { items } = await runtime.read({ waitMs: 0, encoding: 'base64' })
    expect(items).toHaveLength(1)
    expect(atob(items[0]?.base64 ?? '')).toBe('\u0001\u0002\u0003')
  })

  test('a stream joined late is released once its hole is old', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    // seq 0 was sent before this peer existed; 1 and 2 arrive.
    fake.push(frame('ana', 1, 'b'))
    fake.push(frame('ana', 2, 'c'))
    await settle()
    expect((await runtime.read({ waitMs: 0 })).items).toEqual([])
    expect(runtime.status().streams).toEqual([{ from: 'ana', directed: false, received: 0, complete: false }])

    runtime.expire(performance.now() + GAP_TIMEOUT_MS)
    expect((await runtime.read({ waitMs: 0 })).items).toEqual([
      { from: 'ana', directed: false, eof: false, text: 'bc' },
    ])
    expect(runtime.status().streams[0]?.received).toBe(2)
  })

  test('onSent reports every send this runtime made', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    const sent: [string, string | undefined][] = []
    const ended: (string | undefined)[] = []
    runtime.onSent = (text, to) => sent.push([text, to])
    runtime.onSentEof = (to) => ended.push(to)
    await runtime.send('hi')
    await runtime.send('psst', 'bo')
    await runtime.sendEof()
    await runtime.sendEof('bo')
    expect(sent).toEqual([
      ['hi', undefined],
      ['psst', 'bo'],
    ])
    expect(ended).toEqual([undefined, 'bo'])
  })

  test('onChunk sees every chunk as it lands', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    const seen: Chunk[] = []
    runtime.onChunk = (chunk) => seen.push(chunk)
    fake.push(frame('ana', 0, 'a'))
    fake.push(frame('ana', 1, 'b'))
    fake.push(eof('ana', 2))
    await settle()

    expect(seen.map((chunk) => [chunk.text, chunk.eof])).toEqual([
      ['a', false],
      ['b', false],
      ['', true],
    ])
  })

  test('status counts unread and reports each stream', async () => {
    const fake = fakeMesh([
      { nick: 'ana', reach: 'direct', transport: 'unicast', quiet: false } as never,
    ])
    const runtime = new PipeRuntime(fake.mesh)
    fake.push(frame('ana', 0, 'a'))
    fake.push(eof('ana', 1))
    await settle()

    expect(runtime.status()).toEqual({
      id: 'mesh-id',
      name: 'mesh-name',
      nick: 'me',
      peerCount: 1,
      buffered: 2,
      cursor: 2,
      oldestCursor: 0,
      streams: [{ from: 'ana', directed: false, received: 1, complete: true }],
    })
  })

  test('send, sendEof and stateMerge reach the mesh', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    await runtime.send('hi')
    await runtime.send('psst', 'bo')
    await runtime.sendEof()
    await runtime.sendEof('bo')
    await runtime.stateMerge({ k: 'v' })

    expect(fake.sends).toEqual([{ body: 'hi' }, { body: 'psst', to: 'bo' }])
    expect(fake.eofs).toEqual([{}, { to: 'bo' }])
    expect(fake.merges).toEqual([{ k: 'v' }])
    expect(runtime.stateGet()).toEqual({ a: 1 })
  })

  test('a read against a closed mesh does not wait', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    fake.end()
    await settle()
    const started = performance.now()

    expect((await runtime.read({ waitMs: MAX_WAIT_MS })).items).toEqual([])
    expect(performance.now() - started).toBeLessThan(1000)
  })
})
