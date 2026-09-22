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

    const { items } = await runtime.read(0)
    expect(items).toEqual([{ from: 'ana', directed: false, text: 'abc', eof: false }])
  })

  test('read consumes: a second read is empty', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    fake.push(frame('ana', 0, 'a'))
    await settle()

    expect((await runtime.read(0)).items).toHaveLength(1)
    expect((await runtime.read(0)).items).toEqual([])
  })

  test('an eof is its own item and closes the merge', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    fake.push(frame('ana', 0, 'a'))
    fake.push(eof('ana', 1))
    fake.push(frame('ana', 1, 'b'))
    await settle()

    const { items } = await runtime.read(0)
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

    const { items } = await runtime.read(0)
    expect(items).toEqual([{ from: 'ana', directed: false, text: 'é', eof: false }])
  })

  test('streams from two authors, and directed from one, stay apart', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    fake.push(frame('ana', 0, 'a'))
    fake.push(frame('bo', 0, 'b'))
    fake.push(frame('ana', 0, 'd', true))
    await settle()

    const { items } = await runtime.read(0)
    expect(items).toEqual([
      { from: 'ana', directed: false, text: 'a', eof: false },
      { from: 'bo', directed: false, text: 'b', eof: false },
      { from: 'ana', directed: true, text: 'd', eof: false },
    ])
  })

  test('read waits for the first chunk, then returns it', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    const pending = runtime.read(MAX_WAIT_MS)
    await settle()
    fake.push(frame('ana', 0, 'late'))

    const { items } = await pending
    expect(items).toEqual([{ from: 'ana', directed: false, text: 'late', eof: false }])
  })

  test('read gives up after waitMs with nothing', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    const started = performance.now()

    expect((await runtime.read(20)).items).toEqual([])
    expect(performance.now() - started).toBeGreaterThanOrEqual(15)
  })

  test('read returns early when its signal aborts', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    const controller = new AbortController()
    const pending = runtime.read(MAX_WAIT_MS, controller.signal)
    controller.abort()

    expect((await pending).items).toEqual([])
  })

  test('two concurrent reads both wake on one chunk', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    const first = runtime.read(MAX_WAIT_MS)
    const second = runtime.read(MAX_WAIT_MS)
    await settle()
    fake.push(frame('ana', 0, 'x'))

    const [a, b] = await Promise.all([first, second])
    expect(a.items.length + b.items.length).toBe(1)
  })

  test('the inbox drops its oldest past the cap', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh, { inboxCap: 2 })
    fake.push(frame('ana', 0, 'a'))
    fake.push(frame('bo', 0, 'b'))
    fake.push(frame('cy', 0, 'c'))
    await settle()

    const { items } = await runtime.read(0)
    expect(items.map((item) => item.from)).toEqual(['bo', 'cy'])
  })

  test('a stream joined late is released once its hole is old', async () => {
    const fake = fakeMesh()
    const runtime = new PipeRuntime(fake.mesh)
    // seq 0 was sent before this peer existed; 1 and 2 arrive.
    fake.push(frame('ana', 1, 'b'))
    fake.push(frame('ana', 2, 'c'))
    await settle()
    expect((await runtime.read(0)).items).toEqual([])
    expect(runtime.status().streams).toEqual([{ from: 'ana', directed: false, received: 0, complete: false }])

    runtime.expire(performance.now() + GAP_TIMEOUT_MS)
    expect((await runtime.read(0)).items).toEqual([{ from: 'ana', directed: false, text: 'bc', eof: false }])
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
      unread: 2,
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

    expect((await runtime.read(MAX_WAIT_MS)).items).toEqual([])
    expect(performance.now() - started).toBeLessThan(1000)
  })
})
