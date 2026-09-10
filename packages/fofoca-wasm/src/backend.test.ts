import { describe, expect, test } from 'bun:test'

import type { BackendFrame, MeshEvent } from 'fofoca-api'

import { openWasm } from './backend.ts'
import type { FofocaWasmModule, MeshPeerHandle } from './module.ts'

/** A queue whose `take()` waits for the next `put()`. */
class Waiting<T> {
  private items: T[] = []
  private wakers: ((value: T) => void)[] = []

  put(item: T): void {
    const waker = this.wakers.shift()
    if (waker) {
      waker(item)
    } else {
      this.items.push(item)
    }
  }

  take(): Promise<T> {
    const item = this.items.shift()
    if (item !== undefined) {
      return Promise.resolve(item)
    }
    return new Promise((resolve) => this.wakers.push(resolve))
  }
}

function roster(...nicks: string[]): string {
  return JSON.stringify({
    peers: nicks.map((nickname) => ({
      nickname,
      last_seen_secs_ago: 1,
      quiet: false,
      reach: 'direct',
      transport: 'unicast',
    })),
    count: nicks.length + 1,
  })
}

interface Fake {
  frames: Waiting<string | undefined>
  events: Waiting<string | undefined>
  module: FofocaWasmModule
  sent: { to: string | undefined; bytes: Uint8Array }[]
  closes: number
  rosterJson: string
}

function fake(): Fake {
  const frames = new Waiting<string | undefined>()
  const events = new Waiting<string | undefined>()
  const handle: Fake = {
    frames,
    events,
    sent: [],
    closes: 0,
    rosterJson: roster(),
    module: undefined as unknown as FofocaWasmModule,
  }
  const peer: MeshPeerHandle = {
    id: () => 'mesh-id',
    nick: () => 'assigned',
    name: () => 'a-name',
    maxChunk: () => 2112,
    send: (to, bytes) => {
      handle.sent.push({ to, bytes })
      return Promise.resolve()
    },
    sendEof: () => Promise.resolve(),
    nextFrame: () => frames.take(),
    nextEvent: () => events.take(),
    peersJson: () => Promise.resolve(handle.rosterJson),
    peerCount: () => Promise.resolve(0),
    stateJson: () => Promise.resolve('{}'),
    stateMerge: () => Promise.resolve('{"a":1}'),
    close: () => {
      handle.closes += 1
      return Promise.resolve()
    },
  }
  handle.module = {
    default: () => Promise.resolve(undefined),
    initTracing: () => {},
    MeshPeer: { open: () => Promise.resolve(peer) },
  }
  return handle
}

async function settle(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0))
}

describe('openWasm', () => {
  test('opens with the baseline roster and state, pushing presence itself', async () => {
    const wasm = fake()
    wasm.rosterJson = roster('bob')
    const sunk: string[] = []
    const opened = await openWasm(wasm.module, '{}')({
      frame: () => {},
      roster: (json) => sunk.push(json),
      state: () => {},
      presence: () => {},
      failed: () => {},
      closed: () => {},
    })
    expect(opened.backend.id).toBe('mesh-id')
    expect(opened.backend.nick).toBe('assigned')
    expect(opened.backend.maxChunk).toBe(2112)
    expect(opened.pushesPresence).toBe(true)
    expect(opened.rosterJson).toBe(roster('bob'))
    await opened.backend.close()
  })

  test('frames and events reach the sink', async () => {
    const wasm = fake()
    const frames: BackendFrame[] = []
    const presence: { kind: string; nick: string }[] = []
    const states: string[] = []
    const extras: MeshEvent[] = []
    const opened = await openWasm(wasm.module, '{}')({
      frame: (frame) => frames.push(frame),
      roster: () => {},
      state: (json) => states.push(json),
      presence: (event) => presence.push(event),
      failed: () => {},
      closed: () => {},
      event: (event) => extras.push(event),
    })

    wasm.frames.put(JSON.stringify({ nick: 'bob', directed: true, eof: false, bytes: [104, 105] }))
    wasm.events.put(JSON.stringify({ kind: 'joined', nick: 'bob' }))
    wasm.events.put(
      JSON.stringify({
        kind: 'state_changed',
        channel: 'state',
        author: 'bob',
        document: { a: 1 },
        is_self: false,
      }),
    )
    wasm.events.put(JSON.stringify({ kind: 'quiet', nick: 'bob', last_seen_secs_ago: 99 }))
    await settle()

    expect(frames).toHaveLength(1)
    expect(frames[0]?.from).toBe('bob')
    expect(Array.from(frames[0]?.bytes ?? [])).toEqual([104, 105])
    expect(frames[0]?.directed).toBe(true)
    expect(presence).toEqual([{ kind: 'joined', nick: 'bob' }])
    expect(states).toEqual(['{"a":1}'])
    expect(extras).toEqual([{ kind: 'quiet', nick: 'bob' }])
    await opened.backend.close()
  })

  test('a drained event queue closes the mesh; close() itself does not', async () => {
    const wasm = fake()
    const closes: string[] = []
    const opened = await openWasm(wasm.module, '{}')({
      frame: () => {},
      roster: () => {},
      state: () => {},
      presence: () => {},
      failed: () => {},
      closed: (reason) => closes.push(reason),
    })
    await opened.backend.close()
    wasm.events.put(undefined)
    wasm.frames.put(undefined)
    await settle()
    expect(wasm.closes).toBe(1)
    expect(closes).toEqual([])
  })

  test('send passes through with the null-to-undefined bridge', async () => {
    const wasm = fake()
    const opened = await openWasm(wasm.module, '{}')({
      frame: () => {},
      roster: () => {},
      state: () => {},
      presence: () => {},
      failed: () => {},
      closed: () => {},
    })
    await opened.backend.send(null, new Uint8Array([1]))
    await opened.backend.send('bob', new Uint8Array([2]))
    expect(wasm.sent[0]?.to).toBeUndefined()
    expect(wasm.sent[1]?.to).toBe('bob')
    await opened.backend.close()
  })
})
