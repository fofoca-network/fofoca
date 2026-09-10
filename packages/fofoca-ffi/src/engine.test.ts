import { describe, expect, test } from 'bun:test'
import type { NativeLibrary, NativePointer, NativeValue } from './dlopen/native.ts'
import { createEngine } from './engine.ts'
import { encodeFrame } from './frame.ts'
import type { Command, FromWorker, OpenReply, WireOpts } from './protocol.ts'

const OPTS: WireOpts = {
  mesh: null,
  topic: null,
  nick: 'ana',
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

const OPEN: Command = { t: 'open', id: 1, opts: OPTS, lib: '/fake' }

interface FakeFrame {
  readonly nick: string
  readonly directed: boolean
  readonly eof: boolean
  readonly payload: Uint8Array
}

/**
 * A `NativeLibrary` whose pointers are the strings they point at, so
 * `readCString` is the identity and NULL is `null`.
 */
function fakeLib(
  overrides: {
    openFails?: boolean
    sendResult?: number
    recv?: (FakeFrame | 'fail')[]
    lastError?: string
  } = {},
) {
  const utf8 = new TextEncoder()
  const state = {
    roster: '{"count":1,"peers":[]}',
    state: '{}',
    sends: [] as { to: string | null; bytes: Uint8Array }[],
    closes: 0,
  }
  const recvQueue = overrides.recv ?? []

  const writeDocument = (doc: string, args: NativeValue[]): bigint => {
    const bytes = utf8.encode(doc)
    const buffer = args[1] as Uint8Array
    const cap = Number(args[2] as bigint)
    if (bytes.byteLength < cap) {
      buffer.set(bytes, 0)
    }
    return BigInt(bytes.byteLength)
  }

  const lib: NativeLibrary = {
    call: (name, ...args) => {
      switch (name) {
        case 'fofoca_max_chunk':
          return 64n
        case 'fofoca_id':
          return 'mesh-id'
        case 'fofoca_name':
          return 'mesh-name'
        case 'fofoca_nickname':
          return 'ana'
        case 'fofoca_version':
          return '0.0.0-test'
        case 'fofoca_last_error':
          return overrides.lastError ?? 'boom'
        case 'fofoca_peers_json':
          return writeDocument(state.roster, args)
        case 'fofoca_state_json':
          return writeDocument(state.state, args)
        case 'fofoca_send': {
          state.sends.push({ to: args[1] as string | null, bytes: (args[2] as Uint8Array).slice() })
          return overrides.sendResult ?? 0
        }
        case 'fofoca_send_eof':
          return 0
        case 'fofoca_state_merge': {
          state.state = `{"merged":${JSON.stringify(args[1] as string)}}`
          return 0
        }
        case 'fofoca_recv': {
          const next = recvQueue.shift()
          if (next === undefined) {
            return 0n
          }
          if (next === 'fail') {
            return -1n
          }
          const payload = args[1] as Uint8Array
          payload.set(next.payload, 0)
          const meta = args[4] as Uint8Array
          meta.set(
            encodeFrame({
              nick: next.nick,
              directed: next.directed,
              eof: next.eof,
              len: next.payload.byteLength,
            }),
            0,
          )
          return 1n
        }
        case 'fofoca_close': {
          state.closes += 1
          return 0
        }
        case 'fofoca_open':
        case 'fofoca_peer_count':
          throw new Error(`${name} is not part of the engine's vocabulary`)
        default:
          name satisfies never
          throw new Error('unreachable')
      }
    },
    readCString: (pointer: NativePointer) => pointer as string,
    isNull: (pointer: NativePointer) => pointer === null,
    open: () => (overrides.openFails ? null : 'handle'),
    close: () => {},
  }
  return { lib, state }
}

function fakePort() {
  const posted: FromWorker[] = []
  const transfers: ArrayBuffer[][] = []
  return {
    posted,
    transfers,
    port: {
      post: (message: FromWorker, transfer?: ArrayBuffer[]) => {
        posted.push(message)
        transfers.push(transfer ?? [])
      },
    },
  }
}

describe('createEngine', () => {
  test('open assembles the whole reply', () => {
    const { lib } = fakeLib()
    const { port, posted } = fakePort()
    const engine = createEngine(lib, port, () => 0)
    expect(engine.handle(OPEN)).toBe('idle')
    expect(posted).toHaveLength(1)
    const reply = posted[0] as { t: 'ok'; id: number; value: OpenReply }
    expect(reply.t).toBe('ok')
    expect(reply.id).toBe(1)
    expect(reply.value).toEqual({
      id: 'mesh-id',
      name: 'mesh-name',
      nick: 'ana',
      rosterJson: '{"count":1,"peers":[]}',
      stateJson: '{}',
      maxChunk: 64,
      version: '0.0.0-test',
    })
  })

  test('a failed open reports the thread-local error', () => {
    const { lib } = fakeLib({ openFails: true, lastError: 'no such mesh' })
    const { port, posted } = fakePort()
    createEngine(lib, port, () => 0).handle(OPEN)
    expect(posted[0]).toEqual({ t: 'err', id: 1, message: 'fofoca_open: no such mesh' })
  })

  test('a received frame crosses with its payload transferred', () => {
    const payload = new TextEncoder().encode('hi')
    const { lib } = fakeLib({ recv: [{ nick: 'bo', directed: true, eof: false, payload }] })
    const { port, posted, transfers } = fakePort()
    const engine = createEngine(lib, port, () => 0)
    engine.handle(OPEN)
    expect(engine.pumpOnce()).toBe('idle')
    const frame = posted[1] as Extract<FromWorker, { t: 'frame' }>
    expect(frame.t).toBe('frame')
    expect(frame.from).toBe('bo')
    expect(frame.directed).toBe(true)
    expect(frame.eof).toBe(false)
    expect(Array.from(new Uint8Array(frame.bytes))).toEqual(Array.from(payload))
    expect(transfers[1]).toEqual([frame.bytes])
  })

  test('a recv failure closes the mesh and the handle', () => {
    const { lib, state } = fakeLib({ recv: ['fail'], lastError: 'engine gone' })
    const { port, posted } = fakePort()
    const engine = createEngine(lib, port, () => 0)
    engine.handle(OPEN)
    expect(engine.pumpOnce()).toBe('closed')
    expect(posted[1]).toEqual({ t: 'closed', reason: 'fofoca_recv: engine gone' })
    expect(state.closes).toBe(1)
    // Terminal for commands too.
    engine.handle({ t: 'sendEof', id: 9, to: null })
    expect(posted[2]).toEqual({ t: 'err', id: 9, message: 'the mesh is closed' })
  })

  test('send forwards bytes and replies; a failure carries the error', () => {
    const { lib, state } = fakeLib({ sendResult: 0 })
    const { port, posted } = fakePort()
    const engine = createEngine(lib, port, () => 0)
    engine.handle(OPEN)
    const bytes = new TextEncoder().encode('hello')
    engine.handle({ t: 'send', id: 2, to: 'bo', bytes: bytes.slice().buffer as ArrayBuffer })
    expect(posted[1]).toEqual({ t: 'ok', id: 2, value: null })
    expect(state.sends).toHaveLength(1)
    expect(state.sends[0]?.to).toBe('bo')
    expect(Array.from(state.sends[0]?.bytes ?? [])).toEqual(Array.from(bytes))
  })

  test('stateMerge replies with the resulting document', () => {
    const { lib } = fakeLib()
    const { port, posted } = fakePort()
    const engine = createEngine(lib, port, () => 0)
    engine.handle(OPEN)
    engine.handle({ t: 'stateMerge', id: 2, json: '{"a":1}' })
    expect(posted[1]).toEqual({ t: 'ok', id: 2, value: '{"merged":"{\\"a\\":1}"}' })
  })

  test('roster and state are re-read on the poll cadence, not sooner', () => {
    let clock = 0
    const { lib } = fakeLib()
    const { port, posted } = fakePort()
    const engine = createEngine(lib, port, () => clock)
    engine.handle(OPEN)
    clock = 499
    engine.pumpOnce()
    expect(posted).toHaveLength(1) // just the open reply
    clock = 500
    engine.pumpOnce()
    expect(posted[1]).toEqual({ t: 'roster', json: '{"count":1,"peers":[]}' })
    expect(posted[2]).toEqual({ t: 'state', json: '{}' })
    // The cadence rebased on the poll, not on open.
    clock = 999
    engine.pumpOnce()
    expect(posted).toHaveLength(3)
  })

  test('close releases the handle, replies, then announces', () => {
    const { lib, state } = fakeLib()
    const { port, posted } = fakePort()
    const engine = createEngine(lib, port, () => 0)
    engine.handle(OPEN)
    expect(engine.handle({ t: 'close', id: 2 })).toBe('closed')
    expect(state.closes).toBe(1)
    expect(posted[1]).toEqual({ t: 'ok', id: 2, value: null })
    expect(posted[2]).toEqual({ t: 'closed', reason: 'left the mesh' })
  })
})
