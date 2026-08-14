import { describe, expect, test } from 'bun:test'

import type { BackendSink, Opener } from './backend.ts'
import { openMesh } from './mesh.ts'
import type { Mesh, MeshEvent, Message } from './types.ts'

function roster(...peers: string[]): string {
  return JSON.stringify({
    peers: peers.map((nickname) => ({
      nickname,
      last_seen_secs_ago: 1,
      quiet: false,
      reach: 'direct',
      transport: 'unicast',
    })),
    count: peers.length + 1,
  })
}

interface Fake {
  readonly mesh: Mesh
  readonly sink: BackendSink
  readonly sent: { to: string | null; bytes: Uint8Array }[]
  readonly eofs: (string | null)[]
  closes: number
  /** What the next `stateMerge` resolves with. */
  nextState: string
}

async function fake(
  options: { pushesPresence?: boolean; peers?: string[]; state?: string } = {},
): Promise<Fake> {
  let captured: BackendSink | undefined
  const sent: Fake['sent'] = []
  const eofs: Fake['eofs'] = []
  const handle = {
    sent,
    eofs,
    closes: 0,
    nextState: options.state ?? '{}',
  } as Fake & { mesh: Mesh; sink: BackendSink }

  const open: Opener = (sink) => {
    captured = sink
    return Promise.resolve({
      backend: {
        id: 'mesh-id',
        name: 'a-name',
        nick: 'assigned',
        maxChunk: 2112,
        send: (to, bytes) => {
          sent.push({ to, bytes })
          return Promise.resolve()
        },
        sendEof: (to) => {
          eofs.push(to)
          return Promise.resolve()
        },
        stateMerge: () => Promise.resolve(handle.nextState),
        close: () => {
          handle.closes += 1
          return Promise.resolve()
        },
      },
      rosterJson: roster(...(options.peers ?? [])),
      stateJson: options.state ?? '{}',
      pushesPresence: options.pushesPresence ?? false,
    })
  }

  const mesh = await openMesh(open)
  return Object.assign(handle, { mesh, sink: captured as BackendSink })
}

async function drain<T>(iterable: AsyncIterable<T>, count: number): Promise<T[]> {
  const taken: T[] = []
  for await (const value of iterable) {
    taken.push(value)
    if (taken.length === count) {
      break
    }
  }
  return taken
}

describe('identity and the opening snapshot', () => {
  test('the nickname is the one the engine assigned', async () => {
    const { mesh } = await fake()
    expect(mesh.nick).toBe('assigned')
    expect(mesh.id).toBe('mesh-id')
    expect(mesh.name).toBe('a-name')
    expect(mesh.maxChunk).toBe(2112)
  })

  test('peers and state are populated before the first push', async () => {
    const { mesh } = await fake({ peers: ['ana'], state: '{"topic":"standup"}' })
    expect(mesh.peers.map((peer) => peer.nick)).toEqual(['ana'])
    expect(mesh.state.value).toEqual({ topic: 'standup' })
  })

  test('the opening roster is a baseline, not a room full of arrivals', async () => {
    const { mesh, sink } = await fake({ peers: ['ana', 'bo'] })
    const events = drain(mesh.events(), 2)
    sink.roster(roster('ana', 'bo', 'cy'))

    expect(await events).toEqual([{ kind: 'ready' }, { kind: 'joined', nick: 'cy' }])
  })
})

describe('messages', () => {
  test('bytes that decode as UTF-8 carry text', async () => {
    const { mesh, sink } = await fake()
    const messages = drain(mesh.messages(), 1)
    sink.frame({
      from: 'ana',
      bytes: new TextEncoder().encode('hello'),
      directed: false,
      eof: false,
    })

    expect(await messages).toEqual([
      {
        from: 'ana',
        bytes: new TextEncoder().encode('hello'),
        text: 'hello',
        directed: false,
        eof: false,
      },
    ] satisfies Message[])
  })

  test('bytes that do not decode omit text rather than mangling it', async () => {
    const { mesh, sink } = await fake()
    const messages = drain(mesh.messages(), 1)
    sink.frame({ from: 'ana', bytes: new Uint8Array([0xff, 0xfe]), directed: true, eof: false })

    const [message] = await messages
    expect(message).not.toHaveProperty('text')
    expect(message?.directed).toBe(true)
  })

  test('a string body is encoded; bytes pass through', async () => {
    const { mesh, sent } = await fake()
    await mesh.send('hi')
    await mesh.send(new Uint8Array([1, 2]), { to: 'ana' })

    expect(sent).toEqual([
      { to: null, bytes: new TextEncoder().encode('hi') },
      { to: 'ana', bytes: new Uint8Array([1, 2]) },
    ])
  })

  test('sendEof reaches the backend', async () => {
    const { mesh, eofs } = await fake()
    await mesh.sendEof()
    await mesh.sendEof({ to: 'ana' })

    expect(eofs).toEqual([null, 'ana'])
  })
})

describe('presence', () => {
  test('a backend that cannot be told gets joins from the roster diff', async () => {
    const { mesh, sink } = await fake({ pushesPresence: false, peers: ['ana'] })
    const events = drain(mesh.events(), 3)
    sink.roster(roster('ana', 'bo'))
    sink.roster(roster('bo'))

    expect(await events).toEqual([
      { kind: 'ready' },
      { kind: 'joined', nick: 'bo' },
      { kind: 'left', nick: 'ana' },
    ])
  })

  test('a backend that is told keeps its own events and the diff stays quiet', async () => {
    const { mesh, sink } = await fake({ pushesPresence: true, peers: ['ana'] })
    const events = drain(mesh.events(), 2)
    sink.presence({ kind: 'joined', nick: 'bo' })
    // The roster catches up afterwards; it must not announce `bo` a second time.
    sink.roster(roster('ana', 'bo'))

    expect(await events).toEqual([{ kind: 'ready' }, { kind: 'joined', nick: 'bo' }])
    expect(mesh.peers.map((peer) => peer.nick)).toEqual(['ana', 'bo'])
  })

  test('an identical roster is discarded without reparsing', async () => {
    const { mesh, sink } = await fake({ peers: ['ana'] })
    const events = drain(mesh.events(), 2)
    sink.roster(roster('ana'))
    sink.roster(roster('ana', 'bo'))

    expect(await events).toEqual([{ kind: 'ready' }, { kind: 'joined', nick: 'bo' }])
  })

  test('an unreadable roster is an error and the old one survives', async () => {
    const { mesh, sink } = await fake({ peers: ['ana'] })
    const events = drain(mesh.events(), 2)
    sink.roster('{"error":"the loop stopped"}')

    const [, second] = await events
    expect(second?.kind).toBe('error')
    expect(mesh.peers.map((peer) => peer.nick)).toEqual(['ana'])
  })
})

describe('state', () => {
  test('merge resolves before the value is readable back', async () => {
    const handle = await fake({ state: '{}' })
    handle.nextState = '{"topic":"lunch"}'
    await handle.mesh.state.merge({ topic: 'lunch' })

    expect(handle.mesh.state.value).toEqual({ topic: 'lunch' })
  })

  test('a change reaches every changes() iterator', async () => {
    const { mesh, sink } = await fake()
    const changes = drain(mesh.state.changes(), 1)
    sink.state('{"topic":"standup"}')

    expect(await changes).toEqual([{ topic: 'standup' }])
  })

  test('an unchanged document is not a change', async () => {
    const { mesh, sink } = await fake({ state: '{"a":1}' })
    const changes = drain(mesh.state.changes(), 1)
    sink.state('{"a":1}')
    sink.state('{"a":2}')

    expect(await changes).toEqual([{ a: 2 }])
  })

  test('the value is frozen all the way down', async () => {
    const { mesh, sink } = await fake()
    sink.state('{"nested":{"key":"value"}}')

    expect(Object.isFrozen(mesh.state.value)).toBe(true)
    expect(Object.isFrozen((mesh.state.value as { nested: object }).nested)).toBe(true)
  })
})

describe('lifecycle', () => {
  test('ready is replayed, so it is observable at all', async () => {
    const { mesh } = await fake()
    expect(await drain(mesh.events(), 1)).toEqual([{ kind: 'ready' }])
  })

  test('an events() iterator opened after close reports closed and ends', async () => {
    const { mesh } = await fake()
    await mesh.leave()

    const seen: MeshEvent[] = []
    for await (const event of mesh.events()) {
      seen.push(event)
    }
    expect(seen).toEqual([{ kind: 'closed', reason: 'left the mesh' }])
  })

  test('leaving ends the message stream', async () => {
    const { mesh } = await fake()
    const messages = mesh.messages()
    await mesh.leave()

    const seen: Message[] = []
    for await (const message of messages) {
      seen.push(message)
    }
    expect(seen).toEqual([])
  })

  test('leave is idempotent and closes the backend once', async () => {
    const handle = await fake()
    await handle.mesh.leave()
    await handle.mesh.leave()

    expect(handle.closes).toBe(1)
  })

  test('sending after leaving is refused with a reason', async () => {
    const { mesh } = await fake()
    await mesh.leave()

    await expect(mesh.send('hi')).rejects.toThrow(/has been left/)
  })

  test('a backend-reported failure is an error, and the mesh stays open', async () => {
    const { mesh, sink } = await fake()
    const events = drain(mesh.events(), 2)
    sink.failed('fofoca_recv failed: out of memory')

    expect(await events).toEqual([
      { kind: 'ready' },
      { kind: 'error', message: 'fofoca_recv failed: out of memory' },
    ])
    await mesh.send('still works')
  })

  test('a backend that closes underneath us ends every stream once', async () => {
    const { mesh, sink } = await fake()
    const events = drain(mesh.events(), 2)
    sink.closed('the worker exited')
    sink.closed('and again')

    expect(await events).toEqual([
      { kind: 'ready' },
      { kind: 'closed', reason: 'the worker exited' },
    ])
  })

  test('await using disposes the mesh', async () => {
    const handle = await fake()
    {
      await using disposable = handle.mesh
      expect(disposable.id).toBe('mesh-id')
    }
    expect(handle.closes).toBe(1)
  })
})
