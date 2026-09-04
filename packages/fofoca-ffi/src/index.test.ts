import { describe, expect, test } from 'bun:test'
import type { BackendFrame, BackendSink } from 'fofoca-api'
import { createWire, ffiOpener, joinWire } from './index.ts'
import type { Command, FromWorker, OpenReply } from './protocol.ts'
import type { WorkerHost } from './worker-host.ts'

const OPEN_REPLY: OpenReply = {
  id: 'mesh-id',
  name: 'mesh-name',
  nick: 'ana',
  rosterJson: '{"count":1,"peers":[]}',
  stateJson: '{}',
  maxChunk: 64,
  version: '0.0.0-test',
}

function fakeHost(respond: (command: Command, emit: (message: FromWorker) => void) => void) {
  const posted: Command[] = []
  let handler: (message: FromWorker) => void = () => {}
  let terminated = 0
  const host: WorkerHost = {
    post: (command) => {
      posted.push(command)
      respond(command, (message) => handler(message))
    },
    onMessage: (h) => {
      handler = h
    },
    terminate: async () => {
      terminated += 1
    },
  }
  return {
    host,
    posted,
    emit: (message: FromWorker) => handler(message),
    terminatedCount: () => terminated,
  }
}

const openOnly = (command: Command, emit: (message: FromWorker) => void) => {
  if (command.t === 'open') {
    emit({ t: 'ok', id: command.id, value: OPEN_REPLY })
  }
}

function recordingSink() {
  const frames: BackendFrame[] = []
  const rosters: string[] = []
  const states: string[] = []
  const failures: string[] = []
  const closes: string[] = []
  const sink: BackendSink = {
    frame: (frame) => frames.push(frame),
    roster: (json) => rosters.push(json),
    state: (json) => states.push(json),
    presence: () => {},
    failed: (message) => failures.push(message),
    closed: (reason) => closes.push(reason),
  }
  return { sink, frames, rosters, states, failures, closes }
}

describe('ffiOpener', () => {
  test('open crosses the worker and fills BackendOpen', async () => {
    const { host, posted } = fakeHost(openOnly)
    const { sink } = recordingSink()
    const opened = await ffiOpener(joinWire({ topic: 'tea' }), {
      spawn: async () => host,
      lib: '/fake/libfofoca_ffi.dylib',
    })(sink)
    expect(posted[0]).toMatchObject({ t: 'open', lib: '/fake/libfofoca_ffi.dylib' })
    expect(opened.backend.id).toBe('mesh-id')
    expect(opened.backend.nick).toBe('ana')
    expect(opened.backend.maxChunk).toBe(64)
    expect(opened.rosterJson).toBe(OPEN_REPLY.rosterJson)
    expect(opened.stateJson).toBe(OPEN_REPLY.stateJson)
    expect(opened.pushesPresence).toBe(false)
  })

  test('a failed open rejects and tears the worker down', async () => {
    const { host, terminatedCount } = fakeHost((command, emit) => {
      emit({ t: 'err', id: command.id, message: 'no such mesh' })
    })
    const { sink } = recordingSink()
    await expect(
      ffiOpener(joinWire({ topic: 'tea' }), { spawn: async () => host, lib: '/fake' })(sink),
    ).rejects.toThrow('no such mesh')
    expect(terminatedCount()).toBe(1)
  })

  test('pushed frames, rosters and states reach the sink', async () => {
    const { host, emit } = fakeHost(openOnly)
    const recorder = recordingSink()
    await ffiOpener(joinWire({ topic: 'tea' }), { spawn: async () => host, lib: '/fake' })(
      recorder.sink,
    )
    const payload = new TextEncoder().encode('hi')
    emit({ t: 'frame', from: 'bo', directed: false, eof: false, bytes: payload.slice().buffer as ArrayBuffer })
    emit({ t: 'roster', json: '{"count":2,"peers":[]}' })
    emit({ t: 'state', json: '{"a":1}' })
    emit({ t: 'failed', message: 'poll hiccup' })
    expect(recorder.frames).toHaveLength(1)
    expect(recorder.frames[0]?.from).toBe('bo')
    expect(Array.from(recorder.frames[0]?.bytes ?? [])).toEqual(Array.from(payload))
    expect(recorder.rosters).toEqual(['{"count":2,"peers":[]}'])
    expect(recorder.states).toEqual(['{"a":1}'])
    expect(recorder.failures).toEqual(['poll hiccup'])
  })

  test('send copies the payload before transferring it', async () => {
    const { host, posted } = fakeHost((command, emit) => {
      if (command.t === 'open' || command.t === 'send') {
        emit({ t: 'ok', id: command.id, value: command.t === 'open' ? OPEN_REPLY : null })
      }
    })
    const { sink } = recordingSink()
    const opened = await ffiOpener(joinWire({ topic: 'tea' }), {
      spawn: async () => host,
      lib: '/fake',
    })(sink)
    const bytes = new TextEncoder().encode('hello')
    await opened.backend.send('bo', bytes)
    const sent = posted[1] as Extract<Command, { t: 'send' }>
    expect(sent.to).toBe('bo')
    expect(sent.bytes).not.toBe(bytes.buffer)
    expect(Array.from(new Uint8Array(sent.bytes))).toEqual(Array.from(bytes))
    // The caller's view is untouched.
    expect(bytes.byteLength).toBe(5)
  })

  test('close awaits the worker reply and terminates it', async () => {
    const { host, terminatedCount } = fakeHost((command, emit) => {
      if (command.t === 'open') {
        emit({ t: 'ok', id: command.id, value: OPEN_REPLY })
      }
      if (command.t === 'close') {
        emit({ t: 'ok', id: command.id, value: null })
        emit({ t: 'closed', reason: 'left the mesh' })
      }
    })
    const recorder = recordingSink()
    const opened = await ffiOpener(joinWire({ topic: 'tea' }), {
      spawn: async () => host,
      lib: '/fake',
    })(recorder.sink)
    await opened.backend.close()
    expect(recorder.closes).toEqual(['left the mesh'])
    expect(terminatedCount()).toBe(1)
  })

  test('an unsolicited closed rejects everything still pending', async () => {
    let emitOutside: (message: FromWorker) => void = () => {}
    const { host, emit } = fakeHost((command, emit2) => {
      if (command.t === 'open') {
        emit2({ t: 'ok', id: command.id, value: OPEN_REPLY })
      }
      // A send never answered: the engine died first.
    })
    emitOutside = emit
    const recorder = recordingSink()
    const opened = await ffiOpener(joinWire({ topic: 'tea' }), {
      spawn: async () => host,
      lib: '/fake',
    })(recorder.sink)
    const hanging = opened.backend.send(null, new Uint8Array([1]))
    emitOutside({ t: 'closed', reason: 'engine gone' })
    await expect(hanging).rejects.toThrow('the mesh closed: engine gone')
    expect(recorder.closes).toEqual(['engine gone'])
  })
})

describe('wire opts', () => {
  test('join requires exactly one selector', () => {
    expect(() => joinWire({})).toThrow('join needs a topic or an id')
    expect(() => joinWire({ topic: 't', id: 'm' })).toThrow('not both')
    expect(joinWire({ topic: 't' })).toMatchObject({ topic: 't', mesh: null, isPublic: false })
    expect(joinWire({ id: 'm', nick: 'ana', maxPeers: 3 })).toMatchObject({
      mesh: 'm',
      nick: 'ana',
      maxPeers: 3,
    })
  })

  test('create maps the discovery flags and defaults', () => {
    expect(createWire({})).toEqual({
      mesh: null,
      topic: null,
      nick: null,
      name: null,
      isPublic: false,
      mdns: false,
      dht: false,
      relay: false,
      relayTransport: false,
      relayUrls: null,
      disableIp: false,
      disableWebrtc: false,
      maxPeers: 0,
    })
    expect(createWire({ public: true, name: 'salon' })).toMatchObject({
      isPublic: true,
      name: 'salon',
    })
  })

  test('relay ladder, relay transport and transport switches', () => {
    expect(joinWire({ topic: 't', relayTransport: true, relayUrls: ['http://a/', 'http://b/'] })).toMatchObject({
      relayTransport: true,
      relayUrls: 'http://a/,http://b/',
    })
    expect(createWire({ relayUrls: [] })).toMatchObject({ relayUrls: null })
    expect(createWire({ transports: { webrtc: false } })).toMatchObject({
      disableIp: false,
      disableWebrtc: true,
    })
  })
})
