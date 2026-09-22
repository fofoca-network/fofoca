/** A `Mesh` for the tests: frames come in through `push`, sends are recorded. */

import type { Mesh, MeshEvent, Message, Peer } from 'fofoca-api'
import { Fanout } from 'fofoca-api'

export interface FakeMesh {
  readonly mesh: Mesh
  readonly sends: { body: string | Uint8Array; to?: string }[]
  readonly eofs: { to?: string }[]
  readonly merges: Record<string, unknown>[]
  push(message: Message): void
  end(): void
}

export function fakeMesh(peers: Peer[] = []): FakeMesh {
  const messages = new Fanout<Message>()
  const events = new Fanout<MeshEvent>()
  const changes = new Fanout<Record<string, unknown>>()
  const sends: FakeMesh['sends'] = []
  const eofs: FakeMesh['eofs'] = []
  const merges: FakeMesh['merges'] = []
  const mesh: Mesh = {
    id: 'mesh-id',
    name: 'mesh-name',
    nick: 'me',
    peers,
    maxChunk: 2094,
    state: {
      value: { a: 1 },
      merge: async (patch) => {
        merges.push(patch)
      },
      changes: (signal) => changes.iterate(signal),
    },
    send: async (body, opts) => {
      sends.push(opts?.to === undefined ? { body } : { body, to: opts.to })
    },
    sendEof: async (opts) => {
      eofs.push(opts?.to === undefined ? {} : { to: opts.to })
    },
    messages: (signal) => messages.iterate(signal),
    events: (signal) => events.iterate(signal),
    leave: async () => {
      messages.end()
      events.end()
    },
    [Symbol.asyncDispose]: async () => {
      messages.end()
      events.end()
    },
  }
  return {
    mesh,
    sends,
    eofs,
    merges,
    push: (message) => messages.push(message),
    end: () => messages.end(),
  }
}

export function frame(from: string, seq: number, text: string, directed = false): Message {
  const bytes = new TextEncoder().encode(text)
  return { from, bytes, text, directed, eof: false, seq }
}

export function eof(from: string, count: number, directed = false): Message {
  return { from, bytes: new Uint8Array(), directed, eof: true, seq: count }
}

/** Let the runtime's pump drain what was pushed. */
export function settle(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0))
}
