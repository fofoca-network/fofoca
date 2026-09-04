/**
 * The command line. Exactly one way in — a topic, a mesh id, or a create —
 * because `fofoca_open` treats "neither selector" as create and a chat that
 * silently creates a loopback mesh of one hears nobody, forever.
 */

import { parseArgs } from 'node:util'
import type { CreateOpts, JoinOpts } from 'fofoca-ffi'

export const USAGE = `usage: bun src/main.ts <how to reach the mesh> [options]

  one of:
    --topic <string>    join the public mesh derived from a shared string
    --id <base58>       join a mesh by id (the id is a bearer credential)
    --create            create a fresh mesh; reach it via the flags below

  create-only discovery flags:
    --public            the all-on preset (mDNS + DHT + relay)
    --mdns --dht --relay  individual lookups; none of them = loopback only
    --name <string>     the mesh name

  options:
    --nick <string>     nickname (default: random word-word)
    --max-peers <n>     active-view cap
    --json              automation mode: NDJSON events out, commands in
`

export type Entry =
  | { kind: 'join'; opts: JoinOpts }
  | { kind: 'create'; opts: CreateOpts }

export interface ChatArgs {
  entry: Entry
  json: boolean
}

export function parseChatArgs(argv: string[]): ChatArgs {
  const { values } = parseArgs({
    args: argv,
    strict: true,
    options: {
      topic: { type: 'string' },
      id: { type: 'string' },
      create: { type: 'boolean' },
      public: { type: 'boolean' },
      mdns: { type: 'boolean' },
      dht: { type: 'boolean' },
      relay: { type: 'boolean' },
      name: { type: 'string' },
      nick: { type: 'string' },
      'max-peers': { type: 'string' },
      json: { type: 'boolean' },
    },
  })

  const selectors = [values.topic !== undefined, values.id !== undefined, values.create === true]
  if (selectors.filter(Boolean).length !== 1) {
    throw new Error('pass exactly one of --topic, --id or --create')
  }

  const discoveryFlags = values.public || values.mdns || values.dht || values.relay
  if (values.create !== true && (discoveryFlags === true || values.name !== undefined)) {
    throw new Error('--public/--mdns/--dht/--relay/--name only apply to --create')
  }

  let maxPeers: number | undefined
  if (values['max-peers'] !== undefined) {
    maxPeers = Number(values['max-peers'])
    if (!Number.isInteger(maxPeers) || maxPeers < 1) {
      throw new Error('--max-peers takes a positive integer')
    }
  }

  const common = {
    ...(values.nick === undefined ? {} : { nick: values.nick }),
    ...(maxPeers === undefined ? {} : { maxPeers }),
  }

  let entry: Entry
  if (values.create === true) {
    entry = {
      kind: 'create',
      opts: {
        ...common,
        ...(values.name === undefined ? {} : { name: values.name }),
        ...(values.public === undefined ? {} : { public: values.public }),
        ...(values.mdns === undefined ? {} : { mdns: values.mdns }),
        ...(values.dht === undefined ? {} : { dht: values.dht }),
        ...(values.relay === undefined ? {} : { relay: values.relay }),
      },
    }
  } else if (values.topic !== undefined) {
    entry = { kind: 'join', opts: { ...common, topic: values.topic } }
  } else {
    entry = { kind: 'join', opts: { ...common, id: values.id as string } }
  }

  return { entry, json: values.json === true }
}
