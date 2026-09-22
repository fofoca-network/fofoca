/**
 * The pipe page: join the mesh the URL names, show every stream as it
 * arrives, and hand the runtime to whoever drives this tab — a person at the
 * composer, an agent through WebMCP, or the e2e driver through `window.pipe`.
 *
 * URL contract. The selector rides in the **fragment**, which a browser never
 * sends to the server, so a mesh id stays out of access logs:
 * - `#mesh=<id>` or `#topic=<string>` — exactly one. The query string is
 *   read as a fallback for both.
 * - `?nick=` — this peer's nickname.
 * - `?relay=` — a custom relay URL (repeatable); with `topic` it is part of
 *   the derived id, so every member must pass the same.
 * - `?relayTransport=1` — let payload fall back to the relay (id-changing,
 *   same rule).
 * - `?log=` — an `EnvFilter` for the engine's tracing, to the console.
 *
 * DOM contract (what the driver — or a person — reads):
 * - `#status` holds `#ready` (with `data-id`/`data-nick`) once the mesh is
 *   open, or `#failed` with the error text.
 * - `#webmcp` says whether the tools registered (`data-registered`).
 * - `#streams` gets one `<pre data-from data-directed data-complete>` per
 *   (author, directed) stream, text appended in order; own sends — from the
 *   composer, `window.pipe` or a tool alike — land in a
 *   `<pre data-self="true">` (`data-directed="true"` for a directed one).
 * - `#peers` holds the roster JSON, `data-count` the peer count.
 * - `#events` (hidden) gets one `<li data-kind>` per mesh event.
 * - `window.pipe` — the runtime's methods, promise-returning where they are.
 */

import type { Mesh } from 'fofoca-wasm'
import { join } from 'fofoca-wasm'

import type { Chunk, PipeRuntime as Runtime } from './runtime.ts'
import { PipeRuntime } from './runtime.ts'
import { registerPipeTools } from './webmcp.ts'

declare global {
  interface Window {
    pipe?: Pick<
      Runtime,
      'send' | 'sendEof' | 'read' | 'peers' | 'status' | 'stateGet' | 'stateMerge' | 'close'
    >
  }
}

function byId(id: string): HTMLElement {
  const element = document.getElementById(id)
  if (!element) {
    throw new Error(`pipe page is missing #${id}`)
  }
  return element
}

/** The `<pre>` for one stream, created on first sight. */
function streamView(from: string, directed: boolean, self: boolean): HTMLPreElement {
  const streams = byId('streams')
  const selector = `pre[data-from="${CSS.escape(from)}"][data-directed="${directed}"][data-self="${self}"]`
  const existing = streams.querySelector<HTMLPreElement>(selector)
  if (existing) {
    return existing
  }
  const view = document.createElement('pre')
  view.dataset['from'] = from
  view.dataset['directed'] = String(directed)
  view.dataset['self'] = String(self)
  view.dataset['complete'] = 'false'
  streams.append(view)
  return view
}

function showChunk(chunk: Chunk): void {
  const view = streamView(chunk.from, chunk.directed, false)
  if (chunk.eof) {
    view.dataset['complete'] = 'true'
    return
  }
  view.dataset['complete'] = 'false'
  view.append(chunk.text)
  view.scrollTop = view.scrollHeight
}

function selectors(): { id?: string; topic?: string } {
  const fragment = new URLSearchParams(window.location.hash.slice(1))
  const query = new URLSearchParams(window.location.search)
  const id = fragment.get('mesh') ?? query.get('mesh')
  const topic = fragment.get('topic') ?? query.get('topic')
  return { ...(id === null ? {} : { id }), ...(topic === null ? {} : { topic }) }
}

async function main(): Promise<void> {
  const params = new URLSearchParams(window.location.search)
  const nick = params.get('nick')

  let mesh: Mesh
  try {
    mesh = await join({
      ...selectors(),
      ...(nick === null ? {} : { nick }),
      relayTransport: params.get('relayTransport') === '1',
      relayUrls: params.getAll('relay'),
      log: params.get('log') ?? 'warn',
    })
  } catch (error) {
    const failed = document.createElement('div')
    failed.id = 'failed'
    failed.textContent = String(error)
    byId('status').replaceChildren(failed)
    return
  }

  const ready = document.createElement('div')
  ready.id = 'ready'
  ready.dataset['id'] = mesh.id
  ready.dataset['nick'] = mesh.nick
  ready.textContent = `${mesh.nick} · ${mesh.name}`
  byId('status').replaceChildren(ready)

  const runtime = new PipeRuntime(mesh)
  runtime.onChunk = showChunk
  runtime.onSent = (text, to) => {
    const own = streamView(mesh.nick, to !== undefined, true)
    own.dataset['complete'] = 'false'
    own.append(text)
    own.scrollTop = own.scrollHeight
  }
  runtime.onSentEof = (to) => {
    streamView(mesh.nick, to !== undefined, true).dataset['complete'] = 'true'
  }

  const registered = registerPipeTools(runtime)
  byId('webmcp').dataset['registered'] = String(registered)
  byId('webmcp').textContent = registered
    ? 'WebMCP tools registered'
    : 'no WebMCP in this browser — window.pipe only'

  let lastRoster = ''
  const refreshRoster = (): void => {
    const roster = JSON.stringify(mesh.peers, null, 1)
    if (roster === lastRoster) {
      return
    }
    lastRoster = roster
    byId('peers').textContent = roster
    byId('peers').dataset['count'] = String(mesh.peers.length)
  }
  refreshRoster()
  setInterval(refreshRoster, 500)

  window.pipe = {
    send: (text, to) => runtime.send(text, to),
    sendEof: (to) => runtime.sendEof(to),
    read: (waitMs, signal) => runtime.read(waitMs, signal),
    peers: () => runtime.peers(),
    status: () => runtime.status(),
    stateGet: () => runtime.stateGet(),
    stateMerge: (patch) => runtime.stateMerge(patch),
    close: () => runtime.close(),
  }

  const input = byId('input') as HTMLTextAreaElement
  const own = () => streamView(mesh.nick, false, true)
  byId('send').addEventListener('click', () => {
    const text = input.value
    if (text === '') {
      return
    }
    input.value = ''
    void runtime.send(text).catch((error: unknown) => {
      own().append(`\n! send failed: ${String(error)}\n`)
    })
  })
  byId('eof').addEventListener('click', () => {
    void runtime.sendEof().catch((error: unknown) => {
      own().append(`\n! eof failed: ${String(error)}\n`)
    })
  })

  void (async () => {
    for await (const event of mesh.events()) {
      const item = document.createElement('li')
      item.dataset['kind'] = event.kind
      item.textContent = JSON.stringify(event)
      byId('events').append(item)
    }
  })()
}

void main()
