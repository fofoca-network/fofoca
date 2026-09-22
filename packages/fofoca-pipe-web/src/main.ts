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
 * - `?transport=p2p,relay` — let payload fall back to the relay
 *   (id-changing, same rule).
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
 *   Writes are batched per animation frame, so a chunk is on screen within
 *   a frame of arriving rather than the instant it lands. A view holds at
 *   most `MAX_VIEW_CHARS`; past that the oldest text is dropped and
 *   `data-truncated="true"` says so. The runtime keeps every byte.
 * - `#peers` holds the roster JSON, `data-count` the peer count.
 * - `#events` (hidden) gets one `<li data-kind>` per mesh event.
 * - `window.pipe` — the runtime's methods, promise-returning where they are.
 */

import type { Mesh, Transport } from 'fofoca-wasm'
import { join } from 'fofoca-wasm'

import type { Pending, ViewKey } from './render.ts'
import { Batcher, MAX_VIEW_CHARS, fit } from './render.ts'
import type { PipeRuntime as Runtime } from './runtime.ts'
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

/** How often the view is flushed when animation frames are not running. */
const HIDDEN_FLUSH_MS = 500

function byId(id: string): HTMLElement {
  const element = document.getElementById(id)
  if (!element) {
    throw new Error(`pipe page is missing #${id}`)
  }
  return element
}

/** The `<pre>` for one stream, created on first sight. */
function streamView(key: ViewKey): HTMLPreElement {
  const streams = byId('streams')
  const selector = `pre[data-from="${CSS.escape(key.from)}"][data-directed="${key.directed}"][data-self="${key.self}"]`
  const existing = streams.querySelector<HTMLPreElement>(selector)
  if (existing) {
    return existing
  }
  const view = document.createElement('pre')
  view.dataset['from'] = key.from
  view.dataset['directed'] = String(key.directed)
  view.dataset['self'] = String(key.self)
  view.dataset['complete'] = 'false'
  streams.append(view)
  return view
}

/**
 * Apply one batch to its view: at most one text write and one scroll, and
 * the scroll only when the reader was already at the bottom. Reading
 * `scrollHeight` is what forces the layout, so it happens once here rather
 * than once per chunk.
 */
function apply(pending: Pending): void {
  const view = streamView(pending.key)
  if (pending.text !== '') {
    const atBottom = view.scrollHeight - view.scrollTop - view.clientHeight < 4
    const held = view.textContent ?? ''
    const { append, dropChars } = fit(held.length, pending.text, MAX_VIEW_CHARS)
    if (dropChars > 0) {
      view.textContent = held.slice(dropChars) + append
      view.dataset['truncated'] = 'true'
    } else {
      view.append(append)
    }
    if (atBottom) {
      view.scrollTop = view.scrollHeight
    }
  }
  if (pending.complete !== undefined) {
    view.dataset['complete'] = String(pending.complete)
  }
}

/**
 * Drive a batcher from animation frames, and from a timer as well.
 *
 * A hidden tab gets no animation frames at all, so a frame-only loop leaves
 * its view arbitrarily stale and its batch growing. Timers are throttled
 * there rather than stopped, so the slow leg keeps the view roughly current
 * whatever the tab is doing. Flushing twice is harmless: the second flush
 * finds nothing.
 */
function paint(batcher: Batcher): void {
  const flush = (): void => {
    for (const pending of batcher.flush()) {
      apply(pending)
    }
  }
  const frame = (): void => {
    flush()
    requestAnimationFrame(frame)
  }
  requestAnimationFrame(frame)
  setInterval(flush, HIDDEN_FLUSH_MS)
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
  // Passed through as typed: a name that is not a transport is the engine's
  // error to raise, and it names the choices.
  const transport = params.get('transport')?.split(',') as Transport[] | undefined

  let mesh: Mesh
  try {
    mesh = await join({
      ...selectors(),
      ...(nick === null ? {} : { nick }),
      ...(transport === undefined ? {} : { transport }),
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

  const batcher = new Batcher()
  paint(batcher)
  const runtime = new PipeRuntime(mesh)
  runtime.onChunk = (chunk) => {
    const key = { from: chunk.from, directed: chunk.directed, self: false }
    if (chunk.eof) {
      batcher.mark(key, true)
    } else {
      batcher.push(key, chunk.text)
      batcher.mark(key, false)
    }
  }
  runtime.onSent = (text, to) => {
    const key = { from: mesh.nick, directed: to !== undefined, self: true }
    batcher.push(key, text)
    batcher.mark(key, false)
  }
  runtime.onSentEof = (to) => {
    batcher.mark({ from: mesh.nick, directed: to !== undefined, self: true }, true)
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
    read: (opts, signal) => runtime.read(opts, signal),
    peers: () => runtime.peers(),
    status: () => runtime.status(),
    stateGet: () => runtime.stateGet(),
    stateMerge: (patch) => runtime.stateMerge(patch),
    close: () => runtime.close(),
  }

  const input = byId('input') as HTMLTextAreaElement
  const own = () => streamView({ from: mesh.nick, directed: false, self: true })
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
