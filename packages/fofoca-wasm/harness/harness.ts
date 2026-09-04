/**
 * The harness page's script: join the mesh the query string names, mirror
 * everything into the DOM, and expose imperative controls on
 * `window.harness`, so a driver only ever reads elements and evaluates small
 * expressions.
 *
 * Query parameters:
 * - `topic` or `mesh` — the mesh selector (exactly one).
 * - `nick` — this peer's nickname.
 * - `relay` — a custom relay URL (repeatable); with `topic` it is part of
 *   the derived id, so the native side must pass the same.
 * - `relayTransport=1` — let payload fall back to the relay (id-changing,
 *   same rule).
 * - `log` — an `EnvFilter` for the engine's tracing, to the console.
 *
 * DOM contract (what a driver asserts on):
 * - `#ready` appears once the mesh is open, with `data-id`/`data-nick`/`data-name`.
 * - `#failed` appears instead if open threw; its text is the error.
 * - `#peers` holds the roster JSON, `data-count` the peer count (self excluded).
 * - `#frames` gets one `<li data-from data-directed data-eof>` per frame,
 *   text decoded as UTF-8.
 * - `#events` gets one `<li data-kind>` per surfaced event.
 * - `#state` holds the shared state JSON.
 * - `window.harness = { send, sendEof, stateMerge, close }`, all
 *   promise-returning.
 */

import { join } from '../src/index.ts'
import type { Mesh } from '../src/index.ts'

declare global {
  interface Window {
    harness?: {
      send(to: string | null, text: string): Promise<void>
      sendEof(to: string | null): Promise<void>
      stateMerge(json: string): Promise<void>
      close(): Promise<void>
    }
  }
}

function byId(id: string): HTMLElement {
  const element = document.getElementById(id)
  if (!element) {
    throw new Error(`harness page is missing #${id}`)
  }
  return element
}

function mark(id: string, data: Record<string, string>, text = ''): void {
  const element = document.createElement('div')
  element.id = id
  for (const [key, value] of Object.entries(data)) {
    element.dataset[key] = value
  }
  element.textContent = text
  byId('status').replaceChildren(element)
}

function appendItem(listId: string, data: Record<string, string>, text: string): void {
  const item = document.createElement('li')
  for (const [key, value] of Object.entries(data)) {
    item.dataset[key] = value
  }
  item.textContent = text
  byId(listId).append(item)
}

/** Mirror the console into `#log`, so a DOM-reading driver sees the
 * engine's tracing lines (they go to `console.log` via `initTracing`). */
function mirrorConsole(): void {
  const target = document.createElement('pre')
  target.id = 'log'
  target.style.display = 'none'
  document.body.append(target)
  const original = console.log.bind(console)
  // Buffered and flushed on a timer: rebuilding a 20KB text node per line
  // was itself enough main-thread work to starve the mesh at debug volume.
  let pending: string[] = []
  let buffered = ''
  setInterval(() => {
    if (pending.length === 0) {
      return
    }
    buffered = `${buffered}${pending.join('\n')}\n`.slice(-20000)
    pending = []
    target.textContent = buffered
  }, 250)
  console.log = (...parts: unknown[]) => {
    original(...parts)
    // Debug lines stay in the real console only: mirroring their volume
    // into the DOM costs enough main-thread time to starve the mesh.
    const line = parts.map(String).join(' ')
    if (
      line.includes('DEBUG') &&
      !line.includes('PeerInfo') &&
      !line.includes('graft') &&
      !line.includes('webrtc')
    ) {
      return
    }
    pending.push(line)
  }
}

async function main(): Promise<void> {
  mirrorConsole()
  const params = new URLSearchParams(window.location.search)
  const topic = params.get('topic')
  const id = params.get('mesh')
  const nick = params.get('nick')
  const relayUrls = params.getAll('relay')

  let mesh: Mesh
  try {
    mesh = await join({
      ...(topic === null ? {} : { topic }),
      ...(id === null ? {} : { id }),
      ...(nick === null ? {} : { nick }),
      relayTransport: params.get('relayTransport') === '1',
      relayUrls,
      log: params.get('log') ?? 'info',
    })
  } catch (error) {
    mark('failed', {}, String(error))
    return
  }

  mark('ready', { id: mesh.id, nick: mesh.nick, name: mesh.name })

  let lastRoster = ''
  const refreshRoster = (): void => {
    const roster = JSON.stringify(mesh.peers)
    if (roster === lastRoster) {
      return
    }
    lastRoster = roster
    byId('peers').textContent = roster
    byId('peers').dataset['count'] = String(mesh.peers.length)
  }
  refreshRoster()
  setInterval(refreshRoster, 500)

  window.harness = {
    send: (to, text) => mesh.send(text, to === null ? {} : { to }),
    sendEof: (to) => mesh.sendEof(to === null ? {} : { to }),
    stateMerge: async (json) => {
      await mesh.state.merge(JSON.parse(json) as Record<string, unknown>)
    },
    close: () => mesh.leave(),
  }

  void (async () => {
    for await (const message of mesh.messages()) {
      appendItem(
        'frames',
        {
          from: message.from,
          directed: String(message.directed),
          eof: String(message.eof),
        },
        message.text ?? `[${message.bytes.length} bytes]`,
      )
    }
  })()

  void (async () => {
    for await (const change of mesh.state.changes()) {
      byId('state').textContent = JSON.stringify(change)
    }
  })()

  for await (const event of mesh.events()) {
    appendItem('events', { kind: event.kind }, JSON.stringify(event))
    refreshRoster()
  }
}

void main()
