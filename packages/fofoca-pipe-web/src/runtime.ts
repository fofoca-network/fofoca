/**
 * The pipe runtime a page (or an agent through WebMCP) drives: ordered text
 * in, text out, plus the roster and the shared state document.
 *
 * DOM-free on purpose. `main.ts` hooks the DOM onto `onChunk`; the tools in
 * `webmcp.ts` call the methods; the tests drive it over a fake `Mesh`.
 *
 * Frames land shuffled and cut at ~2 KB, so every inbound frame goes through
 * `Streams` for order and through one `TextDecoder` per stream for text — a
 * chunk boundary can split a multibyte character, and a per-chunk decode would
 * corrupt it. Binary payloads come out as U+FFFD; the pipe is a text surface
 * here.
 */

import type { Delivered, Mesh, Message, Peer } from 'fofoca-api'
import { Streams } from 'fofoca-api'

/** One delivered piece of a stream, in order. An `eof` piece carries no text. */
export interface Chunk {
  readonly from: string
  readonly directed: boolean
  readonly text: string
  readonly eof: boolean
}

export interface ReadResult {
  /** Consecutive pieces of one stream merged into one item. */
  readonly items: Chunk[]
}

export interface StreamStatus {
  readonly from: string
  readonly directed: boolean
  /** Chunks delivered so far. */
  readonly received: number
  /** The stream's EOF has been delivered. Cleared when the author writes on. */
  readonly complete: boolean
}

export interface Status {
  readonly id: string
  readonly name: string
  readonly nick: string
  readonly peerCount: number
  /** Chunks waiting for the next `read`. */
  readonly unread: number
  readonly streams: StreamStatus[]
}

/** The longest a `read` waits for the first chunk. Under a browser agent's patience. */
export const MAX_WAIT_MS = 25_000
const DEFAULT_WAIT_MS = 10_000
const DEFAULT_INBOX_CAP = 4096
/** How often open holes in the streams are checked against `GAP_TIMEOUT_MS`. */
const GAP_POLL_MS = 500

interface StreamState {
  readonly decoder: TextDecoder
  readonly status: { from: string; directed: boolean; received: number; complete: boolean }
}

export class PipeRuntime {
  readonly #mesh: Mesh
  readonly #streams = new Streams()
  readonly #state = new Map<string, StreamState>()
  readonly #inboxCap: number
  #inbox: Chunk[] = []
  readonly #waiters = new Set<() => void>()
  #closed = false
  readonly #gapTimer: ReturnType<typeof setInterval>

  /** Called with every chunk as it is delivered — the live view. */
  onChunk: ((chunk: Chunk) => void) | undefined
  /** Called after every send this runtime made, whoever asked for it — the tools, the page, a script. */
  onSent: ((text: string, to: string | undefined) => void) | undefined
  /** Called after every end-of-stream this runtime sent. */
  onSentEof: ((to: string | undefined) => void) | undefined

  constructor(mesh: Mesh, opts?: { inboxCap?: number }) {
    this.#mesh = mesh
    this.#inboxCap = opts?.inboxCap ?? DEFAULT_INBOX_CAP
    this.#gapTimer = setInterval(() => this.expire(), GAP_POLL_MS)
    void this.#pump()
  }

  async #pump(): Promise<void> {
    for await (const message of this.#mesh.messages()) {
      this.ingest(message)
    }
    this.#closed = true
    clearInterval(this.#gapTimer)
    this.#wake()
  }

  /** Route one frame; public so a test can feed frames without a live mesh. */
  ingest(message: Message): void {
    this.#deliver(this.#streams.push(message))
  }

  /** Release what sits behind holes older than `GAP_TIMEOUT_MS`; public so a test can tick without a clock. */
  expire(now?: number): void {
    for (const released of this.#streams.expire(now)) {
      this.#deliver(released)
    }
  }

  #deliver(delivered: Delivered): void {
    const { from, directed } = delivered
    const key = `${directed ? 'd' : 'b'}:${from}`
    let state = this.#state.get(key)
    if (state === undefined) {
      state = {
        decoder: new TextDecoder(),
        status: { from, directed, received: 0, complete: false },
      }
      this.#state.set(key, state)
    }
    for (const bytes of delivered.chunks) {
      state.status.received += 1
      state.status.complete = false
      this.#emit({ from, directed, text: state.decoder.decode(bytes, { stream: true }), eof: false })
    }
    if (delivered.complete) {
      const tail = state.decoder.decode()
      if (tail !== '') {
        this.#emit({ from, directed, text: tail, eof: false })
      }
      state.status.complete = true
      this.#emit({ from, directed, text: '', eof: true })
    }
  }

  #emit(chunk: Chunk): void {
    this.onChunk?.(chunk)
    this.#inbox.push(chunk)
    if (this.#inbox.length > this.#inboxCap) {
      this.#inbox.shift()
    }
    this.#wake()
  }

  #wake(): void {
    for (const waiter of this.#waiters) {
      waiter()
    }
    this.#waiters.clear()
  }

  async send(text: string, to?: string): Promise<void> {
    await this.#mesh.send(text, to === undefined ? {} : { to })
    this.onSent?.(text, to)
  }

  async sendEof(to?: string): Promise<void> {
    await this.#mesh.sendEof(to === undefined ? {} : { to })
    this.onSentEof?.(to)
  }

  /**
   * Everything delivered since the last `read`, consumed. When nothing is
   * waiting, wait up to `waitMs` (clamped to `MAX_WAIT_MS`) for the first
   * chunk, or until `signal` aborts. This is the closest thing to a stream
   * that a one-promise tool call allows.
   */
  async read(waitMs: number = DEFAULT_WAIT_MS, signal?: AbortSignal): Promise<ReadResult> {
    const wait = Math.min(Math.max(0, waitMs), MAX_WAIT_MS)
    if (this.#inbox.length === 0 && wait > 0 && !this.#closed && signal?.aborted !== true) {
      await new Promise<void>((resolve) => {
        const timer = setTimeout(done, wait)
        function done(): void {
          clearTimeout(timer)
          signal?.removeEventListener('abort', done)
          resolve()
        }
        signal?.addEventListener('abort', done, { once: true })
        this.#waiters.add(done)
      })
    }
    const items = coalesce(this.#inbox)
    this.#inbox = []
    return { items }
  }

  peers(): Peer[] {
    return this.#mesh.peers
  }

  status(): Status {
    return {
      id: this.#mesh.id,
      name: this.#mesh.name,
      nick: this.#mesh.nick,
      peerCount: this.#mesh.peers.length,
      unread: this.#inbox.length,
      streams: [...this.#state.values()].map((state) => ({ ...state.status })),
    }
  }

  stateGet(): Record<string, unknown> {
    return this.#mesh.state.value
  }

  stateMerge(patch: Record<string, unknown>): Promise<void> {
    return this.#mesh.state.merge(patch)
  }

  close(): Promise<void> {
    return this.#mesh.leave()
  }
}

/** Merge consecutive text chunks of one stream; an `eof` stays its own item. */
function coalesce(chunks: readonly Chunk[]): Chunk[] {
  const items: Chunk[] = []
  for (const chunk of chunks) {
    const last = items.at(-1)
    if (
      last !== undefined &&
      !last.eof &&
      !chunk.eof &&
      last.from === chunk.from &&
      last.directed === chunk.directed
    ) {
      items[items.length - 1] = { ...last, text: last.text + chunk.text }
    } else {
      items.push(chunk)
    }
  }
  return items
}
