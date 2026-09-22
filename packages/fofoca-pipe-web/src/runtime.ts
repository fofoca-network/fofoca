/**
 * The pipe runtime a page (or an agent through WebMCP) drives: ordered
 * bytes in, readable items out, plus the roster and the shared state
 * document.
 *
 * DOM-free on purpose. `main.ts` hooks the DOM onto `onChunk`; the tools in
 * `webmcp.ts` call the methods; the tests drive it over a fake `Mesh`.
 *
 * Frames land shuffled and cut at ~2 KB, so every inbound frame goes through
 * `Streams` for order and through one `TextDecoder` per stream for text — a
 * chunk boundary can split a multibyte character, and a per-chunk decode
 * would corrupt it.
 *
 * What arrives is kept in a bounded **log**, not a queue that a read drains.
 * Each entry carries its sequence number, its bytes and the text those bytes
 * decoded to, so:
 *
 * - reading destroys nothing, and two readers (two agents, or a tool and the
 *   page) can each follow the stream from their own cursor;
 * - a reader can ask for `base64` and get the bytes back exactly, which text
 *   cannot do for a binary payload;
 * - entries age out by bytes held, so a long stream cannot grow the tab
 *   without bound.
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

/** How `read` renders the bytes it returns. */
export type Encoding = 'text' | 'base64'

/**
 * One item from `read`: consecutive pieces of one stream, merged. `text`
 * when the read asked for text, `base64` when it asked for base64.
 */
export interface Item {
  readonly from: string
  readonly directed: boolean
  readonly eof: boolean
  readonly text?: string
  readonly base64?: string
}

export interface ReadResult {
  readonly items: Item[]
  /** Pass this back as `cursor` to continue where this read stopped. */
  readonly cursor: number
}

export interface ReadOpts {
  /** How long to wait for the first entry when there is nothing to read. */
  readonly waitMs?: number
  /** Where to read from. Absent means the oldest entry still held. */
  readonly cursor?: number
  /** Default `text`. */
  readonly encoding?: Encoding
}

export interface StreamStatus {
  readonly from: string
  readonly directed: boolean
  /** Chunks delivered so far. */
  readonly received: number
  /** The stream's end-of-stream has been delivered. Cleared when the author writes on. */
  readonly complete: boolean
}

export interface Status {
  readonly id: string
  readonly name: string
  readonly nick: string
  readonly peerCount: number
  /** Entries the log holds right now. */
  readonly buffered: number
  /** The cursor a reader wanting only what arrives next would start from. */
  readonly cursor: number
  /** The oldest cursor still readable. Anything older has aged out. */
  readonly oldestCursor: number
  readonly streams: StreamStatus[]
}

/** The longest a `read` waits for the first entry. Under a browser agent's patience. */
export const MAX_WAIT_MS = 25_000
const DEFAULT_WAIT_MS = 10_000
/** Payload bytes the log holds before its oldest entries age out. */
export const DEFAULT_MAX_BYTES = 4_194_304
/** How often open holes in the streams are checked against `GAP_TIMEOUT_MS`. */
const GAP_POLL_MS = 500

interface Entry {
  readonly seq: number
  readonly from: string
  readonly directed: boolean
  readonly eof: boolean
  readonly bytes: Uint8Array
  readonly text: string
}

interface StreamState {
  readonly decoder: TextDecoder
  readonly status: { from: string; directed: boolean; received: number; complete: boolean }
}

export class PipeRuntime {
  readonly #mesh: Mesh
  readonly #streams = new Streams()
  readonly #state = new Map<string, StreamState>()
  readonly #maxBytes: number
  readonly #log: Entry[] = []
  #nextSeq = 0
  #bytesHeld = 0
  readonly #waiters = new Set<() => void>()
  #closed = false
  readonly #gapTimer: ReturnType<typeof setInterval>

  /** Called with every chunk as it is delivered — the live view. */
  onChunk: ((chunk: Chunk) => void) | undefined
  /** Called after every send this runtime made, whoever asked for it — the tools, the page, a script. */
  onSent: ((text: string, to: string | undefined) => void) | undefined
  /** Called after every end-of-stream this runtime sent. */
  onSentEof: ((to: string | undefined) => void) | undefined

  constructor(mesh: Mesh, opts?: { maxBytes?: number }) {
    this.#mesh = mesh
    this.#maxBytes = opts?.maxBytes ?? DEFAULT_MAX_BYTES
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
      // Decoded on the way in, not at read time: the decoder carries the
      // tail of a multibyte character across chunks, and a reader starting
      // mid-stream cannot rebuild that state.
      this.#append({
        from,
        directed,
        eof: false,
        bytes,
        text: state.decoder.decode(bytes, { stream: true }),
      })
    }
    if (delivered.complete) {
      const tail = state.decoder.decode()
      if (tail !== '') {
        this.#append({ from, directed, eof: false, bytes: new Uint8Array(), text: tail })
      }
      state.status.complete = true
      this.#append({ from, directed, eof: true, bytes: new Uint8Array(), text: '' })
    }
  }

  #append(entry: Omit<Entry, 'seq'>): void {
    const full: Entry = { ...entry, seq: this.#nextSeq }
    this.#nextSeq += 1
    this.#log.push(full)
    this.#bytesHeld += full.bytes.byteLength
    while (this.#bytesHeld > this.#maxBytes && this.#log.length > 1) {
      const oldest = this.#log.shift()
      this.#bytesHeld -= oldest?.bytes.byteLength ?? 0
    }
    this.onChunk?.({
      from: full.from,
      directed: full.directed,
      text: full.eof ? '' : full.text,
      eof: full.eof,
    })
    this.#wake()
  }

  #wake(): void {
    for (const waiter of this.#waiters) {
      waiter()
    }
    this.#waiters.clear()
  }

  #oldestCursor(): number {
    return this.#log[0]?.seq ?? this.#nextSeq
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
   * Everything the log holds from `cursor` on, and the cursor to continue
   * from. A read takes nothing away, so a second reader sees the same
   * entries. Without a cursor the read starts at the oldest entry held.
   *
   * With nothing to read it waits up to `waitMs` (clamped to `MAX_WAIT_MS`)
   * for the first entry, or until `signal` aborts. That is the closest thing
   * to a stream a one-promise tool call allows.
   */
  async read(opts: ReadOpts = {}, signal?: AbortSignal): Promise<ReadResult> {
    const wait = Math.min(Math.max(0, opts.waitMs ?? DEFAULT_WAIT_MS), MAX_WAIT_MS)
    let from = opts.cursor ?? this.#oldestCursor()
    if (this.#nextSeq <= from && wait > 0 && !this.#closed && signal?.aborted !== true) {
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
    // A cursor older than the log asks for entries that aged out. Serve what
    // is left rather than nothing, and let the returned cursor say where the
    // reader now stands.
    from = Math.max(from, this.#oldestCursor())
    const window = this.#log.filter((entry) => entry.seq >= from)
    const last = window.at(-1)
    return {
      items: coalesce(window, opts.encoding ?? 'text'),
      cursor: last === undefined ? from : last.seq + 1,
    }
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
      buffered: this.#log.length,
      cursor: this.#nextSeq,
      oldestCursor: this.#oldestCursor(),
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

/**
 * Merge consecutive entries of one stream into one item; an `eof` stays its
 * own. Bytes are joined before they are encoded, because two base64 strings
 * do not concatenate into the base64 of their bytes.
 */
function coalesce(entries: readonly Entry[], encoding: Encoding): Item[] {
  const runs: Entry[][] = []
  for (const entry of entries) {
    const last = runs.at(-1)
    const head = last?.[0]
    if (
      last !== undefined &&
      head !== undefined &&
      !head.eof &&
      !entry.eof &&
      head.from === entry.from &&
      head.directed === entry.directed
    ) {
      last.push(entry)
    } else {
      runs.push([entry])
    }
  }
  return runs.flatMap((run): Item[] => {
    const head = run[0]
    if (head === undefined) {
      return []
    }
    const base = { from: head.from, directed: head.directed, eof: head.eof }
    if (encoding === 'base64') {
      return [{ ...base, base64: toBase64(join(run.map((entry) => entry.bytes))) }]
    }
    return [{ ...base, text: run.map((entry) => entry.text).join('') }]
  })
}

function join(chunks: readonly Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0)
  const out = new Uint8Array(total)
  let at = 0
  for (const chunk of chunks) {
    out.set(chunk, at)
    at += chunk.byteLength
  }
  return out
}

/** `btoa` over bytes, in slices: spreading a megabyte blows the argument limit. */
function toBase64(bytes: Uint8Array): string {
  const SLICE = 0x8000
  let binary = ''
  for (let at = 0; at < bytes.length; at += SLICE) {
    binary += String.fromCharCode(...bytes.subarray(at, at + SLICE))
  }
  return btoa(binary)
}
