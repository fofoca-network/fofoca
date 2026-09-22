/**
 * Stream order over an unordered transport — the receiving half, mirroring
 * `crates/fofoca-pipe/src/reorder.rs`.
 *
 * Gossip keeps no order, so the frames of one stream can arrive shuffled.
 * Every `Message` carries its `seq` in its (`from`, `directed`) stream, and an
 * `eof` carries the stream's frame count. `Streams` routes each frame to its
 * stream and hands back whatever is now deliverable, in order.
 */

import type { Message } from './types.ts'

/**
 * Frames a stream buffers across a gap before giving the gap up. Matches the
 * engine's `INBOUND_CAP`: a gap that outlives that many later frames is not
 * going to fill, and a buffer that only grows is worse than a hole.
 */
export const REORDER_CAP = 256

/**
 * How long (ms) a hole may stay open before the frames behind it are released
 * without it. Matches the engine's `GAP_TIMEOUT`: a late frame arrives well
 * within a second, so an older hole is a frame that never came — the opening
 * of a stream this peer joined late, or one lost on the way.
 */
export const GAP_TIMEOUT_MS = 3000

/**
 * One stream's reassembly. The counter never resets: a sender that writes on
 * after an EOF keeps numbering.
 *
 * A hole is given up after `REORDER_CAP` later frames or after
 * `GAP_TIMEOUT_MS`; the second needs a clock, so a consumer calls `expire`
 * on a timer. `now` is injectable for tests.
 */
export class Reorder {
  #next = 0
  readonly #pending = new Map<number, Uint8Array>()
  #eofAt: number | undefined
  /** When the hole in front of `#pending` opened; undefined while there is none. */
  #gapSince: number | undefined

  /** Take the frame at `seq`; return every chunk now deliverable, in order. Empty on a gap or a duplicate. */
  push(seq: number, bytes: Uint8Array, now: number = performance.now()): Uint8Array[] {
    if (seq < this.#next || this.#pending.has(seq)) {
      return []
    }
    this.#pending.set(seq, bytes)
    if (this.#pending.size > REORDER_CAP) {
      this.#skipHole()
    }
    return this.#drain(now)
  }

  /** Release the frames behind a hole older than `GAP_TIMEOUT_MS`, in order. Empty when there is none. */
  expire(now: number = performance.now()): Uint8Array[] {
    if (this.#gapSince !== undefined && now - this.#gapSince >= GAP_TIMEOUT_MS) {
      this.#skipHole()
    }
    return this.#drain(now)
  }

  #skipHole(): void {
    if (this.#pending.size > 0) {
      this.#next = Math.min(...this.#pending.keys())
    }
  }

  #drain(now: number): Uint8Array[] {
    const delivered: Uint8Array[] = []
    for (;;) {
      const ready = this.#pending.get(this.#next)
      if (ready === undefined) {
        break
      }
      this.#pending.delete(this.#next)
      delivered.push(ready)
      this.#next += 1
    }
    if (this.#pending.size === 0) {
      this.#gapSince = undefined
    } else if (delivered.length > 0) {
      // Progress was made, so whatever hole is left is a new one.
      this.#gapSince = now
    } else {
      this.#gapSince ??= now
    }
    return delivered
  }

  /** Note the stream's end at `count` frames; true when the stream is complete right now. */
  eof(count: number): boolean {
    this.#eofAt = count
    return this.settle()
  }

  /** Everything up to the EOF mark has been delivered. */
  isComplete(): boolean {
    return this.#eofAt === this.#next
  }

  /** Report a completion once, then clear the mark so a later stream on the same pair can end again. */
  settle(): boolean {
    if (!this.isComplete()) {
      return false
    }
    this.#eofAt = undefined
    return true
  }
}

/** What one inbound frame released. */
export interface Delivered {
  readonly from: string
  readonly directed: boolean
  /** In stream order; empty when the frame only filled a buffer. */
  readonly chunks: Uint8Array[]
  /** The frame completed its stream (with or without chunks). */
  readonly complete: boolean
}

/** Every stream this peer receives, keyed by (`from`, `directed`). */
export class Streams {
  readonly #streams = new Map<string, { from: string; directed: boolean; stream: Reorder }>()

  push(frame: Pick<Message, 'from' | 'directed' | 'eof' | 'seq' | 'bytes'>, now?: number): Delivered {
    const key = `${frame.directed ? 'd' : 'b'}:${frame.from}`
    let entry = this.#streams.get(key)
    if (entry === undefined) {
      entry = { from: frame.from, directed: frame.directed, stream: new Reorder() }
      this.#streams.set(key, entry)
    }
    let chunks: Uint8Array[] = []
    let complete: boolean
    if (frame.eof) {
      complete = entry.stream.eof(frame.seq)
    } else {
      chunks = entry.stream.push(frame.seq, frame.bytes, now)
      complete = entry.stream.settle()
    }
    return { from: frame.from, directed: frame.directed, chunks, complete }
  }

  /** Release, on every stream, the frames behind a hole older than `GAP_TIMEOUT_MS`. Call it on a timer. */
  expire(now?: number): Delivered[] {
    const released: Delivered[] = []
    for (const { from, directed, stream } of this.#streams.values()) {
      const chunks = stream.expire(now)
      const complete = stream.settle()
      if (chunks.length > 0 || complete) {
        released.push({ from, directed, chunks, complete })
      }
    }
    return released
  }
}
