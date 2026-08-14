/**
 * What crosses between the main thread and the worker that owns the handle.
 *
 * One `t` tag on every message in both directions, so each side is a single
 * exhaustive `switch` and `noFallthroughCasesInSwitch` plus a `never` default
 * turns a new variant into a type error rather than a dropped message.
 */

/** `fofoca_opts`, as a structured-cloneable object. */
export interface WireOpts {
  readonly mesh: string | null
  readonly topic: string | null
  readonly nick: string | null
  readonly name: string | null
  readonly isPublic: boolean
  readonly mdns: boolean
  readonly dht: boolean
  readonly relay: boolean
  readonly maxPeers: number
}

export type Command =
  /**
   * `lib` is resolved on the *main* thread and passed in: a discovery failure
   * there is an exception with a stack, where inside the worker it would be a
   * `postMessage` arriving before anything is listening.
   */
  | { readonly t: 'open'; readonly id: number; readonly opts: WireOpts; readonly lib: string }
  | {
      readonly t: 'send'
      readonly id: number
      readonly to: string | null
      readonly bytes: ArrayBuffer
    }
  | { readonly t: 'sendEof'; readonly id: number; readonly to: string | null }
  | { readonly t: 'stateMerge'; readonly id: number; readonly json: string }
  | { readonly t: 'close'; readonly id: number }

export interface OpenReply {
  readonly id: string
  readonly name: string
  readonly nick: string
  readonly rosterJson: string
  readonly stateJson: string
  readonly maxChunk: number
  readonly version: string
}

export type Reply = OpenReply | string | null

export type FromWorker =
  | { readonly t: 'ok'; readonly id: number; readonly value: Reply }
  | { readonly t: 'err'; readonly id: number; readonly message: string }
  | {
      readonly t: 'frame'
      readonly from: string
      readonly directed: boolean
      readonly eof: boolean
      readonly bytes: ArrayBuffer
    }
  | { readonly t: 'roster'; readonly json: string }
  | { readonly t: 'state'; readonly json: string }
  /** A failure with nobody waiting on it. The mesh stays open. */
  | { readonly t: 'failed'; readonly message: string }
  /** Terminal. Nothing follows. */
  | { readonly t: 'closed'; readonly reason: string }

/**
 * The recv timeout, and so the command-latency ceiling: a `send` posted just
 * after a recv starts waits at most this long to be noticed. The price is 20
 * wakeups a second on an idle mesh. Raising it makes a keypress echo feel slow;
 * lowering it burns idle CPU for nothing.
 */
export const RECV_TIMEOUT_MS = 50

/**
 * How often the roster and the state document are re-read.
 *
 * This interval *is* the join/leave latency on this backend. There is no push
 * channel to use instead: the C ABI has no callback, so a peer arriving is
 * something this side has to notice rather than be told. `packages/fofoca-wasm`
 * is told, and sees the same change immediately.
 */
export const POLL_MS = 500
