/**
 * The loop that owns the handle, as a pure object: a `NativeLibrary` in, a
 * port out, time injected. `worker.ts` is the thin file that gives it real
 * globals; the tests hand it fakes and a hand-cranked clock.
 *
 * Every `fofoca_*` call for the handle happens through this one object on one
 * thread — the C ABI's thread-confinement rule (`LAST_ERROR` is a
 * `thread_local!` in `crates/fofoca-ffi/src/ffi.rs`), which is why none of
 * this may ever run on a pool thread.
 */

import type { NativeLibrary, NativePointer } from './dlopen/native.ts'
import { FRAME_BYTES, decodeFrame } from './frame.ts'
import {
  type Command,
  type FromWorker,
  type OpenReply,
  POLL_MS,
  RECV_TIMEOUT_MS,
} from './protocol.ts'
import { readDocument } from './query.ts'

export interface EnginePort {
  post(message: FromWorker, transfer?: ArrayBuffer[]): void
}

/** What the drive loop does next: keep spinning, or stop for good. */
export type EngineStatus = 'idle' | 'closed'

export interface Engine {
  handle(command: Command): EngineStatus
  pumpOnce(): EngineStatus
}

export function createEngine(
  lib: NativeLibrary,
  port: EnginePort,
  now: () => number = Date.now,
): Engine {
  let handle: NativePointer | null = null
  let payload = new Uint8Array(0)
  const meta = new Uint8Array(FRAME_BYTES)
  let closed = false
  let lastPoll = 0

  const lastError = (call: string): string => {
    const pointer = lib.call('fofoca_last_error') as NativePointer
    return lib.isNull(pointer) ? `${call} failed` : `${call}: ${lib.readCString(pointer)}`
  }

  const raise = (call: string): never => {
    throw new Error(lastError(call))
  }

  const cstring = (call: 'fofoca_id' | 'fofoca_name' | 'fofoca_nickname' | 'fofoca_version') => {
    const pointer =
      call === 'fofoca_version'
        ? (lib.call(call) as NativePointer)
        : (lib.call(call, handle) as NativePointer)
    if (lib.isNull(pointer)) {
      raise(call)
    }
    return lib.readCString(pointer)
  }

  const readDoc = (name: 'fofoca_state_json' | 'fofoca_peers_json'): string =>
    readDocument(name, (buffer, cap) => lib.call(name, handle, buffer, cap) as bigint, raise)

  const ok = (id: number, value: OpenReply | string | null) => {
    port.post({ t: 'ok', id, value })
  }

  const err = (id: number, message: string) => {
    port.post({ t: 'err', id, message })
  }

  const status = (): EngineStatus => (closed ? 'closed' : 'idle')

  const open = (id: number, command: Command & { t: 'open' }) => {
    if (handle !== null) {
      err(id, 'this worker already owns a mesh')
      return
    }
    const opened = lib.open(command.opts)
    if (lib.isNull(opened)) {
      err(id, lastError('fofoca_open'))
      return
    }
    handle = opened
    const maxChunk = Number(lib.call('fofoca_max_chunk') as bigint)
    payload = new Uint8Array(maxChunk)
    lastPoll = now()
    ok(id, {
      id: cstring('fofoca_id'),
      name: cstring('fofoca_name'),
      nick: cstring('fofoca_nickname'),
      rosterJson: readDoc('fofoca_peers_json'),
      stateJson: readDoc('fofoca_state_json'),
      maxChunk,
      version: cstring('fofoca_version'),
    })
  }

  const handleCommand = (command: Command): EngineStatus => {
    if (closed) {
      err(command.id, 'the mesh is closed')
      return 'closed'
    }
    try {
      switch (command.t) {
        case 'open': {
          open(command.id, command)
          break
        }
        case 'send': {
          if (handle === null) {
            err(command.id, 'no open mesh')
            break
          }
          const bytes = new Uint8Array(command.bytes)
          const code = lib.call(
            'fofoca_send',
            handle,
            command.to,
            bytes,
            BigInt(bytes.byteLength),
          ) as number
          if (code === 0) {
            ok(command.id, null)
          } else {
            err(command.id, lastError('fofoca_send'))
          }
          break
        }
        case 'sendEof': {
          if (handle === null) {
            err(command.id, 'no open mesh')
            break
          }
          const code = lib.call('fofoca_send_eof', handle, command.to) as number
          if (code === 0) {
            ok(command.id, null)
          } else {
            err(command.id, lastError('fofoca_send_eof'))
          }
          break
        }
        case 'stateMerge': {
          if (handle === null) {
            err(command.id, 'no open mesh')
            break
          }
          const code = lib.call('fofoca_state_merge', handle, command.json) as number
          if (code === 0) {
            // Reply with the *resulting* document: `MeshBackend.stateMerge`'s
            // read-your-write contract, which the next poll is up to half a
            // second too late for.
            ok(command.id, readDoc('fofoca_state_json'))
          } else {
            err(command.id, lastError('fofoca_state_merge'))
          }
          break
        }
        case 'close': {
          if (handle !== null) {
            lib.call('fofoca_close', handle)
            handle = null
          }
          closed = true
          ok(command.id, null)
          port.post({ t: 'closed', reason: 'left the mesh' })
          break
        }
        default: {
          command satisfies never
        }
      }
    } catch (error) {
      err(command.id, error instanceof Error ? error.message : String(error))
    }
    return status()
  }

  const pumpOnce = (): EngineStatus => {
    if (closed || handle === null) {
      return status()
    }
    const code = lib.call(
      'fofoca_recv',
      handle,
      payload,
      BigInt(payload.byteLength),
      RECV_TIMEOUT_MS,
      meta,
    ) as bigint
    if (code === 1n) {
      let frame
      try {
        frame = decodeFrame(meta)
      } catch (error) {
        port.post({ t: 'failed', message: String(error) })
        return status()
      }
      const bytes = payload.slice(0, frame.len)
      port.post(
        { t: 'frame', from: frame.nick, directed: frame.directed, eof: frame.eof, bytes: bytes.buffer },
        [bytes.buffer],
      )
    } else if (code < 0n) {
      // A recv failure is terminal: the engine's inbound channel is gone, and
      // pretending otherwise would spin on the same error 20 times a second.
      const reason = lastError('fofoca_recv')
      lib.call('fofoca_close', handle)
      handle = null
      closed = true
      port.post({ t: 'closed', reason })
      return 'closed'
    }

    if (now() - lastPoll >= POLL_MS) {
      lastPoll = now()
      try {
        port.post({ t: 'roster', json: readDoc('fofoca_peers_json') })
        port.post({ t: 'state', json: readDoc('fofoca_state_json') })
      } catch (error) {
        // A poll failure has no caller waiting on it. Surface and keep going.
        port.post({ t: 'failed', message: error instanceof Error ? error.message : String(error) })
      }
    }
    return status()
  }

  return { handle: handleCommand, pumpOnce }
}
