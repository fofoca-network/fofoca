/**
 * The runtime as WebMCP tools.
 *
 * WebMCP (`document.modelContext`, earlier `navigator.modelContext`) lets a
 * page register schema-backed tools a browser agent can call. A tool is one
 * `execute` promise: the spec has no streaming or progress channel yet
 * (webmachinelearning/webmcp #82, #196), so the pipe's inbound side is a
 * consume-once `pipe_read` that long-polls for the first chunk.
 *
 * `pipeTools` is pure so the tests can call each `execute` directly; the
 * registration is the only part that touches the browser, and it no-ops
 * where there is no model context.
 */

import type { PipeRuntime } from './runtime.ts'
import { MAX_WAIT_MS } from './runtime.ts'

export interface ToolResult {
  readonly content: { readonly type: 'text'; readonly text: string }[]
}

export interface ToolDescriptor {
  readonly name: string
  readonly description: string
  readonly inputSchema: Record<string, unknown>
  execute(input: unknown, options?: { signal?: AbortSignal }): Promise<ToolResult>
}

/** The slice of a model context this page uses. */
export interface ModelContextLike {
  registerTool(tool: ToolDescriptor): unknown
}

/** `document.modelContext`, else `navigator.modelContext`, else nothing. */
export function findModelContext(scope: object = globalThis): ModelContextLike | undefined {
  const holders = ['document', 'navigator'] as const
  for (const holder of holders) {
    const owner = (scope as Record<string, unknown>)[holder]
    if (owner === null || typeof owner !== 'object') {
      continue
    }
    const context = (owner as Record<string, unknown>)['modelContext']
    if (isModelContext(context)) {
      return context
    }
  }
  return undefined
}

function isModelContext(value: unknown): value is ModelContextLike {
  return (
    value !== null &&
    typeof value === 'object' &&
    typeof (value as Record<string, unknown>)['registerTool'] === 'function'
  )
}

/** Register every pipe tool. `false` when this browser has no model context. */
export function registerPipeTools(
  runtime: PipeRuntime,
  context: ModelContextLike | undefined = findModelContext(),
): boolean {
  if (context === undefined) {
    return false
  }
  for (const tool of pipeTools(runtime)) {
    context.registerTool(tool)
  }
  return true
}

export function pipeTools(runtime: PipeRuntime): ToolDescriptor[] {
  return [
    {
      name: 'pipe_send',
      description:
        'Send text into the mesh. Broadcast by default; `to` addresses one peer by nickname. Long text is split into frames and reassembled in order on the other side. A directed send to a peer whose transport is still relay-only is parked by the mesh and delivered once a direct path forms; check pipe_peers first.',
      inputSchema: {
        type: 'object',
        properties: {
          text: { type: 'string', description: 'The text to send.' },
          to: { type: 'string', description: 'A peer nickname; omit to broadcast.' },
        },
        required: ['text'],
      },
      execute: async (input) => {
        const { text, to } = fields(input)
        await runtime.send(requireString(text, 'text'), optionalString(to, 'to'))
        return ok({ sent: true })
      },
    },
    {
      name: 'pipe_send_eof',
      description:
        'Mark the end of the stream you have been sending — the receiver learns it has everything. Send again afterwards to start a new stream.',
      inputSchema: {
        type: 'object',
        properties: {
          to: { type: 'string', description: 'The peer nickname the stream went to; omit for the broadcast stream.' },
        },
      },
      execute: async (input) => {
        const { to } = fields(input)
        await runtime.sendEof(optionalString(to, 'to'))
        return ok({ sent: true })
      },
    },
    {
      name: 'pipe_read',
      description: `Read everything received since the last call, in stream order, and consume it. If nothing is waiting, wait up to waitMs (at most ${MAX_WAIT_MS}) for the first chunk, then return what arrived — possibly nothing. Call it in a loop to follow a stream; an item with eof=true closes that sender's stream.`,
      inputSchema: {
        type: 'object',
        properties: {
          waitMs: {
            type: 'integer',
            minimum: 0,
            maximum: MAX_WAIT_MS,
            description: 'How long to wait for the first chunk when the buffer is empty.',
          },
        },
      },
      execute: async (input, options) => {
        const { waitMs } = fields(input)
        const wait = waitMs === undefined ? undefined : requireInteger(waitMs, 'waitMs')
        return ok(await runtime.read(wait, options?.signal))
      },
    },
    {
      name: 'pipe_peers',
      description: 'The peers on the mesh right now, with how each one is reached.',
      inputSchema: { type: 'object', properties: {} },
      execute: async () => ok(runtime.peers()),
    },
    {
      name: 'pipe_status',
      description:
        'This tab on the mesh: its id, name and nickname, the peer count, how many chunks await pipe_read, and every stream seen so far.',
      inputSchema: { type: 'object', properties: {} },
      execute: async () => ok(runtime.status()),
    },
    {
      name: 'pipe_state_get',
      description: 'The shared state document, a JSON object every peer converges on.',
      inputSchema: { type: 'object', properties: {} },
      execute: async () => ok(runtime.stateGet()),
    },
    {
      name: 'pipe_state_merge',
      description:
        'Apply an RFC 7386 merge patch to the shared state document: present keys are set, null keys are deleted, the rest is left alone.',
      inputSchema: {
        type: 'object',
        properties: {
          patch: { type: 'object', description: 'The merge patch.' },
        },
        required: ['patch'],
      },
      execute: async (input) => {
        const { patch } = fields(input)
        await runtime.stateMerge(requireObject(patch, 'patch'))
        return ok({ merged: true })
      },
    },
  ]
}

function ok(value: unknown): ToolResult {
  return { content: [{ type: 'text', text: JSON.stringify(value) }] }
}

function fields(input: unknown): Record<string, unknown> {
  if (input === undefined || input === null) {
    return {}
  }
  if (typeof input !== 'object') {
    throw new TypeError('tool input must be an object')
  }
  return input as Record<string, unknown>
}

function requireString(value: unknown, name: string): string {
  if (typeof value !== 'string') {
    throw new TypeError(`${name} must be a string`)
  }
  return value
}

function optionalString(value: unknown, name: string): string | undefined {
  return value === undefined || value === null ? undefined : requireString(value, name)
}

function requireInteger(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isInteger(value)) {
    throw new TypeError(`${name} must be an integer`)
  }
  return value
}

function requireObject(value: unknown, name: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${name} must be an object`)
  }
  return value as Record<string, unknown>
}
