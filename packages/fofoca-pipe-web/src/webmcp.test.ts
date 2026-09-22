import { describe, expect, test } from 'bun:test'

import { fakeMesh, frame, settle } from './fake-mesh.ts'
import { PipeRuntime } from './runtime.ts'
import type { ModelContextLike, ToolDescriptor } from './webmcp.ts'
import { findModelContext, pipeTools, registerPipeTools } from './webmcp.ts'

function tool(tools: ToolDescriptor[], name: string): ToolDescriptor {
  const found = tools.find((candidate) => candidate.name === name)
  if (!found) {
    throw new Error(`no tool ${name}`)
  }
  return found
}

async function call(tools: ToolDescriptor[], name: string, input?: unknown): Promise<unknown> {
  const result = await tool(tools, name).execute(input)
  const text = result.content[0]?.text
  if (result.content.length !== 1 || result.content[0]?.type !== 'text' || text === undefined) {
    throw new Error('a tool result is one text block')
  }
  return JSON.parse(text)
}

describe('pipeTools', () => {
  test('registers the seven tools, each with an object schema', () => {
    const tools = pipeTools(new PipeRuntime(fakeMesh().mesh))
    expect(tools.map((candidate) => candidate.name)).toEqual([
      'pipe_send',
      'pipe_send_eof',
      'pipe_read',
      'pipe_peers',
      'pipe_status',
      'pipe_state_get',
      'pipe_state_merge',
    ])
    for (const candidate of tools) {
      expect(candidate.inputSchema['type']).toBe('object')
      expect(candidate.description.length).toBeGreaterThan(20)
    }
  })

  test('pipe_send broadcasts, or directs with `to`', async () => {
    const fake = fakeMesh()
    const tools = pipeTools(new PipeRuntime(fake.mesh))
    expect(await call(tools, 'pipe_send', { text: 'hi' })).toEqual({ sent: true })
    expect(await call(tools, 'pipe_send', { text: 'psst', to: 'bo' })).toEqual({ sent: true })
    expect(fake.sends).toEqual([{ body: 'hi' }, { body: 'psst', to: 'bo' }])
  })

  test('pipe_send refuses a missing or non-string text', async () => {
    const tools = pipeTools(new PipeRuntime(fakeMesh().mesh))
    await expect(call(tools, 'pipe_send', {})).rejects.toThrow(/text must be a string/)
    await expect(call(tools, 'pipe_send', { text: 3 })).rejects.toThrow(/text must be a string/)
    await expect(call(tools, 'pipe_send', 'hi')).rejects.toThrow(/must be an object/)
  })

  test('pipe_send_eof ends the broadcast stream, or a directed one', async () => {
    const fake = fakeMesh()
    const tools = pipeTools(new PipeRuntime(fake.mesh))
    await call(tools, 'pipe_send_eof')
    await call(tools, 'pipe_send_eof', { to: 'bo' })
    expect(fake.eofs).toEqual([{}, { to: 'bo' }])
  })

  test('pipe_read returns what arrived, in order, and consumes it', async () => {
    const fake = fakeMesh()
    const tools = pipeTools(new PipeRuntime(fake.mesh))
    fake.push(frame('ana', 1, 'b'))
    fake.push(frame('ana', 0, 'a'))
    await settle()

    expect(await call(tools, 'pipe_read', { waitMs: 0 })).toEqual({
      items: [{ from: 'ana', directed: false, text: 'ab', eof: false }],
    })
    expect(await call(tools, 'pipe_read', { waitMs: 0 })).toEqual({ items: [] })
  })

  test('pipe_read refuses a non-integer wait, and honors the abort signal', async () => {
    const tools = pipeTools(new PipeRuntime(fakeMesh().mesh))
    await expect(call(tools, 'pipe_read', { waitMs: 'soon' })).rejects.toThrow(/waitMs/)

    const controller = new AbortController()
    const pending = tool(tools, 'pipe_read').execute({ waitMs: 25_000 }, { signal: controller.signal })
    controller.abort()
    expect(JSON.parse((await pending).content[0]?.text ?? '')).toEqual({ items: [] })
  })

  test('pipe_peers, pipe_status and pipe_state_get read the mesh', async () => {
    const peers = [{ nick: 'ana', reach: 'direct', transport: 'unicast', quiet: false }]
    const tools = pipeTools(new PipeRuntime(fakeMesh(peers as never).mesh))
    expect(await call(tools, 'pipe_peers')).toEqual(peers)
    expect(await call(tools, 'pipe_status')).toMatchObject({ nick: 'me', peerCount: 1, unread: 0 })
    expect(await call(tools, 'pipe_state_get')).toEqual({ a: 1 })
  })

  test('pipe_state_merge takes an object patch and nothing else', async () => {
    const fake = fakeMesh()
    const tools = pipeTools(new PipeRuntime(fake.mesh))
    expect(await call(tools, 'pipe_state_merge', { patch: { k: null } })).toEqual({ merged: true })
    expect(fake.merges).toEqual([{ k: null }])
    await expect(call(tools, 'pipe_state_merge', { patch: [1] })).rejects.toThrow(/patch/)
    await expect(call(tools, 'pipe_state_merge', {})).rejects.toThrow(/patch/)
  })
})

describe('registerPipeTools', () => {
  test('registers every tool on the context it is given', () => {
    const registered: string[] = []
    const context: ModelContextLike = { registerTool: (candidate) => registered.push(candidate.name) }
    expect(registerPipeTools(new PipeRuntime(fakeMesh().mesh), context)).toBe(true)
    expect(registered).toHaveLength(7)
  })

  test('is a no-op without a context', () => {
    expect(registerPipeTools(new PipeRuntime(fakeMesh().mesh), undefined)).toBe(false)
  })
})

describe('findModelContext', () => {
  const context = { registerTool: () => undefined }

  test('prefers document.modelContext over navigator.modelContext', () => {
    const scope = {
      document: { modelContext: context },
      navigator: { modelContext: { registerTool: () => 'old' } },
    }
    expect(findModelContext(scope)).toBe(context)
  })

  test('falls back to navigator.modelContext', () => {
    expect(findModelContext({ document: {}, navigator: { modelContext: context } })).toBe(context)
  })

  test('finds nothing where neither exists, or where the shape is wrong', () => {
    expect(findModelContext({})).toBeUndefined()
    expect(findModelContext({ document: { modelContext: {} } })).toBeUndefined()
    expect(findModelContext({ navigator: null })).toBeUndefined()
  })
})
