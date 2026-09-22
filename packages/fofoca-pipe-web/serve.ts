/**
 * The dev server: bundle `src/main.ts` once at startup and serve it beside
 * `index.html` and the wasm glue under `packages/fofoca-wasm/wasm/`. No HMR,
 * no watch — the same shape as the chat example's server.
 *
 *   bun run serve [port]          # from packages/fofoca-pipe-web
 *
 * The wasm glue must exist first (`cargo task wasm-peer`). For a deployable
 * copy, see `build.ts`.
 */

import { join } from 'node:path'

import { bundle, contentType, wasmDir } from './build.ts'

const root = import.meta.dir
const port = Number(process.argv[2] ?? '3020')

const mainJs = await bundle()

const server = Bun.serve({
  port,
  async fetch(request) {
    const url = new URL(request.url)
    const path = url.pathname
    if (path === '/' || path === '/index.html') {
      return new Response(Bun.file(join(root, 'index.html')), {
        headers: { 'content-type': 'text/html' },
      })
    }
    if (path === '/main.js') {
      return new Response(mainJs, { headers: { 'content-type': 'text/javascript' } })
    }
    // The generated glue imports `fofoca_wasm_bg.wasm` relative to itself,
    // and the bundle imports the glue by URL — both land here.
    if (path.startsWith('/wasm/')) {
      const file = Bun.file(join(wasmDir, path.slice('/wasm/'.length)))
      if (await file.exists()) {
        return new Response(file, { headers: { 'content-type': contentType(path) } })
      }
    }
    return new Response('not found', { status: 404 })
  },
})

console.log(`pipe on http://127.0.0.1:${server.port}/ (wasm from ${wasmDir})`)
