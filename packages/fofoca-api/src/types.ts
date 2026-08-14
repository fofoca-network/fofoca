/**
 * The mesh API, as types. Both backends implement exactly this, so a program
 * moves between a browser tab and a terminal by changing one import.
 */

export interface JoinOpts {
  /**
   * Derive a mesh from a shared string. Everyone who passes the same string
   * lands in the same mesh.
   *
   * A topic mesh is always reached over the public preset — mDNS, the mainline
   * DHT and the pinned relay ladder. That is not a default you can change: the
   * derivation mixes the lookups into the mesh id, so two reaches over one
   * string are two different meshes that never meet. Hence no discovery flags
   * here.
   */
  topic?: string
  /** Join this mesh id. The id is the bearer credential — treat it as a secret. */
  id?: string
  /** Defaults to a random `word-word` nickname. */
  nick?: string
  /** Active-view cap. Omit for the engine default. */
  maxPeers?: number
}

export interface CreateOpts {
  name?: string
  nick?: string
  /**
   * The all-on discovery preset (mDNS + DHT + relay).
   *
   * Naming no discovery option at all is not "the default one" — it is a
   * loopback mesh, reachable only from this machine. That is useful for tests
   * and surprising everywhere else.
   */
  public?: boolean
  mdns?: boolean
  dht?: boolean
  relay?: boolean
  maxPeers?: number
}

/**
 * The lane a directed frame to a peer would take right now. Mirrors
 * `fofoca::transport::Lane`.
 *
 * A hint, not an observation: the engine derives it from its own send decision,
 * and iroh picks the real path at connect time. It cannot tell you whether a
 * peer is carried over a WebRTC data channel or a relay hop.
 */
export type Lane = 'unicast' | 'multihop' | 'unreachable'

/** How near a peer is. Mirrors `fofoca::embed::Reach`. */
export type Reach = 'direct' | 'gossip'

export interface Peer {
  nick: string
  reach: Reach
  transport: Lane
  /** Heartbeat-evicted, but still known and still returnable. */
  quiet: boolean
  /** Absent until the peer's first heartbeat is timed. */
  lastSeenSecsAgo?: number
}

/**
 * One inbound frame.
 *
 * A frame, not a message: `send` splits a body larger than `MAX_CHUNK` and the
 * receiver sees one of these per chunk, with nothing to rejoin them by. A
 * consumer that needs whole messages frames them itself.
 */
export interface Message {
  from: string
  bytes: Uint8Array
  /** Set when `bytes` decode as UTF-8. */
  text?: string
  /** True when the frame was addressed to us alone. */
  directed: boolean
  /** An end-of-stream marker. `bytes` is empty. */
  eof: boolean
}

export type MeshEvent =
  | { kind: 'ready' }
  | { kind: 'joined'; nick: string }
  | { kind: 'left'; nick: string }
  | { kind: 'error'; message: string }
  | { kind: 'closed'; reason: string }

export interface StateDoc {
  /** The converged document. A CRDT underneath, so concurrent writers merge. */
  readonly value: Record<string, unknown>
  /** Apply an RFC 7386 merge patch: set present keys, delete null keys, leave the rest. */
  merge(patch: Record<string, unknown>): Promise<void>
  changes(signal?: AbortSignal): AsyncIterable<Record<string, unknown>>
}

export interface Mesh extends AsyncDisposable {
  readonly id: string
  readonly name: string
  /** The nickname the engine assigned, which is not always the one requested. */
  readonly nick: string
  readonly peers: Peer[]
  readonly state: StateDoc
  /**
   * The largest payload one frame carries. `send` splits on it for you; a
   * caller that would rather refuse an over-long body than have it arrive in
   * pieces checks against this first.
   *
   * On the mesh rather than a module constant because the FFI backend learns it
   * from `fofoca_max_chunk()`, and a module constant would mean loading the
   * native library at import time.
   */
  readonly maxChunk: number

  send(body: string | Uint8Array, opts?: { to?: string }): Promise<void>
  sendEof(opts?: { to?: string }): Promise<void>

  /**
   * Every frame from the moment this iterator was created. Each call gets its
   * own buffer, so two consumers never split one queue.
   */
  messages(signal?: AbortSignal): AsyncIterable<Message>
  events(signal?: AbortSignal): AsyncIterable<MeshEvent>

  leave(): Promise<void>
}
