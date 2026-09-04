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
  /**
   * Topic only: let payload fall back to the relay. Mixed into the derived
   * id, so every member must pass the same value. Ignored when joining by id
   * (the id carries it).
   */
  relayTransport?: boolean
  /**
   * Topic only: a custom relay ladder replacing the default. Mixed into the
   * derived id like `relayTransport`. Ignored when joining by id.
   */
  relayUrls?: string[]
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
  /** The relay as a lookup: peers find each other through it. */
  relay?: boolean
  /**
   * The relay as a transport: payload may fall back to it. Off by default,
   * so all data is peer to peer and the relay is a meeting point only. Needs
   * `relay` (or `public`). Baked into the mesh id, so joiners inherit it.
   */
  relayTransport?: boolean
  /**
   * A custom relay ladder (ordered URLs, first preferred), replacing the
   * default. Implies the relay lookup, and is part of the mesh id.
   */
  relayUrls?: string[]
  /**
   * This node's transport switches. Per node, not part of the id; everything
   * the target has is on by default.
   */
  transports?: { ip?: boolean; webrtc?: boolean }
  maxPeers?: number
}

/**
 * The lane a directed frame to a peer would take right now. Mirrors
 * `fofoca::transport::Lane`.
 *
 * `relay-only` is the one observation among the hints: on a mesh whose relay
 * is lookup only (the default), it names a peer no payload can reach until a
 * direct path is proven. The rest are derived from the engine's own send
 * decision; iroh picks the real path at connect time.
 */
export type Lane = 'unicast' | 'multihop' | 'relay-only' | 'unreachable'

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
  /** Heartbeat-evicted but still in the roster (`quiet: true`) — not `left`. */
  | { kind: 'quiet'; nick: string }
  | { kind: 'returned'; nick: string }
  /** A peer's per-author hash chain forked: two signed messages at one seq. */
  | { kind: 'fork'; nick: string; pubkey: string; seq: number }
  | { kind: 'info'; message: string }
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
