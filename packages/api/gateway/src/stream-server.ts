/** Host WebSocket owner for multiplexed Typert Remote streams. */

import type { IncomingMessage } from 'node:http'
import type { Duplex } from 'node:stream'
import WebSocket, { WebSocketServer, type RawData } from 'ws'
import {
  parseRemoteStreamClientMessage,
  REMOTE_VIEW_BIND_ENDPOINT,
  REMOTE_VIEW_CALL_ENDPOINT,
  type RemoteStreamFailure,
  type RemoteStreamServerMessage,
} from './stream-protocol.ts'

/** Open one validated Remote stream for a decoded wire request. */
export type RemoteStreamOpener = (
  endpoint: string,
  payload: unknown,
  signal: AbortSignal,
) => Promise<AsyncIterable<unknown>>

/** Convert an invocation or carrier failure to a stable wire value. */
export type RemoteStreamFailureMapper = (error: unknown) => RemoteStreamFailure

/** Connection-local Host operations that own the displayed Session binding. */
export interface RemoteViewOwner {
  readonly bind: (sessionId: string | undefined, signal: AbortSignal) => Promise<unknown>
  readonly call: (
    endpoint: string,
    payload: unknown,
    signal: AbortSignal,
    view: unknown,
  ) => Promise<unknown>
}

/** Own the no-server WebSocket acceptor and every active logical stream. */
export class RemoteStreamMuxServer {
  private readonly server = new WebSocketServer({ noServer: true })
  private readonly connections = new Set<Promise<void>>()
  private readonly heartbeatAlive = new WeakMap<WebSocket, boolean>()
  private heartbeatTimer: NodeJS.Timeout | undefined

  /**
   * @param open - Gateway stream dispatcher.
   * @param failure - Gateway error-to-wire mapper.
   * @param heartbeatIntervalMs - interval between WebSocket Ping control frames.
   */
  constructor(
    private readonly open: RemoteStreamOpener,
    private readonly failure: RemoteStreamFailureMapper,
    private readonly heartbeatIntervalMs: number,
    private readonly view?: RemoteViewOwner,
  ) {}

  /**
   * Upgrade one trusted request and begin serving its logical streams.
   * @param req - authenticated HTTP upgrade request.
   * @param socket - carrier socket transferred to the WebSocket server.
   * @param head - bytes already read after the HTTP upgrade headers.
   */
  handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): void {
    this.server.handleUpgrade(req, socket, head, (websocket) => {
      this.heartbeatAlive.set(websocket, true)
      websocket.on('pong', () => { this.heartbeatAlive.set(websocket, true) })
      this.startHeartbeat()
      const connection = new RemoteStreamMuxConnection(websocket, this.open, this.failure, this.view)
      const done = connection.run()
      this.connections.add(done)
      void done.then(() => { this.connections.delete(done) })
    })
  }

  /** Terminate all sockets and wait until every iterator has returned. */
  async close(): Promise<void> {
    clearInterval(this.heartbeatTimer)
    this.heartbeatTimer = undefined
    for (const socket of this.server.clients) socket.terminate()
    const closed = Promise.withResolvers<void>()
    this.server.close((error) => {
      if (error === undefined) closed.resolve()
      else closed.reject(error)
    })
    await closed.promise
    await Promise.all(this.connections)
  }

  /** Start one `unref()` timer after the first upgrade; it spans empty-client periods until close(). */
  private startHeartbeat(): void {
    if (this.heartbeatTimer !== undefined) return
    this.heartbeatTimer = setInterval(() => {
      for (const socket of this.server.clients) {
        if (socket.readyState !== WebSocket.OPEN) continue
        if (this.heartbeatAlive.get(socket) === false) {
          socket.terminate()
          continue
        }
        this.heartbeatAlive.set(socket, false)
        socket.ping()
      }
    }, this.heartbeatIntervalMs)
    this.heartbeatTimer.unref()
  }
}

interface ActiveStream {
  readonly abort: AbortController
  readonly viewCall: boolean
  done: Promise<void>
}

class RemoteStreamMuxConnection {
  private readonly streams = new Map<string, ActiveStream>()
  private writes = Promise.resolve()
  private viewContext: unknown
  private viewGeneration = 0
  private binding = Promise.resolve()

  constructor(
    private readonly socket: WebSocket,
    private readonly open: RemoteStreamOpener,
    private readonly failure: RemoteStreamFailureMapper,
    private readonly view?: RemoteViewOwner,
  ) {}

  async run(): Promise<void> {
    const closed = new Promise<void>((resolve) => {
      this.socket.once('close', resolve)
      this.socket.once('error', () => { this.socket.terminate() })
      this.socket.on('message', (data, isBinary) => {
        if (isBinary) {
          this.socket.close(1003, 'text messages required')
          return
        }
        try {
          this.receive(rawText(data))
        } catch {
          this.socket.close(1008, 'invalid Remote stream request')
        }
      })
    })
    await closed
    this.invalidateView()
    const active = [...this.streams.values()]
    for (const stream of active) stream.abort.abort(new Error('Remote stream socket closed'))
    await Promise.all(active.map(stream => stream.done))
  }

  private receive(text: string): void {
    const message = parseRemoteStreamClientMessage(text)
    if (message.type === 'cancel') {
      this.streams.get(message.streamId)?.abort.abort(new Error('Remote stream cancelled'))
      return
    }
    if (this.streams.has(message.streamId)) {
      throw new Error(`api gateway: duplicate Remote stream id ${JSON.stringify(message.streamId)}`)
    }
    const abort = new AbortController()
    const active: ActiveStream = {
      abort,
      viewCall: message.endpoint === REMOTE_VIEW_CALL_ENDPOINT,
      done: Promise.resolve(),
    }
    if (message.endpoint === REMOTE_VIEW_BIND_ENDPOINT) this.invalidateView()
    this.streams.set(message.streamId, active)
    const done = this.pump(message.streamId, message.endpoint, message.payload, active)
    active.done = done
    const remove = (): void => { this.streams.delete(message.streamId) }
    void done.then(remove, remove)
  }

  private async pump(
    streamId: string,
    endpoint: string,
    payload: unknown,
    active: ActiveStream,
  ): Promise<void> {
    try {
      const source = await this.openSource(endpoint, payload, active.abort.signal)
      for await (const value of source) {
        await this.send({ type: 'item', streamId, value })
      }
      if (!active.abort.signal.aborted) await this.send({ type: 'end', streamId })
    } catch (error) {
      if (!active.abort.signal.aborted && this.socket.readyState === WebSocket.OPEN) {
        try {
          await this.send({ type: 'error', streamId, error: this.failure(error) })
        } catch {
          // A terminal frame that cannot be encoded or written leaves the
          // logical stream ambiguous, so fail the physical generation.
          this.socket.close(1011, 'Remote stream failure could not be delivered')
        }
      }
    }
  }

  private openSource(endpoint: string, payload: unknown, signal: AbortSignal): Promise<AsyncIterable<unknown>> {
    if (endpoint === REMOTE_VIEW_BIND_ENDPOINT) return Promise.resolve(this.bindView(payload, signal))
    if (endpoint === REMOTE_VIEW_CALL_ENDPOINT) return Promise.resolve(this.callView(payload, signal))
    return this.open(endpoint, payload, signal)
  }

  private async *bindView(payload: unknown, signal: AbortSignal): AsyncGenerator<never> {
    if (this.view === undefined) throw new Error('api gateway: Remote view binding is unavailable')
    const sessionId = parseViewBinding(payload)
    const generation = this.viewGeneration
    const task = this.view.bind(sessionId, signal).then((context) => {
      if (!signal.aborted && generation === this.viewGeneration) this.viewContext = context
    })
    this.binding = task.then(() => undefined, () => undefined)
    await task
  }

  private async *callView(payload: unknown, signal: AbortSignal): AsyncGenerator {
    if (this.view === undefined) throw new Error('api gateway: Remote view calls are unavailable')
    const request = parseViewCall(payload)
    await this.binding
    signal.throwIfAborted()
    const value = await this.view.call(request.endpoint, request.payload, signal, this.viewContext)
    signal.throwIfAborted()
    yield value
  }

  private invalidateView(): void {
    this.viewGeneration += 1
    this.viewContext = undefined
    for (const active of this.streams.values()) {
      if (active.viewCall) active.abort.abort(new Error('Remote view changed'))
    }
  }

  private send(message: RemoteStreamServerMessage): Promise<void> {
    let text: string
    try {
      text = JSON.stringify(message)
    } catch (cause) {
      return Promise.reject(new Error('api gateway: Remote stream item is not JSON serializable', { cause }))
    }
    const delivery = this.writes.then(() => new Promise<void>((resolve, reject) => {
      if (this.socket.readyState !== WebSocket.OPEN) {
        reject(new Error('api gateway: Remote stream socket is closed'))
        return
      }
      this.socket.send(text, (error) => {
        if (error) reject(error)
        else resolve()
      })
    }))
    this.writes = delivery.catch(() => undefined)
    return delivery
  }
}

function parseViewBinding(payload: unknown): string | undefined {
  if (!isRecord(payload)) throw new Error('api gateway: invalid Remote view binding')
  const keys = Reflect.ownKeys(payload)
  if (keys.length === 0) return undefined
  if (keys.length === 1 && keys[0] === 'sessionId'
    && typeof payload.sessionId === 'string' && payload.sessionId.length > 0) return payload.sessionId
  throw new Error('api gateway: invalid Remote view binding')
}

function parseViewCall(payload: unknown): { endpoint: string; payload: unknown } {
  if (!isRecord(payload)
    || Reflect.ownKeys(payload).length !== 2
    || !Object.hasOwn(payload, 'endpoint')
    || !Object.hasOwn(payload, 'payload')
    || typeof payload.endpoint !== 'string'
    || payload.endpoint.length === 0) {
    throw new Error('api gateway: invalid Remote view call')
  }
  return { endpoint: payload.endpoint, payload: payload.payload }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function rawText(data: RawData): string {
  if (Array.isArray(data)) return Buffer.concat(data).toString('utf8')
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString('utf8')
  return Buffer.from(data).toString('utf8')
}

/**
 * Reject an upgrade without transferring socket ownership to ws.
 * @param socket - carrier socket that receives the HTTP rejection.
 * @param status - authentication or browser-trust rejection status.
 */
export function rejectRemoteStreamUpgrade(socket: Duplex, status: 401 | 403): void {
  const reason = status === 401 ? 'Unauthorized' : 'Forbidden'
  const body = reason.toLowerCase()
  socket.end([
    `HTTP/1.1 ${String(status)} ${reason}`,
    'Connection: close',
    'Content-Type: text/plain; charset=utf-8',
    `Content-Length: ${String(Buffer.byteLength(body))}`,
    '',
    body,
  ].join('\r\n'))
}
