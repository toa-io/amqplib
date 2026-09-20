// What the tests of connections and channels stand on: a connection talking to a stand-in server
// over a pair of streams, with no broker.

import { Duplex } from 'node:stream'
import { Connection, type OpenOptions } from './connection.ts'
import { plain } from './credentials.ts'
import * as defs from './defs.ts'
import type { Frame } from './frame.ts'

export const OPEN_OPTIONS: OpenOptions = {
  clientProperties: {},
  mechanism: 'PLAIN',
  response: plain('guest', 'guest').response(),
  locale: 'en_US',
  channelMax: 0,
  frameMax: 0,
  heartbeat: 0,
  virtualHost: '/',
  capabilities: '',
  insist: 0,
}

/** One end of a pair: what is written to it can be read from the other. */
export class End extends Duplex {
  public other!: End

  /** Every chunk written, as it was written. */
  public readonly written: Buffer[] = []

  public override _read(): void {}

  public override _write(chunk: Buffer, _: string, callback: () => void): void {
    const copy = Buffer.from(chunk)

    this.written.push(copy)
    this.other.push(copy)
    callback()
  }

  public override _final(callback: () => void): void {
    this.other.push(null)
    callback()
  }
}

export function pair(): { client: End; server: End } {
  const client = new End()
  const server = new End()

  client.other = server
  server.other = client

  return { client, server }
}

export interface Server {
  connection: Connection
  send(method: number, fields: object, channel?: number, content?: Buffer): void
  next(): Promise<Frame>
  expect(method: number): Promise<Frame>
}

/** The other side of a connection, read from a frame at a time. */
export function serve(stream: Duplex): Server {
  const connection = new Connection(stream)

  connection.expectSocketClose = true
  connection.frameMax = 4294967295

  for (let i = 0; i < 8; i++) connection.freshChannel(null)

  const next = (): Promise<Frame> =>
    new Promise((resolve, reject) => {
      connection.step((error, frame) => (error === null ? resolve(frame!) : reject(error)))
    })

  return {
    connection,
    next,

    send(method, fields, channel, content) {
      channel ??= 0

      if (content === undefined) connection.sendMethod(channel, method, fields)
      else connection.sendMessage(channel, method, fields, defs.BasicProperties, fields, content)
    },

    async expect(method) {
      const frame = await next()

      if (frame.id !== method)
        throw new Error(`Expected ${defs.info(method).name}, got ${JSON.stringify(frame)}`)

      return frame
    },
  }
}

export interface Opened {
  connection: Connection
  server: Server
  client: End
}

/** Opens a connection against a stand-in server that says yes to everything. */
export async function open(
  options: Partial<OpenOptions> = {},
  tune: Partial<defs.ConnectionTuneFields> = {}
): Promise<Opened> {
  const streams = pair()
  const connection = new Connection(streams.client)

  const opened = new Promise<void>((resolve, reject) => {
    connection.open({ ...OPEN_OPTIONS, ...options }, error =>
      error === null ? resolve() : reject(error)
    )
  })

  // the protocol header
  await new Promise(resolve => streams.server.once('readable', resolve))
  streams.server.read(8)

  const server = serve(streams.server)

  server.send(defs.ConnectionStart, {
    versionMajor: 0,
    versionMinor: 9,
    serverProperties: {},
    mechanisms: Buffer.from('PLAIN'),
    locales: Buffer.from('en_US'),
  })

  await server.expect(defs.ConnectionStartOk)
  server.send(defs.ConnectionTune, { channelMax: 0, heartbeat: 0, frameMax: 0, ...tune })
  await server.expect(defs.ConnectionTuneOk)
  await server.expect(defs.ConnectionOpen)
  server.send(defs.ConnectionOpenOk, { knownHosts: '' })
  await opened

  return { connection, server, client: streams.client }
}
