// Makes the socket, and opens a connection over it.

import { readFileSync } from 'node:fs'
import net from 'node:net'
import tls from 'node:tls'
import { unescape as unescapeQuery, parse as parseQuery } from 'node:querystring'
import { format } from 'node:util'
import { Connection, type OpenOptions } from './connection.ts'
import * as credentials from './credentials.ts'
import type { Table } from './codec.ts'
import type { Credentials } from './credentials.ts'
import type { Options, SocketOptions } from './properties.ts'

const manifest: { version: string } = JSON.parse(
  readFileSync(new URL('../package.json', import.meta.url), 'utf8')
)

/**
 * What the socket reads into. There is one per connection, and it is read from before the next
 * read overwrites it: a read allocates nothing.
 */
const READ_BUFFER = 128 * 1024

export type UrlObject = Options.Connect
export type { SocketOptions }

const CLIENT_PROPERTIES = {
  product: 'amqplib',
  version: manifest.version,
  platform: format('Node.JS %s', process.version),
  information: 'https://github.com/toa-io/amqplib',
  capabilities: {
    publisher_confirms: true,
    exchange_exchange_bindings: true,
    'basic.nack': true,
    consumer_cancel_notify: true,
    'connection.blocked': true,
    authentication_failure_close: true,
  },
}

function intOrDefault(value: unknown, otherwise: number): number {
  return value === undefined ? otherwise : parseInt(value as string, 10)
}

function openFrames(
  vhost: string | null | undefined,
  query: Record<string, unknown> | undefined,
  creds: Credentials,
  extraClientProperties: Table
): OpenOptions {
  query ??= {}

  return {
    clientProperties: { ...CLIENT_PROPERTIES, ...extraClientProperties },
    mechanism: creds.mechanism,
    response: creds.response(),
    locale: (query.locale as string) || 'en_US',
    channelMax: intOrDefault(query.channelMax, 0),
    frameMax: intOrDefault(query.frameMax, 131072),
    heartbeat:
      query.heartbeat === undefined || query.heartbeat === null
        ? null
        : intOrDefault(query.heartbeat, 0),
    virtualHost: vhost ? unescapeQuery(vhost) : '/',
    capabilities: '',
    insist: 0,
  }
}

export function credentialsFromUrl(parts: { username?: string; password?: string }): Credentials {
  let user = 'guest'
  let passwd = 'guest'

  if (parts.username !== '' || parts.password !== '') {
    // oxlint-disable-next-line no-restricted-globals -- what the original does with credentials
    user = parts.username ? unescape(parts.username) : ''
    passwd = parts.password ? unescape(parts.password) : ''
  }

  return credentials.plain(user, passwd)
}

export function connect(
  url: string | UrlObject | false | undefined | null,
  socketOptions: SocketOptions | false | undefined | null,
  openCallback: (error: Error | null, connection?: Connection) => void
): void {
  // tls.connect reads `socketOptions` directly, so everything in it is passed on
  const sockopts: Record<string, any> = { ...socketOptions }

  url ||= 'amqp://localhost'

  const noDelay = !!sockopts.noDelay
  const { timeout } = sockopts
  const keepAlive = !!sockopts.keepAlive
  const keepAliveDelay = sockopts.keepAliveDelay || 0
  const extraClientProperties = sockopts.clientProperties || {}

  let protocol: string
  let fields: OpenOptions

  if (typeof url === 'object') {
    protocol = `${url.protocol || 'amqp'}:`
    sockopts.host = url.hostname

    if (!sockopts.servername && !net.isIP(url.hostname ?? '')) sockopts.servername = url.hostname

    sockopts.port = url.port || (protocol === 'amqp:' ? 5672 : 5671)

    let user: string
    let pass: string

    // only if neither is given, so that a password may be given without a user
    if (url.username === undefined && url.password === undefined) {
      user = 'guest'
      pass = 'guest'
    } else {
      user = url.username || ''
      pass = url.password || ''
    }

    const config = {
      locale: url.locale,
      channelMax: url.channelMax,
      frameMax: url.frameMax,
      heartbeat: url.heartbeat,
    }

    fields = openFrames(
      url.vhost,
      config,
      sockopts.credentials || credentials.plain(user, pass),
      extraClientProperties
    )
  } else {
    let parts: Partial<URL>

    try {
      parts = new URL(url)
    } catch {
      parts = {}
    }

    const host = (parts.hostname || '').replace(/^\[|\]$/g, '')

    protocol = parts.protocol || ''
    sockopts.host = host

    if (!sockopts.servername && !net.isIP(host)) sockopts.servername = host

    sockopts.port = parseInt(parts.port!, 10) || (protocol === 'amqp:' ? 5672 : 5671)

    const vhost = parts.pathname ? parts.pathname.slice(1) : null
    const query: Record<string, unknown> = parseQuery(parts.search ? parts.search.slice(1) : '')

    for (const key in query) if (Array.isArray(query[key])) [query[key]] = query[key]

    fields = openFrames(
      vhost,
      query,
      sockopts.credentials || credentialsFromUrl(parts),
      extraClientProperties
    )
  }

  let sockok = false
  let sock: net.Socket
  let connection: Connection | undefined

  // The socket reads into one buffer and hands it over, instead of allocating a buffer per read
  // and emitting it. Nothing arrives before the connection is there to take it: the server
  // speaks second.
  sockopts.onread ??= {
    buffer: Buffer.allocUnsafeSlow(READ_BUFFER),
    callback: (length: number, buffer: Buffer): boolean => {
      connection?.receive(buffer, 0, length)

      return true
    },
  }

  const onConnect = (): void => {
    sockok = true
    sock.setNoDelay(noDelay)

    if (keepAlive) sock.setKeepAlive(keepAlive, keepAliveDelay)

    connection = new Connection(sock, true)

    connection.open(fields, err => {
      // the timeout was for connecting only
      if (timeout) sock.setTimeout(0)

      if (err === null) openCallback(null, connection)
      else {
        // the server has not said so, but the connection is of no use
        sock.end()
        sock.destroy()
        openCallback(err)
      }
    })
  }

  if (protocol === 'amqp:') sock = net.connect(sockopts as net.NetConnectOpts, onConnect)
  else if (protocol === 'amqps:') sock = tls.connect(sockopts as tls.ConnectionOptions, onConnect)
  else throw new Error(`Expected amqp: or amqps: as the protocol; got ${protocol}`)

  if (timeout)
    sock.setTimeout(timeout, () => {
      sock.end()
      sock.destroy()
      openCallback(new Error('connect ETIMEDOUT'))
    })

  sock.once('error', err => {
    if (!sockok) openCallback(err)
  })
}
