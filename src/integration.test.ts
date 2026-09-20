// Against a broker: what the library assumes of a socket, of TLS and of RabbitMQ, checked on
// them. `npm run rabbitmq` starts the broker, and `URL` names another one.

import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import { mkdtempSync, readFileSync } from 'node:fs'
import net from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { after, describe, it } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import tls from 'node:tls'
import { connect, type ChannelModel, type ConnectOptions, type Message } from './index.ts'
import { units } from './heartbeat.ts'

const URL = process.env.URL ?? 'amqp://localhost'
const broker = new globalThis.URL(URL)
const BROKER = { host: broker.hostname, port: Number(broker.port || 5672) }

/**
 * Something between the client and the broker that can do to the connection what networks do:
 * cut it, or go quiet.
 */
class Proxy {
  public readonly server: net.Server
  public silent = false

  private readonly sockets = new Set<net.Socket>()

  public constructor(server: net.Server = net.createServer()) {
    this.server = server

    const event = server instanceof tls.Server ? 'secureConnection' : 'connection'

    server.on(event, (client: net.Socket) => {
      const upstream = net.connect(BROKER)

      for (const socket of [client, upstream]) {
        this.sockets.add(socket)
        socket.on('error', () => undefined)
        socket.on('close', () => this.sockets.delete(socket))
      }

      client.on('data', chunk => this.silent || upstream.write(chunk))
      upstream.on('data', chunk => this.silent || client.write(chunk))
      client.on('end', () => upstream.end())
      upstream.on('end', () => client.end())
    })
  }

  public async listen(): Promise<number> {
    await new Promise<void>(resolve => this.server.listen(0, '127.0.0.1', resolve))

    return (this.server.address() as net.AddressInfo).port
  }

  public cut(): void {
    for (const socket of this.sockets) socket.destroy()
  }

  public close(): void {
    this.cut()
    this.server.close()
  }
}

const connections: ChannelModel[] = []
const proxies: Proxy[] = []

async function open(url = URL, options?: ConnectOptions): Promise<ChannelModel> {
  const connection = await connect(url, options)

  connections.push(connection)

  return connection
}

async function through(proxy: Proxy, protocol = 'amqp', query = ''): Promise<string> {
  proxies.push(proxy)

  return `${protocol}://${broker.username || 'guest'}:${broker.password || 'guest'}@127.0.0.1:${await proxy.listen()}${broker.pathname}${query}`
}

after(async () => {
  for (const connection of connections) await connection.close().catch(() => undefined)

  for (const proxy of proxies) proxy.close()
})

describe('messages', () => {
  for (const size of [0, 1, 4096, 131_064, 131_065, 1_000_000, 16 * 1024 * 1024])
    it(`carries ${size} bytes there and back`, async () => {
      const connection = await open()
      const channel = await connection.createChannel()
      const { queue } = await channel.assertQueue('', { exclusive: true })
      const content = randomBytes(size)
      const received = new Promise<Message>(resolve => channel.consume(queue, m => resolve(m!)))

      channel.sendToQueue(queue, content, {
        contentType: 'application/octet-stream',
        headers: { binary: content.subarray(0, 100), text: 'héllo', nested: { list: [1, 'two'] } },
      })

      const message = await received

      assert.equal(message.content.equals(content), true)
      assert.equal(message.fields.routingKey, queue)
      assert.equal(message.properties.contentType, 'application/octet-stream')

      assert.deepEqual(message.properties.headers, {
        binary: content.subarray(0, 100),
        text: 'héllo',
        nested: { list: [1, 'two'] },
      })
    })

  it('keeps every message as it was while others arrive', async () => {
    const connection = await open()
    const channel = await connection.createConfirmChannel()
    const { queue } = await channel.assertQueue('', { exclusive: true })
    const contents = Array.from({ length: 5000 }, (_, i) => randomBytes(1 + (i % 3000)))
    const received: Message[] = []

    const all = new Promise<void>(resolve =>
      channel.consume(queue, m => received.push(m!) === contents.length && resolve(), {
        noAck: true,
      })
    )

    for (const [i, content] of contents.entries())
      channel.sendToQueue(queue, content, { messageId: String(i) })

    await channel.waitForConfirms()
    await all

    for (const [i, message] of received.entries()) {
      assert.equal(message.properties.messageId, String(i))
      assert.equal(message.content.equals(contents[i]!), true)
    }
  })

  it('tells a publisher that outruns the socket to wait, and when to go on', async () => {
    const connection = await open()
    const channel = await connection.createChannel({ highWaterMark: 1_000_000 })
    const { queue } = await channel.assertQueue('', { exclusive: true })
    const content = Buffer.alloc(256 * 1024)

    let sent = 0

    while (channel.sendToQueue(queue, content)) sent++

    await new Promise(resolve => channel.once('drain', resolve))

    assert.equal(sent > 0 && sent < 1_000_000, true)
    assert.equal(channel.sendToQueue(queue, content), true)

    // everything written before and after the wait gets there
    let received = 0

    await new Promise<void>(resolve =>
      channel.consume(queue, () => ++received === sent + 2 && resolve(), { noAck: true })
    )
  })
})

describe('TLS', () => {
  it('reads and writes over TLS as it does over TCP', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'amqplib-tls-'))
    const key = join(directory, 'key.pem')
    const cert = join(directory, 'cert.pem')

    execFileSync(
      'openssl',
      `req -x509 -newkey rsa:2048 -nodes -keyout ${key} -out ${cert} -days 1 -subj /CN=localhost`.split(
        ' '
      ),
      { stdio: 'ignore' }
    )

    const proxy = new Proxy(tls.createServer({ key: readFileSync(key), cert: readFileSync(cert) }))
    const connection = await open(await through(proxy, 'amqps'), {
      ca: readFileSync(cert),
      servername: 'localhost',
    })
    const channel = await connection.createChannel()
    const { queue } = await channel.assertQueue('', { exclusive: true })
    const content = randomBytes(1_000_000)
    const received = new Promise<Message>(resolve => channel.consume(queue, m => resolve(m!)))

    channel.sendToQueue(queue, content)

    assert.equal((await received).content.equals(content), true)
  })
})

describe('failures', () => {
  it('says so when the connection is cut', async () => {
    const proxy = new Proxy()
    const connection = await connect(await through(proxy))
    const channel = await connection.createChannel()
    const failed = new Promise<Error>(resolve => connection.once('error', resolve))
    const closed = new Promise<Error>(resolve => connection.once('close', resolve))
    const channelClosed = new Promise(resolve => channel.once('close', resolve))

    proxy.cut()

    assert.match((await failed).message, /Unexpected close|ECONNRESET/)
    assert.equal(await closed, await failed)
    await channelClosed

    assert.throws(() => channel.sendToQueue('anywhere', Buffer.alloc(1)), /Channel closed/)
    await assert.rejects(connection.createChannel(), /Connection closed/)
  })

  it('fails what awaits a reply when the connection is cut', async () => {
    const proxy = new Proxy()
    const connection = await connect(await through(proxy))
    const channel = await connection.createChannel()

    connection.on('error', () => undefined)
    proxy.silent = true

    const reply = channel.assertQueue('', { exclusive: true })

    proxy.cut()

    await assert.rejects(reply, /Channel ended, no reply will be forthcoming/)
  })

  it('gives up on a connection that went quiet', async () => {
    const proxy = new Proxy()

    units.ms = 100

    try {
      const connection = await connect(await through(proxy, 'amqp', '?heartbeat=1'))
      const failed = new Promise<Error>(resolve => connection.once('error', resolve))
      const started = Date.now()

      // heartbeats keep it up for as long as they pass
      await sleep(500)
      proxy.silent = true

      assert.match((await failed).message, /Heartbeat timeout/)
      assert.equal(Date.now() - started >= 500, true)
    } finally {
      units.ms = 1000
    }
  })

  it('refuses to connect where nobody listens, and where nobody answers', async () => {
    await assert.rejects(connect('amqp://127.0.0.1:1'), /ECONNREFUSED/)

    const mute = net.createServer(() => undefined)

    await new Promise<void>(resolve => mute.listen(0, '127.0.0.1', resolve))

    try {
      const { port } = mute.address() as net.AddressInfo

      await assert.rejects(connect(`amqp://127.0.0.1:${port}`, { timeout: 200 }), /ETIMEDOUT/)
    } finally {
      mute.close()
    }
  })

  it('comes back after the connection is cut, when asked to recover', async () => {
    const proxy = new Proxy()

    const connection = await connect(await through(proxy), {
      recovery: { initialDelay: 10, jitter: 0 },
    })

    connection.on('error', () => undefined)

    const reconnected = new Promise(resolve => connection.once('connect', resolve))

    proxy.cut()
    await reconnected

    const channel = await connection.createChannel()
    const { queue } = await channel.assertQueue('', { exclusive: true })

    assert.equal(typeof queue, 'string')
    await connection.close()
  })
})
