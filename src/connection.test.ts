import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { setImmediate as turn } from 'node:timers/promises'
import { Channel, type Message } from './channel.ts'
import * as defs from './defs.ts'
import { FRAME_BODY, FRAME_END, FRAME_HEADER, FRAME_METHOD } from './frame.ts'
import { open, pair, serve, type Opened } from './harness.ts'

const DELIVER = {
  consumerTag: 'tag',
  deliveryTag: 1,
  redelivered: false,
  exchange: 'exchange',
  routingKey: 'key',
}

const PUBLISH = { exchange: 'exchange', routingKey: 'key' }

async function channel(opened: Opened): Promise<Channel> {
  const ch = new Channel(opened.connection).allocate()
  const ok = new Promise(resolve =>
    ch._rpc(defs.ChannelOpen, { outOfBand: '' }, defs.ChannelOpenOk, resolve)
  )
  const { channel: number } = await opened.server.expect(defs.ChannelOpen)

  opened.server.send(defs.ChannelOpenOk, { channelId: Buffer.alloc(0) }, number)
  await ok

  return ch
}

/** The bytes of a delivery, as a server that may send frames of `frameMax` would write them. */
async function delivery(content: Buffer, properties: object, frameMax: number): Promise<Buffer> {
  const streams = pair()
  const server = serve(streams.client)

  server.connection.frameMax = frameMax
  server.connection.sendMessage(
    1,
    defs.BasicDeliver,
    DELIVER,
    defs.BasicProperties,
    properties,
    content
  )

  await turn()

  return Buffer.concat(streams.client.written)
}

interface RawFrame {
  type: number
  channel: number
  payload: Buffer
}

function frames(bytes: Buffer): RawFrame[] {
  const found: RawFrame[] = []

  for (let offset = 0; offset < bytes.length;) {
    const size = bytes.readUInt32BE(offset + 3)

    assert.equal(bytes[offset + 7 + size], FRAME_END)

    found.push({
      type: bytes[offset]!,
      channel: bytes.readUInt16BE(offset + 1),
      payload: bytes.subarray(offset + 7, offset + 7 + size),
    })

    offset += size + 8
  }

  return found
}

describe('receiving', () => {
  const content = Buffer.from(Array.from({ length: 10_000 }, (_, i) => i % 251))
  const properties = { contentType: 'application/octet-stream', headers: { a: 1, b: 'two' } }

  for (const piece of [1, 2, 3, 7, 8, 9, 64, 4095, 4096, 4097, 100_000])
    it(`puts a message together from reads of ${piece} bytes`, async () => {
      const opened = await open()
      const ch = await channel(opened)
      const received: Message[] = []

      ch.on('delivery', message => received.push(message))

      const bytes = Buffer.concat([
        await delivery(content, properties, 4096),
        await delivery(Buffer.alloc(0), {}, 4096),
        await delivery(content.subarray(0, 10), properties, 4096),
      ])

      // the buffer handed over is overwritten after each read, as a socket's is
      const read = Buffer.alloc(piece)

      for (let offset = 0; offset < bytes.length; offset += piece) {
        const length = bytes.copy(read, 0, offset, offset + piece)

        opened.connection.receive(read, 0, length)
        read.fill(0)
      }

      assert.equal(received.length, 3)
      assert.deepEqual(received[0]!.content, content)
      assert.deepEqual(received[0]!.fields, DELIVER)
      assert.equal(received[0]!.properties.contentType, 'application/octet-stream')
      assert.deepEqual(received[0]!.properties.headers, { a: 1, b: 'two' })
      assert.equal(received[1]!.content.length, 0)
      assert.deepEqual(received[2]!.content, content.subarray(0, 10))
    })

  it('owns the bytes of a message: nothing of the read buffer is kept', async () => {
    const opened = await open()
    const ch = await channel(opened)
    const received: Message[] = []

    ch.on('delivery', message => received.push(message))

    const bytes = await delivery(
      Buffer.from('content'),
      { headers: { bytes: Buffer.from('b') } },
      4096
    )

    // memory of its own, so that sharing it would show
    const read = Buffer.allocUnsafeSlow(bytes.length)

    bytes.copy(read)
    opened.connection.receive(read, 0, read.length)
    read.fill(0)

    assert.equal(received[0]!.content.toString(), 'content')
    assert.notEqual(received[0]!.content.buffer, read.buffer)
    assert.deepEqual(received[0]!.properties.headers!.bytes, Buffer.from('b'))
    assert.equal(received[0]!.fields.routingKey, 'key')
  })

  it('refuses a frame larger than what was negotiated', async () => {
    const opened = await open({}, { frameMax: 4096 })
    const failed = new Promise<Error>(resolve => opened.connection.once('error', resolve))
    const frame = Buffer.alloc(16)

    frame[0] = FRAME_METHOD
    frame.writeUInt32BE(4096, 3)
    opened.connection.receive(frame, 0, frame.length)

    assert.match((await failed).message, /Frame size exceeds frame max/)
  })
})

describe('sending', () => {
  it('splits content into frames no larger than the frame max', async () => {
    for (const size of [0, 1, 4087, 4088, 4089, 8176, 8177, 100_000]) {
      const content = Buffer.from(Array.from({ length: size }, (_, i) => i % 251))
      const sent = frames(await delivery(content, {}, 4096))
      const bodies = sent.slice(2)

      assert.equal(sent[0]!.type, FRAME_METHOD)
      assert.equal(sent[1]!.type, FRAME_HEADER)
      assert.equal(Number(sent[1]!.payload.readBigUInt64BE(4)), size)

      for (const body of bodies) {
        assert.equal(body.type, FRAME_BODY)
        assert.equal(body.payload.length > 0 && body.payload.length + 8 <= 4096, true)
      }

      assert.equal(bodies.length, Math.ceil(size / 4088))
      assert.deepEqual(Buffer.concat(bodies.map(body => body.payload)), content)
    }
  })

  it('writes what was sent during a turn in one write', async () => {
    const opened = await open()
    const ch = await channel(opened)
    const before = opened.client.written.length

    for (let i = 0; i < 100; i++) ch.sendMessage(PUBLISH, {}, Buffer.alloc(100))

    ch.sendImmediately(defs.BasicAck, { deliveryTag: 1, multiple: false })
    await turn()

    assert.equal(opened.client.written.length, before + 1)
    assert.equal(frames(opened.client.written.at(-1)!).length, 301)
  })

  it('writes the replies to what arrived in one read in one write, before the read returns', async () => {
    const opened = await open()
    const ch = await channel(opened)

    ch.on('delivery', () => ch.sendImmediately(defs.BasicAck, { deliveryTag: 1, multiple: false }))

    const one = await delivery(Buffer.from('x'), {}, 4096)
    const before = opened.client.written.length

    opened.connection.receive(Buffer.concat([one, one, one]), 0, one.length * 3)

    assert.equal(opened.client.written.length, before + 1)
    assert.equal(frames(opened.client.written.at(-1)!).length, 3)
  })

  it('sends a table that is larger than anything allocated so far', async () => {
    const opened = await open()
    const ch = await channel(opened)
    const large = Buffer.alloc(300_000, 7)

    ch.sendMessage(PUBLISH, { headers: { large } }, Buffer.from('after'))
    ch.sendMessage(PUBLISH, { headers: { small: 1 } }, Buffer.from('small'))

    await opened.server.expect(defs.BasicPublish)
    assert.deepEqual((await opened.server.next()).fields!.headers, { large })
    assert.equal((await opened.server.next()).content!.toString(), 'after')
    await opened.server.expect(defs.BasicPublish)
    assert.deepEqual((await opened.server.next()).fields!.headers, { small: 1 })
    assert.equal((await opened.server.next()).content!.toString(), 'small')
  })

  it('leaves nothing behind of a message that could not be encoded', async () => {
    const opened = await open()
    const ch = await channel(opened)

    assert.throws(() => ch.sendMessage(PUBLISH, { contentType: 7 }, Buffer.from('bad')), TypeError)
    ch.sendMessage(PUBLISH, {}, Buffer.from('good'))

    await opened.server.expect(defs.BasicPublish)
    await opened.server.expect(defs.BasicProperties)
    assert.equal((await opened.server.next()).content!.toString(), 'good')
  })

  it('says when a channel has written enough, and when it may go on', async () => {
    const opened = await open()
    const ch = new Channel(opened.connection)

    ch.setOptions({ highWaterMark: 10 })
    ch.allocate()

    let drained = false

    ch.on('drain', () => (drained = true))

    for (let i = 0; i < 10; i++) assert.equal(ch.sendMessage(PUBLISH, {}, Buffer.alloc(1)), true)

    assert.equal(ch.sendMessage(PUBLISH, {}, Buffer.alloc(1)), false)
    assert.equal(drained, false)
    await turn()
    assert.equal(drained, true)
    assert.equal(ch.sendMessage(PUBLISH, {}, Buffer.alloc(1)), true)
  })
})

describe('closing', () => {
  it('writes the CloseOk it owes before it ends the stream, when a close listener throws', async () => {
    const opened = await open()
    const ch = await channel(opened)
    const thrown = new Error('a close listener throws')
    const failed = new Promise<Error>(resolve => opened.connection.once('error', resolve))
    const closed = new Promise(resolve => opened.connection.once('close', resolve))

    ch.once('error', () => undefined)

    ch.on('close', () => {
      throw thrown
    })

    opened.server.send(
      defs.ChannelClose,
      { replyText: 'Forced close', replyCode: 504, classId: 0, methodId: 0 },
      ch.ch
    )

    assert.equal(await failed, thrown)
    await closed
    await opened.server.expect(defs.ChannelCloseOk)
    assert.equal(opened.client.writableEnded, true)
  })
})
