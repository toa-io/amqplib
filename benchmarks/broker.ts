// A socket with a broker behind it, in the same process: it answers what a client needs answered
// to get to publishing and consuming, and counts the rest. With no network and no broker in the
// way, what is left to measure is the library.

import { Duplex } from 'node:stream'
import * as defs from '../src/defs.ts'
import { FRAME_END } from '../src/frame.ts'

const START = {
  versionMajor: 0,
  versionMinor: 9,
  serverProperties: { product: 'benchmark' },
  mechanisms: Buffer.from('PLAIN'),
  locales: Buffer.from('en_US'),
}

function method(id: number, channel: number, fields: object): Buffer {
  const buffer = Buffer.alloc(4096)

  return buffer.subarray(0, defs.encodeMethod(id, buffer, 0, channel, fields))
}

/** One delivery on the wire, its content in frames of at most `frameMax`. */
export function delivery(channel: number, content: Buffer, frameMax: number): Buffer {
  const maxBody = frameMax - defs.FRAME_OVERHEAD
  const buffer = Buffer.alloc(8192 + content.length + Math.ceil(content.length / maxBody) * 8)

  let offset = defs.encodeBasicDeliver(buffer, 0, channel, {
    consumerTag: 'benchmark',
    deliveryTag: 1,
    redelivered: false,
    exchange: 'benchmark.exchange',
    routingKey: 'benchmark.key',
  })

  offset = defs.encodeBasicProperties(buffer, offset, channel, content.length, {
    contentType: 'application/octet-stream',
    deliveryMode: 1,
    headers: {},
  })

  for (let from = 0; from < content.length; from += maxBody) {
    const to = Math.min(from + maxBody, content.length)

    buffer[offset] = 3
    buffer.writeUInt16BE(channel, offset + 1)
    buffer.writeUInt32BE(to - from, offset + 3)
    offset += 7 + content.copy(buffer, offset + 7, from, to)
    buffer[offset++] = FRAME_END
  }

  return buffer.subarray(0, offset)
}

export class Broker extends Duplex {
  public writes = 0
  public bytes = 0
  public published = 0
  public acknowledged = 0

  /** Only counts what is written: for when reading it would be most of what is measured. */
  public quiet = false

  /** What is left of a frame that a write cut in two. */
  private rest: Buffer = Buffer.alloc(0)
  private greeted = false

  /** Hands bytes to the client. A client that reads its socket by hand replaces this. */
  public deliver: (bytes: Buffer) => void = bytes => {
    this.push(bytes)
  }

  public constructor() {
    // a socket's own: what `write` answers past this is what makes a client wait for `drain`
    super({ writableHighWaterMark: 16384 })
  }

  public override _read(): void {}

  public override _write(chunk: Buffer, _: string, callback: () => void): void {
    this.writes++
    this.take(chunk)

    // a socket finishes a write on a later turn
    setImmediate(callback)
  }

  public override _writev(chunks: { chunk: Buffer }[], callback: () => void): void {
    this.writes++

    for (const { chunk } of chunks) this.take(chunk)

    setImmediate(callback)
  }

  private take(chunk: Buffer): void {
    this.bytes += chunk.length

    if (this.quiet) return

    let bytes = this.rest.length === 0 ? chunk : Buffer.concat([this.rest, chunk])

    if (!this.greeted) {
      if (bytes.length < 8) {
        this.rest = bytes

        return
      }

      this.greeted = true
      bytes = bytes.subarray(8)
      this.deliver(method(defs.ConnectionStart, 0, START))
    }

    let offset = 0

    while (bytes.length - offset >= 8) {
      const size = bytes.readUInt32BE(offset + 3)

      if (bytes.length - offset < size + 8) break

      if (bytes[offset] === 1)
        this.answer(bytes.readUInt16BE(offset + 1), bytes.readUInt32BE(offset + 7))

      offset += size + 8
    }

    this.rest = offset === bytes.length ? Buffer.alloc(0) : Buffer.from(bytes.subarray(offset))
  }

  private answer(channel: number, id: number): void {
    switch (id) {
      case defs.BasicPublish:
        this.published++

        return

      case defs.BasicAck:
        this.acknowledged++

        return

      case defs.ConnectionStartOk:
        return this.deliver(
          method(defs.ConnectionTune, 0, { channelMax: 0, frameMax: 131072, heartbeat: 0 })
        )

      case defs.ConnectionOpen:
        return this.deliver(method(defs.ConnectionOpenOk, 0, { knownHosts: '' }))

      case defs.ChannelOpen:
        return this.deliver(method(defs.ChannelOpenOk, channel, { channelId: Buffer.alloc(0) }))

      case defs.BasicConsume:
        return this.deliver(method(defs.BasicConsumeOk, channel, { consumerTag: 'benchmark' }))

      case defs.ConnectionClose:
        return this.deliver(method(defs.ConnectionCloseOk, 0, {}))

      default:
    }
  }
}
