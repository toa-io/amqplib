// A connection: the bytes that arrive are taken apart into frames and handed to channels, and
// the frames that channels send are written out.
//
// Receiving. Bytes are read where they arrived: a frame that came whole is decoded in place, and
// the content of a message is copied straight into the buffer the message will own, piece by
// piece, however the frames that carry it were split across reads. Nothing is concatenated and no
// frame becomes an object. The only bytes that are kept between reads are those of a method or a
// header frame that a read cut in two.
//
// Sending. Frames are encoded into an arena, one after another, and everything written during a
// turn of the event loop goes to the socket in one write. A stretch of the arena that went to the
// socket is never written to again until the socket is done with it.

import { EventEmitter } from 'node:events'
import { format } from 'node:util'
import type { Duplex } from 'node:stream'
import { BitSet } from './bitset.ts'
import { OVERFLOW, readUInt64, type Table } from './codec.ts'
import * as defs from './defs.ts'
import { safeEmit } from './emit.ts'
import { IllegalOperationError, stackCapture } from './error.ts'
import { closeMessage, inspect, methodName } from './format.ts'
import {
  FRAME_BODY,
  FRAME_END,
  FRAME_HEADER,
  FRAME_HEARTBEAT,
  FRAME_METHOD,
  FRAME_PREFIX,
  HEARTBEAT,
  HEARTBEAT_BUF,
  PROTOCOL_HEADER,
  type Frame,
} from './frame.ts'
import { Heart } from './heartbeat.ts'

const { constants, FRAME_OVERHEAD, HEADROOM } = defs

/** The most the encoding can say: what a negotiated zero, which is no limit, comes to. */
const MAX_SHORT = 65535
const MAX_LONG = 4294967295

/** How much is allocated at a time for outgoing frames. */
const ARENA = 64 * 1024

/** How much of what was allocated is kept for writing to again, at most. */
const POOL = 1024 * 1024

/**
 * How many bytes are taken within a turn before whoever writes is told to stop. Nothing goes to
 * the socket until the turn ends, so without this a loop that publishes until told to stop
 * would never be.
 */
const WRITE_LIMIT = 4 * 1024 * 1024

/** How many messages a channel takes within a turn before it says it has had enough. */
const DEFAULT_WRITE_HWM = 1024

/** What a connection asks of the channels it carries. */
export interface ChannelSink {
  onMethod(id: number, buffer: Buffer, offset: number): void
  onHeader(buffer: Buffer, offset: number): void
  onBody(buffer: Buffer, start: number, end: number): void
  onBufferDrain(): void
  toClosed(capturedStack?: string): void
}

interface ChannelRecord {
  channel: ChannelSink | null
  /** The turn the count below belongs to. */
  epoch: number
  /** Messages written during that turn. */
  written: number
  highWaterMark: number
  /** Was told to stop, and is owed a `drain`. */
  starved: boolean
}

export interface OpenOptions {
  clientProperties?: Table
  mechanism: string
  response: Buffer
  locale?: string
  channelMax: number
  frameMax: number
  heartbeat: number | null
  virtualHost?: string
  capabilities?: string
  insist?: number | boolean
  [option: string]: unknown
}

export interface ChannelOptions {
  highWaterMark?: number
}

/**
 * An arena and how many stretches of it the socket has not finished writing. While there are
 * any, nothing of the arena is written over.
 */
interface Lease {
  readonly buffer: Buffer
  pending: number
  readonly done: () => void
}

type State = 'new' | 'opening' | 'open' | 'closing' | 'closed'
type Handshake = (id: number, buffer: Buffer, offset: number) => void
type Callback<T = void> = (error: Error | null, value?: T) => void

const EMPTY = Buffer.alloc(0)
const NO_LEASE: Lease = { buffer: EMPTY, pending: 0, done: () => undefined }

export class Connection extends EventEmitter {
  public readonly stream: Duplex
  public frameMax: number = constants.FRAME_MIN_SIZE
  public channelMax = MAX_SHORT
  public heartbeat = 0
  public heartbeater: Heart | null = null
  public serverProperties: Table | undefined
  public expectSocketClose = false
  public blocked = false
  public sentSinceLastCheck = false
  public recvSinceLastCheck = false

  private readonly fed: boolean
  private state: State = 'new'
  private closedBecause = ''
  private closedAt: string | undefined
  private handshake: Handshake | null = null
  private bail: ((error: Error) => void) | null = null
  private closeCallback: (() => void) | undefined

  private readonly freeChannels = new BitSet()
  private readonly channels: (ChannelRecord | null)[] = [
    { channel: null, epoch: -1, written: 0, highWaterMark: DEFAULT_WRITE_HWM, starved: false },
  ]

  // receiving
  private carry = EMPTY
  private carried = 0
  private bodyLeft = 0
  private bodySink: ChannelSink | null = null
  private endDue = false
  private receiving = false
  private frames: Frame[] | null = null
  private content: Buffer | null = null

  // sending
  private arena: Buffer = EMPTY
  private head = 0
  private tail = 0
  private lease: Lease = NO_LEASE
  private sealed: Buffer[] = []
  private sealedLeases: Lease[] = []
  private readonly pool: Lease[] = []
  private pooled = 0
  private sealedBytes = 0
  private scheduled = false
  private saturated = false
  private epoch = 0
  private starved: ChannelRecord[] = []

  private readonly flushing = (): void => this.flush()

  /**
   * @param fed - the bytes will be handed to `receive` by whoever made the stream, rather than
   *   read from it
   */
  public constructor(stream: Duplex, fed = false) {
    super()

    this.stream = stream
    this.fed = fed
    this.freeChannels.set(0)
  }

  // region opening

  /*
    The opening handshake (section 2.2.4 of the specification):

      protocol header ->
        <- start
      start-ok ->
        <- tune
      tune-ok ->
      open ->
        <- open-ok
  */
  public open(allFields: OpenOptions, openCallback0?: Callback<Frame>): void {
    const openCallback = openCallback0 ?? (() => undefined)

    // what was asked for, under what gets negotiated
    const tunedOptions: OpenOptions = Object.create(allFields)

    const bail = (error: Error): void => {
      if (this.bail === null) return

      this.bail = null
      this.handshake = null
      openCallback(error)
    }

    const send = (method: number): void => {
      this.method(0, method, tunedOptions)
    }

    const unexpected = (expected: number, id: number): void => {
      bail(
        new Error(format('Expected %s; got %s', methodName(expected), inspect({ id, channel: 0 })))
      )
    }

    // `0` means no limit, so whichever of the two sets one wins
    const negotiate = (server: number, desired: number): number =>
      server === 0 || desired === 0 ? Math.max(server, desired) : Math.min(server, desired)

    const onStart: Handshake = (id, buffer, offset) => {
      if (id !== defs.ConnectionStart) return unexpected(defs.ConnectionStart, id)

      const start = defs.decodeConnectionStart(buffer, offset)
      const mechanisms = start.mechanisms.toString().split(' ')

      if (!mechanisms.includes(allFields.mechanism))
        return bail(
          new Error(format('SASL mechanism %s is not provided by the server', allFields.mechanism))
        )

      this.serverProperties = start.serverProperties

      try {
        send(defs.ConnectionStartOk)
      } catch (error) {
        return bail(error as Error)
      }

      this.handshake = afterStartOk
    }

    const afterStartOk: Handshake = (id, buffer, offset) => {
      switch (id) {
        case defs.ConnectionSecure:
          return bail(new Error("Wasn't expecting to have to go through secure"))

        case defs.ConnectionClose:
          return bail(
            new Error(
              format(
                'Handshake terminated by server: %s',
                closeMessage(defs.decodeConnectionClose(buffer, offset))
              )
            )
          )

        case defs.ConnectionTune: {
          const fields = defs.decodeConnectionTune(buffer, offset)

          tunedOptions.frameMax = negotiate(fields.frameMax, allFields.frameMax)
          tunedOptions.channelMax = negotiate(fields.channelMax, allFields.channelMax)

          // no preference takes what the server suggests, and 0 turns heartbeats off
          if (allFields.heartbeat === null || allFields.heartbeat === undefined)
            tunedOptions.heartbeat = fields.heartbeat
          else if (allFields.heartbeat === 0) tunedOptions.heartbeat = 0
          else tunedOptions.heartbeat = negotiate(fields.heartbeat, allFields.heartbeat)

          try {
            send(defs.ConnectionTuneOk)
            send(defs.ConnectionOpen)
          } catch (error) {
            return bail(error as Error)
          }

          this.handshake = onOpenOk

          return
        }

        default:
          bail(
            new Error(
              format(
                'Expected connection.secure, connection.close, or connection.tune during handshake; got %s',
                inspect({ id, channel: 0 })
              )
            )
          )
      }
    }

    const onOpenOk: Handshake = (id, buffer, offset) => {
      if (id !== defs.ConnectionOpenOk) return unexpected(defs.ConnectionOpenOk, id)

      // a negotiated zero is the most the encoding can say
      this.channelMax = tunedOptions.channelMax || MAX_SHORT
      this.frameMax = tunedOptions.frameMax || MAX_LONG
      this.heartbeat = tunedOptions.heartbeat ?? 0
      this.heartbeater = this.startHeartbeater()

      this.stream.removeListener('end', endWhileOpening)
      this.stream.removeListener('error', endWhileOpening)
      this.stream.on('error', error => this.onSocketError(error))
      this.stream.on('end', () => this.onSocketError(new Error('Unexpected close')))
      this.on('frameError', error => this.onSocketError(error))

      this.handshake = null
      this.bail = null
      this.state = 'open'

      openCallback(null, {
        id,
        channel: 0,
        fields: defs.decodeConnectionOpenOk(buffer, offset) as unknown as Table,
      })
    }

    // if the server hangs up now, it is probably because of something it was sent
    const endWhileOpening = (error?: Error): void => {
      bail(error ?? new Error('Socket closed abruptly during opening handshake'))
    }

    this.stream.on('end', endWhileOpening)
    this.stream.on('error', endWhileOpening)

    this.state = 'opening'
    this.bail = bail
    this.handshake = onStart

    if (!this.fed) this.stream.on('data', (chunk: Buffer) => this.receive(chunk, 0, chunk.length))

    this.sendBytes(PROTOCOL_HEADER)
  }

  // endregion

  // region closing

  // The closing handshake is the same for a connection and for a channel. Whoever starts it sends
  // Close and then ignores everything but CloseOk, which says the other side is done, and Close,
  // which says the other side started closing at the same time and wants a CloseOk of its own.
  // A Close out of the blue is an error, or a forced closure, which may as well be one.
  //
  //  RUNNING --- send Close ---> CLOSING ---> recv Close --+
  //     |                           |                      |
  //     |                           +---- send CloseOk ----+
  //  recv Close                recv CloseOk
  //     |                           |
  //     V                           V
  //  send CloseOk ------------->  CLOSED

  public close(closeCallback?: (error: Error | null) => void): void {
    if (this.state === 'closed') {
      closeCallback?.(new IllegalOperationError(this.closedBecause, this.closedAt))

      return
    }

    const k = closeCallback === undefined ? undefined : () => closeCallback(null)

    this.closeBecause('Cheers, thanks', constants.REPLY_SUCCESS, k)
  }

  /** The continuation is called once CloseOk has arrived, before the 'close' event. */
  public closeBecause(reason: string, code: number, k?: () => void): void {
    this.sendMethod(0, defs.ConnectionClose, {
      replyText: reason,
      replyCode: code,
      methodId: 0,
      classId: 0,
    })

    this.toClosing(stackCapture(`closeBecause called: ${reason}`), k)
  }

  public closeWithError(reason: string, code: number, error: Error): void {
    safeEmit(this, 'error', error)
    this.closeBecause(reason, code)
  }

  public onSocketError(error: Error): void {
    if (this.expectSocketClose) return

    // 'error' and 'end' both lead here
    this.expectSocketClose = true
    safeEmit(this, 'error', error)
    this.toClosed(stackCapture('Socket error'), error)
  }

  /** A close has been started: nothing more is sent, and the channels are shut. */
  private toClosing(capturedStack: string, k?: () => void): void {
    // a blocked server reads nothing, so its CloseOk would never come
    if (this.blocked) {
      k?.()
      this.toClosed(stackCapture('Connection blocked: closing immediately'), undefined)

      return
    }

    this.state = 'closing'
    this.closedBecause = 'Connection closing'
    this.closedAt = capturedStack
    this.closeCallback = k
  }

  /** A close has been confirmed: all communication ends. */
  public toClosed(capturedStack: string | undefined, maybeErr?: Error): void {
    for (let i = 1; i < this.channels.length; i++)
      this.channels[i]?.channel?.toClosed(capturedStack)

    this.state = 'closed'
    this.closedBecause = format(
      'Connection closed (%s)',
      maybeErr === undefined ? 'by client' : maybeErr.toString()
    )
    this.closedAt = capturedStack
    this.heartbeater?.clear()
    this.expectSocketClose = true
    this.flush()
    this.stream.end()
    safeEmit(this, 'close', maybeErr)
  }

  // endregion

  public _updateSecret(newSecret: Buffer, reason: string, cb: () => void): void {
    this.sendMethod(0, defs.ConnectionUpdateSecret, { newSecret, reason })
    this.once('update-secret-ok', cb)
  }

  public startHeartbeater(): Heart | null {
    if (this.heartbeat === 0) return null

    return new Heart(
      this.heartbeat,
      () => this.checkSend(),
      () => this.checkRecv(),
      () => this.sendHeartbeat(),
      () => {
        const error = new Error('Heartbeat timeout')

        safeEmit(this, 'error', error)
        this.toClosed(stackCapture('Heartbeat timeout'), error)
      }
    )
  }

  // region channels

  /** Takes the lowest free channel number. */
  public freshChannel(channel: ChannelSink | null, options?: ChannelOptions): number {
    const next = this.freeChannels.nextClearBit(1)

    if (next < 0 || next > this.channelMax) throw new Error('No channels left to allocate')

    this.freeChannels.set(next)

    this.channels[next] = {
      channel,
      epoch: -1,
      written: 0,
      highWaterMark: options?.highWaterMark || DEFAULT_WRITE_HWM,
      starved: false,
    }

    return next
  }

  public releaseChannel(channel: number): void {
    this.freeChannels.clear(channel)
    this.channels[channel] = null
  }

  // endregion

  // region receiving

  /**
   * Takes in a stretch of bytes as they arrived. The bytes are not kept: they may be overwritten
   * as soon as this returns.
   */
  public receive(buffer: Buffer, start: number, end: number): void {
    this.recvSinceLastCheck = true
    this.receiving = true

    try {
      this.parse(buffer, start, end)
    } catch (error) {
      this.receiving = false

      // what was written before it went wrong is still owed to the server
      if (this.head !== this.tail || this.sealed.length > 0) this.schedule()

      if (this.bail !== null) this.bail(error as Error)
      else this.emit('frameError', error)

      return
    }

    this.receiving = false

    // what the frames that came in made channels send goes out in one write
    if (this.head !== this.tail || this.sealed.length > 0) this.flush()
  }

  private parse(buffer: Buffer, offset: number, end: number): void {
    while (offset < end)
      if (this.bodyLeft > 0) {
        const piece = Math.min(this.bodyLeft, end - offset)

        this.body(buffer, offset, offset + piece)
        this.bodyLeft -= piece
        offset += piece
      } else if (this.endDue) {
        if (buffer[offset] !== FRAME_END) throw new Error('Invalid frame')

        this.endDue = false
        offset++
      } else if (this.carried > 0) offset = this.resume(buffer, offset, end)
      else {
        const available = end - offset

        if (available < FRAME_PREFIX) return this.keep(buffer, offset, end)

        const type = buffer[offset]!
        const channel = (buffer[offset + 1]! << 8) | buffer[offset + 2]!
        const size = buffer.readUInt32BE(offset + 3)

        if (size + FRAME_OVERHEAD > this.frameMax) throw new Error('Frame size exceeds frame max')

        if (type === FRAME_BODY) {
          this.expectBody(channel, size)
          offset += FRAME_PREFIX
        } else {
          const total = size + FRAME_OVERHEAD

          if (available < total) return this.keep(buffer, offset, end)

          if (buffer[offset + total - 1] !== FRAME_END) throw new Error('Invalid frame')

          this.dispatch(type, channel, buffer, offset + FRAME_PREFIX, size)
          offset += total
        }
      }
  }

  /** Puts aside the start of a frame whose rest has not arrived yet. */
  private keep(buffer: Buffer, offset: number, end: number): void {
    const length = end - offset

    this.room(length)
    buffer.copy(this.carry, 0, offset, end)
    this.carried = length
  }

  /** Makes sure the frame being put together fits where it is kept. */
  private room(length: number): void {
    if (this.carry.length >= length) return

    const carry = Buffer.allocUnsafeSlow(Math.max(length, 4096))

    this.carry.copy(carry, 0, 0, this.carried)
    this.carry = carry
  }

  /** Goes on with a frame whose start was put aside, and returns how far it read. */
  private resume(buffer: Buffer, offset: number, end: number): number {
    if (this.carried < FRAME_PREFIX) {
      const piece = Math.min(FRAME_PREFIX - this.carried, end - offset)

      this.room(FRAME_PREFIX)
      buffer.copy(this.carry, this.carried, offset, offset + piece)
      this.carried += piece
      offset += piece

      if (this.carried < FRAME_PREFIX) return offset
    }

    const carry = this.carry
    const type = carry[0]!
    const channel = (carry[1]! << 8) | carry[2]!
    const size = carry.readUInt32BE(3)

    if (size + FRAME_OVERHEAD > this.frameMax) throw new Error('Frame size exceeds frame max')

    if (type === FRAME_BODY) {
      this.carried = 0
      this.expectBody(channel, size)

      return offset
    }

    const total = size + FRAME_OVERHEAD
    const piece = Math.min(total - this.carried, end - offset)

    this.room(total)
    buffer.copy(this.carry, this.carried, offset, offset + piece)
    this.carried += piece
    offset += piece

    if (this.carried < total) return offset

    this.carried = 0

    if (this.carry[total - 1] !== FRAME_END) throw new Error('Invalid frame')

    this.dispatch(type, channel, this.carry, FRAME_PREFIX, size)

    return offset
  }

  private dispatch(
    type: number,
    channel: number,
    buffer: Buffer,
    offset: number,
    size: number
  ): void {
    if (this.frames !== null) return this.collect(type, channel, buffer, offset, size)

    switch (type) {
      case FRAME_METHOD: {
        const id = buffer.readUInt32BE(offset)

        if (channel === 0) this.onMethod(id, buffer, offset + 4)
        else if (this.state === 'open') this.sink(channel, id)?.onMethod(id, buffer, offset + 4)
        else if (this.state === 'opening') this.offChannel(id, channel)

        return
      }

      case FRAME_HEADER:
        if (this.state === 'open')
          this.sink(channel, buffer.readUInt16BE(offset))?.onHeader(buffer, offset)
        else if (this.state === 'opening') this.offChannel(buffer.readUInt16BE(offset), channel)

        break

      case FRAME_HEARTBEAT:
        // it has been counted as something received, which is all it is for
        break

      default:
        throw new Error(`Unknown frame type ${type}`)
    }
  }

  /** A content frame has started: what follows is its payload, however many reads that takes. */
  private expectBody(channel: number, size: number): void {
    this.bodyLeft = size
    this.endDue = true

    if (this.frames !== null) {
      const content = Buffer.allocUnsafe(size)

      this.frames.push({ channel, content })

      if (size > 0) this.content = content

      return
    }

    if (this.state === 'open') this.bodySink = this.sink(channel, undefined)
    else {
      this.bodySink = null

      if (this.state === 'opening') this.offChannel(undefined, channel)
    }
  }

  private body(buffer: Buffer, start: number, end: number): void {
    if (this.content !== null) {
      const content = this.content

      buffer.copy(content, content.length - this.bodyLeft, start, end)

      if (end - start === this.bodyLeft) this.content = null
    } else this.bodySink?.onBody(buffer, start, end)
  }

  private sink(channel: number, id: number | undefined): ChannelSink | null {
    const record = this.channels[channel]

    if (record !== null && record !== undefined) return record.channel

    this.closeWithError(
      format('Frame on unknown channel %d', channel),
      constants.CHANNEL_ERROR,
      new Error(format('Frame on unknown channel: %s', inspect({ id, channel })))
    )

    return null
  }

  private offChannel(id: number | undefined, channel: number): void {
    this.bail?.(
      new Error(format('Frame on channel != 0 during handshake: %s', inspect({ id, channel })))
    )
  }

  /** Everything that arrives on channel 0, the one the connection itself is controlled over. */
  private onMethod(id: number, buffer: Buffer, offset: number): void {
    switch (this.state) {
      case 'opening':
        return this.handshake?.(id, buffer, offset)

      case 'open':
        break

      case 'closing':
        if (id === defs.ConnectionCloseOk) {
          this.closeCallback?.()
          this.toClosed(stackCapture('ConnectionCloseOk received'), undefined)
        } else if (id === defs.ConnectionClose) this.method(0, defs.ConnectionCloseOk, {})

        return

      default:
        return
    }

    switch (id) {
      case defs.ConnectionClose: {
        // Nothing more will come, and nothing sent but CloseOk would be read.
        const fields = defs.decodeConnectionClose(buffer, offset)
        const message = format('Connection closed: %s', closeMessage(fields))
        const error: Error & { code?: number } = new Error(message)

        this.method(0, defs.ConnectionCloseOk, {})
        error.code = fields.replyCode

        if (isFatalError(error)) safeEmit(this, 'error', error)

        return this.toClosed(stackCapture(message), error)
      }

      case defs.ConnectionBlocked:
        this.blocked = true

        return safeEmit(this, 'blocked', defs.decodeConnectionBlocked(buffer, offset).reason)

      case defs.ConnectionUnblocked:
        this.blocked = false

        return safeEmit(this, 'unblocked')

      case defs.ConnectionUpdateSecretOk:
        return safeEmit(this, 'update-secret-ok')

      default:
        this.closeWithError(
          'Unexpected frame on channel 0',
          constants.UNEXPECTED_FRAME,
          new Error(format('Unexpected frame on channel 0: %s', inspect({ id, channel: 0 })))
        )
    }
  }

  // endregion

  // region reading frames by hand

  // A connection that is never opened can be read from a frame at a time, which is what a test
  // standing in for a server does.

  private collect(
    type: number,
    channel: number,
    buffer: Buffer,
    offset: number,
    size: number
  ): void {
    const frames = this.frames!

    switch (type) {
      case FRAME_METHOD: {
        const id = buffer.readUInt32BE(offset)

        if (size < 4) throw new Error('Invalid frame')

        frames.push({ id, channel, fields: defs.decode(id, buffer, offset + 4) })

        return
      }

      case FRAME_HEADER: {
        const id = buffer.readUInt16BE(offset)

        frames.push({
          id,
          channel,
          size: readUInt64(buffer, offset + 4),
          fields: defs.decode(id, buffer, offset + 12),
        })

        return
      }

      case FRAME_HEARTBEAT:
        frames.push(HEARTBEAT)

        return

      default:
        throw new Error(`Unknown frame type ${type}`)
    }
  }

  /** The next frame, if all of it has arrived. */
  public recvFrame(): Frame | false {
    this.frames ??= []

    const frames = this.frames

    // a content frame is listed once it starts, and is whole once nothing of it is awaited
    while (frames.length === 0 || (frames.length === 1 && this.content !== null)) {
      const incoming: Buffer | null = this.stream.read()

      if (incoming === null) return false

      this.recvSinceLastCheck = true
      this.parse(incoming, 0, incoming.length)
    }

    return frames.shift()!
  }

  public step(cb: (error: Error | null, frame: Frame | null) => void): void {
    const recv = (): void => {
      let frame: Frame | false

      try {
        frame = this.recvFrame()
      } catch (error) {
        return cb(error as Error, null)
      }

      if (frame === false) this.stream.once('readable', recv)
      else cb(null, frame)
    }

    recv()
  }

  /** What `acceptLoop` hands every frame to. */
  public accept(_frame: Frame): void {}

  public acceptLoop(): void {
    const go = (): void => {
      try {
        for (let frame = this.recvFrame(); frame !== false; frame = this.recvFrame())
          this.accept(frame)
      } catch (error) {
        this.emit('frameError', error)
      }
    }

    this.stream.on('readable', go)
    go()
  }

  // endregion

  // region sending

  public checkSend(): boolean {
    const check = this.sentSinceLastCheck

    this.sentSinceLastCheck = false

    return check
  }

  public checkRecv(): boolean {
    const check = this.recvSinceLastCheck

    this.recvSinceLastCheck = false

    return check
  }

  /** Writes bytes as they are, after everything that is waiting to be written. */
  public sendBytes(bytes: Buffer): void {
    this.sentSinceLastCheck = true
    this.flush()
    this.stream.write(bytes)
  }

  public sendHeartbeat(): void {
    this.reserve(HEARTBEAT_BUF.length)
    this.tail += HEARTBEAT_BUF.copy(this.arena, this.tail)
    this.sentSinceLastCheck = true
    this.schedule()
  }

  public sendMethod(channel: number, method: number, fields: object): boolean {
    if (this.state === 'closing' || this.state === 'closed')
      throw new IllegalOperationError(this.closedBecause, this.closedAt)

    this.method(channel, method, fields)

    return !this.saturated
  }

  public sendMessage(
    channel: number,
    method: number,
    fields: object,
    properties: number,
    props: object,
    content: Buffer
  ): boolean {
    if (this.state === 'closing' || this.state === 'closed')
      throw new IllegalOperationError(this.closedBecause, this.closedAt)

    if (!Buffer.isBuffer(content)) throw new TypeError('content is not a buffer')

    const size = content.length
    const maxBody = this.frameMax - FRAME_OVERHEAD
    const framed = size === 0 ? 0 : size + Math.ceil(size / maxBody) * FRAME_OVERHEAD

    this.reserve(2 * HEADROOM + framed)

    for (;;)
      try {
        const arena = this.arena

        let offset = defs.encodeMethod(method, arena, this.tail, channel, fields)

        offset = defs.encodeProperties(properties, arena, offset, channel, size, props)

        if (offset + framed > arena.length) throw OVERFLOW

        for (let from = 0; from < size; from += maxBody) {
          const to = Math.min(from + maxBody, size)
          const length = to - from

          arena[offset] = FRAME_BODY
          arena[offset + 1] = channel >>> 8
          arena[offset + 2] = channel
          arena[offset + 3] = length >>> 24
          arena[offset + 4] = length >>> 16
          arena[offset + 5] = length >>> 8
          arena[offset + 6] = length
          content.copy(arena, offset + FRAME_PREFIX, from, to)
          offset += FRAME_PREFIX + length
          arena[offset++] = FRAME_END
        }

        this.tail = offset

        break
      } catch (error) {
        if (error !== OVERFLOW) throw error

        // a table took more than was left: start over with room to spare
        this.grow(2 * this.arena.length + framed)
      }

    this.sentSinceLastCheck = true
    this.schedule()

    return this.took(channel)
  }

  /** Whether the channel may go on writing, having written one more message. */
  private took(channel: number): boolean {
    const record = this.channels[channel]!

    if (record.epoch !== this.epoch) {
      record.epoch = this.epoch
      record.written = 0
    }

    if (
      ++record.written <= record.highWaterMark &&
      !this.saturated &&
      this.sealedBytes + this.tail - this.head < WRITE_LIMIT
    )
      return true

    if (!record.starved) {
      record.starved = true
      this.starved.push(record)
    }

    return false
  }

  private method(channel: number, method: number, fields: object): void {
    this.reserve(HEADROOM)

    for (;;)
      try {
        this.tail = defs.encodeMethod(method, this.arena, this.tail, channel, fields)

        break
      } catch (error) {
        if (error !== OVERFLOW) throw error

        this.grow(2 * this.arena.length)
      }

    this.sentSinceLastCheck = true
    this.schedule()
  }

  private reserve(length: number): void {
    if (this.arena.length - this.tail < length) this.grow(length)
  }

  /** Sets what has been written aside for the next flush, and goes on in another arena. */
  private grow(length: number): void {
    const { lease } = this

    if (this.tail > this.head) {
      lease.pending++
      this.sealedBytes += this.tail - this.head
      this.sealed.push(this.arena.subarray(this.head, this.tail))
      this.sealedLeases.push(lease)
    } else if (lease.pending === 0) this.shelve(lease)

    this.lease = this.take(length)
    this.arena = this.lease.buffer
    this.reset()
  }

  /** An arena nothing is using, or a new one. Memory written to before is the cheap kind. */
  private take(length: number): Lease {
    const { pool } = this

    for (let i = 0; i < pool.length; i++)
      if (pool[i]!.buffer.length >= length) {
        const [lease] = pool.splice(i, 1)

        this.pooled -= lease!.buffer.length

        return lease!
      }

    let size = ARENA

    while (size < length) size *= 2

    const lease: Lease = {
      buffer: Buffer.allocUnsafeSlow(size),
      pending: 0,
      done: () => {
        // the socket is done with every stretch of the arena it was given
        if (--lease.pending > 0) return

        if (lease !== this.lease) this.shelve(lease)
        else if (this.head === this.tail) this.reset()
      },
    }

    return lease
  }

  /** Keeps an arena for later, unless enough of them are kept already. */
  private shelve(lease: Lease): void {
    const { length } = lease.buffer

    if (length === 0 || this.pooled + length > POOL) return

    this.pooled += length
    this.pool.push(lease)
  }

  private reset(): void {
    this.head = 0
    this.tail = 0
  }

  private schedule(): void {
    if (this.scheduled || this.receiving) return

    this.scheduled = true
    process.nextTick(this.flushing)
  }

  private flush(): void {
    this.scheduled = false

    const pending = this.tail > this.head

    if (!pending && this.sealed.length === 0) return

    const stream = this.stream

    if (stream.writableEnded || stream.destroyed) {
      this.head = this.tail
      this.sealed.length = 0
      this.sealedLeases.length = 0
      this.sealedBytes = 0

      return
    }

    let ok = true

    if (this.sealed.length === 0) {
      this.lease.pending++
      ok = stream.write(this.arena.subarray(this.head, this.tail), this.lease.done)
    } else {
      stream.cork()

      for (let i = 0; i < this.sealed.length; i++)
        ok = stream.write(this.sealed[i]!, this.sealedLeases[i]!.done)

      if (pending) {
        this.lease.pending++
        ok = stream.write(this.arena.subarray(this.head, this.tail), this.lease.done)
      }

      stream.uncork()
      this.sealed.length = 0
      this.sealedLeases.length = 0
      this.sealedBytes = 0
    }

    this.head = this.tail
    this.epoch++

    if (!ok && !this.saturated) {
      this.saturated = true
      stream.once('drain', () => {
        this.saturated = false
        this.drained()
      })
    } else if (ok && this.starved.length > 0) this.drained()
  }

  /** Tells the channels that were told to stop that they may go on. */
  private drained(): void {
    const starved = this.starved

    this.starved = []

    for (const record of starved) {
      record.starved = false
      record.channel?.onBufferDrain()
    }
  }

  // endregion
}

export function isFatalError(error: { code?: number } | undefined): boolean {
  switch (error?.code) {
    case constants.CONNECTION_FORCED:
    case constants.REPLY_SUCCESS:
      return false
    default:
      return true
  }
}
