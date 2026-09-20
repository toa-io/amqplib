// A channel: the conversation with the server that messages and their acknowledgements travel in.
//
// The machinery shared by the promise and the callback interfaces is here: replies matched to
// requests, publisher confirms, and incoming messages put together from their frames.

import { EventEmitter } from 'node:events'
import { format } from 'node:util'
import { readUInt64, type Table } from './codec.ts'
import * as defs from './defs.ts'
import { safeEmit } from './emit.ts'
import { IllegalOperationError, stackCapture } from './error.ts'
import { closeMessage, inspect, methodName } from './format.ts'
import type { ChannelOptions, ChannelSink, Connection } from './connection.ts'
import type { Frame } from './frame.ts'

const { constants } = defs

export interface Message {
  fields: any
  properties: defs.BasicPropertiesFields
  content: Buffer
}

export interface CloseFrame extends Frame {
  fields: { replyCode: number; replyText: string; classId: number; methodId: number }
}

/**
 * Called with the frame that answered a request; or with an error if none will come; or, if the
 * server closed the channel instead of answering, with its close frame in place of the error.
 */
export type Reply = (error: Error | CloseFrame | null, frame?: Frame) => void

export type ConfirmCallback = (error: Error | null) => void

interface Pending {
  method: number
  fields: object
  reply: Reply
}

type ChannelError = Error & { code?: number; classId?: number; methodId?: number }

// what a channel is waiting for
const METHOD = 0
const HEADER = 1
const BODY = 2

const EMPTY = Buffer.alloc(0)

const nacked = (): Error => new Error('message nacked')

export class Channel extends EventEmitter implements ChannelSink {
  public readonly connection: Connection
  public ch = 0
  public options: ChannelOptions | undefined

  /** The least delivery tag that is not confirmed yet. */
  public lwm = 1

  /**
   * The callbacks of the messages from `lwm` on, in the order they were published. `false` stands
   * for a message published without one, `null` for a message confirmed ahead of its turn.
   */
  public unconfirmed: (ConfirmCallback | false | null)[] = []

  protected reply: Reply | null = null
  protected pending: Pending[] | null = []

  private state: 'open' | 'closing' | 'closed' = 'open'
  private closedBecause = ''
  private closedAt: string | undefined
  private closeCallback: (() => void) | undefined

  // the message being put together
  private awaiting = METHOD
  private event = ''
  private fields: object | null = null
  private message: Message | null = null
  private filled = 0
  private taker: ((message: Message) => void) | null = null

  public constructor(connection: Connection) {
    super()

    this.connection = connection
  }

  public setOptions(options?: ChannelOptions): void {
    this.options = options
  }

  public allocate(): this {
    this.ch = this.connection.freshChannel(this, this.options)

    return this
  }

  // region sending

  public sendImmediately(method: number, fields: object): boolean {
    if (this.state !== 'open') throw new IllegalOperationError(this.closedBecause, this.closedAt)

    return this.connection.sendMethod(this.ch, method, fields)
  }

  /** Requests are answered in the order they were made, so one waits for the one before it. */
  public sendOrEnqueue(method: number, fields: object, reply: Reply): void {
    if (this.state !== 'open') throw new IllegalOperationError(this.closedBecause, this.closedAt)

    if (this.reply === null) {
      this.reply = reply
      this.connection.sendMethod(this.ch, method, fields)
    } else this.pending!.push({ method, fields, reply })
  }

  public sendMessage(fields: object, properties: object, content: Buffer): boolean {
    if (this.state !== 'open') throw new IllegalOperationError(this.closedBecause, this.closedAt)

    return this.connection.sendMessage(
      this.ch,
      defs.BasicPublish,
      fields,
      defs.BasicProperties,
      properties,
      content
    )
  }

  public _rpc(
    method: number,
    fields: object,
    expect: number,
    cb: (error: Error | null, frame?: Frame) => void
  ): void {
    this.sendOrEnqueue(method, fields, (error, frame) => {
      if (error === null) {
        if (frame!.id === expect) return cb(null, frame)

        // something other than what was asked for: the channel is of no use any more
        const expected = methodName(expect)
        const e = new Error(format('Expected %s; got %s', expected, inspect(frame!, false)))

        this.closeWithError(
          frame!.id,
          format('Expected %s; got %s', expected, methodName(frame!.id!)),
          constants.UNEXPECTED_FRAME,
          e
        )

        return cb(e)
      }

      if (error instanceof Error) return cb(error)

      cb(convertCloseFrameToError(method, error))
    })
  }

  // endregion

  // region closing

  public toClosed(capturedStack?: string): void {
    this.rejectPending()
    this.state = 'closed'
    this.closedBecause = 'Channel closed'
    this.closedAt = capturedStack
    this.connection.releaseChannel(this.ch)
    safeEmit(this, 'close')
  }

  /** A close has been started: nothing but the end of the closing handshake is of interest. */
  private toClosing(capturedStack: string, k?: () => void): void {
    this.state = 'closing'
    this.closedBecause = 'Channel closing'
    this.closedAt = capturedStack
    this.closeCallback = k
  }

  private rejectPending(): void {
    const failed = (): Error => new Error('Channel ended, no reply will be forthcoming')

    if (this.reply !== null) this.reply(failed())

    this.reply = null

    if (this.pending !== null) for (const discarded of this.pending) discarded.reply(failed())

    this.pending = null

    const unconfirmed = this.unconfirmed

    if (unconfirmed.length === 0) return

    // A message never confirmed counts as lost. Those published without a callback stay, for
    // `waitForConfirms` to fail on.
    const closed = new Error('channel closed')

    this.unconfirmed = unconfirmed.filter(cb => cb === false)

    for (const cb of unconfirmed) if (cb) cb(closed)
  }

  public closeBecause(reason: string, code: number, k?: () => void): void {
    this.sendImmediately(defs.ChannelClose, {
      replyText: reason,
      replyCode: code,
      methodId: 0,
      classId: 0,
    })

    this.toClosing(stackCapture(`closeBecause called: ${reason}`), k)
  }

  /** Closes the channel, and tells of the error once it is closed. */
  public closeWithError(
    id: number | undefined,
    reason: string,
    code: number,
    error: ChannelError
  ): void {
    this.closeBecause(reason, code, () => {
      error.code = code

      if (id) {
        const about = defs.info(id)

        error.classId = about.classId
        error.methodId = about.methodId
      }

      safeEmit(this, 'error', error)
    })
  }

  // endregion

  // region confirms

  public pushConfirmCallback(cb?: ConfirmCallback | null): void {
    this.unconfirmed.push(cb || false)
  }

  private confirm(fields: defs.BasicAckFields, isNack: boolean): void {
    const tag = fields.deliveryTag
    const unconfirmed = this.unconfirmed

    if (fields.multiple) {
      const confirmed = unconfirmed.splice(0, tag - this.lwm + 1)

      this.lwm = tag + 1

      for (const cb of confirmed) if (cb) cb(isNack ? nacked() : null)

      return
    }

    let cb

    if (tag === this.lwm) {
      cb = unconfirmed.shift()
      this.lwm++

      // what was confirmed ahead of its turn
      while (unconfirmed[0] === null) {
        unconfirmed.shift()
        this.lwm++
      }
    } else {
      cb = unconfirmed[tag - this.lwm]
      unconfirmed[tag - this.lwm] = null
    }

    if (cb) cb(isNack ? nacked() : null)
  }

  // endregion

  public onBufferDrain(): void {
    safeEmit(this, 'drain')
  }

  // region receiving

  public onMethod(id: number, buffer: Buffer, offset: number): void {
    if (this.state !== 'open') return this.whileClosing(id)

    switch (id) {
      case defs.BasicDeliver:
        return this.expectMessage(id, 'delivery', defs.decodeBasicDeliver(buffer, offset))

      case defs.BasicReturn:
        return this.expectMessage(id, 'return', defs.decodeBasicReturn(buffer, offset))

      case defs.BasicAck: {
        const fields = defs.decodeBasicAck(buffer, offset)

        this.confirm(fields, false)

        return safeEmit(this, 'ack', fields)
      }

      case defs.BasicNack: {
        const fields = defs.decodeBasicNack(buffer, offset)

        this.confirm(fields, true)

        return safeEmit(this, 'nack', fields)
      }

      case defs.BasicCancel:
        return safeEmit(this, 'cancel', defs.decodeBasicCancel(buffer, offset))

      case defs.ChannelClose: {
        const frame: CloseFrame = {
          id,
          channel: this.ch,
          fields: defs.decodeChannelClose(buffer, offset),
        }

        // whoever waits for a reply gets the close frame in its place
        if (this.reply !== null) {
          const { reply } = this

          this.reply = null
          reply(frame)
        }

        const message = `Channel closed by server: ${closeMessage(frame.fields)}`
        const error: ChannelError = new Error(message)

        this.connection.sendMethod(this.ch, defs.ChannelCloseOk, {})

        error.code = frame.fields.replyCode
        error.classId = frame.fields.classId
        error.methodId = frame.fields.methodId
        safeEmit(this, 'error', error)

        return this.toClosed(stackCapture(message))
      }

      case defs.ChannelFlow:
        return this.closeWithError(
          id,
          'Flow not implemented',
          constants.NOT_IMPLEMENTED,
          new Error('Flow not implemented')
        )

      default: {
        // a reply: the next request goes out before whoever asked is told, as they may ask again
        const { reply } = this

        if (reply === null)
          return this.violation(id, format('Unexpected %s: nothing awaits a reply', methodName(id)))

        this.reply = null

        const next = this.pending!.shift()

        if (next !== undefined) {
          this.reply = next.reply
          this.connection.sendMethod(this.ch, next.method, next.fields)
        }

        return reply(null, {
          id,
          channel: this.ch,
          fields: defs.decode(id, buffer, offset) as Table,
        })
      }
    }
  }

  private whileClosing(id: number): void {
    if (this.state !== 'closing') return

    if (id === defs.ChannelCloseOk) {
      this.closeCallback?.()
      this.toClosed(stackCapture('ChannelCloseOk frame received'))
    } else if (id === defs.ChannelClose)
      this.connection.sendMethod(this.ch, defs.ChannelCloseOk, {})
  }

  public onHeader(buffer: Buffer, offset: number): void {
    if (this.state !== 'open') return

    if (this.awaiting !== HEADER)
      return this.violation(
        defs.BasicProperties,
        this.awaiting === METHOD
          ? format(
              'Expected BasicDeliver or BasicReturn; got %s',
              inspect({ id: defs.BasicProperties, channel: this.ch })
            )
          : 'Expected content frame after headers'
      )

    const size = readUInt64(buffer, offset + 4)

    const message: Message = {
      fields: this.fields,
      properties: defs.decodeBasicProperties(buffer, offset + 12),
      content: size === 0 ? EMPTY : Buffer.allocUnsafe(size),
    }

    this.fields = null

    if (size === 0) return this.complete(defs.BasicProperties, message)

    this.message = message
    this.filled = 0
    this.awaiting = BODY
  }

  public onBody(buffer: Buffer, start: number, end: number): void {
    if (this.state !== 'open') return

    if (this.awaiting !== BODY)
      return this.violation(
        undefined,
        this.awaiting === METHOD
          ? format(
              'Expected BasicDeliver or BasicReturn; got %s',
              inspect({ channel: this.ch, size: end - start })
            )
          : 'Expected headers frame after delivery'
      )

    const message = this.message!
    const { content } = message
    const filled = this.filled + end - start

    if (filled > content.length)
      return this.violation(
        undefined,
        format('Too much content sent! Expected %d bytes', content.length)
      )

    buffer.copy(content, this.filled, start, end)
    this.filled = filled

    if (filled === content.length) {
      this.message = null
      this.complete(undefined, message)
    }
  }

  /** A method that a message follows has arrived. */
  private expectMessage(id: number, event: string, fields: object): void {
    if (this.awaiting !== METHOD)
      return this.violation(
        id,
        this.awaiting === HEADER
          ? 'Expected headers frame after delivery'
          : 'Expected content frame after headers'
      )

    this.event = event
    this.fields = fields
    this.awaiting = HEADER
  }

  /**
   * Makes the next message go to the one who asked for it rather than to the consumers: the
   * message that follows a `get-ok`.
   */
  protected take(fields: object, taker: (message: Message) => void): void {
    this.event = ''
    this.fields = fields
    this.taker = taker
    this.awaiting = HEADER
  }

  private complete(id: number | undefined, message: Message): void {
    this.awaiting = METHOD

    try {
      if (this.taker === null) this.dispatch(this.event, message)
      else {
        const { taker } = this

        this.taker = null
        taker(message)
      }
    } catch (error) {
      if (error instanceof Error)
        this.closeWithError(id, 'Error while processing message', constants.INTERNAL_ERROR, error)
      else
        this.closeWithError(
          id,
          'Internal error while processing message',
          constants.INTERNAL_ERROR,
          new Error(String(error))
        )
    }
  }

  /** Hands a message over to whoever it is for. */
  protected dispatch(event: string, message: Message): void {
    safeEmit(this, event, message)
  }

  /** The server broke the order frames come in. */
  private violation(id: number | undefined, message: string): void {
    this.awaiting = METHOD
    this.message = null
    this.fields = null
    this.closeWithError(id, message, constants.UNEXPECTED_FRAME, new Error(message))
  }

  // endregion
}

export function convertCloseFrameToError(method: number, close: CloseFrame): Error {
  const { fields } = close
  const reason = (fields.classId << 16) + fields.methodId

  const error: ChannelError = new Error(
    method === reason
      ? format('Operation failed: %s; %s', methodName(method), closeMessage(fields))
      : format('Channel closed by server: %s', closeMessage(fields))
  )

  error.code = fields.replyCode
  error.classId = fields.classId
  error.methodId = fields.methodId

  return error
}

export type Consumer = (message: Message | null) => void

/** A channel that knows its consumers. */
export class BaseChannel extends Channel {
  protected readonly consumers = new Map<string, Consumer>()

  public registerConsumer(tag: string, callback: Consumer): void {
    this.consumers.set(tag, callback)
  }

  public unregisterConsumer(tag: string): void {
    this.consumers.delete(tag)
  }

  public dispatchMessage(fields: { consumerTag: string }, message: Message | null): void {
    const consumer = this.consumers.get(fields.consumerTag)

    if (consumer === undefined) throw new Error(`Unknown consumer: ${fields.consumerTag}`)

    return consumer(message)
  }

  public handleDelivery(message: Message): void {
    return this.dispatchMessage(message.fields, message)
  }

  /** The server cancelled a consumer: it is told so with a `null` in place of a message. */
  public handleCancel(fields: { consumerTag: string }): void {
    const result = this.dispatchMessage(fields, null)

    this.unregisterConsumer(fields.consumerTag)

    return result
  }
}
