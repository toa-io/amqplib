// The callback interface.

import { EventEmitter } from 'node:events'
import * as Args from './args.ts'
import {
  convertCloseFrameToError,
  type ConfirmCallback,
  type Consumer,
  type Message,
} from './channel.ts'
import { awaitConfirms } from './channel_model.ts'
import * as defs from './defs.ts'
import { inspect } from './format.ts'
import { ModelChannel } from './model.ts'
import type { Table } from './codec.ts'
import type { ChannelOptions, Connection } from './connection.ts'

type Callback<T = any> = (error: Error | null, value?: T) => void

export class CallbackModel extends EventEmitter {
  public readonly connection: Connection

  public constructor(connection: Connection) {
    super()

    this.connection = connection

    for (const event of ['error', 'close', 'blocked', 'unblocked', 'update-secret-ok'])
      connection.on(event, (...args: unknown[]) => this.emit(event, ...args))
  }

  public close(cb?: (error: Error | null) => void): void {
    this.connection.close(cb)
  }

  public updateSecret(newSecret: Buffer, reason: string, cb: () => void): void {
    this.connection._updateSecret(newSecret, reason, cb)
  }

  public createChannel(
    options?: ChannelOptions | Callback<Channel>,
    cb?: Callback<Channel>
  ): Channel {
    if (cb === undefined) {
      cb = options as Callback<Channel>
      options = undefined
    }

    const ch = new Channel(this.connection)

    ch.setOptions(options as ChannelOptions)

    ch.open(err => {
      if (err === null) cb?.(null, ch)
      else cb?.(err)
    })

    return ch
  }

  public createConfirmChannel(
    options?: ChannelOptions | Callback<ConfirmChannel>,
    cb?: Callback<ConfirmChannel>
  ): ConfirmChannel {
    if (cb === undefined) {
      cb = options as Callback<ConfirmChannel>
      options = undefined
    }

    const ch = new ConfirmChannel(this.connection)

    ch.setOptions(options as ChannelOptions)

    ch.open(err => {
      if (err !== null) return cb?.(err)

      ch.rpc(defs.ConfirmSelect, { nowait: false }, defs.ConfirmSelectOk, error => {
        if (error === null) cb?.(null, ch)
        else cb?.(error)
      })
    })

    return ch
  }
}

/** Leaves the value out when there is an error, and stands in for a callback not given. */
function wrap<T>(cb?: Callback<T>): Callback<T> {
  if (cb === undefined || cb === null) return () => undefined

  return (error, value) => {
    if (error === null) cb(null, value)
    else cb(error)
  }
}

export class Channel extends ModelChannel {
  public rpc(method: number, fields: object, expect: number, cb0?: Callback): this {
    const cb = wrap(cb0)

    this._rpc(method, fields, expect, (error, frame) => cb(error, frame?.fields))

    return this
  }

  public open(cb: Callback): this | void {
    try {
      this.allocate()
    } catch (error) {
      return cb(error as Error)
    }

    return this.rpc(defs.ChannelOpen, { outOfBand: '' }, defs.ChannelOpenOk, cb)
  }

  public close(cb?: (error: Error | null) => void): void {
    return this.closeBecause('Goodbye', defs.constants.REPLY_SUCCESS, () => cb?.(null))
  }

  public assertQueue(
    queue?: string,
    options?: Args.AssertQueueOptions | null,
    cb?: Callback
  ): this {
    return this.rpc(defs.QueueDeclare, Args.assertQueue(queue, options), defs.QueueDeclareOk, cb)
  }

  public checkQueue(queue: string, cb?: Callback): this {
    return this.rpc(defs.QueueDeclare, Args.checkQueue(queue), defs.QueueDeclareOk, cb)
  }

  public deleteQueue(queue: string, options?: Args.DeleteQueueOptions | null, cb?: Callback): this {
    return this.rpc(defs.QueueDelete, Args.deleteQueue(queue, options), defs.QueueDeleteOk, cb)
  }

  public purgeQueue(queue: string, cb?: Callback): this {
    return this.rpc(defs.QueuePurge, Args.purgeQueue(queue), defs.QueuePurgeOk, cb)
  }

  public bindQueue(
    queue: string,
    source: string,
    pattern: string,
    argt?: Table,
    cb?: Callback
  ): this {
    return this.rpc(
      defs.QueueBind,
      Args.bindQueue(queue, source, pattern, argt),
      defs.QueueBindOk,
      cb
    )
  }

  public unbindQueue(
    queue: string,
    source: string,
    pattern: string,
    argt?: Table,
    cb?: Callback
  ): this {
    return this.rpc(
      defs.QueueUnbind,
      Args.unbindQueue(queue, source, pattern, argt),
      defs.QueueUnbindOk,
      cb
    )
  }

  public assertExchange(
    exchange: string,
    type: string,
    options?: Args.AssertExchangeOptions | null,
    cb0?: Callback<{ exchange: string }>
  ): this {
    const cb = wrap(cb0)

    this._rpc(
      defs.ExchangeDeclare,
      Args.assertExchange(exchange, type, options),
      defs.ExchangeDeclareOk,
      error => cb(error, { exchange })
    )

    return this
  }

  public checkExchange(exchange: string, cb?: Callback): this {
    return this.rpc(defs.ExchangeDeclare, Args.checkExchange(exchange), defs.ExchangeDeclareOk, cb)
  }

  public deleteExchange(
    exchange: string,
    options?: Args.DeleteExchangeOptions | null,
    cb?: Callback
  ): this {
    return this.rpc(
      defs.ExchangeDelete,
      Args.deleteExchange(exchange, options),
      defs.ExchangeDeleteOk,
      cb
    )
  }

  public bindExchange(
    dest: string,
    source: string,
    pattern: string,
    argt?: Table,
    cb?: Callback
  ): this {
    return this.rpc(
      defs.ExchangeBind,
      Args.bindExchange(dest, source, pattern, argt),
      defs.ExchangeBindOk,
      cb
    )
  }

  public unbindExchange(
    dest: string,
    source: string,
    pattern: string,
    argt?: Table,
    cb?: Callback
  ): this {
    return this.rpc(
      defs.ExchangeUnbind,
      Args.unbindExchange(dest, source, pattern, argt),
      defs.ExchangeUnbindOk,
      cb
    )
  }

  public consume(
    queue: string,
    callback: Consumer,
    options?: Args.ConsumeOptions | null,
    cb0?: Callback<defs.BasicConsumeOkFields>
  ): this {
    const cb = wrap(cb0)

    this._rpc(defs.BasicConsume, Args.consume(queue, options), defs.BasicConsumeOk, (err, ok) => {
      if (err !== null) return cb(err)

      const fields = ok!.fields as unknown as defs.BasicConsumeOkFields

      this.registerConsumer(fields.consumerTag, callback)
      cb(null, fields)
    })

    return this
  }

  public cancel(consumerTag: string, cb0?: Callback): this {
    const cb = wrap(cb0)

    this._rpc(defs.BasicCancel, Args.cancel(consumerTag), defs.BasicCancelOk, (err, ok) => {
      if (err !== null) return cb(err)

      this.unregisterConsumer(consumerTag)
      cb(null, ok!.fields)
    })

    return this
  }

  public get(
    queue: string,
    options?: Args.GetOptions | null,
    cb0?: Callback<Message | false>
  ): this {
    const cb = wrap(cb0)

    this.sendOrEnqueue(defs.BasicGet, Args.get(queue, options), (error, frame) => {
      if (error instanceof Error) return cb(error)

      if (error !== null) return cb(convertCloseFrameToError(defs.BasicGet, error))

      if (frame!.id === defs.BasicGetEmpty) cb(null, false)
      else if (frame!.id === defs.BasicGetOk) this.take(frame!.fields!, m => cb(null, m))
      else cb(new Error(`Unexpected response to BasicGet: ${inspect(frame!)}`))
    })

    return this
  }

  public ack(message: Message, allUpTo?: boolean): this {
    this.sendImmediately(defs.BasicAck, Args.ack(message.fields.deliveryTag, allUpTo))

    return this
  }

  public ackAll(): this {
    this.sendImmediately(defs.BasicAck, Args.ack(0, true))

    return this
  }

  public nack(message: Message, allUpTo?: boolean, requeue?: boolean): this {
    this.sendImmediately(defs.BasicNack, Args.nack(message.fields.deliveryTag, allUpTo, requeue))

    return this
  }

  public nackAll(requeue?: boolean): this {
    this.sendImmediately(defs.BasicNack, Args.nack(0, true, requeue))

    return this
  }

  public reject(message: Message, requeue?: boolean): this {
    this.sendImmediately(defs.BasicReject, Args.reject(message.fields.deliveryTag, requeue))

    return this
  }

  public prefetch(count?: number, global?: boolean, cb?: Callback): this {
    return this.rpc(defs.BasicQos, Args.prefetch(count, global), defs.BasicQosOk, cb)
  }

  public recover(cb?: Callback): this {
    return this.rpc(defs.BasicRecover, Args.recover(), defs.BasicRecoverOk, cb)
  }
}

export class ConfirmChannel extends Channel {
  public override publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: Args.PublishOptions | null,
    cb?: ConfirmCallback
  ): boolean {
    const result = super.publish(exchange, routingKey, content, options)

    this.pushConfirmCallback(cb)

    return result
  }

  public override sendToQueue(
    queue: string,
    content: Buffer,
    options?: Args.PublishOptions | null,
    cb?: ConfirmCallback
  ): boolean {
    return this.publish('', queue, content, options, cb)
  }

  public waitForConfirms(k: (error?: Error) => void): Promise<void> {
    return Promise.all(awaitConfirms(this.unconfirmed)).then(
      () => k(),
      error => k(error)
    )
  }
}
