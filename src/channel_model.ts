// The promise interface.

import { EventEmitter } from 'node:events'
import * as Args from './args.ts'
import {
  convertCloseFrameToError,
  type Confirm,
  type ConfirmCallback,
  type Consumer,
  type Message,
} from './channel.ts'
import * as defs from './defs.ts'
import { inspect } from './format.ts'
import { ModelChannel } from './model.ts'
import type { Table } from './codec.ts'
import type { ChannelOptions, Connection } from './connection.ts'
import type { GetMessage, Replies } from './properties.ts'

export class ChannelModel extends EventEmitter {
  public readonly connection: Connection

  public constructor(connection: Connection) {
    super()

    this.connection = connection

    for (const event of ['error', 'close', 'blocked', 'unblocked', 'update-secret-ok'])
      connection.on(event, (...args: unknown[]) => this.emit(event, ...args))
  }

  public close(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.connection.close(error => (error === null ? resolve() : reject(error)))
    })
  }

  public updateSecret(newSecret: Buffer, reason: string): Promise<void> {
    return new Promise(resolve => {
      this.connection._updateSecret(newSecret, reason, resolve)
    })
  }

  public async createChannel(options?: ChannelOptions): Promise<Channel> {
    const channel = new Channel(this.connection)

    channel.setOptions(options)
    await channel.open()

    return channel
  }

  public async createConfirmChannel(options?: ChannelOptions): Promise<ConfirmChannel> {
    const channel = new ConfirmChannel(this.connection)

    channel.setOptions(options)
    await channel.open()
    await channel.rpc(defs.ConfirmSelect, { nowait: false }, defs.ConfirmSelectOk)

    return channel
  }
}

export class Channel extends ModelChannel {
  public rpc(method: number, fields: object, expect: number): Promise<any> {
    return new Promise((resolve, reject) => {
      this._rpc(method, fields, expect, (error, frame) => {
        if (error === null) resolve(frame!.fields)
        else reject(error)
      })
    })
  }

  public async open(): Promise<unknown> {
    this.allocate()

    return this.rpc(defs.ChannelOpen, { outOfBand: '' }, defs.ChannelOpenOk)
  }

  public close(): Promise<void> {
    return new Promise(resolve => {
      this.closeBecause('Goodbye', defs.constants.REPLY_SUCCESS, resolve)
    })
  }

  public assertQueue(
    queue?: string,
    options?: Args.AssertQueueOptions
  ): Promise<Replies.AssertQueue> {
    return this.rpc(defs.QueueDeclare, Args.assertQueue(queue, options), defs.QueueDeclareOk)
  }

  public checkQueue(queue: string): Promise<Replies.AssertQueue> {
    return this.rpc(defs.QueueDeclare, Args.checkQueue(queue), defs.QueueDeclareOk)
  }

  public deleteQueue(
    queue: string,
    options?: Args.DeleteQueueOptions
  ): Promise<Replies.DeleteQueue> {
    return this.rpc(defs.QueueDelete, Args.deleteQueue(queue, options), defs.QueueDeleteOk)
  }

  public purgeQueue(queue: string): Promise<Replies.PurgeQueue> {
    return this.rpc(defs.QueuePurge, Args.purgeQueue(queue), defs.QueuePurgeOk)
  }

  public bindQueue(
    queue: string,
    source: string,
    pattern: string,
    argt?: Table
  ): Promise<Replies.Empty> {
    return this.rpc(defs.QueueBind, Args.bindQueue(queue, source, pattern, argt), defs.QueueBindOk)
  }

  public unbindQueue(
    queue: string,
    source: string,
    pattern: string,
    argt?: Table
  ): Promise<Replies.Empty> {
    return this.rpc(
      defs.QueueUnbind,
      Args.unbindQueue(queue, source, pattern, argt),
      defs.QueueUnbindOk
    )
  }

  public async assertExchange(
    exchange: string,
    type: string,
    options?: Args.AssertExchangeOptions
  ): Promise<Replies.AssertExchange> {
    await this.rpc(
      defs.ExchangeDeclare,
      Args.assertExchange(exchange, type, options),
      defs.ExchangeDeclareOk
    )

    return { exchange }
  }

  public checkExchange(exchange: string): Promise<Replies.Empty> {
    return this.rpc(defs.ExchangeDeclare, Args.checkExchange(exchange), defs.ExchangeDeclareOk)
  }

  public deleteExchange(
    name: string,
    options?: Args.DeleteExchangeOptions
  ): Promise<Replies.Empty> {
    return this.rpc(defs.ExchangeDelete, Args.deleteExchange(name, options), defs.ExchangeDeleteOk)
  }

  public bindExchange(
    dest: string,
    source: string,
    pattern: string,
    argt?: Table
  ): Promise<Replies.Empty> {
    return this.rpc(
      defs.ExchangeBind,
      Args.bindExchange(dest, source, pattern, argt),
      defs.ExchangeBindOk
    )
  }

  public unbindExchange(
    dest: string,
    source: string,
    pattern: string,
    argt?: Table
  ): Promise<Replies.Empty> {
    return this.rpc(
      defs.ExchangeUnbind,
      Args.unbindExchange(dest, source, pattern, argt),
      defs.ExchangeUnbindOk
    )
  }

  public consume(
    queue: string,
    callback: Consumer,
    options?: Args.ConsumeOptions
  ): Promise<Replies.Consume> {
    return new Promise((resolve, reject) => {
      this._rpc(defs.BasicConsume, Args.consume(queue, options), defs.BasicConsumeOk, (err, ok) => {
        if (err !== null) return reject(err)

        const fields = ok!.fields as unknown as defs.BasicConsumeOkFields

        this.registerConsumer(fields.consumerTag, callback)
        resolve(fields)
      })
    })
  }

  public async cancel(consumerTag: string): Promise<Replies.Empty> {
    const fields = await this.rpc(defs.BasicCancel, Args.cancel(consumerTag), defs.BasicCancelOk)

    this.unregisterConsumer(consumerTag)

    return fields
  }

  public get(queue: string, options?: Args.GetOptions): Promise<GetMessage | false> {
    return new Promise((resolve, reject) => {
      this.sendOrEnqueue(defs.BasicGet, Args.get(queue, options), (error, frame) => {
        if (error instanceof Error) return reject(error)

        if (error !== null) return reject(convertCloseFrameToError(defs.BasicGet, error))

        if (frame!.id === defs.BasicGetEmpty) resolve(false)
        else if (frame!.id === defs.BasicGetOk)
          this.take(frame!.fields!, resolve as (message: Message) => void)
        else reject(new Error(`Unexpected response to BasicGet: ${inspect(frame!)}`))
      })
    })
  }

  public ack(message: Message, allUpTo?: boolean): void {
    this.sendImmediately(defs.BasicAck, Args.ack(message.fields.deliveryTag, allUpTo))
  }

  public ackAll(): void {
    this.sendImmediately(defs.BasicAck, Args.ack(0, true))
  }

  public nack(message: Message, allUpTo?: boolean, requeue?: boolean): void {
    this.sendImmediately(defs.BasicNack, Args.nack(message.fields.deliveryTag, allUpTo, requeue))
  }

  public nackAll(requeue?: boolean): void {
    this.sendImmediately(defs.BasicNack, Args.nack(0, true, requeue))
  }

  public reject(message: Message, requeue?: boolean): void {
    this.sendImmediately(defs.BasicReject, Args.reject(message.fields.deliveryTag, requeue))
  }

  public recover(): Promise<Replies.Empty> {
    return this.rpc(defs.BasicRecover, Args.recover(), defs.BasicRecoverOk)
  }

  public qos(count?: number, global?: boolean): Promise<Replies.Empty> {
    return this.rpc(defs.BasicQos, Args.prefetch(count, global), defs.BasicQosOk)
  }

  public prefetch(count?: number, global?: boolean): Promise<Replies.Empty> {
    return this.qos(count, global)
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

  public async waitForConfirms(): Promise<void> {
    const { unconfirmed } = this
    const awaiting = awaitConfirms(unconfirmed)

    // the channel is closed, so no confirm will come
    if (this.pending === null) {
      const closed = new Error('channel closed')

      for (let cb = unconfirmed.shift(); cb !== undefined; cb = unconfirmed.shift())
        if (cb) cb(closed)
    }

    await Promise.all(awaiting)
  }
}

/** Puts a promise in front of every callback that is still waiting for its confirm. */
export function awaitConfirms(unconfirmed: (Confirm | false | null)[]): Promise<void>[] {
  const awaiting: Promise<void>[] = []

  unconfirmed.forEach((cb, index) => {
    // confirmed ahead of its turn
    if (cb === null) return

    awaiting.push(
      new Promise((resolve, reject) => {
        unconfirmed[index] = error => {
          if (cb) cb(error)

          if (error === null) resolve()
          else reject(error)
        }
      })
    )
  })

  return awaiting
}
