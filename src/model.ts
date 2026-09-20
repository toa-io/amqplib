// What the promise and the callback channels have in common: everything that expects no reply.

import * as Args from './args.ts'
import { BaseChannel, type Message } from './channel.ts'
import type { Connection } from './connection.ts'

export abstract class ModelChannel extends BaseChannel {
  public constructor(connection: Connection) {
    super(connection)

    this.on('cancel', fields => this.handleCancel(fields))
  }

  public publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: Args.PublishOptions | null
  ): boolean {
    const fieldsAndProps = Args.publish(exchange, routingKey, options)

    return this.sendMessage(fieldsAndProps, fieldsAndProps, content)
  }

  public sendToQueue(
    queue: string,
    content: Buffer,
    options?: Args.PublishOptions | null
  ): boolean {
    return this.publish('', queue, content, options)
  }

  /** A delivery goes straight to its consumer; whatever else there is goes out as an event. */
  protected override dispatch(event: string, message: Message): void {
    if (event !== 'delivery') return super.dispatch(event, message)

    try {
      this.handleDelivery(message)
    } catch (error) {
      if (this.listenerCount('handler-error') === 0) throw error

      setImmediate(() => this.emit('handler-error', error, event))
    }
  }
}
