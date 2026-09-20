// The two libraries under one interface: a channel on a connection over the broker given.

import { createRequire } from 'node:module'
import { join } from 'node:path'
import { ChannelModel } from '../src/channel_model.ts'
import { Connection } from '../src/connection.ts'
import { OPEN_OPTIONS } from '../src/harness.ts'
import type { Broker } from './broker.ts'

export interface Channel {
  publish(exchange: string, key: string, content: Buffer, options?: object): boolean
  consume(queue: string, consumer: (message: any) => void, options?: object): Promise<unknown>
  ack(message: any): void
  once(event: 'drain', listener: () => void): unknown
}

export interface Subject {
  channel: Channel

  /** Hands the client a stretch of the wire the way its socket would. */
  feed(wire: Buffer, from: number, to: number): void

  /** How much a socket hands over at a time. */
  reads: number
}

export interface Library {
  name: string
  open(broker: Broker): Promise<Subject>
}

const require = createRequire(import.meta.url)
const original = join(import.meta.dirname, '../node_modules/amqplib')

const OPTIONS = { ...OPEN_OPTIONS, frameMax: 131072 }

export const libraries: Library[] = [
  {
    name: 'amqplib',

    async open(broker) {
      const Original = require(join(original, 'lib/connection.js')).Connection
      const Model = require(join(original, 'lib/channel_model.js')).ChannelModel
      const connection = new Original(broker)

      await new Promise<void>((resolve, reject) => {
        connection.open(OPTIONS, (error: Error | null) => (error ? reject(error) : resolve()))
      })

      return {
        channel: await new Model(connection).createChannel(),
        reads: 65536,

        // a socket allocates what it reads into, read by read
        feed: (wire, from, to) => {
          broker.push(Buffer.from(wire.subarray(from, to)))
        },
      }
    },
  },
  {
    name: '@toa.io/amqplib',

    async open(broker) {
      const connection = new Connection(broker, true)
      const read = Buffer.allocUnsafeSlow(131072)

      // the socket reads into the one buffer, as `connect` sets it up to
      broker.deliver = bytes => connection.receive(bytes, 0, bytes.length)

      await new Promise<void>((resolve, reject) => {
        connection.open(OPTIONS, error => (error ? reject(error) : resolve()))
      })

      return {
        channel: await new ChannelModel(connection).createChannel(),
        reads: read.length,
        feed: (wire, from, to) => connection.receive(read, 0, wire.copy(read, 0, from, to)),
      }
    },
  },
]
