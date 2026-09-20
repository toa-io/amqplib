// The types of the interface: options, replies and messages, under the names the original
// library declares them by, so that code written against its types compiles against these.
//
// oxlint-disable typescript/no-namespace, typescript/no-explicit-any, typescript/no-empty-interface

import type * as tls from 'node:tls'

export namespace Replies {
  export interface Empty {}
  export interface AssertQueue {
    queue: string
    messageCount: number
    consumerCount: number
  }
  export interface PurgeQueue {
    messageCount: number
  }
  export interface DeleteQueue {
    messageCount: number
  }
  export interface AssertExchange {
    exchange: string
  }
  export interface Consume {
    consumerTag: string
  }
}

export namespace Options {
  export interface Connect {
    protocol?: string
    hostname?: string
    port?: number
    username?: string
    password?: string
    locale?: string
    frameMax?: number
    heartbeat?: number
    vhost?: string
    channelMax?: number
    credentials?: {
      mechanism: string
      response(): Buffer
      username?: string
      password?: string
    }
  }

  export interface AssertQueue {
    exclusive?: boolean
    durable?: boolean
    autoDelete?: boolean
    arguments?: any
    messageTtl?: number
    expires?: number
    deadLetterExchange?: string
    deadLetterRoutingKey?: string
    maxLength?: number
    maxPriority?: number
    overflow?: string
    queueMode?: string
  }

  export interface DeleteQueue {
    ifUnused?: boolean
    ifEmpty?: boolean
  }

  export interface AssertExchange {
    durable?: boolean
    internal?: boolean
    autoDelete?: boolean
    alternateExchange?: string
    arguments?: any
  }

  export interface DeleteExchange {
    ifUnused?: boolean
  }

  export interface Publish {
    expiration?: string | number
    userId?: string
    CC?: string | string[]
    mandatory?: boolean
    persistent?: boolean
    deliveryMode?: boolean | number
    BCC?: string | string[]
    contentType?: string
    contentEncoding?: string
    headers?: any
    priority?: number
    correlationId?: string
    replyTo?: string
    messageId?: string
    timestamp?: number
    type?: string
    appId?: string
  }

  export interface Consume {
    consumerTag?: string
    noLocal?: boolean
    noAck?: boolean
    exclusive?: boolean
    priority?: number
    arguments?: any
  }

  export interface Get {
    noAck?: boolean
  }
}

export interface SocketOptions extends tls.ConnectionOptions {
  noDelay?: boolean
  timeout?: number
  keepAlive?: boolean
  keepAliveDelay?: number
  clientProperties?: Record<string, unknown>
  credentials?: {
    mechanism: string
    response(): Buffer
    username?: string
    password?: string
  }
}

export interface ChannelOptions {
  highWaterMark?: number
}

export interface Message {
  content: Buffer
  fields: MessageFields
  properties: MessageProperties
}

export interface GetMessage extends Message {
  fields: GetMessageFields
}

export interface ConsumeMessage extends Message {
  fields: ConsumeMessageFields
}

export interface CommonMessageFields {
  deliveryTag: number
  redelivered: boolean
  exchange: string
  routingKey: string
}

export interface MessageFields extends CommonMessageFields {
  messageCount?: number
  consumerTag?: string
}

export interface GetMessageFields extends CommonMessageFields {
  messageCount: number
}

export interface ConsumeMessageFields extends CommonMessageFields {
  consumerTag: string
}

export interface MessageProperties {
  contentType: any | undefined
  contentEncoding: any | undefined
  headers: MessagePropertyHeaders | undefined
  deliveryMode: any | undefined
  priority: any | undefined
  correlationId: any | undefined
  replyTo: any | undefined
  expiration: any | undefined
  messageId: any | undefined
  timestamp: any | undefined
  type: any | undefined
  userId: any | undefined
  appId: any | undefined
  clusterId: any | undefined
}

export interface MessagePropertyHeaders {
  'x-first-death-exchange'?: string
  'x-first-death-queue'?: string
  'x-first-death-reason'?: string
  'x-death'?: XDeath[]
  [key: string]: any
}

export interface XDeath {
  count: number
  reason: 'rejected' | 'expired' | 'maxlen'
  queue: string
  time: {
    '!': 'timestamp'
    value: number
  }
  exchange: string
  'original-expiration'?: any
  'routing-keys': string[]
}

export interface ServerProperties {
  host: string
  product: string
  version: string
  platform: string
  copyright?: string
  information: string
  [key: string]: string | undefined
}
