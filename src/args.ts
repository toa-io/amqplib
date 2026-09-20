// The promise and the callback interfaces take the same arguments and options, and both need the
// same method fields made of them. Each function here makes the fields of one operation.

import type { Table } from './codec.ts'

export interface AssertQueueOptions {
  exclusive?: boolean
  durable?: boolean
  autoDelete?: boolean
  arguments?: Table
  messageTtl?: number
  expires?: number
  deadLetterExchange?: string
  deadLetterRoutingKey?: string
  maxLength?: number
  maxPriority?: number
  overflow?: string
  queueMode?: string
}

export interface DeleteQueueOptions {
  ifUnused?: boolean
  ifEmpty?: boolean
}

export interface AssertExchangeOptions {
  durable?: boolean
  internal?: boolean
  autoDelete?: boolean
  alternateExchange?: string
  arguments?: Table
}

export interface DeleteExchangeOptions {
  ifUnused?: boolean
}

export interface PublishOptions {
  expiration?: string | number
  userId?: string
  CC?: string | string[]
  BCC?: string | string[]
  mandatory?: boolean
  persistent?: boolean
  deliveryMode?: boolean | number
  contentType?: string
  contentEncoding?: string
  headers?: Table
  priority?: number
  correlationId?: string
  replyTo?: string
  messageId?: string
  timestamp?: number
  type?: string
  appId?: string
}

export interface ConsumeOptions {
  consumerTag?: string
  noLocal?: boolean
  noAck?: boolean
  exclusive?: boolean
  priority?: number
  arguments?: Table
}

export interface GetOptions {
  noAck?: boolean
}

const EMPTY = Object.freeze({})
const NO_HEADERS: Table = Object.freeze({})

/**
 * The table an operation sends: the arguments given, with those that have an option of their own
 * on top. The arguments given are inherited rather than copied, and an encoder reads both.
 */
function table(given: Table | undefined, own: [string, unknown][]): Table {
  const argt: Table = Object.create(given ?? null)

  for (const [name, value] of own) if (value !== undefined) argt[name] = value

  return argt
}

export function assertQueue(queue: string | undefined, given?: AssertQueueOptions | null) {
  const options: AssertQueueOptions = given ?? EMPTY

  return {
    queue: queue || '',
    exclusive: !!options.exclusive,
    durable: options.durable === undefined ? true : options.durable,
    autoDelete: !!options.autoDelete,
    arguments: table(options.arguments, [
      ['x-expires', options.expires],
      ['x-message-ttl', options.messageTtl],
      ['x-dead-letter-exchange', options.deadLetterExchange],
      ['x-dead-letter-routing-key', options.deadLetterRoutingKey],
      ['x-max-length', options.maxLength],
      ['x-max-priority', options.maxPriority],
      ['x-overflow', options.overflow],
      ['x-queue-mode', options.queueMode],
    ]),
    passive: false,
    ticket: 0,
    nowait: false,
  }
}

export function checkQueue(queue: string) {
  return {
    queue,
    passive: true,
    nowait: false,
    durable: true,
    autoDelete: false,
    exclusive: false,
    ticket: 0,
  }
}

export function deleteQueue(queue: string, given?: DeleteQueueOptions | null) {
  const options: DeleteQueueOptions = given ?? EMPTY

  return {
    queue,
    ifUnused: !!options.ifUnused,
    ifEmpty: !!options.ifEmpty,
    ticket: 0,
    nowait: false,
  }
}

export function purgeQueue(queue: string) {
  return { queue, ticket: 0, nowait: false }
}

export function bindQueue(queue: string, source: string, pattern: string, argt?: Table) {
  return { queue, exchange: source, routingKey: pattern, arguments: argt, ticket: 0, nowait: false }
}

export const unbindQueue = bindQueue

export function assertExchange(
  exchange: string,
  type: string,
  given?: AssertExchangeOptions | null
) {
  const options: AssertExchangeOptions = given ?? EMPTY

  return {
    exchange,
    ticket: 0,
    type,
    passive: false,
    durable: options.durable === undefined ? true : options.durable,
    autoDelete: !!options.autoDelete,
    internal: !!options.internal,
    nowait: false,
    arguments: table(options.arguments, [['alternate-exchange', options.alternateExchange]]),
  }
}

export function checkExchange(exchange: string) {
  return {
    exchange,
    passive: true,
    nowait: false,
    durable: true,
    internal: false,
    type: '',
    autoDelete: false,
    ticket: 0,
  }
}

export function deleteExchange(exchange: string, given?: DeleteExchangeOptions | null) {
  const options: DeleteExchangeOptions = given ?? EMPTY

  return { exchange, ifUnused: !!options.ifUnused, ticket: 0, nowait: false }
}

export function bindExchange(dest: string, source: string, pattern: string, argt?: Table) {
  return {
    source,
    destination: dest,
    routingKey: pattern,
    arguments: argt,
    ticket: 0,
    nowait: false,
  }
}

export const unbindExchange = bindExchange

function convertCC(cc: string | string[]): string[] {
  return Array.isArray(cc) ? cc.map(String) : [String(cc)]
}

/** The fields of the publish method and the properties of the message, in one object. */
export function publish(exchange: string, routingKey: string, given?: PublishOptions | null) {
  const options: PublishOptions = given ?? EMPTY

  // A message always carries a headers table, so that whoever receives it may read a header
  // without looking whether there are any.
  let headers = options.headers ?? NO_HEADERS

  if (options.CC !== undefined || options.BCC !== undefined) {
    headers = Object.create(headers)

    if (options.CC !== undefined) headers.CC = convertCC(options.CC)

    if (options.BCC !== undefined) headers.BCC = convertCC(options.BCC)
  }

  // left undefined, the server takes it for 1, which is non-persistent
  let deliveryMode: number | undefined

  if (options.persistent !== undefined) deliveryMode = options.persistent ? 2 : 1
  else if (typeof options.deliveryMode === 'number') ({ deliveryMode } = options)
  else if (options.deliveryMode) deliveryMode = 2

  const { expiration } = options

  return {
    exchange,
    routingKey,
    mandatory: !!options.mandatory,
    immediate: false, // RabbitMQ does not implement it
    ticket: undefined,
    contentType: options.contentType,
    contentEncoding: options.contentEncoding,
    headers,
    deliveryMode,
    priority: options.priority,
    correlationId: options.correlationId,
    replyTo: options.replyTo,
    expiration: expiration === undefined ? undefined : expiration.toString(),
    messageId: options.messageId,
    timestamp: options.timestamp,
    type: options.type,
    userId: options.userId,
    appId: options.appId,
    clusterId: undefined,
  }
}

export function consume(queue: string, given?: ConsumeOptions | null) {
  const options: ConsumeOptions = given ?? EMPTY

  return {
    ticket: 0,
    queue,
    consumerTag: options.consumerTag || '',
    noLocal: !!options.noLocal,
    noAck: !!options.noAck,
    exclusive: !!options.exclusive,
    nowait: false,
    arguments: table(options.arguments, [['x-priority', options.priority]]),
  }
}

export function cancel(consumerTag: string) {
  return { consumerTag, nowait: false }
}

export function get(queue: string, given?: GetOptions | null) {
  const options: GetOptions = given ?? EMPTY

  return { ticket: 0, queue, noAck: !!options.noAck }
}

export function ack(tag: number, allUpTo?: boolean) {
  return { deliveryTag: tag, multiple: !!allUpTo }
}

export function nack(tag: number, allUpTo?: boolean, requeue?: boolean) {
  return { deliveryTag: tag, multiple: !!allUpTo, requeue: requeue === undefined ? true : requeue }
}

export function reject(tag: number, requeue?: boolean) {
  return { deliveryTag: tag, requeue: requeue === undefined ? true : requeue }
}

export function prefetch(count?: number, global?: boolean) {
  return { prefetchCount: count || 0, prefetchSize: 0, global: !!global }
}

export function recover() {
  return { requeue: true }
}
