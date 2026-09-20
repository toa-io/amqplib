// This file is written by tools/generate.ts from the AMQP 0-9-1 specification: `npm run generate`.
/* oxlint-disable */
import {
  OVERFLOW,
  bytes,
  decodeFields,
  encodeTable,
  readUInt64,
  writeUInt64,
  type Table,
} from './codec.ts'
import { readShortString, writeShortString, writeSize } from './strings.ts'
export interface Info {
  id: number
  classId?: number
  methodId?: number
  name: string
  args: { type: string; name: string; default?: unknown }[]
}
export const constants = {
  FRAME_METHOD: 1,
  FRAME_HEADER: 2,
  FRAME_BODY: 3,
  FRAME_HEARTBEAT: 8,
  FRAME_MIN_SIZE: 4096,
  FRAME_END: 206,
  REPLY_SUCCESS: 200,
  CONTENT_TOO_LARGE: 311,
  NO_ROUTE: 312,
  NO_CONSUMERS: 313,
  ACCESS_REFUSED: 403,
  NOT_FOUND: 404,
  RESOURCE_LOCKED: 405,
  PRECONDITION_FAILED: 406,
  CONNECTION_FORCED: 320,
  INVALID_PATH: 402,
  FRAME_ERROR: 501,
  SYNTAX_ERROR: 502,
  COMMAND_INVALID: 503,
  CHANNEL_ERROR: 504,
  UNEXPECTED_FRAME: 505,
  RESOURCE_ERROR: 506,
  NOT_ALLOWED: 530,
  NOT_IMPLEMENTED: 540,
  INTERNAL_ERROR: 541,
} as const
export const constant_strs: Record<number, string> = {
  '1': 'FRAME-METHOD',
  '2': 'FRAME-HEADER',
  '3': 'FRAME-BODY',
  '8': 'FRAME-HEARTBEAT',
  '200': 'REPLY-SUCCESS',
  '206': 'FRAME-END',
  '311': 'CONTENT-TOO-LARGE',
  '312': 'NO-ROUTE',
  '313': 'NO-CONSUMERS',
  '320': 'CONNECTION-FORCED',
  '402': 'INVALID-PATH',
  '403': 'ACCESS-REFUSED',
  '404': 'NOT-FOUND',
  '405': 'RESOURCE-LOCKED',
  '406': 'PRECONDITION-FAILED',
  '501': 'FRAME-ERROR',
  '502': 'SYNTAX-ERROR',
  '503': 'COMMAND-INVALID',
  '504': 'CHANNEL-ERROR',
  '505': 'UNEXPECTED-FRAME',
  '506': 'RESOURCE-ERROR',
  '530': 'NOT-ALLOWED',
  '540': 'NOT-IMPLEMENTED',
  '541': 'INTERNAL-ERROR',
  '4096': 'FRAME-MIN-SIZE',
}
/** The bytes of a frame that are not its payload: type, channel, size and the end octet. */
export const FRAME_OVERHEAD = 8
/**
 * What an encoder may write before it checks the buffer for room: the most any frame takes
 * without its long strings and tables. Whoever calls an encoder leaves this much free.
 */
export const HEADROOM = 2606
export function decode(id: number, buffer: Buffer, offset = 0): any {
  switch (id) {
    case 3932170:
      return decodeBasicQos(buffer, offset)
    case 3932171:
      return decodeBasicQosOk(buffer, offset)
    case 3932180:
      return decodeBasicConsume(buffer, offset)
    case 3932181:
      return decodeBasicConsumeOk(buffer, offset)
    case 3932190:
      return decodeBasicCancel(buffer, offset)
    case 3932191:
      return decodeBasicCancelOk(buffer, offset)
    case 3932200:
      return decodeBasicPublish(buffer, offset)
    case 3932210:
      return decodeBasicReturn(buffer, offset)
    case 3932220:
      return decodeBasicDeliver(buffer, offset)
    case 3932230:
      return decodeBasicGet(buffer, offset)
    case 3932231:
      return decodeBasicGetOk(buffer, offset)
    case 3932232:
      return decodeBasicGetEmpty(buffer, offset)
    case 3932240:
      return decodeBasicAck(buffer, offset)
    case 3932250:
      return decodeBasicReject(buffer, offset)
    case 3932260:
      return decodeBasicRecoverAsync(buffer, offset)
    case 3932270:
      return decodeBasicRecover(buffer, offset)
    case 3932271:
      return decodeBasicRecoverOk(buffer, offset)
    case 3932280:
      return decodeBasicNack(buffer, offset)
    case 655370:
      return decodeConnectionStart(buffer, offset)
    case 655371:
      return decodeConnectionStartOk(buffer, offset)
    case 655380:
      return decodeConnectionSecure(buffer, offset)
    case 655381:
      return decodeConnectionSecureOk(buffer, offset)
    case 655390:
      return decodeConnectionTune(buffer, offset)
    case 655391:
      return decodeConnectionTuneOk(buffer, offset)
    case 655400:
      return decodeConnectionOpen(buffer, offset)
    case 655401:
      return decodeConnectionOpenOk(buffer, offset)
    case 655410:
      return decodeConnectionClose(buffer, offset)
    case 655411:
      return decodeConnectionCloseOk(buffer, offset)
    case 655420:
      return decodeConnectionBlocked(buffer, offset)
    case 655421:
      return decodeConnectionUnblocked(buffer, offset)
    case 655430:
      return decodeConnectionUpdateSecret(buffer, offset)
    case 655431:
      return decodeConnectionUpdateSecretOk(buffer, offset)
    case 1310730:
      return decodeChannelOpen(buffer, offset)
    case 1310731:
      return decodeChannelOpenOk(buffer, offset)
    case 1310740:
      return decodeChannelFlow(buffer, offset)
    case 1310741:
      return decodeChannelFlowOk(buffer, offset)
    case 1310760:
      return decodeChannelClose(buffer, offset)
    case 1310761:
      return decodeChannelCloseOk(buffer, offset)
    case 1966090:
      return decodeAccessRequest(buffer, offset)
    case 1966091:
      return decodeAccessRequestOk(buffer, offset)
    case 2621450:
      return decodeExchangeDeclare(buffer, offset)
    case 2621451:
      return decodeExchangeDeclareOk(buffer, offset)
    case 2621460:
      return decodeExchangeDelete(buffer, offset)
    case 2621461:
      return decodeExchangeDeleteOk(buffer, offset)
    case 2621470:
      return decodeExchangeBind(buffer, offset)
    case 2621471:
      return decodeExchangeBindOk(buffer, offset)
    case 2621480:
      return decodeExchangeUnbind(buffer, offset)
    case 2621491:
      return decodeExchangeUnbindOk(buffer, offset)
    case 3276810:
      return decodeQueueDeclare(buffer, offset)
    case 3276811:
      return decodeQueueDeclareOk(buffer, offset)
    case 3276820:
      return decodeQueueBind(buffer, offset)
    case 3276821:
      return decodeQueueBindOk(buffer, offset)
    case 3276830:
      return decodeQueuePurge(buffer, offset)
    case 3276831:
      return decodeQueuePurgeOk(buffer, offset)
    case 3276840:
      return decodeQueueDelete(buffer, offset)
    case 3276841:
      return decodeQueueDeleteOk(buffer, offset)
    case 3276850:
      return decodeQueueUnbind(buffer, offset)
    case 3276851:
      return decodeQueueUnbindOk(buffer, offset)
    case 5898250:
      return decodeTxSelect(buffer, offset)
    case 5898251:
      return decodeTxSelectOk(buffer, offset)
    case 5898260:
      return decodeTxCommit(buffer, offset)
    case 5898261:
      return decodeTxCommitOk(buffer, offset)
    case 5898270:
      return decodeTxRollback(buffer, offset)
    case 5898271:
      return decodeTxRollbackOk(buffer, offset)
    case 5570570:
      return decodeConfirmSelect(buffer, offset)
    case 5570571:
      return decodeConfirmSelectOk(buffer, offset)
    case 60:
      return decodeBasicProperties(buffer, offset)
    default:
      throw new Error('Unknown class/method ID')
  }
}
export function encodeMethod(
  id: number,
  buffer: Buffer,
  offset: number,
  channel: number,
  fields: any
): number {
  switch (id) {
    case 3932170:
      return encodeBasicQos(buffer, offset, channel, fields)
    case 3932171:
      return encodeBasicQosOk(buffer, offset, channel, fields)
    case 3932180:
      return encodeBasicConsume(buffer, offset, channel, fields)
    case 3932181:
      return encodeBasicConsumeOk(buffer, offset, channel, fields)
    case 3932190:
      return encodeBasicCancel(buffer, offset, channel, fields)
    case 3932191:
      return encodeBasicCancelOk(buffer, offset, channel, fields)
    case 3932200:
      return encodeBasicPublish(buffer, offset, channel, fields)
    case 3932210:
      return encodeBasicReturn(buffer, offset, channel, fields)
    case 3932220:
      return encodeBasicDeliver(buffer, offset, channel, fields)
    case 3932230:
      return encodeBasicGet(buffer, offset, channel, fields)
    case 3932231:
      return encodeBasicGetOk(buffer, offset, channel, fields)
    case 3932232:
      return encodeBasicGetEmpty(buffer, offset, channel, fields)
    case 3932240:
      return encodeBasicAck(buffer, offset, channel, fields)
    case 3932250:
      return encodeBasicReject(buffer, offset, channel, fields)
    case 3932260:
      return encodeBasicRecoverAsync(buffer, offset, channel, fields)
    case 3932270:
      return encodeBasicRecover(buffer, offset, channel, fields)
    case 3932271:
      return encodeBasicRecoverOk(buffer, offset, channel, fields)
    case 3932280:
      return encodeBasicNack(buffer, offset, channel, fields)
    case 655370:
      return encodeConnectionStart(buffer, offset, channel, fields)
    case 655371:
      return encodeConnectionStartOk(buffer, offset, channel, fields)
    case 655380:
      return encodeConnectionSecure(buffer, offset, channel, fields)
    case 655381:
      return encodeConnectionSecureOk(buffer, offset, channel, fields)
    case 655390:
      return encodeConnectionTune(buffer, offset, channel, fields)
    case 655391:
      return encodeConnectionTuneOk(buffer, offset, channel, fields)
    case 655400:
      return encodeConnectionOpen(buffer, offset, channel, fields)
    case 655401:
      return encodeConnectionOpenOk(buffer, offset, channel, fields)
    case 655410:
      return encodeConnectionClose(buffer, offset, channel, fields)
    case 655411:
      return encodeConnectionCloseOk(buffer, offset, channel, fields)
    case 655420:
      return encodeConnectionBlocked(buffer, offset, channel, fields)
    case 655421:
      return encodeConnectionUnblocked(buffer, offset, channel, fields)
    case 655430:
      return encodeConnectionUpdateSecret(buffer, offset, channel, fields)
    case 655431:
      return encodeConnectionUpdateSecretOk(buffer, offset, channel, fields)
    case 1310730:
      return encodeChannelOpen(buffer, offset, channel, fields)
    case 1310731:
      return encodeChannelOpenOk(buffer, offset, channel, fields)
    case 1310740:
      return encodeChannelFlow(buffer, offset, channel, fields)
    case 1310741:
      return encodeChannelFlowOk(buffer, offset, channel, fields)
    case 1310760:
      return encodeChannelClose(buffer, offset, channel, fields)
    case 1310761:
      return encodeChannelCloseOk(buffer, offset, channel, fields)
    case 1966090:
      return encodeAccessRequest(buffer, offset, channel, fields)
    case 1966091:
      return encodeAccessRequestOk(buffer, offset, channel, fields)
    case 2621450:
      return encodeExchangeDeclare(buffer, offset, channel, fields)
    case 2621451:
      return encodeExchangeDeclareOk(buffer, offset, channel, fields)
    case 2621460:
      return encodeExchangeDelete(buffer, offset, channel, fields)
    case 2621461:
      return encodeExchangeDeleteOk(buffer, offset, channel, fields)
    case 2621470:
      return encodeExchangeBind(buffer, offset, channel, fields)
    case 2621471:
      return encodeExchangeBindOk(buffer, offset, channel, fields)
    case 2621480:
      return encodeExchangeUnbind(buffer, offset, channel, fields)
    case 2621491:
      return encodeExchangeUnbindOk(buffer, offset, channel, fields)
    case 3276810:
      return encodeQueueDeclare(buffer, offset, channel, fields)
    case 3276811:
      return encodeQueueDeclareOk(buffer, offset, channel, fields)
    case 3276820:
      return encodeQueueBind(buffer, offset, channel, fields)
    case 3276821:
      return encodeQueueBindOk(buffer, offset, channel, fields)
    case 3276830:
      return encodeQueuePurge(buffer, offset, channel, fields)
    case 3276831:
      return encodeQueuePurgeOk(buffer, offset, channel, fields)
    case 3276840:
      return encodeQueueDelete(buffer, offset, channel, fields)
    case 3276841:
      return encodeQueueDeleteOk(buffer, offset, channel, fields)
    case 3276850:
      return encodeQueueUnbind(buffer, offset, channel, fields)
    case 3276851:
      return encodeQueueUnbindOk(buffer, offset, channel, fields)
    case 5898250:
      return encodeTxSelect(buffer, offset, channel, fields)
    case 5898251:
      return encodeTxSelectOk(buffer, offset, channel, fields)
    case 5898260:
      return encodeTxCommit(buffer, offset, channel, fields)
    case 5898261:
      return encodeTxCommitOk(buffer, offset, channel, fields)
    case 5898270:
      return encodeTxRollback(buffer, offset, channel, fields)
    case 5898271:
      return encodeTxRollbackOk(buffer, offset, channel, fields)
    case 5570570:
      return encodeConfirmSelect(buffer, offset, channel, fields)
    case 5570571:
      return encodeConfirmSelectOk(buffer, offset, channel, fields)
    default:
      throw new Error('Unknown class/method ID')
  }
}
export function encodeProperties(
  id: number,
  buffer: Buffer,
  offset: number,
  channel: number,
  size: number,
  fields: any
): number {
  switch (id) {
    case 60:
      return encodeBasicProperties(buffer, offset, channel, size, fields)
    default:
      throw new Error('Unknown class/properties ID')
  }
}
export function info(id: number): Info {
  switch (id) {
    case 3932170:
      return methodInfoBasicQos
    case 3932171:
      return methodInfoBasicQosOk
    case 3932180:
      return methodInfoBasicConsume
    case 3932181:
      return methodInfoBasicConsumeOk
    case 3932190:
      return methodInfoBasicCancel
    case 3932191:
      return methodInfoBasicCancelOk
    case 3932200:
      return methodInfoBasicPublish
    case 3932210:
      return methodInfoBasicReturn
    case 3932220:
      return methodInfoBasicDeliver
    case 3932230:
      return methodInfoBasicGet
    case 3932231:
      return methodInfoBasicGetOk
    case 3932232:
      return methodInfoBasicGetEmpty
    case 3932240:
      return methodInfoBasicAck
    case 3932250:
      return methodInfoBasicReject
    case 3932260:
      return methodInfoBasicRecoverAsync
    case 3932270:
      return methodInfoBasicRecover
    case 3932271:
      return methodInfoBasicRecoverOk
    case 3932280:
      return methodInfoBasicNack
    case 655370:
      return methodInfoConnectionStart
    case 655371:
      return methodInfoConnectionStartOk
    case 655380:
      return methodInfoConnectionSecure
    case 655381:
      return methodInfoConnectionSecureOk
    case 655390:
      return methodInfoConnectionTune
    case 655391:
      return methodInfoConnectionTuneOk
    case 655400:
      return methodInfoConnectionOpen
    case 655401:
      return methodInfoConnectionOpenOk
    case 655410:
      return methodInfoConnectionClose
    case 655411:
      return methodInfoConnectionCloseOk
    case 655420:
      return methodInfoConnectionBlocked
    case 655421:
      return methodInfoConnectionUnblocked
    case 655430:
      return methodInfoConnectionUpdateSecret
    case 655431:
      return methodInfoConnectionUpdateSecretOk
    case 1310730:
      return methodInfoChannelOpen
    case 1310731:
      return methodInfoChannelOpenOk
    case 1310740:
      return methodInfoChannelFlow
    case 1310741:
      return methodInfoChannelFlowOk
    case 1310760:
      return methodInfoChannelClose
    case 1310761:
      return methodInfoChannelCloseOk
    case 1966090:
      return methodInfoAccessRequest
    case 1966091:
      return methodInfoAccessRequestOk
    case 2621450:
      return methodInfoExchangeDeclare
    case 2621451:
      return methodInfoExchangeDeclareOk
    case 2621460:
      return methodInfoExchangeDelete
    case 2621461:
      return methodInfoExchangeDeleteOk
    case 2621470:
      return methodInfoExchangeBind
    case 2621471:
      return methodInfoExchangeBindOk
    case 2621480:
      return methodInfoExchangeUnbind
    case 2621491:
      return methodInfoExchangeUnbindOk
    case 3276810:
      return methodInfoQueueDeclare
    case 3276811:
      return methodInfoQueueDeclareOk
    case 3276820:
      return methodInfoQueueBind
    case 3276821:
      return methodInfoQueueBindOk
    case 3276830:
      return methodInfoQueuePurge
    case 3276831:
      return methodInfoQueuePurgeOk
    case 3276840:
      return methodInfoQueueDelete
    case 3276841:
      return methodInfoQueueDeleteOk
    case 3276850:
      return methodInfoQueueUnbind
    case 3276851:
      return methodInfoQueueUnbindOk
    case 5898250:
      return methodInfoTxSelect
    case 5898251:
      return methodInfoTxSelectOk
    case 5898260:
      return methodInfoTxCommit
    case 5898261:
      return methodInfoTxCommitOk
    case 5898270:
      return methodInfoTxRollback
    case 5898271:
      return methodInfoTxRollbackOk
    case 5570570:
      return methodInfoConfirmSelect
    case 5570571:
      return methodInfoConfirmSelectOk
    case 60:
      return propertiesInfoBasicProperties
    default:
      throw new Error('Unknown class/method ID')
  }
}
export const BasicQos = 3932170
export interface BasicQosFields {
  prefetchSize: number
  prefetchCount: number
  global: boolean
}
export function decodeBasicQos(buffer: Buffer, offset: number): BasicQosFields {
  const fields: BasicQosFields = {
    prefetchSize: undefined as any,
    prefetchCount: undefined as any,
    global: undefined as any,
  }
  fields.prefetchSize = buffer.readUInt32BE(offset)
  offset += 4
  fields.prefetchCount = buffer.readUInt16BE(offset)
  offset += 2
  fields.global = (buffer[offset]! & 1) !== 0
  return fields
}
export function encodeBasicQos(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 10
  let offset = start + 11
  val = fields.prefetchSize
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'prefetchSize' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt32BE(val, offset)
  offset += 4
  val = fields.prefetchCount
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'prefetchCount' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.global
  if (val === undefined) val = false
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicQos: Info = {
  id: 3932170,
  classId: 60,
  methodId: 10,
  name: 'BasicQos',
  args: [
    { type: 'long', name: 'prefetchSize', default: 0 },
    { type: 'short', name: 'prefetchCount', default: 0 },
    { type: 'bit', name: 'global', default: false },
  ],
}
export const BasicQosOk = 3932171
export interface BasicQosOkFields {}
export function decodeBasicQosOk(_buffer: Buffer, _offset: number): BasicQosOkFields {
  return {}
}
export function encodeBasicQosOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 11
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicQosOk: Info = {
  id: 3932171,
  classId: 60,
  methodId: 11,
  name: 'BasicQosOk',
  args: [],
}
export const BasicConsume = 3932180
export interface BasicConsumeFields {
  ticket: number
  queue: string
  consumerTag: string
  noLocal: boolean
  noAck: boolean
  exclusive: boolean
  nowait: boolean
  arguments: Table
}
export function decodeBasicConsume(buffer: Buffer, offset: number): BasicConsumeFields {
  let end = 0
  const fields: BasicConsumeFields = {
    ticket: undefined as any,
    queue: undefined as any,
    consumerTag: undefined as any,
    noLocal: undefined as any,
    noAck: undefined as any,
    exclusive: undefined as any,
    nowait: undefined as any,
    arguments: undefined as any,
  }
  fields.ticket = buffer.readUInt16BE(offset)
  offset += 2
  fields.queue = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.consumerTag = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.noLocal = (buffer[offset]! & 1) !== 0
  fields.noAck = (buffer[offset]! & 2) !== 0
  fields.exclusive = (buffer[offset]! & 4) !== 0
  fields.nowait = (buffer[offset]! & 8) !== 0
  offset++
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.arguments = decodeFields(buffer, offset + 4, end)
  offset = end
  return fields
}
export function encodeBasicConsume(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 20
  let offset = start + 11
  val = fields.ticket
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'ticket' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.queue
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  val = fields.consumerTag
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'consumerTag' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'consumerTag' is the wrong type; must be a string (up to 255 chars)")
  val = fields.noLocal
  if (val === undefined) val = false
  if (val) bits += 1
  val = fields.noAck
  if (val === undefined) val = false
  if (val) bits += 2
  val = fields.exclusive
  if (val === undefined) val = false
  if (val) bits += 4
  val = fields.nowait
  if (val === undefined) val = false
  if (val) bits += 8
  buffer[offset] = bits
  offset++
  bits = 0
  val = fields.arguments
  if (val === undefined) val = {}
  else if (typeof val !== 'object')
    throw new TypeError("Field 'arguments' is the wrong type; must be an object")
  offset += encodeTable(buffer, val, offset)
  if (offset + 520 > buffer.length) throw OVERFLOW
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicConsume: Info = {
  id: 3932180,
  classId: 60,
  methodId: 20,
  name: 'BasicConsume',
  args: [
    { type: 'short', name: 'ticket', default: 0 },
    { type: 'shortstr', name: 'queue', default: '' },
    { type: 'shortstr', name: 'consumerTag', default: '' },
    { type: 'bit', name: 'noLocal', default: false },
    { type: 'bit', name: 'noAck', default: false },
    { type: 'bit', name: 'exclusive', default: false },
    { type: 'bit', name: 'nowait', default: false },
    { type: 'table', name: 'arguments', default: {} },
  ],
}
export const BasicConsumeOk = 3932181
export interface BasicConsumeOkFields {
  consumerTag: string
}
export function decodeBasicConsumeOk(buffer: Buffer, offset: number): BasicConsumeOkFields {
  const fields: BasicConsumeOkFields = {
    consumerTag: undefined as any,
  }
  fields.consumerTag = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  return fields
}
export function encodeBasicConsumeOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 21
  let offset = start + 11
  val = fields.consumerTag
  if (val === undefined) throw new Error("Missing value for mandatory field 'consumerTag'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'consumerTag' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'consumerTag' is the wrong type; must be a string (up to 255 chars)")
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicConsumeOk: Info = {
  id: 3932181,
  classId: 60,
  methodId: 21,
  name: 'BasicConsumeOk',
  args: [{ type: 'shortstr', name: 'consumerTag' }],
}
export const BasicCancel = 3932190
export interface BasicCancelFields {
  consumerTag: string
  nowait: boolean
}
export function decodeBasicCancel(buffer: Buffer, offset: number): BasicCancelFields {
  const fields: BasicCancelFields = {
    consumerTag: undefined as any,
    nowait: undefined as any,
  }
  fields.consumerTag = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.nowait = (buffer[offset]! & 1) !== 0
  return fields
}
export function encodeBasicCancel(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 30
  let offset = start + 11
  val = fields.consumerTag
  if (val === undefined) throw new Error("Missing value for mandatory field 'consumerTag'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'consumerTag' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'consumerTag' is the wrong type; must be a string (up to 255 chars)")
  val = fields.nowait
  if (val === undefined) val = false
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicCancel: Info = {
  id: 3932190,
  classId: 60,
  methodId: 30,
  name: 'BasicCancel',
  args: [
    { type: 'shortstr', name: 'consumerTag' },
    { type: 'bit', name: 'nowait', default: false },
  ],
}
export const BasicCancelOk = 3932191
export interface BasicCancelOkFields {
  consumerTag: string
}
export function decodeBasicCancelOk(buffer: Buffer, offset: number): BasicCancelOkFields {
  const fields: BasicCancelOkFields = {
    consumerTag: undefined as any,
  }
  fields.consumerTag = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  return fields
}
export function encodeBasicCancelOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 31
  let offset = start + 11
  val = fields.consumerTag
  if (val === undefined) throw new Error("Missing value for mandatory field 'consumerTag'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'consumerTag' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'consumerTag' is the wrong type; must be a string (up to 255 chars)")
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicCancelOk: Info = {
  id: 3932191,
  classId: 60,
  methodId: 31,
  name: 'BasicCancelOk',
  args: [{ type: 'shortstr', name: 'consumerTag' }],
}
export const BasicPublish = 3932200
export interface BasicPublishFields {
  ticket: number
  exchange: string
  routingKey: string
  mandatory: boolean
  immediate: boolean
}
export function decodeBasicPublish(buffer: Buffer, offset: number): BasicPublishFields {
  const fields: BasicPublishFields = {
    ticket: undefined as any,
    exchange: undefined as any,
    routingKey: undefined as any,
    mandatory: undefined as any,
    immediate: undefined as any,
  }
  fields.ticket = buffer.readUInt16BE(offset)
  offset += 2
  fields.exchange = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.routingKey = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.mandatory = (buffer[offset]! & 1) !== 0
  fields.immediate = (buffer[offset]! & 2) !== 0
  return fields
}
export function encodeBasicPublish(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 40
  let offset = start + 11
  val = fields.ticket
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'ticket' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.exchange
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  val = fields.routingKey
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  val = fields.mandatory
  if (val === undefined) val = false
  if (val) bits += 1
  val = fields.immediate
  if (val === undefined) val = false
  if (val) bits += 2
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicPublish: Info = {
  id: 3932200,
  classId: 60,
  methodId: 40,
  name: 'BasicPublish',
  args: [
    { type: 'short', name: 'ticket', default: 0 },
    { type: 'shortstr', name: 'exchange', default: '' },
    { type: 'shortstr', name: 'routingKey', default: '' },
    { type: 'bit', name: 'mandatory', default: false },
    { type: 'bit', name: 'immediate', default: false },
  ],
}
export const BasicReturn = 3932210
export interface BasicReturnFields {
  replyCode: number
  replyText: string
  exchange: string
  routingKey: string
}
export function decodeBasicReturn(buffer: Buffer, offset: number): BasicReturnFields {
  const fields: BasicReturnFields = {
    replyCode: undefined as any,
    replyText: undefined as any,
    exchange: undefined as any,
    routingKey: undefined as any,
  }
  fields.replyCode = buffer.readUInt16BE(offset)
  offset += 2
  fields.replyText = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.exchange = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.routingKey = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  return fields
}
export function encodeBasicReturn(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 50
  let offset = start + 11
  val = fields.replyCode
  if (val === undefined) throw new Error("Missing value for mandatory field 'replyCode'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'replyCode' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.replyText
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'replyText' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'replyText' is the wrong type; must be a string (up to 255 chars)")
  val = fields.exchange
  if (val === undefined) throw new Error("Missing value for mandatory field 'exchange'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  val = fields.routingKey
  if (val === undefined) throw new Error("Missing value for mandatory field 'routingKey'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicReturn: Info = {
  id: 3932210,
  classId: 60,
  methodId: 50,
  name: 'BasicReturn',
  args: [
    { type: 'short', name: 'replyCode' },
    { type: 'shortstr', name: 'replyText', default: '' },
    { type: 'shortstr', name: 'exchange' },
    { type: 'shortstr', name: 'routingKey' },
  ],
}
export const BasicDeliver = 3932220
export interface BasicDeliverFields {
  consumerTag: string
  deliveryTag: number
  redelivered: boolean
  exchange: string
  routingKey: string
}
export function decodeBasicDeliver(buffer: Buffer, offset: number): BasicDeliverFields {
  const fields: BasicDeliverFields = {
    consumerTag: undefined as any,
    deliveryTag: undefined as any,
    redelivered: undefined as any,
    exchange: undefined as any,
    routingKey: undefined as any,
  }
  fields.consumerTag = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.deliveryTag = readUInt64(buffer, offset)
  offset += 8
  fields.redelivered = (buffer[offset]! & 1) !== 0
  offset++
  fields.exchange = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.routingKey = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  return fields
}
export function encodeBasicDeliver(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 60
  let offset = start + 11
  val = fields.consumerTag
  if (val === undefined) throw new Error("Missing value for mandatory field 'consumerTag'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'consumerTag' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'consumerTag' is the wrong type; must be a string (up to 255 chars)")
  val = fields.deliveryTag
  if (val === undefined) throw new Error("Missing value for mandatory field 'deliveryTag'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'deliveryTag' is the wrong type; must be a number (but not NaN)")
  writeUInt64(buffer, val, offset)
  offset += 8
  val = fields.redelivered
  if (val === undefined) val = false
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  val = fields.exchange
  if (val === undefined) throw new Error("Missing value for mandatory field 'exchange'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  val = fields.routingKey
  if (val === undefined) throw new Error("Missing value for mandatory field 'routingKey'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicDeliver: Info = {
  id: 3932220,
  classId: 60,
  methodId: 60,
  name: 'BasicDeliver',
  args: [
    { type: 'shortstr', name: 'consumerTag' },
    { type: 'longlong', name: 'deliveryTag' },
    { type: 'bit', name: 'redelivered', default: false },
    { type: 'shortstr', name: 'exchange' },
    { type: 'shortstr', name: 'routingKey' },
  ],
}
export const BasicGet = 3932230
export interface BasicGetFields {
  ticket: number
  queue: string
  noAck: boolean
}
export function decodeBasicGet(buffer: Buffer, offset: number): BasicGetFields {
  const fields: BasicGetFields = {
    ticket: undefined as any,
    queue: undefined as any,
    noAck: undefined as any,
  }
  fields.ticket = buffer.readUInt16BE(offset)
  offset += 2
  fields.queue = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.noAck = (buffer[offset]! & 1) !== 0
  return fields
}
export function encodeBasicGet(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 70
  let offset = start + 11
  val = fields.ticket
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'ticket' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.queue
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  val = fields.noAck
  if (val === undefined) val = false
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicGet: Info = {
  id: 3932230,
  classId: 60,
  methodId: 70,
  name: 'BasicGet',
  args: [
    { type: 'short', name: 'ticket', default: 0 },
    { type: 'shortstr', name: 'queue', default: '' },
    { type: 'bit', name: 'noAck', default: false },
  ],
}
export const BasicGetOk = 3932231
export interface BasicGetOkFields {
  deliveryTag: number
  redelivered: boolean
  exchange: string
  routingKey: string
  messageCount: number
}
export function decodeBasicGetOk(buffer: Buffer, offset: number): BasicGetOkFields {
  const fields: BasicGetOkFields = {
    deliveryTag: undefined as any,
    redelivered: undefined as any,
    exchange: undefined as any,
    routingKey: undefined as any,
    messageCount: undefined as any,
  }
  fields.deliveryTag = readUInt64(buffer, offset)
  offset += 8
  fields.redelivered = (buffer[offset]! & 1) !== 0
  offset++
  fields.exchange = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.routingKey = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.messageCount = buffer.readUInt32BE(offset)
  offset += 4
  return fields
}
export function encodeBasicGetOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 71
  let offset = start + 11
  val = fields.deliveryTag
  if (val === undefined) throw new Error("Missing value for mandatory field 'deliveryTag'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'deliveryTag' is the wrong type; must be a number (but not NaN)")
  writeUInt64(buffer, val, offset)
  offset += 8
  val = fields.redelivered
  if (val === undefined) val = false
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  val = fields.exchange
  if (val === undefined) throw new Error("Missing value for mandatory field 'exchange'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  val = fields.routingKey
  if (val === undefined) throw new Error("Missing value for mandatory field 'routingKey'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  val = fields.messageCount
  if (val === undefined) throw new Error("Missing value for mandatory field 'messageCount'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'messageCount' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt32BE(val, offset)
  offset += 4
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicGetOk: Info = {
  id: 3932231,
  classId: 60,
  methodId: 71,
  name: 'BasicGetOk',
  args: [
    { type: 'longlong', name: 'deliveryTag' },
    { type: 'bit', name: 'redelivered', default: false },
    { type: 'shortstr', name: 'exchange' },
    { type: 'shortstr', name: 'routingKey' },
    { type: 'long', name: 'messageCount' },
  ],
}
export const BasicGetEmpty = 3932232
export interface BasicGetEmptyFields {
  clusterId: string
}
export function decodeBasicGetEmpty(buffer: Buffer, offset: number): BasicGetEmptyFields {
  const fields: BasicGetEmptyFields = {
    clusterId: undefined as any,
  }
  fields.clusterId = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  return fields
}
export function encodeBasicGetEmpty(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 72
  let offset = start + 11
  val = fields.clusterId
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'clusterId' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'clusterId' is the wrong type; must be a string (up to 255 chars)")
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicGetEmpty: Info = {
  id: 3932232,
  classId: 60,
  methodId: 72,
  name: 'BasicGetEmpty',
  args: [{ type: 'shortstr', name: 'clusterId', default: '' }],
}
export const BasicAck = 3932240
export interface BasicAckFields {
  deliveryTag: number
  multiple: boolean
}
export function decodeBasicAck(buffer: Buffer, offset: number): BasicAckFields {
  const fields: BasicAckFields = {
    deliveryTag: undefined as any,
    multiple: undefined as any,
  }
  fields.deliveryTag = readUInt64(buffer, offset)
  offset += 8
  fields.multiple = (buffer[offset]! & 1) !== 0
  return fields
}
export function encodeBasicAck(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 80
  let offset = start + 11
  val = fields.deliveryTag
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'deliveryTag' is the wrong type; must be a number (but not NaN)")
  writeUInt64(buffer, val, offset)
  offset += 8
  val = fields.multiple
  if (val === undefined) val = false
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicAck: Info = {
  id: 3932240,
  classId: 60,
  methodId: 80,
  name: 'BasicAck',
  args: [
    { type: 'longlong', name: 'deliveryTag', default: 0 },
    { type: 'bit', name: 'multiple', default: false },
  ],
}
export const BasicReject = 3932250
export interface BasicRejectFields {
  deliveryTag: number
  requeue: boolean
}
export function decodeBasicReject(buffer: Buffer, offset: number): BasicRejectFields {
  const fields: BasicRejectFields = {
    deliveryTag: undefined as any,
    requeue: undefined as any,
  }
  fields.deliveryTag = readUInt64(buffer, offset)
  offset += 8
  fields.requeue = (buffer[offset]! & 1) !== 0
  return fields
}
export function encodeBasicReject(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 90
  let offset = start + 11
  val = fields.deliveryTag
  if (val === undefined) throw new Error("Missing value for mandatory field 'deliveryTag'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'deliveryTag' is the wrong type; must be a number (but not NaN)")
  writeUInt64(buffer, val, offset)
  offset += 8
  val = fields.requeue
  if (val === undefined) val = true
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicReject: Info = {
  id: 3932250,
  classId: 60,
  methodId: 90,
  name: 'BasicReject',
  args: [
    { type: 'longlong', name: 'deliveryTag' },
    { type: 'bit', name: 'requeue', default: true },
  ],
}
export const BasicRecoverAsync = 3932260
export interface BasicRecoverAsyncFields {
  requeue: boolean
}
export function decodeBasicRecoverAsync(buffer: Buffer, offset: number): BasicRecoverAsyncFields {
  const fields: BasicRecoverAsyncFields = {
    requeue: undefined as any,
  }
  fields.requeue = (buffer[offset]! & 1) !== 0
  return fields
}
export function encodeBasicRecoverAsync(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 100
  let offset = start + 11
  val = fields.requeue
  if (val === undefined) val = false
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicRecoverAsync: Info = {
  id: 3932260,
  classId: 60,
  methodId: 100,
  name: 'BasicRecoverAsync',
  args: [{ type: 'bit', name: 'requeue', default: false }],
}
export const BasicRecover = 3932270
export interface BasicRecoverFields {
  requeue: boolean
}
export function decodeBasicRecover(buffer: Buffer, offset: number): BasicRecoverFields {
  const fields: BasicRecoverFields = {
    requeue: undefined as any,
  }
  fields.requeue = (buffer[offset]! & 1) !== 0
  return fields
}
export function encodeBasicRecover(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 110
  let offset = start + 11
  val = fields.requeue
  if (val === undefined) val = false
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicRecover: Info = {
  id: 3932270,
  classId: 60,
  methodId: 110,
  name: 'BasicRecover',
  args: [{ type: 'bit', name: 'requeue', default: false }],
}
export const BasicRecoverOk = 3932271
export interface BasicRecoverOkFields {}
export function decodeBasicRecoverOk(_buffer: Buffer, _offset: number): BasicRecoverOkFields {
  return {}
}
export function encodeBasicRecoverOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 111
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicRecoverOk: Info = {
  id: 3932271,
  classId: 60,
  methodId: 111,
  name: 'BasicRecoverOk',
  args: [],
}
export const BasicNack = 3932280
export interface BasicNackFields {
  deliveryTag: number
  multiple: boolean
  requeue: boolean
}
export function decodeBasicNack(buffer: Buffer, offset: number): BasicNackFields {
  const fields: BasicNackFields = {
    deliveryTag: undefined as any,
    multiple: undefined as any,
    requeue: undefined as any,
  }
  fields.deliveryTag = readUInt64(buffer, offset)
  offset += 8
  fields.multiple = (buffer[offset]! & 1) !== 0
  fields.requeue = (buffer[offset]! & 2) !== 0
  return fields
}
export function encodeBasicNack(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 120
  let offset = start + 11
  val = fields.deliveryTag
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'deliveryTag' is the wrong type; must be a number (but not NaN)")
  writeUInt64(buffer, val, offset)
  offset += 8
  val = fields.multiple
  if (val === undefined) val = false
  if (val) bits += 1
  val = fields.requeue
  if (val === undefined) val = true
  if (val) bits += 2
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoBasicNack: Info = {
  id: 3932280,
  classId: 60,
  methodId: 120,
  name: 'BasicNack',
  args: [
    { type: 'longlong', name: 'deliveryTag', default: 0 },
    { type: 'bit', name: 'multiple', default: false },
    { type: 'bit', name: 'requeue', default: true },
  ],
}
export const ConnectionStart = 655370
export interface ConnectionStartFields {
  versionMajor: number
  versionMinor: number
  serverProperties: Table
  mechanisms: Buffer
  locales: Buffer
}
export function decodeConnectionStart(buffer: Buffer, offset: number): ConnectionStartFields {
  let end = 0
  const fields: ConnectionStartFields = {
    versionMajor: undefined as any,
    versionMinor: undefined as any,
    serverProperties: undefined as any,
    mechanisms: undefined as any,
    locales: undefined as any,
  }
  fields.versionMajor = buffer.readUInt8(offset)
  offset++
  fields.versionMinor = buffer.readUInt8(offset)
  offset++
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.serverProperties = decodeFields(buffer, offset + 4, end)
  offset = end
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.mechanisms = bytes(buffer, offset + 4, end)
  offset = end
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.locales = bytes(buffer, offset + 4, end)
  offset = end
  return fields
}
export function encodeConnectionStart(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 10
  buffer[start + 9] = 0
  buffer[start + 10] = 10
  let offset = start + 11
  val = fields.versionMajor
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'versionMajor' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt8(val, offset)
  offset++
  val = fields.versionMinor
  if (val === undefined) val = 9
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'versionMinor' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt8(val, offset)
  offset++
  val = fields.serverProperties
  if (val === undefined) throw new Error("Missing value for mandatory field 'serverProperties'")
  else if (typeof val !== 'object')
    throw new TypeError("Field 'serverProperties' is the wrong type; must be an object")
  offset += encodeTable(buffer, val, offset)
  if (offset + 15 > buffer.length) throw OVERFLOW
  val = fields.mechanisms
  if (val === undefined) val = Buffer.from('PLAIN')
  else if (!Buffer.isBuffer(val))
    throw new TypeError("Field 'mechanisms' is the wrong type; must be a Buffer")
  if (offset + val.length + 15 > buffer.length) throw OVERFLOW
  buffer.writeUInt32BE(val.length, offset)
  offset += 4
  offset += val.copy(buffer, offset)
  val = fields.locales
  if (val === undefined) val = Buffer.from('en_US')
  else if (!Buffer.isBuffer(val))
    throw new TypeError("Field 'locales' is the wrong type; must be a Buffer")
  if (offset + val.length + 15 > buffer.length) throw OVERFLOW
  buffer.writeUInt32BE(val.length, offset)
  offset += 4
  offset += val.copy(buffer, offset)
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConnectionStart: Info = {
  id: 655370,
  classId: 10,
  methodId: 10,
  name: 'ConnectionStart',
  args: [
    { type: 'octet', name: 'versionMajor', default: 0 },
    { type: 'octet', name: 'versionMinor', default: 9 },
    { type: 'table', name: 'serverProperties' },
    { type: 'longstr', name: 'mechanisms', default: 'PLAIN' },
    { type: 'longstr', name: 'locales', default: 'en_US' },
  ],
}
export const ConnectionStartOk = 655371
export interface ConnectionStartOkFields {
  clientProperties: Table
  mechanism: string
  response: Buffer
  locale: string
}
export function decodeConnectionStartOk(buffer: Buffer, offset: number): ConnectionStartOkFields {
  let end = 0
  const fields: ConnectionStartOkFields = {
    clientProperties: undefined as any,
    mechanism: undefined as any,
    response: undefined as any,
    locale: undefined as any,
  }
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.clientProperties = decodeFields(buffer, offset + 4, end)
  offset = end
  fields.mechanism = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.response = bytes(buffer, offset + 4, end)
  offset = end
  fields.locale = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  return fields
}
export function encodeConnectionStartOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 10
  buffer[start + 9] = 0
  buffer[start + 10] = 11
  let offset = start + 11
  val = fields.clientProperties
  if (val === undefined) throw new Error("Missing value for mandatory field 'clientProperties'")
  else if (typeof val !== 'object')
    throw new TypeError("Field 'clientProperties' is the wrong type; must be an object")
  offset += encodeTable(buffer, val, offset)
  if (offset + 521 > buffer.length) throw OVERFLOW
  val = fields.mechanism
  if (val === undefined) val = 'PLAIN'
  else if (typeof val !== 'string')
    throw new TypeError("Field 'mechanism' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'mechanism' is the wrong type; must be a string (up to 255 chars)")
  val = fields.response
  if (val === undefined) throw new Error("Missing value for mandatory field 'response'")
  else if (!Buffer.isBuffer(val))
    throw new TypeError("Field 'response' is the wrong type; must be a Buffer")
  if (offset + val.length + 521 > buffer.length) throw OVERFLOW
  buffer.writeUInt32BE(val.length, offset)
  offset += 4
  offset += val.copy(buffer, offset)
  val = fields.locale
  if (val === undefined) val = 'en_US'
  else if (typeof val !== 'string')
    throw new TypeError("Field 'locale' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'locale' is the wrong type; must be a string (up to 255 chars)")
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConnectionStartOk: Info = {
  id: 655371,
  classId: 10,
  methodId: 11,
  name: 'ConnectionStartOk',
  args: [
    { type: 'table', name: 'clientProperties' },
    { type: 'shortstr', name: 'mechanism', default: 'PLAIN' },
    { type: 'longstr', name: 'response' },
    { type: 'shortstr', name: 'locale', default: 'en_US' },
  ],
}
export const ConnectionSecure = 655380
export interface ConnectionSecureFields {
  challenge: Buffer
}
export function decodeConnectionSecure(buffer: Buffer, offset: number): ConnectionSecureFields {
  let end = 0
  const fields: ConnectionSecureFields = {
    challenge: undefined as any,
  }
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.challenge = bytes(buffer, offset + 4, end)
  offset = end
  return fields
}
export function encodeConnectionSecure(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 10
  buffer[start + 9] = 0
  buffer[start + 10] = 20
  let offset = start + 11
  val = fields.challenge
  if (val === undefined) throw new Error("Missing value for mandatory field 'challenge'")
  else if (!Buffer.isBuffer(val))
    throw new TypeError("Field 'challenge' is the wrong type; must be a Buffer")
  if (offset + val.length + 5 > buffer.length) throw OVERFLOW
  buffer.writeUInt32BE(val.length, offset)
  offset += 4
  offset += val.copy(buffer, offset)
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConnectionSecure: Info = {
  id: 655380,
  classId: 10,
  methodId: 20,
  name: 'ConnectionSecure',
  args: [{ type: 'longstr', name: 'challenge' }],
}
export const ConnectionSecureOk = 655381
export interface ConnectionSecureOkFields {
  response: Buffer
}
export function decodeConnectionSecureOk(buffer: Buffer, offset: number): ConnectionSecureOkFields {
  let end = 0
  const fields: ConnectionSecureOkFields = {
    response: undefined as any,
  }
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.response = bytes(buffer, offset + 4, end)
  offset = end
  return fields
}
export function encodeConnectionSecureOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 10
  buffer[start + 9] = 0
  buffer[start + 10] = 21
  let offset = start + 11
  val = fields.response
  if (val === undefined) throw new Error("Missing value for mandatory field 'response'")
  else if (!Buffer.isBuffer(val))
    throw new TypeError("Field 'response' is the wrong type; must be a Buffer")
  if (offset + val.length + 5 > buffer.length) throw OVERFLOW
  buffer.writeUInt32BE(val.length, offset)
  offset += 4
  offset += val.copy(buffer, offset)
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConnectionSecureOk: Info = {
  id: 655381,
  classId: 10,
  methodId: 21,
  name: 'ConnectionSecureOk',
  args: [{ type: 'longstr', name: 'response' }],
}
export const ConnectionTune = 655390
export interface ConnectionTuneFields {
  channelMax: number
  frameMax: number
  heartbeat: number
}
export function decodeConnectionTune(buffer: Buffer, offset: number): ConnectionTuneFields {
  const fields: ConnectionTuneFields = {
    channelMax: undefined as any,
    frameMax: undefined as any,
    heartbeat: undefined as any,
  }
  fields.channelMax = buffer.readUInt16BE(offset)
  offset += 2
  fields.frameMax = buffer.readUInt32BE(offset)
  offset += 4
  fields.heartbeat = buffer.readUInt16BE(offset)
  offset += 2
  return fields
}
export function encodeConnectionTune(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 10
  buffer[start + 9] = 0
  buffer[start + 10] = 30
  let offset = start + 11
  val = fields.channelMax
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'channelMax' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.frameMax
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'frameMax' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt32BE(val, offset)
  offset += 4
  val = fields.heartbeat
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'heartbeat' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConnectionTune: Info = {
  id: 655390,
  classId: 10,
  methodId: 30,
  name: 'ConnectionTune',
  args: [
    { type: 'short', name: 'channelMax', default: 0 },
    { type: 'long', name: 'frameMax', default: 0 },
    { type: 'short', name: 'heartbeat', default: 0 },
  ],
}
export const ConnectionTuneOk = 655391
export interface ConnectionTuneOkFields {
  channelMax: number
  frameMax: number
  heartbeat: number
}
export function decodeConnectionTuneOk(buffer: Buffer, offset: number): ConnectionTuneOkFields {
  const fields: ConnectionTuneOkFields = {
    channelMax: undefined as any,
    frameMax: undefined as any,
    heartbeat: undefined as any,
  }
  fields.channelMax = buffer.readUInt16BE(offset)
  offset += 2
  fields.frameMax = buffer.readUInt32BE(offset)
  offset += 4
  fields.heartbeat = buffer.readUInt16BE(offset)
  offset += 2
  return fields
}
export function encodeConnectionTuneOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 10
  buffer[start + 9] = 0
  buffer[start + 10] = 31
  let offset = start + 11
  val = fields.channelMax
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'channelMax' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.frameMax
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'frameMax' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt32BE(val, offset)
  offset += 4
  val = fields.heartbeat
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'heartbeat' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConnectionTuneOk: Info = {
  id: 655391,
  classId: 10,
  methodId: 31,
  name: 'ConnectionTuneOk',
  args: [
    { type: 'short', name: 'channelMax', default: 0 },
    { type: 'long', name: 'frameMax', default: 0 },
    { type: 'short', name: 'heartbeat', default: 0 },
  ],
}
export const ConnectionOpen = 655400
export interface ConnectionOpenFields {
  virtualHost: string
  capabilities: string
  insist: boolean
}
export function decodeConnectionOpen(buffer: Buffer, offset: number): ConnectionOpenFields {
  const fields: ConnectionOpenFields = {
    virtualHost: undefined as any,
    capabilities: undefined as any,
    insist: undefined as any,
  }
  fields.virtualHost = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.capabilities = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.insist = (buffer[offset]! & 1) !== 0
  return fields
}
export function encodeConnectionOpen(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 10
  buffer[start + 9] = 0
  buffer[start + 10] = 40
  let offset = start + 11
  val = fields.virtualHost
  if (val === undefined) val = '/'
  else if (typeof val !== 'string')
    throw new TypeError("Field 'virtualHost' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'virtualHost' is the wrong type; must be a string (up to 255 chars)")
  val = fields.capabilities
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError(
      "Field 'capabilities' is the wrong type; must be a string (up to 255 chars)"
    )
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError(
      "Field 'capabilities' is the wrong type; must be a string (up to 255 chars)"
    )
  val = fields.insist
  if (val === undefined) val = false
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConnectionOpen: Info = {
  id: 655400,
  classId: 10,
  methodId: 40,
  name: 'ConnectionOpen',
  args: [
    { type: 'shortstr', name: 'virtualHost', default: '/' },
    { type: 'shortstr', name: 'capabilities', default: '' },
    { type: 'bit', name: 'insist', default: false },
  ],
}
export const ConnectionOpenOk = 655401
export interface ConnectionOpenOkFields {
  knownHosts: string
}
export function decodeConnectionOpenOk(buffer: Buffer, offset: number): ConnectionOpenOkFields {
  const fields: ConnectionOpenOkFields = {
    knownHosts: undefined as any,
  }
  fields.knownHosts = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  return fields
}
export function encodeConnectionOpenOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 10
  buffer[start + 9] = 0
  buffer[start + 10] = 41
  let offset = start + 11
  val = fields.knownHosts
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'knownHosts' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'knownHosts' is the wrong type; must be a string (up to 255 chars)")
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConnectionOpenOk: Info = {
  id: 655401,
  classId: 10,
  methodId: 41,
  name: 'ConnectionOpenOk',
  args: [{ type: 'shortstr', name: 'knownHosts', default: '' }],
}
export const ConnectionClose = 655410
export interface ConnectionCloseFields {
  replyCode: number
  replyText: string
  classId: number
  methodId: number
}
export function decodeConnectionClose(buffer: Buffer, offset: number): ConnectionCloseFields {
  const fields: ConnectionCloseFields = {
    replyCode: undefined as any,
    replyText: undefined as any,
    classId: undefined as any,
    methodId: undefined as any,
  }
  fields.replyCode = buffer.readUInt16BE(offset)
  offset += 2
  fields.replyText = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.classId = buffer.readUInt16BE(offset)
  offset += 2
  fields.methodId = buffer.readUInt16BE(offset)
  offset += 2
  return fields
}
export function encodeConnectionClose(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 10
  buffer[start + 9] = 0
  buffer[start + 10] = 50
  let offset = start + 11
  val = fields.replyCode
  if (val === undefined) throw new Error("Missing value for mandatory field 'replyCode'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'replyCode' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.replyText
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'replyText' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'replyText' is the wrong type; must be a string (up to 255 chars)")
  val = fields.classId
  if (val === undefined) throw new Error("Missing value for mandatory field 'classId'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'classId' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.methodId
  if (val === undefined) throw new Error("Missing value for mandatory field 'methodId'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'methodId' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConnectionClose: Info = {
  id: 655410,
  classId: 10,
  methodId: 50,
  name: 'ConnectionClose',
  args: [
    { type: 'short', name: 'replyCode' },
    { type: 'shortstr', name: 'replyText', default: '' },
    { type: 'short', name: 'classId' },
    { type: 'short', name: 'methodId' },
  ],
}
export const ConnectionCloseOk = 655411
export interface ConnectionCloseOkFields {}
export function decodeConnectionCloseOk(_buffer: Buffer, _offset: number): ConnectionCloseOkFields {
  return {}
}
export function encodeConnectionCloseOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 10
  buffer[start + 9] = 0
  buffer[start + 10] = 51
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConnectionCloseOk: Info = {
  id: 655411,
  classId: 10,
  methodId: 51,
  name: 'ConnectionCloseOk',
  args: [],
}
export const ConnectionBlocked = 655420
export interface ConnectionBlockedFields {
  reason: string
}
export function decodeConnectionBlocked(buffer: Buffer, offset: number): ConnectionBlockedFields {
  const fields: ConnectionBlockedFields = {
    reason: undefined as any,
  }
  fields.reason = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  return fields
}
export function encodeConnectionBlocked(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 10
  buffer[start + 9] = 0
  buffer[start + 10] = 60
  let offset = start + 11
  val = fields.reason
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'reason' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'reason' is the wrong type; must be a string (up to 255 chars)")
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConnectionBlocked: Info = {
  id: 655420,
  classId: 10,
  methodId: 60,
  name: 'ConnectionBlocked',
  args: [{ type: 'shortstr', name: 'reason', default: '' }],
}
export const ConnectionUnblocked = 655421
export interface ConnectionUnblockedFields {}
export function decodeConnectionUnblocked(
  _buffer: Buffer,
  _offset: number
): ConnectionUnblockedFields {
  return {}
}
export function encodeConnectionUnblocked(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 10
  buffer[start + 9] = 0
  buffer[start + 10] = 61
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConnectionUnblocked: Info = {
  id: 655421,
  classId: 10,
  methodId: 61,
  name: 'ConnectionUnblocked',
  args: [],
}
export const ConnectionUpdateSecret = 655430
export interface ConnectionUpdateSecretFields {
  newSecret: Buffer
  reason: string
}
export function decodeConnectionUpdateSecret(
  buffer: Buffer,
  offset: number
): ConnectionUpdateSecretFields {
  let end = 0
  const fields: ConnectionUpdateSecretFields = {
    newSecret: undefined as any,
    reason: undefined as any,
  }
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.newSecret = bytes(buffer, offset + 4, end)
  offset = end
  fields.reason = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  return fields
}
export function encodeConnectionUpdateSecret(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 10
  buffer[start + 9] = 0
  buffer[start + 10] = 70
  let offset = start + 11
  val = fields.newSecret
  if (val === undefined) throw new Error("Missing value for mandatory field 'newSecret'")
  else if (!Buffer.isBuffer(val))
    throw new TypeError("Field 'newSecret' is the wrong type; must be a Buffer")
  if (offset + val.length + 261 > buffer.length) throw OVERFLOW
  buffer.writeUInt32BE(val.length, offset)
  offset += 4
  offset += val.copy(buffer, offset)
  val = fields.reason
  if (val === undefined) throw new Error("Missing value for mandatory field 'reason'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'reason' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'reason' is the wrong type; must be a string (up to 255 chars)")
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConnectionUpdateSecret: Info = {
  id: 655430,
  classId: 10,
  methodId: 70,
  name: 'ConnectionUpdateSecret',
  args: [
    { type: 'longstr', name: 'newSecret' },
    { type: 'shortstr', name: 'reason' },
  ],
}
export const ConnectionUpdateSecretOk = 655431
export interface ConnectionUpdateSecretOkFields {}
export function decodeConnectionUpdateSecretOk(
  _buffer: Buffer,
  _offset: number
): ConnectionUpdateSecretOkFields {
  return {}
}
export function encodeConnectionUpdateSecretOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 10
  buffer[start + 9] = 0
  buffer[start + 10] = 71
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConnectionUpdateSecretOk: Info = {
  id: 655431,
  classId: 10,
  methodId: 71,
  name: 'ConnectionUpdateSecretOk',
  args: [],
}
export const ChannelOpen = 1310730
export interface ChannelOpenFields {
  outOfBand: string
}
export function decodeChannelOpen(buffer: Buffer, offset: number): ChannelOpenFields {
  const fields: ChannelOpenFields = {
    outOfBand: undefined as any,
  }
  fields.outOfBand = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  return fields
}
export function encodeChannelOpen(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 20
  buffer[start + 9] = 0
  buffer[start + 10] = 10
  let offset = start + 11
  val = fields.outOfBand
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'outOfBand' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'outOfBand' is the wrong type; must be a string (up to 255 chars)")
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoChannelOpen: Info = {
  id: 1310730,
  classId: 20,
  methodId: 10,
  name: 'ChannelOpen',
  args: [{ type: 'shortstr', name: 'outOfBand', default: '' }],
}
export const ChannelOpenOk = 1310731
export interface ChannelOpenOkFields {
  channelId: Buffer
}
export function decodeChannelOpenOk(buffer: Buffer, offset: number): ChannelOpenOkFields {
  let end = 0
  const fields: ChannelOpenOkFields = {
    channelId: undefined as any,
  }
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.channelId = bytes(buffer, offset + 4, end)
  offset = end
  return fields
}
export function encodeChannelOpenOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 20
  buffer[start + 9] = 0
  buffer[start + 10] = 11
  let offset = start + 11
  val = fields.channelId
  if (val === undefined) val = Buffer.from('')
  else if (!Buffer.isBuffer(val))
    throw new TypeError("Field 'channelId' is the wrong type; must be a Buffer")
  if (offset + val.length + 5 > buffer.length) throw OVERFLOW
  buffer.writeUInt32BE(val.length, offset)
  offset += 4
  offset += val.copy(buffer, offset)
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoChannelOpenOk: Info = {
  id: 1310731,
  classId: 20,
  methodId: 11,
  name: 'ChannelOpenOk',
  args: [{ type: 'longstr', name: 'channelId', default: '' }],
}
export const ChannelFlow = 1310740
export interface ChannelFlowFields {
  active: boolean
}
export function decodeChannelFlow(buffer: Buffer, offset: number): ChannelFlowFields {
  const fields: ChannelFlowFields = {
    active: undefined as any,
  }
  fields.active = (buffer[offset]! & 1) !== 0
  return fields
}
export function encodeChannelFlow(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 20
  buffer[start + 9] = 0
  buffer[start + 10] = 20
  let offset = start + 11
  val = fields.active
  if (val === undefined) throw new Error("Missing value for mandatory field 'active'")
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoChannelFlow: Info = {
  id: 1310740,
  classId: 20,
  methodId: 20,
  name: 'ChannelFlow',
  args: [{ type: 'bit', name: 'active' }],
}
export const ChannelFlowOk = 1310741
export interface ChannelFlowOkFields {
  active: boolean
}
export function decodeChannelFlowOk(buffer: Buffer, offset: number): ChannelFlowOkFields {
  const fields: ChannelFlowOkFields = {
    active: undefined as any,
  }
  fields.active = (buffer[offset]! & 1) !== 0
  return fields
}
export function encodeChannelFlowOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 20
  buffer[start + 9] = 0
  buffer[start + 10] = 21
  let offset = start + 11
  val = fields.active
  if (val === undefined) throw new Error("Missing value for mandatory field 'active'")
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoChannelFlowOk: Info = {
  id: 1310741,
  classId: 20,
  methodId: 21,
  name: 'ChannelFlowOk',
  args: [{ type: 'bit', name: 'active' }],
}
export const ChannelClose = 1310760
export interface ChannelCloseFields {
  replyCode: number
  replyText: string
  classId: number
  methodId: number
}
export function decodeChannelClose(buffer: Buffer, offset: number): ChannelCloseFields {
  const fields: ChannelCloseFields = {
    replyCode: undefined as any,
    replyText: undefined as any,
    classId: undefined as any,
    methodId: undefined as any,
  }
  fields.replyCode = buffer.readUInt16BE(offset)
  offset += 2
  fields.replyText = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.classId = buffer.readUInt16BE(offset)
  offset += 2
  fields.methodId = buffer.readUInt16BE(offset)
  offset += 2
  return fields
}
export function encodeChannelClose(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 20
  buffer[start + 9] = 0
  buffer[start + 10] = 40
  let offset = start + 11
  val = fields.replyCode
  if (val === undefined) throw new Error("Missing value for mandatory field 'replyCode'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'replyCode' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.replyText
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'replyText' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'replyText' is the wrong type; must be a string (up to 255 chars)")
  val = fields.classId
  if (val === undefined) throw new Error("Missing value for mandatory field 'classId'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'classId' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.methodId
  if (val === undefined) throw new Error("Missing value for mandatory field 'methodId'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'methodId' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoChannelClose: Info = {
  id: 1310760,
  classId: 20,
  methodId: 40,
  name: 'ChannelClose',
  args: [
    { type: 'short', name: 'replyCode' },
    { type: 'shortstr', name: 'replyText', default: '' },
    { type: 'short', name: 'classId' },
    { type: 'short', name: 'methodId' },
  ],
}
export const ChannelCloseOk = 1310761
export interface ChannelCloseOkFields {}
export function decodeChannelCloseOk(_buffer: Buffer, _offset: number): ChannelCloseOkFields {
  return {}
}
export function encodeChannelCloseOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 20
  buffer[start + 9] = 0
  buffer[start + 10] = 41
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoChannelCloseOk: Info = {
  id: 1310761,
  classId: 20,
  methodId: 41,
  name: 'ChannelCloseOk',
  args: [],
}
export const AccessRequest = 1966090
export interface AccessRequestFields {
  realm: string
  exclusive: boolean
  passive: boolean
  active: boolean
  write: boolean
  read: boolean
}
export function decodeAccessRequest(buffer: Buffer, offset: number): AccessRequestFields {
  const fields: AccessRequestFields = {
    realm: undefined as any,
    exclusive: undefined as any,
    passive: undefined as any,
    active: undefined as any,
    write: undefined as any,
    read: undefined as any,
  }
  fields.realm = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.exclusive = (buffer[offset]! & 1) !== 0
  fields.passive = (buffer[offset]! & 2) !== 0
  fields.active = (buffer[offset]! & 4) !== 0
  fields.write = (buffer[offset]! & 8) !== 0
  fields.read = (buffer[offset]! & 16) !== 0
  return fields
}
export function encodeAccessRequest(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 30
  buffer[start + 9] = 0
  buffer[start + 10] = 10
  let offset = start + 11
  val = fields.realm
  if (val === undefined) val = '/data'
  else if (typeof val !== 'string')
    throw new TypeError("Field 'realm' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'realm' is the wrong type; must be a string (up to 255 chars)")
  val = fields.exclusive
  if (val === undefined) val = false
  if (val) bits += 1
  val = fields.passive
  if (val === undefined) val = true
  if (val) bits += 2
  val = fields.active
  if (val === undefined) val = true
  if (val) bits += 4
  val = fields.write
  if (val === undefined) val = true
  if (val) bits += 8
  val = fields.read
  if (val === undefined) val = true
  if (val) bits += 16
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoAccessRequest: Info = {
  id: 1966090,
  classId: 30,
  methodId: 10,
  name: 'AccessRequest',
  args: [
    { type: 'shortstr', name: 'realm', default: '/data' },
    { type: 'bit', name: 'exclusive', default: false },
    { type: 'bit', name: 'passive', default: true },
    { type: 'bit', name: 'active', default: true },
    { type: 'bit', name: 'write', default: true },
    { type: 'bit', name: 'read', default: true },
  ],
}
export const AccessRequestOk = 1966091
export interface AccessRequestOkFields {
  ticket: number
}
export function decodeAccessRequestOk(buffer: Buffer, offset: number): AccessRequestOkFields {
  const fields: AccessRequestOkFields = {
    ticket: undefined as any,
  }
  fields.ticket = buffer.readUInt16BE(offset)
  offset += 2
  return fields
}
export function encodeAccessRequestOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 30
  buffer[start + 9] = 0
  buffer[start + 10] = 11
  let offset = start + 11
  val = fields.ticket
  if (val === undefined) val = 1
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'ticket' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoAccessRequestOk: Info = {
  id: 1966091,
  classId: 30,
  methodId: 11,
  name: 'AccessRequestOk',
  args: [{ type: 'short', name: 'ticket', default: 1 }],
}
export const ExchangeDeclare = 2621450
export interface ExchangeDeclareFields {
  ticket: number
  exchange: string
  type: string
  passive: boolean
  durable: boolean
  autoDelete: boolean
  internal: boolean
  nowait: boolean
  arguments: Table
}
export function decodeExchangeDeclare(buffer: Buffer, offset: number): ExchangeDeclareFields {
  let end = 0
  const fields: ExchangeDeclareFields = {
    ticket: undefined as any,
    exchange: undefined as any,
    type: undefined as any,
    passive: undefined as any,
    durable: undefined as any,
    autoDelete: undefined as any,
    internal: undefined as any,
    nowait: undefined as any,
    arguments: undefined as any,
  }
  fields.ticket = buffer.readUInt16BE(offset)
  offset += 2
  fields.exchange = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.type = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.passive = (buffer[offset]! & 1) !== 0
  fields.durable = (buffer[offset]! & 2) !== 0
  fields.autoDelete = (buffer[offset]! & 4) !== 0
  fields.internal = (buffer[offset]! & 8) !== 0
  fields.nowait = (buffer[offset]! & 16) !== 0
  offset++
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.arguments = decodeFields(buffer, offset + 4, end)
  offset = end
  return fields
}
export function encodeExchangeDeclare(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 40
  buffer[start + 9] = 0
  buffer[start + 10] = 10
  let offset = start + 11
  val = fields.ticket
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'ticket' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.exchange
  if (val === undefined) throw new Error("Missing value for mandatory field 'exchange'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  val = fields.type
  if (val === undefined) val = 'direct'
  else if (typeof val !== 'string')
    throw new TypeError("Field 'type' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'type' is the wrong type; must be a string (up to 255 chars)")
  val = fields.passive
  if (val === undefined) val = false
  if (val) bits += 1
  val = fields.durable
  if (val === undefined) val = false
  if (val) bits += 2
  val = fields.autoDelete
  if (val === undefined) val = false
  if (val) bits += 4
  val = fields.internal
  if (val === undefined) val = false
  if (val) bits += 8
  val = fields.nowait
  if (val === undefined) val = false
  if (val) bits += 16
  buffer[offset] = bits
  offset++
  bits = 0
  val = fields.arguments
  if (val === undefined) val = {}
  else if (typeof val !== 'object')
    throw new TypeError("Field 'arguments' is the wrong type; must be an object")
  offset += encodeTable(buffer, val, offset)
  if (offset + 520 > buffer.length) throw OVERFLOW
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoExchangeDeclare: Info = {
  id: 2621450,
  classId: 40,
  methodId: 10,
  name: 'ExchangeDeclare',
  args: [
    { type: 'short', name: 'ticket', default: 0 },
    { type: 'shortstr', name: 'exchange' },
    { type: 'shortstr', name: 'type', default: 'direct' },
    { type: 'bit', name: 'passive', default: false },
    { type: 'bit', name: 'durable', default: false },
    { type: 'bit', name: 'autoDelete', default: false },
    { type: 'bit', name: 'internal', default: false },
    { type: 'bit', name: 'nowait', default: false },
    { type: 'table', name: 'arguments', default: {} },
  ],
}
export const ExchangeDeclareOk = 2621451
export interface ExchangeDeclareOkFields {}
export function decodeExchangeDeclareOk(_buffer: Buffer, _offset: number): ExchangeDeclareOkFields {
  return {}
}
export function encodeExchangeDeclareOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 40
  buffer[start + 9] = 0
  buffer[start + 10] = 11
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoExchangeDeclareOk: Info = {
  id: 2621451,
  classId: 40,
  methodId: 11,
  name: 'ExchangeDeclareOk',
  args: [],
}
export const ExchangeDelete = 2621460
export interface ExchangeDeleteFields {
  ticket: number
  exchange: string
  ifUnused: boolean
  nowait: boolean
}
export function decodeExchangeDelete(buffer: Buffer, offset: number): ExchangeDeleteFields {
  const fields: ExchangeDeleteFields = {
    ticket: undefined as any,
    exchange: undefined as any,
    ifUnused: undefined as any,
    nowait: undefined as any,
  }
  fields.ticket = buffer.readUInt16BE(offset)
  offset += 2
  fields.exchange = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.ifUnused = (buffer[offset]! & 1) !== 0
  fields.nowait = (buffer[offset]! & 2) !== 0
  return fields
}
export function encodeExchangeDelete(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 40
  buffer[start + 9] = 0
  buffer[start + 10] = 20
  let offset = start + 11
  val = fields.ticket
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'ticket' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.exchange
  if (val === undefined) throw new Error("Missing value for mandatory field 'exchange'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  val = fields.ifUnused
  if (val === undefined) val = false
  if (val) bits += 1
  val = fields.nowait
  if (val === undefined) val = false
  if (val) bits += 2
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoExchangeDelete: Info = {
  id: 2621460,
  classId: 40,
  methodId: 20,
  name: 'ExchangeDelete',
  args: [
    { type: 'short', name: 'ticket', default: 0 },
    { type: 'shortstr', name: 'exchange' },
    { type: 'bit', name: 'ifUnused', default: false },
    { type: 'bit', name: 'nowait', default: false },
  ],
}
export const ExchangeDeleteOk = 2621461
export interface ExchangeDeleteOkFields {}
export function decodeExchangeDeleteOk(_buffer: Buffer, _offset: number): ExchangeDeleteOkFields {
  return {}
}
export function encodeExchangeDeleteOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 40
  buffer[start + 9] = 0
  buffer[start + 10] = 21
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoExchangeDeleteOk: Info = {
  id: 2621461,
  classId: 40,
  methodId: 21,
  name: 'ExchangeDeleteOk',
  args: [],
}
export const ExchangeBind = 2621470
export interface ExchangeBindFields {
  ticket: number
  destination: string
  source: string
  routingKey: string
  nowait: boolean
  arguments: Table
}
export function decodeExchangeBind(buffer: Buffer, offset: number): ExchangeBindFields {
  let end = 0
  const fields: ExchangeBindFields = {
    ticket: undefined as any,
    destination: undefined as any,
    source: undefined as any,
    routingKey: undefined as any,
    nowait: undefined as any,
    arguments: undefined as any,
  }
  fields.ticket = buffer.readUInt16BE(offset)
  offset += 2
  fields.destination = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.source = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.routingKey = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.nowait = (buffer[offset]! & 1) !== 0
  offset++
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.arguments = decodeFields(buffer, offset + 4, end)
  offset = end
  return fields
}
export function encodeExchangeBind(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 40
  buffer[start + 9] = 0
  buffer[start + 10] = 30
  let offset = start + 11
  val = fields.ticket
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'ticket' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.destination
  if (val === undefined) throw new Error("Missing value for mandatory field 'destination'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'destination' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'destination' is the wrong type; must be a string (up to 255 chars)")
  val = fields.source
  if (val === undefined) throw new Error("Missing value for mandatory field 'source'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'source' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'source' is the wrong type; must be a string (up to 255 chars)")
  val = fields.routingKey
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  val = fields.nowait
  if (val === undefined) val = false
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  val = fields.arguments
  if (val === undefined) val = {}
  else if (typeof val !== 'object')
    throw new TypeError("Field 'arguments' is the wrong type; must be an object")
  offset += encodeTable(buffer, val, offset)
  if (offset + 776 > buffer.length) throw OVERFLOW
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoExchangeBind: Info = {
  id: 2621470,
  classId: 40,
  methodId: 30,
  name: 'ExchangeBind',
  args: [
    { type: 'short', name: 'ticket', default: 0 },
    { type: 'shortstr', name: 'destination' },
    { type: 'shortstr', name: 'source' },
    { type: 'shortstr', name: 'routingKey', default: '' },
    { type: 'bit', name: 'nowait', default: false },
    { type: 'table', name: 'arguments', default: {} },
  ],
}
export const ExchangeBindOk = 2621471
export interface ExchangeBindOkFields {}
export function decodeExchangeBindOk(_buffer: Buffer, _offset: number): ExchangeBindOkFields {
  return {}
}
export function encodeExchangeBindOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 40
  buffer[start + 9] = 0
  buffer[start + 10] = 31
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoExchangeBindOk: Info = {
  id: 2621471,
  classId: 40,
  methodId: 31,
  name: 'ExchangeBindOk',
  args: [],
}
export const ExchangeUnbind = 2621480
export interface ExchangeUnbindFields {
  ticket: number
  destination: string
  source: string
  routingKey: string
  nowait: boolean
  arguments: Table
}
export function decodeExchangeUnbind(buffer: Buffer, offset: number): ExchangeUnbindFields {
  let end = 0
  const fields: ExchangeUnbindFields = {
    ticket: undefined as any,
    destination: undefined as any,
    source: undefined as any,
    routingKey: undefined as any,
    nowait: undefined as any,
    arguments: undefined as any,
  }
  fields.ticket = buffer.readUInt16BE(offset)
  offset += 2
  fields.destination = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.source = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.routingKey = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.nowait = (buffer[offset]! & 1) !== 0
  offset++
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.arguments = decodeFields(buffer, offset + 4, end)
  offset = end
  return fields
}
export function encodeExchangeUnbind(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 40
  buffer[start + 9] = 0
  buffer[start + 10] = 40
  let offset = start + 11
  val = fields.ticket
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'ticket' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.destination
  if (val === undefined) throw new Error("Missing value for mandatory field 'destination'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'destination' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'destination' is the wrong type; must be a string (up to 255 chars)")
  val = fields.source
  if (val === undefined) throw new Error("Missing value for mandatory field 'source'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'source' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'source' is the wrong type; must be a string (up to 255 chars)")
  val = fields.routingKey
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  val = fields.nowait
  if (val === undefined) val = false
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  val = fields.arguments
  if (val === undefined) val = {}
  else if (typeof val !== 'object')
    throw new TypeError("Field 'arguments' is the wrong type; must be an object")
  offset += encodeTable(buffer, val, offset)
  if (offset + 776 > buffer.length) throw OVERFLOW
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoExchangeUnbind: Info = {
  id: 2621480,
  classId: 40,
  methodId: 40,
  name: 'ExchangeUnbind',
  args: [
    { type: 'short', name: 'ticket', default: 0 },
    { type: 'shortstr', name: 'destination' },
    { type: 'shortstr', name: 'source' },
    { type: 'shortstr', name: 'routingKey', default: '' },
    { type: 'bit', name: 'nowait', default: false },
    { type: 'table', name: 'arguments', default: {} },
  ],
}
export const ExchangeUnbindOk = 2621491
export interface ExchangeUnbindOkFields {}
export function decodeExchangeUnbindOk(_buffer: Buffer, _offset: number): ExchangeUnbindOkFields {
  return {}
}
export function encodeExchangeUnbindOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 40
  buffer[start + 9] = 0
  buffer[start + 10] = 51
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoExchangeUnbindOk: Info = {
  id: 2621491,
  classId: 40,
  methodId: 51,
  name: 'ExchangeUnbindOk',
  args: [],
}
export const QueueDeclare = 3276810
export interface QueueDeclareFields {
  ticket: number
  queue: string
  passive: boolean
  durable: boolean
  exclusive: boolean
  autoDelete: boolean
  nowait: boolean
  arguments: Table
}
export function decodeQueueDeclare(buffer: Buffer, offset: number): QueueDeclareFields {
  let end = 0
  const fields: QueueDeclareFields = {
    ticket: undefined as any,
    queue: undefined as any,
    passive: undefined as any,
    durable: undefined as any,
    exclusive: undefined as any,
    autoDelete: undefined as any,
    nowait: undefined as any,
    arguments: undefined as any,
  }
  fields.ticket = buffer.readUInt16BE(offset)
  offset += 2
  fields.queue = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.passive = (buffer[offset]! & 1) !== 0
  fields.durable = (buffer[offset]! & 2) !== 0
  fields.exclusive = (buffer[offset]! & 4) !== 0
  fields.autoDelete = (buffer[offset]! & 8) !== 0
  fields.nowait = (buffer[offset]! & 16) !== 0
  offset++
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.arguments = decodeFields(buffer, offset + 4, end)
  offset = end
  return fields
}
export function encodeQueueDeclare(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 50
  buffer[start + 9] = 0
  buffer[start + 10] = 10
  let offset = start + 11
  val = fields.ticket
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'ticket' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.queue
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  val = fields.passive
  if (val === undefined) val = false
  if (val) bits += 1
  val = fields.durable
  if (val === undefined) val = false
  if (val) bits += 2
  val = fields.exclusive
  if (val === undefined) val = false
  if (val) bits += 4
  val = fields.autoDelete
  if (val === undefined) val = false
  if (val) bits += 8
  val = fields.nowait
  if (val === undefined) val = false
  if (val) bits += 16
  buffer[offset] = bits
  offset++
  bits = 0
  val = fields.arguments
  if (val === undefined) val = {}
  else if (typeof val !== 'object')
    throw new TypeError("Field 'arguments' is the wrong type; must be an object")
  offset += encodeTable(buffer, val, offset)
  if (offset + 264 > buffer.length) throw OVERFLOW
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoQueueDeclare: Info = {
  id: 3276810,
  classId: 50,
  methodId: 10,
  name: 'QueueDeclare',
  args: [
    { type: 'short', name: 'ticket', default: 0 },
    { type: 'shortstr', name: 'queue', default: '' },
    { type: 'bit', name: 'passive', default: false },
    { type: 'bit', name: 'durable', default: false },
    { type: 'bit', name: 'exclusive', default: false },
    { type: 'bit', name: 'autoDelete', default: false },
    { type: 'bit', name: 'nowait', default: false },
    { type: 'table', name: 'arguments', default: {} },
  ],
}
export const QueueDeclareOk = 3276811
export interface QueueDeclareOkFields {
  queue: string
  messageCount: number
  consumerCount: number
}
export function decodeQueueDeclareOk(buffer: Buffer, offset: number): QueueDeclareOkFields {
  const fields: QueueDeclareOkFields = {
    queue: undefined as any,
    messageCount: undefined as any,
    consumerCount: undefined as any,
  }
  fields.queue = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.messageCount = buffer.readUInt32BE(offset)
  offset += 4
  fields.consumerCount = buffer.readUInt32BE(offset)
  offset += 4
  return fields
}
export function encodeQueueDeclareOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 50
  buffer[start + 9] = 0
  buffer[start + 10] = 11
  let offset = start + 11
  val = fields.queue
  if (val === undefined) throw new Error("Missing value for mandatory field 'queue'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  val = fields.messageCount
  if (val === undefined) throw new Error("Missing value for mandatory field 'messageCount'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'messageCount' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt32BE(val, offset)
  offset += 4
  val = fields.consumerCount
  if (val === undefined) throw new Error("Missing value for mandatory field 'consumerCount'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'consumerCount' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt32BE(val, offset)
  offset += 4
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoQueueDeclareOk: Info = {
  id: 3276811,
  classId: 50,
  methodId: 11,
  name: 'QueueDeclareOk',
  args: [
    { type: 'shortstr', name: 'queue' },
    { type: 'long', name: 'messageCount' },
    { type: 'long', name: 'consumerCount' },
  ],
}
export const QueueBind = 3276820
export interface QueueBindFields {
  ticket: number
  queue: string
  exchange: string
  routingKey: string
  nowait: boolean
  arguments: Table
}
export function decodeQueueBind(buffer: Buffer, offset: number): QueueBindFields {
  let end = 0
  const fields: QueueBindFields = {
    ticket: undefined as any,
    queue: undefined as any,
    exchange: undefined as any,
    routingKey: undefined as any,
    nowait: undefined as any,
    arguments: undefined as any,
  }
  fields.ticket = buffer.readUInt16BE(offset)
  offset += 2
  fields.queue = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.exchange = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.routingKey = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.nowait = (buffer[offset]! & 1) !== 0
  offset++
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.arguments = decodeFields(buffer, offset + 4, end)
  offset = end
  return fields
}
export function encodeQueueBind(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 50
  buffer[start + 9] = 0
  buffer[start + 10] = 20
  let offset = start + 11
  val = fields.ticket
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'ticket' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.queue
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  val = fields.exchange
  if (val === undefined) throw new Error("Missing value for mandatory field 'exchange'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  val = fields.routingKey
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  val = fields.nowait
  if (val === undefined) val = false
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  val = fields.arguments
  if (val === undefined) val = {}
  else if (typeof val !== 'object')
    throw new TypeError("Field 'arguments' is the wrong type; must be an object")
  offset += encodeTable(buffer, val, offset)
  if (offset + 776 > buffer.length) throw OVERFLOW
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoQueueBind: Info = {
  id: 3276820,
  classId: 50,
  methodId: 20,
  name: 'QueueBind',
  args: [
    { type: 'short', name: 'ticket', default: 0 },
    { type: 'shortstr', name: 'queue', default: '' },
    { type: 'shortstr', name: 'exchange' },
    { type: 'shortstr', name: 'routingKey', default: '' },
    { type: 'bit', name: 'nowait', default: false },
    { type: 'table', name: 'arguments', default: {} },
  ],
}
export const QueueBindOk = 3276821
export interface QueueBindOkFields {}
export function decodeQueueBindOk(_buffer: Buffer, _offset: number): QueueBindOkFields {
  return {}
}
export function encodeQueueBindOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 50
  buffer[start + 9] = 0
  buffer[start + 10] = 21
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoQueueBindOk: Info = {
  id: 3276821,
  classId: 50,
  methodId: 21,
  name: 'QueueBindOk',
  args: [],
}
export const QueuePurge = 3276830
export interface QueuePurgeFields {
  ticket: number
  queue: string
  nowait: boolean
}
export function decodeQueuePurge(buffer: Buffer, offset: number): QueuePurgeFields {
  const fields: QueuePurgeFields = {
    ticket: undefined as any,
    queue: undefined as any,
    nowait: undefined as any,
  }
  fields.ticket = buffer.readUInt16BE(offset)
  offset += 2
  fields.queue = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.nowait = (buffer[offset]! & 1) !== 0
  return fields
}
export function encodeQueuePurge(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 50
  buffer[start + 9] = 0
  buffer[start + 10] = 30
  let offset = start + 11
  val = fields.ticket
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'ticket' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.queue
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  val = fields.nowait
  if (val === undefined) val = false
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoQueuePurge: Info = {
  id: 3276830,
  classId: 50,
  methodId: 30,
  name: 'QueuePurge',
  args: [
    { type: 'short', name: 'ticket', default: 0 },
    { type: 'shortstr', name: 'queue', default: '' },
    { type: 'bit', name: 'nowait', default: false },
  ],
}
export const QueuePurgeOk = 3276831
export interface QueuePurgeOkFields {
  messageCount: number
}
export function decodeQueuePurgeOk(buffer: Buffer, offset: number): QueuePurgeOkFields {
  const fields: QueuePurgeOkFields = {
    messageCount: undefined as any,
  }
  fields.messageCount = buffer.readUInt32BE(offset)
  offset += 4
  return fields
}
export function encodeQueuePurgeOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 50
  buffer[start + 9] = 0
  buffer[start + 10] = 31
  let offset = start + 11
  val = fields.messageCount
  if (val === undefined) throw new Error("Missing value for mandatory field 'messageCount'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'messageCount' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt32BE(val, offset)
  offset += 4
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoQueuePurgeOk: Info = {
  id: 3276831,
  classId: 50,
  methodId: 31,
  name: 'QueuePurgeOk',
  args: [{ type: 'long', name: 'messageCount' }],
}
export const QueueDelete = 3276840
export interface QueueDeleteFields {
  ticket: number
  queue: string
  ifUnused: boolean
  ifEmpty: boolean
  nowait: boolean
}
export function decodeQueueDelete(buffer: Buffer, offset: number): QueueDeleteFields {
  const fields: QueueDeleteFields = {
    ticket: undefined as any,
    queue: undefined as any,
    ifUnused: undefined as any,
    ifEmpty: undefined as any,
    nowait: undefined as any,
  }
  fields.ticket = buffer.readUInt16BE(offset)
  offset += 2
  fields.queue = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.ifUnused = (buffer[offset]! & 1) !== 0
  fields.ifEmpty = (buffer[offset]! & 2) !== 0
  fields.nowait = (buffer[offset]! & 4) !== 0
  return fields
}
export function encodeQueueDelete(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 50
  buffer[start + 9] = 0
  buffer[start + 10] = 40
  let offset = start + 11
  val = fields.ticket
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'ticket' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.queue
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  val = fields.ifUnused
  if (val === undefined) val = false
  if (val) bits += 1
  val = fields.ifEmpty
  if (val === undefined) val = false
  if (val) bits += 2
  val = fields.nowait
  if (val === undefined) val = false
  if (val) bits += 4
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoQueueDelete: Info = {
  id: 3276840,
  classId: 50,
  methodId: 40,
  name: 'QueueDelete',
  args: [
    { type: 'short', name: 'ticket', default: 0 },
    { type: 'shortstr', name: 'queue', default: '' },
    { type: 'bit', name: 'ifUnused', default: false },
    { type: 'bit', name: 'ifEmpty', default: false },
    { type: 'bit', name: 'nowait', default: false },
  ],
}
export const QueueDeleteOk = 3276841
export interface QueueDeleteOkFields {
  messageCount: number
}
export function decodeQueueDeleteOk(buffer: Buffer, offset: number): QueueDeleteOkFields {
  const fields: QueueDeleteOkFields = {
    messageCount: undefined as any,
  }
  fields.messageCount = buffer.readUInt32BE(offset)
  offset += 4
  return fields
}
export function encodeQueueDeleteOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 50
  buffer[start + 9] = 0
  buffer[start + 10] = 41
  let offset = start + 11
  val = fields.messageCount
  if (val === undefined) throw new Error("Missing value for mandatory field 'messageCount'")
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'messageCount' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt32BE(val, offset)
  offset += 4
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoQueueDeleteOk: Info = {
  id: 3276841,
  classId: 50,
  methodId: 41,
  name: 'QueueDeleteOk',
  args: [{ type: 'long', name: 'messageCount' }],
}
export const QueueUnbind = 3276850
export interface QueueUnbindFields {
  ticket: number
  queue: string
  exchange: string
  routingKey: string
  arguments: Table
}
export function decodeQueueUnbind(buffer: Buffer, offset: number): QueueUnbindFields {
  let end = 0
  const fields: QueueUnbindFields = {
    ticket: undefined as any,
    queue: undefined as any,
    exchange: undefined as any,
    routingKey: undefined as any,
    arguments: undefined as any,
  }
  fields.ticket = buffer.readUInt16BE(offset)
  offset += 2
  fields.queue = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.exchange = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  fields.routingKey = readShortString(buffer, offset)
  offset += 1 + buffer[offset]!
  end = offset + 4 + buffer.readUInt32BE(offset)
  fields.arguments = decodeFields(buffer, offset + 4, end)
  offset = end
  return fields
}
export function encodeQueueUnbind(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 50
  buffer[start + 9] = 0
  buffer[start + 10] = 50
  let offset = start + 11
  val = fields.ticket
  if (val === undefined) val = 0
  else if (typeof val !== 'number' || Number.isNaN(val))
    throw new TypeError("Field 'ticket' is the wrong type; must be a number (but not NaN)")
  buffer.writeUInt16BE(val, offset)
  offset += 2
  val = fields.queue
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'queue' is the wrong type; must be a string (up to 255 chars)")
  val = fields.exchange
  if (val === undefined) throw new Error("Missing value for mandatory field 'exchange'")
  else if (typeof val !== 'string')
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'exchange' is the wrong type; must be a string (up to 255 chars)")
  val = fields.routingKey
  if (val === undefined) val = ''
  else if (typeof val !== 'string')
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  offset = writeShortString(buffer, val, offset)
  if (offset < 0)
    throw new TypeError("Field 'routingKey' is the wrong type; must be a string (up to 255 chars)")
  val = fields.arguments
  if (val === undefined) val = {}
  else if (typeof val !== 'object')
    throw new TypeError("Field 'arguments' is the wrong type; must be an object")
  offset += encodeTable(buffer, val, offset)
  if (offset + 775 > buffer.length) throw OVERFLOW
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoQueueUnbind: Info = {
  id: 3276850,
  classId: 50,
  methodId: 50,
  name: 'QueueUnbind',
  args: [
    { type: 'short', name: 'ticket', default: 0 },
    { type: 'shortstr', name: 'queue', default: '' },
    { type: 'shortstr', name: 'exchange' },
    { type: 'shortstr', name: 'routingKey', default: '' },
    { type: 'table', name: 'arguments', default: {} },
  ],
}
export const QueueUnbindOk = 3276851
export interface QueueUnbindOkFields {}
export function decodeQueueUnbindOk(_buffer: Buffer, _offset: number): QueueUnbindOkFields {
  return {}
}
export function encodeQueueUnbindOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 50
  buffer[start + 9] = 0
  buffer[start + 10] = 51
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoQueueUnbindOk: Info = {
  id: 3276851,
  classId: 50,
  methodId: 51,
  name: 'QueueUnbindOk',
  args: [],
}
export const TxSelect = 5898250
export interface TxSelectFields {}
export function decodeTxSelect(_buffer: Buffer, _offset: number): TxSelectFields {
  return {}
}
export function encodeTxSelect(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 90
  buffer[start + 9] = 0
  buffer[start + 10] = 10
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoTxSelect: Info = {
  id: 5898250,
  classId: 90,
  methodId: 10,
  name: 'TxSelect',
  args: [],
}
export const TxSelectOk = 5898251
export interface TxSelectOkFields {}
export function decodeTxSelectOk(_buffer: Buffer, _offset: number): TxSelectOkFields {
  return {}
}
export function encodeTxSelectOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 90
  buffer[start + 9] = 0
  buffer[start + 10] = 11
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoTxSelectOk: Info = {
  id: 5898251,
  classId: 90,
  methodId: 11,
  name: 'TxSelectOk',
  args: [],
}
export const TxCommit = 5898260
export interface TxCommitFields {}
export function decodeTxCommit(_buffer: Buffer, _offset: number): TxCommitFields {
  return {}
}
export function encodeTxCommit(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 90
  buffer[start + 9] = 0
  buffer[start + 10] = 20
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoTxCommit: Info = {
  id: 5898260,
  classId: 90,
  methodId: 20,
  name: 'TxCommit',
  args: [],
}
export const TxCommitOk = 5898261
export interface TxCommitOkFields {}
export function decodeTxCommitOk(_buffer: Buffer, _offset: number): TxCommitOkFields {
  return {}
}
export function encodeTxCommitOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 90
  buffer[start + 9] = 0
  buffer[start + 10] = 21
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoTxCommitOk: Info = {
  id: 5898261,
  classId: 90,
  methodId: 21,
  name: 'TxCommitOk',
  args: [],
}
export const TxRollback = 5898270
export interface TxRollbackFields {}
export function decodeTxRollback(_buffer: Buffer, _offset: number): TxRollbackFields {
  return {}
}
export function encodeTxRollback(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 90
  buffer[start + 9] = 0
  buffer[start + 10] = 30
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoTxRollback: Info = {
  id: 5898270,
  classId: 90,
  methodId: 30,
  name: 'TxRollback',
  args: [],
}
export const TxRollbackOk = 5898271
export interface TxRollbackOkFields {}
export function decodeTxRollbackOk(_buffer: Buffer, _offset: number): TxRollbackOkFields {
  return {}
}
export function encodeTxRollbackOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 90
  buffer[start + 9] = 0
  buffer[start + 10] = 31
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoTxRollbackOk: Info = {
  id: 5898271,
  classId: 90,
  methodId: 31,
  name: 'TxRollbackOk',
  args: [],
}
export const ConfirmSelect = 5570570
export interface ConfirmSelectFields {
  nowait: boolean
}
export function decodeConfirmSelect(buffer: Buffer, offset: number): ConfirmSelectFields {
  const fields: ConfirmSelectFields = {
    nowait: undefined as any,
  }
  fields.nowait = (buffer[offset]! & 1) !== 0
  return fields
}
export function encodeConfirmSelect(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  let val: any
  let bits = 0
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 85
  buffer[start + 9] = 0
  buffer[start + 10] = 10
  let offset = start + 11
  val = fields.nowait
  if (val === undefined) val = false
  if (val) bits += 1
  buffer[offset] = bits
  offset++
  bits = 0
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConfirmSelect: Info = {
  id: 5570570,
  classId: 85,
  methodId: 10,
  name: 'ConfirmSelect',
  args: [{ type: 'bit', name: 'nowait', default: false }],
}
export const ConfirmSelectOk = 5570571
export interface ConfirmSelectOkFields {}
export function decodeConfirmSelectOk(_buffer: Buffer, _offset: number): ConfirmSelectOkFields {
  return {}
}
export function encodeConfirmSelectOk(
  buffer: Buffer,
  start: number,
  channel: number,
  fields: any
): number {
  buffer[start] = 1
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 85
  buffer[start + 9] = 0
  buffer[start + 10] = 11
  let offset = start + 11
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export const methodInfoConfirmSelectOk: Info = {
  id: 5570571,
  classId: 85,
  methodId: 11,
  name: 'ConfirmSelectOk',
  args: [],
}
export const BasicProperties = 60
export interface BasicPropertiesFields {
  contentType?: string
  contentEncoding?: string
  headers?: Table
  deliveryMode?: number
  priority?: number
  correlationId?: string
  replyTo?: string
  expiration?: string
  messageId?: string
  timestamp?: number
  type?: string
  userId?: string
  appId?: string
  clusterId?: string
}
export function encodeBasicProperties(
  buffer: Buffer,
  start: number,
  channel: number,
  size: number,
  fields: any
): number {
  let val: any
  let flags = 0
  buffer[start] = 2
  buffer[start + 1] = channel >>> 8
  buffer[start + 2] = channel
  buffer[start + 7] = 0
  buffer[start + 8] = 60
  buffer[start + 9] = 0
  buffer[start + 10] = 0
  writeUInt64(buffer, size, start + 11)
  let offset = start + 21
  val = fields.contentType
  if (val !== undefined && val !== null) {
    if (typeof val !== 'string')
      throw new TypeError(
        "Field 'contentType' is the wrong type; must be a string (up to 255 chars)"
      )
    flags += 32768
    offset = writeShortString(buffer, val, offset)
    if (offset < 0)
      throw new TypeError(
        "Field 'contentType' is the wrong type; must be a string (up to 255 chars)"
      )
  }
  val = fields.contentEncoding
  if (val !== undefined && val !== null) {
    if (typeof val !== 'string')
      throw new TypeError(
        "Field 'contentEncoding' is the wrong type; must be a string (up to 255 chars)"
      )
    flags += 16384
    offset = writeShortString(buffer, val, offset)
    if (offset < 0)
      throw new TypeError(
        "Field 'contentEncoding' is the wrong type; must be a string (up to 255 chars)"
      )
  }
  val = fields.headers
  if (val !== undefined && val !== null) {
    if (typeof val !== 'object')
      throw new TypeError("Field 'headers' is the wrong type; must be an object")
    flags += 8192
    offset += encodeTable(buffer, val, offset)
    if (offset + 2575 > buffer.length) throw OVERFLOW
  }
  val = fields.deliveryMode
  if (val !== undefined && val !== null) {
    if (typeof val !== 'number' || Number.isNaN(val))
      throw new TypeError("Field 'deliveryMode' is the wrong type; must be a number (but not NaN)")
    flags += 4096
    buffer.writeUInt8(val, offset)
    offset++
  }
  val = fields.priority
  if (val !== undefined && val !== null) {
    if (typeof val !== 'number' || Number.isNaN(val))
      throw new TypeError("Field 'priority' is the wrong type; must be a number (but not NaN)")
    flags += 2048
    buffer.writeUInt8(val, offset)
    offset++
  }
  val = fields.correlationId
  if (val !== undefined && val !== null) {
    if (typeof val !== 'string')
      throw new TypeError(
        "Field 'correlationId' is the wrong type; must be a string (up to 255 chars)"
      )
    flags += 1024
    offset = writeShortString(buffer, val, offset)
    if (offset < 0)
      throw new TypeError(
        "Field 'correlationId' is the wrong type; must be a string (up to 255 chars)"
      )
  }
  val = fields.replyTo
  if (val !== undefined && val !== null) {
    if (typeof val !== 'string')
      throw new TypeError("Field 'replyTo' is the wrong type; must be a string (up to 255 chars)")
    flags += 512
    offset = writeShortString(buffer, val, offset)
    if (offset < 0)
      throw new TypeError("Field 'replyTo' is the wrong type; must be a string (up to 255 chars)")
  }
  val = fields.expiration
  if (val !== undefined && val !== null) {
    if (typeof val !== 'string')
      throw new TypeError(
        "Field 'expiration' is the wrong type; must be a string (up to 255 chars)"
      )
    flags += 256
    offset = writeShortString(buffer, val, offset)
    if (offset < 0)
      throw new TypeError(
        "Field 'expiration' is the wrong type; must be a string (up to 255 chars)"
      )
  }
  val = fields.messageId
  if (val !== undefined && val !== null) {
    if (typeof val !== 'string')
      throw new TypeError("Field 'messageId' is the wrong type; must be a string (up to 255 chars)")
    flags += 128
    offset = writeShortString(buffer, val, offset)
    if (offset < 0)
      throw new TypeError("Field 'messageId' is the wrong type; must be a string (up to 255 chars)")
  }
  val = fields.timestamp
  if (val !== undefined && val !== null) {
    if (typeof val !== 'number' || Number.isNaN(val))
      throw new TypeError("Field 'timestamp' is the wrong type; must be a number (but not NaN)")
    flags += 64
    writeUInt64(buffer, val, offset)
    offset += 8
  }
  val = fields.type
  if (val !== undefined && val !== null) {
    if (typeof val !== 'string')
      throw new TypeError("Field 'type' is the wrong type; must be a string (up to 255 chars)")
    flags += 32
    offset = writeShortString(buffer, val, offset)
    if (offset < 0)
      throw new TypeError("Field 'type' is the wrong type; must be a string (up to 255 chars)")
  }
  val = fields.userId
  if (val !== undefined && val !== null) {
    if (typeof val !== 'string')
      throw new TypeError("Field 'userId' is the wrong type; must be a string (up to 255 chars)")
    flags += 16
    offset = writeShortString(buffer, val, offset)
    if (offset < 0)
      throw new TypeError("Field 'userId' is the wrong type; must be a string (up to 255 chars)")
  }
  val = fields.appId
  if (val !== undefined && val !== null) {
    if (typeof val !== 'string')
      throw new TypeError("Field 'appId' is the wrong type; must be a string (up to 255 chars)")
    flags += 8
    offset = writeShortString(buffer, val, offset)
    if (offset < 0)
      throw new TypeError("Field 'appId' is the wrong type; must be a string (up to 255 chars)")
  }
  val = fields.clusterId
  if (val !== undefined && val !== null) {
    if (typeof val !== 'string')
      throw new TypeError("Field 'clusterId' is the wrong type; must be a string (up to 255 chars)")
    flags += 4
    offset = writeShortString(buffer, val, offset)
    if (offset < 0)
      throw new TypeError("Field 'clusterId' is the wrong type; must be a string (up to 255 chars)")
  }
  buffer[start + 19] = flags >>> 8
  buffer[start + 20] = flags
  buffer[offset] = 206
  writeSize(buffer, start, offset - start - 7)
  return offset + 1
}
export function decodeBasicProperties(buffer: Buffer, offset: number): BasicPropertiesFields {
  let end = 0
  const flags = buffer.readUInt16BE(offset)
  if (flags === 0) return {} as BasicPropertiesFields
  offset += 2
  const fields: BasicPropertiesFields = {
    contentType: undefined as any,
    contentEncoding: undefined as any,
    headers: undefined as any,
    deliveryMode: undefined as any,
    priority: undefined as any,
    correlationId: undefined as any,
    replyTo: undefined as any,
    expiration: undefined as any,
    messageId: undefined as any,
    timestamp: undefined as any,
    type: undefined as any,
    userId: undefined as any,
    appId: undefined as any,
    clusterId: undefined as any,
  }
  if ((flags & 32768) !== 0) {
    fields.contentType = readShortString(buffer, offset)
    offset += 1 + buffer[offset]!
  }
  if ((flags & 16384) !== 0) {
    fields.contentEncoding = readShortString(buffer, offset)
    offset += 1 + buffer[offset]!
  }
  if ((flags & 8192) !== 0) {
    end = offset + 4 + buffer.readUInt32BE(offset)
    fields.headers = decodeFields(buffer, offset + 4, end)
    offset = end
  }
  if ((flags & 4096) !== 0) {
    fields.deliveryMode = buffer.readUInt8(offset)
    offset++
  }
  if ((flags & 2048) !== 0) {
    fields.priority = buffer.readUInt8(offset)
    offset++
  }
  if ((flags & 1024) !== 0) {
    fields.correlationId = readShortString(buffer, offset)
    offset += 1 + buffer[offset]!
  }
  if ((flags & 512) !== 0) {
    fields.replyTo = readShortString(buffer, offset)
    offset += 1 + buffer[offset]!
  }
  if ((flags & 256) !== 0) {
    fields.expiration = readShortString(buffer, offset)
    offset += 1 + buffer[offset]!
  }
  if ((flags & 128) !== 0) {
    fields.messageId = readShortString(buffer, offset)
    offset += 1 + buffer[offset]!
  }
  if ((flags & 64) !== 0) {
    fields.timestamp = readUInt64(buffer, offset)
    offset += 8
  }
  if ((flags & 32) !== 0) {
    fields.type = readShortString(buffer, offset)
    offset += 1 + buffer[offset]!
  }
  if ((flags & 16) !== 0) {
    fields.userId = readShortString(buffer, offset)
    offset += 1 + buffer[offset]!
  }
  if ((flags & 8) !== 0) {
    fields.appId = readShortString(buffer, offset)
    offset += 1 + buffer[offset]!
  }
  if ((flags & 4) !== 0) {
    fields.clusterId = readShortString(buffer, offset)
    offset += 1 + buffer[offset]!
  }
  return fields
}
export const propertiesInfoBasicProperties: Info = {
  id: 60,
  name: 'BasicProperties',
  args: [
    { type: 'shortstr', name: 'contentType' },
    { type: 'shortstr', name: 'contentEncoding' },
    { type: 'table', name: 'headers' },
    { type: 'octet', name: 'deliveryMode' },
    { type: 'octet', name: 'priority' },
    { type: 'shortstr', name: 'correlationId' },
    { type: 'shortstr', name: 'replyTo' },
    { type: 'shortstr', name: 'expiration' },
    { type: 'shortstr', name: 'messageId' },
    { type: 'timestamp', name: 'timestamp' },
    { type: 'shortstr', name: 'type' },
    { type: 'shortstr', name: 'userId' },
    { type: 'shortstr', name: 'appId' },
    { type: 'shortstr', name: 'clusterId' },
  ],
}
