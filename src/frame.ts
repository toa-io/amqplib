// What a frame is made of.
//
//   0      1         3             7                size+7 size+8
//   +------+---------+-------------+ +------------+ +-----------+
//   | type | channel | size        | | payload    | | frame-end |
//   +------+---------+-------------+ +------------+ +-----------+
//    octet   short     long            size octets    octet

import type { Table } from './codec.ts'

export const FRAME_METHOD = 1
export const FRAME_HEADER = 2
export const FRAME_BODY = 3
export const FRAME_HEARTBEAT = 8
export const FRAME_END = 206

/** The bytes before the payload: type, channel and size. */
export const FRAME_PREFIX = 7

// "AMQP", then 0, 0, 9, 1
export const PROTOCOL_HEADER = Buffer.from([65, 77, 81, 80, 0, 0, 9, 1])

export const HEARTBEAT_BUF = Buffer.from([FRAME_HEARTBEAT, 0, 0, 0, 0, 0, 0, FRAME_END])

/** A frame read by hand: `Connection.recvFrame` and `Connection.step`. */
export interface Frame {
  channel: number
  id?: number
  size?: number
  fields?: Table
  content?: Buffer
}

/** The one heartbeat frame there is. */
export const HEARTBEAT: Frame = { channel: 0 }
