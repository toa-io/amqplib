// Field tables: the tagged key-value encoding used for method arguments and message headers,
// with the type tags RabbitMQ uses (https://www.rabbitmq.com/amqp-0-9-1-errata.html).
//
// Everything reads and writes at an offset of a buffer it is given. Nothing is sliced on the way
// in and nothing is allocated on the way out, apart from the values themselves.

import { readString, writeString } from './strings.ts'

/**
 * Thrown by an encoder that ran out of buffer. The one who owns the buffer catches it and
 * tries again with a larger one.
 */
export const OVERFLOW = new RangeError('The buffer is too small for the value being encoded')

export type Table = Record<string, unknown>

const TWO_32 = 0x100000000

function isFloatingPoint(n: number): boolean {
  return n >= 0x8000000000000000 || (Math.abs(n) < 0x4000000000000 && Math.floor(n) !== n)
}

/** Writes the table with its length prefix and returns how many bytes that took. */
export function encodeTable(buffer: Buffer, table: Table, offset: number): number {
  const start = offset

  offset += 4

  for (const key in table) {
    const value = table[key]

    if (value === undefined) continue

    // three bytes per character is the most UTF-8 takes of a string
    if (
      offset + 1 + 3 * key.length > buffer.length &&
      offset + 1 + Buffer.byteLength(key) > buffer.length
    )
      throw OVERFLOW

    const length = writeString(buffer, key, offset + 1)

    buffer.writeUInt8(length, offset)
    offset = encodeValue(buffer, value, offset + 1 + length)
  }

  if (offset > buffer.length) throw OVERFLOW

  buffer.writeUInt32BE(offset - start - 4, start)

  return offset - start
}

function encodeArray(buffer: Buffer, values: unknown[], offset: number): number {
  const start = offset

  offset += 4

  for (let i = 0; i < values.length; i++) offset = encodeValue(buffer, values[i], offset)

  if (offset > buffer.length) throw OVERFLOW

  buffer.writeUInt32BE(offset - start - 4, start)

  return offset
}

// The widest fixed-size value: a tag and eight bytes.
const FIXED = 9

/** Writes one tagged value and returns the offset after it. */
function encodeValue(buffer: Buffer, value: unknown, offset: number): number {
  let type: string = typeof value
  let val: any = value

  // an explicitly typed value: { '!': 'int32', value: 50 }
  if (value !== null && type === 'object' && Object.hasOwn(value as object, '!')) {
    val = (value as { value: unknown }).value
    type = (value as { '!': string })['!']
  }

  if (type === 'number')
    if (isFloatingPoint(val)) type = 'double'
    else if (val < 128 && val >= -128) type = 'byte'
    else if (val >= -0x8000 && val < 0x8000) type = 'short'
    else if (val >= -0x80000000 && val < 0x80000000) type = 'int'
    else type = 'long'

  if (offset + FIXED > buffer.length) throw OVERFLOW

  switch (type) {
    case 'string': {
      if (
        offset + 5 + 3 * val.length > buffer.length &&
        offset + 5 + Buffer.byteLength(val, 'utf8') > buffer.length
      )
        throw OVERFLOW

      const length = writeString(buffer, val, offset + 5)

      buffer[offset] = 83 // S
      buffer.writeUInt32BE(length, offset + 1)

      return offset + 5 + length
    }

    case 'object':
      if (val === null) {
        buffer[offset] = 86 // V

        return offset + 1
      }

      if (Array.isArray(val)) {
        buffer[offset] = 65 // A

        return encodeArray(buffer, val, offset + 1)
      }

      if (Buffer.isBuffer(val)) {
        if (offset + 5 + val.length > buffer.length) throw OVERFLOW

        buffer[offset] = 120 // x
        buffer.writeUInt32BE(val.length, offset + 1)
        val.copy(buffer, offset + 5)

        return offset + 5 + val.length
      }

      buffer[offset] = 70 // F

      return offset + 1 + encodeTable(buffer, val, offset + 1)

    case 'boolean':
      buffer[offset] = 116 // t
      buffer[offset + 1] = val ? 1 : 0

      return offset + 2

    case 'double':
    case 'float64':
      buffer[offset] = 100 // d
      buffer.writeDoubleBE(val, offset + 1)

      return offset + 9

    case 'byte':
    case 'int8':
      buffer[offset] = 98 // b
      buffer.writeInt8(val, offset + 1)

      return offset + 2

    case 'unsignedbyte':
    case 'uint8':
      buffer[offset] = 66 // B
      buffer.writeUInt8(val, offset + 1)

      return offset + 2

    case 'short':
    case 'int16':
      buffer[offset] = 115 // s
      buffer.writeInt16BE(val, offset + 1)

      return offset + 3

    case 'unsignedshort':
    case 'uint16':
      buffer[offset] = 117 // u
      buffer.writeUInt16BE(val, offset + 1)

      return offset + 3

    case 'int':
    case 'int32':
      buffer[offset] = 73 // I
      buffer.writeInt32BE(val, offset + 1)

      return offset + 5

    case 'unsignedint':
    case 'uint32':
      buffer[offset] = 105 // i
      buffer.writeUInt32BE(val, offset + 1)

      return offset + 5

    case 'long':
    case 'int64':
      buffer[offset] = 108 // l
      buffer.writeBigInt64BE(BigInt(val), offset + 1)

      return offset + 9

    case 'timestamp':
      buffer[offset] = 84 // T
      writeUInt64(buffer, val, offset + 1)

      return offset + 9

    case 'float':
      buffer[offset] = 102 // f
      buffer.writeFloatBE(val, offset + 1)

      return offset + 5

    case 'decimal':
      if (
        Object.hasOwn(val, 'places') &&
        Object.hasOwn(val, 'digits') &&
        val.places >= 0 &&
        val.places < 256
      ) {
        buffer[offset] = 68 // D
        buffer[offset + 1] = val.places
        buffer.writeUInt32BE(val.digits, offset + 2)

        return offset + 6
      }

      throw new TypeError(
        `Decimal value must be {'places': 0..255, 'digits': uint32}, got ${JSON.stringify(val)}`
      )

    default:
      throw new TypeError(`Unknown type to encode: ${type}`)
  }
}

/**
 * Writes an unsigned 64-bit integer. What fits a double exactly goes out as two halves; anything
 * else takes the BigInt route, which is also what rejects a fraction or a negative.
 */
export function writeUInt64(buffer: Buffer, value: number, offset: number): void {
  if (Number.isSafeInteger(value) && value >= 0) {
    buffer.writeUInt32BE(Math.floor(value / TWO_32), offset)
    buffer.writeUInt32BE(value >>> 0, offset + 4)
  } else buffer.writeBigUInt64BE(BigInt(value), offset)
}

/** Reads an unsigned 64-bit integer as the nearest double. */
export function readUInt64(buffer: Buffer, offset: number): number {
  return buffer.readUInt32BE(offset) * TWO_32 + buffer.readUInt32BE(offset + 4)
}

// Where the value just decoded ended: a second result of `decodeValue`, read straight after it.
let cursor = 0

/** Decodes the key-value pairs between two offsets, without the table's length prefix. */
export function decodeFields(buffer: Buffer, start = 0, end = buffer.length): Table {
  const fields: Table = {}

  let offset = start

  while (offset < end) {
    const length = buffer[offset]!
    const key = readString(buffer, offset + 1, length)

    fields[key] = decodeValue(buffer, offset + 1 + length)
    offset = cursor
  }

  return fields
}

function decodeArray(buffer: Buffer, start: number, end: number): unknown[] {
  const values: unknown[] = []

  let offset = start

  while (offset < end) {
    values.push(decodeValue(buffer, offset))
    offset = cursor
  }

  return values
}

function decodeValue(buffer: Buffer, offset: number): unknown {
  const tag = buffer[offset]

  offset++

  switch (tag) {
    case 98: // b
      cursor = offset + 1

      return buffer.readInt8(offset)

    case 66: // B
      cursor = offset + 1

      return buffer.readUInt8(offset)

    case 83: {
      // S
      const length = buffer.readUInt32BE(offset)

      cursor = offset + 4 + length

      return readString(buffer, offset + 4, length)
    }

    case 73: // I
      cursor = offset + 4

      return buffer.readInt32BE(offset)

    case 105: // i
      cursor = offset + 4

      return buffer.readUInt32BE(offset)

    case 68: // D
      cursor = offset + 5

      return {
        '!': 'decimal',
        value: { places: buffer.readUInt8(offset), digits: buffer.readUInt32BE(offset + 1) },
      }

    case 84: // T
      cursor = offset + 8

      return { '!': 'timestamp', value: readUInt64(buffer, offset) }

    case 70: {
      // F
      const end = offset + 4 + buffer.readUInt32BE(offset)
      const value = decodeFields(buffer, offset + 4, end)

      cursor = end

      return value
    }

    case 65: {
      // A
      const end = offset + 4 + buffer.readUInt32BE(offset)
      const value = decodeArray(buffer, offset + 4, end)

      cursor = end

      return value
    }

    case 100: // d
      cursor = offset + 8

      return buffer.readDoubleBE(offset)

    case 102: // f
      cursor = offset + 4

      return buffer.readFloatBE(offset)

    case 108: // l
      cursor = offset + 8

      return Number(buffer.readBigInt64BE(offset))

    case 115: // s
      cursor = offset + 2

      return buffer.readInt16BE(offset)

    case 117: // u
      cursor = offset + 2

      return buffer.readUInt16BE(offset)

    case 116: // t
      cursor = offset + 1

      return buffer.readUInt8(offset) !== 0

    case 86: // V
      cursor = offset

      return null

    case 120: {
      // x
      const end = offset + 4 + buffer.readUInt32BE(offset)

      cursor = end

      return bytes(buffer, offset + 4, end)
    }

    default:
      throw new TypeError(`Unexpected type tag "${String.fromCharCode(tag ?? 0)}"`)
  }
}

/**
 * A copy of a stretch of bytes. What is decoded from is a read buffer that the next read
 * overwrites, so a value that outlives the call cannot be a view of it.
 */
export function bytes(buffer: Buffer, start: number, end: number): Buffer {
  if (end > buffer.length) throw new RangeError('Attempt to read beyond the end of the frame')

  const copy = Buffer.allocUnsafe(end - start)

  buffer.copy(copy, 0, start, end)

  return copy
}
