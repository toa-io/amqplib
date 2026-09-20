// Strings on the wire, and the frame size.
//
// The strings of a protocol repeat: a consumer tag comes with every delivery, and exchanges,
// routing keys, content types and header names are few. Decoding one makes a new string every
// time, which is most of what taking a small message in costs, so a string that was seen before
// is found by its bytes instead. It is then also the same object, which a lookup by it (the
// consumer by its tag) gets to benefit from.

/** Longer strings are rare enough to repeat that looking them up is not worth it. */
const LONGEST = 64

const SLOTS = 1024

const seen: (string | undefined)[] = Array.from({ length: SLOTS })

/** Decodes the UTF-8 string of `length` bytes at `start`. */
export function readString(buffer: Buffer, start: number, length: number): string {
  if (length === 0) return ''

  const end = start + length

  if (length > LONGEST) return buffer.utf8Slice(start, end)

  // a few bytes are enough to tell most strings apart, and the rest is compared anyway
  const hash =
    Math.imul(length, 31) +
    Math.imul(buffer[start]!, 7) +
    Math.imul(buffer[end - 1]!, 13) +
    Math.imul(buffer[start + (length >> 1)]!, 17) +
    buffer[start + (length >> 2)]!

  const slot = hash & (SLOTS - 1)
  const known = seen[slot]

  if (known !== undefined && known.length === length) {
    let i = 0

    while (i < length && known.charCodeAt(i) === buffer[start + i]) i++

    if (i === length) return known
  }

  // anything but ASCII takes more than a byte per character: nothing to compare bytes with
  for (let i = start; i < end; i++) if (buffer[i]! > 127) return buffer.utf8Slice(start, end)

  return (seen[slot] = buffer.latin1Slice(start, end))
}

/** Reads a string prefixed with its length in one byte. */
export function readShortString(buffer: Buffer, offset: number): string {
  return readString(buffer, offset + 1, buffer[offset]!)
}

/**
 * Writes a string as UTF-8 and returns how many bytes it took. The room for it has to be there:
 * three bytes per character at most.
 */
export function writeString(buffer: Buffer, value: string, offset: number): number {
  const { length } = value

  if (length > LONGEST) return buffer.utf8Write(value, offset)

  for (let i = 0; i < length; i++) {
    const code = value.charCodeAt(i)

    // not ASCII after all: start over the long way
    if (code > 127) return buffer.utf8Write(value, offset)

    buffer[offset + i] = code
  }

  return length
}

/**
 * Writes a string prefixed with its length in one byte and returns the offset after it, or -1
 * if the string takes more than 255 bytes.
 */
export function writeShortString(buffer: Buffer, value: string, offset: number): number {
  const { length } = value

  if (length > 255) return -1

  for (let i = 0; i < length; i++) {
    const code = value.charCodeAt(i)

    if (code > 127) return writeWideShortString(buffer, value, offset)

    buffer[offset + 1 + i] = code
  }

  buffer[offset] = length

  return offset + 1 + length
}

function writeWideShortString(buffer: Buffer, value: string, offset: number): number {
  const length = Buffer.byteLength(value, 'utf8')

  if (length > 255) return -1

  buffer[offset] = length
  buffer.utf8Write(value, offset + 1, length)

  return offset + 1 + length
}

/** Writes the payload size into the header of the frame that starts at `start`. */
export function writeSize(buffer: Buffer, start: number, size: number): void {
  buffer[start + 3] = size >>> 24
  buffer[start + 4] = size >>> 16
  buffer[start + 5] = size >>> 8
  buffer[start + 6] = size
}
