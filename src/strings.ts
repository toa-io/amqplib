// Short strings and the frame size: the few bytes every frame is made of.

/** Reads a string prefixed with its length in one byte. */
export function readShortString(buffer: Buffer, offset: number): string {
  return buffer.toString('utf8', offset + 1, offset + 1 + buffer[offset]!)
}

/**
 * Writes a string prefixed with its length in one byte and returns the offset after it, or -1
 * if the string takes more than 255 bytes.
 */
export function writeShortString(buffer: Buffer, value: string, offset: number): number {
  const length = Buffer.byteLength(value, 'utf8')

  if (length > 255) return -1

  buffer[offset] = length
  buffer.write(value, offset + 1, 'utf8')

  return offset + 1 + length
}

/** Writes the payload size into the header of the frame that starts at `start`. */
export function writeSize(buffer: Buffer, start: number, size: number): void {
  buffer[start + 3] = size >>> 24
  buffer[start + 4] = size >>> 16
  buffer[start + 5] = size >>> 8
  buffer[start + 6] = size
}
