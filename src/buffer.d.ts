// The slices and writes `Buffer.toString` and `Buffer.write` come down to once they have worked
// out the encoding. They are part of every Buffer, but not of its declared type.

declare global {
  // oxlint-disable-next-line no-unused-vars -- has to match the declaration it adds to
  interface Buffer<TArrayBuffer extends ArrayBufferLike = ArrayBufferLike> {
    utf8Slice(start: number, end: number): string
    latin1Slice(start: number, end: number): string
    utf8Write(value: string, offset?: number, length?: number): number
  }
}

export {}
