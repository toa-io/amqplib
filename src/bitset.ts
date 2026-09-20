// Which channel numbers are taken. The lowest free one is what gets allocated, which keeps the
// connection's array of channels short and dense.

export class BitSet {
  private words: Int32Array

  public constructor(size = 64) {
    this.words = new Int32Array(Math.ceil(size / 32))
  }

  public set(index: number): void {
    const word = index >>> 5

    if (word >= this.words.length) {
      const words = new Int32Array(Math.max(word + 1, this.words.length * 2))

      words.set(this.words)
      this.words = words
    }

    this.words[word]! |= 1 << index
  }

  public clear(index: number): void {
    const word = index >>> 5

    if (word < this.words.length) this.words[word]! &= ~(1 << index)
  }

  public get(index: number): boolean {
    const word = index >>> 5

    return word < this.words.length && (this.words[word]! & (1 << index)) !== 0
  }

  /** The first set bit at or after the index, or -1 if there is none. */
  public nextSetBit(from: number): number {
    let w = from >>> 5

    if (w >= this.words.length) return -1

    let word = this.words[w]! & (-1 << from)

    while (word === 0) {
      if (++w === this.words.length) return -1

      word = this.words[w]!
    }

    return w * 32 + trailingZeros(word)
  }

  /** The first clear bit at or after the index. */
  public nextClearBit(from: number): number {
    let w = from >>> 5

    if (w >= this.words.length) return from

    let word = ~this.words[w]! & (-1 << from)

    while (word === 0) {
      if (++w === this.words.length) return w * 32

      word = ~this.words[w]!
    }

    return w * 32 + trailingZeros(word)
  }
}

function trailingZeros(word: number): number {
  return 31 - Math.clz32(word & -word)
}
