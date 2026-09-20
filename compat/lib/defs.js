// The original encoders return a frame as a buffer of its own; these write at an offset of a
// buffer they are given. A frame is cut out of a scratch buffer here to give the tests the former.

const defs = require('../../src/defs.ts')
const { OVERFLOW } = require('../../src/codec.ts')

function frame(encode) {
  for (let size = 65536; ; size *= 2) {
    const scratch = Buffer.allocUnsafe(size)

    try {
      return scratch.subarray(0, encode(scratch))
    } catch (error) {
      if (error !== OVERFLOW) throw error
    }
  }
}

module.exports = {
  ...defs,
  decode: (id, buffer) => defs.decode(id, buffer, 0),
  encodeMethod: (id, channel, fields) => frame(b => defs.encodeMethod(id, b, 0, channel, fields)),
  encodeProperties: (id, channel, size, fields) =>
    frame(b => defs.encodeProperties(id, b, 0, channel, size, fields)),
}
