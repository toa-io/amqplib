// A CommonJS module's exports can be assigned to, which is how the tests shorten the heartbeat
// unit. The exports of an ES module cannot, so the assignment is passed on.

const heartbeat = require('../../src/heartbeat.ts')

module.exports = {
  Heart: heartbeat.Heart,

  get UNITS_TO_MS() {
    return heartbeat.units.ms
  },

  set UNITS_TO_MS(ms) {
    heartbeat.units.ms = ms
  },
}
