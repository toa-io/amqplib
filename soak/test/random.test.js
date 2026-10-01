const { describe, it } = require('node:test');
const assert = require('node:assert');
const { mulberry32, sizeFor, KB, MB } = require('../lib/random');

describe('sizes', () => {
  const opts = { maxSize: 8 * MB, frameMax: 131072 };

  it('is repeatable from a seed', () => {
    const a = mulberry32(42);
    const b = mulberry32(42);
    for (let i = 0; i < 1000; i++) assert.strictEqual(sizeFor(a, opts), sizeFor(b, opts));
  });

  it('stays within bounds and covers every class', () => {
    const rng = mulberry32(7);
    const seen = { zero: 0, frame: 0, boundary: 0, large: 0 };
    for (let i = 0; i < 20000; i++) {
      const size = sizeFor(rng, opts);
      assert.ok(Number.isInteger(size) && size >= 0 && size <= opts.maxSize, `size ${size}`);
      if (size === 0) seen.zero++;
      if (size > 0 && size <= KB) seen.frame++;
      if (Math.abs((size % (opts.frameMax - 8)) - 0) <= 3 && size > KB) seen.boundary++;
      if (size >= MB) seen.large++;
    }
    for (const [k, n] of Object.entries(seen)) assert.ok(n > 50, `${k} seen ${n} times`);
  });

  it('respects a small maximum', () => {
    const rng = mulberry32(3);
    for (let i = 0; i < 5000; i++) assert.ok(sizeFor(rng, { maxSize: 100, frameMax: 4096 }) <= 100);
  });
});
