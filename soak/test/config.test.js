const { describe, it } = require('node:test');
const assert = require('node:assert');
const { configFrom, parseDuration, parseBytes } = require('../lib/config');

describe('config', () => {
  it('reads durations', () => {
    assert.strictEqual(parseDuration('30s'), 30_000);
    assert.strictEqual(parseDuration('2m'), 120_000);
    assert.strictEqual(parseDuration('250ms'), 250);
    assert.strictEqual(parseDuration('5'), 5000);
    assert.throws(() => parseDuration('soon'));
  });

  it('reads sizes', () => {
    assert.strictEqual(parseBytes('512KB'), 512 * 1024);
    assert.strictEqual(parseBytes('8MB'), 8 * 1024 * 1024);
    assert.strictEqual(parseBytes('100'), 100);
    assert.throws(() => parseBytes('lots'));
  });

  it('applies defaults and overrides', () => {
    const config = configFrom(['--duration', '10s', '--transports', 'plain', '--seed', '99']);
    assert.strictEqual(config.durationMs, 10_000);
    assert.deepStrictEqual(config.transports, ['plain']);
    assert.strictEqual(config.seed, 99);
    assert.strictEqual(config.channels, 8);
    assert.strictEqual(config.frameMax, 131072);
  });

  it('rejects a frame size the broker would refuse', () => {
    assert.throws(() => configFrom(['--frame-max', '4096']), /at least 8192/);
  });
});
