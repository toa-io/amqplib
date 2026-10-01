const { describe, it, after } = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const crypto = require('node:crypto');
const { startProxy } = require('../lib/proxy');

describe('fragmenting proxy', () => {
  const servers = [];
  after(() => {
    for (const s of servers) s.close();
  });

  it('delivers every byte, in order, in more pieces than it received', async () => {
    const echo = net.createServer((socket) => socket.pipe(socket));
    await new Promise((resolve) => echo.listen(0, '127.0.0.1', resolve));
    servers.push(echo);

    const proxy = await startProxy({ port: 0, targetHost: '127.0.0.1', targetPort: echo.address().port, seed: 1 });
    servers.push(proxy);

    const sent = crypto.randomBytes(300 * 1024);
    const client = net.connect(proxy.address().port, '127.0.0.1');
    await new Promise((resolve) => client.once('connect', resolve));

    const pieces = [];
    client.on('data', (chunk) => pieces.push(Buffer.from(chunk)));
    client.write(sent);

    await new Promise((resolve, reject) => {
      const check = () => {
        if (Buffer.concat(pieces).length >= sent.length) resolve();
      };
      client.on('data', check);
      client.on('error', reject);
      setTimeout(
        () => reject(new Error(`only ${Buffer.concat(pieces).length} of ${sent.length} bytes arrived`)),
        10_000,
      );
    });
    client.destroy();

    const got = Buffer.concat(pieces);
    assert.strictEqual(got.length, sent.length);
    assert.ok(got.equals(sent), 'bytes differ');
    assert.ok(pieces.length > 20, `arrived in only ${pieces.length} pieces`);
  });
});
