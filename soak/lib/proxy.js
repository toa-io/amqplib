const net = require('node:net');
const { mulberry32, between } = require('./random');

// A TCP proxy that forwards to the broker untouched in one direction and, in the other, hands
// the client what the broker sent in pieces of random length, many of them a few bytes long.
// A real socket delivers frames in large chunks that rarely split a header or leave a frame-end
// byte on its own; this makes those cuts happen constantly.
function startProxy({ port, host = '127.0.0.1', targetHost, targetPort, seed }) {
  const rng = mulberry32(seed);

  const server = net.createServer((client) => {
    const upstream = net.connect(targetPort, targetHost);
    client.setNoDelay(true);
    upstream.setNoDelay(true);

    client.pipe(upstream);

    upstream.on('data', async (chunk) => {
      upstream.pause();
      try {
        await fragment(client, chunk, rng);
      } catch {
        // the client went away mid-chunk; the close handlers tidy up
      }
      if (!upstream.destroyed) upstream.resume();
    });

    upstream.on('end', () => client.end());
    client.on('end', () => upstream.end());
    upstream.on('error', () => client.destroy());
    client.on('error', () => upstream.destroy());
    upstream.on('close', () => client.destroy());
    client.on('close', () => upstream.destroy());
  });

  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, () => resolve(server));
  });
}

async function fragment(socket, chunk, rng) {
  let offset = 0;
  while (offset < chunk.length) {
    const length = Math.min(chunk.length - offset, rng() < 0.3 ? between(rng, 1, 16) : between(rng, 17, 8192));
    const piece = chunk.subarray(offset, offset + length);
    offset += length;

    if (socket.destroyed) throw new Error('client closed');
    if (!socket.write(piece)) await drained(socket);

    // give the client a chance to read this piece before the next one lands
    if (rng() < 0.05) await new Promise((resolve) => setTimeout(resolve, 1));
    else await new Promise((resolve) => setImmediate(resolve));
  }
}

function drained(socket) {
  return new Promise((resolve, reject) => {
    const done = () => {
      socket.off('drain', done);
      socket.off('close', fail);
      resolve();
    };
    const fail = () => {
      socket.off('drain', done);
      socket.off('close', fail);
      reject(new Error('client closed'));
    };
    socket.once('drain', done);
    socket.once('close', fail);
  });
}

module.exports = { startProxy };
