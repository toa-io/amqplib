// A publisher process: one connection, a confirm channel per queue, and on each a stream of
// messages of random size whose headers carry what the consumer needs to check them.
const crypto = require('node:crypto');
const amqp = require('../amqplib');
const { Metrics } = require('./metrics');
const { mulberry32, between, sizeFor } = require('./random');
const { queueName, queueOptions, sha256, fatal, sleep } = require('./queues');

let running = true;
const published = {};
let messages = 0;
let bytes = 0;
let nacks = 0;

process.on('message', (m) => {
  if (m.type === 'start') start(m).catch(fatal);
  if (m.type === 'stop') running = false;
});

async function start({ config, transport, seed }) {
  const metrics = new Metrics();
  metrics.start();

  const connection = await amqp.connect(transport.url, transport.socketOptions);
  connection.on('error', fatal);

  // one pool of random bytes; every body is a window onto it
  const pool = crypto.randomBytes(config.maxSize + 1);
  const options = queueOptions(config);

  const pumps = [];
  for (let i = 0; i < config.channels; i++) {
    const queue = queueName(transport.name, i);
    const channel = await connection.createConfirmChannel();
    channel.on('error', fatal);
    await channel.assertQueue(queue, options);
    published[queue] = 0;
    pumps.push({ channel, queue, rng: mulberry32(seed + i) });
  }

  process.send({ type: 'ready' });

  const progress = setInterval(() => process.send({ type: 'progress', messages, bytes, nacks }), 1000);
  progress.unref();

  await Promise.all(pumps.map((p) => pump(p, pool, config)));

  clearInterval(progress);
  await connection.close();
  const snapshot = metrics.snapshot();
  metrics.stop();
  process.send({ type: 'done', published, messages, bytes, nacks, metrics: snapshot });
  process.exit(0);
}

async function pump({ channel, queue, rng }, pool, config) {
  let seq = 0;
  while (running) {
    const size = sizeFor(rng, config);
    const offset = between(rng, 0, pool.length - size);
    const content = pool.subarray(offset, offset + size);
    const headers = { seq: ++seq, size, sha256: sha256(content) };

    // one message in flight per queue, retried until the broker takes it, so sequence
    // numbers arrive in order and none is lost when a queue is full
    while (!(await publish(channel, queue, content, headers))) {
      nacks++;
      await sleep(20);
    }

    published[queue] = seq;
    messages++;
    bytes += size;
  }
}

function publish(channel, queue, content, headers) {
  return new Promise((resolve) => {
    channel.sendToQueue(
      queue,
      content,
      { headers, persistent: false, contentType: 'application/octet-stream' },
      (err) => resolve(!err),
    );
  });
}
