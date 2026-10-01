// A consumer process: one connection, a channel per queue, and a check on every delivery that
// the body is the length and bytes the publisher sent, in the order it sent them. Each body is
// then kept for a while and checked again, since a body that is a view of a socket buffer could
// be right on arrival and wrong once that buffer is reused.
const amqp = require('amqplib');
const { Metrics } = require('./metrics');
const { queueName, queueOptions, sha256, fatal, sleep } = require('./queues');

const received = {};
const next = {};
let messages = 0;
let bytes = 0;
let reverified = 0;
let failures = 0;

let expected = null;
let onProgress = () => {};

const held = [];
let heldBytes = 0;
let holdMs = 500;
let holdBytes = 128 * 1024 * 1024;
let transportName = '';

process.on('message', (m) => {
  if (m.type === 'start') start(m).catch(fatal);
  if (m.type === 'expect') {
    expected = m.published;
    // whatever has not arrived by the deadline is missing, and the run ends without it
    setTimeout(() => {
      if (complete()) return;
      for (const queue of Object.keys(expected)) {
        const got = received[queue] || 0;
        if (got < expected[queue])
          fail('missing', queue, got + 1, `${expected[queue] - got} of ${expected[queue]} never arrived`);
        expected[queue] = got;
      }
      onProgress();
    }, m.graceMs).unref();
    onProgress();
  }
});

async function start({ config, transport }) {
  transportName = transport.name;
  holdMs = config.holdMs;
  holdBytes = config.holdBytes;

  const metrics = new Metrics();
  metrics.start();

  const connection = await amqp.connect(transport.url, transport.socketOptions);
  connection.on('error', fatal);

  const options = queueOptions(config);
  const channels = [];
  for (let i = 0; i < config.channels; i++) {
    const queue = queueName(transport.name, i);
    const channel = await connection.createChannel();
    channel.on('error', fatal);
    await channel.assertQueue(queue, options);
    await channel.purgeQueue(queue);
    await channel.prefetch(config.prefetch);
    received[queue] = 0;
    next[queue] = 1;
    const { consumerTag } = await channel.consume(queue, (msg) => deliver(channel, queue, msg), { noAck: false });
    channels.push({ channel, queue, consumerTag });
  }

  process.send({ type: 'ready' });

  const progress = setInterval(() => process.send({ type: 'progress', messages, bytes, failures }), 1000);
  progress.unref();
  const releaser = setInterval(releaseStale, 50);
  releaser.unref();

  await new Promise((resolve) => {
    onProgress = () => {
      if (expected && complete()) resolve();
    };
  });

  clearInterval(progress);
  for (const { channel, consumerTag } of channels) await channel.cancel(consumerTag);

  // let the last deliveries sit for the hold period before their final check
  await sleep(holdMs + 100);
  clearInterval(releaser);
  while (held.length > 0) release(held.shift());

  for (const { channel, queue } of channels) await channel.deleteQueue(queue);
  await connection.close();

  const snapshot = metrics.snapshot();
  metrics.stop();
  process.send({ type: 'done', received, messages, bytes, reverified, failures, metrics: snapshot });
  process.exit(0);
}

function complete() {
  for (const queue of Object.keys(expected)) {
    if ((received[queue] || 0) < expected[queue]) return false;
  }
  return true;
}

function deliver(channel, queue, msg) {
  if (msg === null) return;

  const content = msg.content;
  const headers = msg.properties.headers || {};
  const { seq, size, sha256: digest } = headers;

  if (!Buffer.isBuffer(content)) {
    fail('type', queue, seq, `content is ${Object.prototype.toString.call(content)}, not a Buffer`);
  } else {
    if (content.length !== size) fail('size', queue, seq, `expected ${size} bytes, got ${content.length}`);
    const actual = sha256(content);
    if (actual !== digest) fail('hash', queue, seq, `expected ${digest}, got ${actual}`);
    // held against the hash it arrived with, so this check reports only bytes that change afterwards
    hold({ queue, seq, content, digest: actual, size: content.length, at: Date.now() });
  }

  if (seq !== next[queue]) fail('order', queue, seq, `expected seq ${next[queue]}, got ${seq}`);
  next[queue] = seq + 1;

  received[queue]++;
  messages++;
  bytes += size;
  channel.ack(msg);
  onProgress();
}

function hold(item) {
  held.push(item);
  heldBytes += item.size;
  while (heldBytes > holdBytes && held.length > 0) release(held.shift());
}

function releaseStale() {
  const cutoff = Date.now() - holdMs;
  while (held.length > 0 && held[0].at <= cutoff) release(held.shift());
}

function release(item) {
  heldBytes -= item.size;
  reverified++;
  if (item.content.length !== item.size) {
    fail('retention', item.queue, item.seq, `length changed from ${item.size} to ${item.content.length} while held`);
    return;
  }
  const actual = sha256(item.content);
  if (actual !== item.digest) {
    fail('retention', item.queue, item.seq, `content changed while held: expected ${item.digest}, got ${actual}`);
  }
}

function fail(kind, queue, seq, detail) {
  failures++;
  process.send({ type: 'failure', failure: { kind, transport: transportName, queue, seq, detail } });
}
