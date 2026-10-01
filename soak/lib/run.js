const path = require('node:path');
const { EventEmitter } = require('node:events');
const { fork } = require('node:child_process');
const { transportsFor } = require('./transports');
const { startProxy } = require('./proxy');
const { amqplibVersion } = require('./version');

const READY_TIMEOUT_MS = 30_000;
const STOP_TIMEOUT_MS = 60_000;
const DRAIN_GRACE_MS = 120_000;
const MAX_FAILURES_KEPT = 1000;

class Worker extends EventEmitter {
  constructor(role, transport, config, seed) {
    super();
    this.role = role;
    this.transport = transport;
    this.progress = { messages: 0, bytes: 0, nacks: 0, failures: 0 };
    this.exited = null;
    this.child = fork(path.join(__dirname, `${role}.js`), [], { serialization: 'json' });
    this.child.on('message', (m) => {
      if (m.type === 'progress') this.progress = m;
      this.emit(m.type, m);
    });
    this.child.on('exit', (code, signal) => {
      this.exited = { code, signal };
      this.emit('exit', this.exited);
    });
    this.child.send({ type: 'start', config, transport, seed });
  }

  get label() {
    return `${this.transport.name} ${this.role}`;
  }

  send(message) {
    if (!this.exited) this.child.send(message);
  }

  // Resolves with the next message of the given type; rejects if the worker reports an error,
  // exits first, or the timeout passes.
  expect(type, timeoutMs) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => fail(new Error(`${this.label}: no '${type}' within ${timeoutMs / 1000}s`)),
        timeoutMs,
      );
      const done = (m) => {
        cleanup();
        resolve(m);
      };
      const errored = (m) => fail(new Error(`${this.label}: ${m.message}${m.stack ? `\n${m.stack}` : ''}`));
      const exited = ({ code, signal }) =>
        fail(new Error(`${this.label}: exited with ${signal || `code ${code}`} before '${type}'`));
      const fail = (error) => {
        cleanup();
        reject(error);
      };
      const cleanup = () => {
        clearTimeout(timer);
        this.off(type, done);
        this.off('error', errored);
        this.off('exit', exited);
      };
      this.on(type, done);
      this.on('error', errored);
      this.on('exit', exited);
      if (this.exited) exited(this.exited);
    });
  }

  kill() {
    if (!this.exited) this.child.kill('SIGKILL');
  }
}

async function run(config, out = process.stdout) {
  const startedAt = new Date();
  const amqplib = amqplibVersion();
  const transports = transportsFor(config);
  const failures = [];
  const workers = [];
  let proxy = null;
  let infrastructureError = null;

  const log = (line) => out.write(`${line}\n`);
  const abort = (error) => {
    if (!infrastructureError) infrastructureError = error;
  };

  log(`amqplib ${amqplib.version} from ${amqplib.resolved || amqplib.dir}`);
  log(
    `node ${process.version}, seed ${config.seed}, ${config.transports.join('+')}, ${config.channels} channels each, ` +
      `bodies up to ${config.maxSize} bytes, frameMax ${config.frameMax}, heartbeat ${config.heartbeat}s, ` +
      `${config.durationMs / 1000}s`,
  );

  const onFailure = ({ failure }) => {
    if (failures.length < MAX_FAILURES_KEPT) failures.push(failure);
    if (config.verbose)
      log(`  FAIL ${failure.transport} ${failure.queue} seq ${failure.seq}: ${failure.kind}: ${failure.detail}`);
  };
  const onError = (worker) => (m) => abort(new Error(`${worker.label}: ${m.message}${m.stack ? `\n${m.stack}` : ''}`));

  const spawn = (role, transport, seed) => {
    const worker = new Worker(role, transport, config, seed);
    worker.on('failure', onFailure);
    worker.on('error', onError(worker));
    workers.push(worker);
    return worker;
  };

  const ticker = setInterval(() => {
    for (const transport of transports) {
      const of = (role) => workers.find((w) => w.role === role && w.transport.name === transport.name);
      const pub = of('publisher')?.progress || {};
      const con = of('consumer')?.progress || {};
      log(
        `  ${transport.name.padEnd(8)} published ${String(pub.messages || 0).padStart(7)} (${mb(pub.bytes)} MB, ${pub.nacks || 0} refused)` +
          `  consumed ${String(con.messages || 0).padStart(7)} (${mb(con.bytes)} MB)  failures ${failures.length}`,
      );
    }
  }, 5000);

  try {
    if (config.transports.includes('chunked')) {
      proxy = await startProxy({
        port: config.proxyPort,
        targetHost: config.host,
        targetPort: config.plainPort,
        seed: config.seed ^ 0x5eed,
      });
    }

    const consumers = transports.map((t, i) => spawn('consumer', t, config.seed + 1000 * (i + 1)));
    await Promise.all(consumers.map((w) => w.expect('ready', READY_TIMEOUT_MS)));

    const publishers = transports.map((t, i) => spawn('publisher', t, config.seed + 1000 * (i + 1)));
    await Promise.all(publishers.map((w) => w.expect('ready', READY_TIMEOUT_MS)));
    log('all workers connected; publishing');

    await raceWithAbort(sleep(config.durationMs), () => infrastructureError);

    log('duration reached; stopping publishers');
    for (const w of publishers) w.send({ type: 'stop' });
    const published = await Promise.all(publishers.map((w) => w.expect('done', STOP_TIMEOUT_MS)));

    log('publishers stopped; waiting for consumers to drain');
    consumers.forEach((w, i) => {
      w.send({ type: 'expect', published: published[i].published, graceMs: DRAIN_GRACE_MS });
    });
    const consumed = await Promise.all(consumers.map((w) => w.expect('done', DRAIN_GRACE_MS + 30_000)));

    clearInterval(ticker);
    return report({ startedAt, config, amqplib, transports, publishers, published, consumers, consumed, failures });
  } catch (error) {
    clearInterval(ticker);
    abort(error);
    return report({ startedAt, config, amqplib, transports, failures, infrastructureError });
  } finally {
    clearInterval(ticker);
    for (const w of workers) w.kill();
    if (proxy) proxy.close();
  }
}

function report({
  startedAt,
  config,
  amqplib,
  transports,
  publishers = [],
  published = [],
  consumers = [],
  consumed = [],
  failures,
  infrastructureError = null,
}) {
  const rows = [];
  transports.forEach((t, i) => {
    if (published[i]) rows.push(row(t.name, 'publisher', published[i], publishers[i]));
    if (consumed[i]) rows.push(row(t.name, 'consumer', consumed[i], consumers[i]));
  });
  const verdict = infrastructureError ? 'error' : failures.length > 0 ? 'fail' : 'pass';
  return {
    verdict,
    exitCode: verdict === 'pass' ? 0 : verdict === 'fail' ? 1 : 2,
    startedAt: startedAt.toISOString(),
    node: process.version,
    amqplib,
    config,
    rows,
    failures,
    error: infrastructureError ? infrastructureError.message : null,
  };
}

function row(transport, role, done, worker) {
  const m = done.metrics;
  return {
    transport,
    role,
    messages: done.messages,
    bytes: done.bytes,
    refused: done.nacks || 0,
    reverified: done.reverified || 0,
    elapsedMs: m.elapsedMs,
    cpuMs: m.cpuMs,
    gcCount: m.gcCount,
    gcMs: m.gcMs,
    peakRss: m.peakRss,
    peakHeap: m.peakHeap,
    pid: worker.child.pid,
  };
}

function mb(bytes) {
  return ((bytes || 0) / 1048576).toFixed(1);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Waits on a promise, but gives up early once an infrastructure error has been recorded
async function raceWithAbort(promise, errored) {
  let settled = false;
  const poll = (async () => {
    while (!settled) {
      const error = errored();
      if (error) throw error;
      await sleep(200);
    }
  })();
  try {
    await Promise.race([promise, poll]);
  } finally {
    settled = true;
  }
}

module.exports = { run };
