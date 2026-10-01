const { parseArgs } = require('node:util');
const { KB, MB } = require('./random');

const OPTIONS = {
  duration: { type: 'string', default: '60s', help: 'how long to publish for, e.g. 30s, 5m' },
  transports: { type: 'string', default: 'plain,tls,chunked', help: 'comma-separated: plain, tls, chunked' },
  channels: { type: 'string', default: '8', help: 'channels (and queues) per connection' },
  'max-size': { type: 'string', default: '8MB', help: 'largest message body, e.g. 512KB, 8MB' },
  'frame-max': {
    type: 'string',
    default: '131072',
    help: 'frame size to negotiate; 8192 is the smallest RabbitMQ 4 allows',
  },
  heartbeat: { type: 'string', default: '5', help: 'heartbeat interval in seconds; 0 disables' },
  prefetch: { type: 'string', default: '32', help: 'consumer prefetch per channel' },
  'queue-length': { type: 'string', default: '128', help: 'messages a queue holds before publishes are refused' },
  'hold-ms': {
    type: 'string',
    default: '500',
    help: 'how long the consumer keeps each delivery before re-verifying it',
  },
  'hold-bytes': {
    type: 'string',
    default: '128MB',
    help: 'most content the consumer holds for re-verification at once',
  },
  seed: { type: 'string', help: 'seed for the random generator; random when absent' },
  host: { type: 'string', default: 'localhost' },
  'plain-port': { type: 'string', default: '5672' },
  'tls-port': { type: 'string', default: '5671' },
  'proxy-port': { type: 'string', default: '5670', help: 'local port the chunked transport listens on' },
  user: { type: 'string', default: 'guest' },
  password: { type: 'string', default: 'guest' },
  json: { type: 'string', help: 'write the report as JSON to this file' },
  verbose: { type: 'boolean', default: false, help: 'print every failure as it happens' },
  help: { type: 'boolean', short: 'h', default: false },
};

function usage() {
  const lines = ['Usage: soak [options]', ''];
  for (const [name, opt] of Object.entries(OPTIONS)) {
    const flag = `--${name}${opt.type === 'string' ? ' <value>' : ''}`;
    const dflt = opt.default !== undefined && opt.type === 'string' ? ` (default ${opt.default})` : '';
    lines.push(`  ${flag.padEnd(24)} ${opt.help || ''}${dflt}`);
  }
  return lines.join('\n');
}

function parseDuration(text) {
  const m = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h)?$/.exec(String(text).trim());
  if (!m) throw new Error(`Cannot read duration '${text}'`);
  const n = Number(m[1]);
  switch (m[2] || 's') {
    case 'ms':
      return n;
    case 's':
      return n * 1000;
    case 'm':
      return n * 60 * 1000;
    default:
      return n * 60 * 60 * 1000;
  }
}

function parseBytes(text) {
  const m = /^(\d+(?:\.\d+)?)\s*(B|KB|MB|GB)?$/i.exec(String(text).trim());
  if (!m) throw new Error(`Cannot read size '${text}'`);
  const n = Number(m[1]);
  switch ((m[2] || 'B').toUpperCase()) {
    case 'B':
      return Math.floor(n);
    case 'KB':
      return Math.floor(n * KB);
    case 'MB':
      return Math.floor(n * MB);
    default:
      return Math.floor(n * 1024 * MB);
  }
}

function integer(name, text) {
  const n = Number(text);
  if (!Number.isInteger(n) || n < 0) throw new Error(`--${name} must be a whole number, not '${text}'`);
  return n;
}

function configFrom(argv) {
  const { values } = parseArgs({ args: argv, options: OPTIONS, allowPositionals: false });
  if (values.help) return { help: usage() };

  const config = {
    durationMs: parseDuration(values.duration),
    transports: values.transports
      .split(',')
      .map((t) => t.trim())
      .filter(Boolean),
    channels: integer('channels', values.channels),
    maxSize: parseBytes(values['max-size']),
    frameMax: integer('frame-max', values['frame-max']),
    heartbeat: integer('heartbeat', values.heartbeat),
    prefetch: integer('prefetch', values.prefetch),
    queueLength: integer('queue-length', values['queue-length']),
    holdMs: integer('hold-ms', values['hold-ms']),
    holdBytes: parseBytes(values['hold-bytes']),
    seed: values.seed === undefined ? Math.floor(Math.random() * 0xffffffff) : integer('seed', values.seed),
    host: values.host,
    plainPort: integer('plain-port', values['plain-port']),
    tlsPort: integer('tls-port', values['tls-port']),
    proxyPort: integer('proxy-port', values['proxy-port']),
    user: values.user,
    password: values.password,
    json: values.json || null,
    verbose: values.verbose,
  };

  if (config.channels < 1) throw new Error('--channels must be at least 1');
  if (config.frameMax < 8192) throw new Error('--frame-max must be at least 8192, the least RabbitMQ 4 accepts');
  if (config.transports.length === 0) throw new Error('--transports must name at least one transport');

  return config;
}

module.exports = { configFrom, parseDuration, parseBytes, usage };
