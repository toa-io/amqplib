const crypto = require('node:crypto');

function queueName(transport, index) {
  return `soak.${transport}.${index}`;
}

// Queues refuse publishes when full rather than dropping, so a lagging consumer bounds the
// broker's memory without any message going missing; the publisher retries a refused publish.
function queueOptions(config) {
  return {
    durable: false,
    autoDelete: false,
    arguments: {
      'x-max-length': config.queueLength,
      'x-max-length-bytes': Math.max(128 * 1024 * 1024, 4 * config.maxSize),
      'x-overflow': 'reject-publish',
    },
  };
}

function sha256(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

function fatal(error) {
  process.send({ type: 'error', message: error?.message ? error.message : String(error), stack: error?.stack });
  setTimeout(() => process.exit(2), 100);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = { queueName, queueOptions, sha256, fatal, sleep };
