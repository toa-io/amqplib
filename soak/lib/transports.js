const fs = require('node:fs');
const path = require('node:path');

const CERTS = path.join(__dirname, '..', 'certs');

// How the client reaches the broker: directly, over TLS, or through the fragmenting proxy.
function transportsFor(config) {
  const query = `frameMax=${config.frameMax}&heartbeat=${config.heartbeat}`;
  const auth = `${encodeURIComponent(config.user)}:${encodeURIComponent(config.password)}`;
  const all = {
    plain: () => ({
      name: 'plain',
      url: `amqp://${auth}@${config.host}:${config.plainPort}/?${query}`,
      socketOptions: {},
    }),
    tls: () => ({
      name: 'tls',
      url: `amqps://${auth}@${config.host}:${config.tlsPort}/?${query}`,
      // text rather than a Buffer, since it travels to the worker processes as JSON
      socketOptions: { ca: [fs.readFileSync(path.join(CERTS, 'ca.pem'), 'utf8')], servername: config.host },
    }),
    chunked: () => ({
      name: 'chunked',
      url: `amqp://${auth}@127.0.0.1:${config.proxyPort}/?${query}`,
      socketOptions: {},
    }),
  };

  return config.transports.map((name) => {
    if (!all[name]) throw new Error(`Unknown transport '${name}'; choose from ${Object.keys(all).join(', ')}`);
    return all[name]();
  });
}

module.exports = { transportsFor };
