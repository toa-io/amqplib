const fs = require('node:fs');
const path = require('node:path');

// Which amqplib a run actually loaded, so a report is unambiguous about what it tested.
function amqplibVersion() {
  const entry = require.resolve('amqplib');
  const dir = fs.realpathSync(path.dirname(entry));
  const pkg = JSON.parse(fs.readFileSync(path.join(dir, 'package.json'), 'utf8'));
  let resolved = null;
  try {
    const lock = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'node_modules', '.package-lock.json'), 'utf8'));
    resolved = lock.packages?.['node_modules/amqplib']?.resolved || null;
  } catch {
    // no hidden lockfile, which is fine
  }
  return { version: pkg.version, dir, resolved };
}

module.exports = { amqplibVersion };
