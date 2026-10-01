const fs = require('node:fs');
const path = require('node:path');

// Which library a run actually loaded, so a report is unambiguous about what it tested: this
// repository's sources, as they are in the working tree.
function amqplibVersion() {
  const dir = path.join(__dirname, '..', '..');
  const pkg = JSON.parse(fs.readFileSync(path.join(dir, 'package.json'), 'utf8'));
  return { version: pkg.name, dir: path.join(dir, 'src'), resolved: null };
}

module.exports = { amqplibVersion };
