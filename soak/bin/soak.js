#!/usr/bin/env node
const { configFrom, usage } = require('../lib/config');
const { run } = require('../lib/run');
const { format, writeJson } = require('../lib/report');

async function main() {
  let config;
  try {
    config = configFrom(process.argv.slice(2));
  } catch (error) {
    console.error(error.message);
    console.error();
    console.error(usage());
    return 2;
  }
  if (config.help) {
    console.log(config.help);
    return 0;
  }

  const result = await run(config);
  console.log(format(result));
  if (config.json) writeJson(config.json, result);
  return result.exitCode;
}

main().then(
  (code) => process.exit(code),
  (error) => {
    console.error(error.stack || error);
    process.exit(2);
  },
);
