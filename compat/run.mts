// Runs the test suite of the original library against this one.
//
// `compat/test` is the original's `test` directory as it is, and `compat/lib` stands where the
// original's modules were, so that what the tests require resolves to the modules here.

import { spawnSync } from 'node:child_process'
import { readdirSync } from 'node:fs'
import { join } from 'node:path'
import { files, tests } from './exemptions.mts'

const directory = join(import.meta.dirname, 'test')

const suites = readdirSync(directory)
  .filter(name => name.endsWith('.test.js') && !Object.hasOwn(files, name))
  .map(name => join(directory, name))

const skipped = Object.keys(tests).map(
  name => `--test-skip-pattern=/^${name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$/`
)

const { status } = spawnSync(
  process.execPath,
  ['--test', '--test-timeout=1000', ...skipped, ...process.argv.slice(2), ...suites],
  { stdio: 'inherit' }
)

process.exit(status ?? 1)
