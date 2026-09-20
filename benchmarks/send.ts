// What publishing a message costs, from `publish` to the bytes handed to the socket.
//
//   node benchmarks/send.ts [--sizes=100,1k,16k,64k,448k] [--library=<name>] [--headers]
//
// Each library runs in a process of its own. A publisher that respects `drain` is what is
// measured: it publishes until told to stop, and goes on when told it may.

import { fork } from 'node:child_process'
import { setImmediate as turn } from 'node:timers/promises'
import { Broker } from './broker.ts'
import { libraries } from './libraries.ts'
import { measure, option, size, type Cost } from './measure.ts'

const SIZES = option('sizes', '100,1k,16k,64k,448k').split(',').map(size)
const HEADERS = process.argv.includes('--headers')

/** How many are published within one turn of the loop: a burst, as a handler makes one. */
const BURST = 64

const count = (bytes: number): number => Math.round(Math.min(1_000_000, 2e9 / (bytes + 2000)))

interface Result extends Cost {
  writes: number
}

async function run(name: string, bytes: number): Promise<Result> {
  const library = libraries.find(one => one.name === name)!
  const broker = new Broker()
  const { channel } = await library.open(broker)
  const content = Buffer.alloc(bytes, 'x')

  const options = HEADERS
    ? {
        contentType: 'application/json',
        correlationId: '0f5ee5d5-6a3b-4a59-9d55-6a2d2f4f4b0e',
        replyTo: 'amq.rabbitmq.reply-to',
        persistent: true,
        headers: {
          'x-tenant': 'benchmark',
          'x-attempt': 1,
          'x-trace': '4bf92f3577b34da6a3ce929d0e0e4736',
        },
      }
    : {}

  broker.quiet = true

  const pass = async (messages: number): Promise<void> => {
    for (let sent = 0; sent < messages;) {
      let ok = true

      for (let i = 0; i < BURST && sent < messages; i++, sent++)
        ok = channel.publish('benchmark.exchange', 'benchmark.key', content, options)

      if (ok) await turn()
      else await new Promise<void>(resolve => channel.once('drain', resolve))
    }

    await turn()
  }

  const messages = count(bytes)

  await pass(Math.ceil(messages / 10))

  const writes = broker.writes
  const cost = await measure(messages, () => pass(messages))

  return { ...cost, writes: (broker.writes - writes) / messages }
}

if (process.send !== undefined) {
  const [name, bytes] = process.argv.slice(2)

  process.send(await run(name!, Number(bytes)))
  process.exit(0)
}

const selected = option('library', '')

console.log(`| message | library | CPU µs | GC µs | GC/1k | writes | RSS MB | buffers MB |`)
console.log('| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |')

for (const bytes of SIZES)
  for (const { name } of libraries) {
    if (selected !== '' && selected !== name) continue

    const child = fork(import.meta.filename, [
      name,
      String(bytes),
      ...(HEADERS ? ['--headers'] : []),
    ])
    const cost = await new Promise<Result>(resolve => child.once('message', resolve as () => void))

    console.log(
      `| ${bytes} | ${name} | ${cost.cpu.toFixed(2)} | ${cost.gc.toFixed(2)} | ` +
        `${cost.collections.toFixed(2)} | ${cost.writes.toFixed(3)} | ${cost.rss.toFixed(0)} | ` +
        `${cost.buffers.toFixed(1)} |`
    )
  }
