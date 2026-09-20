// What taking a message in costs, from the bytes a socket reads to the consumer's callback.
//
//   node benchmarks/receive.ts [--sizes=100,1k,16k,64k,448k] [--library=<name>] [--ack]
//
// Each library runs in a process of its own, so that neither warms up or fragments the other's.

import { fork } from 'node:child_process'
import { setImmediate as turn } from 'node:timers/promises'
import { Broker, delivery } from './broker.ts'
import { libraries } from './libraries.ts'
import { measure, option, size, type Cost } from './measure.ts'

const SIZES = option('sizes', '100,1k,16k,64k,448k').split(',').map(size)
const ACK = process.argv.includes('--ack')

/** Enough messages for the run to take about a second. */
const count = (bytes: number): number => Math.round(Math.min(2_000_000, 4e9 / (bytes + 2000)))

async function run(name: string, bytes: number): Promise<Cost> {
  const library = libraries.find(one => one.name === name)!
  const broker = new Broker()
  const { channel, feed, reads } = await library.open(broker)

  let received = 0

  await channel.consume(
    'benchmark',
    ACK
      ? message => {
          received++
          channel.ack(message)
        }
      : () => received++,
    { noAck: !ACK }
  )

  broker.quiet = true

  // Deliveries back to back, as many as make a few reads, so that frames fall across reads the
  // way they do on a socket.
  const one = delivery(1, Buffer.alloc(bytes, 'x'), 131072)
  const batch = Math.max(1, Math.ceil((4 * reads) / one.length))
  const wire = Buffer.concat(Array.from({ length: batch }, () => one))

  const pass = async (messages: number): Promise<void> => {
    for (let sent = 0; sent < messages; sent += batch) {
      for (let at = 0; at < wire.length; at += reads)
        feed(wire, at, Math.min(at + reads, wire.length))

      // a socket is read once per turn of the loop
      await turn()
    }

    while (received < messages) await turn()
  }

  const messages = Math.ceil(count(bytes) / batch) * batch

  await pass(Math.ceil(messages / 10 / batch) * batch)
  received = 0

  return measure(messages, () => pass(messages))
}

if (process.send !== undefined) {
  const [name, bytes] = process.argv.slice(2)

  process.send(await run(name!, Number(bytes)))
  process.exit(0)
}

const selected = option('library', '')

console.log(`| message | library | CPU µs | GC µs | GC/1k | RSS MB | buffers MB |`)
console.log('| ---: | --- | ---: | ---: | ---: | ---: | ---: |')

for (const bytes of SIZES)
  for (const { name } of libraries) {
    if (selected !== '' && selected !== name) continue

    const child = fork(import.meta.filename, [name, String(bytes), ...(ACK ? ['--ack'] : [])])
    const cost = await new Promise<Cost>(resolve => child.once('message', resolve as () => void))

    console.log(
      `| ${bytes} | ${name} | ${cost.cpu.toFixed(2)} | ${cost.gc.toFixed(2)} | ` +
        `${cost.collections.toFixed(2)} | ${cost.rss.toFixed(0)} | ${cost.buffers.toFixed(1)} |`
    )
  }
