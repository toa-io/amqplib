// The process under measurement: it takes in, or sends, what a scenario calls for, and reports
// what that cost it.
//
// The socket's writes and every copy between buffers are counted here rather than in a library:
// what a message costs a process is mostly the syscalls it makes and the bytes it moves.

import net from 'node:net'
import { PerformanceObserver } from 'node:perf_hooks'

const counters = { write: 0, writev: 0, copies: 0, copied: 0, collections: 0, gc: 0 }

const socket = net.Socket.prototype as unknown as Record<string, (...args: any[]) => unknown>
const { _write: write, _writev: writev } = socket
const { concat } = Buffer
const { copy } = Buffer.prototype

socket._write = function (...args) {
  counters.write++

  return write!.apply(this, args)
}

socket._writev = function (...args) {
  counters.writev++

  return writev!.apply(this, args)
}

// Every byte moved from one buffer to another counts, however it is moved, so that replacing a
// concatenation with a copy shows as the difference it makes and not as a way round the metric.
Buffer.concat = ((list: Uint8Array[], length?: number) => {
  counters.copies++
  counters.copied += length ?? list.reduce((sum, buffer) => sum + buffer.length, 0)

  return concat(list, length)
}) as typeof Buffer.concat

Buffer.prototype.copy = function (...args: Parameters<typeof copy>) {
  const copied = copy.apply(this, args)

  counters.copies++
  counters.copied += copied

  return copied
}

new PerformanceObserver(list => {
  for (const entry of list.getEntries()) {
    counters.collections++
    counters.gc += entry.duration
  }
}).observe({ entryTypes: ['gc'] })

// Read on a timer: what a process holds while it works is its high-water mark, and reading it
// only when asked would miss it.
const memory = { samples: 0, rss: 0, buffers: 0, peak: 0 }

setInterval(() => {
  const { rss, arrayBuffers } = process.memoryUsage()

  memory.samples++
  memory.rss += rss
  memory.buffers += arrayBuffers
  memory.peak = Math.max(memory.peak, rss)
}, 100).unref()

const QUEUE = 'amqplib.benchmark'
const url = process.env.BENCHMARK_URL ?? 'amqp://localhost'
const mode = process.env.BENCHMARK_MODE ?? 'deliver'
const library = process.env.BENCHMARK_LIBRARY ?? 'amqplib'

const amqp = await import(library === 'amqplib' ? 'amqplib' : '../dist/index.js')
const connection = await amqp.connect(url)
const channel = await connection.createChannel()

let messages = 0

if (mode === 'publish') {
  // Sends at the rate asked for to a key nothing is bound to, so that the broker drops what it
  // is sent and nothing but sending is measured.
  const content = Buffer.alloc(Number(process.env.BENCHMARK_SIZE), 'x')
  const rate = Number(process.env.BENCHMARK_RATE)
  const started = Date.now()

  const options = {
    contentType: 'application/octet-stream',
    correlationId: '0f5ee5d5-6a3b-4a59-9d55-6a2d2f4f4b0e',
    headers: { 'x-tenant': 'benchmark', 'x-attempt': 1 },
  }

  setInterval(() => {
    const due = Math.round(((Date.now() - started) / 1000) * rate)

    while (messages < due) {
      messages++
      channel.publish('', 'amqplib.benchmark.nowhere', content, options)
    }
  }, 2)
} else {
  await channel.assertQueue(QUEUE, { durable: false, autoDelete: true })
  await channel.purgeQueue(QUEUE)
  await channel.prefetch(0)

  // `deliver` takes a message in and does nothing else; `turn` answers it and acknowledges it,
  // which is the two frames a served request leaves in.
  await channel.consume(
    QUEUE,
    mode === 'deliver'
      ? () => messages++
      : (message: any) => {
          messages++

          channel.sendToQueue(message.properties.replyTo, message.content, {
            correlationId: message.properties.correlationId,
          })

          channel.ack(message)
        },
    { noAck: mode === 'deliver' }
  )
}

process.on('message', ask => {
  // ends by returning rather than by a signal, so that a profile is written on the way out
  if (ask === 'stop') process.exit(0)

  const { minorPageFault, majorPageFault } = process.resourceUsage()

  process.send!({
    messages,
    cpu: process.cpuUsage(),
    faults: minorPageFault + majorPageFault,
    ...memory,
    ...counters,
  })

  memory.peak = 0
})

process.send!('ready')
