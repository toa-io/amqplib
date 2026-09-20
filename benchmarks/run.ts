// What a message costs the process that takes it in or sends it, with a broker: for each
// scenario, the original library and this one, one after the other.
//
//   npm run rabbitmq
//   npm run benchmark [-- deliver.1k,turn.1k]

import { fork, type ChildProcess } from 'node:child_process'
import { join } from 'node:path'
import { setTimeout as sleep } from 'node:timers/promises'
import * as amqp from '../src/index.ts'

const QUEUE = 'amqplib.benchmark'
const WINDOW = 5000
const WARMUP = 2000
const LIBRARIES = ['amqplib', '@toa.io/amqplib']

interface Scenario {
  id: string
  mode: 'deliver' | 'turn' | 'publish'
  size: number
  rate: number
}

// The sizes either side of 64 KB are there because that is what a socket reads at a time, while
// a content frame may be up to 128 KB: which is larger decides whether a message arrives whole.
const SCENARIOS: Scenario[] = [
  { id: 'deliver.100', mode: 'deliver', size: 100, rate: 30000 },
  { id: 'deliver.1k', mode: 'deliver', size: 1024, rate: 20000 },
  { id: 'deliver.32k', mode: 'deliver', size: 32 * 1024, rate: 1400 },
  { id: 'deliver.64k', mode: 'deliver', size: 64 * 1024, rate: 700 },
  { id: 'deliver.96k', mode: 'deliver', size: 96 * 1024, rate: 400 },
  { id: 'deliver.448k', mode: 'deliver', size: 448 * 1024, rate: 100 },
  { id: 'turn.1k', mode: 'turn', size: 1024, rate: 5000 },
  { id: 'turn.448k', mode: 'turn', size: 448 * 1024, rate: 50 },
  { id: 'publish.100', mode: 'publish', size: 100, rate: 30000 },
  { id: 'publish.1k', mode: 'publish', size: 1024, rate: 20000 },
  { id: 'publish.64k', mode: 'publish', size: 64 * 1024, rate: 700 },
  { id: 'publish.448k', mode: 'publish', size: 448 * 1024, rate: 100 },
]

type Stats = Record<string, number> & { cpu: { user: number; system: number } }

/** Starts the process under measurement and waits for it to say it is ready. */
async function start(url: string, library: string, scenario: Scenario): Promise<ChildProcess> {
  const peer = fork(join(import.meta.dirname, 'peer.ts'), {
    env: {
      ...process.env,
      BENCHMARK_URL: url,
      BENCHMARK_LIBRARY: library,
      BENCHMARK_MODE: scenario.mode,
      BENCHMARK_SIZE: String(scenario.size),
      BENCHMARK_RATE: String(scenario.rate),
    },
  })

  await new Promise((resolve, reject) => {
    peer.once('message', resolve)
    peer.once('exit', code => reject(new Error(`the peer exited with ${code}`)))
  })

  return peer
}

async function stop(peer: ChildProcess): Promise<void> {
  const ended = new Promise(resolve => peer.once('exit', resolve))

  peer.send('stop')

  const abandoned = setTimeout(() => peer.kill(), 2000)

  await ended
  clearTimeout(abandoned)
}

function meter(peer: ChildProcess): Promise<Stats> {
  return new Promise(resolve => {
    peer.once('message', resolve as () => void)
    peer.send('stats')
  })
}

/**
 * Sends at the rate asked for without waiting for anything: what is measured is what taking the
 * messages in costs, and a sender that waits would measure the round trip instead.
 */
function send(
  channel: amqp.Channel,
  scenario: Scenario,
  replyTo: string | undefined,
  deadline: number,
  sent: bigint[]
): Promise<void> {
  const payload = Buffer.alloc(scenario.size, 'x')
  const started = Date.now()

  let count = 0

  return new Promise(resolve => {
    const timer = setInterval(() => {
      if (Date.now() >= deadline) {
        clearInterval(timer)

        return resolve()
      }

      const due = Math.round(((Date.now() - started) / 1000) * scenario.rate)

      while (count < due) {
        count++
        sent.push(process.hrtime.bigint())

        channel.sendToQueue(
          QUEUE,
          payload,
          replyTo === undefined ? {} : { replyTo, correlationId: String(count) }
        )
      }
    }, 2)
  })
}

function percentile(values: number[], share: number): number {
  if (values.length === 0) return Number.NaN

  const sorted = values.toSorted((a, b) => a - b)

  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * share))]!
}

async function round(url: string, library: string, scenario: Scenario): Promise<string> {
  const peer = await start(url, library, scenario)
  const connection = await amqp.connect(url)
  const channel = await connection.createChannel()
  const latencies: number[] = []
  const sent: bigint[] = []

  let replyTo: string | undefined

  if (scenario.mode === 'turn') {
    replyTo = (await channel.assertQueue('', { exclusive: true })).queue

    await channel.consume(
      replyTo,
      message => {
        const at = sent[Number(message!.properties.correlationId) - 1]

        if (at !== undefined) latencies.push(Number(process.hrtime.bigint() - at) / 1e6)
      },
      { noAck: true }
    )
  }

  const drive = (duration: number, stamps: bigint[]): Promise<void> =>
    scenario.mode === 'publish'
      ? sleep(duration)
      : send(channel, scenario, replyTo, Date.now() + duration, stamps)

  try {
    await drive(WARMUP, [])
    await sleep(200)

    const before = await meter(peer)

    latencies.length = 0
    await drive(WINDOW, sent)
    await sleep(scenario.mode === 'publish' ? 0 : 500)

    const after = await meter(peer)
    const delta = (name: string): number => after[name]! - before[name]!
    const messages = delta('messages')
    const system = after.cpu.system - before.cpu.system
    const cpu = after.cpu.user - before.cpu.user + system
    const samples = Math.max(1, delta('samples'))
    const p50 = percentile(latencies, 0.5)

    return [
      scenario.id,
      library,
      Math.round((messages / WINDOW) * 1000),
      (cpu / messages).toFixed(1),
      (system / messages).toFixed(1),
      (delta('faults') / messages).toFixed(2),
      (delta('rss') / samples / 1e6).toFixed(0),
      (after.peak! / 1e6).toFixed(0),
      (delta('buffers') / samples / 1e6).toFixed(1),
      (delta('copied') / messages / 1024).toFixed(1),
      (delta('copies') / messages).toFixed(2),
      (delta('write') / messages).toFixed(3),
      (delta('writev') / messages).toFixed(3),
      ((delta('gc') * 1000) / messages).toFixed(1),
      ((delta('collections') * 1000) / messages).toFixed(1),
      Number.isNaN(p50) ? '—' : p50.toFixed(2),
      Number.isNaN(p50) ? '—' : percentile(latencies, 0.99).toFixed(2),
    ].join(' | ')
  } finally {
    await channel.close().catch(() => undefined)
    await connection.close().catch(() => undefined)
    await stop(peer)
  }
}

const url = process.env.BENCHMARK_URL ?? 'amqp://localhost'
const asked = process.argv[2]?.split(',')
const selected = asked === undefined ? SCENARIOS : SCENARIOS.filter(one => asked.includes(one.id))

for (const pass of [1, 2]) {
  console.log(`\n## Round ${pass}\n`)

  console.log(
    '| scenario | library | messages/s | CPU µs | system µs | faults | RSS MB | peak MB | buffers MB | copied KB | copies | write | writev | GC µs | GC/1k | p50 ms | p99 ms |'
  )

  console.log(`|${' --- |'.repeat(2)}${' ---: |'.repeat(15)}`)

  for (const scenario of selected)
    for (const library of LIBRARIES) console.log(`| ${await round(url, library, scenario)} |`)
}
