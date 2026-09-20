// What a stretch of work cost the process: CPU, garbage collection and memory.

import { PerformanceObserver } from 'node:perf_hooks'

export interface Cost {
  /** CPU time per operation, µs: user and system. */
  cpu: number
  /** Garbage collections per thousand operations. */
  collections: number
  /** What they took per operation, µs. */
  gc: number
  /** Resident set at the end, MB. */
  rss: number
  /** Bytes behind Buffers at the end, MB. */
  buffers: number
}

const gc = { count: 0, duration: 0 }

new PerformanceObserver(list => {
  for (const entry of list.getEntries()) {
    gc.count++
    gc.duration += entry.duration
  }
}).observe({ entryTypes: ['gc'] })

export async function measure(operations: number, work: () => Promise<void>): Promise<Cost> {
  const collections = gc.count
  const { duration } = gc
  const cpu = process.cpuUsage()

  await work()

  const spent = process.cpuUsage(cpu)
  const memory = process.memoryUsage()

  return {
    cpu: (spent.user + spent.system) / operations,
    collections: ((gc.count - collections) * 1000) / operations,
    gc: ((gc.duration - duration) * 1000) / operations,
    rss: memory.rss / 1e6,
    buffers: memory.arrayBuffers / 1e6,
  }
}

export function option(name: string, fallback: string): string {
  const argument = process.argv.find(one => one.startsWith(`--${name}=`))

  return argument === undefined ? fallback : argument.slice(name.length + 3)
}

export function size(text: string): number {
  const factors: Record<string, number> = { k: 1024, m: 1024 * 1024 }

  return parseInt(text, 10) * (factors[text.at(-1)!] ?? 1)
}
