const { PerformanceObserver } = require('node:perf_hooks');

// What a process spent while it ran: CPU, garbage collection, and the most memory it held.
class Metrics {
  constructor() {
    this.gcCount = 0;
    this.gcMs = 0;
    this.peakRss = 0;
    this.peakHeap = 0;
    this.startedAt = 0;
    this.cpuAtStart = null;
  }

  start() {
    this.startedAt = Date.now();
    this.cpuAtStart = process.cpuUsage();
    this.observer = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        this.gcCount++;
        this.gcMs += entry.duration;
      }
    });
    this.observer.observe({ entryTypes: ['gc'] });
    this.sample();
    this.sampler = setInterval(() => this.sample(), 250);
    this.sampler.unref();
  }

  sample() {
    const usage = process.memoryUsage();
    if (usage.rss > this.peakRss) this.peakRss = usage.rss;
    if (usage.heapUsed > this.peakHeap) this.peakHeap = usage.heapUsed;
  }

  snapshot() {
    this.sample();
    const cpu = process.cpuUsage(this.cpuAtStart);
    return {
      elapsedMs: Date.now() - this.startedAt,
      cpuMs: (cpu.user + cpu.system) / 1000,
      gcCount: this.gcCount,
      gcMs: this.gcMs,
      peakRss: this.peakRss,
      peakHeap: this.peakHeap,
    };
  }

  stop() {
    clearInterval(this.sampler);
    if (this.observer) this.observer.disconnect();
  }
}

module.exports = { Metrics };
