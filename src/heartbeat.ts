// Heartbeats work in both directions. Sending: if nothing went out during half the interval, a
// heartbeat frame goes out. Receiving: if nothing came in during two whole intervals, the
// connection is dead.

/** Milliseconds in the unit the interval is given in. Tests shorten it. */
export const units = { ms: 1000 }

export class Heart {
  public readonly interval: number

  private readonly sendTimer: NodeJS.Timeout
  private readonly recvTimer: NodeJS.Timeout
  private missed = 0

  public constructor(
    interval: number,
    checkSend: () => boolean,
    checkRecv: () => boolean,
    beat: () => void,
    timeout: () => void
  ) {
    this.interval = interval

    const ms = interval * units.ms

    this.sendTimer = setInterval(() => {
      if (!checkSend()) beat()
    }, ms / 2)

    this.recvTimer = setInterval(() => {
      if (checkRecv()) this.missed = 0
      else if (++this.missed === 2) timeout()
    }, ms)
  }

  public clear(): void {
    clearInterval(this.sendTimer)
    clearInterval(this.recvTimer)
  }
}
