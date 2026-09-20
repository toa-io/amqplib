function trimStack(stack: string | undefined, lines: number): string | undefined {
  return stack?.split('\n').slice(lines).join('\n')
}

/** Thrown when something is asked of a connection or a channel that is closing or closed. */
export class IllegalOperationError extends Error {
  /** Where the connection or the channel went into the state that forbids the operation. */
  public readonly stackAtStateChange: string | undefined
  public override name = 'IllegalOperationError' as const

  public constructor(message: string, stack?: string) {
    super(message)

    this.stackAtStateChange = stack
  }
}

export function stackCapture(reason: string): string {
  return `Stack capture: ${reason}\n${trimStack(new Error(reason).stack, 2)}`
}
