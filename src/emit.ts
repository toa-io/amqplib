import type { EventEmitter } from 'node:events'

/**
 * Emits an event on behalf of the library. What a listener throws goes to `handler-error` if
 * anyone listens for it, so that a faulty listener does not take the connection down with it.
 */
export function safeEmit(emitter: EventEmitter, event: string, ...args: unknown[]): void {
  try {
    emitter.emit(event, ...args)
  } catch (error) {
    if (emitter.listenerCount('handler-error') === 0) throw error

    setImmediate(() => emitter.emit('handler-error', error, event))
  }
}
