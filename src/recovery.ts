// A connection that comes back: when it is lost, it is opened again after a delay that grows
// with every failed attempt. Channels are not brought back; `setup` is where an application
// makes what it needs on each new connection.

import { EventEmitter } from 'node:events'
import type { ChannelOptions } from './properties.ts'

type ChannelCallback = (error: any, channel: any) => void

export interface RecoveryOptions<Model = any> {
  enabled?: boolean
  initialDelay?: number
  maxDelay?: number
  factor?: number
  jitter?: number
  maxRetries?: number
  setup?:
    | ((model: Model) => unknown)
    | ((model: Model, done: (error?: Error) => void) => void)
    | null
}

type Recovery = Required<Omit<RecoveryOptions, 'enabled'>>

/** What a model is asked for, in the manner of its interface. */
interface Adapters {
  closeModel(model: any): Promise<void>
  createChannel(model: any, options: unknown, confirm: boolean): Promise<unknown>
  updateSecret(model: any, newSecret: Buffer, reason: string): Promise<unknown>
}

interface Waiter {
  resolve(model: unknown): void
  reject(error: Error): void
}

const DEFAULT_RECOVERY = {
  initialDelay: 100,
  maxDelay: 30000,
  factor: 2,
  jitter: 0.2,
}

const EVENTS = [
  'connect',
  'disconnect',
  'connect-failed',
  'reconnect-scheduled',
  'reconnect-failed',
  'blocked',
  'unblocked',
  'error',
  'update-secret-ok',
  'handler-error',
]

export function splitConnectionOptions<T extends object>(
  connOptions: T | false | undefined | null
): { connectionOptions: any; recovery: boolean | RecoveryOptions | null | undefined } {
  if (!connOptions || typeof connOptions !== 'object' || !Object.hasOwn(connOptions, 'recovery'))
    return { connectionOptions: connOptions, recovery: null }

  const { recovery, ...connectionOptions } = connOptions as T & {
    recovery?: boolean | RecoveryOptions
  }

  return { connectionOptions, recovery }
}

export function recoveryEnabled(
  recovery: boolean | RecoveryOptions | null | undefined
): recovery is true | RecoveryOptions {
  if (!recovery) return false

  return recovery === true || recovery.enabled !== false
}

function toFiniteNumber(value: unknown, fallback: number): number {
  const n = Number(value)

  return Number.isFinite(n) ? n : fallback
}

function normaliseRecoveryOptions(recovery: true | RecoveryOptions | null | undefined): Recovery {
  const source = recovery === true ? {} : (recovery ?? {})
  const initialDelay = Math.max(
    0,
    toFiniteNumber(source.initialDelay, DEFAULT_RECOVERY.initialDelay)
  )
  const maxRetries =
    source.maxRetries === undefined || source.maxRetries === null
      ? Infinity
      : toFiniteNumber(source.maxRetries, Infinity)

  return {
    initialDelay,
    maxDelay: Math.max(initialDelay, toFiniteNumber(source.maxDelay, DEFAULT_RECOVERY.maxDelay)),
    factor: Math.max(1, toFiniteNumber(source.factor, DEFAULT_RECOVERY.factor)),
    jitter: Math.min(1, Math.max(0, toFiniteNumber(source.jitter, DEFAULT_RECOVERY.jitter))),
    maxRetries: maxRetries < 0 ? 0 : maxRetries,
    setup: typeof source.setup === 'function' ? source.setup : null,
  }
}

function toError(error: unknown, fallbackMessage: string): Error {
  if (error instanceof Error) return error

  return new Error(error ? String(error) : fallbackMessage)
}

function calculateDelay(recovery: Recovery, attempt: number): number {
  const base = Math.min(recovery.maxDelay, recovery.initialDelay * recovery.factor ** (attempt - 1))
  const jitter = base * recovery.jitter
  const offset = jitter > 0 ? Math.random() * jitter * 2 - jitter : 0

  return Math.max(0, Math.round(base + offset))
}

// oxlint-disable promise/no-callback-in-promise -- this is where promises meet callbacks

/** Runs `setup`, which either takes a callback or returns a promise. */
function runSetup(setup: Recovery['setup'], model: unknown): Promise<void> {
  if (setup === null) return Promise.resolve()

  return new Promise((resolve, reject) => {
    let settled = false

    const done = (error?: unknown): void => {
      if (settled) return

      settled = true

      if (error) reject(toError(error, 'Recovery setup failed'))
      else resolve()
    }

    try {
      if (setup.length >= 2) setup(model, done)
      else Promise.resolve((setup as (model: unknown) => unknown)(model)).then(() => done(), done)
    } catch (error) {
      done(error)
    }
  })
}

function makeCallback<T>(promise: Promise<T>, cb: unknown): void {
  const callback = typeof cb === 'function' ? cb : () => undefined

  promise.then(
    value => callback(null, value),
    error => callback(toError(error, 'Operation failed'))
  )
}

class RecoveringCore extends EventEmitter {
  private readonly openModel: () => Promise<unknown>
  private readonly adapters: Adapters
  private readonly recovery: Recovery
  private model: any = null
  private unbind: (() => void) | null = null
  private waiters: Waiter[] = []
  private connecting = false
  private stopped = false
  private timer: NodeJS.Timeout | null = null
  private attempt = 0
  private initialReady = false
  private readonly initialWait: Promise<void>
  private resolveInitial!: () => void
  private rejectInitial!: (error: Error) => void

  public constructor(
    openModel: () => Promise<unknown>,
    recovery: true | RecoveryOptions | null | undefined,
    adapters: Adapters
  ) {
    super()

    this.openModel = openModel
    this.adapters = adapters
    this.recovery = normaliseRecoveryOptions(recovery)

    this.initialWait = new Promise((resolve, reject) => {
      this.resolveInitial = resolve
      this.rejectInitial = reject
    })

    this.connect()
  }

  public waitForConnect(): Promise<void> {
    return this.initialWait
  }

  public async close(): Promise<void> {
    if (this.stopped) return

    this.stopped = true

    if (this.timer !== null) {
      clearTimeout(this.timer)
      this.timer = null
    }

    const closed = new Error('Connection closed')

    this.rejectPendingWaiters(closed)
    this.rejectInitialConnection(closed)

    if (this.model !== null) {
      const { model } = this

      this.model = null
      this.unbindModel()
      await this.closeModelNoThrow(model)
    }
  }

  public async createChannel(options: unknown, confirm: boolean): Promise<unknown> {
    return this.adapters.createChannel(await this.waitForConnection(), options, confirm)
  }

  public async updateSecret(newSecret: Buffer, reason: string): Promise<unknown> {
    return this.adapters.updateSecret(await this.waitForConnection(), newSecret, reason)
  }

  private rejectInitialConnection(error: Error): void {
    if (this.initialReady) return

    this.initialReady = true
    this.rejectInitial(error)
  }

  private rejectPendingWaiters(error: Error): void {
    for (const waiter of this.waiters.splice(0)) waiter.reject(error)
  }

  private waitForConnection(): Promise<unknown> {
    if (this.model !== null) return Promise.resolve(this.model)

    if (this.stopped) return Promise.reject(new Error('Connection closed'))

    return new Promise((resolve, reject) => {
      this.waiters.push({ resolve, reject })
    })
  }

  private bindModel(model: EventEmitter): void {
    const listeners: [string, (...args: any[]) => void][] = [
      [
        'close',
        (maybeErr?: Error) => {
          if (this.model !== model) return

          this.model = null
          this.unbindModel()

          if (this.stopped) return

          const error = toError(maybeErr, 'Connection closed')

          this.emit('disconnect', error)
          this.scheduleReconnect(error)
        },
      ],
      ['blocked', reason => this.emit('blocked', reason)],
      ['unblocked', () => this.emit('unblocked')],
      ['error', error => this.emit('error', error)],
      ['update-secret-ok', () => this.emit('update-secret-ok')],
      ['handler-error', (error, event) => this.emit('handler-error', error, event)],
    ]

    for (const [event, listener] of listeners) model.on(event, listener)

    this.unbind = () => {
      for (const [event, listener] of listeners) model.removeListener(event, listener)

      // an error of a connection let go of must not bring the process down
      model.on('error', () => undefined)
    }
  }

  private unbindModel(): void {
    this.unbind?.()
    this.unbind = null
  }

  /** Never rejects: a failed attempt schedules the next one. */
  private async connect(): Promise<void> {
    if (this.stopped || this.connecting) return

    this.connecting = true

    let model: any

    try {
      model = await this.openModel()
      await runSetup(this.recovery.setup, model)

      if (this.stopped) {
        await this.closeModelNoThrow(model)

        return
      }

      this.model = model
      this.attempt = 0
      this.bindModel(model)

      if (!this.initialReady) {
        this.initialReady = true
        this.resolveInitial()
      }

      for (const waiter of this.waiters.splice(0)) waiter.resolve(model)

      this.emit('connect', model)
    } catch (err) {
      const error = toError(err, 'Connection recovery failed')

      if (model !== undefined) await this.closeModelNoThrow(model)

      this.emit('connect-failed', error)
      this.scheduleReconnect(error)
    } finally {
      this.connecting = false
    }
  }

  private scheduleReconnect(error: Error): void {
    if (this.stopped || this.timer !== null) return

    if (this.attempt >= this.recovery.maxRetries) {
      this.rejectInitialConnection(error)
      this.rejectPendingWaiters(error)
      this.emit('reconnect-failed', error)

      return
    }

    const attempt = ++this.attempt
    const delay = calculateDelay(this.recovery, attempt)

    this.emit('reconnect-scheduled', { attempt, delay, error })

    this.timer = setTimeout(() => {
      this.timer = null
      this.connect()
    }, delay)
  }

  private closeModelNoThrow(model: unknown): Promise<void> {
    return this.adapters.closeModel(model).catch(() => undefined)
  }
}

const PROMISE_ADAPTERS: Adapters = {
  closeModel: model => model.close(),
  createChannel: (model, options, confirm) =>
    confirm ? model.createConfirmChannel(options) : model.createChannel(options),
  updateSecret: (model, newSecret, reason) => model.updateSecret(newSecret, reason),
}

const CALLBACK_ADAPTERS: Adapters = {
  closeModel: model =>
    new Promise((resolve, reject) => {
      model.close((error: Error | null) => (error ? reject(error) : resolve()))
    }),

  createChannel: (model, options, confirm) =>
    new Promise((resolve, reject) => {
      const method = confirm ? 'createConfirmChannel' : 'createChannel'
      const cb = (error: Error | null, channel: unknown): void =>
        error ? reject(error) : resolve(channel)

      if (options === undefined) model[method](cb)
      else model[method](options, cb)
    }),

  updateSecret: (model, newSecret, reason) =>
    new Promise((resolve, reject) => {
      model.updateSecret(newSecret, reason, (error: Error | null, ok: unknown) =>
        error ? reject(error) : resolve(ok)
      )
    }),
}

function wire(source: EventEmitter, target: EventEmitter): void {
  for (const event of EVENTS) source.on(event, (...args) => target.emit(event, ...args))
}

export class RecoveringPromiseModel extends EventEmitter {
  private readonly core: RecoveringCore

  public constructor(openModel: () => Promise<unknown>, recovery?: true | RecoveryOptions | null) {
    super()

    this.core = new RecoveringCore(openModel, recovery, PROMISE_ADAPTERS)
    wire(this.core, this)
  }

  public async waitForConnect(): Promise<this> {
    await this.core.waitForConnect()

    return this
  }

  public close(): Promise<void> {
    return this.core.close()
  }

  public createChannel(options?: unknown): Promise<any> {
    return this.core.createChannel(options, false)
  }

  public createConfirmChannel(options?: unknown): Promise<any> {
    return this.core.createChannel(options, true)
  }

  public updateSecret(newSecret: Buffer, reason: string): Promise<unknown> {
    return this.core.updateSecret(newSecret, reason)
  }
}

export class RecoveringCallbackModel extends EventEmitter {
  private readonly core: RecoveringCore

  public constructor(openModel: () => Promise<unknown>, recovery?: true | RecoveryOptions | null) {
    super()

    this.core = new RecoveringCore(openModel, recovery, CALLBACK_ADAPTERS)
    wire(this.core, this)
  }

  public waitForConnect(cb?: unknown): this {
    makeCallback(
      this.core.waitForConnect().then(() => this),
      cb
    )

    return this
  }

  public close(cb?: (error: any) => void): this {
    makeCallback(this.core.close(), cb)

    return this
  }

  public updateSecret(newSecret: Buffer, reason: string, cb?: (error: any) => void): this {
    makeCallback(this.core.updateSecret(newSecret, reason), cb)

    return this
  }

  public createChannel(cb: ChannelCallback): this
  public createChannel(options: ChannelOptions | undefined, cb: ChannelCallback): this
  public createChannel(options?: unknown, cb?: unknown): this {
    if (typeof options === 'function') {
      cb = options
      options = undefined
    }

    makeCallback(this.core.createChannel(options, false), cb)

    return this
  }

  public createConfirmChannel(cb: ChannelCallback): this
  public createConfirmChannel(options: ChannelOptions | undefined, cb: ChannelCallback): this
  public createConfirmChannel(options?: unknown, cb?: unknown): this {
    if (typeof options === 'function') {
      cb = options
      options = undefined
    }

    makeCallback(this.core.createChannel(options, true), cb)

    return this
  }
}

export function connectWithRecoveryPromise(
  openModel: () => Promise<unknown>,
  recovery?: true | RecoveryOptions | null
): Promise<RecoveringPromiseModel> {
  return new RecoveringPromiseModel(openModel, recovery).waitForConnect()
}

export function connectWithRecoveryCallback(
  openModel: () => Promise<unknown>,
  recovery: true | RecoveryOptions | null | undefined,
  cb?: unknown
): RecoveringCallbackModel {
  return new RecoveringCallbackModel(openModel, recovery).waitForConnect(cb)
}
