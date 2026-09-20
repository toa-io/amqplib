// The callback interface: `import amqp from '@toa.io/amqplib/callback_api'`.

import { CallbackModel } from './callback_model.ts'
import { connect as connectRaw, type SocketOptions, type UrlObject } from './connect.ts'
import * as credentials from './credentials.ts'
import { IllegalOperationError } from './error.ts'
import {
  connectWithRecoveryCallback,
  recoveryEnabled,
  splitConnectionOptions,
  type RecoveringCallbackModel,
  type RecoveryOptions,
} from './recovery.ts'

export type ConnectOptions = SocketOptions & { recovery?: boolean | RecoveryOptions }

type Callback = (error: Error | null, model?: any) => void

// connect(url, options, callback), connect(url, callback), connect(callback)
export function connect(
  url: string | UrlObject | Callback | undefined,
  options?: ConnectOptions | Callback,
  cb?: Callback
): RecoveringCallbackModel | void {
  if (typeof url === 'function') {
    cb = url
    url = undefined
    options = undefined
  } else if (typeof options === 'function') {
    cb = options
    options = undefined
  }

  const target = url
  const { connectionOptions, recovery } = splitConnectionOptions(options)

  if (recoveryEnabled(recovery)) {
    const openModel = (): Promise<CallbackModel> =>
      new Promise((resolve, reject) => {
        connectRaw(target, connectionOptions, (error, connection) => {
          if (error === null) resolve(new CallbackModel(connection!))
          else reject(error)
        })
      })

    return connectWithRecoveryCallback(openModel, recovery, cb)
  }

  connectRaw(target, connectionOptions, (error, connection) => {
    if (error === null) cb!(null, new CallbackModel(connection!))
    else cb!(error)
  })
}

export { credentials, IllegalOperationError }
export { CallbackModel, Channel, ConfirmChannel } from './callback_model.ts'
export type { RecoveringCallbackModel, RecoveryOptions } from './recovery.ts'

export default { connect, credentials, IllegalOperationError }
