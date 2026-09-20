// The promise interface: `import amqp from '@toa.io/amqplib'`.

import { ChannelModel } from './channel_model.ts'
import { connect as connectRaw, type SocketOptions, type UrlObject } from './connect.ts'
import * as credentials from './credentials.ts'
import { IllegalOperationError } from './error.ts'
import {
  connectWithRecoveryPromise,
  recoveryEnabled,
  splitConnectionOptions,
  type RecoveringPromiseModel,
  type RecoveryOptions as Recovery,
} from './recovery.ts'

export type ConnectOptions = SocketOptions & { recovery?: boolean | RecoveryOptions }

function open(url?: string | UrlObject, options?: SocketOptions): Promise<ChannelModel> {
  return new Promise((resolve, reject) => {
    connectRaw(url, options, (error, connection) => {
      if (error === null) resolve(new ChannelModel(connection!))
      else reject(error)
    })
  })
}

export function connect(
  url: string | UrlObject | undefined,
  options: ConnectOptions & { recovery: true | RecoveryOptions }
): Promise<RecoveringPromiseModel>
export function connect(url?: string | UrlObject, options?: ConnectOptions): Promise<ChannelModel>
export function connect(
  url?: string | UrlObject,
  options?: ConnectOptions
): Promise<ChannelModel | RecoveringPromiseModel> {
  const { connectionOptions, recovery } = splitConnectionOptions(options)

  if (recoveryEnabled(recovery))
    return connectWithRecoveryPromise(() => open(url, connectionOptions), recovery)

  return open(url, connectionOptions)
}

export { credentials, IllegalOperationError }
export { ChannelModel, Channel, ConfirmChannel } from './channel_model.ts'
export type RecoveryOptions = Recovery<ChannelModel>
export type { RecoveringPromiseModel as RecoveringChannelModel } from './recovery.ts'
export type { Connection } from './connection.ts'
export type { Credentials } from './credentials.ts'
export type * from './properties.ts'

export default { connect, credentials, IllegalOperationError }
