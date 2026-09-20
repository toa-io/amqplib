import { format } from 'node:util'
import { constant_strs, info } from './defs.ts'
import { HEARTBEAT, type Frame } from './frame.ts'

export function closeMessage(fields: { replyCode: number; replyText: string }): string {
  const code = fields.replyCode

  return format('%d (%s) with message "%s"', code, constant_strs[code], fields.replyText)
}

export function methodName(id: number): string {
  return info(id).name
}

export function inspect(frame: Frame, showFields = false): string {
  if (frame === HEARTBEAT) return '<Heartbeat>'

  if (frame.id === undefined)
    return format('<Content channel:%d size:%d>', frame.channel, frame.size)

  return format(
    '<%s channel:%d%s>',
    info(frame.id).name,
    frame.channel,
    showFields ? ` ${JSON.stringify(frame.fields, undefined, 2)}` : ''
  )
}
