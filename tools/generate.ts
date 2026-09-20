// Writes src/defs.ts from RabbitMQ's machine-readable AMQP 0-9-1 specification.
//
// Every method and every set of properties gets an encoder that writes a whole frame at an offset
// of a buffer it is given, and a decoder that reads the arguments at an offset. A frame is never a
// buffer of its own.

import { readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

interface SpecArgument {
  name: string
  type?: string
  domain?: string
  'default-value'?: unknown
}

interface SpecMethod {
  id: number
  name: string
  arguments: SpecArgument[]
}

interface SpecClass {
  id: number
  name: string
  methods: SpecMethod[]
  properties?: SpecArgument[]
}

interface Spec {
  domains: [string, string][]
  constants: { name: string; value: number }[]
  classes: SpecClass[]
}

interface Argument {
  name: string
  type: string
  default: unknown
}

interface Definition {
  id: number
  name: string
  classId?: number
  methodId?: number
  args: Argument[]
}

const FRAME_METHOD = 1
const FRAME_HEADER = 2
const FRAME_END = 206

const spec: Spec = JSON.parse(
  readFileSync(join(import.meta.dirname, 'amqp-rabbitmq-0.9.1.json'), 'utf8')
)

const domains = new Map(spec.domains)
const initial = (part: string): string => part.charAt(0).toUpperCase() + part.slice(1)

const camel = (dashed: string): string => {
  const [first, ...rest] = dashed.split('-')

  return first + rest.map(initial).join('')
}

const argument = (a: SpecArgument): Argument => ({
  type: a.type ?? domains.get(a.domain!)!,
  name: camel(a.name),
  default: a['default-value'],
})

const methods: Definition[] = []
const propertieses: Definition[] = []

for (const clazz of spec.classes) {
  for (const method of clazz.methods)
    methods.push({
      id: (clazz.id << 16) + method.id,
      name: initial(clazz.name) + method.name.split('-').map(initial).join(''),
      classId: clazz.id,
      methodId: method.id,
      args: method.arguments.map(argument),
    })

  if (clazz.properties !== undefined && clazz.properties.length > 0)
    propertieses.push({
      id: clazz.id,
      name: `${initial(clazz.name)}Properties`,
      args: clazz.properties.map(argument),
    })
}

const lines: string[] = []
const emit = (...text: string[]): void => {
  lines.push(...text)
}

const tsType = (type: string): string => {
  switch (type) {
    case 'bit':
      return 'boolean'
    case 'shortstr':
      return 'string'
    case 'longstr':
      return 'Buffer'
    case 'table':
      return 'Table'
    default:
      return 'number'
  }
}

const description = (type: string): string => {
  switch (type) {
    case 'bit':
      return 'booleany'
    case 'shortstr':
      return 'a string (up to 255 chars)'
    case 'longstr':
      return 'a Buffer'
    case 'table':
      return 'an object'
    default:
      return 'a number (but not NaN)'
  }
}

const wrongType = (a: Argument): string =>
  `throw new TypeError("Field '${a.name}' is the wrong type; must be ${description(a.type)}")`

/** The condition under which a value is not of the argument's type. */
const mismatch = (a: Argument): string | null => {
  switch (a.type) {
    case 'bit':
      return null
    case 'shortstr':
      return "typeof val !== 'string'"
    case 'longstr':
      return '!Buffer.isBuffer(val)'
    case 'table':
      return "typeof val !== 'object'"
    default:
      return "typeof val !== 'number' || Number.isNaN(val)"
  }
}

const defaultValue = (a: Argument): string =>
  a.type === 'longstr' ? `Buffer.from(${JSON.stringify(a.default)})` : JSON.stringify(a.default)

/** How many bytes the arguments take at most, leaving out long strings and tables. */
const bound = (args: Argument[]): number => {
  let size = 0
  let bits = 0

  for (const a of args) {
    if (a.type === 'bit') {
      if (bits++ % 8 === 0) size++

      continue
    }

    bits = 0

    switch (a.type) {
      case 'octet':
        size += 1
        break
      case 'short':
        size += 2
        break
      case 'long':
      case 'longstr':
      case 'table':
        size += 4
        break
      case 'longlong':
      case 'timestamp':
        size += 8
        break
      case 'shortstr':
        size += 256
        break
    }
  }

  return size
}

/** The statements that write a value already checked to be of the argument's type. */
const write = (a: Argument, bounded: number): string[] => {
  switch (a.type) {
    case 'octet':
      return ['buffer.writeUInt8(val, offset)', 'offset++']
    case 'short':
      return ['buffer.writeUInt16BE(val, offset)', 'offset += 2']
    case 'long':
      return ['buffer.writeUInt32BE(val, offset)', 'offset += 4']
    case 'longlong':
    case 'timestamp':
      return ['writeUInt64(buffer, val, offset)', 'offset += 8']
    case 'shortstr':
      return ['offset = writeShortString(buffer, val, offset)', `if (offset < 0) ${wrongType(a)}`]
    case 'longstr':
      return [
        `if (offset + val.length + ${bounded} > buffer.length) throw OVERFLOW`,
        'buffer.writeUInt32BE(val.length, offset)',
        'offset += 4',
        'offset += val.copy(buffer, offset)',
      ]
    case 'table':
      return [
        'offset += encodeTable(buffer, val, offset)',
        `if (offset + ${bounded} > buffer.length) throw OVERFLOW`,
      ]
    default:
      throw new Error(`Unexpected argument type: ${a.type}`)
  }
}

const fieldsInterface = (d: Definition, optional = ''): void => {
  emit(`export interface ${d.name}Fields {`)

  for (const a of d.args) emit(`${a.name}${optional}: ${tsType(a.type)}`)

  emit('}', '')
}

const methodEncoder = (m: Definition): void => {
  const bounded = bound(m.args) + 1

  emit(
    `export function encode${m.name}(buffer: Buffer, start: number, channel: number, fields: any): number {`,
    m.args.length > 0 ? 'let val: any' : '',
    m.args.some(a => a.type === 'bit') ? 'let bits = 0' : '',
    `buffer[start] = ${FRAME_METHOD}`,
    'buffer[start + 1] = channel >>> 8',
    'buffer[start + 2] = channel',
    `buffer[start + 7] = ${m.id >>> 24}`,
    `buffer[start + 8] = ${(m.id >>> 16) & 255}`,
    `buffer[start + 9] = ${(m.id >>> 8) & 255}`,
    `buffer[start + 10] = ${m.id & 255}`,
    'let offset = start + 11'
  )

  let bits = 0

  const flush = (): void => {
    if (bits > 0) emit('buffer[offset] = bits', 'offset++', 'bits = 0')

    bits = 0
  }

  for (const a of m.args) {
    if (a.type !== 'bit') flush()

    emit(`val = fields.${a.name}`)

    const missing =
      a.default === undefined
        ? `throw new Error("Missing value for mandatory field '${a.name}'")`
        : `val = ${defaultValue(a)}`

    const condition = mismatch(a)

    emit(`if (val === undefined) ${missing}`)

    if (condition !== null) emit(`else if (${condition}) ${wrongType(a)}`)

    if (a.type === 'bit') {
      emit(`if (val) bits += ${1 << bits}`)

      if (++bits === 8) flush()
    } else emit(...write(a, bounded))
  }

  flush()

  emit(
    `buffer[offset] = ${FRAME_END}`,
    'writeSize(buffer, start, offset - start - 7)',
    'return offset + 1',
    '}',
    ''
  )
}

const methodDecoder = (m: Definition): void => {
  if (m.args.length === 0) {
    emit(
      `export function decode${m.name}(_buffer: Buffer, _offset: number): ${m.name}Fields {`,
      'return {}',
      '}',
      ''
    )

    return
  }

  // Every argument is read into a constant, and the fields are made of them in one go: an object
  // written once is cheaper than one filled in field by field.
  emit(
    `export function decode${m.name}(buffer: Buffer, offset: number): ${m.name}Fields {`,
    m.args.some(a => a.type === 'longstr' || a.type === 'table') ? 'let end = 0' : ''
  )

  let bits = 0

  for (const a of m.args) {
    if (a.type !== 'bit' && bits > 0) {
      emit('offset++')
      bits = 0
    }

    emit(...read(a, bits, `const $${a.name}`))

    if (a.type === 'bit' && ++bits === 8) {
      emit('offset++')
      bits = 0
    }
  }

  // the last argument leaves the offset where nobody reads it
  while (lines.at(-1)!.startsWith('offset')) lines.pop()

  emit('return {', ...m.args.map(a => `${a.name}: $${a.name},`), '}', '}', '')
}

/** The statements that read an argument into its field. */
const read = (a: Argument, bit = 0, field = `fields.${a.name}`): string[] => {
  switch (a.type) {
    case 'bit':
      return [`${field} = (buffer[offset]! & ${1 << bit}) !== 0`]
    case 'octet':
      return [`${field} = buffer.readUInt8(offset)`, 'offset++']
    case 'short':
      return [`${field} = buffer.readUInt16BE(offset)`, 'offset += 2']
    case 'long':
      return [`${field} = buffer.readUInt32BE(offset)`, 'offset += 4']
    case 'longlong':
    case 'timestamp':
      return [`${field} = readUInt64(buffer, offset)`, 'offset += 8']
    case 'shortstr':
      return [`${field} = readShortString(buffer, offset)`, 'offset += 1 + buffer[offset]!']
    case 'longstr':
      return [
        'end = offset + 4 + buffer.readUInt32BE(offset)',
        `${field} = bytes(buffer, offset + 4, end)`,
        'offset = end',
      ]
    case 'table':
      return [
        'end = offset + 4 + buffer.readUInt32BE(offset)',
        `${field} = decodeFields(buffer, offset + 4, end)`,
        'offset = end',
      ]
    default:
      throw new TypeError(`Unexpected type in argument list: ${a.type}`)
  }
}

const flag = (index: number): number => 1 << (15 - index)

const propertiesEncoder = (p: Definition): void => {
  const bounded = bound(p.args) + 1

  emit(
    `export function encode${p.name}(buffer: Buffer, start: number, channel: number, size: number, fields: any): number {`,
    'let val: any',
    'let flags = 0',
    `buffer[start] = ${FRAME_HEADER}`,
    'buffer[start + 1] = channel >>> 8',
    'buffer[start + 2] = channel',
    `buffer[start + 7] = ${p.id >>> 8}`,
    `buffer[start + 8] = ${p.id & 255}`,
    'buffer[start + 9] = 0',
    'buffer[start + 10] = 0',
    'writeUInt64(buffer, size, start + 11)',
    'let offset = start + 21'
  )

  p.args.forEach((a, index) => {
    emit(`val = fields.${a.name}`, 'if (val !== undefined && val !== null) {')

    if (a.type === 'bit') emit(`if (val) flags += ${flag(index)}`)
    else
      emit(`if (${mismatch(a)}) ${wrongType(a)}`, `flags += ${flag(index)}`, ...write(a, bounded))

    emit('}')
  })

  emit(
    'buffer[start + 19] = flags >>> 8',
    'buffer[start + 20] = flags',
    `buffer[offset] = ${FRAME_END}`,
    'writeSize(buffer, start, offset - start - 7)',
    'return offset + 1',
    '}',
    ''
  )
}

const propertiesDecoder = (p: Definition): void => {
  emit(
    `export function decode${p.name}(buffer: Buffer, offset: number): ${p.name}Fields {`,
    'let end = 0',
    'const flags = buffer.readUInt16BE(offset)',
    `if (flags === 0) return {} as ${p.name}Fields`,
    'offset += 2',
    `const fields: ${p.name}Fields = {`,
    ...p.args.map(a => `${a.name}: undefined as any,`),
    '}'
  )

  p.args.forEach((a, index) => {
    emit(`if ((flags & ${flag(index)}) !== 0) {`)

    if (a.type === 'bit') emit(`fields.${a.name} = true`)
    else emit(...read(a))

    emit('}')
  })

  emit('return fields', '}', '')
}

const info = (d: Definition, prefix: string): void => {
  const value = { id: d.id, classId: d.classId, methodId: d.methodId, name: d.name, args: d.args }

  emit(`export const ${prefix}Info${d.name}: Info = ${JSON.stringify(value)}`, '')
}

const dispatch = (signature: string, cases: string[], otherwise: string): void => {
  emit(
    `export function ${signature} {`,
    'switch (id) {',
    ...cases,
    `default: throw new Error(${JSON.stringify(otherwise)})`,
    '}',
    '}',
    ''
  )
}

emit(
  '// This file is written by tools/generate.ts from the AMQP 0-9-1 specification: `npm run generate`.',
  '',
  '/* oxlint-disable */',
  '',
  "import { OVERFLOW, bytes, decodeFields, encodeTable, readUInt64, writeUInt64, type Table } from './codec.ts'",
  "import { readShortString, writeShortString, writeSize } from './strings.ts'",
  '',
  'export interface Info {',
  'id: number',
  'classId?: number',
  'methodId?: number',
  'name: string',
  'args: { type: string; name: string; default?: unknown }[]',
  '}',
  ''
)

const constants: Record<string, number> = {}
const constantNames: Record<number, string> = {}

for (const constant of spec.constants) {
  constants[constant.name.replace(/-/g, '_')] = constant.value
  constantNames[constant.value] = constant.name
}

const headroom = Math.max(...[...methods, ...propertieses].map(d => bound(d.args))) + 32

emit(
  `export const constants = ${JSON.stringify(constants)} as const`,
  '',
  `export const constant_strs: Record<number, string> = ${JSON.stringify(constantNames)}`,
  '',
  '/** The bytes of a frame that are not its payload: type, channel, size and the end octet. */',
  'export const FRAME_OVERHEAD = 8',
  '',
  '/**',
  ' * What an encoder may write before it checks the buffer for room: the most any frame takes',
  ' * without its long strings and tables. Whoever calls an encoder leaves this much free.',
  ' */',
  `export const HEADROOM = ${headroom}`,
  ''
)

dispatch(
  'decode(id: number, buffer: Buffer, offset = 0): any',
  [...methods, ...propertieses].map(d => `case ${d.id}: return decode${d.name}(buffer, offset)`),
  'Unknown class/method ID'
)

dispatch(
  'encodeMethod(id: number, buffer: Buffer, offset: number, channel: number, fields: any): number',
  methods.map(m => `case ${m.id}: return encode${m.name}(buffer, offset, channel, fields)`),
  'Unknown class/method ID'
)

dispatch(
  'encodeProperties(id: number, buffer: Buffer, offset: number, channel: number, size: number, fields: any): number',
  propertieses.map(
    p => `case ${p.id}: return encode${p.name}(buffer, offset, channel, size, fields)`
  ),
  'Unknown class/properties ID'
)

dispatch(
  'info(id: number): Info',
  [
    ...methods.map(m => `case ${m.id}: return methodInfo${m.name}`),
    ...propertieses.map(p => `case ${p.id}: return propertiesInfo${p.name}`),
  ],
  'Unknown class/method ID'
)

for (const m of methods) {
  emit(`export const ${m.name} = ${m.id}`, '')
  fieldsInterface(m)
  methodDecoder(m)
  methodEncoder(m)
  info(m, 'method')
}

for (const p of propertieses) {
  emit(`export const ${p.name} = ${p.id}`, '')
  fieldsInterface(p, '?')
  propertiesEncoder(p)
  propertiesDecoder(p)
  info(p, 'properties')
}

writeFileSync(join(import.meta.dirname, '../src/defs.ts'), lines.filter(l => l !== '').join('\n'))
