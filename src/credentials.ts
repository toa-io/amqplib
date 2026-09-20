import { encodeTable } from './codec.ts'

export interface Credentials {
  mechanism: string
  response(): Buffer
  username?: string
  password?: string
}

const NUL = String.fromCharCode(0)

export function plain(user: string, passwd: string): Credentials {
  return {
    mechanism: 'PLAIN',
    response: () => Buffer.from(['', user, passwd].join(NUL)),
    username: user,
    password: passwd,
  }
}

export function amqplain(user: string, passwd: string): Credentials {
  return {
    mechanism: 'AMQPLAIN',
    response: () => {
      const buffer = Buffer.alloc(16384)
      const size = encodeTable(buffer, { LOGIN: user, PASSWORD: passwd }, 0)

      return buffer.subarray(4, size)
    },
    username: user,
    password: passwd,
  }
}

export function external(): Credentials {
  return {
    mechanism: 'EXTERNAL',
    response: () => Buffer.from(''),
  }
}
