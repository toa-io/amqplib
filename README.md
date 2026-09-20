# amqplib

An AMQP 0-9-1 client for RabbitMQ, for Node.js: the interface of
[amqplib](https://github.com/amqp-node/amqplib), built for throughput and a small memory
footprint.

> This library is a from-scratch implementation of the interface designed by
> [Michael Bridgen](https://github.com/squaremo) and the contributors to
> [amqp-node/amqplib](https://github.com/amqp-node/amqplib). It is a drop-in replacement and
> passes the original project's test suite.

```sh
npm install @toa.io/amqplib
```

```ts
import amqp from '@toa.io/amqplib'

const connection = await amqp.connect('amqp://localhost')
const channel = await connection.createChannel()

await channel.assertQueue('tasks')

await channel.consume('tasks', message => {
  if (message === null) return // the server cancelled the consumer

  console.log(message.content.toString())
  channel.ack(message)
})

channel.sendToQueue('tasks', Buffer.from('something to do'))
```

The callback interface is `@toa.io/amqplib/callback_api`. Both are documented by the original:
[amqp-node.github.io/amqplib](https://amqp-node.github.io/amqplib/channel_api.html).

## In place of amqplib

What is the same is checked rather than claimed. `compat/test` is the original's test suite at
`v2.0.1` as it is, and it runs against this library on every change, as do the original's type
tests, which compile against the types declared here under the names the original declares them
by: `Options.Publish`, `Replies.AssertQueue`, `ConsumeMessage` and the rest. Two things of that
suite are not run, and `compat/exemptions.mts` says why: the tests of the original's stream
multiplexer, an internal module that has no counterpart here, and one test that waits for the
original to write to a stream it has already ended.

What differs:

- It is an ES module, and needs Node.js 24. `require()` loads it as it loads any ES module.
- Only the two entry points can be imported. The original's `lib/` modules were never part of its
  interface, and there is nothing at those paths.
- `publish` also returns `false` once 4 MB are waiting to be written, whatever the channel's
  `highWaterMark`, and `drain` follows as it does otherwise.

## What it does differently

**A message owns its bytes.** `message.content` is a buffer of exactly the content's size, and
nothing else is kept alive by it. The socket reads into one buffer per connection, for as long as
the connection lives, instead of allocating one per read; content is copied out of it once,
straight into the message's buffer, however its frames were split across reads. Nothing is
concatenated, and no frame becomes an object on the way.

**What is sent during a turn of the event loop goes out in one write.** Frames are encoded one
after another into memory that is written to again once the socket is done with it, and handed to
the socket when the turn ends — or, for what is sent while handling what just arrived, before the
read that brought it returns. A reply to a message leaves in the same turn the message came in.
Content is copied when it is published, as the original copies it: the buffer given to `publish`
is the caller's again as soon as `publish` returns.

**Strings are found, not made.** A consumer tag comes with every delivery, and exchanges, routing
keys, content types and header names are few. A string seen before is found by its bytes rather
than decoded again.

## Measured

Against `amqplib@2.0.1`, on the same machine, one after the other; `benchmarks/readme.md` says
how, and what each column is.

With RabbitMQ 4.2 on the same host, per message, in the process that takes the messages in
(`deliver`), answers and acknowledges them (`turn`), or sends them (`publish`):

| scenario     | library         | CPU µs | copied KB | socket writes | GC/1k | RSS MB | p50 ms |
| ------------ | --------------- | -----: | --------: | ------------: | ----: | -----: | -----: |
| deliver.100  | amqplib         |    3.3 |       0.2 |          0.00 |   0.8 |     92 |      — |
| deliver.100  | @toa.io/amqplib |    2.2 |       0.1 |          0.00 |   0.3 |     89 |      — |
| deliver.1k   | amqplib         |    5.8 |       1.1 |          0.00 |   1.2 |     91 |      — |
| deliver.1k   | @toa.io/amqplib |    4.8 |       1.0 |          0.00 |   0.3 |     90 |      — |
| deliver.32k  | amqplib         |   25.6 |      33.1 |          0.00 |   2.6 |    107 |      — |
| deliver.32k  | @toa.io/amqplib |   22.7 |      32.0 |          0.00 |   1.6 |    123 |      — |
| deliver.64k  | amqplib         |   57.2 |     127.8 |          0.00 |   3.7 |    132 |      — |
| deliver.64k  | @toa.io/amqplib |   52.2 |      64.0 |          0.00 |   2.9 |    111 |      — |
| deliver.96k  | amqplib         |   74.9 |     160.4 |          0.00 |   4.5 |    136 |      — |
| deliver.96k  | @toa.io/amqplib |   71.6 |      96.0 |          0.00 |   5.0 |    130 |      — |
| deliver.448k | amqplib         |  490.5 |    1534.8 |          0.00 |  56.0 |    148 |      — |
| deliver.448k | @toa.io/amqplib |  273.9 |     448.0 |          0.00 |  20.0 |    121 |      — |
| turn.1k      | amqplib         |   16.2 |       3.2 |          2.00 |   3.6 |     91 |   1.93 |
| turn.1k      | @toa.io/amqplib |    9.7 |       2.0 |          0.41 |   0.6 |     92 |   0.50 |
| turn.448k    | amqplib         |  724.9 |    1982.8 |          6.00 |  72.0 |    146 |   1.07 |
| turn.448k    | @toa.io/amqplib |  575.8 |     896.0 |          1.00 |  36.0 |    101 |   1.06 |
| publish.100  | amqplib         |    4.6 |       0.4 |          1.00 |   0.6 |     93 |      — |
| publish.100  | @toa.io/amqplib |    0.8 |       0.1 |          0.02 |   0.1 |     90 |      — |
| publish.1k   | amqplib         |    5.4 |       2.2 |          1.00 |   0.8 |     95 |      — |
| publish.1k   | @toa.io/amqplib |    1.2 |       1.0 |          0.02 |   0.1 |     89 |      — |
| publish.64k  | amqplib         |   62.3 |      64.2 |          2.00 |   3.1 |    101 |      — |
| publish.64k  | @toa.io/amqplib |   39.1 |      64.0 |          0.67 |   1.4 |     87 |      — |
| publish.448k | amqplib         |  360.6 |     448.2 |          5.00 |  20.0 |    122 |      — |
| publish.448k | @toa.io/amqplib |  185.4 |     448.0 |          1.00 |   4.0 |     86 |      — |

With no broker and no network, which leaves the library alone. From the bytes a socket reads to
the consumer's callback:

| content, bytes | library         | CPU µs | GC/1k | RSS MB |
| -------------: | --------------- | -----: | ----: | -----: |
|            100 | amqplib         |   0.81 |  0.36 |    122 |
|            100 | @toa.io/amqplib |   0.29 |  0.12 |    107 |
|           1024 | amqplib         |   0.92 |  0.44 |    133 |
|           1024 | @toa.io/amqplib |   0.34 |  0.14 |    114 |
|          16384 | amqplib         |   6.89 |  1.41 |    183 |
|          16384 | @toa.io/amqplib |   2.38 |  0.47 |    169 |
|          65536 | amqplib         |  24.54 |  5.50 |    173 |
|          65536 | @toa.io/amqplib |   8.44 |  1.89 |    166 |
|         458752 | amqplib         | 227.49 | 49.53 |    184 |
|         458752 | @toa.io/amqplib |  67.14 | 12.44 |    152 |

And from `publish` to the bytes handed to the socket, in bursts of 64:

| content, bytes | library         | CPU µs | socket writes | RSS MB |
| -------------: | --------------- | -----: | ------------: | -----: |
|            100 | amqplib         |   2.90 |         0.022 |    166 |
|            100 | @toa.io/amqplib |   0.22 |         0.016 |    102 |
|           1024 | amqplib         |   3.28 |         0.133 |    187 |
|           1024 | @toa.io/amqplib |   0.26 |         0.016 |    100 |
|          16384 | amqplib         |   5.89 |         1.991 |    205 |
|          16384 | @toa.io/amqplib |   1.33 |         0.016 |    167 |
|          65536 | amqplib         |  16.00 |         1.981 |    249 |
|          65536 | @toa.io/amqplib |  11.43 |         0.016 |    163 |
|         458752 | amqplib         | 102.23 |         5.000 |    401 |
|         458752 | @toa.io/amqplib |  74.38 |         0.016 |    204 |

With every message acknowledged, the original takes 2.28 µs and 832 MB to receive 100-byte
messages where this library takes 0.35 µs and 108 MB.

Node.js 24.21 on a Ryzen 7 7800X3D, Linux 7.0.

**Large messages and the allocator.** A message's content is one allocation of its size, and how
glibc serves allocations of hundreds of kilobytes decides what receiving them costs: by default
each one is mapped from the system and handed back, page fault by page fault. Telling it to keep
the memory, which is a decision for the application rather than for a library, takes
`deliver.448k` from 274 µs to 211 (the original: 490 to 445) and `turn.448k` from 576 µs to 415
(the original: 725 to 718):

```sh
MALLOC_MMAP_THRESHOLD_=33554432 MALLOC_TRIM_THRESHOLD_=67108864 node service.js
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
