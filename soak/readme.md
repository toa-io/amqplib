# Soak

Publishes messages of random size, from nothing to several megabytes, over several kinds of
connection at once, and checks every delivery for the exact bytes the publisher sent, in the order
it sent them. It catches what tests against a fixed byte stream cannot: framing mistakes that only
show under real sockets, TLS records, heartbeats interleaved with content, and awkward chunk
boundaries.

This is [cressie176/amqplib-soak](https://github.com/cressie176/amqplib-soak) at `08348d9`, under
its MIT licence (`LICENSE-MIT`), as it is except for what it loads: `lib/consumer.js` and
`lib/publisher.js` require `../amqplib`, which is this repository's `src`, and `lib/version.js`
names this package in the report.

## What it checks

For every message delivered:

- the body is a `Buffer` of the length the publisher recorded in the headers;
- its SHA-256 matches the one the publisher computed before sending;
- its sequence number is the one after the previous message on that queue;
- after being held for a while (500 ms by default) the body still has the same length and hash. A
  body that is a view of a socket buffer can be right on arrival and wrong once that buffer is
  reused, and this is the check that would notice.

At the end, every message that was published must have arrived. A run that loses any exits
non-zero.

## Transports

Each transport gets its own connection on the publisher and on the consumer, with a channel and a
queue per `--channels`.

| transport | what it exercises                                                                                                                                                                   |
| --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `plain`   | a TCP socket                                                                                                                                                                        |
| `tls`     | an `amqps://` connection through the broker's TLS listener, chunks of TLS-record size                                                                                               |
| `chunked` | a local proxy that hands the client what the broker sent in pieces of random length, a third of them under 17 bytes, so frame headers and frame-end bytes constantly straddle reads |

## Running

Needs Docker with Compose and `openssl` on the path.

```sh
npm run soak:broker        # a throwaway CA and certificate, RabbitMQ 4 with TLS on 5671
npm run soak               # 60 seconds, all transports, 8 channels each
npm run soak -- --frame-max 8192 --heartbeat 1
npm run soak -- --help
```

The broker listens on 5672 as well, so stop the one `npm run rabbitmq` starts first, and stop this
one with `docker compose -f soak/docker-compose.yml down`. The harness's own tests are
`node --test 'soak/test/*.test.js'`.

| option         | default             | why you might change it                                                                                          |
| -------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `--duration`   | `60s`               | longer runs shake out rarer coincidences of boundaries                                                           |
| `--transports` | `plain,tls,chunked` | isolate one                                                                                                      |
| `--channels`   | `8`                 | more concurrency, more interleaving of frames across channels                                                    |
| `--max-size`   | `8MB`               | the largest body                                                                                                 |
| `--frame-max`  | `131072`            | `8192`, the least RabbitMQ 4 accepts, makes a 1 MB body 128 content frames                                       |
| `--heartbeat`  | `5`                 | `1` puts a heartbeat frame in the middle of most large messages                                                  |
| `--seed`       | random              | repeat the same message sizes in the same order; the proxy's cuts depend on timing, so those differ between runs |
| `--hold-ms`    | `500`               | how long deliveries are kept before their second check                                                           |

Sizes over-represent bodies whose length is within three bytes of a multiple of the frame size or of
64 KB, and bodies of zero length, which the broker sends with no content frame at all.

## Reading the report

One row per process:

| column             | meaning                                                                  |
| ------------------ | ------------------------------------------------------------------------ |
| messages, MB       | what that process published or consumed                                  |
| msg/s, MB/s        | over the process's whole life, including connection setup                |
| CPU ms, CPU µs/msg | user plus system time of the process                                     |
| GC, GC ms, GC %    | collections, time in them, and that time as a share of CPU               |
| peak RSS MB        | the most resident memory the process reached                             |
| refused            | publishes the broker turned away because the queue was full, all retried |
| rechecked          | deliveries verified a second time after being held                       |

The publisher keeps one message in flight per queue, which is what makes the ordering check exact,
and the consumer hashes every body twice: the CPU columns are not a benchmark of the library.

Exit codes: `0` every check passed, `1` at least one check failed (details are printed), `2` the
run could not complete, for instance because the broker was unreachable.
