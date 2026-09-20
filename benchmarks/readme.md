# Benchmarks

What a message costs the process that takes it in or sends it: the CPU it spends, the garbage it
leaves, the bytes it copies, the socket writes it makes. Every benchmark runs the original
library and this one the same way, each in a process of its own.

A number here means something only beside another number from the same machine.

## Without a broker

```sh
node benchmarks/receive.ts [--sizes=100,1k,16k,64k,448k] [--ack] [--library=<name>]
node benchmarks/send.ts    [--sizes=100,1k,16k,64k,448k] [--headers] [--library=<name>]
```

`broker.ts` is a socket with a broker behind it in the same process: it answers what a client
needs answered to get to publishing and consuming. With no network and no broker in the way, what
is left to measure is the library.

`receive` hands each library the same bytes the way its socket would: the original gets a newly
allocated buffer per read of 64 KB, which is what Node does for it, and this library gets its one
read buffer filled again, which is what Node does when asked to. Deliveries come back to back, so
frames fall across reads as they do on a socket. It is measured from those bytes to the consumer's
callback; `--ack` makes the consumer acknowledge every message.

`send` publishes in bursts of 64 within a turn of the event loop, waits for `drain` when told to,
and is measured from `publish` to the bytes handed to the socket. `--headers` adds the properties
and headers a message usually carries. `writes` is socket writes per message.

| column                 | what it is                                                                      |
| ---------------------- | ------------------------------------------------------------------------------- |
| `CPU µs`               | user and system time of the process, per message                                |
| `GC µs`, `GC/1k`       | what garbage collection cost per message, and collections per thousand messages |
| `RSS MB`, `buffers MB` | resident set, and the bytes behind Buffers, when the run ended                  |

## With a broker

```sh
npm run rabbitmq
npm run benchmark [-- deliver.1k,turn.1k,publish.1k]
```

The process under measurement, `peer.ts`, is started on its own, since its CPU is what is read.
`deliver.*` take a message in and do nothing else; `turn.*` answer each message and acknowledge
it, which is the two frames a served request leaves in; `publish.*` send to a key nothing is bound
to, so that the broker drops what it gets. The driver sends at a fixed rate without waiting for
anything, which is how traffic arrives: a rate below what the process can take is the honest
place to read latency. Both rounds are printed rather than averaged, so a run that drifted shows.

| column                            | what it is                                                                |
| --------------------------------- | ------------------------------------------------------------------------- |
| `messages/s`                      | taken in, or sent, during the window                                      |
| `CPU µs`, `system µs`             | user and system time per message, and how much of it was the kernel's     |
| `faults`                          | page faults per message                                                   |
| `RSS MB`, `peak MB`, `buffers MB` | resident set on average and at most, and the bytes behind Buffers         |
| `copied KB`, `copies`             | bytes moved from one buffer to another per message, and in how many moves |
| `write`, `writev`                 | socket write calls per message                                            |
| `GC µs`, `GC/1k`                  | garbage collection per message, and collections per thousand messages     |
| `p50 ms`, `p99 ms`                | what the driver waited for a reply, where there is one                    |

`copied` counts every `Buffer.concat` and every `Buffer.copy`, so that replacing one with the
other shows as the difference it makes.

**The allocator is part of what is measured.** A process that allocates and frees tens of
kilobytes per message hands pages back to the system as often as glibc's trim threshold tells it
to, which shows as `system µs` and `faults`. Past 128 KB an allocation is mapped from the system
and unmapped again, every time, unless something larger was freed before it: the threshold follows
the largest mapping freed, and an allocation as large as the threshold is still mapped. A process
that takes in messages of one large size never gets past it. The environment separates the library
from the allocator:

```sh
MALLOC_MMAP_THRESHOLD_=33554432 MALLOC_TRIM_THRESHOLD_=67108864 npm run benchmark -- deliver.448k
```
