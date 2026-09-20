# Contributing

## Working on a change

The tests run against a real RabbitMQ and fail without one:

```sh
npm run rabbitmq     # rabbitmq:4.2-alpine on 5672

npm install
npm run check
```

`URL` points them at another broker.

`npm test` is three suites:

- `test:unit` — the tests next to the sources. `src/integration.test.ts` among them is where what
  the library assumes of sockets, of TLS and of the broker is checked on them, along with what
  happens when a connection is cut or goes quiet.
- `test:compat` — the test suite of the original library, run against this one. `compat/test` is
  the `test` directory of [amqp-node/amqplib](https://github.com/amqp-node/amqplib) at `v2.0.1`
  as it is, and is not to be edited: it is replaced as a whole when the original moves.
  `compat/lib` stands where the original's modules were. What is not run is listed in
  `compat/exemptions.mts`, each with its reason.
- `test:types` — the original's type tests, `compat/types`, compiled against the build.

`src/defs.ts` is written by `npm run generate` from the protocol specification in `tools/`, and is
not to be edited either.

## Performance

Throughput and memory are what this library is for, so a change to how frames are read or written
comes with numbers from before and after it:

```sh
node benchmarks/receive.ts    # bytes to consumer, no broker
node benchmarks/send.ts       # publish to bytes, no broker
npm run benchmark             # with a broker, both libraries, scenario by scenario
```

`benchmarks/readme.md` says what each reports and how to read it.

Branch off `dev`, and open a pull request back into it. A pull request needs a
passing `check` run, an approving review, and every review thread resolved.

Commit messages must follow [conventional commits](https://www.conventionalcommits.org)
— `commit-msg` lints them locally, and they decide the next released version:
`fix:` a patch, `feat:` a minor. A `!` or a `BREAKING CHANGE:` footer also
releases a minor while the package is pre-1.0.

## Dependency bumps

Dependabot opens its bumps against `dev` weekly. Minor and patch bumps merge
themselves: the `dependabot` workflow approves each one and arms auto-merge, so
a bump lands the moment `check` goes green — and never lands without it. A
major bump waits for a review and a merge by hand, as does any bump whose
commit message does not name its update type.

The approval comes from the Actions token, so _Allow GitHub Actions to create
and approve pull requests_ has to stay on in the repository settings. To keep a
bump out, close it, or comment `@dependabot ignore this major version` on it —
that stops the next one as well.

## Releasing

Merge `dev` into `release` through a pull request. That is the whole release
procedure — `semantic-release` reads the commits since the last tag, and then
tags, publishes to npm, and writes the GitHub Release. Nothing is published
from a laptop, and no version number is edited by hand.

Both branches reject force-pushes and deletion, as do the `v*` tags: a released
version can never be moved or removed.
