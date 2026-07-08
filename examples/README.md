# effect-nats examples

Effect-first programs that mirror common [nats.js](../repos/nats.js) scenarios
(pub/sub, headers, queues, JetStream, KV, ObjectStore, micro services) without
copying the SDK’s imperative style.

## Prerequisites

1. **NATS server** listening on `nats://127.0.0.1:4222` (override with `NATS_URL`).
2. **JetStream enabled** for JetStream / KV / ObjectStore examples:

   ```sh
   nats-server -js
   ```

   or Docker:

   ```sh
   docker run --rm -p 4222:4222 nats:latest -js
   ```

3. **Install workspace deps** from the repo root:

   ```sh
   bun install
   ```

## Run an example

From the repository root:

```sh
bun examples/quickstart.ts
bun examples/headers.ts
bun examples/queue-group.ts
bun examples/wildcards.ts
bun examples/connection-status.ts
bun examples/jetstream-publish.ts
bun examples/jetstream-manager.ts
bun examples/jetstream-pull.ts
bun examples/jetstream-consume.ts
bun examples/kv.ts
bun examples/object-store.ts
bun examples/micro.ts
```

Or via package scripts:

```sh
bun run example examples/quickstart.ts
```

Examples print a labeled success value (or error) and exit. They are also
included in the root `tsconfig.json` so `bun run typecheck` covers them.

## Shared helpers

[`_shared.ts`](./_shared.ts) provides:

- `NatsLive` — TCP client (`NodeConnector` + `NatsClient.layer`)
- `JetStreamLive` / `ManagerLive` / `FullJetStreamLive` — JetStream stacks on one connection
- `runMain` — `Effect.runPromise` wrapper for CLI entrypoints
- `natsUrl`, `encoder`, `decoder`

## Scenario map (nats.js → these files)

| Scenario (nats.js)                  | Example                |
| ----------------------------------- | ---------------------- |
| core basics / pub-sub / request     | `quickstart.ts`        |
| headers                             | `headers.ts`           |
| queue groups                        | `queue-group.ts`       |
| wildcard subscriptions              | `wildcards.ts`         |
| connection status / rtt             | `connection-status.ts` |
| JetStream publish + msgID / expect  | `jetstream-publish.ts` |
| JetStream manager streams/consumers | `jetstream-manager.ts` |
| next + fetch batches                | `jetstream-pull.ts`    |
| continuous consume + processWith    | `jetstream-consume.ts` |
| KV create/put/get/watch/history     | `kv.ts`                |
| ObjectStore put/get/list            | `object-store.ts`      |
| services multi-endpoint + discover  | `micro.ts`             |

## Notes

- Prefer **Layers** and **Stream** over manual subscribe loops.
- Prefer **`JsMessage.processWith`** for ack/nak/term instead of hand-rolled ack.
- JetStream examples create short-lived streams/buckets and clean up when possible.
- These are teaching programs, not benchmarks (skip nats.js `bench` ports).
