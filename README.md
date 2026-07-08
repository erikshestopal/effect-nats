# effect-nats

Effect-native wrappers for the NATS JavaScript SDK. The package keeps the upstream SDK as the transport and protocol source of truth, while exposing scoped connections, typed errors, `Stream`-based subscriptions, JetStream, KV, ObjectStore, and the NATS services framework as Effect values.

See the full design contract in [`docs/DESIGN.html`](docs/DESIGN.html).

Runnable, Effect-first programs live in [`examples/`](examples/) (see [`examples/README.md`](examples/README.md) for how to run them against a local `nats-server`). The snippets below are the short form of those scenarios.

## Quickstart

```ts
import { Effect, Layer, Stream } from "effect";
import * as NatsClient from "effect-nats/NatsClient";
import * as NodeConnector from "effect-nats/NodeConnector";

const NatsLive = NatsClient.layer({ servers: "nats://127.0.0.1:4222" }).pipe(Layer.provide(NodeConnector.layer));

const program = Effect.gen(function* () {
  const nats = yield* NatsClient.NatsClient;

  yield* nats.publish("events.created", { payload: new TextEncoder().encode("hello") });

  const fiber = yield* nats
    .subscribe("events.created")
    .pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped({ startImmediately: true }));

  return yield* fiber.await;
}).pipe(Effect.scoped, Effect.provide(NatsLive));
```

## Request/reply

```ts
const requestProgram = Effect.gen(function* () {
  const nats = yield* NatsClient.NatsClient;
  const response = yield* nats.request("rpc.echo", {
    payload: new TextEncoder().encode("ping"),
  });
  return response.text;
});
```

## JetStream consume

```ts
import * as JetStream from "effect-nats/JetStream";
import * as JsMessage from "effect-nats/JsMessage";

const consumeProgram = Effect.gen(function* () {
  const js = yield* JetStream.JetStream;
  const consumer = yield* js.consumer("ORDERS", "processor");

  yield* consumer.consume({ maxMessages: 10 }).pipe(
    Stream.mapEffect((msg) =>
      Effect.gen(function* () {
        // process msg.payload
        const messages = yield* JsMessage.JsMessageService;
        yield* messages.ack(msg);
      }),
    ),
    Stream.runDrain,
  );
});
```

## NATS services framework

```ts
import * as NatsMicro from "effect-nats/NatsMicro";

const EchoService = NatsMicro.layer({
  name: "echo",
  version: "1.0.0",
  endpoints: {
    echo: {
      handler: (msg) => Effect.succeed(msg.payload),
    },
  },
});
```

## Transports

| Runtime      | TCP                   | WebSocket                      |
| ------------ | --------------------- | ------------------------------ |
| Node.js ≥ 22 | `NodeConnector.layer` | `NatsConnector.layerWebSocket` |
| Bun          | `NodeConnector.layer` | `NatsConnector.layerWebSocket` |
| Browser      | not supported         | `NatsConnector.layerWebSocket` |

## Commands

```sh
bun install
bun run typecheck
bunx --bun vp test run
bunx --bun vp test run --coverage
vp run --log labeled check:all
```

`check:all` runs lint, ast-grep, tests, coverage, and `tsgo --noEmit`.
