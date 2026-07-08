/**
 * Scenario: core pub/sub + request/reply (nats.js basics / nats-pub / nats-req).
 *
 * Run: `bun examples/quickstart.ts`
 */
import { Console, Effect, Fiber, Stream } from "effect";
import * as NatsClient from "effect-nats/NatsClient";
import * as NatsMessage from "effect-nats/NatsMessage";
import { encoder, NatsLive, runMain } from "./_shared.ts";

const program = Effect.gen(function* () {
  const nats = yield* NatsClient.NatsClient;

  // Request handler: echo payload as text.
  yield* nats.subscribe("rpc.echo").pipe(
    Stream.take(1),
    Stream.mapEffect((message) => NatsMessage.respond(message, { payload: encoder.encode(`echo:${message.text}`) })),
    Stream.runDrain,
    Effect.forkScoped({ startImmediately: true }),
  );

  // Pub/sub: take one message after publish.
  const sub = yield* nats
    .subscribe("events.created")
    .pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped({ startImmediately: true }));

  yield* nats.publish("events.created", { payload: encoder.encode("hello") });
  const events = yield* Fiber.join(sub);

  const reply = yield* nats.request("rpc.echo", { payload: encoder.encode("ping") });

  yield* Console.log(
    "events",
    [...events].map((message) => message.text),
  );
  yield* Console.log("reply", reply.text);

  return { eventCount: events.length, reply: reply.text };
}).pipe(Effect.scoped, Effect.provide(NatsLive));

runMain({ label: "quickstart", effect: program });
