/**
 * Scenario: wildcard subscriptions (nats.js wildcard_subscriptions.ts).
 *
 * Run: `bun examples/wildcards.ts`
 */
import { Console, Effect, Fiber, Stream } from "effect";
import * as NatsClient from "effect-nats/NatsClient";
import { encoder, NatsLive, runMain } from "./_shared.ts";

const program = Effect.gen(function* () {
  const nats = yield* NatsClient.NatsClient;

  // `*` matches a single token; `>` matches the rest of the subject.
  const star = yield* nats
    .subscribe("time.*.east")
    .pipe(Stream.take(2), Stream.runCollect, Effect.forkScoped({ startImmediately: true }));

  const rest = yield* nats
    .subscribe("time.>")
    .pipe(Stream.take(3), Stream.runCollect, Effect.forkScoped({ startImmediately: true }));

  yield* nats.publish("time.us.east", { payload: encoder.encode("us-east") });
  yield* nats.publish("time.eu.east", { payload: encoder.encode("eu-east") });
  yield* nats.publish("time.us.west", { payload: encoder.encode("us-west") });

  const starMessages = yield* Fiber.join(star);
  const restMessages = yield* Fiber.join(rest);

  const result = {
    star: [...starMessages].map((message) => message.subject),
    rest: [...restMessages].map((message) => message.subject),
  };

  yield* Console.log(result);
  return result;
}).pipe(Effect.scoped, Effect.provide(NatsLive));

runMain({ label: "wildcards", effect: program });
