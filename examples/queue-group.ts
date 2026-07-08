/**
 * Scenario: queue groups for load-balanced subscribers (nats.js queuegroups.ts).
 *
 * Run: `bun examples/queue-group.ts`
 */
import { Array as Arr, Console, Effect, Number as Num, Ref, Stream } from "effect";
import * as NatsClient from "effect-nats/NatsClient";
import { encoder, NatsLive, runMain } from "./_shared.ts";

const program = Effect.gen(function* () {
  const nats = yield* NatsClient.NatsClient;
  const workerA = yield* Ref.make(0);
  const workerB = yield* Ref.make(0);

  const worker = (options: { readonly name: "A" | "B"; readonly counter: Ref.Ref<number> }) =>
    nats
      .subscribe("jobs.echo", { queue: "workers" })
      .pipe(
        Stream.mapEffect((message) =>
          Ref.update(options.counter, Num.increment).pipe(
            Effect.andThen(Console.log(`worker ${options.name} got`, message.text)),
          ),
        ),
      );

  const collect = worker({ name: "A", counter: workerA }).pipe(
    Stream.merge(worker({ name: "B", counter: workerB })),
    Stream.take(4),
    Stream.runDrain,
  );

  // Four messages into one queue group — each is delivered to exactly one worker.
  const publish = Effect.forEach(
    Arr.makeBy(4, (index) => String(index)),
    (payload) => nats.publish("jobs.echo", { payload: encoder.encode(payload) }),
  );

  yield* Effect.all([collect, Effect.yieldNow.pipe(Effect.andThen(publish))], { concurrency: "unbounded" });

  const counts = {
    A: yield* Ref.get(workerA),
    B: yield* Ref.get(workerB),
  };

  yield* Console.log("delivery counts (should sum to 4)", counts);
  return counts;
}).pipe(Effect.scoped, Effect.provide(NatsLive));

runMain({ label: "queue-group", effect: program });
