/**
 * Scenario: KV create/put/get/watch/history (nats.js kv/README.md).
 *
 * Requires `nats-server -js`.
 * Run: `bun examples/kv.ts`
 */
import { Console, Effect, Fiber, Option, Stream } from "effect";
import * as NatsKv from "effect-nats/NatsKv";
import { encoder, JetStreamLive, runMain } from "./_shared.ts";

const bucket = "EX_KV";

const program = Effect.gen(function* () {
  const kv = yield* NatsKv.create(bucket, { history: 5, storage: "memory" });

  const firstRevision = yield* kv.put("config.url", encoder.encode("nats://localhost"));
  yield* kv.put("config.url", encoder.encode("nats://127.0.0.1:4222"));

  const current = yield* kv.get("config.url").pipe(Effect.map(Option.getOrThrow));
  const missing = yield* kv.get("config.missing");
  const historical = yield* kv.get("config.url", { revision: firstRevision }).pipe(Effect.map(Option.getOrThrow));

  yield* kv.put("feature.a", encoder.encode("on"));
  yield* kv.put("feature.b", encoder.encode("off"));

  const keys = yield* kv.keys().pipe(Stream.runCollect);
  const history = yield* kv.history({ key: "config.url" }).pipe(Stream.runCollect);

  // Watch starts with the latest value for each key, then streams updates.
  const watchFiber = yield* kv
    .watch({ key: "feature.>" })
    .pipe(Stream.take(3), Stream.runCollect, Effect.forkScoped({ startImmediately: true }));
  yield* kv.put("feature.c", encoder.encode("maybe"));
  const watched = yield* Fiber.join(watchFiber);

  yield* Console.log("current", NatsKv.entryText(current));
  yield* Console.log(
    "history ops",
    [...history].map((entry) => entry.operation),
  );

  return {
    current: NatsKv.entryText(current),
    previous: NatsKv.entryText(historical),
    missing: Option.isNone(missing),
    keys: [...keys].sort(),
    watchKeys: [...watched].map((entry) => entry.key).sort(),
  };
}).pipe(Effect.scoped, Effect.provide(JetStreamLive));

runMain({ label: "kv", effect: program });
