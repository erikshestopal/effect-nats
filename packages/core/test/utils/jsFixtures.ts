import { Effect } from "effect";
import * as JetStream from "effect-nats/JetStream";

export const withStream = (options: { readonly name: string; readonly subjects: ReadonlyArray<string> }) =>
  Effect.acquireRelease(
    Effect.gen(function* () {
      const js = yield* JetStream.JetStream;
      const manager = yield* Effect.tryPromise(() => js.client.jetstreamManager(false));
      yield* Effect.tryPromise(() => manager.streams.add({ name: options.name, subjects: [...options.subjects] }));
      return manager;
    }),
    (manager) => Effect.tryPromise(() => manager.streams.delete(options.name)).pipe(Effect.ignore),
  );
