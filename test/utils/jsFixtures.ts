import { Effect, Predicate } from "effect";
import * as JetStream from "effect-nats/JetStream";
import { AckPolicy, DeliverPolicy } from "@nats-io/jetstream";

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

export const withConsumer = (options: {
  readonly stream: string;
  readonly name: string;
  readonly ackWaitNanos?: number;
}) =>
  Effect.acquireRelease(
    Effect.gen(function* () {
      const js = yield* JetStream.JetStream;
      const manager = yield* Effect.tryPromise(() => js.client.jetstreamManager(false));
      yield* Effect.tryPromise(() =>
        manager.consumers.add(options.stream, {
          durable_name: options.name,
          deliver_policy: DeliverPolicy.All,
          ack_policy: AckPolicy.Explicit,
          ...(Predicate.isUndefined(options.ackWaitNanos) ? {} : { ack_wait: options.ackWaitNanos }),
        }),
      );
      return manager;
    }),
    (manager) => Effect.tryPromise(() => manager.consumers.delete(options.stream, options.name)).pipe(Effect.ignore),
  );
