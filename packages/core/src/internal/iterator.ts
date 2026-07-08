import { Effect, Stream } from "effect";
import type { Scope } from "effect";

export interface QueuedIteratorLike<A> extends AsyncIterable<A> {
  readonly stop?: (error?: Error) => void;
}

export const streamFromQueuedIterator = <A, I extends QueuedIteratorLike<A>, B, E>(options: {
  readonly acquire: Effect.Effect<I, E, Scope.Scope>;
  readonly transform: (value: A) => B;
  readonly onError: (error: unknown) => E;
  readonly onRelease?: (iter: I) => Effect.Effect<void>;
}): Stream.Stream<B, E> =>
  Stream.scoped(
    Stream.fromEffect(
      Effect.acquireRelease(options.acquire, (iter) => options.onRelease?.(iter) ?? Effect.sync(() => iter.stop?.())),
    ).pipe(
      Stream.flatMap((iter) => Stream.fromAsyncIterable(iter, options.onError).pipe(Stream.map(options.transform))),
    ),
  );
