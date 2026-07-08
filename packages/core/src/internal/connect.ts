import { Effect } from "effect";
import * as Semaphore from "effect/Semaphore";
import type { ConnectionOptions, NatsConnection } from "@nats-io/nats-core";
import * as Errors from "./errors.ts";

const semaphore = Semaphore.makeUnsafe(1);

export const connect = (options: {
  readonly dial: (options: ConnectionOptions) => Promise<NatsConnection>;
  readonly connectionOptions: ConnectionOptions;
}) =>
  semaphore.withPermit(
    Effect.tryPromise({
      try: () => options.dial(options.connectionOptions),
      catch: Errors.mapConnectError,
    }),
  );
