/**
 * JetStream manager service.
 *
 * @since 0.1.0
 */
import { Context, DateTime, Effect, Iterable, Layer, Option, Stream } from "effect";
import { jetstreamManager } from "@nats-io/jetstream";
import type { Input as DurationInput } from "effect/Duration";
import type {
  ConsumerConfig,
  ConsumerInfo,
  ConsumerUpdateConfig,
  JetStreamManager as SdkJetStreamManager,
  Lister,
  PurgeOpts,
  PurgeResponse,
  StreamConfig,
  StreamInfo,
  StreamUpdateConfig,
} from "@nats-io/jetstream";
import * as JetStream from "./JetStream.ts";
import * as JetStreamError from "./JetStreamError.ts";
import * as NatsClient from "./NatsClient.ts";
import * as NatsError from "./NatsError.ts";
import * as JsManagerOptions from "./internal/jsManagerOptions.ts";
import * as JsOptions from "./internal/jsOptions.ts";
import * as JsErrors from "./internal/mapJsError.ts";

export type StreamConfigInput = Omit<Partial<StreamConfig>, "name" | "max_age" | "duplicate_window"> & {
  readonly name: string;
  readonly max_age?: DurationInput;
  readonly duplicate_window?: DurationInput;
};

export type StreamUpdateInput = Omit<Partial<StreamUpdateConfig>, "max_age" | "duplicate_window"> & {
  readonly max_age?: DurationInput;
  readonly duplicate_window?: DurationInput;
};

export type ConsumerConfigInput = Omit<Partial<ConsumerConfig>, DurationConsumerFields> & DurationConsumerInput;

export type ConsumerUpdateInput = Omit<Partial<ConsumerUpdateConfig>, DurationConsumerFields> & DurationConsumerInput;

type DurationConsumerFields = "ack_wait" | "idle_heartbeat" | "max_expires" | "inactive_threshold" | "backoff";

type DurationConsumerInput = {
  readonly ack_wait?: DurationInput;
  readonly idle_heartbeat?: DurationInput;
  readonly max_expires?: DurationInput;
  readonly inactive_threshold?: DurationInput;
  readonly backoff?: ReadonlyArray<DurationInput>;
};

export type PauseOptions = {
  readonly until: DateTime.Utc;
};

export type PauseResponse = {
  readonly paused: boolean;
  readonly pause_until?: string;
};

export interface Service {
  readonly manager: SdkJetStreamManager;
  readonly streams: {
    readonly add: (config: StreamConfigInput) => Effect.Effect<StreamInfo, JetStreamError.JetStreamErrors>;
    readonly update: (
      name: string,
      config: StreamUpdateInput,
    ) => Effect.Effect<StreamInfo, JetStreamError.JetStreamErrors>;
    readonly info: (
      name: string,
    ) => Effect.Effect<StreamInfo, JetStreamError.StreamNotFoundError | JetStreamError.JetStreamApiError>;
    readonly delete: (name: string) => Effect.Effect<void, JetStreamError.JetStreamErrors>;
    readonly purge: (name: string, options?: PurgeOpts) => Effect.Effect<PurgeResponse, JetStreamError.JetStreamErrors>;
    readonly list: (subject?: string) => Stream.Stream<StreamInfo, JetStreamError.JetStreamErrors>;
    readonly names: (subject?: string) => Stream.Stream<string, JetStreamError.JetStreamErrors>;
  };
  readonly consumers: {
    readonly add: (
      stream: string,
      config: ConsumerConfigInput,
    ) => Effect.Effect<ConsumerInfo, JetStreamError.JetStreamErrors>;
    readonly update: (
      stream: string,
      durable: string,
      config: ConsumerUpdateInput,
    ) => Effect.Effect<ConsumerInfo, JetStreamError.JetStreamErrors>;
    readonly info: (
      stream: string,
      consumer: string,
    ) => Effect.Effect<ConsumerInfo, JetStreamError.ConsumerNotFoundError | JetStreamError.JetStreamApiError>;
    readonly delete: (stream: string, consumer: string) => Effect.Effect<void, JetStreamError.JetStreamErrors>;
    readonly list: (stream: string) => Stream.Stream<ConsumerInfo, JetStreamError.JetStreamErrors>;
    readonly pause: (
      stream: string,
      consumer: string,
      options: PauseOptions,
    ) => Effect.Effect<PauseResponse, JetStreamError.JetStreamErrors>;
    readonly resume: (stream: string, consumer: string) => Effect.Effect<PauseResponse, JetStreamError.JetStreamErrors>;
  };
}

export class JetStreamManager extends Context.Service<JetStreamManager, Service>()("effect-nats/JetStreamManager") {}

export const make = Effect.fnUntraced(function* (options: JetStream.JetStreamOptions = {}) {
  const nats = yield* NatsClient.NatsClient;
  const manager = yield* Effect.tryPromise({
    try: () => jetstreamManager(nats.connection, JsOptions.translateOptions(options)),
    catch: JsErrors.mapCreateManagerError,
  });
  return JetStreamManager.of({
    manager,
    streams: {
      add: (config) =>
        Effect.tryPromise({
          try: () => manager.streams.add(JsManagerOptions.translateStreamConfig(config)),
          catch: JsErrors.mapJetStreamError,
        }),
      update: (name, config) =>
        Effect.tryPromise({
          try: () => manager.streams.update(name, JsManagerOptions.translateStreamUpdate(config)),
          catch: JsErrors.mapJetStreamError,
        }),
      info: (name) =>
        Effect.tryPromise({
          try: () => manager.streams.info(name),
          catch: JsErrors.mapStreamInfoError(name),
        }),
      delete: (name) =>
        Effect.tryPromise({
          try: () => manager.streams.delete(name),
          catch: JsErrors.mapJetStreamError,
        }).pipe(Effect.asVoid),
      purge: (name, purgeOptions) =>
        Effect.tryPromise({
          try: () => manager.streams.purge(name, purgeOptions),
          catch: JsErrors.mapJetStreamError,
        }),
      list: (subject) => paginateLister(Effect.sync(() => manager.streams.list(subject))),
      names: (subject) => paginateLister(Effect.sync(() => manager.streams.names(subject))),
    },
    consumers: {
      add: (stream, config) =>
        Effect.tryPromise({
          try: () => manager.consumers.add(stream, JsManagerOptions.translateConsumerConfig(config)),
          catch: JsErrors.mapJetStreamError,
        }),
      update: (stream, durable, config) =>
        Effect.tryPromise({
          try: () => manager.consumers.update(stream, durable, JsManagerOptions.translateConsumerUpdate(config)),
          catch: JsErrors.mapJetStreamError,
        }),
      info: (stream, consumer) =>
        Effect.tryPromise({
          try: () => manager.consumers.info(stream, consumer),
          catch: JsErrors.mapConsumerInfoError({ stream, consumer }),
        }),
      delete: (stream, consumer) =>
        Effect.tryPromise({
          try: () => manager.consumers.delete(stream, consumer),
          catch: JsErrors.mapJetStreamError,
        }).pipe(Effect.asVoid),
      list: (stream) => paginateLister(Effect.sync(() => manager.consumers.list(stream))),
      pause: (stream, consumer, pauseOptions) =>
        Effect.tryPromise({
          try: () => manager.consumers.pause(stream, consumer, DateTime.toDateUtc(pauseOptions.until)),
          catch: JsErrors.mapJetStreamError,
        }),
      resume: (stream, consumer) =>
        Effect.tryPromise({
          try: () => manager.consumers.resume(stream, consumer),
          catch: JsErrors.mapJetStreamError,
        }),
    },
  });
});

const paginateLister = <A>(
  acquire: Effect.Effect<Lister<A>, JetStreamError.JetStreamErrors>,
): Stream.Stream<A, JetStreamError.JetStreamErrors> =>
  Stream.unwrap(
    acquire.pipe(
      Effect.map((lister) =>
        Stream.paginate(lister, (current) =>
          Effect.tryPromise({
            try: () => current.next(),
            catch: JsErrors.mapJetStreamError,
          }).pipe(
            Effect.map((page): readonly [ReadonlyArray<A>, Option.Option<Lister<A>>] => [
              page,
              Iterable.isEmpty(page) ? Option.none() : Option.some(current),
            ]),
          ),
        ),
      ),
    ),
  );

export const layer = (
  options: JetStream.JetStreamOptions = {},
): Layer.Layer<
  JetStreamManager,
  JetStreamError.JetStreamNotEnabledError | NatsError.TimeoutError,
  NatsClient.NatsClient
> => Layer.effect(JetStreamManager, make(options));

export { AckPolicy, DeliverPolicy, ReplayPolicy, RetentionPolicy, StorageType } from "@nats-io/jetstream";
export type { ConsumerInfo, StreamInfo };
