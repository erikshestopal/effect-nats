/**
 * Declarative NATS services framework wrapper.
 *
 * @since 0.1.0
 */
import { Array as Arr, Effect, Layer, Option, Predicate, Record as Rec, Schema, Scope, Stream } from "effect";
import { ServiceError, ServiceErrorCodeHeader, ServiceErrorHeader, Svcm } from "@nats-io/services/internal";
import type { Payload, QueuedIterator } from "@nats-io/nats-core";
import type {
  Service as SdkService,
  ServiceConfig as SdkServiceConfig,
  ServiceIdentity,
  ServiceInfo,
  ServiceMsg,
  ServiceStats,
} from "@nats-io/services/internal";
import * as NatsClient from "./NatsClient.ts";
import * as NatsError from "./NatsError.ts";
import * as NatsMessage from "./NatsMessage.ts";
import * as Errors from "./internal/errors.ts";
import * as Iterators from "./internal/iterator.ts";

/** @since 0.1.0 @category errors */
export class EndpointError extends Schema.TaggedErrorClass<EndpointError>("effect-nats/NatsMicro/EndpointError")(
  "EndpointError",
  {
    code: Schema.Int,
    description: Schema.String,
  },
) {}

/** @since 0.1.0 @category options */
export type Endpoint<R> = {
  readonly handler: (msg: NatsMessage.NatsMessage) => Effect.Effect<Payload | void, EndpointError, R>;
  readonly subject?: string;
  readonly queue?: string;
  readonly metadata?: Readonly<Record<string, string>>;
  readonly concurrency?: number | "unbounded";
};

/** @since 0.1.0 @category options */
export type ServiceOptions<R> = {
  readonly name: string;
  readonly version: string;
  readonly description?: string;
  readonly metadata?: Readonly<Record<string, string>>;
  readonly queue?: string;
  readonly endpoints: Readonly<Record<string, Endpoint<R>>>;
};

/** @since 0.1.0 @category models */
export type RunningService = {
  readonly service: SdkService;
  readonly info: ServiceInfo;
  readonly stopped: Effect.Effect<Option.Option<NatsError.NatsError>>;
};

/** @since 0.1.0 @category options */
export type DiscoverOptions = {
  readonly name?: string;
  readonly id?: string;
};

/** @since 0.1.0 @category models */
export interface MicroClient {
  readonly ping: (options?: DiscoverOptions) => Stream.Stream<ServiceIdentity, NatsError.NatsError>;
  readonly info: (options?: DiscoverOptions) => Stream.Stream<ServiceInfo, NatsError.NatsError>;
  readonly stats: (options?: DiscoverOptions) => Stream.Stream<ServiceStats, NatsError.NatsError>;
}

/** @since 0.1.0 @category constructors */
export const make = <R>(
  options: ServiceOptions<R>,
): Effect.Effect<RunningService, NatsError.NatsError, NatsClient.NatsClient | Scope.Scope | R> =>
  Effect.gen(function* () {
    const nats = yield* NatsClient.NatsClient;
    const service = yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () => new Svcm(nats.connection).add(translateConfig(options)),
        catch: Errors.mapError,
      }),
      release,
    );
    const errorCounts: Record<string, number> = {};
    patchStats({ service, errorCounts });
    yield* Effect.all(
      Arr.map(Rec.toEntries(options.endpoints), ([name, endpoint]) =>
        runEndpoint({ service, name, endpoint, errorCounts }),
      ),
    );
    return {
      service,
      info: service.info(),
      /* v8 ignore next -- stopped resolves when the owning scope closes */
      stopped: Effect.promise(() => service.stopped).pipe(
        Effect.map((cause) => (Predicate.isNull(cause) ? Option.none() : Option.some(Errors.mapError(cause)))),
      ),
    };
  });

/**
 * Runs a declarative NATS service for the layer lifetime.
 *
 * @example
 * ```ts
 * import { Effect } from "effect"
 * import * as NatsMicro from "effect-nats/NatsMicro"
 *
 * const Echo = NatsMicro.layer({
 *   name: "echo",
 *   version: "1.0.0",
 *   endpoints: {
 *     echo: { handler: (message) => Effect.succeed(message.payload) }
 *   }
 * })
 * ```
 *
 * @see {@link make} for constructing and returning the running service
 *
 * @since 0.1.0
 * @category layers
 */
export const layer = <R>(
  options: ServiceOptions<R>,
): Layer.Layer<never, NatsError.NatsError, NatsClient.NatsClient | R> => Layer.effectDiscard(make(options));

/** @since 0.1.0 @category constructors */
export const client: Effect.Effect<MicroClient, never, NatsClient.NatsClient> = Effect.map(
  NatsClient.NatsClient,
  (nats) => {
    const sdk = new Svcm(nats.connection).client();
    return {
      ping: (options = {}) => discover(() => sdk.ping(options.name, options.id)),
      info: (options = {}) => discover(() => sdk.info(options.name, options.id)),
      stats: (options = {}) => discover(() => sdk.stats(options.name, options.id)),
    };
  },
);

const runEndpoint = <R>(options: {
  readonly service: SdkService;
  readonly name: string;
  readonly endpoint: Endpoint<R>;
  readonly errorCounts: Record<string, number>;
}): Effect.Effect<void, never, R | Scope.Scope> =>
  Iterators.streamFromQueuedIterator<ServiceMsg, QueuedIterator<ServiceMsg>, ServiceMsg, NatsError.NatsError>({
    acquire: Effect.sync(() =>
      options.service.addEndpoint(options.name, {
        ...(Predicate.isNotUndefined(options.endpoint.subject) ? { subject: options.endpoint.subject } : {}),
        ...(Predicate.isNotUndefined(options.endpoint.queue) ? { queue: options.endpoint.queue } : {}),
        ...(Predicate.isNotUndefined(options.endpoint.metadata) ? { metadata: options.endpoint.metadata } : {}),
      }),
    ),
    transform: (message) => message,
    onError: Errors.mapError,
  }).pipe(
    Stream.mapEffect(
      handleOne({
        endpoint: options.endpoint,
        countError: () => incrementError({ errorCounts: options.errorCounts, name: options.name }),
      }),
      {
        concurrency: options.endpoint.concurrency ?? 1,
      },
    ),
    Stream.runDrain,
    Effect.forkScoped({ startImmediately: true }),
    Effect.asVoid,
  );

const handleOne =
  <R>(options: { readonly endpoint: Endpoint<R>; readonly countError: () => void }) =>
  (message: ServiceMsg): Effect.Effect<void, never, R> =>
    options.endpoint.handler(NatsMessage.fromMsg(message)).pipe(
      Effect.flatMap((payload) =>
        isPayload(payload) ? Effect.sync(() => message.respond(payload)).pipe(Effect.asVoid) : Effect.void,
      ),
      Effect.catchTag("EndpointError", (error) =>
        Effect.sync(() => {
          options.countError();
          message.respondError(error.code, error.description);
        }),
      ),
      Effect.catchCause((cause) =>
        Effect.logError(cause).pipe(
          Effect.andThen(
            Effect.sync(() => {
              options.countError();
              message.respondError(500, "internal error");
            }),
          ),
          Effect.asVoid,
        ),
      ),
    );

const discover = <A>(acquire: () => Promise<QueuedIterator<A>>): Stream.Stream<A, NatsError.NatsError> =>
  Iterators.streamFromQueuedIterator<A, QueuedIterator<A>, A, NatsError.NatsError>({
    acquire: Effect.tryPromise({
      try: acquire,
      catch: Errors.mapError,
    }),
    transform: (value) => value,
    onError: Errors.mapError,
  });

const isPayload = (payload: Payload | void): payload is Payload => Predicate.isNotUndefined(payload);

const translateConfig = <R>(options: ServiceOptions<R>): SdkServiceConfig => ({
  name: options.name,
  version: options.version,
  ...(Predicate.isNotUndefined(options.description) ? { description: options.description } : {}),
  ...(Predicate.isNotUndefined(options.metadata) ? { metadata: options.metadata } : {}),
  ...(Predicate.isNotUndefined(options.queue) ? { queue: options.queue } : {}),
});

const patchStats = (options: { readonly service: SdkService; readonly errorCounts: Record<string, number> }): void => {
  const stats = options.service.stats.bind(options.service);
  options.service.stats = () =>
    stats().then((serviceStats) => ({
      ...serviceStats,
      ...Option.fromNullishOr(serviceStats.endpoints).pipe(
        Option.match({
          /* v8 ignore next -- SDK stats always include endpoints after service construction */
          onNone: () => ({}),
          onSome: (endpoints) => ({
            endpoints: Arr.map(endpoints, (endpoint) => ({
              ...endpoint,
              num_errors: endpoint.num_errors + (options.errorCounts[endpoint.name] ?? 0),
            })),
          }),
        }),
      ),
    }));
};

const incrementError = (options: { readonly errorCounts: Record<string, number>; readonly name: string }): void => {
  const { errorCounts, name } = options;
  errorCounts[name] = (errorCounts[name] ?? 0) + 1;
};

const release = (service: SdkService): Effect.Effect<void> =>
  Effect.tryPromise(() => service.stop()).pipe(Effect.asVoid, Effect.ignore);

/** @since 0.1.0 @category errors */
export { ServiceError, ServiceErrorCodeHeader, ServiceErrorHeader };

/** @since 0.1.0 @category models */
export type { ServiceIdentity, ServiceInfo, ServiceStats };
