/**
 * Scoped NATS client service.
 *
 * @since 0.1.0
 */
import { Config, Context, Duration, Effect, Layer, Option, Predicate, Schema, Scope, Stream } from "effect";
import type {
  ConnectionOptions,
  Msg,
  NatsConnection,
  Payload,
  Stats,
  Status,
  Subscription,
  TlsOptions,
} from "@nats-io/nats-core";
import type { Input as DurationInput } from "effect/Duration";
import * as NatsHeaders from "./NatsHeaders.ts";
import * as NatsMessage from "./NatsMessage.ts";
import * as NatsConnector from "./NatsConnector.ts";
import * as NatsError from "./NatsError.ts";
import * as Errors from "./internal/errors.ts";
import * as Iterator from "./internal/iterator.ts";
import * as OptionsInternal from "./internal/options.ts";

/** @since 0.1.0 @category models */
export type ConnectionStatus = Status;

/** @since 0.1.0 @category models */
export type { Stats };

/**
 * Runtime NATS connection operations.
 *
 * @example
 * ```ts
 * import { Effect, Stream } from "effect"
 * import * as NatsClient from "effect-nats/NatsClient"
 *
 * const program = Effect.gen(function*() {
 *   const nats = yield* NatsClient.NatsClient
 *   yield* nats.publish("events.created", { payload: new TextEncoder().encode("hello") })
 *   const response = yield* nats.request("rpc.echo")
 *   yield* nats.subscribe("events.created").pipe(Stream.take(1), Stream.runDrain)
 *   return response.text
 * })
 * ```
 *
 * @since 0.1.0
 * @category models
 */
export interface Service {
  readonly connection: NatsConnection;
  readonly closed: Effect.Effect<Option.Option<NatsError.NatsError>>;
  readonly publish: (
    subject: string,
    options?: PublishOptions,
  ) => Effect.Effect<
    void,
    | NatsError.InvalidSubjectError
    | NatsError.ClosedConnectionError
    | NatsError.DrainingConnectionError
    | NatsError.PermissionViolationError
  >;
  readonly flush: Effect.Effect<void, NatsError.ClosedConnectionError | NatsError.TimeoutError>;
  readonly rtt: Effect.Effect<
    Duration.Duration,
    NatsError.ClosedConnectionError | NatsError.DrainingConnectionError | NatsError.TimeoutError
  >;
  readonly request: (
    subject: string,
    options?: RequestOptions,
  ) => Effect.Effect<
    NatsMessage.NatsMessage,
    | NatsError.TimeoutError
    | NatsError.NoRespondersError
    | NatsError.RequestError
    | NatsError.InvalidSubjectError
    | NatsError.ClosedConnectionError
    | NatsError.DrainingConnectionError
  >;
  readonly subscribe: (
    subject: string,
    options?: SubscribeOptions,
  ) => Stream.Stream<NatsMessage.NatsMessage, NatsError.NatsError>;
  readonly subscribeRaw: (subject: string, options?: SubscribeOptions) => Stream.Stream<Msg, NatsError.NatsError>;
  readonly requestMany: (
    subject: string,
    options: RequestManyOptions,
  ) => Stream.Stream<NatsMessage.NatsMessage, NatsError.NatsError>;
  readonly status: Stream.Stream<ConnectionStatus>;
  readonly stats: Effect.Effect<Stats>;
}

/** @since 0.1.0 @category options */
export type PublishOptions = {
  readonly payload?: Payload;
  readonly headers?: NatsHeaders.Input;
  readonly replyTo?: string;
};

/** @since 0.1.0 @category options */
export type RequestOptions = {
  readonly payload?: Payload;
  readonly headers?: NatsHeaders.Input;
  /**
   * The request deadline. Interrupting the returned Effect abandons the SDK promise; it does not cancel the
   * server-side interest.
   *
   * @default "1 second"
   */
  readonly timeout?: DurationInput;
};

/** @since 0.1.0 @category options */
export type SubscribeOptions = {
  readonly queue?: string;
  readonly max?: number;
};

/** @since 0.1.0 @category options */
export type RequestManyOptions = {
  readonly payload?: Payload;
  readonly headers?: NatsHeaders.Input;
  readonly maxWait: DurationInput;
  readonly maxMessages?: number;
  readonly stall?: DurationInput;
  readonly sentinel?: boolean;
};

/**
 * Service tag for a scoped NATS connection.
 *
 * @see {@link make} for constructing the service effectfully
 * @see {@link layer} for providing it from explicit options
 * @see {@link layerConfig} for providing it from `Config`
 *
 * @since 0.1.0
 * @category services
 */
export class NatsClient extends Context.Service<NatsClient, Service>()("effect-nats/NatsClient") {}

/** @since 0.1.0 @category options */
export type Options = {
  readonly servers?: string | ReadonlyArray<string>;
  readonly name?: string;
  readonly auth?: Auth;
  readonly timeout?: DurationInput;
  readonly pingInterval?: DurationInput;
  readonly maxPingOut?: number;
  readonly reconnect?: boolean | ReconnectOptions;
  readonly noEcho?: boolean;
  readonly inboxPrefix?: string;
  readonly tls?: TlsOptions;
  readonly ignoreClusterUpdates?: boolean;
  readonly transformOptions?: (options: ConnectionOptions) => ConnectionOptions;
};

/** @since 0.1.0 @category options */
export type ReconnectOptions = {
  readonly maxAttempts?: number;
  readonly wait?: DurationInput;
  readonly jitter?: DurationInput;
  readonly waitOnFirstConnect?: boolean;
};

/** @since 0.1.0 @category options */
export class UserPass extends Schema.TaggedClass<UserPass>()("UserPass", {
  user: Schema.String,
  pass: Schema.Redacted(Schema.String),
}) {}

/** @since 0.1.0 @category options */
export class Token extends Schema.TaggedClass<Token>()("Token", {
  token: Schema.Redacted(Schema.String),
}) {}

/** @since 0.1.0 @category options */
export class Creds extends Schema.TaggedClass<Creds>()("Creds", {
  creds: Schema.Redacted(Schema.String),
}) {}

/** @since 0.1.0 @category options */
export class NKey extends Schema.TaggedClass<NKey>()("NKey", {
  seed: Schema.Redacted(Schema.String),
}) {}

/** @since 0.1.0 @category options */
export type Auth = UserPass | Token | Creds | NKey;

export const drainTimeout = "5 seconds";
const subscriptionDrainTimeout = "100 millis";

const release = (connection: NatsConnection) =>
  Effect.tryPromise(() => connection.drain()).pipe(
    Effect.timeout(drainTimeout),
    Effect.catch(() => Effect.tryPromise(() => connection.close())),
    Effect.ignore,
  );

const releaseSubscription = (subscription: Subscription) =>
  Effect.tryPromise(() => subscription.drain()).pipe(
    Effect.timeout(subscriptionDrainTimeout),
    Effect.catch(() => Effect.sync(() => subscription.unsubscribe())),
    Effect.ignore,
  );

const requestManyStrategy = (options: RequestManyOptions) => {
  if (Predicate.isNotUndefined(options.maxMessages)) {
    return "count";
  }
  if (Predicate.isNotUndefined(options.stall)) {
    return "stall";
  }
  if (options.sentinel === true) {
    return "sentinel";
  }
  return "timer";
};

/* v8 ignore next -- SDK status iterators are not expected to throw. */
const statusError = (cause: unknown): never => {
  throw cause;
};

/**
 * Constructs a scoped NATS client service.
 *
 * @see {@link layer} for the layer form
 * @see {@link layerConfig} for the config-driven layer form
 *
 * @since 0.1.0
 * @category constructors
 */
export const make = (
  options: Options = {},
): Effect.Effect<
  Service,
  NatsError.ConnectionError | NatsError.TimeoutError,
  NatsConnector.NatsConnector | Scope.Scope
> =>
  Effect.gen(function* () {
    const connector = yield* NatsConnector.NatsConnector;
    const connection = yield* Effect.acquireRelease(connector.connect(OptionsInternal.translate(options)), release);
    return NatsClient.of({
      connection,
      closed: Effect.promise(() => connection.closed()).pipe(Effect.map(Errors.mapClosed)),
      publish: (subject, options = {}) =>
        Effect.try({
          try: () =>
            connection.publish(subject, options.payload, {
              ...(Predicate.isNotUndefined(options.headers) ? { headers: NatsHeaders.toMsgHdrs(options.headers) } : {}),
              ...(Predicate.isNotUndefined(options.replyTo) ? { reply: options.replyTo } : {}),
            }),
          catch: (cause) => Errors.mapPublishError({ subject, cause }),
        }),
      flush: Effect.tryPromise({
        try: () => connection.flush(),
        catch: Errors.mapFlushError,
      }),
      rtt: Effect.tryPromise({
        try: () => connection.rtt(),
        catch: Errors.mapRttError,
      }).pipe(Effect.map(Duration.millis)),
      request: (subject, options = {}) =>
        Effect.tryPromise({
          try: () =>
            connection.request(subject, options.payload, {
              timeout: Predicate.isNotUndefined(options.timeout) ? Duration.toMillis(options.timeout) : 1_000,
              ...(Predicate.isNotUndefined(options.headers) ? { headers: NatsHeaders.toMsgHdrs(options.headers) } : {}),
            }),
          catch: (cause) => Errors.mapRequestError({ subject, cause }),
        }).pipe(Effect.map(NatsMessage.fromMsg)),
      subscribeRaw: (subject, options = {}) =>
        Iterator.streamFromQueuedIterator<Msg, Subscription, Msg, NatsError.NatsError>({
          acquire: Effect.try({
            try: () => connection.subscribe(subject, options),
            catch: Errors.mapError,
          }),
          transform: (message) => message,
          onError: Errors.mapError,
          onRelease: releaseSubscription,
        }),
      subscribe: (subject, options = {}) =>
        Iterator.streamFromQueuedIterator<Msg, Subscription, NatsMessage.NatsMessage, NatsError.NatsError>({
          acquire: Effect.try({
            try: () => connection.subscribe(subject, options),
            catch: Errors.mapError,
          }),
          transform: NatsMessage.fromMsg,
          onError: Errors.mapError,
          onRelease: releaseSubscription,
        }),
      requestMany: (subject, options) =>
        Stream.unwrap(
          Effect.tryPromise({
            try: () =>
              connection.requestMany(subject, options.payload, {
                strategy: requestManyStrategy(options),
                maxWait: Duration.toMillis(options.maxWait),
                ...(Predicate.isNotUndefined(options.maxMessages) ? { maxMessages: options.maxMessages } : {}),
                ...(Predicate.isNotUndefined(options.stall) ? { stall: Duration.toMillis(options.stall) } : {}),
                ...(Predicate.isNotUndefined(options.headers)
                  ? { headers: NatsHeaders.toMsgHdrs(options.headers) }
                  : {}),
              }),
            catch: Errors.mapError,
          }).pipe(
            Effect.map((iter) =>
              Iterator.streamFromQueuedIterator<Msg, AsyncIterable<Msg>, NatsMessage.NatsMessage, NatsError.NatsError>({
                acquire: Effect.succeed(iter),
                transform: NatsMessage.fromMsg,
                onError: Errors.mapError,
              }),
            ),
          ),
        ),
      status: Stream.fromAsyncIterable(connection.status(), statusError),
      stats: Effect.sync(() => connection.stats()),
    });
  });

/**
 * Provides a scoped NATS client from explicit connection options.
 *
 * @example
 * ```ts
 * import { Effect, Layer } from "effect"
 * import * as NatsClient from "effect-nats/NatsClient"
 * import * as NodeConnector from "effect-nats/NodeConnector"
 *
 * const NatsLive = NatsClient.layer({ servers: "nats://127.0.0.1:4222" }).pipe(
 *   Layer.provide(NodeConnector.layer)
 * )
 *
 * const program = Effect.gen(function*() {
 *   const nats = yield* NatsClient.NatsClient
 *   yield* nats.publish("events.created")
 * }).pipe(Effect.scoped, Effect.provide(NatsLive))
 * ```
 *
 * @see {@link make} for constructing the service effectfully
 * @see {@link layerConfig} for the config-driven layer form
 *
 * @since 0.1.0
 * @category layers
 */
export const layer = (
  options: Options = {},
): Layer.Layer<NatsClient, NatsError.ConnectionError | NatsError.TimeoutError, NatsConnector.NatsConnector> =>
  Layer.effect(NatsClient, make(options));

/**
 * Provides a scoped NATS client from `Config` values.
 *
 * @see {@link make} for constructing the service effectfully
 * @see {@link layer} for explicit options
 *
 * @since 0.1.0
 * @category layers
 */
export const layerConfig = (options: {
  readonly servers?: Config.Config<string | ReadonlyArray<string>>;
  readonly name?: Config.Config<string>;
  readonly auth?: Config.Config<Auth>;
  readonly timeout?: Config.Config<DurationInput>;
  readonly pingInterval?: Config.Config<DurationInput>;
  readonly maxPingOut?: Config.Config<number>;
  readonly reconnect?: Config.Config<boolean | ReconnectOptions>;
  readonly noEcho?: Config.Config<boolean>;
  readonly inboxPrefix?: Config.Config<string>;
  readonly tls?: Config.Config<TlsOptions>;
  readonly ignoreClusterUpdates?: Config.Config<boolean>;
  readonly transformOptions?: (options: ConnectionOptions) => ConnectionOptions;
}): Layer.Layer<
  NatsClient,
  NatsError.ConnectionError | NatsError.TimeoutError | Config.ConfigError,
  NatsConnector.NatsConnector
> =>
  Layer.effect(
    NatsClient,
    Effect.gen(function* () {
      const servers = Predicate.isNotUndefined(options.servers) ? yield* options.servers : undefined;
      const name = Predicate.isNotUndefined(options.name) ? yield* options.name : undefined;
      const auth = Predicate.isNotUndefined(options.auth) ? yield* options.auth : undefined;
      const timeout = Predicate.isNotUndefined(options.timeout) ? yield* options.timeout : undefined;
      const pingInterval = Predicate.isNotUndefined(options.pingInterval) ? yield* options.pingInterval : undefined;
      const maxPingOut = Predicate.isNotUndefined(options.maxPingOut) ? yield* options.maxPingOut : undefined;
      const reconnect = Predicate.isNotUndefined(options.reconnect) ? yield* options.reconnect : undefined;
      const noEcho = Predicate.isNotUndefined(options.noEcho) ? yield* options.noEcho : undefined;
      const inboxPrefix = Predicate.isNotUndefined(options.inboxPrefix) ? yield* options.inboxPrefix : undefined;
      const tls = Predicate.isNotUndefined(options.tls) ? yield* options.tls : undefined;
      const ignoreClusterUpdates = Predicate.isNotUndefined(options.ignoreClusterUpdates)
        ? yield* options.ignoreClusterUpdates
        : undefined;
      return yield* make({
        ...(Predicate.isNotUndefined(servers) ? { servers } : {}),
        ...(Predicate.isNotUndefined(name) ? { name } : {}),
        ...(Predicate.isNotUndefined(auth) ? { auth } : {}),
        ...(Predicate.isNotUndefined(timeout) ? { timeout } : {}),
        ...(Predicate.isNotUndefined(pingInterval) ? { pingInterval } : {}),
        ...(Predicate.isNotUndefined(maxPingOut) ? { maxPingOut } : {}),
        ...(Predicate.isNotUndefined(reconnect) ? { reconnect } : {}),
        ...(Predicate.isNotUndefined(noEcho) ? { noEcho } : {}),
        ...(Predicate.isNotUndefined(inboxPrefix) ? { inboxPrefix } : {}),
        ...(Predicate.isNotUndefined(tls) ? { tls } : {}),
        ...(Predicate.isNotUndefined(ignoreClusterUpdates) ? { ignoreClusterUpdates } : {}),
        ...(Predicate.isNotUndefined(options.transformOptions) ? { transformOptions: options.transformOptions } : {}),
      });
    }),
  );
