/**
 * Scoped NATS client service.
 *
 * @since 0.1.0
 */
import { Config, Context, Effect, Layer, Option, Predicate, Schema, Scope } from "effect";
import type { ConnectionOptions, NatsConnection, TlsOptions } from "@nats-io/nats-core";
import type { Input as DurationInput } from "effect/Duration";
import * as NatsConnector from "./NatsConnector.ts";
import * as NatsError from "./NatsError.ts";
import * as Errors from "./internal/errors.ts";
import * as OptionsInternal from "./internal/options.ts";

/** @since 0.1.0 @category models */
export interface Service {
  readonly connection: NatsConnection;
  readonly closed: Effect.Effect<Option.Option<NatsError.NatsError>>;
}

/** @since 0.1.0 @category services */
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

const release = (connection: NatsConnection) =>
  Effect.tryPromise(() => connection.drain()).pipe(
    Effect.timeout(drainTimeout),
    Effect.catch(() => Effect.tryPromise(() => connection.close())),
    Effect.ignore,
  );

/** @since 0.1.0 @category constructors */
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
    });
  });

/** @since 0.1.0 @category layers */
export const layer = (
  options: Options = {},
): Layer.Layer<NatsClient, NatsError.ConnectionError | NatsError.TimeoutError, NatsConnector.NatsConnector> =>
  Layer.effect(NatsClient, make(options));

/** @since 0.1.0 @category layers */
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
