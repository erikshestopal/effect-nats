/**
 * NATS KeyValue bucket service.
 *
 * @since 0.1.0
 */
/* eslint-disable agent/max-positional-params */
import { Context, DateTime, Duration, Effect, Layer, Option, Predicate, Schema, Stream, String as Str } from "effect";
import { Kvm, KvWatchInclude } from "@nats-io/kv";
import * as KeyValueStore from "effect/unstable/persistence/KeyValueStore";
import type { Payload } from "@nats-io/nats-core";
import type { KV as SdkKv, KvEntry as SdkKvEntry, KvOptions, KvStatus } from "@nats-io/kv";
import type { Input as DurationInput } from "effect/Duration";
import * as JetStream from "./JetStream.ts";
import * as JetStreamError from "./JetStreamError.ts";
import * as NatsError from "./NatsError.ts";
import * as Iterators from "./internal/iterator.ts";
import * as JsErrors from "./internal/mapJsError.ts";

/** @since 0.1.0 @category errors */
export class BucketNotFoundError extends Schema.TaggedErrorClass<BucketNotFoundError>(
  "effect-nats/NatsKv/BucketNotFoundError",
)("BucketNotFoundError", { bucket: Schema.String }) {}

/** @since 0.1.0 @category errors */
export class KeyExistsError extends Schema.TaggedErrorClass<KeyExistsError>("effect-nats/NatsKv/KeyExistsError")(
  "KeyExistsError",
  { key: Schema.String },
) {}

/** @since 0.1.0 @category models */
export class KvEntry extends Schema.Class<KvEntry>("effect-nats/NatsKv/KvEntry")({
  key: Schema.String,
  value: Schema.Uint8Array,
  revision: Schema.Finite,
  operation: Schema.Literals(["PUT", "DEL", "PURGE"]),
  created: Schema.DateTimeUtc,
  isUpdate: Schema.Boolean,
}) {}

/** @since 0.1.0 @category options */
export type BucketOptions = {
  readonly history?: number;
  readonly ttl?: DurationInput;
  readonly maxBucketSize?: number;
  readonly maxValueSize?: number;
  readonly storage?: "file" | "memory";
  readonly replicas?: number;
  readonly description?: string;
  readonly compression?: boolean;
  readonly transformOptions?: (options: Partial<KvOptions>) => Partial<KvOptions>;
};

/** @since 0.1.0 @category options */
export type WatchOptions = {
  readonly key?: string | ReadonlyArray<string>;
  readonly include?: "lastValue" | "allHistory" | "updatesOnly";
  readonly ignoreDeletes?: boolean;
  readonly resumeFromRevision?: number;
};

/** @since 0.1.0 @category models */
export interface Kv {
  readonly get: (
    key: string,
    options?: { readonly revision?: number },
  ) => Effect.Effect<Option.Option<KvEntry>, JetStreamError.JetStreamErrors>;
  readonly put: (key: string, value: Payload) => Effect.Effect<number, JetStreamError.JetStreamErrors>;
  readonly create: (
    key: string,
    value: Payload,
  ) => Effect.Effect<number, KeyExistsError | JetStreamError.JetStreamErrors | NatsError.TimeoutError>;
  readonly update: (
    key: string,
    value: Payload,
    options: { readonly revision: number },
  ) => Effect.Effect<
    number,
    JetStreamError.WrongLastSequenceError | JetStreamError.JetStreamErrors | NatsError.TimeoutError
  >;
  readonly delete: (key: string) => Effect.Effect<void, JetStreamError.JetStreamErrors>;
  readonly purge: (key: string) => Effect.Effect<void, JetStreamError.JetStreamErrors>;
  readonly keys: (filter?: string) => Stream.Stream<string, JetStreamError.JetStreamErrors>;
  readonly history: (options?: { readonly key?: string }) => Stream.Stream<KvEntry, JetStreamError.JetStreamErrors>;
  readonly watch: (options?: WatchOptions) => Stream.Stream<KvEntry, JetStreamError.JetStreamErrors>;
  readonly status: Effect.Effect<KvStatus, JetStreamError.JetStreamErrors>;
}

/** @since 0.1.0 @category services */
export class NatsKv extends Context.Service<NatsKv, Kv>()("effect-nats/NatsKv") {}

const decoder = new TextDecoder();

/** @since 0.1.0 @category accessors */
export const entryText = (entry: KvEntry): string => decoder.decode(entry.value);

/** @since 0.1.0 @category accessors */
export const entrySchemaJson = <S extends Schema.Top>(entry: KvEntry, schema: S) =>
  Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(entryText(entry));

/** @since 0.1.0 @category constructors */
export const open = (
  bucket: string,
): Effect.Effect<Kv, BucketNotFoundError | JetStreamError.JetStreamErrors, JetStream.JetStream> =>
  Effect.gen(function* () {
    const js = yield* JetStream.JetStream;
    const sdk = yield* Effect.tryPromise({
      try: () => new Kvm(js.client).open(bucket),
      /* v8 ignore next */
      catch: (cause) => (isNotFound(cause) ? new BucketNotFoundError({ bucket }) : JsErrors.mapJetStreamError(cause)),
    });
    const kv = makeBucket(sdk);
    yield* kv.status.pipe(Effect.catchTag("JetStreamApiError", () => Effect.fail(new BucketNotFoundError({ bucket }))));
    return kv;
  });

/** @since 0.1.0 @category constructors */
export const create = (
  bucket: string,
  options: BucketOptions = {},
): Effect.Effect<Kv, JetStreamError.JetStreamErrors, JetStream.JetStream> =>
  Effect.gen(function* () {
    const js = yield* JetStream.JetStream;
    const sdk = yield* Effect.tryPromise({
      try: () => new Kvm(js.client).create(bucket, translateBucketOptions(options)),
      catch: JsErrors.mapJetStreamError,
    });
    return makeBucket(sdk);
  });

/**
 * Provides a scoped KV bucket service.
 *
 * @example
 * ```ts
 * import { Effect } from "effect"
 * import * as NatsKv from "effect-nats/NatsKv"
 *
 * const program = Effect.gen(function*() {
 *   const kv = yield* NatsKv.NatsKv
 *   yield* kv.put("config", new TextEncoder().encode("value"))
 *   return yield* kv.get("config")
 * })
 * ```
 *
 * @see {@link create} for constructing a bucket handle effectfully
 * @see {@link open} for opening an existing bucket
 *
 * @since 0.1.0
 * @category layers
 */
export const layer = (
  bucket: string,
  options: BucketOptions = {},
): Layer.Layer<NatsKv, JetStreamError.JetStreamErrors, JetStream.JetStream> =>
  Layer.effect(NatsKv, create(bucket, options));

/** @since 0.1.0 @category layers */
export const layerKeyValueStore = (
  bucket: string,
  options: BucketOptions = {},
): Layer.Layer<KeyValueStore.KeyValueStore, JetStreamError.JetStreamErrors, JetStream.JetStream> =>
  Layer.effect(KeyValueStore.KeyValueStore, create(bucket, options).pipe(Effect.map(makeKeyValueStore)));

const makeBucket = (sdk: SdkKv): Kv => ({
  get: (key, options) =>
    Effect.tryPromise({
      try: () => sdk.get(key, Predicate.isNotUndefined(options?.revision) ? { revision: options.revision } : undefined),
      catch: JsErrors.mapJetStreamError,
    }).pipe(
      Effect.map((entry) =>
        Option.fromNullishOr(entry).pipe(
          Option.map(fromSdkEntry(false)),
          Option.filter((kvEntry) => kvEntry.operation === "PUT"),
        ),
      ),
    ),
  put: (key, value) =>
    Effect.tryPromise({
      try: () => sdk.put(key, value),
      catch: JsErrors.mapJetStreamError,
    }),
  create: (key, value) =>
    Effect.tryPromise({
      try: () => sdk.create(key, value),
      catch: (cause) => {
        const mapped = JsErrors.mapPublishError(cause);
        return Predicate.isTagged(mapped, "WrongLastSequenceError") ? new KeyExistsError({ key }) : mapped;
      },
    }),
  update: (key, value, options) =>
    Effect.tryPromise({
      try: () => sdk.update(key, value, options.revision),
      catch: JsErrors.mapPublishError,
    }),
  delete: (key) =>
    Effect.tryPromise({
      try: () => sdk.delete(key),
      catch: JsErrors.mapJetStreamError,
    }),
  purge: (key) =>
    Effect.tryPromise({
      try: () => sdk.purge(key),
      catch: JsErrors.mapJetStreamError,
    }),
  keys: (filter) => {
    const stream: Stream.Stream<string, JetStreamError.JetStreamErrors> = Iterators.streamFromQueuedIterator<
      string,
      Awaited<ReturnType<SdkKv["keys"]>>,
      string,
      JetStreamError.JetStreamErrors
    >({
      acquire: Effect.tryPromise({
        try: () => sdk.keys(filter),
        catch: JsErrors.mapJetStreamError,
      }),
      transform: (key) => key,
      onError: JsErrors.mapJetStreamError,
    });
    return stream;
  },
  history: (options = {}) =>
    Iterators.streamFromQueuedIterator<
      SdkKvEntry,
      Awaited<ReturnType<SdkKv["history"]>>,
      KvEntry,
      JetStreamError.JetStreamErrors
    >({
      acquire: Effect.tryPromise({
        try: () => sdk.history(options),
        catch: JsErrors.mapJetStreamError,
      }),
      transform: fromSdkEntry(false),
      onError: JsErrors.mapJetStreamError,
    }),
  watch: (options = {}) => {
    let firstEntry = true;
    return Iterators.streamFromQueuedIterator<
      SdkKvEntry & { readonly isUpdate: boolean },
      Awaited<ReturnType<SdkKv["watch"]>>,
      KvEntry,
      JetStreamError.JetStreamErrors
    >({
      acquire: Effect.tryPromise({
        try: () => sdk.watch(translateWatchOptions(options)),
        catch: JsErrors.mapJetStreamError,
      }),
      transform: (entry) => {
        const isUpdate = options.include === "updatesOnly" || !firstEntry;
        firstEntry = false;
        return fromSdkEntry(isUpdate)(entry);
      },
      onError: JsErrors.mapJetStreamError,
    });
  },
  status: Effect.tryPromise({
    try: () => sdk.status(),
    catch: JsErrors.mapJetStreamError,
  }),
});

const makeKeyValueStore = (kv: Kv): KeyValueStore.KeyValueStore =>
  KeyValueStore.make({
    get: (key) =>
      kv
        .get(key)
        .pipe(Effect.map(Option.map(entryText)), Effect.map(Option.getOrUndefined), mapKvStoreError("get", key)),
    getUint8Array: (key) =>
      kv
        .get(key)
        .pipe(
          Effect.map(Option.map((entry) => entry.value)),
          Effect.map(Option.getOrUndefined),
          mapKvStoreError("getUint8Array", key),
        ),
    set: (key, value) => kv.put(key, value).pipe(Effect.asVoid, mapKvStoreError("set", key)),
    remove: (key) => kv.delete(key).pipe(mapKvStoreError("remove", key)),
    clear: kv.keys().pipe(
      Stream.runForEach((key) => kv.purge(key)),
      mapKvStoreError("clear"),
    ),
    size: kv.keys().pipe(
      Stream.runCollect,
      Effect.map((keys) => keys.length),
      mapKvStoreError("size"),
    ),
  });

const mapKvStoreError = (method: string, key?: string) =>
  Predicate.isUndefined(key)
    ? <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(
          Effect.mapError(
            /* v8 ignore next */
            (cause) => new KeyValueStore.KeyValueStoreError({ message: `NATS KV ${method} failed`, method, cause }),
          ),
        )
    : <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(
          Effect.mapError(
            (cause) =>
              new KeyValueStore.KeyValueStoreError({ message: `NATS KV ${method} failed`, method, key, cause }),
          ),
        );

const fromSdkEntry =
  (isUpdate: boolean) =>
  (entry: SdkKvEntry): KvEntry =>
    KvEntry.make({
      key: entry.key,
      value: entry.value,
      revision: entry.revision,
      operation: entry.operation,
      created: DateTime.fromDateUnsafe(entry.created),
      isUpdate,
    });

const translateBucketOptions = (options: BucketOptions): Partial<KvOptions> => {
  const translated: Partial<KvOptions> = {};
  if (Predicate.isNotUndefined(options.history)) {
    translated.history = options.history;
  }
  if (Predicate.isNotUndefined(options.ttl)) {
    translated.ttl = Duration.toMillis(options.ttl);
  }
  if (Predicate.isNotUndefined(options.maxBucketSize)) {
    translated.max_bytes = options.maxBucketSize;
  }
  if (Predicate.isNotUndefined(options.maxValueSize)) {
    translated.maxValueSize = options.maxValueSize;
  }
  if (Predicate.isNotUndefined(options.storage)) {
    translated.storage = options.storage;
  }
  if (Predicate.isNotUndefined(options.replicas)) {
    translated.replicas = options.replicas;
  }
  if (Predicate.isNotUndefined(options.description)) {
    translated.description = options.description;
  }
  if (Predicate.isNotUndefined(options.compression)) {
    translated.compression = options.compression;
  }
  return options.transformOptions?.(translated) ?? translated;
};

const translateWatchOptions = (options: WatchOptions) => {
  const translated: {
    key?: string | string[];
    include?: KvWatchInclude;
    ignoreDeletes?: boolean;
    resumeFromRevision?: number;
  } = {};
  if (Predicate.isNotUndefined(options.key)) {
    translated.key = Predicate.isString(options.key) ? options.key : [...options.key];
  }
  Option.map(translateWatchInclude(options.include), (include) => {
    translated.include = include;
  });
  if (Predicate.isNotUndefined(options.ignoreDeletes)) {
    translated.ignoreDeletes = options.ignoreDeletes;
  }
  if (Predicate.isNotUndefined(options.resumeFromRevision)) {
    translated.resumeFromRevision = options.resumeFromRevision;
  }
  return translated;
};

const translateWatchInclude = (include: WatchOptions["include"]): Option.Option<KvWatchInclude> =>
  include === "allHistory"
    ? Option.some(KvWatchInclude.AllHistory)
    : include === "updatesOnly"
      ? Option.some(KvWatchInclude.UpdatesOnly)
      : include === "lastValue"
        ? Option.some(KvWatchInclude.LastValue)
        : Option.none();

/* v8 ignore next */
const isNotFound = (cause: unknown): boolean =>
  Predicate.isError(cause) && Option.isSome(Str.indexOf("stream not found")(cause.message));
