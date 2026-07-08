/**
 * NATS KeyValue bucket service.
 *
 * @since 0.1.0
 */
/* eslint-disable agent/max-positional-params */
import {
  Context,
  DateTime,
  Duration,
  Effect,
  Layer,
  Match,
  Option,
  Predicate,
  Schema,
  Stream,
  String as Str,
} from "effect";
import { identity } from "effect/Function";
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

export class BucketNotFoundError extends Schema.TaggedErrorClass<BucketNotFoundError>(
  "effect-nats/NatsKv/BucketNotFoundError",
)("BucketNotFoundError", { bucket: Schema.String }) {}

export class KeyExistsError extends Schema.TaggedErrorClass<KeyExistsError>("effect-nats/NatsKv/KeyExistsError")(
  "KeyExistsError",
  { key: Schema.String },
) {}

export class KvEntry extends Schema.Class<KvEntry>("effect-nats/NatsKv/KvEntry")({
  key: Schema.String,
  value: Schema.Uint8Array,
  revision: Schema.Finite,
  operation: Schema.Literals(["PUT", "DEL", "PURGE"]),
  created: Schema.DateTimeUtc,
  isUpdate: Schema.Boolean,
}) {}

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

export type WatchOptions = {
  readonly key?: string | ReadonlyArray<string>;
  readonly include?: "lastValue" | "allHistory" | "updatesOnly";
  readonly ignoreDeletes?: boolean;
  readonly resumeFromRevision?: number;
};

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

export class NatsKv extends Context.Service<NatsKv, Kv>()("effect-nats/NatsKv") {}

const decoder = new TextDecoder();

export const entryText = (entry: KvEntry): string => decoder.decode(entry.value);

export const entrySchemaJson = <S extends Schema.Top>(entry: KvEntry, schema: S) =>
  Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(entryText(entry));

export const open = Effect.fnUntraced(function* (bucket: string) {
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

export const create = Effect.fnUntraced(function* (bucket: string, options: BucketOptions = {}) {
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
  keys: (filter) =>
    Iterators.streamFromQueuedIterator<
      string,
      Awaited<ReturnType<SdkKv["keys"]>>,
      string,
      JetStreamError.JetStreamErrors
    >({
      acquire: Effect.tryPromise({
        try: () => sdk.keys(filter),
        catch: JsErrors.mapJetStreamError,
      }),
      transform: identity,
      onError: JsErrors.mapJetStreamError,
    }),
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
      kv.get(key).pipe(
        Effect.map((entry) => Option.getOrUndefined(Option.map(entry, entryText))),
        mapKvStoreError("get", key),
      ),
    getUint8Array: (key) =>
      kv.get(key).pipe(
        Effect.map((entry) => Option.getOrUndefined(Option.map(entry, (kvEntry) => kvEntry.value))),
        mapKvStoreError("getUint8Array", key),
      ),
    set: (key, value) => kv.put(key, value).pipe(Effect.asVoid, mapKvStoreError("set", key)),
    remove: (key) => kv.delete(key).pipe(mapKvStoreError("remove", key)),
    clear: kv.keys().pipe(
      Stream.runForEach((key) => kv.purge(key)),
      mapKvStoreError("clear"),
    ),
    size: kv.keys().pipe(Stream.runCount, mapKvStoreError("size")),
  });

const mapKvStoreError =
  (method: string, key?: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError(
        /* v8 ignore next */
        (cause) =>
          new KeyValueStore.KeyValueStoreError({
            message: `NATS KV ${method} failed`,
            method,
            ...(Predicate.isNotUndefined(key) ? { key } : {}),
            cause,
          }),
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
  const include = translateWatchInclude(options.include);
  if (Option.isSome(include)) {
    translated.include = include.value;
  }
  if (Predicate.isNotUndefined(options.ignoreDeletes)) {
    translated.ignoreDeletes = options.ignoreDeletes;
  }
  if (Predicate.isNotUndefined(options.resumeFromRevision)) {
    translated.resumeFromRevision = options.resumeFromRevision;
  }
  return translated;
};

const translateWatchInclude = (include: WatchOptions["include"]): Option.Option<KvWatchInclude> =>
  Match.value(include).pipe(
    Match.when("allHistory", () => Option.some(KvWatchInclude.AllHistory)),
    Match.when("updatesOnly", () => Option.some(KvWatchInclude.UpdatesOnly)),
    Match.when("lastValue", () => Option.some(KvWatchInclude.LastValue)),
    Match.orElse(() => Option.none()),
  );

/* v8 ignore next */
const isNotFound = (cause: unknown): boolean =>
  Predicate.isError(cause) && Str.includes("stream not found")(cause.message);
