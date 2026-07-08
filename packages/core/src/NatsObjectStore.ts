/**
 * NATS ObjectStore bucket service.
 *
 * @since 0.1.0
 */
/* eslint-disable agent/max-positional-params */
import {
  Array as Arr,
  Context,
  DateTime,
  Duration,
  Effect,
  Layer,
  Option,
  Predicate,
  Schema,
  Stream,
  String as Str,
} from "effect";
import { Objm } from "@nats-io/obj";
import { nanos } from "@nats-io/nats-core";
import type { Input as DurationInput } from "effect/Duration";
import type {
  ObjectInfo as SdkObjectInfo,
  ObjectResult,
  ObjectStore as SdkObjectStore,
  ObjectStoreOptions as SdkObjectStoreOptions,
  ObjectStoreStatus,
} from "@nats-io/obj";
import * as JetStream from "./JetStream.ts";
import * as NatsHeaders from "./NatsHeaders.ts";
import * as Iterators from "./internal/iterator.ts";

export class ObjectStoreError extends Schema.TaggedErrorClass<ObjectStoreError>(
  "effect-nats/NatsObjectStore/ObjectStoreError",
)("ObjectStoreError", { cause: Schema.Defect() }) {}

export class BucketNotFoundError extends Schema.TaggedErrorClass<BucketNotFoundError>(
  "effect-nats/NatsObjectStore/BucketNotFoundError",
)("BucketNotFoundError", { bucket: Schema.String }) {}

export class DigestMismatchError extends Schema.TaggedErrorClass<DigestMismatchError>(
  "effect-nats/NatsObjectStore/DigestMismatchError",
)("DigestMismatchError", { name: Schema.String, cause: Schema.Defect() }) {}

export const ObjectStoreErrors = Schema.Union([ObjectStoreError, BucketNotFoundError, DigestMismatchError]);

export type ObjectStoreErrors = typeof ObjectStoreErrors.Type;

export class ObjectInfo extends Schema.Class<ObjectInfo>("effect-nats/NatsObjectStore/ObjectInfo")({
  name: Schema.String,
  bucket: Schema.String,
  size: Schema.Finite,
  chunks: Schema.Finite,
  digest: Schema.String,
  nuid: Schema.String,
  deleted: Schema.Boolean,
  revision: Schema.Finite,
  mtime: Schema.DateTimeUtc,
  description: Schema.optionalKey(Schema.String),
  metadata: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
}) {}

export type BucketOptions = {
  readonly description?: string;
  readonly ttl?: DurationInput;
  readonly storage?: "file" | "memory";
  readonly replicas?: number;
  readonly maxBucketSize?: number;
  readonly metadata?: Readonly<Record<string, string>>;
  readonly compression?: boolean;
};

export type ObjectMeta = {
  readonly name: string;
  readonly description?: string;
  readonly headers?: NatsHeaders.Input;
  readonly maxChunkSize?: number;
  readonly metadata?: Readonly<Record<string, string>>;
};

export type ObjectEntry = {
  readonly info: ObjectInfo;
  readonly data: Stream.Stream<Uint8Array, ObjectStoreErrors>;
};

export interface ObjectStore {
  readonly put: (
    meta: ObjectMeta,
    data: Stream.Stream<Uint8Array, unknown, never>,
  ) => Effect.Effect<ObjectInfo, ObjectStoreErrors>;
  readonly get: (name: string) => Effect.Effect<Option.Option<ObjectEntry>, ObjectStoreErrors>;
  readonly putBlob: (meta: ObjectMeta, data: Uint8Array) => Effect.Effect<ObjectInfo, ObjectStoreErrors>;
  readonly getBlob: (name: string) => Effect.Effect<Option.Option<Uint8Array>, ObjectStoreErrors>;
  readonly info: (name: string) => Effect.Effect<Option.Option<ObjectInfo>, ObjectStoreErrors>;
  readonly list: Effect.Effect<ReadonlyArray<ObjectInfo>, ObjectStoreErrors>;
  readonly delete: (name: string) => Effect.Effect<void, ObjectStoreErrors>;
  readonly watch: (options?: { readonly includeHistory?: boolean }) => Stream.Stream<ObjectInfo, ObjectStoreErrors>;
  readonly seal: Effect.Effect<void, ObjectStoreErrors>;
  readonly status: Effect.Effect<ObjectStoreStatus, ObjectStoreErrors>;
}

export class NatsObjectStore extends Context.Service<NatsObjectStore, ObjectStore>()("effect-nats/NatsObjectStore") {}

export const open = Effect.fnUntraced(function* (bucket: string) {
  const js = yield* JetStream.JetStream;
  const sdk = yield* Effect.tryPromise({
    try: () => new Objm(js.client).open(bucket),
    /* v8 ignore next -- defensive SDK failure mapping */
    catch: (cause) =>
      isNotFound(cause) ? new BucketNotFoundError({ bucket }) : new ObjectStoreError({ cause: toCause(cause) }),
  });
  return makeStore(sdk);
});

export const create = Effect.fnUntraced(function* (bucket: string, options: BucketOptions = {}) {
  const js = yield* JetStream.JetStream;
  const sdk = yield* Effect.tryPromise({
    try: () => new Objm(js.client).create(bucket, translateBucketOptions(options)),
    /* v8 ignore next -- defensive SDK failure mapping */
    catch: mapError,
  });
  return makeStore(sdk);
});

/**
 * Provides a scoped ObjectStore bucket service.
 *
 * @example
 * ```ts
 * import { Effect, Option, Stream } from "effect"
 * import * as NatsObjectStore from "effect-nats/NatsObjectStore"
 *
 * const program = Effect.gen(function*() {
 *   const store = yield* NatsObjectStore.NatsObjectStore
 *   yield* store.put({ name: "artifact" }, Stream.make(new TextEncoder().encode("data")))
 *   return yield* store.get("artifact").pipe(Effect.map(Option.getOrThrow))
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
): Layer.Layer<NatsObjectStore, ObjectStoreErrors, JetStream.JetStream> =>
  Layer.effect(NatsObjectStore, create(bucket, options));

/* v8 ignore next -- defensive SDK failure mapping */
const mapError = (cause: unknown) => new ObjectStoreError({ cause: toCause(cause) });

const trySdk = <A>(try_: () => PromiseLike<A>): Effect.Effect<A, ObjectStoreError> =>
  Effect.tryPromise({ try: try_, catch: mapError });

const makeStore = (sdk: SdkObjectStore): ObjectStore => ({
  put: Effect.fnUntraced(function* (meta: ObjectMeta, data: Stream.Stream<Uint8Array, unknown, never>) {
    const readable = yield* Stream.toReadableStreamEffect(data.pipe(Stream.mapError(mapError)));
    return yield* trySdk(() => sdk.put(translateMeta(meta), readable)).pipe(Effect.map(fromSdkInfo));
  }),
  get: (name) =>
    trySdk(() => sdk.get(name)).pipe(Effect.map((result) => Option.map(Option.fromNullishOr(result), fromResult))),
  putBlob: (meta, data) => trySdk(() => sdk.putBlob(translateMeta(meta), data)).pipe(Effect.map(fromSdkInfo)),
  getBlob: (name) => trySdk(() => sdk.getBlob(name)).pipe(Effect.map(Option.fromNullishOr)),
  info: (name) =>
    trySdk(() => sdk.info(name)).pipe(
      Effect.map((info) =>
        Option.fromNullishOr(info).pipe(
          Option.filter((objectInfo) => !objectInfo.deleted),
          Option.map(fromSdkInfo),
        ),
      ),
    ),
  list: trySdk(() => sdk.list()).pipe(Effect.map(Arr.map(fromSdkInfo))),
  delete: (name) => trySdk(() => sdk.delete(name)).pipe(Effect.asVoid),
  watch: (options = {}) =>
    Iterators.streamFromQueuedIterator({
      acquire: trySdk(() =>
        sdk.watch(Predicate.isUndefined(options.includeHistory) ? {} : { includeHistory: options.includeHistory }),
      ),
      transform: fromSdkInfo,
      onError: mapError,
    }),
  seal: trySdk(() => sdk.seal()).pipe(Effect.asVoid),
  status: trySdk(() => sdk.status()),
});

const fromResult = (result: ObjectResult): ObjectEntry => ({
  info: fromSdkInfo(result.info),
  data: Stream.fromReadableStream({
    evaluate: () => result.data,
    /* v8 ignore next -- defensive readable stream failure mapping */
    onError: (cause) => mapReadError(result.info.name, cause),
  }).pipe(Stream.concat(Stream.fromEffect(checkDigest(result)).pipe(Stream.drain))),
});

const checkDigest = (result: ObjectResult): Effect.Effect<void, ObjectStoreErrors> =>
  Effect.promise(() => result.error).pipe(
    Effect.map(Option.fromNullishOr),
    Effect.flatMap((error) =>
      Option.match(error, {
        onNone: () => Effect.void,
        /* v8 ignore next -- SDK reports digest failures through the readable stream in covered broker tests */
        onSome: (cause) => Effect.fail(mapReadError(result.info.name, cause)),
      }),
    ),
  );

const mapReadError = (name: string, cause: unknown): ObjectStoreError | DigestMismatchError =>
  Option.match(Option.liftPredicate(Str.includes("digest"))(String(cause)), {
    /* v8 ignore next -- non-digest read failures are defensive SDK mapping */
    onNone: () => new ObjectStoreError({ cause: toCause(cause) }),
    onSome: () => new DigestMismatchError({ name, cause }),
  });

const fromSdkInfo = (info: SdkObjectInfo): ObjectInfo =>
  ObjectInfo.make({
    name: info.name,
    bucket: info.bucket,
    size: info.size,
    chunks: info.chunks,
    digest: info.digest,
    nuid: info.nuid,
    deleted: info.deleted,
    revision: info.revision ?? 0,
    mtime: DateTime.makeUnsafe(info.mtime),
    /* v8 ignore next -- upstream ObjectInfoImpl normalizes absent descriptions to an empty string */
    ...(Predicate.isNotUndefined(info.description) ? { description: info.description } : {}),
    ...(Predicate.isNotUndefined(info.metadata) ? { metadata: info.metadata } : {}),
  });

/* v8 ignore next -- undefined rejections are defensive SDK mapping */
const toCause = (cause: unknown): unknown => (Predicate.isUndefined(cause) ? "object store operation failed" : cause);

const translateMeta = (meta: ObjectMeta) => ({
  name: meta.name,
  ...(Predicate.isNotUndefined(meta.description) ? { description: meta.description } : {}),
  ...(Predicate.isNotUndefined(meta.headers) ? { headers: NatsHeaders.toMsgHdrs(meta.headers) } : {}),
  ...(Predicate.isNotUndefined(meta.maxChunkSize) ? { options: { max_chunk_size: meta.maxChunkSize } } : {}),
  ...(Predicate.isNotUndefined(meta.metadata) ? { metadata: meta.metadata } : {}),
});

const translateBucketOptions = (options: BucketOptions): Partial<SdkObjectStoreOptions> => ({
  ...(Predicate.isNotUndefined(options.description) ? { description: options.description } : {}),
  ...(Predicate.isNotUndefined(options.ttl) ? { ttl: nanos(Duration.toMillis(options.ttl)) } : {}),
  ...(Predicate.isNotUndefined(options.storage) ? { storage: options.storage } : {}),
  ...(Predicate.isNotUndefined(options.replicas) ? { replicas: options.replicas } : {}),
  ...(Predicate.isNotUndefined(options.maxBucketSize) ? { max_bytes: options.maxBucketSize } : {}),
  ...(Predicate.isNotUndefined(options.metadata) ? { metadata: options.metadata } : {}),
  ...(Predicate.isNotUndefined(options.compression) ? { compression: options.compression } : {}),
});

const isNotFound = (cause: unknown): boolean =>
  Predicate.isError(cause) && Str.includes("object store not found")(cause.message);
