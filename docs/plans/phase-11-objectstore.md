# Phase 11 — NatsObjectStore

Blocked by: phase 6. Parallel with: phases 7, 8, 9, 10, 12.

## Mission

Ship the ObjectStore projection with `Stream<Uint8Array>` I/O and the
digest-error folding that makes a completed stream mean "digest verified".

## Required reading

- `docs/DESIGN.html` §13 (normative — especially the error-promise folding)
- `repos/nats.js/obj/src/types.ts`: `ObjectStore` (~235–345),
  `ObjectResult` (~206 — note `error: Promise<Error | null>`),
  `ObjectInfo` (~63), `ObjectStoreOptions` (~165)
- `repos/nats.js/obj/src/objectstore.ts`: `Objm` (~97), put/get impls
- `repos/effect-v4/packages/effect/src/Stream.ts`: `fromReadableStream`
  (~1406), `toReadableStreamWith` (~11145)

## Deliverables — `src/NatsObjectStore.ts`

Per DESIGN §13 (repo spelling). Additional decisions:

- Errors in-module: `ObjectStoreError{cause}` (general),
  `BucketNotFoundError{bucket}`, `DigestMismatchError{name}` (folded from
  the error promise; if the SDK reports other post-stream errors, carry
  them in `ObjectStoreError`). Ids `"effect-nats/NatsObjectStore/<Tag>"`.
  The union for signatures: `ObjectStoreErrors`.
- `ObjectMeta`: `{ readonly name: string; readonly description?: string;
  readonly headers?: NatsHeaders.Input;
  readonly maxChunkSize?: number;
  readonly metadata?: Readonly<Record<string, string>> }`.
- `ObjectInfo`: immutable view — name, bucket, size, chunks, digest, nuid,
  deleted, revision, mtime as `DateTime.Utc`, description/metadata as
  they come.
- `put(meta, data)`: `Stream.toReadableStreamWith` (needs the current
  context — check its signature; alternatively `Stream.toReadableStream`
  variant available without context) feeding SDK `put`. The input stream's
  error type is `unknown`-tolerant: fail `ObjectStoreError` wrapping the
  upstream cause.
- `get(name)`: SDK returns `ObjectResult | null` → `Option<ObjectEntry>`.
  Fold: `Stream.fromReadableStream(result.data, onError)` concatenated with
  a final effectful check `Effect.promise(() => result.error)` that fails
  `DigestMismatchError`/`ObjectStoreError` when non-null (use
  `Stream.concat(Stream.execute(...))` or `Stream.ensuringWith` — pick the
  operator that guarantees the check runs after the last chunk and BEFORE
  the stream completes; verify ordering in a test with a corrupted store).
- `watch`: infinite (adapter; `includeHistory` option); entries are
  `ObjectInfo` views (SDK `ObjectWatchInfo` adds nothing we keep).
- `seal`, `status`, `delete`, `list`, `info`, `putBlob`, `getBlob` per
  DESIGN §13 signatures.

### Barrel

Add `NatsObjectStore`.

## Tests (integration on `layerJetStream`)

- put/get round-trip multi-chunk: 1 MiB of random bytes with
  `maxChunkSize: 64KiB` → info.chunks > 1; get streams back identical bytes
  (compare digests computed test-side).
- putBlob/getBlob round-trip; getBlob on missing name → `Option.none`.
- get on missing name → `Option.none`; info/list reflect puts; delete →
  `deleted: true` in info (SDK semantics: info of deleted returns null? —
  verify against `objectstore.ts` and assert observed behavior).
- Digest failure: corrupt a chunk under the hood (raw-SDK jsm
  `deleteMessage` on one chunk message of the underlying `OBJ_` stream, or
  publish a garbage chunk) → get's stream **fails typed**, does not complete
  silently. This test is mandatory — it verifies the §13 folding claim.
- put with failing input stream: our stream fails mid-way → put fails
  `ObjectStoreError`, no half-written visible object (assert info missing
  or deleted per SDK behavior; document).
- watch: put/delete sequence observed live; scope close ends it cleanly.
- seal: seals; subsequent put fails typed.

100% coverage on files added.

## Out of scope

`link`/`linkStore` (DESIGN §13 defers). Compression option nuances beyond
passthrough.
