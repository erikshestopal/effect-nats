# Phase 10 — NatsKv (+ KeyValueStore adapter)

Blocked by: phase 6 (uses `JetStream` service + phase 5 adapter). Parallel
with: phases 7, 8, 9, 11, 12.

## Mission

Ship the KV projection: bucket handles with Option-returning reads,
CAS-typed writes, watch/history/keys streams, and the drop-in
`effect/unstable/persistence/KeyValueStore` layer.

## Required reading

- `docs/DESIGN.html` §12 (normative, including termination semantics)
- `repos/nats.js/kv/src/kv.ts`: `Kvm` (~211), revision semantics (~590–669),
  watch impl (~895–950), keys self-termination (~952–999)
- `repos/nats.js/kv/src/types.ts`: `KvEntry` (~24), `KvWatchEntry` (~48),
  `KvOptions`/`KvLimits` (~267), `KvWatchOptions` + `KvWatchInclude` (~298–340),
  `KV`/`RoKV` (~342–452)
- `repos/effect-v4/packages/effect/src/unstable/persistence/KeyValueStore.ts`
  (~38 interface, ~183 `KeyValueStoreError`, `make` constructor if present)

## Deliverables — `src/NatsKv.ts`

Handles + single-bucket service, per DESIGN §12 verbatim (with repo
spelling). Key points beyond the doc's signatures:

- `open` maps the SDK's bucket-missing failure to `BucketNotFoundError{bucket}`
  (add it to `JetStreamError.ts`? No — it is KV-domain: define
  `BucketNotFoundError` and `KeyExistsError` in `NatsKv.ts` itself,
  `Schema.TaggedErrorClass`, ids `"effect-nats/NatsKv/<Tag>"`).
- `BucketOptions`: subset of `KvOptions` with Effect types —
  `history?: number`, `ttl?: Duration.Input`, `maxBucketSize?: number`,
  `maxValueSize?: number`, `storage?: "file" | "memory"`, `replicas?: number`,
  `description?: string`, `compression?: boolean`, plus
  `transformOptions?: (o: Partial<KvOptions>) => Partial<KvOptions>` hatch.
- `KvEntry`: our immutable view — `key`, `value: Uint8Array`,
  `revision: number`, `operation: "PUT" | "DEL" | "PURGE"`,
  `created: DateTime.Utc`, `isUpdate: boolean` (false except on watch
  entries during live tail — mirror `KvWatchEntry`), plus module fns
  `NatsKv.entryText(entry)`, `NatsKv.entrySchemaJson(schema)` reusing the
  shared payload helpers.
- `create` detects the underlying CAS conflict (phase 6's
  `WrongLastSequenceError` mapping) and re-tags `KeyExistsError{key}`;
  mirror the SDK's deleted-key-recreate behavior by delegating to SDK
  `kv.create` (do not reimplement its retry logic — wrap it and map).
- `update` surfaces `WrongLastSequenceError` directly (typed CAS).
- `watch` options: `key?: string | ReadonlyArray<string>`,
  `include?: "lastValue" | "allHistory" | "updatesOnly"` (map to
  `KvWatchInclude`), `ignoreDeletes?: boolean`,
  `resumeFromRevision?: number`. Infinite stream via the shared adapter
  (release = `stop()`). `history`/`keys` are bounded — they end naturally.
- `layerKeyValueStore(bucket, options?)`: implements the v4
  `KeyValueStore` interface over a bucket — read the interface carefully and
  implement it completely (get/getUint8Array/set/remove/clear/size/has/
  modify — whatever the interface actually requires; map our typed errors
  into `KeyValueStoreError` with the cause preserved). `clear` = purge all
  keys (`kv.keys` + purge loop, or destroy+recreate — pick the one that
  matches interface semantics and document).

### Barrel

Add `NatsKv`.

## Tests (integration on `layerJetStream`)

- create/open: `open` on missing bucket → `BucketNotFoundError`; `create`
  is idempotent-open per SDK.
- get: missing key → `Option.none`; after put → `Option.some` with
  value/revision; `revision` option reads historical value (history ≥ 2).
- put returns increasing revisions.
- create on live key → `KeyExistsError`; create on deleted key succeeds
  (SDK recreate path).
- update CAS: stale revision → `WrongLastSequenceError`; typed retry loop
  (get → update) converges under concurrent writers (race two fibers).
- delete vs purge: delete leaves history (history stream shows DEL
  tombstone + prior PUT); purge erases (history shows only PURGE).
- keys: bounded, lists live keys only, filter works, ends by itself.
- history: bounded, ordered revisions for a key.
- watch: `lastValue` catch-up then live updates (`isUpdate` flips);
  `updatesOnly` sees only new puts; `ignoreDeletes`; infinite — ends only
  via scope close (interrupt and assert clean exit).
- entrySchemaJson decodes; malformed → SchemaError.
- KeyValueStore adapter: run the interface contract — get/set/remove/has/
  size round-trips; and if the v4 repo exposes a reusable test-suite helper
  for KeyValueStore implementations (`KeyValueStore.test`? check
  `repos/effect-v4` tests), run it against the layer.

100% coverage on files added.

## Out of scope

KV codecs (`KvCodecs` — escape hatch via `transformOptions`), mirrors/
sources, `Kvm.list`.
