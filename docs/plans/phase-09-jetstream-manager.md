# Phase 9 — JetStreamManager

Blocked by: phase 6. Parallel with: phases 7, 8, 10, 11, 12.

## Mission

Ship the admin-plane service: stream and consumer CRUD with
`Duration`-converted config fields and paginated listers exposed as Streams.

## Required reading

- `docs/DESIGN.html` §11.5 (config passthrough + listers-are-streams — normative)
- `repos/nats.js/jetstream/src/types.ts`: `StreamAPI` (~348),
  `ConsumerAPI` (~427), `JetStreamManager` (~542), `Lister` (~340)
- `repos/nats.js/jetstream/src/jsapi_types.ts`: `StreamConfig` (~140),
  `StreamUpdateConfig` (~187), `ConsumerConfig` (~1084), `StreamInfo` (~72),
  `ConsumerInfo` (~876)
- `repos/effect-v4/packages/effect/src/Stream.ts` `paginate` (~1684) — or
  `paginateChunkEffect`-style variant; pick what fits `Lister.next(): Promise<T[]>`

## Deliverables — `src/JetStreamManager.ts`

```ts
export class JetStreamManager extends Context.Service<JetStreamManager, Service>()(
  "effect-nats/JetStreamManager"
) {}
export const make: (options?: JetStream.JetStreamOptions) =>
  Effect.Effect<Service, JetStreamError.JetStreamNotEnabledError | NatsError.TimeoutError, NatsClient>
export const layer: (options?: JetStream.JetStreamOptions) =>
  Layer.Layer<JetStreamManager, JetStreamError.JetStreamNotEnabledError | NatsError.TimeoutError, NatsClient>

export interface Service {
  readonly manager: SDK.JetStreamManager      // escape hatch
  readonly streams: {
    readonly add:    (config: StreamConfigInput) => Effect.Effect<StreamInfo, JetStreamError.JetStreamErrors>
    readonly update: (name: string, config: StreamUpdateInput) => Effect.Effect<StreamInfo, JetStreamError.JetStreamErrors>
    readonly info:   (name: string) => Effect.Effect<StreamInfo, JetStreamError.StreamNotFoundError | JetStreamError.JetStreamApiError>
    readonly delete: (name: string) => Effect.Effect<void, JetStreamError.JetStreamErrors>
    readonly purge:  (name: string, options?: PurgeOptions) => Effect.Effect<PurgeResponse, JetStreamError.JetStreamErrors>
    readonly list:   (subject?: string) => Stream.Stream<StreamInfo, JetStreamError.JetStreamErrors>
    readonly names:  (subject?: string) => Stream.Stream<string, JetStreamError.JetStreamErrors>
  }
  readonly consumers: {
    readonly add:    (stream: string, config: ConsumerConfigInput) => Effect.Effect<ConsumerInfo, JetStreamError.JetStreamErrors>
    readonly update: (stream: string, durable: string, config: ConsumerUpdateInput) => Effect.Effect<ConsumerInfo, JetStreamError.JetStreamErrors>
    readonly info:   (stream: string, consumer: string) => Effect.Effect<ConsumerInfo, JetStreamError.ConsumerNotFoundError | JetStreamError.JetStreamApiError>
    readonly delete: (stream: string, consumer: string) => Effect.Effect<void, JetStreamError.JetStreamErrors>
    readonly list:   (stream: string) => Stream.Stream<ConsumerInfo, JetStreamError.JetStreamErrors>
    readonly pause:  (stream: string, consumer: string, options: { readonly until: DateTime.Utc }) => Effect.Effect<PauseResponse, JetStreamError.JetStreamErrors>
    readonly resume: (stream: string, consumer: string) => Effect.Effect<PauseResponse, JetStreamError.JetStreamErrors>
  }
}
```

Config typing (DESIGN §11.5 normative): `StreamConfigInput` /
`ConsumerConfigInput` are the SDK `Partial<StreamConfig>` /
`Partial<ConsumerConfig>` shapes **with duration-valued fields replaced** by
`Duration.Input` and converted internally:
`max_age`, `duplicate_window`, `ack_wait`, `idle_heartbeat`, `max_expires`,
`inactive_threshold`, `backoff` (array) are `Nanos` upstream — convert via
`Duration.toNanos`? Check v4 Duration for a nanos accessor; the SDK exports
`nanos(millis)` — converting `Duration.toMillis` → `nanos()` is acceptable
and loses nothing above 1ms resolution. Everything else passes through
verbatim (snake_case preserved; re-export SDK `StreamInfo`, `ConsumerInfo`,
enums `RetentionPolicy`, `StorageType`, `AckPolicy`, `DeliverPolicy`,
`ReplayPolicy` as types).

Listers: wrap `Lister.next(): Promise<T[]>` pages with `Stream.paginate*`
(empty page = done — verify against `jslister.ts` termination behavior).

`streams.info` maps err_code 10059 → `StreamNotFoundError`;
`consumers.info` maps 10014 → `ConsumerNotFoundError` (mapper exists from
phase 6).

### Barrel

Add `JetStreamManager`.

## Tests (integration on `layerJetStream`)

- Stream CRUD round-trip: add (with `max_age: "1 hour"` as Duration.Input —
  assert server-side nanos via returned `config.max_age`), info, update,
  purge (message count drops), delete → info fails `StreamNotFoundError`.
- Consumer CRUD: add durable with `ack_wait: "2 seconds"`, info, update,
  delete → `ConsumerNotFoundError`.
- pause/resume: paused consumer delivers nothing (short consume with
  expires), resume delivers.
- Listers: create 7 streams, `streams.list()` collects 7 via Stream (page
  size is server-controlled; assert count + distinct names);
  `consumers.list` similarly.
- `names()` stream.
- Replace phase 6/7's raw-SDK `withStream` fixture usage? **No** — leave
  fixtures as-is (they are test-side); but add a note in the PR if you
  choose to migrate them.

100% coverage on files added.

## Out of scope

`DirectStreamAPI`, `getMessage`/`deleteMessage` (escape hatch),
`advisories`, account stats, `consumers.unpin/reset`.
