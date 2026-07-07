# Phase 6 — JetStream service, publish, JetStreamError

Blocked by: phase 5. Blocks: phases 7, 9, 10, 11 (parallel after this lands).

## Mission

Ship the `JetStream` service layered on `NatsClient`, its error module, and
acknowledged publishing with msgID dedup and optimistic-concurrency
expectations. This is the foundation the consumer phases and both
projections (KV, ObjectStore) build on.

## Required reading

- `docs/DESIGN.html` §11.1–11.2, §11.5 (errors)
- `repos/nats.js/jetstream/src/jsclient.ts` (~195 `jetstream()`, ~387–439
  publish retry loop)
- `repos/nats.js/jetstream/src/types.ts`: `JetStreamOptions` (~55),
  `PubAck` (~99), `JetStreamPublishOptions` + `StreamExpectations` (~247),
  `JetStreamClient` (~1185)
- `repos/nats.js/jetstream/src/jserrors.ts` (entire file) +
  `JetStreamApiCodes`

## Deliverables

### `src/JetStreamError.ts`

Tagged errors (`Schema.TaggedErrorClass`, ids
`"effect-nats/JetStreamError/<Tag>"`):
`JetStreamNotEnabledError{cause}`, `JetStreamError{cause}`,
`JetStreamStatusError{code, description}`,
`JetStreamApiError{code, status, description}` (code = NATS err_code,
status = HTTP-ish, per upstream getters), `StreamNotFoundError{stream}`,
`ConsumerNotFoundError{stream, consumer: Option<string>}`,
`InvalidNameError{name}`, `WrongLastSequenceError{expected: Option<number>, cause}`
(err_code 10071/10164). Union alias `AnyJetStreamError` (naming: keep
`JetStreamError` for the base class per SDK; the union export must not
collide — use `export type All = ...` inside the module namespace or name the
union `JetStreamErrors`; pick one, document in JSDoc, stay consistent).
Boundary mapper `src/internal/mapJsError.ts` (instanceof-suppressed like
phase 2's), discriminating `JetStreamApiError` subtypes by `err_code` via the
SDK's `JetStreamApiCodes`.

### `src/JetStream.ts`

```ts
export class JetStream extends Context.Service<JetStream, Service>()("effect-nats/JetStream") {}

export interface Service {
  readonly client: JetStreamClient          // escape hatch (raw SDK)
  readonly publish: (subject: string, options?: PublishOptions) =>
    Effect.Effect<PubAck,
      JetStreamError.JetStreamError | JetStreamError.JetStreamNotEnabledError
      | JetStreamError.WrongLastSequenceError | JetStreamError.JetStreamApiError
      | NatsError.TimeoutError>
  // consumer() lands in phase 7 — leave a doc comment breadcrumb.
}

export type JetStreamOptions = {
  readonly apiPrefix?: string
  readonly domain?: string
  readonly timeout?: Duration.Input
}
export type PublishOptions = {
  readonly payload?: Payload
  readonly headers?: NatsHeaders.Input
  readonly msgID?: string
  readonly timeout?: Duration.Input
  readonly retries?: number
  readonly expect?: {
    readonly streamName?: string
    readonly lastMsgID?: string
    readonly lastSequence?: number
    readonly lastSubjectSequence?: number
  }
}
export type PubAck = {
  readonly stream: string
  readonly seq: number
  readonly duplicate: boolean
}

export const make: (options?: JetStreamOptions) => Effect.Effect<Service, never, NatsClient>
export const layer: (options?: JetStreamOptions) => Layer.Layer<JetStream, never, NatsClient>
```

Notes: `make` is not scoped (the SDK `jetstream(nc)` allocates nothing);
translate our PublishOptions → `Partial<JetStreamPublishOptions>` in
`src/internal/` (expect.* mapping is 1:1 to `StreamExpectations` fields).
PubAck is re-shaped to our three fields (drop `domain` passthrough? keep it:
add `readonly domain: Option<string>` — DESIGN shows three fields; amend the
doc if you keep domain, or drop it; recommended: include
`domain: Option<string>` and amend DESIGN §11.2 in your PR).

### Test fixture: stream management

Tests need streams. Do NOT wait for phase 9 — create/purge streams in
fixtures via the raw SDK `jetstreamManager` from
`@nats-io/jetstream` inside `test/utils/jsFixtures.ts`
(`withStream(name, subjects)` scoped helper: add stream on acquire, delete
on release). Runs on `TestNatsServer.layerJetStream`.

### Barrel

Add `JetStream`, `JetStreamError`.

## Tests (integration on `layerJetStream`)

- publish → PubAck: correct stream, seq increments, `duplicate: false`.
- msgID dedup: same msgID twice within the window → second ack
  `duplicate: true`.
- `expect.lastSubjectSequence` conflict → `WrongLastSequenceError` (assert
  `_tag` and that a fresh correct sequence succeeds — the CAS loop shape).
- `expect.streamName` mismatch → `JetStreamApiError`.
- Publish to a subject no stream captures → typed failure (the SDK
  surfaces no-responders/timeout here; assert the mapped tag; document
  observed behavior in the test).
- JetStream disabled (plain `TestNatsServer.layer`): publish →
  `JetStreamNotEnabledError`.
- Error mapper unit tests: every jserrors class + err_code discrimination
  (construct `JetStreamApiError` from a synthetic ApiError via the SDK class).

100% coverage on files added.

## Out of scope

Consumers (phase 7+). `startBatch`/FastIngest/scheduled publish (DESIGN
§11.5 deferred list). Manager module (phase 9) — fixtures use the raw SDK.
