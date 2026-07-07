# Phase 7 — JsMessage + consumer handle with next/fetch

Blocked by: phase 6. Blocks: phase 8. Parallel with: phases 9, 10, 11, 12.

## Mission

Ship the JetStream message view with its acknowledgment vocabulary as
effects, the `processWith` combinator, and the consumer handle's two bounded
verbs (`next`, `fetch`). The unbounded `consume` is phase 8.

## Required reading

- `docs/DESIGN.html` §11.3 (the three-verbs table — fetch rows), §11.4 (acks)
- `repos/nats.js/jetstream/src/jsmsg.ts` (JsMsg full surface, `doAck`,
  `parseInfo`, nak delay encoding)
- `repos/nats.js/jetstream/src/jsapi_types.ts` ~1345 (`DeliveryInfo`)
- `repos/nats.js/jetstream/src/types.ts`: `Consumers.get` (~1278),
  `Consumer`/`ExportedConsumer` (~891), `NextOptions`/`FetchOptions` mixins
  (~590–702), `OrderedConsumerOptions` (~929)
- `repos/nats.js/jetstream/src/consumer.ts` ~379–416 (fetch self-termination
  + heartbeat error path — this decides your stream failure semantics)

## Deliverables

### `src/JsMessage.ts`

Per DESIGN §11.4 (interface extends the NatsMessage shape; reuse phase 2's
lazy-view machinery from `src/internal/message.ts`, extended for JsMsg):

```ts
export interface JsMessage extends NatsMessage.NatsMessage {
  readonly stream: string
  readonly consumer: string
  readonly seq: number
  readonly deliveryCount: number
  readonly redelivered: boolean
  readonly pending: number
  readonly time: DateTime.Utc
}
export const isJsMessage: (u: unknown) => u is JsMessage
export const ack:     (self: JsMessage) => Effect.Effect<void>
export const nak:     (self: JsMessage, options?: { readonly delay?: Duration.Input }) => Effect.Effect<void>
export const working: (self: JsMessage) => Effect.Effect<void>
export const term:    (self: JsMessage, options?: { readonly reason?: string }) => Effect.Effect<void>
export const ackAck:  (self: JsMessage) => Effect.Effect<boolean, NatsError.TimeoutError>
export const processWith: <A, E, R>(
  handler: (msg: JsMessage) => Effect.Effect<A, E, R>,
  options?: { readonly nakDelay?: Duration.Input }
) => (msg: JsMessage) => Effect.Effect<void, never, R>
```

`processWith` semantics (DESIGN §11.4): handler succeeds → `ack`; handler
fails (typed) → `nak` with optional delay; handler dies → `term` +
`Effect.logError` the cause. It never fails. `text`/`schemaJson` come for
free via the `NatsMessage` interface — verify they work on a `JsMessage`
in tests.

### `src/JetStream.ts` — `Service` addition + `JsConsumer`

```ts
readonly consumer: (stream: string, name?: string | OrderedConsumerOptions) =>
  Effect.Effect<JsConsumer,
    JetStreamError.StreamNotFoundError | JetStreamError.ConsumerNotFoundError
    | JetStreamError.JetStreamApiError>

export interface JsConsumer {
  readonly next: (options?: NextOptions) =>
    Effect.Effect<Option.Option<JsMessage.JsMessage>, JetStreamError.JetStreamErrors>
  readonly fetch: (options?: FetchOptions) =>
    Stream.Stream<JsMessage.JsMessage, JetStreamError.JetStreamErrors>
  readonly info: (options?: { readonly cached?: boolean }) =>
    Effect.Effect<ConsumerInfo, JetStreamError.JetStreamApiError>   // ConsumerInfo = SDK passthrough re-export
  // consume() lands in phase 8
}
export type NextOptions = { readonly expires?: Duration.Input }
export type FetchOptions = {
  readonly maxMessages?: number
  readonly maxBytes?: number
  readonly expires?: Duration.Input
}
```

Notes: `next` maps SDK `JsMsg | null` → `Option`. `fetch` uses phase 5's
`streamFromQueuedIterator` (acquire = `consumer.fetch(translated)`, release =
`close()`); it ends naturally on budget exhaustion and **fails typed** on the
heartbeats-missed error (`consumer.ts:388-416` throws
`JetStreamError("heartbeats missed")` for non-ordered fetch).

### Barrel

Add `JsMessage`.

## Tests (integration on `layerJetStream`, streams via phase 6's `withStream`;
consumers created in fixtures via raw-SDK `jetstreamManager().consumers.add`)

- `consumer()` resolves for an existing durable; `StreamNotFoundError` /
  `ConsumerNotFoundError` tags for missing ones; ordered consumer via
  options object (omit name).
- `next`: message present → `Option.some` with correct
  seq/deliveryCount/subject/time (DateTime.Utc equality vs published
  time within tolerance); empty stream + short expires → `Option.none`.
- ack: after ack, message not redelivered (recreate consumer with short
  `ack_wait`, assert no redelivery within 2× ack_wait).
- nak with delay: redelivered after ≥ delay, `redelivered === true`,
  `deliveryCount === 2`.
- term: never redelivered even past ack_wait.
- working: extends ack_wait (nak-less hold beyond ack_wait without
  redelivery while sending working; then ack).
- ackAck: returns true on first confirmed ack.
- `processWith`: success → acked; typed failure → redelivered; defect →
  termed + not redelivered (and does not fail the outer effect).
- fetch: exactly `maxMessages` when available; fewer + natural end on
  expires; `maxBytes` path; stream ends (assert completion, not hang).
- fetch heartbeat failure: hard to force without proxy — cover the mapping
  branch via a unit test on the adapter/error mapper with a synthetic
  `JetStreamError("heartbeats missed")`.
- schemaJson works on JsMessage payloads.

100% coverage on files added/touched.

## Out of scope

`consume` (phase 8). Push consumers. `msg.next()` (experimental SDK combo).
