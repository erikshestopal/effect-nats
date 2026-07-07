# Phase 8 — consume: the endless pull stream

Blocked by: phase 7. Parallel with: phases 9, 10, 11, 12.

## Mission

Ship `JsConsumer.consume` — the infinite, threshold-refilled,
interruption-safe pull stream with the `onNotification` side channel. This is
the flagship API of the library; its semantics table in DESIGN §11.3 is the
spec.

## Required reading

- `docs/DESIGN.html` §11.3 (consume row + notifications paragraph + scope-tied
  shutdown paragraph — normative)
- `repos/nats.js/jetstream/src/consumer.ts` ~208–416 (start loop, threshold
  re-pull at 75%, `closeListener`, heartbeat monitor: consume path only
  notifies + `resetPending()`), ~731 (`close()`)
- `repos/nats.js/jetstream/src/types.ts`: `ConsumeOptions` mixins (~592–720),
  `ConsumerMessages` (~915), `ConsumerNotification` all 15 variants (~726–876)

## Deliverables — `src/JetStream.ts` (`JsConsumer` addition)

```ts
readonly consume: (options?: ConsumeOptions) =>
  Stream.Stream<JsMessage.JsMessage, JetStreamError.JetStreamErrors>

export type ConsumeOptions = {
  readonly maxMessages?: number            // SDK default 100
  readonly maxBytes?: number
  readonly thresholdMessages?: number      // SDK default 75% of maxMessages
  readonly thresholdBytes?: number
  readonly abortOnMissingResource?: boolean
  readonly bind?: boolean
  readonly onNotification?: (n: ConsumerNotification) => Effect.Effect<unknown>
}
// ConsumerNotification: re-export the SDK union type as-is.
```

Implementation:
- Reuse `streamFromQueuedIterator`: acquire = `consumer.consume(translated)`,
  release = `messages.close()` (await it — it resolves after handover).
- `onNotification`: on acquire, fork a scope-bound fiber
  (`Effect.forkScoped`) draining `messages.status()` and running the handler
  per notification; handler failures are logged (`Effect.logWarning` with
  cause), never fail the message stream; fiber dies with the scope.
- Ordered consumers (consumer handle created name-less in phase 7) must flow
  through unchanged — the SDK recreates them transparently.
- The stream must fail typed only on the SDK's fatal paths (protocol 400 →
  `stop(err)`); connection close ends the stream (the SDK stops the iterator
  — decide: end vs fail; the SDK calls `stop()` without error on connection
  close, so the stream **ends**; document this in JSDoc and assert it).

## Tests (integration on `layerJetStream`)

- Endless flow: publish 50, consume with `maxMessages: 10` (forces ≥5
  re-pulls), `Stream.take(50)` collects all in order (durable, explicit
  acks via `processWith`).
- Backpressure observable: with `maxMessages: 10`, a slow consumer
  (`Effect.sleep` per message) never has more than ~10+threshold unacked
  in-flight — assert via consumer `info()` `num_ack_pending` bound.
- Scope-tied shutdown: interrupt mid-stream after 20 of 50; assert
  redelivery of unacked messages to a fresh consume session
  (interruption-safety, DESIGN §11.3's headline).
- `onNotification`: delete the consumer server-side mid-consume (raw-SDK
  jsm in the test) → handler receives `consumer_not_found`/
  `consumer_deleted` variants; with `abortOnMissingResource: true` the
  stream fails/ends per SDK semantics — assert observed behavior and
  document it in the test.
- Ordered consumer: consume across a server-side consumer delete → stream
  keeps flowing (SDK recreates), `ordered_consumer_recreated` notification
  observed.
- Notification handler failure: handler that dies does not disturb the
  message stream (assert full collection + a logged warning).
- Connection close mid-consume: close the client scope; stream ends (not
  fails); no leaked fibers (test exits).

100% coverage on lines added.

## Out of scope

Push consumers, `PrioritizedOptions`/`OverflowOptions` (pass nothing;
escape hatch covers them). Callback-mode consume.
