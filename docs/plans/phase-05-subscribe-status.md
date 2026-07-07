# Phase 5 — Subscribe, requestMany, status/closed + the iterator adapter

Blocked by: phase 4. Blocks: phases 6 and 12.

## Mission

Turn subscriptions into scope-tied Streams, ship `requestMany`, and expose
connection status. Critically, you also build the **shared
QueuedIterator→Stream adapter** in `src/internal/` that phases 7–12 (consume,
KV watch, discovery) reuse verbatim — design it once, well.

## Required reading

- `docs/DESIGN.html` §8 (subscriptions), §9 (requestMany), §10 (status/closed)
- `repos/nats.js/core/src/core.ts`: `Subscription` (~647), `SubOpts` (~93),
  `Status` union (~19–83), `QueuedIterator` (~769)
- `repos/nats.js/core/src/queued_iterator.ts` (termination/error semantics —
  `stop(err?)`, push of callbacks)
- `repos/nats.js/core/src/request.ts` (`RequestMany` strategies)
- `repos/effect-v4/packages/effect/src/Stream.ts`: `fromAsyncIterable`
  (~1458), `callback` (~777)

## Deliverables

### `src/internal/iterator.ts` — the shared adapter

```ts
export const streamFromQueuedIterator: <A, B, E>(options: {
  readonly acquire: Effect.Effect<QueuedIteratorLike<A>, E, Scope.Scope>
  readonly transform: (a: A) => B
  readonly onError: (u: unknown) => E
  readonly onRelease?: (iter: QueuedIteratorLike<A>) => Effect.Effect<void>
}) => Stream.Stream<B, E>
```

Semantics: acquire inside the stream's scope; iterate via
`Stream.fromAsyncIterable`; map thrown iterator errors through `onError`;
on scope close run `onRelease` (default: `stop()` where available). It must
tolerate iterators that end naturally (bounded fetch/keys) and iterators
stopped from the release path without double-termination. This is internal —
shape it to fit the five call sites (subscribe, requestMany, consume, KV
watch/history/keys, micro discovery), not for public consumption.

### `src/NatsClient.ts` `Service` additions

```ts
readonly subscribe: (subject: string, options?: SubscribeOptions) =>
  Stream.Stream<NatsMessage.NatsMessage, NatsError.NatsError>
export type SubscribeOptions = {
  readonly queue?: string
  readonly max?: number
}

readonly subscribeRaw: (subject: string, options?: SubscribeOptions) =>
  Stream.Stream<Msg, NatsError.NatsError>          // performance hatch, DESIGN §8

readonly requestMany: (subject: string, options: RequestManyOptions) =>
  Stream.Stream<NatsMessage.NatsMessage, NatsError.NatsError>
export type RequestManyOptions = {
  readonly payload?: Payload
  readonly headers?: NatsHeaders.Input
  readonly maxWait: Duration.Input
  readonly maxMessages?: number
  readonly stall?: Duration.Input
  readonly sentinel?: boolean
}

readonly status: Stream.Stream<ConnectionStatus>   // re-export SDK Status type as ConnectionStatus
readonly stats: Effect.Effect<Stats>               // SDK Stats passthrough type
```

Implementation notes:
- `subscribe`: `acquireRelease(nc.subscribe(...))` with release =
  `drain()` bounded, fallback `unsubscribe()`; DESIGN §8's diagram is the
  spec. A subscription closed by the server with an error
  (`sub.closed` resolves an Error) must surface as a typed stream failure —
  wire through `mapError`.
- `requestMany`: strategy derivation per DESIGN §9 —
  `maxMessages` → count, `stall` → stall, `sentinel: true` → sentinel,
  none → timer; always set `maxWait`. SDK returns
  `Promise<AsyncIterable<Msg>>` → `Stream.unwrap` + the adapter.
- `status`: each stream run calls `nc.status()` fresh (multiple consumers
  allowed). Infinite, never fails.

### Barrel

No new modules; `ConnectionStatus`/`Stats` re-exports live in `NatsClient.ts`.

## Tests (integration on `TestNatsServer.layer`)

- subscribe receives published messages in order; wildcard subject works.
- Scope-tied cleanup: `Stream.take(3)` on a subscription, then assert the
  server-side interest is gone (publish more, confirm a parallel raw-SDK
  subscription still gets them but the count on our side stays 3, and
  `getSubscriptions`-style check via a second connection or
  `connection.stats()` delta). Simplest robust assertion: after take(3)
  completes, `service.connection` has no active subscriptions
  (`(connection as any)` is banned — use the observable route: publish 10
  more, sleep a beat, assert our handler ran exactly 3 times).
- Queue groups: two subscribers same queue, N publishes, each message
  delivered exactly once across the pair.
- `max`: auto-unsubscribe after n; stream ends.
- Interruption mid-stream: fork, interrupt, assert clean exit (no leak).
- Server-forced subscription error (permissions require server config —
  skip; instead cover the error path by unit-testing the adapter's
  `onError` with a stub iterator that throws).
- requestMany: three responders → count strategy collects 3; stall strategy
  ends after quiet gap; sentinel: responder sends empty payload last;
  timer: collects until maxWait. No-responders → typed failure fast.
- status: with `reconnect` on, kill server, restart (respawn on same port —
  extend TestNatsServer with a `restart` capability if needed), assert the
  stream yields `disconnect` then `reconnecting` then `reconnect` typed
  variants.
- Adapter unit tests (stub iterators): natural end, thrown error, release
  stop, double-stop tolerance.

100% coverage on files added/touched.

## Out of scope

JetStream anything. Callback-mode subscription fast path (DESIGN §8 defers).
