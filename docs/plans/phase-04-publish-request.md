# Phase 4 — Publish, flush, rtt, request

Blocked by: phase 3. Blocks: phase 5.

## Mission

Add the one-shot operations to `NatsClient.Service`: fire-and-forget publish,
flush/rtt, and typed request/reply including the `NoRespondersError` fast
path.

## Required reading

- `docs/DESIGN.html` §9 (request), §6 (narrowest-union rule)
- `repos/nats.js/core/src/core.ts`: `publish` (~404), `request` (~445),
  `RequestOptions` (~324), `PublishOptions` (~742)
- `repos/nats.js/core/src/nats.ts` ~371–430 (`request` impl: default
  timeout 1000ms, RequestError wrapping, `isNoResponders()` on cause)

## Deliverables — extend `src/NatsClient.ts` `Service`

```ts
readonly publish: (subject: string, options?: PublishOptions) =>
  Effect.Effect<void,
    NatsError.InvalidSubjectError | NatsError.ClosedConnectionError
    | NatsError.DrainingConnectionError | NatsError.PermissionViolationError>
export type PublishOptions = {
  readonly payload?: Payload
  readonly headers?: NatsHeaders.Input
  readonly replyTo?: string
}

readonly flush: Effect.Effect<void, NatsError.ClosedConnectionError | NatsError.TimeoutError>
readonly rtt: Effect.Effect<Duration.Duration,
  NatsError.ClosedConnectionError | NatsError.DrainingConnectionError | NatsError.TimeoutError>

readonly request: (subject: string, options?: RequestOptions) =>
  Effect.Effect<NatsMessage.NatsMessage,
    NatsError.TimeoutError | NatsError.NoRespondersError | NatsError.RequestError
    | NatsError.InvalidSubjectError | NatsError.ClosedConnectionError
    | NatsError.DrainingConnectionError>
export type RequestOptions = {
  readonly payload?: Payload
  readonly headers?: NatsHeaders.Input
  readonly timeout?: Duration.Input      // @default "1 second" (SDK default)
}
```

Implementation notes:
- `publish` is synchronous in the SDK (throws) — wrap with `Effect.try` +
  `mapError`; it is NOT `tryPromise`.
- `request` unwraps the SDK's `RequestError` wrapper: when
  `cause` is a `NoRespondersError`, surface `NoRespondersError{subject}`;
  when cause is a timeout, surface `TimeoutError`; otherwise
  `RequestError{subject, cause}`. Discriminate inside
  `src/internal/mapError.ts` helpers (instanceof stays confined there).
- `rtt` returns `Duration.millis(n)`.
- Interruption semantics: abandon the SDK promise (document in JSDoc,
  DESIGN §9).
- If phase 2's `respond` left a connection-dependency seam open, close it
  now: `NatsMessage.respond` must work for messages received by this client
  (it delegates to the retained raw `Msg`).

## Tests (integration on `TestNatsServer.layer` unless marked)

- publish→receive: raw-SDK subscription (test-side, via
  `service.connection.subscribe`) sees payload, headers, replyTo.
- publish after scope close → `ClosedConnectionError` (grab the service,
  close the scope, then call — assert `_tag`).
- Invalid subject (`""` or `"a b"`) → `InvalidSubjectError`.
- `flush` and `rtt`: rtt returns a positive Duration.
- request happy path: responder fixture (raw SDK sub that
  `msg.respond`s); assert echoed payload + headers on the `NatsMessage`.
- request no responders → `NoRespondersError` with the subject, and it
  resolves fast (well under the timeout — assert elapsed < 500ms with
  timeout "2 seconds").
- request timeout: responder that never replies (subscribe but don't
  respond) → `TimeoutError` at ~the configured timeout.
- request + respond round-trip through OUR types: responder implemented with
  a raw subscription calling `NatsMessage.respond` on a wrapped message.
- Interruption mid-request: no unhandled rejection, test exits.

100% coverage on lines added this phase.

## Out of scope

`requestMany` (phase 5 — it needs the iterator adapter). `publishMessage`,
`respondMessage` (escape hatch).
