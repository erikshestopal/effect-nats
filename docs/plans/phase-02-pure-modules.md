# Phase 2 — Pure modules: NatsError, NatsHeaders, NatsMessage

Blocked by: phase 1. Blocks: phase 3.

## Mission

Ship the three server-independent modules: the tagged error taxonomy plus the
internal SDK-error boundary mapper, immutable headers, and the immutable
message view. Everything here is unit-testable without a NATS server.

## Required reading

- `docs/DESIGN.html` §6 (errors), §7 (messages & headers)
- `repos/nats.js/core/src/errors.ts` (entire file — the taxonomy you mirror)
- `repos/nats.js/core/src/core.ts` lines ~300–330 (`MsgHdrs`), ~576–619 (`Msg`)
- `repos/nats.js/core/src/headers.ts` (`MsgHdrsImpl`, `headers()`, `Match`)
- `repos/effect-v4/packages/effect/src/unstable/socket/Socket.ts` lines 200–350
  (the `Schema.ErrorClass`/`TaggedErrorClass` idiom to copy)
- `repos/effect-v4/packages/effect/src/Schema.ts` — `fromJsonString` (~line 10950)

## Deliverables

### `src/NatsError.ts`

Twelve `Schema.TaggedErrorClass` errors exactly as DESIGN §6's table:
`ConnectionError{cause}`, `TimeoutError`, `ClosedConnectionError`,
`DrainingConnectionError`, `NoRespondersError{subject}`,
`RequestError{subject, cause}`, `AuthorizationError`,
`PermissionViolationError{operation, subject, queue: Option<string>}`,
`ProtocolError{cause}`, `InvalidSubjectError{subject}`,
`UserAuthenticationExpiredError`, `NoReplySubjectError{subject}`.
Identifier pattern: `"effect-nats/NatsError/<Tag>"`. Plus:

```ts
export type NatsError = /* union of all twelve */
export const isNatsError: (u: unknown) => u is NatsError
```

`cause` fields use `Schema.Defect`. `queue` uses an Option schema
(`Schema.Option(Schema.String)` or v4 equivalent — check Schema.ts for the
current combinator name).

### `src/internal/mapError.ts`

`mapError(u: unknown): NatsError` — discriminates SDK errors via
`instanceof` against classes imported from `@nats-io/nats-core`
(each line suppressed with `// ast-grep-ignore: no-instanceof`; this file is
the *only* place instanceof is permitted). Unrecognized inputs are **not**
mapped — return them via a thrown defect path (`Effect.die`) at call sites;
expose `mapErrorOrDie(u): Effect<never, NatsError>` if that reads better.
`InvalidOperationError`/`InvalidArgumentError` are deliberately unmapped
(defects), per DESIGN §6. Never match on `.name` — SDK `NoRespondersError`
has `name === "NoResponders"`.

### `src/NatsHeaders.ts`

Immutable view + constructor per DESIGN §7:

```ts
export const TypeId = "~effect-nats/NatsHeaders"
export interface NatsHeaders extends Iterable<readonly [string, ReadonlyArray<string>]> { readonly [TypeId]: ... }
export type Input =
  | Readonly<Record<string, string | ReadonlyArray<string>>>
  | Iterable<readonly [string, string]>
export const empty: NatsHeaders
export const fromInput: (input?: Input) => NatsHeaders
export const get: dual — (self, key) => Option.Option<string>
export const getAll: dual — (self, key) => ReadonlyArray<string>
export const keys: (self) => ReadonlyArray<string>
export const toRecord: (self) => Record<string, ReadonlyArray<string>>
export const isNatsHeaders: (u: unknown) => u is NatsHeaders
```

Internal (not exported from the module; place helpers in
`src/internal/headers.ts`): `fromMsgHdrs(h: MsgHdrs | undefined): NatsHeaders`
lazy view (no copying until read) and `toMsgHdrs(input): MsgHdrs` for the
publish path (via the SDK `headers()` factory). Default matching is the SDK
default (exact, case-sensitive); do not expose `Match` in v1.

### `src/NatsMessage.ts`

Per DESIGN §7:

```ts
export const TypeId = "~effect-nats/NatsMessage"
export interface NatsMessage {
  readonly [TypeId]: typeof TypeId
  readonly subject: string
  readonly payload: Uint8Array
  readonly replyTo: Option.Option<string>
  readonly headers: NatsHeaders.NatsHeaders
}
export const isNatsMessage: (u: unknown) => u is NatsMessage
export const text: (self: NatsMessage) => string
export const schemaJson: <S extends Schema.Top>(schema: S) =>
  (self: NatsMessage) => Effect.Effect<S["Type"], Schema.SchemaError>
export const respond: dual —
  (self: NatsMessage, options?: RespondOptions) =>
    Effect.Effect<void, NatsError.NoReplySubjectError | NatsError.ClosedConnectionError>
export type RespondOptions = {
  readonly payload?: Payload            // Uint8Array | string, re-export from nats-core
  readonly headers?: NatsHeaders.Input
}
```

Internal constructor `fromMsg(msg: Msg): NatsMessage` in
`src/internal/message.ts`: lazy getters (compute `replyTo`/`headers` on first
access), retains the raw `Msg` privately (a non-exported symbol key) so
`respond` delegates to `msg.respond()`. `respond` fails
`NoReplySubjectError` when `replyTo` is none, maps a `false` return from the
SDK (connection closed) appropriately. `schemaJson` = decode bytes with
`TextDecoder` once, then `Schema.fromJsonString(schema)` decoding — build the
decoder once per `schemaJson(schema)` application, not per message.

### Barrel

Add `NatsError`, `NatsHeaders`, `NatsMessage` lines to `src/index.ts`.

## Tests (unit, no server)

- `NatsError.test.ts`: every SDK error class maps to its tagged twin
  (construct SDK errors directly from `@nats-io/nats-core` exports);
  `NoResponders` name-quirk covered; unknown error → defect;
  `isNatsError` accepts all twelve, rejects foreign values; schema round-trip
  (encode/decode one error).
- `NatsHeaders.test.ts`: `fromInput` record + iterable forms; `get` returns
  `Option.none` on absent key; `getAll` multi-value; `empty` iterates to
  nothing; view over a real `MsgHdrsImpl` (build via SDK `headers()`).
- `NatsMessage.test.ts`: build a stub SDK `Msg` (plain object satisfying the
  interface, or `MsgImpl` if constructible without a connection); `replyTo`
  none/some; `text`; `schemaJson` success and failure (assert
  `Schema.SchemaError`, check the issue path); `respond` fails
  `NoReplySubjectError` without reply; `respond` delegates payload+headers
  (spy on the stub).

100% coverage on all files this phase adds.

## Out of scope

Anything touching a live connection. `subscribeRaw`. Header `Match` modes.
