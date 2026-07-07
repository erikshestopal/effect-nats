# Phase 3 — Connection: NatsConnector, NodeConnector, NatsClient lifecycle, TestNatsServer

Blocked by: phase 2. Blocks: phase 4.

## Mission

Ship the scoped connection service with both transports and the two-tier
layer API, plus the `TestNatsServer` fixture every later phase's integration
tests stand on. After this phase, `yield* NatsClient` works against a live
local server over both TCP and WebSocket, and interruption can never leak a
socket or a child process.

## Required reading

- `docs/DESIGN.html` §4 (service + transports + escape hatch), §5 (Options/Auth/layerConfig)
- `repos/nats.js/core/src/core.ts` ~872–1076 (`ConnectionOptions`, `TlsOptions`), ~377–564 (`NatsConnection`)
- `repos/nats.js/core/src/ws_transport.ts` ~340–370 (`wsconnect`, the global factory write)
- `repos/nats.js/transport-node/src/connect.ts` (node `connect`, `NodeConnectionOptions`, ws rejection)
- `repos/nats.js/core/src/authenticator.ts` (creds/nkey/token/userpass authenticator factories)
- `repos/effect-v4/packages/ai/anthropic/src/AnthropicClient.ts` (make/layer/layerConfig shape — copy it)
- `repos/effect-v4/packages/effect/src/Semaphore.ts` (connect-time critical section)
- `repos/nats.js/nst/launcher.ts` (how upstream spawns nats-server; config/port knobs)

## Deliverables

### `src/NatsConnector.ts`

```ts
export class NatsConnector extends Context.Service<NatsConnector, Connector>()(
  "effect-nats/NatsConnector"
) {}
export interface Connector {
  readonly connect: (options: ConnectionOptions) =>
    Effect.Effect<NatsConnection, NatsError.ConnectionError | NatsError.TimeoutError>
}
export const layerWebSocket: Layer.Layer<NatsConnector>   // wsconnect from @nats-io/nats-core
```

### `src/NodeConnector.ts`

```ts
export const layer: Layer.Layer<NatsConnector.NatsConnector>  // connect from @nats-io/transport-node
```

Separate file so WebSocket-only bundles never load `node:net` (DESIGN §4.2).

### `src/internal/connect.ts`

A module-level `Effect.makeSemaphoreUnsafe(1)` (check the v4 constructor
name in `Semaphore.ts`) wrapping every `connector.connect` call, so the SDK's
global `setTransportFactory` + dial pair is atomic within this library
(DESIGN §4.2). Both connector layers route through it.

### `src/NatsClient.ts`

Exactly DESIGN §4.1 + §5:

```ts
export class NatsClient extends Context.Service<NatsClient, Service>()("effect-nats/NatsClient") {}

export interface Service {
  readonly connection: NatsConnection            // escape hatch (§4.3)
  // publish/subscribe/request/... land in phases 4–5; declare the interface
  // incrementally: THIS phase ships connection, plus:
  readonly closed: Effect.Effect<Option.Option<NatsError.NatsError>>
}

export type Options = { /* DESIGN §5 verbatim (repo spelling: `?: T`, no `| undefined`) */ }
export type ReconnectOptions = { /* §5 */ }
export type Auth = /* §5 tagged union: UserPass | Token | Creds | NKey */
export const authUserPass, authToken, authCreds, authNKey  // one constructor per variant

export const make: (options?: Options) =>
  Effect.Effect<Service, NatsError.ConnectionError | NatsError.TimeoutError, NatsConnector | Scope.Scope>
export const layer: (options?: Options) =>
  Layer.Layer<NatsClient, NatsError.ConnectionError | NatsError.TimeoutError, NatsConnector>
export const layerConfig: (options: {
  readonly servers?: Config.Config<string | ReadonlyArray<string>>
  readonly name?: Config.Config<string>
  readonly auth?: Config.Config<Auth>
  /* mirror the remaining Options fields as Config values */
}) => Layer.Layer<NatsClient, NatsError.ConnectionError | NatsError.TimeoutError | Config.ConfigError, NatsConnector>
```

Internals:
- `make` = `Effect.acquireRelease(connector.connect(translated), release)`;
  release = `drain()` with a bounded timeout (default `"5 seconds"`,
  overridable via a non-public constant for tests) falling back to `close()`,
  and swallowing errors into the release path per Effect finalizer rules.
- Options translation (`src/internal/options.ts`): `Duration.Input` →
  millis via `Duration.toMillis(Duration.decode(...))`; `Auth` union →
  SDK authenticators with `Redacted.value` unwrapped at the last moment;
  `reconnect: false` → `reconnect: false`, `reconnect: {…}` → the four SDK
  fields; `transformOptions` applied last.
- `closed` = wrap `connection.closed()`; `void` → `Option.none()`, an Error →
  `Option.some(mapError(e))`.

### `test/utils/TestNatsServer.ts` (fixture — the contract later phases rely on)

```ts
export class TestNatsServer extends Context.Service<TestNatsServer, {
  readonly port: number
  readonly url: string          // `nats://127.0.0.1:${port}`
  readonly wsPort: number
  readonly wsUrl: string        // `ws://127.0.0.1:${wsPort}`
}>()("test/TestNatsServer") {}
export const layer: Layer.Layer<TestNatsServer>            // plain server
export const layerJetStream: Layer.Layer<TestNatsServer>   // adds -js with a temp store dir
```

Implementation: spawn `nats-server` via `node:child_process` inside
`Effect.acquireRelease` with `-a 127.0.0.1 -p 0`, a websocket block (write a
minimal temp config file enabling `websocket { port: 0, no_tls: true }`), and
for `layerJetStream` also `-js -sd <mkdtemp>`. Parse the actual ports from
the server's stderr startup lines ("Listening for client connections on
…" / websocket listener line) with a readiness deadline (~5s → die with a
clear message telling the operator to install nats-server). Release =
SIGTERM, await exit, remove temp dir. The layer must be reusable
per-test-file (`it.layer(TestNatsServer.layer)` style).

### Barrel

Add `NatsClient`, `NatsConnector`, `NodeConnector`.

## Tests (integration; unit where marked)

- Options translation (unit): each `Auth` variant produces the right SDK
  authenticator kind; Duration fields convert; `transformOptions` wins last;
  `Redacted` never appears in translated output as a wrapper.
- TCP connect/close: `NatsClient.layer` + `NodeConnector.layer` +
  `TestNatsServer.layer` — acquire, assert `connection.info` populated,
  scope close drains cleanly (server sees the client gone; assert via a
  second connection's `nc.request` on `$SYS`? keep it simple: assert
  `connection.isClosed()` after scope close and that the test process exits).
- WebSocket connect/close: same over `layerWebSocket` + `wsUrl`.
- Both transports concurrently: build one TCP and one WS client in parallel
  fibers repeatedly (≥20 iterations) — the semaphore must prevent transport
  cross-wiring (assert each `connection.getServer()` matches its intended
  port).
- Interruption: fork a fiber holding the layer scope, interrupt it
  mid-connect and post-connect; no leaked sockets (process exits), no
  unhandled rejections.
- `closed`: force-close the server while a client is connected with
  `reconnect: false`; `closed` resolves `Option.some(<NatsError>)`; graceful
  scope close resolves `Option.none()`.
- Connect failure: unreachable port → typed `ConnectionError`/`TimeoutError`
  (assert `_tag`), not a defect.
- `layerConfig`: provide via `ConfigProvider.fromMap`/`fromJson` (check v4
  ConfigProvider API), assert env-sourced connect works and missing config
  fails with `ConfigError`.

100% coverage on files added this phase.

## Out of scope

publish/subscribe/request (phases 4–5). Deno. TLS certificate testing
(`tls` passes through; unit-test the translation only).
