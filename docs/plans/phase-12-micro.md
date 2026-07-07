# Phase 12 — NatsMicro: the services framework

Blocked by: phase 5 (core only — no JetStream dependency). Parallel with:
phases 6–11.

## Mission

Ship the declarative micro-services wrapper: endpoints-as-values with
effectful handlers (dependencies in `R`), the error-header protocol, scoped
service lifetime, and the discovery client.

## Required reading

- `docs/DESIGN.html` §14 (normative — handler semantics, defect policy,
  priced trade-off)
- `repos/nats.js/services/src/types.ts`: `ServiceMsg` (~24),
  `ServiceHandler`/`Endpoint` (~33–59), `ServiceGroup` (~67),
  `ServiceConfig` (~190), `Service` (~230–268), `ServiceError` +
  headers (~270–294), `ServiceClient` (~303)
- `repos/nats.js/services/src/service.ts`: `_addEndpoint` (~615 — iterator
  mode), control subjects (~92–98), auto-stop on sub error, close listener
- `repos/nats.js/services/src/internal_mod.ts`: `Svcm` (~33)

## Deliverables — `src/NatsMicro.ts`

Per DESIGN §14 signatures (repo spelling), with these implementation
decisions locked:

- `make<R>` wraps `new Svcm(nc).add(config)` in `acquireRelease`
  (release = `svc.stop()`); for each endpoint, call `addEndpoint(name,
  { subject?, queue?, metadata? })` **without** a handler function (iterator
  mode), adapt the returned `QueuedIterator<ServiceMsg>` through phase 5's
  adapter, and run:
  `Stream.mapEffect(handleOne, { concurrency: endpoint.concurrency ?? 1 })`
  in an `Effect.forkScoped` fiber per endpoint.
- `handleOne`: wrap the `ServiceMsg` as `NatsMessage` (retain the raw
  ServiceMsg for respondError); run the handler; on success with a
  `Payload` → `respond(payload)`; on success `void`/`undefined` → do
  nothing (handler responded manually via `NatsMessage.respond` or chose
  not to); on `EndpointError{code, description}` →
  `respondError(code, description)`; on any other typed failure or defect →
  `respondError(500, "internal error")` + `Effect.logError(cause)`. The
  per-message effect never fails (parallel to `JsMessage.processWith`).
- `EndpointError` as in DESIGN §14 (TaggedErrorClass, fields
  `code: Schema.Int`, `description: Schema.String`).
- `RunningService`: `{ readonly service: SDK.Service /* escape hatch */;
  readonly info: ServiceInfo; readonly stopped:
  Effect.Effect<Option.Option<NatsError.NatsError>> }` (from
  `svc.stopped` promise, same folding as `NatsClient.closed`).
- `layer<R>` = `Layer.effectDiscard(make(options))` — runs for the layer's
  lifetime; requirements `NatsClient | R`.
- `client`: wraps `Svcm.client()` — `ping`/`info`/`stats` are bounded
  discovery streams (adapter over the returned `QueuedIterator`s;
  they terminate per requestMany semantics). `DiscoverOptions =
  { readonly name?: string; readonly id?: string }`.
- Validate `name`/`version` shape? No — the SDK validates; map its
  rejection into a typed `ConnectionError`-family or defect per what it
  actually throws (`InvalidArgumentError` → defect per §6 policy; assert in
  a test and document).

### Barrel

Add `NatsMicro`.

## Tests (integration on `TestNatsServer.layer` — plain server, no JetStream)

- End-to-end request: layer up a service with an `echo` endpoint; call via
  `NatsClient.request("<name-default-subject>")`; payload round-trips.
- Handler with dependency in `R`: endpoint requiring a test service;
  `NatsMicro.layer` requirement surfaces in the type (typecheck test with
  `expect-type` — the repo has `expect-type` as a devDependency) and works
  provided.
- `EndpointError`: handler fails typed → caller sees
  `Nats-Service-Error`/`Nats-Service-Error-Code` headers with code/
  description (assert via raw request's headers, and via SDK
  `ServiceError.isServiceError`).
- Defect: handler dies → caller gets 500 "internal error" headers; service
  keeps serving (send a second request; it succeeds).
- Concurrency: endpoint with `concurrency: 4` and a 100ms sleep; 8
  concurrent requests complete in ~2 batches not 8 serial (assert elapsed
  < serial bound).
- Manual respond: handler returns void after calling
  `NatsMessage.respond` itself; caller gets it; no double-respond.
- Discovery: `$SRV` control subjects answer — `client.ping()` finds the
  service (name/id/version); `client.stats()` shows `num_requests` > 0
  after traffic and `num_errors` > 0 after an EndpointError.
- Queue-group scaling: two instances of the same service; each request
  answered exactly once.
- Scoped shutdown: close the layer scope; `$SRV.PING` no longer answers
  (requestMany with short maxWait → empty); test exits clean.
- Groups/custom subject: endpoint with explicit `subject` containing a
  group-style prefix (e.g. `"acct.v1.check"`) is reachable there.

100% coverage on files added.

## Out of scope

Runtime endpoint mutation (DESIGN §14 prices this). `statsHandler`
custom-stats callback (escape hatch). Typed subject routing.
