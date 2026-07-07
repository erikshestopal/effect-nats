# effect-nats implementation plans

Thirteen phases implementing the design in `docs/DESIGN.html` (rev 2, locked).
Each phase file is a self-contained brief for an independent agent. Read this
README fully before your phase file; it is the shared context every phase
assumes.

## The contract

`docs/DESIGN.html` is the design contract. Your phase file restates the exact
signatures you must ship, but the design doc carries the reasoning and the
semantics (termination behavior, error mapping, scope rules). If your
implementation work uncovers a contradiction with the design doc, **stop,
amend the doc in your PR, and call the amendment out in the PR description**
— the document, not the diff, is the contract.

## Required reading (every phase, in order)

1. `AGENTS.md` (repo root) — commands and conventions.
2. `docs/DESIGN.html` — at minimum §1–§7, plus the sections your phase file names.
3. `rules/*.yml` — every ast-grep rule; they are `severity: error` and gate CI.
4. The vendored-source files your phase file lists. **Never** read
   `node_modules` or the web for SDK/Effect facts; the vendored repos are the
   source of truth:
   - `repos/effect-v4/` — Effect v4 source. Idiom references:
     `packages/ai/anthropic/src/AnthropicClient.ts` (service/layer/layerConfig
     shape), `packages/effect/src/unstable/socket/Socket.ts` (errors, TypeId,
     JSDoc), `packages/effect/src/Stream.ts`, `packages/effect/src/Context.ts`.
   - `repos/nats.js/` — NATS client v3 monorepo. Core surface:
     `core/src/core.ts`, `core/src/errors.ts`, `core/src/nats.ts`.

## Environment

- Runtime/PM: **bun** (`bun install` at repo root). Node is present but the
  toolchain is bun-first.
- Typecheck: `bun run typecheck` (tsgo). **Never** run `tsc`.
- Full gate: `vp run --log labeled check:all` = lint (oxlint via `vp check`)
  + `sg scan packages` (ast-grep) + tests + `tsgo --noEmit`.
- Tests only: `bunx --bun vp test run`. Coverage: `bunx --bun vp test run --coverage`
  (wired in phase 1).
- Integration tests need a local `nats-server` binary on `$PATH`
  (`brew install nats-server`, or download from
  github.com/nats-io/nats-server/releases; verify with `nats-server --version`).
  Tests **spawn their own server per test file** via the `TestNatsServer`
  helper (built in phase 3) — never assume a server is already running, never
  use a fixed port.

## Repo layout and naming

- Package: `packages/core`, npm name `effect-nats` (already renamed; root
  tsconfig paths map `effect-nats` / `effect-nats/*` →
  `packages/core/src/*.ts`, so tests and examples import from
  `effect-nats/NatsClient` etc.).
- One flat PascalCase file per public module in `packages/core/src/`
  (`NatsClient.ts`, `NatsError.ts`, …). Non-public helpers live in
  `packages/core/src/internal/` — blocked from the exports map, exempt from
  JSDoc-completeness expectations, still subject to all lint/ast-grep rules.
- `packages/core/src/index.ts` is the barrel: one
  `export * as ModuleName from "./ModuleName.ts"` per public module, each with
  a `@since 0.1.0` JSDoc block, alphabetized. Every phase that adds a module
  adds its barrel line.
- Tests in `packages/core/test/<ModuleName>.test.ts`; shared fixtures in
  `packages/core/test/utils/`.

## Code conventions (deviations = review rejection)

- Effect v4 idioms only. Services: class-style
  `Context.Service<Self, Shape>()("effect-nats/Name")` (the design doc keeps
  `effect-nats/…` service-key strings even though the npm name is scoped —
  keys are identifiers, not import paths). Errors:
  `Schema.TaggedErrorClass` / `Schema.ErrorClass`. Layers named `layer`,
  `layerConfig`, `layerWebSocket` — never `Live`/`Default`.
- Prefer Effect modules over hand-rolled code: `Option`, `Predicate`,
  `Match`, `Duration`, `Redacted`, `Config`, `Schema`, `Stream`, `Queue`,
  `DateTime`, `Equal`, `Order`. The ast-grep rules enforce much of this
  (`no-native-*`, `no-switch-statement`, `no-undefined-equality`, …).
- Optionality: options-bag fields are `readonly field?: T` — **never**
  `| undefined` or `| null` anywhere (ast-grep `no-nullish-types` is an
  error). `exactOptionalPropertyTypes` is **enabled**: optional means
  present-or-absent, and explicitly passing `undefined` is a type error.
  Build conditional option objects with conditional spreads
  (`...(Predicate.isNotUndefined(x) ? { timeout: x } : {})`), including at
  the SDK-translation boundary — never assign `undefined` into a field.
  Model positions use `Option.Option<T>`. See DESIGN §5.
- One options object over positional parameters (lint rule
  `agent/max-positional-params` enforces).
- `instanceof` is banned (`no-instanceof`) including `toBeInstanceOf` in
  tests. The **only** sanctioned use is SDK-error discrimination inside
  `src/internal/` boundary mappers, each occurrence suppressed with a
  preceding `// ast-grep-ignore: no-instanceof` line. Match on `instanceof`,
  never on `error.name` (the SDK's `NoRespondersError` sets
  `name = "NoResponders"`). Tests assert on `_tag` via module guards
  (`NatsError.isNatsError`, `Predicate.isTagged`), never `toBeInstanceOf`.
- JSDoc on every public export: `@since 0.1.0`, `@category`, and an
  `@example` fenced block on primary entry points; prose sections
  (`**When to use**`, `**Details**`) follow the style in
  `repos/effect-v4/packages/effect/src/unstable/socket/Socket.ts`.
- Tests: `@effect/vitest` — `import { assert, describe, it } from "@effect/vitest"`,
  `it.effect` for effectful tests, `Effect.gen` bodies, layers provided via
  `it.layer` or explicit `Effect.provide`. No raw `async` test bodies for
  effectful code.

## Verification protocol (every phase, no exceptions)

A phase is done when **all** of the following pass locally, in this order:

1. `bun install` clean (lockfile updated only if your phase adds deps).
2. `vp run --log labeled check:all` — zero errors.
3. `bunx --bun vp test run --coverage` — **100% line, statement, function,
   and branch coverage for every `packages/core/src/**` file your phase adds
   or touches** (global 100% is enforced from phase 13 onward). No
   `c8 ignore`/`istanbul ignore` comments — if a branch is unreachable,
   restructure so it does not exist (e.g. `Match.exhaustive`).
4. Integration tests actually exercised a live `nats-server` (phases 3+):
   spawn, connect, assert observable behavior, clean shutdown, no leaked
   processes (the test run must exit by itself).

## Dependency graph

```
P1 scaffolding
 └─ P2 pure modules (NatsError, NatsHeaders, NatsMessage)
     └─ P3 connection (NatsConnector, NodeConnector, NatsClient, TestNatsServer)
         └─ P4 publish + request
             └─ P5 subscribe + requestMany + status  (adds internal iterator→Stream adapter)
                 ├─ P6 JetStream publish + JetStreamError
                 │   ├─ P7 JsMessage + next/fetch ── P8 consume
                 │   ├─ P9 JetStreamManager          (parallel with P7/P8)
                 │   ├─ P10 NatsKv                   (parallel with P7–P9)
                 │   └─ P11 NatsObjectStore          (parallel with P7–P10)
                 └─ P12 NatsMicro                    (parallel with P6–P11)
                     └─ P13 polish + coverage + docs (after all)
```

Sequential spine: P1 → P2 → P3 → P4 → P5. After P5, P12 can start; after P6,
P7/P8 (sequential pair), P9, P10, P11 run **in parallel** — they touch
disjoint module files and each adds only its own barrel line (a one-line,
trivially-mergeable conflict).

## Branch & handoff protocol

- Branch per phase: `phase-NN-<slug>` off the latest merged spine.
- Touch only the files your phase owns, plus `index.ts` (your barrel line)
  and, for dep-adding phases, `packages/core/package.json` + lockfile.
- PR description: what shipped, the checklist output, any design-doc
  amendments, and anything the next phase must know that isn't in its plan.
- Commit style: small, imperative subjects; the pre-commit hook runs
  `vp staged` and must pass.
