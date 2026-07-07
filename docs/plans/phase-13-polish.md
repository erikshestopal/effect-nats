# Phase 13 — Polish: JSDoc audit, global coverage gate, docs

Blocked by: all previous phases merged. Final.

## Mission

Bring the whole package to Effect-publishable documentation standard, flip
the 100%-coverage gate into CI, and write the user-facing README. No new
features; API changes only if the audit finds a genuine design-contract
violation (amend `docs/DESIGN.html` in the same PR if so).

## Required reading

- `docs/plans/README.md`, `docs/DESIGN.html` (whole doc — you are auditing
  against it)
- `repos/effect-v4/packages/effect/src/unstable/socket/Socket.ts` and
  `repos/effect-v4/packages/ai/anthropic/src/AnthropicClient.ts` — the JSDoc
  standard you are matching

## Deliverables

1. **JSDoc audit** across every public export in `packages/core/src/*.ts`:
   `@since 0.1.0`, `@category` (use the v4 vocabulary: `type IDs`, `guards`,
   `models`, `constructors`, `services`, `layers`, `errors`, `combinators`,
   `destructors`), `@see` cross-links between `make`/`layer`/`layerConfig`,
   and an `@example` fenced ts block on each module's primary entry points
   (NatsClient.layer, subscribe, request, JetStream.publish, consume,
   processWith, NatsKv.layer, NatsObjectStore put/get, NatsMicro.layer).
   Examples must typecheck: wire the repo's doctest/example checking if the
   template supports it (check `jsdocs.config.json` in
   `repos/effect-v4` for how upstream does it; if nothing equivalent exists
   here, add `packages/core/test/examples.test.ts` that imports and
   compiles each example as code — pragmatic substitute, document it).
2. **Coverage gate**: verify global
   100% lines/statements/functions/branches over `packages/core/src/**`
   (including `internal/`). Close any gaps with tests (not with exclusions).
   Add `check:coverage` to `check:all`'s `dependsOn` in `vite.config.ts`.
3. **Consistency sweep**: error identifier prefixes uniform
   (`effect-nats/<Module>/<Tag>`); every module has `TypeId` + guard where
   it defines a data type; barrel alphabetized and complete; no `any` in
   public signatures (`typescript/no-explicit-any` is already an error —
   also audit for `as` casts hiding one); options bags all `?: T` spelling;
   duals used only on message-operating functions.
4. **README.md** (repo root, replace template content): what the library
   is, quickstart (connect + subscribe + request), one JetStream consume
   example, one NatsMicro example, transport matrix (node/bun TCP,
   node≥22/bun/browser WS), link to `docs/DESIGN.html`.
5. **AGENTS.md**: add a "packages/core conventions" section — the §6 error
   policy (instanceof only in internal mappers with suppressions), the
   optionality rule, the adapter reuse rule (all iterators go through
   `internal/iterator.ts`), TestNatsServer usage, coverage expectations.
6. **Design-doc reconciliation**: diff the shipped public surface against
   DESIGN §15's table; amend the doc where implementation legitimately
   diverged (each divergence listed in the PR description).

## Acceptance checklist

- [ ] `vp run --log labeled check:all` green **with** coverage in the gate.
- [ ] `bunx --bun vp test run --coverage` reports 100/100/100/100 on
      `packages/core/src/**`.
- [ ] Every public export documented (spot-check: `grep -L "@since"` over
      src returns nothing).
- [ ] README examples compile.
- [ ] DESIGN.html reconciled; amendments listed in PR.

## Out of scope

New features, new modules, version bumps, publishing.
