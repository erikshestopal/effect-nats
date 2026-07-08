# Learnings

Read this file before continuing implementation work and before compacting or handing off context.

## Effect API style feedback

- Use TDD for each slice: write failing public behavior tests first, then implement the smallest code to pass.
- Test public behavior and public APIs only. Do not test implementation details or Effect/Schema internals.
- Prefer idioms from `repos/effect-v4/LLMS.md`, `repos/effect-v4/.patterns/*`, and nearby Effect source over inventing local helpers.
- For schema-backed domain models, use `Schema.Class` and put model behavior on the class when it naturally belongs there.
- Do not add redundant `TypeId` fields to simple `Schema.Class` models.
- Do not add wrapper constructors that only call `Class.make` unchanged.
- Avoid curried module helpers unless an Effect source pattern supports them for that API. For model-specific behavior, prefer instance methods/getters on the schema class.
- For collection APIs modeled after Effect modules, use `dual` for public data-first/data-last combinators instead of options-object workarounds.
- Do not use type assertions. Make values typecheck structurally or redesign the type boundary.
- `NatsHeaders` should model NATS header semantics, not Effect HTTP headers directly: NATS preserves header case and multi-values. Still follow `effect/unstable/http/Headers` API style where applicable.
- Run `bun run check` as the normal verification gate, not only focused tests/typecheck/ast-grep.

## Effect module idioms (v4)

Prefer Effect data modules over native operators and ad-hoc helpers:

- `Number.increment` / `Number.sum` instead of `n + 1` / `a + b` for counters.
- `String.isNonEmpty` / `String.includes` / `Option.liftPredicate` instead of `=== ""` or `indexOf` + `Option.isSome`.
- `Match.value` / `Match.when` / `Match.defined` for string unions and option-shaped strategy selection.
- `Iterable.isEmpty` for empty-collection checks; `Stream.runCount` instead of `runCollect` + `.length`.
- `Array.makeBy` / `Array.fromIterable` instead of `Array.from({ length })` / `Array.from(iterable)` in tests.
- `Effect.fnUntraced` for reusable effectful constructors (`make` / `open` / `create`). Keep `Effect.gen` for immediate-use inline bodies (do not call `Effect.fnUntraced(...)()` — tsgo `effectFnIife` flags it).
- `Option.match` / `Option.fromNullishOr` for nullish SDK handles instead of imperative `if (isUndefined)`.
- When adding Effect String helpers such as `Str.includes`, keep ast-grep `no-native-array-methods` exclusions for `Str`/`String` (both modules share method names like `includes`).
