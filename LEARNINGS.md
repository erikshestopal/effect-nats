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
