# effect-nats

Two-package Effect TS starter using Bun, Vite+ (`vp`), TypeScript 7 RC, tsgo, oxlint, Vitest, and ast-grep.

## Conventions

- Read `LEARNINGS.md` before continuing implementation work and before compacting or handing off context. Treat it as binding feedback about Effect API style and local review expectations.
- All source and test code lives under `packages/*/src` and `packages/*/test`.
- Use Effect APIs and data types first; avoid native JS helpers where ast-grep rules enforce Effect alternatives.
- Prefer one options object over multiple positional parameters for exported functions.
- Tests use `@effect/vitest`, `it.effect`, and `assert`.
- Typecheck with `bun run typecheck` (`tsgo --noEmit`); do not use `tsc`.
- This template intentionally has no build/emit path yet.

## packages/core conventions

- Error policy follows `docs/DESIGN.html` §6: map SDK errors to schema-backed tagged errors at module boundaries; use `instanceof` only in `packages/core/src/internal/*` mappers with the existing ast-grep suppression comments.
- Optional fields use `?: T` on option bags. Avoid `T | undefined` public signatures.
- All SDK `QueuedIterator` / async iterator adaptation goes through `packages/core/src/internal/iterator.ts`; do not hand-roll iterator loops in feature modules.
- Public data models should be `Schema.Class` when they are schema-backed values. Do not add redundant `TypeId` fields to simple schema classes.
- NATS headers intentionally preserve NATS semantics (case and multi-values); follow the Effect `Headers` dual API style without copying HTTP normalization semantics.
- Integration tests that need a broker should use `packages/core/test/utils/TestNatsServer.ts`. Use `TestNatsServer.layer` for core services and `TestNatsServer.layerJetStream` for JetStream/KV/ObjectStore.
- Coverage is expected to stay at 100% lines/statements/functions/branches for `packages/core/src/**`. Prefer public behavior tests for gaps; only use `v8 ignore` for unreachable defensive SDK branches with a specific comment.

## Commands

- `vp run --log labeled check:all` — lint + ast-grep + coverage tests + `tsgo --noEmit`.
- `bun run typecheck` — typecheck only with `tsgo --noEmit`.
- `sg scan packages` — ast-grep rules only.
- `bunx --bun vp test run` — tests only.
- `bunx vp test run --coverage` — coverage only (V8 coverage APIs are not available under `bunx --bun` in this workspace).

Pre-commit hook runs `vp staged` (`vp check --fix` on staged files).
