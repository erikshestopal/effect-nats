# effect-nats

Two-package Effect TS starter using Bun, Vite+ (`vp`), TypeScript 7 RC, tsgo, oxlint, Vitest, and ast-grep.

## Conventions

- All source and test code lives under `packages/*/src` and `packages/*/test`.
- Use Effect APIs and data types first; avoid native JS helpers where ast-grep rules enforce Effect alternatives.
- Prefer one options object over multiple positional parameters for exported functions.
- Tests use `@effect/vitest`, `it.effect`, and `assert`.
- Typecheck with `bun run typecheck` (`tsgo --noEmit`); do not use `tsc`.
- This template intentionally has no build/emit path yet.

## Commands

- `vp run --log labeled check:all` — lint + ast-grep + tests + `tsgo --noEmit`.
- `bun run typecheck` — typecheck only with `tsgo --noEmit`.
- `sg scan packages` — ast-grep rules only.
- `bunx --bun vp test run` — tests only.

Pre-commit hook runs `vp staged` (`vp check --fix` on staged files).
