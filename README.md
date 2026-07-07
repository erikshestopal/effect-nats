# effect-nats

A two-package Effect TS monorepo using Bun, Vite+ (`vp`), TypeScript 7 RC, tsgo, oxlint, Vitest, and ast-grep rules copied from `effect-agent-v0`.

This template intentionally does not build or emit artifacts yet. Typechecking is done with `tsgo --noEmit`; package build/publish output can be added later when the TypeScript 7-compatible build path is chosen.

## Layout

```text
packages/
  core/
    src/
    test/
  feature/
    src/
    test/
```

All project code lives under `packages/`. The root owns shared tooling configuration.

## Commands

```sh
bun install
bun run check
bun run typecheck
sg scan packages
bunx --bun vp test run
```

`bun run check` runs linting, ast-grep, tests, and TypeScript 7 typechecking via `tsgo --noEmit`.

## TypeScript

- `typescript` is pinned to the latest TypeScript 7 RC.
- `@typescript/native-preview` provides `tsgo`.
- Use `bun run typecheck` for typechecking (`tsgo --noEmit`). Do not use `tsc` in this template.

## Packages

- Root package: `effect-nats`
- Workspace packages: `@effect-nats/core`, `@effect-nats/feature`
- TS path aliases live in `tsconfig.json`
