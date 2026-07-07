# Phase 1 — Scaffolding

Blocked by: nothing. Blocks: everything.

## Mission

Turn the starter template into the `effect-nats` package skeleton: SDK
dependencies, coverage gating, a clean barrel, and a green `check:all` with
zero product code. You ship infrastructure, not features. (The package
rename to `effect-nats` and the tsconfig path cleanup are already done —
verify, don't redo.)

## Required reading

- `docs/plans/README.md` (all of it)
- `docs/DESIGN.html` §15 (module tree, dependency list)
- `vite.config.ts` (repo root) — the `run.tasks`, `test`, `lint` blocks
- `packages/core/package.json`, root `tsconfig.json`

## Deliverables

1. **Dependencies** in `packages/core/package.json` (regular deps; versions
   matching the vendored monorepo line, currently 3.x — check
   `repos/nats.js/package.json` workspace version and use the same
   major.minor from npm):
   `@nats-io/nats-core`, `@nats-io/transport-node`, `@nats-io/jetstream`,
   `@nats-io/kv`, `@nats-io/obj`, `@nats-io/services`.
   Root devDependency: `@vitest/coverage-v8` (match the vitest line vite-plus
   bundles; if version resolution fights you, document what you pinned and why).
2. **Coverage wiring**: `test.coverage` in `vite.config.ts` —
   provider `"v8"`, `include: ["packages/*/src/**"]`,
   `exclude` test/, plus `thresholds: { lines: 100, functions: 100, branches: 100, statements: 100 }`
   gated behind a flag or separate task so the spine phases (which each cover
   only their own files) can run per-file checks: add a `check:coverage` task
   to `run.tasks` running `bunx --bun vp test run --coverage`. Do **not** add
   it to `check:all`'s `dependsOn` yet — phase 13 flips that switch. Verify a
   coverage run produces a report; if `vp test run --coverage` does not pass
   the flag through, wire an equivalent script and document it in the PR.
3. **Clean slate**: delete `packages/core/src/Greeting.ts` and
   `packages/core/test/Greeting.test.ts`. Replace `src/index.ts` with an
   empty barrel carrying only the file-level JSDoc header (`@since 0.1.0`).
4. **Test fixture dir**: create `packages/core/test/utils/.gitkeep` (or a
   trivial shared helper) so the path exists.

## Acceptance checklist

- [ ] `bun install` clean; lockfile committed.
- [ ] `vp run --log labeled check:all` green.
- [ ] `bunx --bun vp test run --coverage` runs and produces a v8 report
      (empty coverage is fine at this phase; `passWithNoTests` is already on).
- [ ] `bun run typecheck` green.
- [ ] Importing `@nats-io/nats-core` from a scratch file typechecks (delete
      the scratch file before commit).

## Out of scope

Any module under `src/` beyond the empty barrel. Any test beyond keeping the
runner green.
