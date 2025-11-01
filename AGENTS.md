# Repository Guidelines

## Project Structure & Module Organization

TypeScript sources live in `src/`, split by responsibility: CLI entrypoints (`cli.ts`, `session-*.ts`), sync engines (`merge.ts`, `rsync*.ts`), filesystem
walkers (`scan.ts`, `hotwatch.ts`), and utility layers (hashing, ignore rules, database helpers). Integration and unit tests sit in `src/tests/` alongside
shared fixtures. Runtime bundles are emitted to `dist/`, while the published CLI wrapper is under `bin/reflect-sync.mjs`. Generated SQLite files such as
`alpha.db` and SEA artifacts stay out of version control; regenerate them locally when needed.

## Build, Test, and Development Commands

`pnpm build` compiles the TypeScript sources via `tsc`. Run `pnpm test` to build and execute the Jest suite with `ts-jest`. During active development `pnpm
  test:watch` keeps Jest in watch mode. To ship a distributable CLI, use `pnpm bundle`, which runs Rollup for both the worker and main targets; `pnpm sea`
extends that flow with the self-extracting archive steps.

## Coding Style & Naming Conventions

Code targets Node 22+ using ESM modules. Follow the existing pattern of 2-space indentation, trailing commas, and descriptive camelCase identifiers
(`runScan`, `watchSymlinkLoop`). CLI commands and environment constants use screaming snake case (`CLI_NAME`, `MAX_WATCHERS`). Prefer small,
composable modules and keep side-effect imports at the top. When adding files, mirror the current naming scheme (`<feature>.ts` for modules,
`<feature>.<scenario>.test.ts` for tests). Run `pnpm build` before committing to catch TypeScript regressions.

## Testing Guidelines

Jest with `ts-jest` powers the suite. Place new tests in `src/tests/`, naming them `<topic>.<behavior>.test.ts`; reuse helpers from `src/tests/util.ts`.
Integration tests that touch SSH or the filesystem should guard slow paths with `describe.skip` toggles or environment checks. Aim to cover both sync
planner logic and CLI options; prefer deterministic fixtures over live infrastructure. Always execute `pnpm test` (or the targeted `jest <pattern>`
command) before opening a PR.

## Commit & Pull Request Guidelines

History favors concise, present-tense messages (`fix remote watcher reconnect`, `implement compression`). Keep the subject under ~60 characters and skip
trailing punctuation. For PRs, include a short problem statement, outline the solution, and link any tracked issues. Attach CLI transcripts or screenshots
when changing user-visible behavior, and call out test coverage adjustments. Request at least one review when touching scheduler or rsync pipelines.
