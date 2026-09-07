# Repository Guidelines

## Project Structure & Module Organization

TypeScript sources live in `src/`, split by responsibility: CLI entrypoints (`cli.ts`, `session-*.ts`), sync engines (`merge.ts`, `rsync*.ts`), filesystem
walkers (`scan.ts`, `hotwatch.ts`), and utility layers (hashing, ignore rules, database helpers). Integration and unit tests sit in `src/tests/` alongside
shared fixtures. Runtime bundles are emitted to `dist/`, while the published CLI wrapper is under `bin/reflect-sync.mjs`. Generated SQLite files such as
`alpha.db` and SEA artifacts stay out of version control; regenerate them locally when needed.

- Ignore any file ending in "tasks".  This is where devs keep their todo list.  Do not delete or change it.

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

## Git and Validation

- By default, agents should auto-commit completed change-sets after relevant validation passes.
- The default workflow is: make the change, run the relevant checks, commit, then let the user review and request follow-up fixes in a new commit if needed.
- Do not wait for an explicit "commit" request unless the user asked not to commit, the work is clearly exploratory/incomplete, or there are unrelated worktree changes that would make an automatic commit unsafe.
- Commit messages should be prefixed by area/package, e.g. `frontend/chat: ...`.
- By default, write commit messages with:
  - a concise first line (subject), and
  - a detailed markdown body explaining details of the commit, which is more succinct than the agent turn summary, including only information that is valuable longterm.
  - do not include a dedicated `Tests and validation` section; mention verification only when it adds long-term value.
  - do not embed literal escaped newlines (e.g. `\n` or `\\n`) in commit messages.
  - For multiline commit messages, always use stdin/heredoc or a message file instead of `git commit -m`.
  - In `exec_command` / shell tool calls, do not rely on quoted `\n` sequences to create commit-message line breaks; use literal newlines in the heredoc body.
  - Safe default pattern:

```
git commit -F - <<'EOF'
<subject line>

<body>
EOF
```

- `git commit -m` is only for subject-only commits with no body.
- Prefer follow-up commits over amending or rewriting history unless the user explicitly asks for that.
- For new source files that use the standard CoCalc file header comment, set the copyright year to the current year.
- Before finishing a change-set, run relevant typecheck/tests for touched packages.
- Run `pnpm -C src prettier --write <file>` on modified files as needed.
- For frontend changes, also run `pnpm -C src lint:frontend`. Treat frontend lint failures the same way as test or typecheck failures.
