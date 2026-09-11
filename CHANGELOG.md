# Changelog

All notable changes to ReflectSync are recorded here. The project follows
semantic versioning while its public interfaces remain pre-1.0.

## [0.17.0] - Unreleased

- Move all sync commands under `reflect sync`; replace immediate `sync ID`
  with `sync flush ID`. No top-level sync aliases remain.
- Standardize stop/remove semantics; `remove --stop` explicitly permits
  stopping active work, and failed stops retain configuration and exit nonzero.
- Add forward start/stop/restart/status and consistent management JSON output.
- Separate Jupyter target/environment/discovery commands and add persistent
  local session IDs while retaining remote UUIDs. Add stopped-session removal.
- Support explicit first-use SSH trust and a versioned CoCalc tools baseline.

## [0.16.0] - 2026-09-02

### Added

- Reproducible Node 26 single-executable builds for Linux x64, Linux ARM64,
  and macOS ARM64, with native smoke tests and complete license bundles.
- Separately packaged upstream rsync 3.5 managed runtimes, corresponding
  source archives, capability manifests, and verified user-scoped resolution.
- `reflect doctor` human and JSON diagnostics for platform, SSH, filesystem,
  state database, and rsync compatibility.
- Required localhost SSH CI, package-consumer tests, test-inventory guards,
  CodeQL, dependency review, and Dependabot configuration.
- Draft-only release assembly with consolidated checksums, an SPDX SBOM,
  release metadata, and GitHub/Sigstore artifact attestations.

### Changed

- Updated the pnpm, TypeScript/ESM, lint, Rollup, and runtime dependency stack;
  replaced Jest/ts-jest with Vitest.
- Made binary installation the primary future distribution path while keeping
  the npm package as a tested secondary channel.
- Defined regular files, directories, and symlinks as the supported file-type
  contract; device nodes, FIFOs, and sockets are explicitly excluded.

### Security

- Pinned GitHub Actions by commit digest and reduced workflow permissions.
- Eliminated all currently reported production and development audit findings.
- Added digest verification and atomic installation for managed rsync runtimes.

[0.16.0]: https://github.com/sagemathinc/reflect-sync/releases/tag/v0.16.0
