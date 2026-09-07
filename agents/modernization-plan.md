# ReflectSync modernization and release plan

Date: 2026-09-01

Repository baseline: `ea2dffb` (`master`)

Original scope: planning only. The implementation checkpoint below now records
the repository work completed from this plan; external repository settings and
credentials remain owner-controlled.

## Implementation checkpoint — PRs 1–8

Status on 2026-09-02: implemented in the current review worktree, with no
commits or external repository-setting changes made.

1. **Toolchain baseline complete:** pnpm 11.25.0 is pinned; the dependency and
   lockfile graph is current and audit-clean; TypeScript uses NodeNext; build,
   clean, and publish scripts no longer reinstall dependencies or rely on POSIX
   deletion commands. TypeScript remains on the latest 5.9 release until the
   typescript-eslint support range includes TypeScript 7.
2. **Honest tests complete:** Vitest replaces Jest/ts-jest; focused and silent
   skip patterns are policy failures; future-capability cases are explicit; the
   inventory guard records 49 files and 138 tests; and the packed npm consumer
   test covers both CLI aliases, ESM imports, declarations, and package contents.
3. **SSH CI complete:** CI configures an ephemeral localhost sshd and requires
   the SSH suite instead of silently accepting an unavailable daemon.
4. **Repository security files complete:** Actions are commit-SHA pinned with
   least-privilege permissions; the Node matrix covers 22.12.0, 24, and 26.7.0;
   CodeQL, dependency review, Dependabot, scheduled audits, and policy checks
   are present. Admin-only GitHub settings remain in the owner-action list.
5. **Runtime preflight complete:** `reflect doctor` and `doctor --json` report
   the executable/runtime, OS/libc, SSH, filesystem state, selected rsync, and
   its probed feature compatibility locally or remotely.
6. **Managed rsync runtime complete:** upstream rsync 3.5.0 is source-digest
   pinned as runtime `3.5.0-reflect.1`; Linux x64/ARM static-musl and macOS ARM
   native builders run the upstream suite, emit capability/build manifests,
   package GPL notices and exact corresponding source, verify checksums and
   installed archives, and resolve explicit override → managed → compatible
   system runtime. ReflectSync explicitly supports regular files, directories,
   and symlinks—not device nodes, FIFOs, or sockets.
7. **Node 26 SEA builder complete:** Node 26.7.0's built-in `--build-sea` path
   produces deterministic Linux x64/ARM and macOS ARM executables and canonical
   archives with Node/project/dependency licenses, build identity, checksums,
   functional SQLite scan smoke tests, and native-archive install tests. macOS
   internal artifacts receive a valid ad-hoc signature after SEA injection so
   they can be tested; Developer ID signing/notarization remains PR 9.
8. **Release rehearsal complete:** version `0.16.0` has a changelog and an exact
   tag/version/commit gate; one workflow runs release-quality checks and native
   builders, then assembles rather than rebuilds their tested outputs. The
   deterministic finalizer verifies native sidecars, emits one `SHA256SUMS`, a
   detailed release manifest, license notices, and an SPDX 2.3 SBOM, and rejects
   missing, duplicate, dirty, wrongly targeted, or tampered inputs. Explicit
   tag/manual runs can use GitHub's current OIDC `actions/attest` path for SLSA
   provenance and SPDX attestations. The only write-capable job can create or
   repair and re-download a draft prerelease; it contains no publish operation
   and refuses to alter an existing non-draft release.

Native validation passed on this Linux x64 workspace, `bench-arm`, and `m1`.
The full ReflectSync inventory passes with both system and managed rsync; native
ARM and macOS SEA/runtime archives launch after extraction; repeated x64 and
macOS builds/packages are byte-reproducible. GitHub workflow syntax was checked
with actionlint. A real three-platform `0.16.0` candidate passed the local
release finalizer and the generated SPDX document passed the official SPDX 2.3
JSON schema. The next engineering slice is PR 9 (Developer ID signing and Apple
notarization); the working SageJS setup provides a known-good credential model.
The purchased `reflect-sync.dev` domain remains reserved for the installer/site
work in PRs 11 and 14.

## Executive recommendation

Treat ReflectSync as a greenfield 0.x product with a strong sync engine but no compatibility burden. Make the binary release the primary product, keep npm as a supported secondary installation/library channel, and make both the SSH-side agent and a pinned upstream rsync build managed runtime dependencies that ReflectSync installs into versioned per-user caches.

The work should happen in this order:

1. Make the dependency, test, and CI baseline trustworthy.
2. Produce immutable, verified Node 26 binaries on native GitHub-hosted runners.
3. Build portable, independently versioned upstream rsync runtime artifacts for each supported target.
4. Add Apple Developer ID signing/notarization and npm OIDC trusted publishing.
5. Build checksum-verifying installers on top of those release assets.
6. Add protocol negotiation and an automatic remote bootstrap that never assumes the remote has outbound internet.
7. Replace the current landing page with an honest, task-oriented site.
8. Support Windows via WSL first, then gate any native-Windows claim on a transport and filesystem conformance suite.
9. Before calling the project stable, add model-based safety tests, crash/network fault tests, and repeatable performance benchmarks.

The desired first-run experience is:

```sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://reflect-sync.dev/install.sh | sh

reflect doctor
reflect create ~/work user@server:/srv/work
```

On the last command, ReflectSync should discover the remote platform, install the exact compatible remote agent and rsync runtime without root access when needed, verify them, and continue. The controller downloads and verifies the artifacts locally, then transfers them over the already-authenticated SSH connection, so a deliberately isolated remote sandbox needs no DNS, HTTPS, package manager, Node, or other outbound network access. A missing or incompatible remote binary should never produce an endless retry loop.

## Principles and non-negotiable gates

- Data safety outranks feature velocity. A bidirectional sync bug can destroy the only good copy of data.
- A platform is “supported” only when its release artifact and conformance suite pass in CI and on a clean validation host.
- Build once, test that exact output, then publish that exact output. Publishing must not rebuild it.
- Release assets are immutable, checksummed, provenance-attested, and signed where the operating system has a native signing system.
- Remote bootstrap is user-scoped, version-pinned, atomic, observable, and never uses `sudo`.
- Remote outbound network access is never a prerequisite. The normal bootstrap path fetches and verifies artifacts on the controller and transfers them over SSH; direct remote HTTPS download is only an optional optimization.
- Upstream rsync remains a separately executed and replaceable program. Its runtime release is independently versioned, capability-tested, and distributed with complete GPL notices, corresponding source, patches, and build recipes.
- “Latest dependencies” means migrating to current supported versions, committing the lockfile, and pinning release inputs—not floating unreviewed versions during a release.
- Keep an explicit no-telemetry default. Adoption feedback can come from issues/discussions until there is a compelling opt-in use case.
- Do not preserve accidental APIs or packaging behavior merely because they exist in 0.x.

## Evidence-backed baseline

### Repository and package

- The package is `reflect-sync@0.15.2`; npm’s `latest` tag is also `0.15.2`.
- The npm tarball is populated and contains the compiled library, bundled CLI, declarations, source maps, README, and license. Local `npm pack --dry-run --ignore-scripts` only contained four files when `dist/` was absent, which shows why a clean staged-package test is essential.
- The only GitHub release is the `0.14.1` prerelease with one Linux x86_64 asset. There is no current binary release workflow.
- The two workflows are Linux/Node 22 CI and GitHub Pages deployment. There is no npm workflow, signing workflow, artifact manifest, checksum file, SBOM, or provenance attestation.
- The npm package has registry signatures but no published npm provenance attestation.
- The public JS export surface exposes many database and scheduler internals. There are no package-consumer tests proving ESM/types behavior, and the README’s library claim is not backed by API stability documentation.

### Dependency and toolchain state

- `pnpm audit --prod` reports zero production vulnerabilities.
- Full `pnpm audit` reports 1 critical, 22 high, 10 moderate, and 3 low findings, all in the development/build graph. Build-only vulnerabilities still matter because those packages execute in CI and release jobs.
- The `pnpm.overrides` block in `package.json` is ignored by pnpm 10. Overrides now belong in `pnpm-workspace.yaml`; the current warning appears on every pnpm command.
- The lockfile pins pnpm 10.19.0-era tooling while current major releases exist for TypeScript, Jest, ESLint, Commander, Chokidar, and several build plugins. This needs migration and testing, not a blind force-upgrade.
- After `pnpm install --frozen-lockfile`, the pinned toolchain builds and lints on Node 26.7.0. A global TypeScript 7 compiler rejects the current `moduleResolution: "node"`, so the planned TypeScript upgrade must move to modern Node module resolution.
- `clean` deletes `node_modules`, `prepublishOnly` cleans/reinstalls/rebuilds, and several lifecycle scripts use POSIX shell commands. That is slow, makes the published artifact differ from the previously tested artifact, and cannot be the Windows-compatible release path.

### Tests and correctness signals

- The current pinned suite on Linux x86_64/Node 26 passed 38 of 47 suites and 105 of 129 tests; 9 suites and 24 tests were skipped. Lint passed.
- CI does not configure localhost SSH, so the real SSH suites are skipped. One SSH test returns early and appears as a pass when sshd is unavailable.
- `src/tests/integration/basic-ssh.session.test.ts` contains an accidental `it.only`; on an SSH-enabled run it suppresses the other tests in that file.
- Several capability tests are intentionally skipped, including hard links, xattrs, sparse files, a directory-mode conflict, and case-only rename behavior.
- The documentation makes stronger guarantees than some implementations: for example, filesystem capability probe failure currently logs and assumes a case-sensitive/non-normalizing filesystem rather than failing fast.
- There is no property/model-based convergence test, fault-injection matrix, compatibility suite for rsync features, coverage gate, or benchmark regression harness.

### Packaging and SEA state

- The existing Node SEA path works locally on Node 26: it produced a 142 MB Linux executable (30 MB xz) and the binary passed `--version`/`--help` smoke tests.
- It uses the legacy blob-plus-`postject` path and executes an unpinned `pnpx postject` download during the build. Node 26 has a built-in `node --build-sea` path, so `postject` is no longer needed.
- The SEA inherits Node’s native runtime requirements. Current official Node Linux binaries require glibc >= 2.28 and, starting with Node 25, a system `libatomic` runtime. “Single executable” therefore does not mean “runs on every Linux distribution.”
- SEA archives currently have no fixed naming contract, manifest, checksums, provenance, license bundle, minimum-platform declaration, or install/uninstall contract.
- Deno cross-compile scripts duplicate the intended Node-based release path and should be removed after the Node 26 builder is proven.

### SSH and remote-agent UX

- Remote startup searches for `reflect-sync` in the remote PATH, then requires exact package-version equality.
- A missing remote command is not converted into a single actionable installation state; downstream commands run with an empty command and the scheduler retries/fails noisily.
- There is no independent protocol version or JSON capability handshake.
- Remote commands are assembled in several places as shell strings. Root paths and plumbing arguments need one audited quoting/encoding layer before automatic installation expands this trust boundary.
- Remote state paths are hard-coded under `~/.local/share` in session creation even though local code otherwise understands XDG/macOS/Windows data directories.
- The current SSH flow supports only one remote side, which is acceptable for the first modern release but must be documented as a constraint.

### Target-host findings

| Host              | Observed environment                                                                                                       | Packaging implication                                                                                                               |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Current workspace | Linux x86_64, Node 26.7, upstream rsync 3.4.1                                                                              | Existing SEA builds and runs; not a minimum-runtime test host.                                                                      |
| `m1`              | macOS 26.4 arm64, no Node in noninteractive PATH, Apple `codesign`/`notarytool`, `/usr/bin/rsync` is openrsync protocol 29 | Exactly the use case for an SEA plus managed upstream rsync runtime. Apple’s rsync lacks multiple flags ReflectSync currently uses. |
| `bench-arm`       | Linux aarch64, glibc 2.39, upstream rsync 3.2.7, no Node in PATH                                                           | Good post-build ARM and remote-bootstrap validation host.                                                                           |
| `windows`         | Windows Server 2022 x64, Node 26.5.1, OpenSSH 9.5, no rsync                                                                | A Windows SEA alone does not make sync work. WSL or a deliberate native transport/rsync strategy is required.                       |
| `bench-1`         | SSH public-key authentication failed from this environment                                                                 | Restore access before using it as the independent Linux x64 validation host.                                                        |

Long-lived machines should validate releases, not build them. GitHub-hosted runners should remain the reproducible release builders.

### Website and repository posture

- The Pages site is a polished single landing page but only offers npm/pnpm installation. It has no binary installer, platform matrix, full quick start, troubleshooting flow, security verification instructions, or remote-bootstrap explanation.
- README content is duplicated and contains stale or absolute claims. The site also claims production usage that should be verified or removed.
- GitHub’s default workflow token is writable, actions are not required to be SHA-pinned, `master` is unprotected, no rulesets exist, and Dependabot security updates, secret scanning, and code scanning are disabled.
- No Apple signing, notarization, npm, or release environment secrets/variables are configured.

## Product and support contract

Adopt explicit tiers before changing code:

| Platform                     | Initial status                           | Required runtime dependencies                           | Notes                                                                                                                              |
| ---------------------------- | ---------------------------------------- | ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| Linux glibc x86_64           | Supported                                | OpenSSH client; glibc/libatomic compatible with Node 26 | Required release target. The installer supplies the pinned rsync runtime without modifying system packages.                        |
| Linux glibc arm64            | Supported                                | Same as Linux x86_64                                    | Required release target. The installer supplies the pinned rsync runtime.                                                          |
| macOS arm64 >= 13.5          | Supported                                | OpenSSH                                                 | Required, Developer ID-signed and notarized. The installer supplies signed upstream rsync because Apple openrsync is insufficient. |
| macOS x86_64                 | Candidate                                | Same as macOS arm64                                     | Cheap to add via GitHub Intel runner, but secondary to the requested ARM target.                                                   |
| Windows through WSL2         | Supported after WSL conformance passes   | WSL2 distro and OpenSSH                                 | Fastest honest Windows support and preserves POSIX semantics; the Linux installer supplies managed rsync.                          |
| Native Windows x64           | Experimental until all native gates pass | To be decided by transport spike                        | A signed `.exe` may be published for `help`/`doctor` testing without claiming sync support.                                        |
| Alpine/musl, 32-bit, FreeBSD | Unsupported initially                    | —                                                       | Fail clearly in the installer; do not silently select a glibc binary.                                                              |

Determine the actual rsync minimum by probing required features, not only parsing `rsync --version`. Today the code needs capabilities such as NUL-delimited file lists, delete-missing behavior, progress output, transfer logging, and modern compression negotiation.

Keep the CLI as the primary compatibility contract. Mark the JS API experimental for the remainder of 0.x, add a package-consumer test, and narrow it to intentional high-level operations before 1.0 instead of stabilizing the current internal surface.

Node 26 is still Current on the date of this plan and is scheduled to enter LTS on 2026-10-28. Build the requested Node 26 artifacts now, but keep releases prerelease/beta until the runtime and product gates pass; use Node 26 LTS for a stable/1.0 claim.

## Definition of done

The modernization program is complete when all of these are true:

- A frozen clean install, build, lint, format check, full audit, test suite, npm pack test, and SEA smoke test pass in CI.
- Full and production pnpm audits both report zero known vulnerabilities; any temporary override has an owner and removal condition.
- No focused tests exist; conditional suites report an explicit skip and required release suites cannot silently skip.
- A `vX.Y.Z` tag builds Linux x64, Linux arm64, and macOS arm64 artifacts using a pinned Node 26 patch release on native GitHub-hosted runners.
- The macOS binary has a valid Developer ID Application signature, hardened runtime, secure timestamp, and an accepted Apple notarization record.
- Every release has SHA-256 checksums, a machine-readable manifest, license notices, GitHub build provenance, smoke-test evidence, and immutable assets.
- Every supported target has a separately downloadable, reproducibly built rsync runtime plus its corresponding source, build recipe, digest, license, capability manifest, and native signature where applicable.
- The npm package is built once, installed/tested from its tarball, and published from GitHub Actions with npm trusted publishing/OIDC and automatic provenance—no npm token or OTP in the workflow.
- The POSIX installer is idempotent, non-root by default, checksum-verifying, version-pinnable, and tested on clean Linux/macOS hosts.
- `reflect doctor` reports local and remote dependency/capability failures once with exact remediation.
- A release build can create a session against a clean supported SSH target with no Node, ReflectSync, compatible rsync, DNS, or outbound network access; the controller transfers exact compatible agent and rsync artifacts over SSH, installs them atomically, and reuses them.
- The website accurately documents install, first sync, security model, supported platforms, limitations, uninstall/update, and troubleshooting.
- Windows is described as WSL-supported until native path, metadata, process, daemon, SSH, and transfer conformance all pass.
- The safety suite proves convergence/no-unilateral-data-loss invariants across representative operations and injected failures.

## Phase 0 — Make the baseline trustworthy

### 0.1 Dependency and toolchain migration

1. Move every pnpm override into `pnpm-workspace.yaml`, update the lockfile, and remove the ignored `package.json#pnpm.overrides` block.
2. Upgrade pnpm to the current supported major and pin it in `packageManager`. Use the same version locally and in Actions.
3. Update direct dependencies and build tools in small coherent groups:
   - runtime/CLI dependencies;
   - TypeScript and Node types;
   - ESLint/typescript-eslint/import rules;
   - Rollup and plugins;
   - test runner and coverage stack.
4. Move TypeScript to `NodeNext`/modern Node resolution as part of the TypeScript 7 migration. Do not suppress the deprecation.
5. If current Jest/ts-jest cannot produce a clean, supported ESM toolchain, migrate to Vitest rather than accumulating permanent transitive overrides. Preserve behavior and test counts during the migration.
6. Prefer direct-parent upgrades over overrides. Temporary overrides must use patched versions, be centralized, and have comments explaining their removal condition.
7. Split cleaning into generated outputs versus a deliberate full dependency clean. Do not delete `node_modules` in normal build or publish lifecycle scripts.
8. Replace POSIX-only package scripts with Node scripts or cross-platform tools where those scripts are expected to run on Windows.
9. Add `format:check`, `typecheck`, `test:unit`, `test:integration`, `test:ssh`, `test:package`, and `test:sea` commands with non-overlapping responsibilities.

Acceptance gate:

- `pnpm install --frozen-lockfile`, `pnpm build`, `pnpm lint`, `pnpm format:check`, `pnpm test`, `pnpm audit`, and `pnpm audit --prod` all succeed from a clean clone.
- No ignored override warning remains.
- Dependency changes do not alter sync results in the existing test corpus.

### 0.2 Make test results honest

1. Remove the accidental `it.only` and add an ESLint `no-focused-tests` rule plus a cheap repository check for `.only`.
2. Replace “return and pass” environmental skips with framework-level skips carrying a visible reason.
3. Move unimplemented future specifications out of the normal green test count or mark them in a separately reported future-capabilities project.
4. Configure an ephemeral localhost sshd in Linux CI and make the SSH suite required. Install the just-built CLI at the remote test path rather than relying on a developer machine.
5. Fix/quarantine the control-master resilience test only with a linked issue and a deterministic reproduction; release CI must not hide a known flaky destructive-path test indefinitely.
6. Record expected suite/test counts so accidental mass-skipping fails CI.
7. Add package-consumer fixtures that install the tarball and exercise the CLI, ESM import, type declarations, and documented API.

### 0.3 CI and repository hardening

1. Upgrade official Actions to current majors and pin every action to a reviewed full commit SHA, with a version comment.
2. Set workflow permissions to `contents: read` by default; grant write/OIDC/attestation scopes only to the exact release jobs that need them.
3. Test the minimum supported Node, current LTS Node, and Node 26. Run the full Linux suite once and smaller build/smoke matrices on the other Node versions.
4. Add native platform smoke jobs for Linux arm64 and macOS arm64. Add Windows build/tests as native abstractions land.
5. Add dependency review for pull requests, CodeQL for JavaScript/TypeScript, and scheduled full-audit runs.
6. Enable Dependabot security updates and grouped weekly version updates for npm and GitHub Actions.
7. Enable secret scanning/push protection where the organization plan permits it.
8. Protect `master` with required CI checks, no force pushes/deletions, and at least one review for release/scheduler/rsync changes. Use a repository ruleset rather than relying on convention.
9. Change the repository default Actions token to read-only and disable Actions from approving pull requests unless a specific workflow requires it.

## Phase 1 — Build trustworthy Node 26 binaries

### 1.1 One canonical SEA builder

1. Pin an exact Node 26 patch in a checked-in version file. Update it through reviewed dependency PRs; record the exact Node version in every release manifest.
2. Replace blob generation plus `pnpx postject` with Node 26’s built-in `node --build-sea sea.config.json` flow.
3. Build on each target architecture. Do not cross-generate V8 code cache or claim an architecture that was not natively smoke-tested.
4. Put npm staging output and release output in separate directories so a prior SEA can never leak into `npm pack` through `dist/**`.
5. Make the bundler deterministic, fail on warnings that indicate incomplete bundling, and remove the `remote.ts`/`ssh-control.ts` circular dependency.
6. Remove Deno release scripts after the Node path covers the matrix.
7. Include ReflectSync’s license, Node’s license/notices, bundled dependency notices, and a concise `README.txt` in each archive.
8. Use archive formats available on clean hosts: `.tar.gz` for Linux, a notarization-compatible `.zip` for macOS, and `.zip` for Windows. Do not require `xz` merely to install.

Canonical assets inside each release should use stable names so GitHub’s `releases/latest/download/...` URL works:

```text
reflect-sync-linux-x64.tar.gz
reflect-sync-linux-arm64.tar.gz
reflect-sync-darwin-arm64.zip
reflect-sync-windows-x64.zip          # experimental until native support gates pass
reflect-sync-rsync-<runtime>-linux-x64.tar.gz
reflect-sync-rsync-<runtime>-linux-arm64.tar.gz
reflect-sync-rsync-<runtime>-darwin-arm64.zip
reflect-sync-rsync-<runtime>-windows-x64.zip  # experimental; includes its runtime DLL closure
reflect-sync-rsync-<runtime>-sources.tar.gz
SHA256SUMS
release-manifest.json
THIRD_PARTY_LICENSES.txt
```

The manifest should include package version, git commit, protocol version, Node version, rsync runtime/build version and feature set, target, minimum OS/libc assumptions, artifact digest, build workflow/run, and support tier. ReflectSync and rsync remain separate downloadable artifacts even when the installer manages both.

### 1.2 Native build and smoke matrix

| Target      | GitHub runner                    | Required release checks                                                                                               |
| ----------- | -------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Linux x64   | `ubuntu-24.04`                   | `file`, launch, version, help, doctor, local sync smoke, archive install smoke, glibc/libatomic dependency inspection |
| Linux arm64 | `ubuntu-24.04-arm`               | Same checks natively; post-release run on `bench-arm`                                                                 |
| macOS arm64 | `macos-15`                       | Same functional checks, then signing/notarization checks; post-release run on macOS 26 `m1`                           |
| macOS x64   | `macos-15-intel`                 | Candidate after required matrix is stable                                                                             |
| Windows x64 | `windows-2025` or `windows-2022` | Build/help/doctor first; sync checks only after Phase 5 gates                                                         |

Additionally run Linux artifacts in a minimum supported glibc container and explicitly check for the Node 26 `libatomic` requirement. A friendly installer/doctor error is required where the runtime cannot launch.

### 1.3 Build a managed upstream rsync runtime

Do not reimplement rsync and do not embed it in the Node SEA. Publish it as a small, independently versioned runtime artifact that the installer and SSH controller can fetch, verify, cache, replace, and diagnose separately.

1. Pin an upstream rsync release and a ReflectSync packaging revision, for example `<upstream>-reflect.<revision>`. Record the exact source digest, configure flags, compiler, SDK/minimum OS, dependency versions, and patches.
2. Build natively for Linux x64, Linux arm64, and macOS arm64 in GitHub Actions. Use rsync's included popt where appropriate and make each optional dependency (zlib/zstd/LZ4/xxHash/OpenSSL) a deliberate portability/feature decision rather than inheriting a Homebrew or distro dependency graph.
3. Prefer a hermetic executable with no non-system shared-library dependencies. Inspect the result with `ldd`/`readelf` on Linux and `otool -L` on macOS, then test it on the minimum supported OS—not only the build runner.
4. Run upstream rsync tests plus a ReflectSync capability/conformance suite covering every option and output format the planner depends on. Publish the resulting capability set in the runtime manifest; do not infer compatibility from the version string alone.
5. Package the executable, manifest, license/copyright notices, and build identity in a dedicated archive. Publish a corresponding-source archive containing the exact upstream source, vendored dependency sources, patches, and reproducible build scripts needed to satisfy the GPL distribution obligations.
6. Install into a versioned private directory such as `${XDG_DATA_HOME:-$HOME/.local/share}/reflect-sync/runtimes/rsync/<runtime>/<target>/bin/rsync`; use the native user data location on macOS/Windows. Never overwrite `/usr/bin`, Homebrew, or another system installation.
7. Resolve rsync in this order: explicit `--rsync-path`/configuration override, the compatible managed runtime, then a capability-compatible system rsync fallback. Report the selected path, version, build identity, and capabilities in `doctor` and the agent handshake.
8. Keep runtime releases independently replaceable so an rsync security update does not require a protocol change. A ReflectSync release manifest declares the allowed runtime build(s), and bootstrap refuses an unapproved downgrade.
9. On macOS, sign the rsync Mach-O before archiving it, notarize its distribution archive as required, and verify the signature again after installation. For a future native Windows bundle, include and Authenticode-sign the complete required DLL/runtime closure—not only `rsync.exe`.
10. Test cache concurrency, corrupt archives, wrong target selection, interrupted upgrade, rollback to the last known-good runtime, explicit system override, and cleanup of versions no longer referenced by any installed agent.

Acceptance gate:

- A clean supported host with no usable system rsync can install and run the managed runtime without root or a package manager.
- The release archive has no undeclared shared-library dependency, passes capability/conformance tests, and is byte-for-byte associated with its published digest, provenance, signature, and source archive.
- A clean remote sandbox with outbound networking disabled receives the same verified artifact over SSH and completes an end-to-end sync.

### 1.4 Proper macOS signing and notarization

GitHub cannot substitute for Apple trust. The GitHub-hosted macOS job must use credentials from an Apple Developer account:

1. Create a Developer ID Application certificate, export it as password-protected PKCS#12, and store the base64 certificate plus password as protected GitHub environment secrets.
2. Prefer an App Store Connect/Notary API key (`.p8`, key ID, issuer ID, team ID) over an Apple ID password for `notarytool`.
3. Import the certificate into an ephemeral CI keychain and delete the keychain in an `always()` cleanup step.
4. Generate the final SEA first, then sign the Mach-O with Developer ID, hardened runtime (`--options runtime`), and a secure timestamp. Separately sign the managed rsync Mach-O before its archive is assembled. Any mutation after signing invalidates the signature.
5. Verify with `codesign --verify --strict --verbose=2` and inspect the authority/timestamp.
6. Archive with Apple’s `ditto`, submit that exact archive using `xcrun notarytool submit --wait`, require an `Accepted` result, and retain the notarization log as a workflow artifact.
7. Test Gatekeeper assessment on a clean host. Apple can notarize a ZIP containing a standalone CLI, but cannot staple a ticket to a ZIP or standalone binary. If offline stapling is a requirement, additionally ship a signed/notarized/stapled `.pkg`; otherwise document that Gatekeeper retrieves the standalone binary’s ticket online.

Release must fail, rather than emit an “unsigned stable” macOS asset, if protected signing credentials are missing.

### 1.5 Release integrity and orchestration

Create one tag-driven `.github/workflows/release.yml`:

1. Trigger on `v*` tags and optional manual prerelease dispatch. Use `concurrency` without `cancel-in-progress`.
2. Validate that the tag, `package.json` version, changelog entry, and generated protocol metadata agree.
3. Run the release-quality test suite before any publish operation.
4. Build each native ReflectSync and rsync runtime artifact, smoke/conformance-test it, and upload it only as an internal workflow artifact.
5. In a finalizer job, download all outputs, generate `SHA256SUMS`, the manifest, license notices, corresponding-source bundle, and an SPDX or CycloneDX SBOM.
6. Generate GitHub/Sigstore provenance for every ReflectSync and rsync artifact using the current official `actions/attest` action with `id-token: write` and `attestations: write` only in that job. Treat hashes as integrity identifiers, not an independent signature when the artifact and checksum arrive through the same channel; native platform signatures and provenance provide the additional authentication.
7. Create a draft GitHub release, attach every asset, verify download/digests from the draft, then publish it. Configure immutable releases after the workflow has completed a successful rehearsal.
8. Make reruns idempotent: an existing draft can be repaired; an already-published immutable version must be verified and left untouched.

After the npm job from Phase 2 is added, use this irreversible-operation order: build/test everything, assemble and verify the draft GitHub release, publish the exact tested npm tarball through OIDC, publish the GitHub release, then verify both channels.

A rerun must recognize an npm version that already exists, compare its digest/provenance with the staged tarball, and continue without attempting to overwrite it.

Use a protected `release` GitHub Environment with a human approval gate. Pull-request workflows must never receive Apple/npm release authority.

## Phase 2 — Publish npm without recurring 2FA codes

### 2.1 Make the npm artifact deliberate

1. Decide and document the supported JS API. Keep only intentional exports and add an `exports` map with tested ESM/types behavior; add CommonJS only if it is actually built and tested.
2. Correct metadata: site homepage, bugs URL, license filename, funding if desired, supported engines, and `publishConfig.access = "public"`.
3. Stage npm output in a clean directory with an explicit allowlist. Exclude SEA binaries, blobs, internal logs, tests, and unrelated source maps unless source maps are intentionally supported.
4. Build the `.tgz` once, inspect its file list/size, install that exact tarball in a clean temporary project, run both CLI names, import the public API, and typecheck a consumer.
5. Remove the clean/reinstall/rebuild behavior from `prepublishOnly`. Publish the exact tested tarball with `npm publish ./reflect-sync-X.Y.Z.tgz`.

### 2.2 Configure npm trusted publishing

One-time npmjs.com setup by the package owner:

- Add a GitHub Actions trusted publisher for `sagemathinc/reflect-sync`.
- Set the exact workflow filename to `release.yml`.
- If the npm form specifies an environment, use the exact protected environment name chosen for the npm job (recommended: `npm`).
- Allow `npm publish` only unless staged publishing is deliberately adopted later.

Workflow requirements:

- Run the publish job on a GitHub-hosted Ubuntu runner.
- Use Node/npm versions satisfying npm trusted publishing (npm >= 11.5.1 and Node >= 22.14); the pinned Node 26 release does.
- Grant `id-token: write` and `contents: read`; do not set `NODE_AUTH_TOKEN` and do not store an npm write token.
- Let npm trusted publishing generate provenance automatically, then verify the published registry version, tarball digest, provenance link, CLI smoke test, and repository metadata.
- After the first successful OIDC publish, revoke old automation tokens and restrict token-based publication while retaining account-level 2FA for human security.

This removes OTP prompts from normal releases without weakening the npm account.

## Phase 3 — Installer, doctor, and automatic SSH bootstrap

### 3.1 Secure public installers

Add a POSIX `install.sh` and, when the Windows artifact is useful, `install.ps1`.

The POSIX installer must:

- run under portable `sh` on supported Linux/macOS systems;
- map `uname -s`/`uname -m` to only supported release targets and reject musl/unknown platforms clearly;
- default to the latest stable release but accept an exact `--version` and alternate `--install-dir`;
- download from a fixed HTTPS GitHub release URL using `curl`, with a `wget` fallback where practical;
- download `SHA256SUMS`, select the exact expected line, and verify before extraction;
- select the compatible managed rsync runtime from the authenticated, provenance-attested release manifest, download it as a separate asset, and verify its digest before extraction;
- install ReflectSync and its rsync runtime atomically via private temporary directories, mode `0755`, and rename;
- default to `~/.local/bin`, create the `reflect` alias safely, never require `sudo`, and print a precise PATH instruction;
- be idempotent, preserve a working old binary until verification passes, support `--force`/`--dry-run`, and document uninstall;
- run `reflect --version` and `reflect doctor --install-check` after installation;
- clearly disclose that it installs ReflectSync's managed upstream rsync runtime, its version, location, license, and uninstall path;
- never silently install Homebrew, system rsync, OpenSSH, WSL, or system libraries. It may offer exact opt-in commands.

Test the literal website one-liner in clean Linux and macOS environments on every release. Also publish a safer two-step “download, inspect, run” variant.

### 3.2 Add `reflect doctor`

`doctor` becomes the single preflight and support-report command. It should report human-readable and `--json` output for:

- ReflectSync/build/protocol/Node SEA version and support tier;
- OS, architecture, libc and `libatomic` where applicable;
- `ssh` availability and relevant options;
- local rsync path, whether it is managed/system/overridden, implementation, version, runtime build identity, digest, signature status where applicable, and each required feature flag;
- remote reachability, shell, platform/architecture, free space, writable agent/data directories, rsync path/capabilities, and installed agent handshake;
- filesystem case/Unicode behavior and symlink/permission limitations;
- state DB integrity/schema version and daemon status;
- a redacted diagnostic bundle suitable for an issue.

On macOS, explicitly detect Apple openrsync and explain why it is insufficient, then install/use the signed managed runtime without changing Homebrew or the system. An explicit compatible Homebrew/system override remains supported for administrators.

### 3.3 Version a real remote-agent protocol

1. Introduce a protocol version independent of npm semver.
2. Add a narrow hidden command such as `reflect agent probe --json` returning package version, protocol range, OS/arch, feature flags, rsync path/runtime build/digest/capabilities, and data/cache paths.
3. Permit patch/minor version differences when protocol ranges and required features overlap. Stop requiring exact package-version strings as the sole compatibility test.
4. Centralize SSH invocation, POSIX argument quoting, timeouts, cancellation, logging redaction, and error classification. Add hostile-path tests (spaces, quotes, leading dashes, Unicode, shell metacharacters).
5. Use the remote XDG/home result instead of hard-coding `~/.local/share`.

### 3.4 Automatic remote-agent installation

Recommended algorithm:

1. Try a configured absolute agent path, then a cached compatible version, then PATH.
2. If none handshakes, run a minimal POSIX-shell probe over SSH for OS, architecture, home/XDG directory, free space, and an available SHA-256 command. Do not probe for or require `curl`, `wget`, DNS, or general internet access.
3. Select both the exact agent and approved rsync runtime matching the local protocol/package policy and remote target. Development builds must not impersonate a public release; require explicit local artifact overrides for them.
4. Download the immutable cross-platform assets and authenticated release manifest into a content-addressed controller-side artifact cache. Verify digests, provenance/release signature policy, target metadata, and downgrade policy locally before sending any executable bytes. Reuse the cached bytes across hosts and permit an explicit prefetch/import directory for controllers that will later run offline.
5. Stream the verified agent and rsync archives over the existing authenticated SSH connection. Remote direct HTTPS download may be an explicit optimization, but it must never be required and must apply the identical verification policy.
6. Install the agent to `${XDG_DATA_HOME:-$HOME/.local/share}/reflect-sync/agents/<version>/<target>/reflect-sync` and rsync to the managed runtime directory. Do not modify remote PATH or system packages.
7. Use lock directories, private temporary filenames, remote SHA-256 verification, `chmod`, atomic rename, and post-install `agent probe`. Treat the SSH transport as authenticated but still verify the received bytes. Concurrent sessions must converge on one valid installation.
8. Cache the absolute agent and rsync paths per host/port/user/target/protocol/runtime. Keep versions side by side so different controllers cannot break one another.
9. Log one clear install/update event describing both artifacts. Cache permanent capability failures and back off transient failures so the scheduler does not spam retries.
10. Expose `--remote-agent auto|never|<absolute-path>` and `--remote-rsync auto|system|<absolute-path>` plus policy/config equivalents. Default release builds to managed `auto`; default unversioned development builds to `never` unless explicitly enabled.
11. Add `reflect remote runtimes list/prune` or an age-based safe pruning mechanism. Session termination must not remove a shared agent or runtime.

Provide an explicit prewarming path such as `reflect runtime fetch --target linux-arm64` and a documented offline import/export workflow. This makes sandbox bootstrap deterministic in organizations where only a staging machine may access GitHub.

Security review gate:

- Threat-model compromised release assets, checksum substitution, hostile remote output, command injection, symlink races, partial downloads, downgrade attacks, GitHub outage/rate limiting, local proxy use, and a remote host with no DNS, default route, `curl`, or `wget`.
- Do not weaken SSH host-key checking or authentication to make bootstrap convenient.
- Never execute a moving `main`-branch script on the remote; install an exact immutable release.

End-to-end acceptance cases:

- clean Linux x64 and ARM remotes with SSH but no Node, ReflectSync, or compatible rsync;
- macOS ARM with no Node/Homebrew and only Apple openrsync;
- a remote sandbox with DNS and outbound networking blocked, using local download/verification plus SSH transfer for both agent and rsync;
- a controller with networking disabled after its artifact cache was prewarmed or imported;
- compatible agent already cached;
- incompatible protocol requiring side-by-side install;
- corrupt/partial download, checksum mismatch, read-only home, disk full, interrupted install, and concurrent installs;
- `--remote-agent never` produces one actionable error and no remote mutation.

## Phase 4 — Replace the website and documentation

Keep the first redesign dependency-light: accessible static HTML/CSS with a very small script only where it improves copy/platform selection. A docs framework is justified later only if versioned/reference content outgrows the static site.

### Information architecture

1. **Home:** one-sentence value proposition, honest beta/support badge, 30-second binary install, a real local-to-SSH example, and links to source/releases.
2. **Get started:** prerequisites, install, `doctor`, local/local session, SSH session with auto-bootstrap, daemon, inspect/stop/terminate, and uninstall.
3. **How it works:** concise diagram of scan → SQLite state → three-way plan → rsync → verification, plus the exact conflict/delete model.
4. **Safety:** initial reconciliation behavior, dry-run/plan workflow, backups/version-history limitation, symlink/case/Unicode behavior, failure recovery, privacy/no telemetry, and security verification.
5. **Platform support:** explicit table matching the support contract, minimum libc/OS, managed-rsync behavior, macOS openrsync note, and WSL versus native Windows status.
6. **Reference/troubleshooting:** commands, exit codes, configuration/environment, state locations, structured logs, `doctor`, common SSH/rsync errors, managed runtime and remote agent caches, and issue-report instructions.
7. **Performance:** reproducible benchmark methodology and results, never unsupported marketing numbers.
8. **Project:** roadmap, changelog, contributing, security policy, license, and comparison with Mutagen/Syncthing/Unison that avoids unverifiable claims.

### Design and delivery requirements

- Replace generic claims with concrete demonstrations and limitations. Remove or substantiate “production usage” and absolute correctness claims.
- Deduplicate/restructure the README into a short GitHub entry point; make the site the coherent guide and keep deep design material in maintained docs.
- Lead with the binary installer, retain npm install for Node/library users, and show the WSL command separately.
- Add copy buttons, command output, a short terminal recording, responsive navigation, visible focus states, reduced-motion behavior, semantic markup, and strong contrast.
- Prefer system/self-hosted fonts over third-party font requests.
- Add canonical/Open Graph metadata, favicon/logo assets, sitemap/robots, 404 page, and release/version links.
- Test HTML, internal/external links, install snippets, mobile layout, accessibility (axe), and Lighthouse performance/accessibility in CI.
- Keep Pages deployment least-privileged and prevent a site-only change from gaining release credentials.

Use the owner-purchased `reflect-sync.dev` domain as the primary product/docs domain. Keep the GitHub Pages origin working as a fallback and configure the custom domain only after HTTPS and the deployment workflow are validated. The public installer URL should remain stable even as releases change:

```text
https://reflect-sync.dev/install.sh
```

The script may resolve “latest,” but every ReflectSync and rsync runtime URL it ultimately installs must identify an immutable release and pass digest verification. Until the custom domain is configured, document `https://sagemathinc.github.io/reflect-sync/install.sh` as the canonical fallback.

## Phase 5 — Windows support, in honest stages

### W0: Supported WSL2 workflow

1. Document and test installing WSL2 and OpenSSH inside the distro, installing the Linux x64 ReflectSync binary plus its managed rsync runtime, and syncing Linux files.
2. Test Windows-mounted roots (`/mnt/c/...`) separately and document their case, metadata, watcher, symlink, and performance limitations.
3. Add a Windows/WSL CI smoke and validate on the provided Windows Server where WSL can be enabled without disrupting other work. Enabling WSL may require administrator action/reboot and is a manual prerequisite, not an automatic project action.
4. Provide a PowerShell helper that invokes the WSL installer, but do not call this “native Windows.”

### W1: Native executable and platform foundations

1. Build a Node 26 `reflect-sync.exe` with the same release manifest/checksum/provenance pipeline.
2. Make `--help`, `--version`, `doctor`, database/query commands, install paths, and endpoint parsing work natively.
3. Replace POSIX-only relative-path helpers with a canonical internal path model plus tested POSIX/Windows adapters. Cover drive roots, UNC paths, separators, reserved names, trailing dots/spaces, long paths, and case-only renames.
4. Define metadata projection rather than pretending POSIX uid/gid/mode maps losslessly to NTFS. Probe case sensitivity, symlink privilege, junction/reparse points, ACL behavior, locked files, and timestamp granularity.
5. Replace POSIX process/signal assumptions with a process-tree abstraction. Add a Windows daemon strategy (Windows Service or Task Scheduler) and reliable child cleanup.
6. Disable SSH ControlMaster cleanly where the installed Windows OpenSSH does not support the required multiplexing behavior.

This stage may publish an explicitly experimental `.exe`, but its installer and website must say that file synchronization is not supported until W2/W3 pass.

### W2: Decide the native transfer strategy

Write a short architecture decision record and prototype these options against the actual Windows host:

| Option                                                                           | Benefit                                                                   | Cost/risk                                                                                                                           |
| -------------------------------------------------------------------------------- | ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Require an external MSYS2/cwRsync-compatible rsync                               | Fastest path to a beta                                                    | Installation friction, POSIX path translation, remote-shell differences, support variability                                        |
| Ship rsync plus required runtime DLLs as a separately downloaded managed runtime | Predictable runtime and the same installer/bootstrap model as macOS/Linux | Larger multi-file distribution, POSIX translation behavior, GPLv3/license-source obligations, signing/updates, antivirus reputation |
| Add a native Reflect agent transfer protocol behind a transport interface        | Real native semantics and no rsync port dependency                        | Largest correctness/security scope; risks duplicating the battle-tested transport the design intentionally chose                    |

Recommendation: support WSL first; prototype a separately published MSYS2/cwRsync-derived managed runtime for native beta using the same manifest, source, signing, caching, and SSH-transfer architecture. Do not promise full native support until that runtime and its path/metadata projection pass the full safety suite. Do not add a custom transfer protocol merely to obtain a single executable.

### W3: Native conformance and signing

Before “native Windows supported” appears on the homepage, pass local/local and Windows↔Linux SSH cases for create/modify/delete/rename, directories, Unicode/case collisions, symlinks/junctions, permissions policy, large/sparse files, locked files, network interruption, daemon restart, reboot, and uninstall.

Sign `.exe`/installer artifacts with SHA-256 Authenticode and RFC 3161 timestamping. Prefer Microsoft Azure Artifact Signing (formerly Trusted Signing) with its official GitHub Action on a protected Windows release job. Verify with SignTool and test SmartScreen behavior. GitHub provenance is complementary; it does not replace Authenticode.

## Phase 6 — Data safety, performance, and operability

### 6.1 Safety model and destructive-operation UX

1. State invariants in executable tests, especially: unilateral edits are never overwritten by an older observation; completed cycles converge; retries are idempotent; case/Unicode collisions never destroy an unrepresentable source path.
2. Add a pure reference model and property/state-machine tests generating creates, writes, deletes, renames, chmods, symlinks, concurrent edits, clock skew, and watcher loss. Compare planner output and final trees with the model.
3. Add an initial reconciliation preview. When both roots are non-empty, show a summary/plan and require confirmation in an interactive CLI unless `--yes` or an explicit policy is supplied.
4. Make `--dry-run` genuinely end-to-end and add `reflect plan` output suitable for humans and JSON automation.
5. Clearly state that ReflectSync is not versioned backup. Consider an opt-in archive/trash retention mode for overwritten/deleted files before 1.0.
6. Version DB schemas and migrations; test upgrade, interrupted migration, backup, downgrade refusal, and recovery from corrupt state.

### 6.2 Fault and platform conformance

Inject failures at scan, hash, plan, rsync, DB commit, base update, watcher lock/release, and daemon restart boundaries:

- kill/crash/power-loss simulation;
- SSH disconnect/control socket death/host reboot;
- files changing or vanishing mid-transfer;
- disk full, quota, read-only directories, permission denied;
- corrupt NDJSON and truncated streams;
- SQLite busy/corrupt/WAL recovery;
- huge names, newlines, leading dashes, Unicode normalization, and case collisions;
- cross-device mounts and symlink loops.

Run a filesystem matrix where feasible: ext4, a case-folding filesystem, APFS default behavior, NTFS/DrvFS, and an overlay/container filesystem. Turn every skipped capability into either a supported test or an explicit documented non-goal.

### 6.3 Reproducible benchmarks

Create a checked-in benchmark harness with generated, deterministic datasets:

- many tiny files, large files, deep trees, git checkout/build churn, rename/delete storms, and idle daemon overhead;
- cold scan, warm scan, hot single-file latency, full reconciliation, memory high-water mark, DB size, wire bytes, and CPU time;
- local, LAN-like, and latency/bandwidth-shaped SSH runs;
- Linux x64/ARM and macOS ARM baselines using `bench-1`, `bench-arm`, and `m1` as validation hosts.

Publish methodology, exact versions, and variance. Start with reporting; add regression thresholds only after stable baselines exist.

### 6.4 Operability

- Use stable exit-code categories and structured errors for dependency, auth, remote, conflict, data-integrity, and transient failures.
- Add `reflect diagnostics`/`doctor --bundle` with explicit redaction and user review before sharing.
- Rate-limit repeated scheduler errors and surface a durable “action required” state instead of flooding logs.
- Add shell completions and a concise man page after the command surface settles.
- Define update and rollback behavior for the local CLI and cached remote agents.

## Phase 7 — Distribution, governance, and adoption

After the core binary installer is stable:

1. Add a Homebrew tap. The formula should install/use ReflectSync's pinned managed rsync runtime rather than acquire behavior from whichever Homebrew rsync version happens to resolve; allow an explicit compatible system-rsync override.
2. Consider a checksum-pinned Nix package and Linux packages only when maintainership is clear. Avoid many stale channels.
3. Add WinGet/Scoop only after native Windows is genuinely supported and signed.
4. Maintain `CHANGELOG.md`, `SECURITY.md`, `CONTRIBUTING.md`, support policy, issue/bug templates, and release notes generated from reviewed changes.
5. Add a bug template that asks for redacted `reflect doctor --json` output and a minimal reproduction.
6. Use GitHub Discussions or a clearly labeled feedback issue for early adopters; do not add silent telemetry to infer usage.
7. Define a 1.0 gate: Node 26 LTS, two or more successful release cycles, clean upgrade from prior state, no known data-loss bug, passing platform/safety matrix, signed releases, remote bootstrap, and accurate docs.

## Suggested pull-request sequence

Keep each change reviewable and leave `master` releasable:

1. **toolchain baseline:** pnpm config, audited dependency upgrades, TypeScript resolution, lifecycle cleanup.
2. **honest tests:** remove focus, explicit skips, test-count guard, package-consumer fixture.
3. **SSH CI:** ephemeral sshd and required remote tests; stabilize control-master recovery.
4. **repository security:** CI matrices, permissions, pinned Actions, Dependabot/CodeQL/config files.
5. **runtime preflight:** rsync/SSH feature detection and `reflect doctor`.
6. **managed rsync runtime:** reproducible native builds, capability tests, source/license artifacts, runtime manifest, and cache resolver.
7. **SEA builder:** Node 26 `--build-sea`, canonical artifacts, licenses, native smoke scripts.
8. **release workflow:** native build matrix, manifest/checksums/SBOM/attestations, draft release rehearsal.
9. **macOS trust:** sign/notarize ReflectSync and rsync independently, then validate both on clean `m1`.
10. **npm trust:** package staging/smoke tests and OIDC trusted publishing rehearsal.
11. **installer:** POSIX installer, managed-rsync installation, stable domain/fallback URL, clean-host tests, update/uninstall behavior.
12. **agent protocol:** JSON handshake, protocol/runtime compatibility, centralized SSH command layer.
13. **offline-first remote bootstrap:** local fetch/verification, SSH transfer of agent and rsync, atomic caches, failure/backoff tests.
14. **website/docs:** information architecture, accurate content, accessibility/link/install tests.
15. **safety harness:** reference model, property tests, fault injection, initial-sync preview.
16. **WSL support:** documented/tested WSL workflow and Windows-host validation.
17. **native Windows spike:** ADR, path/process abstractions, managed rsync runtime prototype, experimental signed executable.
18. **native Windows conformance:** only if the spike produces a supportable transport.
19. **distribution extras:** Homebrew first; other package managers only with maintainers/tests.

PRs 1–5 can proceed without release credentials. PRs 6–8 can produce unsigned internal artifacts. PR 9 needs Apple credentials. PR 10 needs the one-time npm trusted-publisher configuration. This ordering keeps external credentials from blocking useful engineering work.

## One-time owner/admin actions

These require repository, npm, Apple, or Azure authority and should not be guessed or automated by a coding agent:

- Restore `bench-1` SSH access if it will be a validation host.
- Enroll/confirm Apple Developer Program access; create Developer ID Application credentials and a Notary API key; add them only to a protected GitHub Environment.
- Configure npm trusted publishing for the exact `release.yml` workflow/environment, then revoke obsolete automation tokens after a successful OIDC release.
- Create `release` and `npm` GitHub Environments with appropriate reviewers.
- Enable branch/ruleset protection, read-only default workflow permissions, immutable releases, Dependabot/security features, and secret scanning where available.
- If native Windows signing proceeds, provision Azure Artifact Signing identity/profile and protected GitHub OIDC or service credentials.
- Configure the purchased `reflect-sync.dev` domain only after the GitHub Pages deployment and HTTPS validation are stable. DNS changes remain explicit owner actions; the existing Pages URL is the technical fallback.

Never commit signing certificates, API keys, app-specific passwords, npm tokens, or generated keychains.

## Decisions to make, with recommended defaults

| Decision                            | Recommended default                                                                                                                                          |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Ship Node 26 before it becomes LTS? | Build and prerelease now; use Node 26 LTS for stable/1.0.                                                                                                    |
| Remote install default              | `auto` for immutable release builds, `never` for unversioned development builds; always user-scoped.                                                         |
| Version compatibility               | Independent protocol range/capabilities, not exact package semver.                                                                                           |
| rsync distribution                  | Publish a pinned upstream build as a separate managed runtime asset; do not embed it in the SEA or replace the algorithm.                                    |
| macOS system openrsync              | Detect and explain incompatibility; use the signed managed runtime by default and permit an explicit compatible system override.                             |
| npm versus binaries                 | Binary installer is primary UX; npm remains supported for Node/library users.                                                                                |
| Public JS API                       | Mark experimental and narrow before 1.0.                                                                                                                     |
| macOS x64                           | Add after the required three-target matrix if signing/test cost remains low.                                                                                 |
| Native Windows                      | WSL supported first; prototype a separately published managed rsync/DLL runtime before considering a custom transport.                                       |
| Remote network assumption           | None. Download and verify on the controller, then transfer the agent and rsync runtime over SSH by default.                                                  |
| Managed dependency installation     | The installer/bootstrap may install exact, disclosed, user-scoped ReflectSync agent and rsync runtime artifacts; never mutate system packages or use `sudo`. |
| Public domain                       | Prefer `reflect-sync.dev`; keep the GitHub Pages URL operational as a fallback.                                                                              |
| Telemetry                           | None by default.                                                                                                                                             |

## Explicitly deferred

- A GUI, hosted control plane, account system, mobile client, mesh topology, or cloud relay.
- Remote-to-remote rsync until the common local-to-remote product is polished.
- Embedding rsync inside the SEA; it remains a separately published, licensed, signed, replaceable runtime artifact.
- Supporting every Linux libc/distribution or legacy macOS version.
- Broad package-manager coverage before the canonical release/install channel is reliable.
- Stabilizing the current wide internal JS API merely for compatibility with hypothetical users.

## Primary references checked for this plan

- Node 26 single executable applications and built-in `--build-sea`: <https://nodejs.org/docs/latest-v26.x/api/single-executable-applications.html>
- Node release status/schedule: <https://nodejs.org/en/about/previous-releases>
- Node supported platforms/runtime requirements: <https://github.com/nodejs/node/blob/main/BUILDING.md>
- npm trusted publishing/OIDC: <https://docs.npmjs.com/trusted-publishers/>
- npm provenance: <https://docs.npmjs.com/generating-provenance-statements/>
- GitHub-hosted ARM/x64 runner matrix: <https://docs.github.com/en/actions/reference/runners/github-hosted-runners>
- GitHub macOS certificate setup: <https://docs.github.com/en/actions/how-tos/deploy/deploy-to-third-party-platforms/sign-xcode-applications>
- Apple Developer ID notarization requirements: <https://developer.apple.com/documentation/security/notarizing_macos_software_before_distribution>
- Apple command-line notarization/stapling details: <https://developer.apple.com/documentation/security/customizing-the-notarization-workflow>
- GitHub artifact attestations: <https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations>
- GitHub immutable releases: <https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases>
- Rsync build options and optional dependencies: <https://download.samba.org/pub/rsync/INSTALL.html>
- Rsync release/security notes and GPLv3-or-later notice: <https://download.samba.org/pub/rsync/NEWS.html>
- Homebrew rsync dependency graph (why its bottle is not the portable artifact): <https://github.com/Homebrew/homebrew-core/blob/main/Formula/r/rsync.rb>
- MSYS2 Windows rsync package/dependency baseline: <https://packages.msys2.org/packages/rsync?variant=x86_64>
- Microsoft WSL installation/support: <https://learn.microsoft.com/en-us/windows/wsl/install>
- Microsoft/Azure Artifact Signing action: <https://github.com/Azure/artifact-signing-action>
- pnpm workspace override configuration: <https://pnpm.io/settings#overrides>
