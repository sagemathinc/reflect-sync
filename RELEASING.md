# ReflectSync release process

The current workflow implements the credential-free release rehearsal through
PR 8. It builds, tests, assembles, and optionally attests exact native artifacts,
but it cannot publish a public release. macOS Developer ID signing and Apple
notarization are deliberately required before that boundary is removed.

## Rehearsal modes

- A pull request that changes release inputs runs the full release-quality gate,
  three native builders, deterministic assembly, archive smoke tests, checksum
  verification, and SBOM validation. It receives read-only repository access.
- A manual run with no inputs performs the same build-only rehearsal.
- A manual run with **attest** enabled also creates GitHub/Sigstore build and
  SPDX attestations. It does not create a GitHub release.
- A manual run with **create_draft** requires an existing `vX.Y.Z` tag matching
  `package.json`; it attests the candidate, creates or repairs a draft
  prerelease, downloads every attached asset, and verifies it again.
- A pushed `v*` tag follows the same attested draft-only path automatically.

No PR 8 job contains a command that removes draft status. The draft job is the
only job with `contents: write`, and it uses the `release` environment, which
the repository owner should protect with required reviewers. An existing
non-draft release is immutable from this workflow and causes a hard failure.

## Preparing a version

1. Set the intended version in `package.json`.
2. Add a dated `## [X.Y.Z] - YYYY-MM-DD` entry to `CHANGELOG.md`.
3. Land and validate the clean commit.
4. Create the exact annotated or lightweight tag `vX.Y.Z` at that commit.
5. Push the tag or manually dispatch the workflow with that tag.

The validator requires the tag, package version, changelog entry, clean checkout,
and full commit identity to agree before any native build starts.

## Candidate contents and verification

The finalizer downloads tested native workflow artifacts and refuses missing,
duplicate, dirty, wrongly targeted, or digest-mismatched inputs. It emits:

- three Node 26 SEA archives;
- three managed rsync runtime archives and one corresponding-source archive;
- `release-manifest.json`;
- consolidated `SHA256SUMS`;
- `reflect-sync-X.Y.Z.spdx.json`;
- project and bundled-dependency license notices.

Verify a downloaded directory with:

```sh
node scripts/verify-release.mjs /path/to/downloaded/assets
```

After an attested rehearsal, verify an archive and its SPDX predicate with:

```sh
gh attestation verify ARTIFACT -R sagemathinc/reflect-sync
gh attestation verify ARTIFACT -R sagemathinc/reflect-sync \
  --predicate-type https://spdx.dev/Document/v2.3
```

Until PR 9 is complete, the manifest identifies macOS SEA signatures as ad-hoc
and macOS rsync as unsigned, and labels the entire candidate
`unsigned-rehearsal`. Such a draft must not be published as a stable release.
