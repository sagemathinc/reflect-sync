#!/usr/bin/env node

import {
  copyFile,
  mkdir,
  readFile,
  readdir,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { thirdPartyNotices } from "./license-utils.mjs";
import { parseChecksumText, sha256File } from "./release-integrity.mjs";
import { createReleaseSpdx } from "./release-sbom.mjs";
import { verifyReleaseDirectory } from "./release-verification.mjs";

const root = path.resolve(import.meta.dirname, "..");
const distRoot = path.join(root, "dist");

function parseArgs(argv) {
  const result = {};
  for (let index = 0; index < argv.length; index += 2) {
    const key = argv[index];
    const value = argv[index + 1];
    if (!key?.startsWith("--") || value === undefined) {
      throw new Error(
        `invalid release assembly argument: ${key ?? "<missing>"}`,
      );
    }
    result[key.slice(2)] = value;
  }
  return result;
}

function assertGeneratedPath(file, label) {
  const relative = path.relative(distRoot, file);
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error(`${label} must be a child of ${distRoot}`);
  }
}

function isWithin(parent, candidate) {
  const relative = path.relative(parent, candidate);
  return !relative.startsWith("..") && !path.isAbsolute(relative);
}

async function walkFiles(directory) {
  const result = [];
  async function visit(current) {
    const entries = (await readdir(current, { withFileTypes: true })).sort(
      (left, right) => left.name.localeCompare(right.name),
    );
    for (const entry of entries) {
      const absolute = path.join(current, entry.name);
      if (entry.isDirectory()) await visit(absolute);
      else if (entry.isFile()) result.push(absolute);
      else
        throw new Error(`incoming release artifact is not a file: ${absolute}`);
    }
  }
  await visit(directory);
  return result;
}

function requireUnique(files, basename) {
  const matches = files.filter((file) => path.basename(file) === basename);
  if (matches.length !== 1) {
    throw new Error(
      `expected exactly one ${basename}, found ${matches.length}: ${matches.join(", ")}`,
    );
  }
  return matches[0];
}

async function verifyNativeSidecar(file, files) {
  const sidecar = requireUnique(files, `${path.basename(file)}.sha256`);
  if (path.dirname(sidecar) !== path.dirname(file)) {
    throw new Error(`checksum sidecar is not adjacent to ${file}`);
  }
  const entries = parseChecksumText(await readFile(sidecar, "utf8"));
  if (entries.size !== 1 || !entries.has(path.basename(file))) {
    throw new Error(`invalid native checksum sidecar for ${file}`);
  }
  const actual = await sha256File(file);
  if (entries.get(path.basename(file)) !== actual) {
    throw new Error(`native checksum mismatch for ${file}`);
  }
  return actual;
}

function mediaType(name) {
  if (name.endsWith(".tar.gz")) return "application/gzip";
  if (name.endsWith(".zip")) return "application/zip";
  if (name.endsWith(".json")) return "application/json";
  return "text/plain";
}

async function descriptor(file, fields) {
  return {
    name: path.basename(file),
    ...fields,
    mediaType: mediaType(file),
    sha256: await sha256File(file),
    size: (await stat(file)).size,
  };
}

export async function assembleRelease(options) {
  const input = path.resolve(options.input);
  const output = path.resolve(options.output ?? path.join(distRoot, "release"));
  const attestationSubjects = path.resolve(
    options.attestationSubjects ??
      path.join(distRoot, "release-metadata", "attestation-subjects.txt"),
  );
  assertGeneratedPath(output, "release output");
  assertGeneratedPath(attestationSubjects, "attestation subject file");
  if (isWithin(input, output) || isWithin(output, input)) {
    throw new Error("release input and output directories must not overlap");
  }

  const commit = options.commit;
  if (!/^[a-f0-9]{40}$/u.test(commit)) {
    throw new Error("--commit must be a full lowercase SHA-1");
  }
  const createdDate = new Date(options.created);
  if (Number.isNaN(createdDate.valueOf())) {
    throw new Error("--created must be an ISO-8601 timestamp");
  }
  const created = createdDate.toISOString();
  const packageMetadata = JSON.parse(
    await readFile(path.join(root, "package.json"), "utf8"),
  );
  const runtimeConfig = JSON.parse(
    await readFile(path.join(root, "runtime", "rsync", "runtime.json"), "utf8"),
  );
  const nodeVersion = (
    await readFile(path.join(root, ".node-version"), "utf8")
  ).trim();
  const tag = options.tag || null;
  if (tag && tag !== `v${packageMetadata.version}`) {
    throw new Error(
      `tag ${tag} does not match version ${packageMetadata.version}`,
    );
  }

  const files = await walkFiles(input);
  const targets = ["linux-x64", "linux-arm64", "darwin-arm64"];
  const buildInfo = new Map();
  for (const target of targets) {
    const metadata = JSON.parse(
      await readFile(requireUnique(files, `build-info-${target}.json`), "utf8"),
    );
    if (
      metadata.target !== target ||
      metadata.packageVersion !== packageMetadata.version ||
      metadata.nodeVersion !== nodeVersion ||
      metadata.gitCommit !== commit ||
      metadata.sourceDirty !== false
    ) {
      throw new Error(`invalid SEA build identity for ${target}`);
    }
    buildInfo.set(target, metadata);
  }

  const runtimeManifests = new Map();
  for (const file of files.filter(
    (file) => path.basename(file) === "manifest.json",
  )) {
    const manifest = JSON.parse(await readFile(file, "utf8"));
    if (!targets.includes(manifest.target)) continue;
    if (runtimeManifests.has(manifest.target)) {
      throw new Error(`duplicate rsync manifest for ${manifest.target}`);
    }
    if (
      manifest.runtimeVersion !== runtimeConfig.runtimeVersion ||
      manifest.rsyncVersion !== runtimeConfig.upstreamVersion ||
      manifest.source?.sha256 !== runtimeConfig.sourceSha256
    ) {
      throw new Error(`invalid rsync build identity for ${manifest.target}`);
    }
    runtimeManifests.set(manifest.target, manifest);
  }
  for (const target of targets) {
    if (!runtimeManifests.has(target)) {
      throw new Error(`missing rsync manifest for ${target}`);
    }
  }

  await rm(output, { recursive: true, force: true });
  await mkdir(output, { recursive: true });
  await mkdir(path.dirname(attestationSubjects), { recursive: true });

  const coreArtifacts = [];
  for (const target of targets) {
    const seaName = `reflect-sync-${target}${target === "darwin-arm64" ? ".zip" : ".tar.gz"}`;
    const source = requireUnique(files, seaName);
    const digest = await verifyNativeSidecar(source, files);
    const destination = path.join(output, seaName);
    await copyFile(source, destination);
    const info = buildInfo.get(target);
    coreArtifacts.push(
      await descriptor(destination, {
        kind: "sea",
        target,
        executableSha256: info.sha256,
        executableSize: info.size,
        nodeVersion: info.nodeVersion,
        minimumRuntime: info.minimumRuntime,
        supportTier: info.supportTier,
        platformSignature:
          target === "darwin-arm64" ? "ad-hoc-rehearsal" : "not-applicable",
        nativeArchiveSha256: digest,
      }),
    );

    const runtimeName = `reflect-sync-rsync-${runtimeConfig.runtimeVersion}-${target}${target === "darwin-arm64" ? ".zip" : ".tar.gz"}`;
    const runtimeSource = requireUnique(files, runtimeName);
    const runtimeDigest = await verifyNativeSidecar(runtimeSource, files);
    const runtimeDestination = path.join(output, runtimeName);
    await copyFile(runtimeSource, runtimeDestination);
    const runtimeManifest = runtimeManifests.get(target);
    coreArtifacts.push(
      await descriptor(runtimeDestination, {
        kind: "rsync-runtime",
        target,
        runtimeVersion: runtimeManifest.runtimeVersion,
        rsyncVersion: runtimeManifest.rsyncVersion,
        protocol: runtimeManifest.protocol,
        executableSha256: runtimeManifest.executableSha256,
        executableSize: runtimeManifest.executableSize,
        features: runtimeManifest.features,
        platformSignature:
          target === "darwin-arm64" ? "unsigned-rehearsal" : "not-applicable",
        nativeArchiveSha256: runtimeDigest,
      }),
    );
  }

  const sourceName = `reflect-sync-rsync-${runtimeConfig.runtimeVersion}-sources.tar.gz`;
  const sourceArchive = requireUnique(files, sourceName);
  const sourceDigest = await verifyNativeSidecar(sourceArchive, files);
  const sourceDestination = path.join(output, sourceName);
  await copyFile(sourceArchive, sourceDestination);
  coreArtifacts.push(
    await descriptor(sourceDestination, {
      kind: "rsync-source",
      target: "all",
      runtimeVersion: runtimeConfig.runtimeVersion,
      upstreamVersion: runtimeConfig.upstreamVersion,
      upstreamSourceSha256: runtimeConfig.sourceSha256,
      nativeArchiveSha256: sourceDigest,
    }),
  );

  await copyFile(
    path.join(root, "LICENSE.txt"),
    path.join(output, "LICENSE.txt"),
  );
  await writeFile(
    path.join(output, "THIRD_PARTY_LICENSES.txt"),
    await thirdPartyNotices(root),
  );
  const sbomName = `reflect-sync-${packageMetadata.version}.spdx.json`;
  await writeFile(
    path.join(output, sbomName),
    `${JSON.stringify(
      createReleaseSpdx({
        root,
        packageMetadata,
        runtimeConfig,
        nodeVersion,
        commit,
        created,
        artifacts: coreArtifacts,
      }),
      null,
      2,
    )}\n`,
  );

  const metadataArtifacts = await Promise.all([
    descriptor(path.join(output, "LICENSE.txt"), {
      kind: "license",
      target: "all",
    }),
    descriptor(path.join(output, "THIRD_PARTY_LICENSES.txt"), {
      kind: "license-notices",
      target: "all",
    }),
    descriptor(path.join(output, sbomName), {
      kind: "sbom",
      target: "all",
      format: "SPDX-2.3",
    }),
  ]);
  const artifacts = [...coreArtifacts, ...metadataArtifacts].sort(
    (left, right) => left.name.localeCompare(right.name),
  );
  const manifest = {
    schemaVersion: 1,
    name: packageMetadata.name,
    version: packageMetadata.version,
    releaseTag: tag,
    releaseStatus: "unsigned-rehearsal",
    publicationPolicy: "draft-only-until-macos-developer-id-notarization",
    gitCommit: commit,
    created,
    nodeVersion,
    rsyncRuntimeVersion: runtimeConfig.runtimeVersion,
    supportedTargets: targets,
    agentCompatibility: {
      scheme: "exact-package-version",
      version: packageMetadata.version,
    },
    verification: {
      checksums: "SHA256SUMS",
      sbom: sbomName,
      provenance: `gh attestation verify <artifact> -R ${packageMetadata.repository.url.replace("https://github.com/", "")}`,
      sbomAttestation:
        `gh attestation verify <artifact> -R ${packageMetadata.repository.url.replace("https://github.com/", "")} ` +
        "--predicate-type https://spdx.dev/Document/v2.3",
    },
    artifacts,
  };
  await writeFile(
    path.join(output, "release-manifest.json"),
    `${JSON.stringify(manifest, null, 2)}\n`,
  );

  const releaseNames = (await readdir(output)).sort();
  const checksumLines = [];
  for (const name of releaseNames) {
    checksumLines.push(`${await sha256File(path.join(output, name))}  ${name}`);
  }
  await writeFile(
    path.join(output, "SHA256SUMS"),
    `${checksumLines.join("\n")}\n`,
  );
  await writeFile(
    attestationSubjects,
    `${coreArtifacts
      .sort((left, right) => left.name.localeCompare(right.name))
      .map((artifact) => `${artifact.sha256}  ${artifact.name}`)
      .join("\n")}\n`,
  );

  await verifyReleaseDirectory(output, {
    version: packageMetadata.version,
    runtimeVersion: runtimeConfig.runtimeVersion,
  });
  return { output, attestationSubjects, manifest };
}

if (
  process.argv[1] &&
  pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url
) {
  const args = parseArgs(process.argv.slice(2));
  if (!args.input || !args.commit || !args.created) {
    throw new Error(
      "usage: assemble-release.mjs --input <dir> --output <dist-dir> --commit <sha> --created <iso> [--tag <tag>] [--attestation-subjects <file>]",
    );
  }
  const result = await assembleRelease({
    input: args.input,
    output: args.output,
    commit: args.commit,
    created: args.created,
    tag: args.tag,
    attestationSubjects: args["attestation-subjects"],
  });
  console.log(`assembled release candidate at ${result.output}`);
}
