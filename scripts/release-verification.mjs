import { readFile, readdir, stat } from "node:fs/promises";
import path from "node:path";
import { parseChecksumText, sha256File } from "./release-integrity.mjs";

function sameMembers(actual, expected, label) {
  const a = [...actual].sort();
  const e = [...expected].sort();
  if (JSON.stringify(a) !== JSON.stringify(e)) {
    throw new Error(`${label} mismatch:\nactual: ${a}\nexpected: ${e}`);
  }
}

export async function verifyReleaseDirectory(
  directory,
  { version: expectedVersion, runtimeVersion },
) {
  const entries = await readdir(directory, { withFileTypes: true });
  if (entries.some((entry) => !entry.isFile())) {
    throw new Error("release directory must contain only regular files");
  }
  const names = entries.map((entry) => entry.name).sort();
  const checksumText = await readFile(
    path.join(directory, "SHA256SUMS"),
    "utf8",
  );
  const checksums = parseChecksumText(checksumText);
  sameMembers(
    checksums.keys(),
    names.filter((name) => name !== "SHA256SUMS"),
    "SHA256SUMS entries",
  );
  for (const [name, expected] of checksums) {
    const actual = await sha256File(path.join(directory, name));
    if (actual !== expected) {
      throw new Error(`release digest mismatch for ${name}`);
    }
  }

  const manifest = JSON.parse(
    await readFile(path.join(directory, "release-manifest.json"), "utf8"),
  );
  if (manifest.schemaVersion !== 1) {
    throw new Error("unsupported release manifest schema");
  }
  if (manifest.version !== expectedVersion) {
    throw new Error(
      `manifest version ${manifest.version} does not match ${expectedVersion}`,
    );
  }
  if (manifest.rsyncRuntimeVersion !== runtimeVersion) {
    throw new Error(
      `manifest rsync runtime ${manifest.rsyncRuntimeVersion} does not match ${runtimeVersion}`,
    );
  }
  const targets = ["linux-x64", "linux-arm64", "darwin-arm64"];
  sameMembers(manifest.supportedTargets, targets, "supported release targets");
  if (!/^[a-f0-9]{40}$/u.test(manifest.gitCommit)) {
    throw new Error("manifest gitCommit is not a full SHA-1");
  }
  const describedNames = manifest.artifacts.map((artifact) => artifact.name);
  sameMembers(
    describedNames,
    names.filter(
      (name) => name !== "SHA256SUMS" && name !== "release-manifest.json",
    ),
    "release manifest artifacts",
  );
  for (const artifact of manifest.artifacts) {
    const file = path.join(directory, artifact.name);
    if ((await sha256File(file)) !== artifact.sha256) {
      throw new Error(`manifest digest mismatch for ${artifact.name}`);
    }
    if ((await stat(file)).size !== artifact.size) {
      throw new Error(`manifest size mismatch for ${artifact.name}`);
    }
  }

  const sbomName = `reflect-sync-${expectedVersion}.spdx.json`;
  const sbom = JSON.parse(
    await readFile(path.join(directory, sbomName), "utf8"),
  );
  if (sbom.spdxVersion !== "SPDX-2.3") {
    throw new Error("release SBOM is not SPDX 2.3");
  }
  const coreArtifacts = manifest.artifacts.filter((artifact) =>
    ["sea", "rsync-runtime", "rsync-source"].includes(artifact.kind),
  );
  sameMembers(
    coreArtifacts.map((artifact) => artifact.name),
    [
      "reflect-sync-linux-x64.tar.gz",
      "reflect-sync-linux-arm64.tar.gz",
      "reflect-sync-darwin-arm64.zip",
      `reflect-sync-rsync-${runtimeVersion}-linux-x64.tar.gz`,
      `reflect-sync-rsync-${runtimeVersion}-linux-arm64.tar.gz`,
      `reflect-sync-rsync-${runtimeVersion}-darwin-arm64.zip`,
      `reflect-sync-rsync-${runtimeVersion}-sources.tar.gz`,
    ],
    "core release artifacts",
  );
  sameMembers(
    sbom.files.map((file) => file.fileName.replace(/^\.\//u, "")),
    coreArtifacts.map((artifact) => artifact.name),
    "SBOM artifact files",
  );
  for (const artifact of coreArtifacts) {
    const sbomFile = sbom.files.find(
      (file) => file.fileName === `./${artifact.name}`,
    );
    if (sbomFile?.checksums?.[0]?.checksumValue !== artifact.sha256) {
      throw new Error(`SBOM digest mismatch for ${artifact.name}`);
    }
  }
  return manifest;
}
