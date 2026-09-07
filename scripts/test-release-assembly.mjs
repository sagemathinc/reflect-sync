#!/usr/bin/env node

import { createHash } from "node:crypto";
import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { assembleRelease } from "./assemble-release.mjs";

const root = path.resolve(import.meta.dirname, "..");
const packageMetadata = JSON.parse(
  await readFile(path.join(root, "package.json"), "utf8"),
);
const runtimeConfig = JSON.parse(
  await readFile(path.join(root, "runtime", "rsync", "runtime.json"), "utf8"),
);
const nodeVersion = (
  await readFile(path.join(root, ".node-version"), "utf8")
).trim();
const base = path.join(root, "dist", `release-assembly-test-${process.pid}`);
const input = path.join(base, "incoming");
const commit = "a".repeat(40);
const created = "2026-09-02T00:00:00.000Z";

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

async function writeJson(file, value) {
  await mkdir(path.dirname(file), { recursive: true });
  await writeFile(file, `${JSON.stringify(value, null, 2)}\n`);
}

async function writeArtifact(directory, name, contents) {
  await mkdir(directory, { recursive: true });
  const file = path.join(directory, name);
  const bytes = Buffer.from(contents);
  await writeFile(file, bytes);
  await writeFile(`${file}.sha256`, `${sha256(bytes)}  ${name}\n`);
  return file;
}

async function assertDirectoriesEqual(left, right) {
  const leftNames = (await readdir(left)).sort();
  const rightNames = (await readdir(right)).sort();
  if (JSON.stringify(leftNames) !== JSON.stringify(rightNames)) {
    throw new Error("repeated release assembly changed its file inventory");
  }
  for (const name of leftNames) {
    const a = await readFile(path.join(left, name));
    const b = await readFile(path.join(right, name));
    if (!a.equals(b)) {
      throw new Error(`repeated release assembly changed ${name}`);
    }
  }
}

try {
  const targets = ["linux-x64", "linux-arm64", "darwin-arm64"];
  for (const target of targets) {
    const directory = path.join(input, `reflect-release-${target}`);
    const seaName = `reflect-sync-${target}${target === "darwin-arm64" ? ".zip" : ".tar.gz"}`;
    await writeArtifact(directory, seaName, `sea:${target}`);
    await writeJson(path.join(directory, `build-info-${target}.json`), {
      schemaVersion: 1,
      packageVersion: packageMetadata.version,
      target,
      file: `reflect-sync-${target}`,
      nodeVersion,
      gitCommit: commit,
      sourceDirty: false,
      agentCompatibility: {
        scheme: "exact-package-version",
        version: packageMetadata.version,
      },
      rsyncRuntimeVersion: runtimeConfig.runtimeVersion,
      minimumRuntime: target.startsWith("linux-")
        ? "glibc >= 2.28 with libatomic"
        : "macOS >= 13.5",
      supportTier: "supported",
      workflow: null,
      sha256: sha256(`executable:${target}`),
      size: 1000 + target.length,
    });

    const runtimeName = `reflect-sync-rsync-${runtimeConfig.runtimeVersion}-${target}${target === "darwin-arm64" ? ".zip" : ".tar.gz"}`;
    await writeArtifact(directory, runtimeName, `rsync:${target}`);
    await writeJson(path.join(directory, target, "manifest.json"), {
      schemaVersion: 1,
      runtimeVersion: runtimeConfig.runtimeVersion,
      target,
      executable: "bin/rsync",
      executableSha256: sha256(`rsync-executable:${target}`),
      executableSize: 500 + target.length,
      rsyncVersion: runtimeConfig.upstreamVersion,
      protocol: 32,
      source: {
        url: runtimeConfig.sourceUrl,
        sha256: runtimeConfig.sourceSha256,
      },
      features: {
        from0: true,
        relative: true,
        outbuf: true,
        infoProgress2: true,
        logFile: true,
        logFileFormat: true,
        compression: runtimeConfig.compression,
      },
    });
  }
  const sourceName = `reflect-sync-rsync-${runtimeConfig.runtimeVersion}-sources.tar.gz`;
  const sourceArchive = await writeArtifact(
    path.join(input, "reflect-release-source"),
    sourceName,
    "corresponding source",
  );

  const outputA = path.join(base, "output-a");
  const outputB = path.join(base, "output-b");
  await assembleRelease({
    input,
    output: outputA,
    commit,
    created,
    tag: `v${packageMetadata.version}`,
    attestationSubjects: path.join(base, "metadata-a", "subjects.txt"),
  });
  await assembleRelease({
    input,
    output: outputB,
    commit,
    created,
    tag: `v${packageMetadata.version}`,
    attestationSubjects: path.join(base, "metadata-b", "subjects.txt"),
  });
  await assertDirectoriesEqual(outputA, outputB);

  await writeFile(sourceArchive, "tampered source");
  let rejectedTamper = false;
  try {
    await assembleRelease({
      input,
      output: path.join(base, "output-tampered"),
      commit,
      created,
      tag: `v${packageMetadata.version}`,
      attestationSubjects: path.join(base, "metadata-tampered", "subjects.txt"),
    });
  } catch (error) {
    rejectedTamper = /checksum mismatch/u.test(String(error));
  }
  if (!rejectedTamper) {
    throw new Error(
      "release assembly did not reject a tampered native artifact",
    );
  }
  console.log(
    "release assembly test passed (determinism and tamper rejection)",
  );
} finally {
  await rm(base, { recursive: true, force: true });
}
