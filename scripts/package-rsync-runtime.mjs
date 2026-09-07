#!/usr/bin/env node

import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { chmod, cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { archiveFileEntries, normalizeArchiveTree } from "./archive-utils.mjs";

const root = path.resolve(import.meta.dirname, "..");
const platform = process.platform === "win32" ? "windows" : process.platform;
const arch = process.arch === "x64" ? "x64" : process.arch;
const currentTarget = `${platform}-${arch}`;
const input = path.resolve(
  process.argv[2] ?? path.join(root, "dist", "rsync-runtime", currentTarget),
);
const manifest = JSON.parse(
  await readFile(path.join(input, "manifest.json"), "utf8"),
);
if (manifest.target !== currentTarget) {
  throw new Error(
    `runtime target ${manifest.target} does not match build host ${currentTarget}`,
  );
}

const releaseDirectory = path.join(root, "dist", "release");
const runtimeName = `reflect-sync-rsync-${manifest.runtimeVersion}-${manifest.target}`;
const sourcesName = `reflect-sync-rsync-${manifest.runtimeVersion}-sources`;
const runtimeStage = path.join(releaseDirectory, runtimeName);
const sourcesStage = path.join(releaseDirectory, sourcesName);

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: root,
    encoding: "utf8",
    ...options,
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} exited ${result.status}: ${result.stderr}`,
    );
  }
}

async function checksum(file) {
  return createHash("sha256")
    .update(await readFile(file))
    .digest("hex");
}

async function writeChecksum(file) {
  const digest = await checksum(file);
  await writeFile(`${file}.sha256`, `${digest}  ${path.basename(file)}\n`);
  console.log(`${digest}  ${path.basename(file)}`);
}

await mkdir(releaseDirectory, { recursive: true });
await rm(runtimeStage, { recursive: true, force: true });
await rm(sourcesStage, { recursive: true, force: true });
await mkdir(runtimeStage, { recursive: true });
await mkdir(sourcesStage, { recursive: true });

for (const name of [
  "bin",
  "licenses",
  "tests",
  "build-config.json",
  "manifest.json",
]) {
  await cp(path.join(input, name), path.join(runtimeStage, name), {
    recursive: true,
  });
}
if (process.platform !== "win32") {
  await chmod(path.join(runtimeStage, manifest.executable), 0o755);
}
await cp(path.join(input, "source"), path.join(sourcesStage, "source"), {
  recursive: true,
});
await cp(
  path.join(input, "build-config.json"),
  path.join(sourcesStage, "build-config.json"),
);
await cp(
  path.join(input, "licenses", "rsync-COPYING"),
  path.join(sourcesStage, "COPYING"),
);

let runtimeArchive;
if (process.platform === "darwin") {
  runtimeArchive = path.join(releaseDirectory, `${runtimeName}.zip`);
  await rm(runtimeArchive, { force: true });
  await normalizeArchiveTree(runtimeStage);
  run(
    "zip",
    [
      "-X",
      "-q",
      runtimeArchive,
      ...(await archiveFileEntries(runtimeStage, runtimeName)),
    ],
    {
      cwd: releaseDirectory,
      env: { ...process.env, TZ: "UTC" },
    },
  );
} else if (process.platform === "linux") {
  runtimeArchive = path.join(releaseDirectory, `${runtimeName}.tar.gz`);
  await rm(runtimeArchive, { force: true });
  run("tar", [
    "--sort=name",
    "--mtime=@0",
    "--owner=0",
    "--group=0",
    "--numeric-owner",
    "-czf",
    runtimeArchive,
    "-C",
    releaseDirectory,
    runtimeName,
  ]);
} else {
  throw new Error(`rsync runtime packaging is not implemented for ${platform}`);
}

const sourcesArchive = path.join(releaseDirectory, `${sourcesName}.tar.gz`);
await rm(sourcesArchive, { force: true });
if (process.platform === "linux") {
  run("tar", [
    "--sort=name",
    "--mtime=@0",
    "--owner=0",
    "--group=0",
    "--numeric-owner",
    "-czf",
    sourcesArchive,
    "-C",
    releaseDirectory,
    sourcesName,
  ]);
} else {
  await normalizeArchiveTree(sourcesStage);
  const uncompressedSourcesArchive = sourcesArchive.replace(/\.gz$/u, "");
  await rm(uncompressedSourcesArchive, { force: true });
  run(
    "tar",
    [
      "-cf",
      uncompressedSourcesArchive,
      "-C",
      releaseDirectory,
      ...(await archiveFileEntries(sourcesStage, sourcesName)),
    ],
    { env: { ...process.env, COPYFILE_DISABLE: "1", TZ: "UTC" } },
  );
  run("gzip", ["-n", "-f", uncompressedSourcesArchive], {
    env: { ...process.env, TZ: "UTC" },
  });
}

await writeChecksum(runtimeArchive);
await writeChecksum(sourcesArchive);
console.log(`Packaged ${runtimeArchive}`);
console.log(`Packaged ${sourcesArchive}`);
