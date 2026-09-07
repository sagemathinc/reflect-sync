#!/usr/bin/env node

import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { get } from "node:https";
import {
  chmod,
  copyFile,
  mkdir,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import path from "node:path";
import { archiveFileEntries, normalizeArchiveTree } from "./archive-utils.mjs";
import { thirdPartyNotices } from "./license-utils.mjs";

const root = path.resolve(import.meta.dirname, "..");
const platform = process.platform === "win32" ? "windows" : process.platform;
const arch = process.arch === "x64" ? "x64" : process.arch;
const target = `${platform}-${arch}`;
const executableSuffix = process.platform === "win32" ? ".exe" : "";
const sourceExecutable = path.join(
  root,
  "dist",
  "sea",
  `reflect-sync-${target}${executableSuffix}`,
);
const releaseDirectory = path.join(root, "dist", "release");
const packageName = `reflect-sync-${target}`;
const packageDirectory = path.join(releaseDirectory, packageName);
const nodeLicenseUrl = `https://raw.githubusercontent.com/nodejs/node/v${process.versions.node}/LICENSE`;
const nodeLicenseSha256 =
  "5888dbb9a1d2b18f2c3e6c5f6af1b39de658372b402a0577b002777f14c62ace";

if (process.versions.node !== "26.7.0") {
  throw new Error(
    `SEA release packaging requires Node 26.7.0; running ${process.versions.node}`,
  );
}

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
  return result.stdout;
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function download(url) {
  return new Promise((resolve, reject) => {
    get(url, (response) => {
      if (response.statusCode !== 200) {
        response.resume();
        reject(new Error(`download failed (${response.statusCode}): ${url}`));
        return;
      }
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("end", () => resolve(Buffer.concat(chunks)));
      response.on("error", reject);
    }).on("error", reject);
  });
}

await rm(packageDirectory, { recursive: true, force: true });
await mkdir(packageDirectory, { recursive: true });

const packagedExecutable = path.join(
  packageDirectory,
  `reflect-sync${executableSuffix}`,
);
await copyFile(sourceExecutable, packagedExecutable);
if (process.platform !== "win32") await chmod(packagedExecutable, 0o755);
await copyFile(
  path.join(root, "LICENSE.txt"),
  path.join(packageDirectory, "LICENSE.txt"),
);
await copyFile(
  path.join(root, "dist", "sea", `build-info-${target}.json`),
  path.join(packageDirectory, "build-info.json"),
);

const nodeLicense = await download(nodeLicenseUrl);
if (sha256(nodeLicense) !== nodeLicenseSha256) {
  throw new Error(`Node license digest mismatch for ${nodeLicenseUrl}`);
}
await writeFile(path.join(packageDirectory, "NODE-LICENSE.txt"), nodeLicense);
await writeFile(
  path.join(packageDirectory, "THIRD_PARTY_LICENSES.txt"),
  await thirdPartyNotices(root),
);
await writeFile(
  path.join(packageDirectory, "README.txt"),
  `ReflectSync ${JSON.parse(await readFile(path.join(root, "package.json"), "utf8")).version}\n\n` +
    `Target: ${target}\nNode runtime: ${process.versions.node}\n\n` +
    `Install the reflect-sync executable somewhere on PATH, then run:\n\n` +
    `    reflect-sync doctor\n\n` +
    `The separately distributed managed rsync runtime is selected automatically\n` +
    `when installed. See https://reflect-sync.dev for installation, platform,\n` +
    `security, and source information.\n`,
);

let archive;
if (process.platform === "darwin") {
  archive = path.join(releaseDirectory, `${packageName}.zip`);
  await rm(archive, { force: true });
  await normalizeArchiveTree(packageDirectory);
  run(
    "zip",
    [
      "-X",
      "-q",
      archive,
      ...(await archiveFileEntries(packageDirectory, packageName)),
    ],
    {
      cwd: releaseDirectory,
      env: { ...process.env, TZ: "UTC" },
    },
  );
} else if (process.platform === "linux") {
  archive = path.join(releaseDirectory, `${packageName}.tar.gz`);
  await rm(archive, { force: true });
  run("tar", [
    "--sort=name",
    "--mtime=@0",
    "--owner=0",
    "--group=0",
    "--numeric-owner",
    "-czf",
    archive,
    "-C",
    releaseDirectory,
    packageName,
  ]);
} else {
  throw new Error(`SEA release packaging is not implemented for ${platform}`);
}

const archiveDigest = sha256(await readFile(archive));
const checksumFile = `${archive}.sha256`;
await writeFile(checksumFile, `${archiveDigest}  ${path.basename(archive)}\n`);
console.log(`Packaged ${archive}`);
console.log(`${archiveDigest}  ${path.basename(archive)}`);
