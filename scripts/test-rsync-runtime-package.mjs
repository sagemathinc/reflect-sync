#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { verifyChecksum } from "./release-integrity.mjs";

const root = path.resolve(import.meta.dirname, "..");
const config = JSON.parse(
  await readFile(path.join(root, "runtime", "rsync", "runtime.json"), "utf8"),
);
const platform = process.platform === "win32" ? "windows" : process.platform;
const arch = process.arch === "x64" ? "x64" : process.arch;
const target = `${platform}-${arch}`;
const runtimeName = `reflect-sync-rsync-${config.runtimeVersion}-${target}`;
const sourcesName = `reflect-sync-rsync-${config.runtimeVersion}-sources`;
const runtimeArchive = path.join(
  root,
  "dist",
  "release",
  `${runtimeName}${process.platform === "darwin" ? ".zip" : ".tar.gz"}`,
);
const sourcesArchive = path.join(
  root,
  "dist",
  "release",
  `${sourcesName}.tar.gz`,
);
const temporary = await mkdtemp(
  path.join(os.tmpdir(), "reflect-rsync-package-"),
);

function run(command, args) {
  const result = spawnSync(command, args, { encoding: "utf8" });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} exited ${result.status}: ${result.stderr}`,
    );
  }
  return result;
}

try {
  await verifyChecksum(runtimeArchive);
  await verifyChecksum(sourcesArchive);
  if (process.platform === "darwin") {
    run("ditto", ["-x", "-k", runtimeArchive, temporary]);
  } else {
    run("tar", ["-xzf", runtimeArchive, "-C", temporary]);
  }
  const packageDirectory = path.join(temporary, runtimeName);
  const manifest = JSON.parse(
    await readFile(path.join(packageDirectory, "manifest.json"), "utf8"),
  );
  await readFile(path.join(packageDirectory, "licenses", "rsync-COPYING"));
  await readFile(path.join(packageDirectory, manifest.upstreamTests.log));
  const executable = path.join(packageDirectory, manifest.executable);
  run(executable, ["--version"]);
  run(executable, ["--help"]);

  const sourceListing = run("tar", ["-tzf", sourcesArchive]).stdout;
  for (const required of [
    `${sourcesName}/COPYING`,
    `${sourcesName}/build-config.json`,
    `${sourcesName}/source/build-rsync-runtime.sh`,
    `${sourcesName}/source/rsync-${config.upstreamVersion}.tar.gz`,
  ]) {
    if (!sourceListing.split(/\r?\n/u).includes(required)) {
      throw new Error(`source archive omits ${required}`);
    }
  }
  console.log(`rsync runtime archive smoke test passed: ${runtimeArchive}`);
} finally {
  await rm(temporary, { recursive: true, force: true });
}
