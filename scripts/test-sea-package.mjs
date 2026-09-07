#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { verifyChecksum } from "./release-integrity.mjs";

const root = path.resolve(import.meta.dirname, "..");
const platform = process.platform === "win32" ? "windows" : process.platform;
const arch = process.arch === "x64" ? "x64" : process.arch;
const target = `${platform}-${arch}`;
const packageName = `reflect-sync-${target}`;
const extension = process.platform === "darwin" ? ".zip" : ".tar.gz";
const archive = path.join(
  root,
  "dist",
  "release",
  `${packageName}${extension}`,
);
const temporary = await mkdtemp(path.join(os.tmpdir(), "reflect-sea-package-"));

function run(command, args, allowedStatuses = [0]) {
  const result = spawnSync(command, args, { encoding: "utf8" });
  if (result.error) throw result.error;
  if (!allowedStatuses.includes(result.status)) {
    throw new Error(
      `${command} ${args.join(" ")} exited ${result.status}: ${result.stderr}`,
    );
  }
  return result;
}

try {
  await verifyChecksum(archive);

  if (process.platform === "darwin") {
    run("ditto", ["-x", "-k", archive, temporary]);
  } else {
    run("tar", ["-xzf", archive, "-C", temporary]);
  }

  const packageDirectory = path.join(temporary, packageName);
  for (const name of [
    "LICENSE.txt",
    "NODE-LICENSE.txt",
    "THIRD_PARTY_LICENSES.txt",
    "README.txt",
    "build-info.json",
  ]) {
    await readFile(path.join(packageDirectory, name));
  }
  const buildInfo = JSON.parse(
    await readFile(path.join(packageDirectory, "build-info.json"), "utf8"),
  );
  if (buildInfo.target !== target || buildInfo.nodeVersion !== "26.7.0") {
    throw new Error("SEA archive build identity does not match its target");
  }
  const executable = path.join(
    packageDirectory,
    process.platform === "win32" ? "reflect-sync.exe" : "reflect-sync",
  );
  run(executable, ["--version"]);
  const doctor = run(executable, ["doctor", "--json"], [0, 1]);
  const report = JSON.parse(doctor.stdout);
  if (!report.reflectSync?.bundled) {
    throw new Error("packaged executable did not report SEA runtime");
  }
  console.log(`SEA archive smoke test passed: ${archive}`);
} finally {
  await rm(temporary, { recursive: true, force: true });
}
