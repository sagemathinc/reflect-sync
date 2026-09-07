#!/usr/bin/env node

import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { readFile, stat, writeFile } from "node:fs/promises";
import path from "node:path";

const [directory, target, runtimeVersion] = process.argv.slice(2);
if (!directory || !target || !runtimeVersion) {
  throw new Error(
    "usage: create-rsync-runtime-manifest.mjs <directory> <target> <runtime-version>",
  );
}
const executable = path.join(directory, "bin", "rsync");
const bytes = await readFile(executable);
const digest = createHash("sha256").update(bytes).digest("hex");
const version = spawnSync(executable, ["--version"], { encoding: "utf8" });
const help = spawnSync(executable, ["--help"], { encoding: "utf8" });
if (version.status !== 0 || help.status !== 0) {
  throw new Error(`built rsync does not run: ${version.stderr || help.stderr}`);
}
const all = `${version.stdout}\n${version.stderr}\n${help.stdout}\n${help.stderr}`;
const required = [
  "--from0",
  "--relative",
  "--outbuf",
  "--info",
  "--log-file",
  "--log-file-format",
];
const missing = required.filter((option) => !all.includes(option));
if (missing.length) throw new Error(`built rsync lacks ${missing.join(", ")}`);

const metadata = await stat(executable);
const buildConfig = JSON.parse(
  await readFile(path.join(directory, "build-config.json"), "utf8"),
);
const compiler = spawnSync(process.env.CC ?? "cc", ["--version"], {
  encoding: "utf8",
});
if (compiler.status !== 0) {
  throw new Error(`unable to identify compiler: ${compiler.stderr}`);
}
const manifest = {
  schemaVersion: 1,
  runtimeVersion,
  target,
  executable: "bin/rsync",
  executableSha256: digest,
  executableSize: metadata.size,
  rsyncVersion: all.match(/\bversion\s+(\d+(?:\.\d+){1,3})/u)?.[1] ?? null,
  protocol: Number(all.match(/protocol version\s+(\d+)/u)?.[1] ?? 0) || null,
  source: {
    url: buildConfig.sourceUrl,
    sha256: buildConfig.sourceSha256,
  },
  build: {
    compilerCommand: process.env.CC ?? "cc",
    compiler: compiler.stdout.split(/\r?\n/u)[0],
    configureFlags: buildConfig.configureFlags,
    sourceDateEpoch: process.env.SOURCE_DATE_EPOCH,
    minimumMacos:
      target === "darwin-arm64"
        ? (process.env.MACOSX_DEPLOYMENT_TARGET ?? "13.5")
        : null,
  },
  upstreamTests: {
    command: "make check",
    exclusions:
      buildConfig.upstreamTestExclusions[
        target.startsWith("linux-") ? "linux" : "darwin"
      ],
    log: "tests/upstream.log",
  },
  features: {
    from0: true,
    relative: true,
    outbuf: true,
    infoProgress2: true,
    logFile: true,
    logFileFormat: true,
    compression: ["zlib", "zlibx", "none"],
  },
  license: "GPL-3.0-or-later",
  correspondingSource: `source/rsync-${all.match(/\bversion\s+(\d+(?:\.\d+){1,3})/u)?.[1]}.tar.gz`,
};
await writeFile(
  path.join(directory, "manifest.json"),
  `${JSON.stringify(manifest, null, 2)}\n`,
);
