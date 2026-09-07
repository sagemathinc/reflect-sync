#!/usr/bin/env node

import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
const packageMetadata = JSON.parse(
  await readFile(path.join(root, "package.json"), "utf8"),
);
const rsyncRuntime = JSON.parse(
  await readFile(path.join(root, "runtime", "rsync", "runtime.json"), "utf8"),
);
const nodeVersion = process.versions.node;
if (Number(nodeVersion.split(".")[0]) !== 26) {
  throw new Error(
    `Node 26 is required to build SEA artifacts; running ${nodeVersion}`,
  );
}
const command = process.platform === "win32" ? "pnpm.cmd" : "pnpm";

function run(executable, args) {
  const result = spawnSync(executable, args, { cwd: root, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${executable} ${args.join(" ")} exited ${result.status}`);
  }
}

run(command, ["bundle"]);

const platform = process.platform === "win32" ? "windows" : process.platform;
const arch = process.arch === "x64" ? "x64" : process.arch;
const target = `${platform}-${arch}`;
const suffix = process.platform === "win32" ? ".exe" : "";
const outputDirectory = path.join(root, "dist", "sea");
const output = path.join(outputDirectory, `reflect-sync-${target}${suffix}`);
await mkdir(outputDirectory, { recursive: true });

const baseConfig = JSON.parse(
  await readFile(path.join(root, "sea.config.json"), "utf8"),
);
const generatedConfig = path.join(outputDirectory, `sea-config-${target}.json`);
await writeFile(
  generatedConfig,
  `${JSON.stringify({ ...baseConfig, output: path.relative(root, output) }, null, 2)}\n`,
);
run(process.execPath, ["--build-sea", generatedConfig]);
if (process.platform !== "win32") await chmod(output, 0o755);
if (process.platform === "darwin") {
  // Mach-O binaries cannot launch after SEA construction until the mutated
  // executable is signed. Ad-hoc signing makes internal CI artifacts runnable;
  // the later release trust job replaces this with Developer ID signing.
  run("codesign", ["--force", "--sign", "-", output]);
}

const bytes = await readFile(output);
const gitCommitResult = spawnSync("git", ["rev-parse", "HEAD"], {
  cwd: root,
  encoding: "utf8",
});
const gitStatusResult = spawnSync("git", ["status", "--porcelain"], {
  cwd: root,
  encoding: "utf8",
});
const gitCommit =
  process.env.REFLECT_BUILD_GIT_COMMIT ??
  process.env.GITHUB_SHA ??
  (gitCommitResult.status === 0 ? gitCommitResult.stdout.trim() : undefined);
if (!gitCommit) {
  throw new Error("unable to identify SEA source revision");
}
const sourceDirty = process.env.REFLECT_BUILD_SOURCE_DIRTY
  ? process.env.REFLECT_BUILD_SOURCE_DIRTY === "1"
  : gitStatusResult.status === 0
    ? Boolean(gitStatusResult.stdout.trim())
    : false;
const buildInfo = {
  schemaVersion: 1,
  packageVersion: packageMetadata.version,
  target,
  file: path.basename(output),
  nodeVersion,
  gitCommit,
  sourceDirty,
  agentCompatibility: {
    scheme: "exact-package-version",
    version: packageMetadata.version,
  },
  rsyncRuntimeVersion: rsyncRuntime.runtimeVersion,
  minimumRuntime:
    process.platform === "linux"
      ? "glibc >= 2.28 with libatomic"
      : process.platform === "darwin"
        ? "macOS >= 13.5"
        : "experimental",
  supportTier: ["linux-x64", "linux-arm64", "darwin-arm64"].includes(target)
    ? "supported"
    : "experimental",
  workflow: process.env.GITHUB_WORKFLOW
    ? {
        name: process.env.GITHUB_WORKFLOW,
        runId: process.env.GITHUB_RUN_ID,
        runAttempt: process.env.GITHUB_RUN_ATTEMPT,
        repository: process.env.GITHUB_REPOSITORY,
      }
    : null,
  sha256: createHash("sha256").update(bytes).digest("hex"),
  size: bytes.length,
};
await writeFile(
  path.join(outputDirectory, `build-info-${target}.json`),
  `${JSON.stringify(buildInfo, null, 2)}\n`,
);
console.log(`Built ${output} (${bytes.length} bytes)`);
