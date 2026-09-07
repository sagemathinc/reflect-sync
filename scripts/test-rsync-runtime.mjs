#!/usr/bin/env node

import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import path from "node:path";

const platform = process.platform === "win32" ? "windows" : process.platform;
const arch = process.arch === "x64" ? "x64" : process.arch;
const target = `${platform}-${arch}`;
const directory =
  process.argv[2] ??
  path.resolve(import.meta.dirname, "..", "dist", "rsync-runtime", target);
const manifest = JSON.parse(
  await readFile(path.join(directory, "manifest.json"), "utf8"),
);
const executable = path.join(directory, manifest.executable);
const digest = createHash("sha256")
  .update(await readFile(executable))
  .digest("hex");
if (digest !== manifest.executableSha256) {
  throw new Error("rsync runtime executable digest does not match manifest");
}
for (const args of [["--version"], ["--help"]]) {
  const result = spawnSync(executable, args, { encoding: "utf8" });
  if (result.status !== 0) {
    throw new Error(`${executable} ${args.join(" ")} failed: ${result.stderr}`);
  }
}

if (process.platform === "linux") {
  const file = spawnSync("file", [executable], { encoding: "utf8" });
  if (file.status !== 0 || !/statically linked/u.test(file.stdout)) {
    throw new Error(`Linux runtime is not statically linked: ${file.stdout}`);
  }
} else if (process.platform === "darwin") {
  const links = spawnSync("otool", ["-L", executable], { encoding: "utf8" });
  if (links.status !== 0) throw new Error(`otool failed: ${links.stderr}`);
  const unexpected = links.stdout
    .split(/\r?\n/u)
    .slice(1)
    .map((line) => line.trim())
    .filter(Boolean)
    .filter(
      (line) =>
        !line.startsWith("/usr/lib/") && !line.startsWith("/System/Library/"),
    );
  if (unexpected.length) {
    throw new Error(
      `unexpected macOS runtime libraries: ${unexpected.join(", ")}`,
    );
  }
}

for (const required of [
  "licenses/rsync-COPYING",
  "source/build-rsync-runtime.sh",
  `source/rsync-${manifest.rsyncVersion}.tar.gz`,
  "tests/upstream.log",
]) {
  await readFile(path.join(directory, required));
}
