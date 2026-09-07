#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import path from "node:path";
import { verifyReleaseDirectory } from "./release-verification.mjs";

const root = path.resolve(import.meta.dirname, "..");
const directory = path.resolve(
  process.argv[2] ?? path.join(root, "dist", "release"),
);
const packageMetadata = JSON.parse(
  await readFile(path.join(root, "package.json"), "utf8"),
);
const runtimeConfig = JSON.parse(
  await readFile(path.join(root, "runtime", "rsync", "runtime.json"), "utf8"),
);
await verifyReleaseDirectory(directory, {
  version: packageMetadata.version,
  runtimeVersion: runtimeConfig.runtimeVersion,
});
console.log(`release candidate verified: ${directory}`);
