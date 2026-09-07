#!/usr/bin/env node

import { rm } from "node:fs/promises";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
const generated = ["dist", "bundle", "coverage"];
if (process.argv.includes("--all")) generated.push("node_modules");

for (const relative of generated) {
  await rm(path.join(root, relative), { recursive: true, force: true });
}
