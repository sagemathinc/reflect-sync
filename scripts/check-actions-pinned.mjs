#!/usr/bin/env node

import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

const workflows = path.resolve(import.meta.dirname, "../.github/workflows");
const violations = [];

for (const entry of await readdir(workflows, { withFileTypes: true })) {
  if (!entry.isFile() || !/\.ya?ml$/u.test(entry.name)) continue;
  const source = await readFile(path.join(workflows, entry.name), "utf8");
  for (const match of source.matchAll(/^\s*-?\s*uses:\s*([^\s#]+)/gmu)) {
    const action = match[1];
    if (action.startsWith("./") || action.startsWith("docker://")) continue;
    if (!/@[0-9a-f]{40}$/u.test(action)) {
      violations.push(`${entry.name}: ${action} is not pinned to a commit SHA`);
    }
  }
}

if (violations.length) {
  throw new Error(`workflow policy violations:\n${violations.join("\n")}`);
}
