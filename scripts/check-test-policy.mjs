#!/usr/bin/env node

import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

const testsRoot = path.resolve(import.meta.dirname, "../src/tests");
const violations = [];

async function visit(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const file = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      await visit(file);
    } else if (entry.isFile() && entry.name.endsWith(".test.ts")) {
      const source = await readFile(file, "utf8");
      if (/\b(?:describe|it|test)\.only\s*\(/u.test(source)) {
        violations.push(`${path.relative(testsRoot, file)} contains .only`);
      }
      if (/\b(?:it|test)\.skip\s*\(/u.test(source)) {
        violations.push(
          `${path.relative(testsRoot, file)} uses .skip; use .todo or an explicit capability suite`,
        );
      }
    }
  }
}

await visit(testsRoot);
if (violations.length) {
  throw new Error(`test policy violations:\n${violations.join("\n")}`);
}
