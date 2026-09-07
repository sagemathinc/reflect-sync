#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
const report = path.join(os.tmpdir(), `reflect-vitest-${process.pid}.json`);
const command = process.platform === "win32" ? "pnpm.cmd" : "pnpm";
const result = spawnSync(
  command,
  ["exec", "vitest", "run", "--reporter=json", `--outputFile=${report}`],
  { cwd: root, stdio: "inherit", env: process.env },
);

if (result.error) throw result.error;
if (result.status !== 0) process.exit(result.status ?? 1);

try {
  const actual = JSON.parse(await readFile(report, "utf8"));
  const expected = JSON.parse(
    await readFile(path.join(root, "test-expectations.json"), "utf8"),
  );
  const totals = {
    files: actual.testResults.length,
    tests: actual.numTotalTests,
  };
  if (totals.files !== expected.files || totals.tests !== expected.tests) {
    throw new Error(
      `test inventory changed: expected ${expected.files} files/${expected.tests} tests, got ${totals.files} files/${totals.tests} tests; verify the change and update test-expectations.json`,
    );
  }
  console.log(
    `Test inventory verified: ${totals.files} files, ${totals.tests} tests (${actual.numPassedTests} passed, ${actual.numPendingTests} skipped, ${actual.numTodoTests} todo).`,
  );
} finally {
  await rm(report, { force: true });
}
