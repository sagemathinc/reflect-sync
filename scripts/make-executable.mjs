#!/usr/bin/env node

import { chmod } from "node:fs/promises";

if (process.platform !== "win32") {
  const file = process.argv[2];
  if (!file) throw new Error("usage: make-executable.mjs <path>");
  await chmod(file, 0o755);
}
