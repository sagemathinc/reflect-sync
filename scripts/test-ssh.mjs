#!/usr/bin/env node

import { spawnSync } from "node:child_process";

const command = process.platform === "win32" ? "pnpm.cmd" : "pnpm";
const result = spawnSync(
  command,
  ["exec", "vitest", "run", "--project", "ssh"],
  {
    stdio: "inherit",
    env: {
      ...process.env,
      REFLECT_REQUIRE_SSH: "1",
    },
  },
);

if (result.error) throw result.error;
process.exit(result.status ?? 1);
