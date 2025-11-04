#!/usr/bin/env node
// Re-exec with --no-warnings, then run the real CLI.
// Keeps other execArgv (e.g. --inspect) and user args intact.

import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { execPath, execArgv, argv, env, exit } from "node:process";
import path from "node:path";
import fs from "node:fs";

const bundlePath = fileURLToPath(new URL("../dist/bundle.mjs", import.meta.url));
const cliFallback = fileURLToPath(new URL("../dist/cli.js", import.meta.url));
const targetPath = fileExistsSync(bundlePath) ? bundlePath : cliFallback;
const hasNoWarn =
  execArgv.includes("--no-warnings") ||
  String(env.NODE_OPTIONS || "").includes("--no-warnings");

if (!hasNoWarn) {
  const child = spawn(
    execPath,
    ["--no-warnings", ...execArgv, targetPath, ...argv.slice(2)],
    {
      stdio: "inherit",
      env, // preserve env
    },
  );
  child.on("exit", (code, signal) => {
    if (signal) process.kill(process.pid, signal);
    else exit(code ?? 0);
  });
} else {
  // Already running with --no-warnings; just load the CLI.
  await import(pathToFileURL(targetPath).href);
}

function pathToFileURL(p) {
  const u = new URL("file://");
  // node >=16 has URL.pathToFileURL but doing inline to avoid extra import
  u.pathname = path.resolve(p).split(path.sep).join("/");
  return u;
}

function fileExistsSync(p) {
  try {
    fs.statSync(p);
    return true;
  } catch {
    return false;
  }
}
