/**
 * pnpm test ssh.remote-watch.test.ts
 *
 * Verifies: scheduler starts with beta as a remote side, and a file created
 * on the remote (beta) arrives locally on alpha via microSync before the next
 * full cycle.
 */

import { ChildProcess, spawn } from "node:child_process";
import fsp from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { countSchedulerCycles, fileExists, waitFor } from "./util";
import { canSshLocalhost } from "./ssh-util";

// Resolve scheduler entrypoint directly to avoid CLI multi-proc trees
const SCHED = path.resolve(__dirname, "../../dist/scheduler.js");

function startSchedulerRemote(opts: {
  alphaRoot: string; // local
  betaRootRemote: string; // path we treat as "remote" (on localhost)
  alphaDb: string;
  betaDb: string;
  baseDb: string;
  prefer?: "alpha" | "beta";
  verbose?: boolean;
}): ChildProcess {
  const args = [
    SCHED,
    "--alpha-root",
    opts.alphaRoot,
    "--beta-root",
    opts.betaRootRemote,
    "--alpha-db",
    opts.alphaDb,
    "--beta-db",
    opts.betaDb,
    "--base-db",
    opts.baseDb,
    "--prefer",
    opts.prefer ?? "alpha",
    "--beta-host",
    "localhost", // mark beta as remote so scheduler uses ssh-based watch/scan
    "--remote-scan-cmd",
    "ccsync scan",
    "--remote-watch-cmd",
    "ccsync watch",
  ];
  if (opts.verbose) args.push("--verbose");

  // Make full cycles slow; micro-sync snappy for the test
  const env = {
    ...process.env,
    SCHED_MIN_MS: "5000",
    SCHED_MAX_MS: "5000",
    SCHED_MAX_BACKOFF_MS: "5000",
    SCHED_JITTER_MS: "0",
    MICRO_DEBOUNCE_MS: "50",
    COOLDOWN_MS: "50",
    SHALLOW_DEPTH: "1",
    HOT_DEPTH: "1",
    MAX_HOT_WATCHERS: "32",
  };

  return spawn(process.execPath, args, {
    stdio: ["ignore", "ignore", "inherit"],
    env,
  });
}

async function stopScheduler(p: ChildProcess) {
  if (!p.pid) return;
  try {
    process.kill(p.pid, "SIGINT");
  } catch {}
  await Promise.race([
    new Promise<void>((resolve) => p.once("exit", () => resolve())),
    new Promise<void>((resolve) => setTimeout(resolve, 1500)),
  ]);
  if (p.pid) {
    try {
      process.kill(p.pid, "SIGKILL");
    } catch {}
  }
}

describe("SSH remote watch → microSync", () => {
  let tmp: string;
  let alphaRoot: string, betaRootRemote: string;
  let alphaDb: string, betaDb: string, baseDb: string;

  beforeAll(async () => {
    // We still require ssh localhost because the scheduler uses ssh to run the
    // remote watch agent, but we won't use ssh to mutate the filesystem.
    const ok = await canSshLocalhost();
    if (!ok) {
      // @ts-ignore
      pending("ssh localhost unavailable; skipping remote-watch test");
    }

    tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "ccsync-ssh-watch-"));
    alphaRoot = path.join(tmp, "alpha-local");
    betaRootRemote = path.join(tmp, "beta-remote");
    alphaDb = path.join(tmp, "alpha.db");
    betaDb = path.join(tmp, "beta.db");
    baseDb = path.join(tmp, "base.db");

    await fsp.mkdir(alphaRoot, { recursive: true });
    await fsp.mkdir(betaRootRemote, { recursive: true });
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("file created on remote (beta) appears locally on alpha via microSync", async () => {
    const child = startSchedulerRemote({
      alphaRoot,
      betaRootRemote,
      alphaDb,
      betaDb,
      baseDb,
      prefer: "alpha",
      verbose: false,
    });

    try {
      // Wait for first full cycle so watcher + remote agent are armed
      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= 1,
        10_000,
        100,
      );

      // Create a file under the "remote" path directly on the filesystem.
      // The remote watch agent (via ssh on localhost) should pick this up.
      const remoteFile = path.join(betaRootRemote, "hello.txt");
      await fsp.writeFile(remoteFile, "hi\n", "utf8");

      // Expect it to arrive on alpha quickly via microSync
      const localFile = path.join(alphaRoot, "hello.txt");
      await waitFor(
        () => fileExists(localFile),
        (ok) => ok === true,
        2500,
        50,
      );
    } finally {
      await stopScheduler(child);
    }
  }, 20_000);
});
