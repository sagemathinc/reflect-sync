/**
 * pnpm test ssh.remote-sync.test.ts
 *
 * Verifies: scheduler starts with beta as a remote side, and a file created
 * on the remote (beta) arrives locally on alpha via microSync before the next
 * full cycle.
 */

import { ChildProcess, spawn } from "node:child_process";
import fsp from "node:fs/promises";
import { join, resolve } from "node:path";
import os from "node:os";
import { countSchedulerCycles, dirExists, linkExists, waitFor } from "./util";
import { canSshLocalhost } from "./ssh-util";
//import { wait } from "./util";

// Resolve scheduler entrypoint directly to avoid CLI multi-proc trees
const SCHED = resolve(__dirname, "../../dist/scheduler.js");

function startSchedulerRemote(opts: {
  alphaRoot: string; // local
  betaRootRemote: string; // path we treat as "remote" (on localhost)
  alphaDb: string;
  betaDb: string;
  baseDb: string;
  prefer?: "alpha" | "beta";
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
    "--disable-hot-watch",
    "--beta-host",
    "localhost", // mark beta as remote so scheduler uses ssh-based watch/scan
  ];
  // Make full cycles very FAST
  const env = {
    ...process.env,
    SCHED_MIN_MS: "100",
    SCHED_MAX_MS: "200",
    SCHED_MAX_BACKOFF_MS: "50",
    SCHED_JITTER_MS: "0",
    MICRO_DEBOUNCE_MS: "0",
    COOLDOWN_MS: "10",
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
    new Promise<void>((resolve) => setTimeout(resolve, 500)),
  ]);
  if (p.pid) {
    try {
      process.kill(p.pid, "SIGKILL");
    } catch {}
  }
}

describe("SSH remote sync", () => {
  let tmp: string = "";
  let alphaRoot: string, betaRootRemote: string;
  let alphaDb: string, betaDb: string, baseDb: string;

  beforeAll(async () => {
    // We still require ssh localhost because the scheduler uses ssh to run the
    // remote watch agent, but we won't use ssh to mutate the filesystem.
    const ok = await canSshLocalhost();
    if (!ok) {
      throw Error("ssh localhost unavailable; skipping remote-watch test");
    }

    tmp = await fsp.mkdtemp(join(os.tmpdir(), "rfsync-ssh-watch-"));
    alphaRoot = join(tmp, "alpha-local");
    betaRootRemote = join(tmp, "beta-remote");
    alphaDb = join(tmp, "alpha.db");
    betaDb = join(tmp, "beta.db");
    baseDb = join(tmp, "base.db");

    await fsp.mkdir(alphaRoot, { recursive: true });
    await fsp.mkdir(betaRootRemote, { recursive: true });
  });

  afterAll(async () => {
    if (tmp) {
      await fsp.rm(tmp, { recursive: true, force: true });
    }
  });

  test("create directory that is target of symlink, sync, move directory, sync", async () => {
    const child = startSchedulerRemote({
      alphaRoot,
      betaRootRemote,
      alphaDb,
      betaDb,
      baseDb,
      prefer: "alpha",
    });

    try {
      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= 1,
        15_000,
        10,
      );
      await fsp.mkdir(join(alphaRoot, "x"));
      await fsp.symlink("x", join(alphaRoot, "x.link"));

      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= 2,
        15_000,
        10,
      );

      await expect(linkExists(join(betaRootRemote, "x.link")));
      await expect(dirExists(join(betaRootRemote, "x")));

      // move the directory
      await fsp.rename(join(betaRootRemote, "x"), join(betaRootRemote, "x2"));

      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= 3,
        15_000,
        10,
      );

      expect(await fsp.readdir(alphaRoot)).toEqual(["x.link", "x2"]);
      expect(await fsp.readdir(betaRootRemote)).toEqual(["x.link", "x2"]);
      await expect(linkExists(join(alphaRoot, "x.link")));
      await expect(linkExists(join(betaRootRemote, "x.link")));
    } finally {
      await stopScheduler(child);
    }
  }, 20_000);
});
