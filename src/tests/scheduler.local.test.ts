/**
 * pnpm test scheduler.local.test.ts
 *
 * Verifies: scheduler starts, completes an initial full cycle, and
 * restricted hot-sync cycles mirror a newly-written file from alpha -> beta quickly,
 * well before the next scheduled full cycle.
 */

import { ChildProcess, spawn } from "node:child_process";
import fsp from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { countSchedulerCycles, fileExists, waitFor } from "./util";
import { argsJoin } from "../remote.js";

// Resolve SCHED once (safer than relying on PATH)
const SCHED = path.resolve(__dirname, "../../dist/scheduler.js");

function startScheduler(opts: {
  alphaRoot: string;
  betaRoot: string;
  alphaDb: string;
  betaDb: string;
  baseDb: string;
  prefer?: "alpha" | "beta";
  env?: NodeJS.ProcessEnv;
  extraArgs?: string[];
}): ChildProcess {
  const args = [
    SCHED,
    "--alpha-root",
    opts.alphaRoot,
    "--beta-root",
    opts.betaRoot,
    "--alpha-db",
    opts.alphaDb,
    "--beta-db",
    opts.betaDb,
    "--base-db",
    opts.baseDb,
    "--prefer",
    opts.prefer ?? "alpha",
  ];
  if (opts.extraArgs?.length) {
    args.push(...opts.extraArgs);
  }

  const env = { ...process.env };
  if (env.DEBUG_TESTS) {
    env.REFLECT_DISABLE_LOG_ECHO = "";
    console.log(`${process.execPath} ${argsJoin(args)}`);
  }

  // Use node to run the ESM CLI directly
  const child = spawn(process.execPath, args, {
    stdio: process.env.DEBUG_TESTS
      ? ["inherit", "inherit", "inherit"]
      : ["ignore", "inherit", "inherit"],
    env: {
      ...env,
      // Make the loop slow, but hot-sync fast.
      SCHED_MIN_MS: "5000",
      SCHED_MAX_MS: "5000",
      SCHED_MAX_BACKOFF_MS: "5000",
      SCHED_JITTER_MS: "0",
      HOT_DEBOUNCE_MS: "50",
      COOLDOWN_MS: "50",
      SHALLOW_DEPTH: "1",
      HOT_DEPTH: "1",
      MAX_HOT_WATCHERS: "32",
      ...(opts.env || {}),
    },
  });

  return child;
}

async function stopScheduler(p: ChildProcess) {
  if (!p.pid) return;
  p.kill("SIGINT");
  // give it a moment to cleanly exit
  const done = new Promise<void>((resolve) => p.once("exit", () => resolve()));
  const race = Promise.race([
    done,
    new Promise<void>((r) => setTimeout(r, 500)),
  ]);
  await race;
  // hard kill if still around
  const stillAlive = (() => {
    try {
      process.kill(p.pid!, 0);
      return true;
    } catch {
      return false;
    }
  })();
  if (stillAlive) {
    try {
      process.kill(p.pid!, "SIGKILL");
    } catch {}
  }
}

describe("scheduler (local watchers + hot-sync)", () => {
  let tmp: string;
  let alphaRoot: string, betaRoot: string;
  let alphaDb: string, betaDb: string, baseDb: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "ccsync-sched-"));
    alphaRoot = path.join(tmp, "alpha");
    betaRoot = path.join(tmp, "beta");
    alphaDb = path.join(tmp, "alpha.db");
    betaDb = path.join(tmp, "beta.db");
    baseDb = path.join(tmp, "base.db");
    await fsp.mkdir(alphaRoot, { recursive: true });
    await fsp.mkdir(betaRoot, { recursive: true });
  });

  beforeEach(async () => {
    await fsp.rm(alphaRoot, { recursive: true, force: true });
    await fsp.rm(betaRoot, { recursive: true, force: true });
    await fsp.mkdir(alphaRoot, { recursive: true });
    await fsp.mkdir(betaRoot, { recursive: true });
    await fsp.rm(alphaDb, { force: true });
    await fsp.rm(betaDb, { force: true });
    await fsp.rm(baseDb, { force: true });
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("realtime: new file in alpha appears in beta before next full cycle", async () => {
    const child = startScheduler({
      alphaRoot,
      betaRoot,
      alphaDb,
      betaDb,
      baseDb,
      prefer: "alpha",
      extraArgs: ["--disable-full-cycle"],
    });

    try {
      // make sure we're right after a sync cycle, so hot-sync has to pick up our change.
      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= 1,
        10_000,
        100,
      );
      // wait for chokidar to finish its initial crawl so we don't miss the event
      await new Promise((resolve) => setTimeout(resolve, 3500));
      // Create a file under alpha
      const aFile = path.join(alphaRoot, "hello.txt");
      await fsp.mkdir(path.dirname(aFile), { recursive: true });
      await fsp.writeFile(aFile, "hi\n", "utf8");

      const bFile = path.join(betaRoot, "hello.txt");
      await waitFor(
        async () => await fileExists(bFile),
        (ok) => ok === true,
        4500,
        50,
      );
    } finally {
      await stopScheduler(child);
    }
  }, 20_000);

  test("realtime: modify in beta mirrors back to alpha when prefer=alpha still set", async () => {
    // prefer doesn't affect one-sided changes; just sanity-check reverse path.
    const child = startScheduler({
      alphaRoot,
      betaRoot,
      alphaDb,
      betaDb,
      baseDb,
      prefer: "alpha",
    });
    // make sure we're right after a sync cycle, so hot-sync has to pick up our change.
    await waitFor(
      () => countSchedulerCycles(baseDb),
      (n) => n >= 1,
      10_000,
      100,
    );
    // wait for chokidar to settle so we don't miss the event
    await new Promise((resolve) => setTimeout(resolve, 1500));

    try {
      const bFile = path.join(betaRoot, "note.txt");
      await fsp.writeFile(bFile, "from beta\n", "utf8");

      const aFile = path.join(alphaRoot, "note.txt");

      await waitFor(
        async () => await fileExists(aFile),
        (ok) => ok === true,
        2000,
        50,
      );

      expect(await fsp.readFile(aFile, "utf8")).toBe("from beta\n");
    } finally {
      await stopScheduler(child);
    }
  }, 20_000);

  test("disable full cycle prevents additional automatic cycles", async () => {
    const child = startScheduler({
      alphaRoot,
      betaRoot,
      alphaDb,
      betaDb,
      baseDb,
      prefer: "alpha",
      extraArgs: ["--disable-full-cycle"],
    });

    try {
      const baseline = countSchedulerCycles(baseDb);
      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= baseline + 1,
        10_000,
        100,
      );
      const cyclesAfterFirst = countSchedulerCycles(baseDb);
      expect(cyclesAfterFirst).toBe(baseline + 1);

      await new Promise((resolve) => setTimeout(resolve, 7000));
      const cyclesAfterWait = countSchedulerCycles(baseDb);
      expect(cyclesAfterWait).toBe(cyclesAfterFirst);
    } finally {
      await stopScheduler(child);
    }
  }, 20_000);

  test("disable hot sync defers propagation until the next full cycle", async () => {
    const child = startScheduler({
      alphaRoot,
      betaRoot,
      alphaDb,
      betaDb,
      baseDb,
      prefer: "alpha",
      extraArgs: ["--disable-hot-sync"],
    });

    try {
      const baseline = countSchedulerCycles(baseDb);
      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= baseline + 1,
        10_000,
        100,
      );

      const aFile = path.join(alphaRoot, "hot-disabled.txt");
      const bFile = path.join(betaRoot, "hot-disabled.txt");
      await fsp.mkdir(path.dirname(aFile), { recursive: true });
      await fsp.rm(aFile, { force: true });
      await fsp.rm(bFile, { force: true });

      await fsp.writeFile(aFile, "test\n", "utf8");

      await new Promise((resolve) => setTimeout(resolve, 1000));
      expect(await fileExists(bFile)).toBe(false);

      await waitFor(
        async () => await fileExists(bFile),
        (ok) => ok === true,
        8000,
        100,
      );
    } finally {
      await stopScheduler(child);
    }
  }, 25_000);
});
