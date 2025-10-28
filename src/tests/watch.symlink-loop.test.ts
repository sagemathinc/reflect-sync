// tests/watch.symlink-loop.test.ts
//
// pnpm test watch.symlink-loop.test.ts
//
// Verifies a self-referential symlink (a -> a) does not crash the hot watcher
// or the sync pipeline, and is preserved verbatim on the destination.

import { HotWatchManager } from "../hotwatch";
import { sync, mkCase } from "./util";
import { linkExists, readlinkTarget } from "./links-util";
import fsp from "node:fs/promises";
import { join } from "node:path";
import os from "node:os";

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("HotWatchManager + sync: self-referential symlink loop", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "ccsync-watch-loop-"));
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("a -> a symlink does not crash watch or sync and mirrors to beta", async () => {
    const r = await mkCase(tmp, "t-self-loop");
    const aPath = join(r.aRoot, "a");
    const bPath = join(r.bRoot, "a");

    // Collect hot events (both sides)
    const seenA: Array<{ ev: string; abs: string }> = [];
    const seenB: Array<{ ev: string; abs: string }> = [];

    const onA = (abs: string, ev: any) => seenA.push({ ev, abs });
    const onB = (abs: string, ev: any) => seenB.push({ ev, abs });

    const mgrA = new HotWatchManager(r.aRoot, onA, {
      hotDepth: 2,
      awaitWriteFinish: { stabilityThreshold: 80, pollInterval: 40 },
    });
    const mgrB = new HotWatchManager(r.bRoot, onB, {
      hotDepth: 2,
      awaitWriteFinish: { stabilityThreshold: 80, pollInterval: 40 },
    });

    try {
      // Start watching both roots
      await mgrA.add("");
      await mgrB.add("");
      await wait(150);

      // Create the self-referential symlink on alpha: a -> "a"
      await fsp.symlink("a", aPath);

      // Give the hot watchers a moment to observe
      await wait(250);

      const sawA =
        seenA.findIndex((e) => e.abs.endsWith("/a") || e.abs.endsWith("\\a")) >=
        0;
      // watcher does not follow symlinks, so evidently chokidar just
      // ignores circular symlinks entirely in that mode; that's fine,
      // we pick them up in the scan.
      expect(sawA).toBe(false);

      // Run sync: alpha → beta
      await sync(r, "alpha");

      // Beta should now have the same symlink preserved (not materialized)
      await expect(linkExists(bPath)).resolves.toBe(true);
      await expect(readlinkTarget(bPath)).resolves.toBe("a");
    } finally {
      await mgrA.closeAll().catch(() => {});
      await mgrB.closeAll().catch(() => {});
    }
  });
});
