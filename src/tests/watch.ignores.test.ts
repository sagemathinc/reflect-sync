// tests/watch.ignores.test.ts
//
// pnpm test watch.ignores.test.ts
//
// These tests target the hot watcher + ignore behavior directly.
// They do not use the scheduler, to keep the surface small & fast.

import { HotWatchManager } from "../hotwatch";
import fsp from "node:fs/promises";
import { join } from "node:path";
import os from "node:os";
import { IGNORE_FILE } from "../constants";

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("HotWatchManager + ignores", () => {
  let tmp: string;
  let mgr: HotWatchManager | null = null;
  let seen: Array<{ ev: string; abs: string }> = [];

  const resetLog = () => (seen = []);
  const onHotEvent = (abs: string, ev: any) => {
    seen.push({ ev, abs });
  };

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "ccsync-watch-ignores-"));
  });

  afterEach(async () => {
    if (mgr) {
      await mgr.closeAll();
      mgr = null;
    }
    resetLog();
    // clear directory contents between tests
    await fsp.rm(tmp, { recursive: true, force: true });
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "ccsync-watch-ignores-"));
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("events inside an ignored dir are dropped", async () => {
    // ignore dist/
    await fsp.writeFile(join(tmp, IGNORE_FILE), "dist/\n");

    mgr = new HotWatchManager(tmp, onHotEvent, {
      hotDepth: 2,
      awaitWriteFinish: { stabilityThreshold: 100, pollInterval: 50 },
    });

    // Start watching at root
    await mgr.add("");

    // Create ignored dir & files within
    await fsp.mkdir(join(tmp, "dist", "sub"), { recursive: true });
    await fsp.writeFile(join(tmp, "dist", "a.txt"), "A");
    await fsp.writeFile(join(tmp, "dist", "sub", "b.txt"), "B");

    await wait(300); // allow chokidar to settle

    // We might still see an 'add' for .ccsyncignore depending on timing, but no dist/* events.
    const anyDist = seen.some((e) => e.abs.includes("/dist/"));
    expect(anyDist).toBe(false);
  });

  test("addDir for an ignored dir stops descending (subtree unwatched)", async () => {
    // Set ignore, then create the dir after watchers start.
    await fsp.writeFile(join(tmp, IGNORE_FILE), "logs/\n");

    mgr = new HotWatchManager(tmp, onHotEvent, {
      hotDepth: 2,
      awaitWriteFinish: { stabilityThreshold: 100, pollInterval: 50 },
    });
    await mgr.add("");

    // Create the ignored directory
    await fsp.mkdir(join(tmp, "logs"), { recursive: true });
    await wait(150);

    resetLog();

    // Now create a file *under* the ignored directory; should not emit
    await fsp.writeFile(join(tmp, "logs", "today.txt"), "hello");
    await fsp.mkdir(join(tmp, "logs", "sub"), { recursive: true });
    await fsp.writeFile(join(tmp, "logs", "sub", "deep.txt"), "x");

    await wait(300);

    expect(seen.length).toBe(0);
  });

  test("hot-reload: adding .ccsyncignore live suppresses subsequent events", async () => {
    mgr = new HotWatchManager(tmp, onHotEvent, {
      hotDepth: 2,
      awaitWriteFinish: { stabilityThreshold: 50, pollInterval: 20 },
    });
    // Let chokidar fully initialize
    await mgr.add("");
    await wait(200);

    // Initially allowed
    await fsp.writeFile(join(tmp, "keep.txt"), "1");
    await wait(200);
    const before = seen.length;
    expect(before).toBeGreaterThan(0); // we saw some events

    // Add ignore live
    await fsp.writeFile(join(tmp, IGNORE_FILE), "keep.txt\n");
    await wait(250); // let the ignore watcher reload
    resetLog();

    // Modify the same file; should now be ignored
    await fsp.writeFile(join(tmp, "keep.txt"), "2");
    await wait(250);

    expect(seen.length).toBe(0);
  });

  test("external 'either-side' ignore predicate is honored", async () => {
    // Local side ignores nothing; external says ignore cache/**
    await fsp.writeFile(join(tmp, IGNORE_FILE), "\n");

    mgr = new HotWatchManager(tmp, onHotEvent, {
      hotDepth: 2,
      awaitWriteFinish: { stabilityThreshold: 100, pollInterval: 50 },
      isIgnored: (r, isDir) =>
        r.startsWith("cache/") || r === "cache" || (isDir && r === "cache/"),
    });
    await mgr.add("");

    await fsp.mkdir(join(tmp, "cache"), { recursive: true });
    await fsp.writeFile(join(tmp, "cache", "a.bin"), "x");
    await fsp.writeFile(join(tmp, "cache", "b.bin"), "y");

    await wait(300);

    // No events should be emitted for cache/*
    expect(seen.length).toBe(0);

    // A non-ignored file should still emit
    await fsp.writeFile(join(tmp, "ok.txt"), "ok");
    await wait(200);
    expect(
      seen.some((e) => e.abs.endsWith("/ok.txt") || e.abs.endsWith("\\ok.txt")),
    ).toBe(true);
  });

  test("unignored sibling still emits while ignored dir remains quiet", async () => {
    await fsp.writeFile(join(tmp, IGNORE_FILE), "build/\n");

    mgr = new HotWatchManager(tmp, onHotEvent, {
      hotDepth: 2,
      awaitWriteFinish: { stabilityThreshold: 100, pollInterval: 50 },
    });
    await mgr.add("");

    // Touch ignored and non-ignored siblings
    await fsp.mkdir(join(tmp, "build"), { recursive: true });
    await fsp.writeFile(join(tmp, "build", "artifact.js"), "art");
    await fsp.writeFile(join(tmp, "src.ts"), "src");
    await wait(300);

    const anyBuild = seen.some((e) => e.abs.includes("/build/"));
    const anySrc = seen.some(
      (e) => e.abs.endsWith("/src.ts") || e.abs.endsWith("\\src.ts"),
    );
    expect(anyBuild).toBe(false);
    expect(anySrc).toBe(true);
  });

  test("unlink events inside ignored dir are dropped", async () => {
    await fsp.writeFile(join(tmp, IGNORE_FILE), "out/\n");

    // Precreate content (before watcher)
    await fsp.mkdir(join(tmp, "out", "sub"), { recursive: true });
    await fsp.writeFile(join(tmp, "out", "sub", "x.txt"), "x");
    await fsp.writeFile(join(tmp, "out", "gone.txt"), "y");

    mgr = new HotWatchManager(tmp, onHotEvent, {
      hotDepth: 3,
      awaitWriteFinish: { stabilityThreshold: 100, pollInterval: 50 },
    });
    await mgr.add("");

    resetLog();

    // Deletes under ignored path should not emit
    await fsp.rm(join(tmp, "out", "gone.txt"), { force: true });
    await fsp.rm(join(tmp, "out", "sub"), { recursive: true, force: true });
    await wait(300);

    expect(seen.length).toBe(0);
  });
});
