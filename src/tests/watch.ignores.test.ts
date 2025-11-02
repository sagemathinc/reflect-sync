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
import { wait } from "./util";

describe("HotWatchManager + ignores", () => {
  let tmp: string;
  let mgr: HotWatchManager | null = null;
  let seen: Array<{ ev: string; abs: string }> = [];

  const resetLog = () => (seen = []);
  const onHotEvent = (abs: string, ev: any) => {
    seen.push({ ev, abs });
  };

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "rfsync-watch-ignores-"));
  });

  afterEach(async () => {
    if (mgr) {
      await mgr.closeAll();
      mgr = null;
    }
    resetLog();
    await fsp.rm(tmp, { recursive: true, force: true });
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "rfsync-watch-ignores-"));
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("events inside an ignored dir are dropped", async () => {
    mgr = new HotWatchManager(tmp, onHotEvent, {
      hotDepth: 2,
      awaitWriteFinish: { stabilityThreshold: 100, pollInterval: 50 },
      ignoreRules: ["dist/"],
    });

    await mgr.add("");

    await fsp.mkdir(join(tmp, "dist", "sub"), { recursive: true });
    await fsp.writeFile(join(tmp, "dist", "a.txt"), "A");
    await fsp.writeFile(join(tmp, "dist", "sub", "b.txt"), "B");

    await wait(300);

    const anyDist = seen.some((e) => e.abs.includes("/dist/"));
    expect(anyDist).toBe(false);
  });

  test("addDir for an ignored dir stops descending (subtree unwatched)", async () => {
    mgr = new HotWatchManager(tmp, onHotEvent, {
      hotDepth: 2,
      awaitWriteFinish: { stabilityThreshold: 100, pollInterval: 50 },
      ignoreRules: ["logs/"],
    });
    await mgr.add("");

    await fsp.mkdir(join(tmp, "logs"), { recursive: true });
    await wait(150);

    resetLog();

    await fsp.writeFile(join(tmp, "logs", "today.txt"), "hello");
    await fsp.mkdir(join(tmp, "logs", "sub"), { recursive: true });
    await fsp.writeFile(join(tmp, "logs", "sub", "deep.txt"), "x");

    await wait(300);

    expect(seen.length).toBe(0);
  });

  test("setIgnoreRules live suppresses subsequent events", async () => {
    mgr = new HotWatchManager(tmp, onHotEvent, {
      hotDepth: 2,
      awaitWriteFinish: { stabilityThreshold: 50, pollInterval: 20 },
    });
    await mgr.add("");
    await wait(200);

    await fsp.writeFile(join(tmp, "keep.txt"), "1");
    await wait(200);
    expect(seen.length).toBeGreaterThan(0);

    mgr.setIgnoreRules(["keep.txt"]);
    resetLog();

    await fsp.writeFile(join(tmp, "keep.txt"), "2");
    await wait(200);

    expect(seen.length).toBe(0);
  });

  test("external 'either-side' ignore predicate is honored", async () => {
    mgr = new HotWatchManager(tmp, onHotEvent, {
      hotDepth: 2,
      awaitWriteFinish: { stabilityThreshold: 100, pollInterval: 50 },
      ignoreRules: [],
      isIgnored: (r, isDir) =>
        r.startsWith("cache/") || r === "cache" || (isDir && r === "cache/"),
    });
    await mgr.add("");

    await fsp.mkdir(join(tmp, "cache"), { recursive: true });
    await fsp.writeFile(join(tmp, "cache", "a.bin"), "x");
    await fsp.writeFile(join(tmp, "cache", "b.bin"), "y");

    await wait(300);
    expect(seen.length).toBe(0);

    await fsp.writeFile(join(tmp, "ok.txt"), "ok");
    await wait(200);
    expect(
      seen.some((e) => e.abs.endsWith("/ok.txt") || e.abs.endsWith("\\ok.txt")),
    ).toBe(true);
  });

  test("unignored sibling still emits while ignored dir remains quiet", async () => {
    mgr = new HotWatchManager(tmp, onHotEvent, {
      hotDepth: 2,
      awaitWriteFinish: { stabilityThreshold: 100, pollInterval: 50 },
      ignoreRules: ["build/"],
    });
    await mgr.add("");

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
    await fsp.mkdir(join(tmp, "out", "sub"), { recursive: true });
    await fsp.writeFile(join(tmp, "out", "sub", "x.txt"), "x");
    await fsp.writeFile(join(tmp, "out", "gone.txt"), "y");

    mgr = new HotWatchManager(tmp, onHotEvent, {
      hotDepth: 3,
      awaitWriteFinish: { stabilityThreshold: 100, pollInterval: 50 },
      ignoreRules: ["out/"],
    });
    await mgr.add("");

    resetLog();

    await fsp.rm(join(tmp, "out", "gone.txt"), { force: true });
    await fsp.rm(join(tmp, "out", "sub"), { recursive: true, force: true });
    await wait(300);

    expect(seen.length).toBe(0);
  });
});
