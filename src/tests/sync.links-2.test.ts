// tests/sync.links-2.test.ts
//
// pnpm test sync.links-2.test.ts
//
// More edge case symlink-focused scenarios

import { sync, dirExists, fileExists, mkCase } from "./util";
import fsp from "node:fs/promises";
import { join } from "node:path";
import os from "node:os";
import { linkExists, isRegularFile, readlinkTarget } from "./links-util";

describe("reflex-sync: more symlink edge case tests", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "rfsync-test-links-"));
  });

  afterAll(async () => {
    // comment out to inspect failures
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("LWW epsilon tie: both sides change ~simultaneously → prefer side wins (alpha)", async () => {
    const r = await mkCase(tmp, "t-link-epsilon-tie");
    const aDir = join(r.aRoot, "e");
    const bDir = join(r.bRoot, "e");
    const aFile = join(aDir, "x");
    const bPath = join(bDir, "x");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.writeFile(aFile, "seed");
    await sync(r, "alpha"); // seed

    // Beta turns x into a symlink
    await fsp.writeFile(join(bDir, "y.txt"), "Y");
    await fsp.rm(bPath).catch(() => {});
    await fsp.symlink("y.txt", bPath);

    // Alpha also changes x (bytes change) within epsilon window
    await new Promise((r2) => setTimeout(r2, 25));
    await fsp.writeFile(aFile, "A1"); // hash change

    // Use a large epsilon so it's treated as a tie, then prefer alpha
    await sync(r, "alpha", false, undefined);

    await expect(isRegularFile(bPath)).resolves.toBe(true);
    await expect(fsp.readFile(bPath, "utf8")).resolves.toBe("A1");
  });

  test("LWW no tie: beta’s change is clearly newer → beta wins even if prefer alpha", async () => {
    const r = await mkCase(tmp, "t-link-lww-beta-newer");
    const aDir = join(r.aRoot, "n");
    const bDir = join(r.bRoot, "n");
    const aFile = join(aDir, "x");
    const bPath = join(bDir, "x");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.writeFile(aFile, "seed");
    await sync(r, "alpha");

    // Alpha changes first (older)
    await fsp.writeFile(aFile, "A2");

    // Ensure a new mtime tick before beta’s change
    await new Promise((r2) => setTimeout(r2, 1200));

    // Beta turns x into a symlink to z.txt (newer)
    const z = join(bDir, "z.txt");
    await fsp.writeFile(z, "Z");
    await fsp.rm(bPath).catch(() => {});
    await fsp.symlink("z.txt", bPath);

    // Try to bump the *symlink* mtime further into the future
    const future = new Date(Date.now() + 5000);
    await fsp.lutimes(bPath, future, future);

    await sync(r, "alpha", false, undefined);

    // Beta should win: keep symlink
    await expect(linkExists(bPath)).resolves.toBe(true);
    await expect(readlinkTarget(bPath)).resolves.toBe("z.txt");
  });

  test("Delete on alpha vs make symlink on beta (both changed) → prefer alpha deletes", async () => {
    const r = await mkCase(tmp, "t-link-del-vs-create-alpha-pref");
    const aDir = join(r.aRoot, "r");
    const bDir = join(r.bRoot, "r");
    const aFile = join(aDir, "x");
    const bPath = join(bDir, "x");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.writeFile(aFile, "seed");
    await sync(r, "alpha"); // seed

    // Both sides change:
    await fsp.rm(aFile); // alpha deletes
    await fsp.writeFile(join(bDir, "t.txt"), "T");
    await fsp.rm(bPath).catch(() => {});
    await fsp.symlink("t.txt", bPath); // beta creates link

    // Small sleep to avoid same-tick mtimes on coarse filesystems
    await new Promise((r2) => setTimeout(r2, 1100));
    await sync(r, "alpha", false, undefined);

    await expect(fileExists(join(aDir, "x"))).resolves.toBe(false);
    await expect(linkExists(bPath)).resolves.toBe(false);
  });

  test("create directory that is target of symlink, sync, move directory, sync", async () => {
    const r = await mkCase(tmp, "link-to-dir-moved");
    await fsp.mkdir(join(r.aRoot, "x"));
    await fsp.symlink(join(r.aRoot, "x"), join(r.aRoot, "x.link"));
    await sync(r, "alpha");

    await expect(linkExists(join(r.bRoot, "x.link")));
    await expect(dirExists(join(r.bRoot, "x")));

    // move the directory
    await fsp.rename(join(r.bRoot, "x"), join(r.bRoot, "x2"));
    await sync(r, "alpha");

    expect(await fsp.readdir(r.aRoot)).toEqual(["x.link", "x2"]);
    expect(await fsp.readdir(r.bRoot)).toEqual(["x.link", "x2"]);
    await expect(linkExists(join(r.aRoot, "x.link")));
    await expect(linkExists(join(r.bRoot, "x.link")));
  });
});
