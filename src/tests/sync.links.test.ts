// tests/sync.links.test.ts
//
// pnpm test sync.links.test.ts
//
// Symlink-focused scenarios: creation, deletion, target changes, broken links,
// file<->link conflicts (alpha/beta preference), symlink-to-dir, loops, chains,
// and absolute-target preservation.

import { sync, fileExists, mkCase } from "./util";
import fsp from "node:fs/promises";
import os from "node:os";
import { linkExists, isRegularFile, readlinkTarget } from "./links-util";
import { join, dirname } from "node:path";

describe("ccsync: symlink semantics", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "ccsync-test-links-"));
  });

  afterAll(async () => {
    // comment out to inspect failures
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("symlink to file: create propagates alpha→beta", async () => {
    const r = await mkCase(tmp, "t-link-create");
    const aDir = join(r.aRoot, "docs");
    const bDir = join(r.bRoot, "docs");
    const aFile = join(aDir, "a.txt");
    const aLink = join(aDir, "a.lnk");
    const bFile = join(bDir, "a.txt");
    const bLink = join(bDir, "a.lnk");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.writeFile(aFile, "hello");
    // Relative link target — should be preserved verbatim
    await fsp.symlink("a.txt", aLink);

    await sync(r, "alpha");

    await expect(linkExists(bLink)).resolves.toBe(true);
    await expect(readlinkTarget(bLink)).resolves.toBe("a.txt");
    // Dereference works end-to-end
    await expect(fsp.readFile(bLink, "utf8")).resolves.toBe("hello");
    await expect(fileExists(bFile)).resolves.toBe(true);
  });

  test("broken symlink propagation alpha→beta", async () => {
    const r = await mkCase(tmp, "t-link-broken");
    const aDir = join(r.aRoot, "d");
    const bDir = join(r.bRoot, "d");
    const aLink = join(aDir, "dangling");
    const bLink = join(bDir, "dangling");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.symlink("no-such-file", aLink);

    await sync(r, "alpha");

    await expect(linkExists(bLink)).resolves.toBe(true);
    await expect(readlinkTarget(bLink)).resolves.toBe("no-such-file");
    // Dereferencing should fail (still broken)
    await expect(fsp.readFile(bLink, "utf8")).rejects.toBeTruthy();
  });

  test("changing symlink target updates on beta", async () => {
    const r = await mkCase(tmp, "t-link-retarget");
    const aDir = join(r.aRoot, "m");
    const bDir = join(r.bRoot, "m");
    const aA = join(aDir, "a.txt");
    const aB = join(aDir, "b.txt");
    const aL = join(aDir, "p");
    // const bA = join(bDir, "a.txt");
    const bB = join(bDir, "b.txt");
    const bL = join(bDir, "p");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.writeFile(aA, "A");
    await fsp.writeFile(aB, "B");
    await fsp.symlink("a.txt", aL);

    await sync(r, "alpha");

    await expect(readlinkTarget(bL)).resolves.toBe("a.txt");
    await expect(fsp.readFile(bL, "utf8")).resolves.toBe("A");

    // Repoint to b.txt
    await fsp.rm(aL);
    await fsp.symlink("b.txt", aL);

    await sync(r, "alpha");

    await expect(readlinkTarget(bL)).resolves.toBe("b.txt");
    await expect(fileExists(bB)).resolves.toBe(true);
    await expect(fsp.readFile(bL, "utf8")).resolves.toBe("B");
  });

  test("symlink delete propagates alpha→beta", async () => {
    const r = await mkCase(tmp, "t-link-delete");
    const aDir = join(r.aRoot, "x");
    const bDir = join(r.bRoot, "x");
    const aFile = join(aDir, "t.txt");
    const aL = join(aDir, "t.lnk");
    const bL = join(bDir, "t.lnk");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.writeFile(aFile, "x");
    await fsp.symlink("t.txt", aL);
    await sync(r, "alpha");
    await expect(linkExists(bL)).resolves.toBe(true);

    await fsp.rm(aL);
    await sync(r, "alpha");

    await expect(linkExists(bL)).resolves.toBe(false);
  });

  test("file vs symlink conflict (prefer alpha): keep file, replace symlink on beta", async () => {
    const r = await mkCase(tmp, "t-file-vs-link-alpha");
    const aDir = join(r.aRoot, "c");
    const bDir = join(r.bRoot, "c");
    const aFile = join(aDir, "x");
    const bPath = join(bDir, "x");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.writeFile(aFile, "alpha1");
    await sync(r, "alpha"); // seed beta with the file

    // Turn beta's path into a symlink to some other content
    await fsp.rm(bPath).catch(() => {});
    await fsp.writeFile(join(bDir, "other.txt"), "other");
    await fsp.symlink("other.txt", bPath);

    // Make a competing alpha change so it's a real conflict
    await new Promise((resolve) => setTimeout(resolve, 1100));
    // rewrite same contents to bump mtime; or use utimes if you prefer
    await fsp.writeFile(aFile, "alpha2");

    await sync(r, "alpha", false, undefined, ["--lww-epsilon-ms", "50"]);

    // Expect beta back to a regular file with alpha's content
    await expect(isRegularFile(bPath)).resolves.toBe(true);
    await expect(fsp.readFile(bPath, "utf8")).resolves.toBe("alpha2");
  });

  test("file vs symlink conflict (prefer beta): replace file on alpha with symlink", async () => {
    const r = await mkCase(tmp, "t-file-vs-link-beta");
    const aDir = join(r.aRoot, "c2");
    const bDir = join(r.bRoot, "c2");
    const aPath = join(aDir, "x");
    const bPath = join(bDir, "x");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.writeFile(aPath, "alpha");
    await sync(r, "alpha"); // seed

    // Beta turns x into a symlink to y.txt; create y.txt on both for deref
    await fsp.writeFile(join(aDir, "y.txt"), "Y");
    await fsp.writeFile(join(bDir, "y.txt"), "Y");
    await fsp.rm(bPath).catch(() => {});
    await fsp.symlink("y.txt", bPath);

    await sync(r, "beta");

    // Alpha should now have a symlink 'x' -> 'y.txt'
    await expect(linkExists(aPath)).resolves.toBe(true);
    await expect(readlinkTarget(aPath)).resolves.toBe("y.txt");
    await expect(fsp.readFile(aPath, "utf8")).resolves.toBe("Y");
  });

  test("symlink to directory is preserved as a link (not materialized)", async () => {
    const r = await mkCase(tmp, "t-link-to-dir");
    const aTop = join(r.aRoot, "t");
    const bTop = join(r.bRoot, "t");
    const aDir = join(aTop, "dir");
    const aDirFile = join(aDir, "inside.txt");
    const aLink = join(aTop, "dlink");
    const bDirFileViaLink = join(bTop, "dlink", "inside.txt");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.writeFile(aDirFile, "inside");
    await fsp.symlink("dir", aLink); // relative to aTop

    await sync(r, "alpha");

    await expect(linkExists(join(bTop, "dlink"))).resolves.toBe(true);
    await expect(readlinkTarget(join(bTop, "dlink"))).resolves.toBe("dir");
    // Following the link should access the file within the target dir
    await expect(fsp.readFile(bDirFileViaLink, "utf8")).resolves.toBe("inside");
  });

  test("symlink loop does not hang scan/sync and is preserved", async () => {
    const r = await mkCase(tmp, "t-link-loop");
    const aLoopDir = join(r.aRoot, "loop");
    const bLoopDir = join(r.bRoot, "loop");
    const aSelf = join(aLoopDir, "self");
    const bSelf = join(bLoopDir, "self");

    await fsp.mkdir(aLoopDir, { recursive: true });
    // link pointing to current directory (.), classic loop
    await fsp.symlink(".", aSelf);

    await sync(r, "alpha");

    await expect(linkExists(bSelf)).resolves.toBe(true);
    await expect(readlinkTarget(bSelf)).resolves.toBe(".");
  });

  test("symlink chain (L2 -> L1 -> file) preserved verbatim", async () => {
    const r = await mkCase(tmp, "t-link-chain");
    const aDir = join(r.aRoot, "chain");
    const bDir = join(r.bRoot, "chain");
    const aFile = join(aDir, "base.txt");
    const aL1 = join(aDir, "l1");
    const aL2 = join(aDir, "l2");
    const bL1 = join(bDir, "l1");
    const bL2 = join(bDir, "l2");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.writeFile(aFile, "Z");
    await fsp.symlink("base.txt", aL1);
    await fsp.symlink("l1", aL2);

    await sync(r, "alpha");

    await expect(linkExists(bL1)).resolves.toBe(true);
    await expect(linkExists(bL2)).resolves.toBe(true);
    await expect(readlinkTarget(bL1)).resolves.toBe("base.txt");
    await expect(readlinkTarget(bL2)).resolves.toBe("l1");
    await expect(
      fsp.readFile(join(dirname(bL2), await readlinkTarget(bL2)!), "utf8"),
    ).resolves.toBe("Z"); // follow l2->l1->base
  });

  test("absolute symlink target is preserved verbatim", async () => {
    const r = await mkCase(tmp, "t-link-absolute");
    const aDir = join(r.aRoot, "abs");
    const bDir = join(r.bRoot, "abs");
    const aFile = join(aDir, "here.txt");
    const aLink = join(aDir, "alink");
    const bLink = join(bDir, "alink");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.writeFile(aFile, "ABS");
    await fsp.symlink(aFile, aLink); // absolute target

    await sync(r, "alpha");

    await expect(linkExists(bLink)).resolves.toBe(true);
    await expect(readlinkTarget(bLink)).resolves.toBe(aFile); // exact absolute string
    // We don't assert deref here (it may or may not resolve depending on environment);
    // the key is we preserved the link's target string verbatim.
  });
});
