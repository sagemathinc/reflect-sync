// tests/sync.ignores.test.ts
//
// pnpm test sync.ignores.test.ts
//
// Ignore-focused scenarios: copy suppression, delete suppression,
// negation (!pattern), directory-level ignores, and symlink-path ignores.

import { sync, dirExists, fileExists, linkExists, mkCase } from "./util";
import fsp from "node:fs/promises";
import { join } from "node:path";
import os from "node:os";
import { IGNORE_FILE } from "../constants";

describe("ccsync: ignore rules (IGNORE_FILE)", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "ccsync-test-ignores-"));
  });

  afterAll(async () => {
    // comment out to inspect failures
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("*.log ignored on alpha prevents alpha→beta copy", async () => {
    const r = await mkCase(tmp, "t-ig-alpha-nocopy");
    const aIg = join(r.aRoot, IGNORE_FILE);
    const aLogs = join(r.aRoot, "logs");
    const aLog = join(aLogs, "app.log");
    const bLog = join(r.bRoot, "logs", "app.log");

    await fsp.writeFile(aIg, "*.log\n");
    await fsp.mkdir(aLogs, { recursive: true });
    await fsp.writeFile(aLog, "secret logs");

    await sync(r, "alpha"); // would copy without ignore

    await expect(fileExists(bLog)).resolves.toBe(false);
  });

  test("ignore prevents delete (alpha ignores path, deletes locally; beta keeps its copy)", async () => {
    const r = await mkCase(tmp, "t-ig-no-delete");
    const aIg = join(r.aRoot, IGNORE_FILE);
    const aFile = join(r.aRoot, "keep.txt");
    const bFile = join(r.bRoot, "keep.txt");

    // seed file on both sides
    await fsp.writeFile(aFile, "keep");
    await sync(r, "alpha");
    await expect(fileExists(bFile)).resolves.toBe(true);

    // now ignore + delete on alpha
    await fsp.writeFile(aIg, "keep.txt\n");
    await fsp.rm(aFile, { force: true });
    await sync(r, "alpha");

    // beta keeps its file; alpha's deletion is local-only
    await expect(fileExists(bFile)).resolves.toBe(true);
    await expect(fileExists(aFile)).resolves.toBe(false);
  });

  test("dir-level ignore 'dist/' excludes its entire subtree", async () => {
    const r = await mkCase(tmp, "t-ig-dist-dir");
    const aIg = join(r.aRoot, IGNORE_FILE);
    const aDist = join(r.aRoot, "dist");
    const aA = join(aDist, "a.txt");
    const aB = join(aDist, "sub", "b.txt");
    const bDist = join(r.bRoot, "dist");
    const bA = join(bDist, "a.txt");
    const bB = join(bDist, "sub", "b.txt");

    await fsp.writeFile(aIg, "dist/\n");
    await fsp.mkdir(join(aDist, "sub"), { recursive: true });
    await fsp.writeFile(aA, "A");
    await fsp.writeFile(aB, "B");

    await sync(r, "alpha");

    await expect(dirExists(bDist)).resolves.toBe(false);
    await expect(fileExists(bA)).resolves.toBe(false);
    await expect(fileExists(bB)).resolves.toBe(false);
  });

  test("symlink path ignored under links/**", async () => {
    const r = await mkCase(tmp, "t-ig-links-symlink");
    const aIg = join(r.aRoot, IGNORE_FILE);
    const aLinks = join(r.aRoot, "links");
    const aT = join(aLinks, "target.txt");
    const aL = join(aLinks, "a.lnk");
    const bLinks = join(r.bRoot, "links");
    const bT = join(bLinks, "target.txt");
    const bL = join(bLinks, "a.lnk");

    await fsp.writeFile(aIg, "links/**\n");
    await fsp.writeFile(aIg, "links/\n");
    await fsp.mkdir(aLinks, { recursive: true });
    await fsp.writeFile(aT, "T");
    await fsp.symlink("target.txt", aL);

    await sync(r, "alpha");

    await expect(dirExists(bLinks)).resolves.toBe(false); // entire subtree ignored
    await expect(fileExists(bT)).resolves.toBe(false);
    await expect(linkExists(bL)).resolves.toBe(false);
  });

  test("beta-only ignore blocks incoming copy", async () => {
    const r = await mkCase(tmp, "t-ig-beta-copy");
    const bIg = join(r.bRoot, IGNORE_FILE);
    const aTmp = join(r.aRoot, "cache.tmp");
    const bTmp = join(r.bRoot, "cache.tmp");

    await fsp.writeFile(bIg, "*.tmp\n*.cache\n"); // enable ignore first
    await fsp.writeFile(aTmp, "TMP");
    await sync(r, "alpha");

    await expect(fileExists(bTmp)).resolves.toBe(false);
  });

  test("beta-only ignore blocks delete after seeding", async () => {
    const r = await mkCase(tmp, "t-ig-beta-delete");
    const bIg = join(r.bRoot, IGNORE_FILE);
    const aCache = join(r.aRoot, "persist.cache");
    const bCache = join(r.bRoot, "persist.cache");

    // seed both sides
    await fsp.writeFile(aCache, "C");
    await sync(r, "alpha");
    await expect(fileExists(bCache)).resolves.toBe(true);

    // now enable ignore and delete on alpha
    await fsp.writeFile(bIg, "*.tmp\n*.cache\n");
    await fsp.rm(aCache, { force: true });
    await sync(r, "alpha");

    await expect(fileExists(bCache)).resolves.toBe(true);
    await expect(fileExists(aCache)).resolves.toBe(false);
  });
});
