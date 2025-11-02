// tests/sync.ignores.test.ts
//
// pnpm test sync.ignores.test.ts
//
// Ignore-focused scenarios: copy suppression, delete suppression,
// directory-level ignores, and symlink-path ignores.

import { sync, dirExists, fileExists, linkExists, mkCase } from "./util";
import fsp from "node:fs/promises";
import { join } from "node:path";
import os from "node:os";

describe("rfsync: ignore rules", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "rfsync-test-ignores-"));
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("*.log ignored prevents alpha→beta copy", async () => {
    const r = await mkCase(tmp, "t-ig-alpha-copy");
    const alphaLog = join(r.aRoot, "logs", "app.log");
    const betaLog = join(r.bRoot, "logs", "app.log");
    await fsp.mkdir(join(r.aRoot, "logs"), { recursive: true });
    await fsp.writeFile(alphaLog, "alpha logs");

    await sync(r, "alpha", undefined, ["--ignore", "*.log"]);
    await expect(fileExists(betaLog)).resolves.toBe(false);
  });

  test("*.log ignored prevents beta→alpha copy", async () => {
    const r = await mkCase(tmp, "t-ig-beta-copy");
    const alphaLog = join(r.aRoot, "logs", "app.log");
    const betaLog = join(r.bRoot, "logs", "app.log");
    await fsp.mkdir(join(r.bRoot, "logs"), { recursive: true });
    await fsp.writeFile(betaLog, "beta logs");

    await sync(r, "beta", undefined, ["--ignore", "*.log"]);
    await expect(fileExists(alphaLog)).resolves.toBe(false);
  });

  test("ignore prevents delete from propagating", async () => {
    const r = await mkCase(tmp, "t-ig-no-delete");
    const shared = join(r.aRoot, "keep.txt");
    const betaFile = join(r.bRoot, "keep.txt");

    await fsp.writeFile(shared, "keep");
    await sync(r, "alpha");
    await expect(fileExists(betaFile)).resolves.toBe(true);

    await fsp.rm(shared, { force: true });
    await sync(r, "alpha", undefined, ["--ignore", "keep.txt"]);

    await expect(fileExists(betaFile)).resolves.toBe(true);
    await expect(fileExists(shared)).resolves.toBe(false);
  });

  test("dir-level ignore excludes entire subtree", async () => {
    const r = await mkCase(tmp, "t-ig-dist-dir");
    const dist = join(r.aRoot, "dist");
    await fsp.mkdir(join(dist, "sub"), { recursive: true });
    await fsp.writeFile(join(dist, "a.txt"), "A");
    await fsp.writeFile(join(dist, "sub", "b.txt"), "B");

    await sync(r, "alpha", undefined, ["--ignore", "dist/"]);

    await expect(dirExists(join(r.bRoot, "dist"))).resolves.toBe(false);
    await expect(fileExists(join(r.bRoot, "dist", "a.txt"))).resolves.toBe(
      false,
    );
    await expect(fileExists(join(r.bRoot, "dist", "sub", "b.txt"))).resolves.toBe(
      false,
    );
  });

  test("symlink path ignored under links/**", async () => {
    const r = await mkCase(tmp, "t-ig-links-symlink");
    const linksDir = join(r.aRoot, "links");
    await fsp.mkdir(linksDir, { recursive: true });
    await fsp.writeFile(join(linksDir, "target.txt"), "T");
    await fsp.symlink("target.txt", join(linksDir, "a.lnk"));

    await sync(r, "alpha", undefined, ["--ignore", "links/"]);

    await expect(dirExists(join(r.bRoot, "links"))).resolves.toBe(false);
    await expect(fileExists(join(r.bRoot, "links", "target.txt"))).resolves.toBe(
      false,
    );
    await expect(linkExists(join(r.bRoot, "links", "a.lnk"))).resolves.toBe(
      false,
    );
  });
});
