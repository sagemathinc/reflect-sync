import fsp from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { Database } from "../db";
import { mkCase, runDist } from "./util";

describe("restricted scan and merge", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "reflect-restrict-"));
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("scan with restricted paths/dirs only records requested entries", async () => {
    const r = await mkCase(tmp, "restricted-scan");
    await fsp.writeFile(path.join(r.aRoot, "include.txt"), "alpha-include");
    await fsp.mkdir(path.join(r.aRoot, "dirA"), { recursive: true });
    await fsp.writeFile(path.join(r.aRoot, "dirA/fileA.txt"), "alpha-dir-file");
    await fsp.mkdir(path.join(r.aRoot, "dirB"), { recursive: true });
    await fsp.writeFile(path.join(r.aRoot, "dirB/skip.txt"), "skip-me");
    await fsp.writeFile(path.join(r.aRoot, "excluded.txt"), "exclude");

    await runDist("scan.js", [
      "--root",
      r.aRoot,
      "--db",
      r.aDb,
      "--restricted-path",
      "include.txt",
      "--restricted-dir",
      "dirA",
    ]);

    const db = new Database(r.aDb);
    try {
      const files = db
        .prepare(`SELECT path FROM files WHERE deleted = 0 ORDER BY path`)
        .all()
        .map((row: { path: string }) => row.path);
      expect(files).toEqual(["dirA/fileA.txt", "include.txt"]);

      const dirs = db
        .prepare(`SELECT path FROM dirs WHERE deleted = 0 ORDER BY path`)
        .all()
        .map((row: { path: string }) => row.path);
      expect(dirs).toEqual(["dirA"]);
    } finally {
      db.close();
    }
  });

  test("merge restricted paths only copies requested subset", async () => {
    const r = await mkCase(tmp, "restricted-merge");

    // Populate beta with baseline content
    await fsp.writeFile(path.join(r.bRoot, "include.txt"), "beta-include");
    await fsp.mkdir(path.join(r.bRoot, "dirA"), { recursive: true });
    await fsp.writeFile(path.join(r.bRoot, "dirA/fileA.txt"), "beta-dir");
    await fsp.writeFile(path.join(r.bRoot, "excluded.txt"), "beta-only");

    // Alpha has new content we want to sync selectively
    await fsp.writeFile(path.join(r.aRoot, "include.txt"), "alpha-include");
    await fsp.mkdir(path.join(r.aRoot, "dirA"), { recursive: true });
    await fsp.writeFile(path.join(r.aRoot, "dirA/fileA.txt"), "alpha-dir-file");
    await fsp.writeFile(path.join(r.aRoot, "excluded.txt"), "alpha-excluded");

    // Full scans to seed DBs
    await runDist("scan.js", ["--root", r.aRoot, "--db", r.aDb]);
    await runDist("scan.js", ["--root", r.bRoot, "--db", r.bDb]);

    // Restricted merge should only copy include.txt and dirA/*
    await runDist("merge.js", [
      "--alpha-root",
      r.aRoot,
      "--beta-root",
      r.bRoot,
      "--alpha-db",
      r.aDb,
      "--beta-db",
      r.bDb,
      "--base-db",
      r.baseDb,
      "--restricted-path",
      "include.txt",
      "--restricted-dir",
      "dirA",
    ]);

    await expect(
      fsp.readFile(path.join(r.bRoot, "include.txt"), "utf8"),
    ).resolves.toBe("alpha-include");
    await expect(
      fsp.readFile(path.join(r.bRoot, "dirA/fileA.txt"), "utf8"),
    ).resolves.toBe("alpha-dir-file");
    await expect(
      fsp.readFile(path.join(r.bRoot, "excluded.txt"), "utf8"),
    ).resolves.toBe("beta-only");
  });
});
