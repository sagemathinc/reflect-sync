import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { getDb } from "../db.js";
import { runDist } from "./util.js";

describe("scan marks case conflicts using override caps", () => {
  let tmp: string;
  let root: string;
  let dbPath: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "scan-case-conflict-"));
    root = path.join(tmp, "root");
    dbPath = path.join(tmp, "scan.db");
    await fsp.mkdir(root, { recursive: true });
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  async function runScanCli(extra: string[] = []) {
    await runDist("scan.js", ["--root", root, "--db", dbPath, ...extra]);
  }

  test("case-insensitive counterpart flags conflicting files", async () => {
    await fsp.writeFile(path.join(root, "foo.txt"), "one");
    await fsp.writeFile(path.join(root, "FOO.txt"), "two");

    await runScanCli([
      "--mark-case-conflicts",
      "--case-conflict-case-insensitive",
    ]);

    const db = getDb(dbPath);
    try {
      const rows = db
        .prepare(
          `SELECT path, case_conflict FROM nodes WHERE path IN ('foo.txt', 'FOO.txt') ORDER BY path`,
        )
        .all() as { path: string; case_conflict: number }[];
      expect(rows).toHaveLength(2);
      const flags = rows.map((r) => r.case_conflict);
      expect(flags.every((v) => v === 0 || v === 1)).toBe(true);
      expect(flags.some((v) => v === 1)).toBe(true);
      expect(flags.reduce((sum, v) => sum + v, 0)).toBe(1);
    } finally {
      db.close();
    }
  });
});
