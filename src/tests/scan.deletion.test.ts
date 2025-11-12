import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { runDist } from "./util";
import { getDb } from "../db.js";

describe("scan deletes", () => {
  let tmp: string;
  let root: string;
  let dbPath: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "scan-delete-"));
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

  test("removing a file clears hash and size when marked deleted", async () => {
    const file = path.join(root, "foo.txt");
    await fsp.writeFile(file, "hello", "utf8");

    await runScanCli();

    let liveMeta: { last_seen: number } | null = null;
    {
      const db = getDb(dbPath);
      try {
        liveMeta =
          (db
            .prepare(`SELECT last_seen FROM nodes WHERE path = ?`)
            .get("foo.txt") as { last_seen: number } | undefined) ?? null;
      } finally {
        db.close();
      }
    }

    await fsp.rm(file);

    await runScanCli();

    const db = getDb(dbPath);
    try {
      const row = db
        .prepare(
          `SELECT deleted, hash, hashed_ctime, size, mtime, last_seen, updated, change_start, change_end
             FROM nodes WHERE path = ?`,
        )
        .get("foo.txt") as {
        deleted: number;
        hash: string;
        hashed_ctime: number | null;
        size: number;
        mtime: number;
        last_seen: number | null;
        updated: number;
        change_start: number | null;
        change_end: number | null;
      };

      expect(row.deleted).toBe(1);
      expect(row.hash).toBe("");
      expect(row.hashed_ctime).toBeNull();
      expect(row.size).toBe(0);
      if (!liveMeta) {
        throw new Error("missing initial metadata for foo.txt");
      }
      expect(row.last_seen).toBe(liveMeta.last_seen);
      expect(row.mtime).toBe(liveMeta.last_seen + 1);
      expect(row.change_end).toBe(row.updated);
      expect(row.change_end).not.toBeNull();
      expect(row.change_start).not.toBeNull();
      if (row.change_start !== null && row.change_end !== null) {
        expect(row.change_start).toBeLessThanOrEqual(row.change_end);
      }
    } finally {
      db.close();
    }
  });
});
