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
    await runDist("scan.js", [
      "--root",
      root,
      "--db",
      dbPath,
      ...extra,
    ]);
  }

  test("removing a file clears hash and size when marked deleted", async () => {
    const file = path.join(root, "foo.txt");
    await fsp.writeFile(file, "hello", "utf8");

    await runScanCli();

    await fsp.rm(file);

    await runScanCli();

    const db = getDb(dbPath);
    try {
      const row = db
        .prepare(
          `SELECT deleted, hash, hashed_ctime, size FROM nodes WHERE path = ?`,
        )
        .get("foo.txt") as {
        deleted: number;
        hash: string;
        hashed_ctime: number | null;
        size: number;
      };

      expect(row.deleted).toBe(1);
      expect(row.hash).toBe("");
      expect(row.hashed_ctime).toBeNull();
      expect(row.size).toBe(0);
    } finally {
      db.close();
    }
  });
});
