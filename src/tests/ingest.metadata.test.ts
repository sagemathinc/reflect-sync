import os from "node:os";
import path from "node:path";
import fsp from "node:fs/promises";
import { spawn } from "node:child_process";
import { mkCase } from "./util";
import { getDb } from "../db.js";

const INGEST = path.join(__dirname, "../../dist/ingest-delta.js");

function ingestLines(dbPath: string, events: any[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = spawn(process.execPath, [INGEST, "--db", dbPath], {
      stdio: ["pipe", "inherit", "inherit"],
    });
    proc.on("exit", (code) =>
      code === 0
        ? resolve()
        : reject(new Error(`ingest exited with code ${code}`)),
    );
    proc.on("error", reject);
    for (const ev of events) {
      proc.stdin!.write(JSON.stringify(ev) + "\n");
    }
    proc.stdin!.end();
  });
}

describe("ingest metadata tie-breaker", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "reflect-ingest-meta-"));
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("file hash updates when hashed_ctime increases even if op_ts is unchanged", async () => {
    const roots = await mkCase(tmp, "metadata");
    // ensure schema exists
    getDb(roots.aDb).close();

    const relPath = "upperdir/usr/bin/ssh-agent";
    const baseTs = Date.now() - 10_000;
    const initial = {
      path: relPath,
      size: 375304,
      ctime: baseTs,
      mtime: baseTs,
      op_ts: baseTs,
      hash: "hash-old|1ed|0:0",
      deleted: 0,
    };
    const metadataChange = {
      path: relPath,
      size: 375304,
      ctime: baseTs + 500,
      mtime: baseTs, // unchanged mtime triggers op_ts tie
      op_ts: baseTs,
      hash: "hash-new|5ed|0:101",
      deleted: 0,
    };

    await ingestLines(roots.aDb, [initial, metadataChange]);

    const db = getDb(roots.aDb);
    try {
      const row = db
        .prepare(
          `SELECT hash, hashed_ctime, updated FROM nodes WHERE path = ? AND kind = 'f'`,
        )
        .get(relPath) as {
        hash?: string;
        hashed_ctime?: number;
        updated?: number;
      };
      expect(row?.hash).toBe(metadataChange.hash);
      expect(row?.hashed_ctime).toBe(metadataChange.ctime);
      expect(row?.updated).toBeGreaterThanOrEqual(initial.op_ts);
    } finally {
      db.close();
    }
  });
});
