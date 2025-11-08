import { join, dirname } from "node:path";
import os from "node:os";
import fsp from "node:fs/promises";

import { countSchedulerCycles, waitFor } from "./util";
import { getDb } from "../db";
import {
  describeIfSsh,
  startSchedulerRemote,
  stopScheduler,
  wait,
  hashFile,
} from "./ssh.remote-test-util";

describeIfSsh("SSH remote sync – sqlite workload", () => {
  let tmp = "";
  let alphaRoot = "";
  let betaRootRemote = "";
  let alphaDb = "";
  let betaDb = "";
  let baseDb = "";

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "rfsync-sqlite-"));
    alphaRoot = join(tmp, "alpha");
    betaRootRemote = join(tmp, "beta");
    alphaDb = join(tmp, "alpha.db");
    betaDb = join(tmp, "beta.db");
    baseDb = join(tmp, "base.db");
    await fsp.mkdir(alphaRoot, { recursive: true });
    await fsp.mkdir(betaRootRemote, { recursive: true });
  });

  beforeEach(async () => {
    await fsp.rm(alphaRoot, { recursive: true, force: true });
    await fsp.rm(betaRootRemote, { recursive: true, force: true });
    await fsp.mkdir(alphaRoot, { recursive: true });
    await fsp.mkdir(betaRootRemote, { recursive: true });
    await Promise.all(
      [alphaDb, betaDb, baseDb].map((p) =>
        fsp.rm(p, { recursive: true, force: true }).catch(() => {}),
      ),
    );
  });

  afterAll(async () => {
    if (tmp) {
      await fsp.rm(tmp, { recursive: true, force: true });
    }
  });

  it("sqlite workload on alpha stays consistent on remote beta", async () => {
    const child = startSchedulerRemote({
      alphaRoot,
      betaRootRemote,
      alphaDb,
      betaDb,
      baseDb,
      prefer: "alpha",
    });

    const relPath = "databases/work.db";
    const alphaFile = join(alphaRoot, relPath);
    const betaFile = join(betaRootRemote, relPath);

    try {
      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= 1,
        20_000,
        20,
      );

      await fsp.mkdir(dirname(alphaFile), { recursive: true });
      const db = getDb(alphaFile);
      try {
        db.exec(`PRAGMA journal_mode = WAL;`);
        db.exec(
          `CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY, value TEXT);`,
        );
        const insert = db.prepare(
          `INSERT OR REPLACE INTO kv(key, value) VALUES (?, ?)`,
        );
        const tx = db.transaction(
          (rows: Array<{ key: string; value: string }>) => {
            for (const row of rows) insert.run(row.key, row.value);
          },
        );
        for (let batch = 0; batch < 40; batch++) {
          const rows = Array.from({ length: 50 }, (_, j) => ({
            key: `key-${batch}-${j}`,
            value: `value-${batch}-${j}`,
          }));
          tx(rows);
          await wait(5);
        }
        db.exec(`PRAGMA wal_checkpoint(TRUNCATE);`);
      } finally {
        db.close();
      }

      await waitFor(
        async () => {
          try {
            const [statAlpha, statBeta] = await Promise.all([
              fsp.stat(alphaFile),
              fsp.stat(betaFile),
            ]);
            return statAlpha.size === statBeta.size && statBeta.size > 0;
          } catch {
            return false;
          }
        },
        (ok) => ok,
        30_000,
        50,
      );

      const [alphaHash, betaHash] = await Promise.all([
        hashFile(alphaFile),
        hashFile(betaFile),
      ]);
      expect(betaHash).toBe(alphaHash);

      const betaDbHandle = getDb(betaFile);
      try {
        const rowCount = betaDbHandle
          .prepare(`SELECT COUNT(*) AS n FROM kv`)
          .get() as { n: number };
        expect(rowCount.n).toBeGreaterThanOrEqual(40 * 50);
      } finally {
        betaDbHandle.close();
      }

      const meta = getDb(alphaDb);
      try {
        const row = meta
          .prepare(
            `SELECT signature FROM recent_send WHERE direction='beta->alpha' AND path = ?`,
          )
          .get(relPath);
        expect(row).toBeUndefined();
      } finally {
        meta.close();
      }
    } finally {
      await stopScheduler(child);
    }
  }, 25_000);
});
