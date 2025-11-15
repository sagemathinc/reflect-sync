import { mkCase, sync, syncPrefer } from "./util";
import fsp from "node:fs/promises";
import { join } from "node:path";
import os from "node:os";
import { Database, getDb, getBaseDb } from "../db";
import { planThreeWayMerge } from "../three-way-merge.js";

jest.setTimeout(10_000);

describe("LWW: basic conflicting edits", () => {
  let tmp: string;
  const CLEANUP = !process.env.KEEP_TEST_TMP;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "rfsync-lww-1-"));
  });

  afterAll(async () => {
    if (!CLEANUP) return;
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("beta-only change wins even when prefer=alpha", async () => {
    const r = await mkCase(tmp, "t-lww-newer-beta");
    const alphaDb = getDb(r.aDb);
    const betaDb = getDb(r.bDb);
    const baseDb = getBaseDb(r.baseDb);

    const baseRow = {
      path: "conf.txt",
      kind: "f",
      hash: "seed|1a4",
      mtime: 1_000,
      ctime: 1_000,
      change_start: 900,
      change_end: 1_000,
      confirmed_at: 1_000,
      hashed_ctime: 1_000,
      updated: 1_000,
      size: 4,
      deleted: 0,
      hash_pending: 0,
      copy_pending: 0,
      last_seen: null,
      link_target: null,
      last_error: null,
    };

    const upsert = (db: Database, row: typeof baseRow) => {
      db.prepare(
        `INSERT INTO nodes(path, kind, hash, mtime, ctime, change_start, change_end, confirmed_at, hashed_ctime, updated, size, deleted, hash_pending, copy_pending, last_seen, link_target, last_error)
         VALUES (@path,@kind,@hash,@mtime,@ctime,@change_start,@change_end,@confirmed_at,@hashed_ctime,@updated,@size,@deleted,@hash_pending,@copy_pending,@last_seen,@link_target,@last_error)
         ON CONFLICT(path) DO UPDATE SET
           kind=excluded.kind,
           hash=excluded.hash,
           mtime=excluded.mtime,
           ctime=excluded.ctime,
           change_start=excluded.change_start,
           change_end=excluded.change_end,
           confirmed_at=excluded.confirmed_at,
           hashed_ctime=excluded.hashed_ctime,
           updated=excluded.updated,
           size=excluded.size,
           deleted=excluded.deleted,
           hash_pending=excluded.hash_pending,
           copy_pending=excluded.copy_pending,
           last_seen=excluded.last_seen,
           link_target=excluded.link_target,
           last_error=excluded.last_error`,
      ).run(row);
    };

    try {
      upsert(alphaDb, baseRow);
      upsert(betaDb, {
        ...baseRow,
        hash: "beta-new|1a4",
        change_start: 1_000,
        change_end: 1_100,
        confirmed_at: 1_100,
        updated: 1_100,
        mtime: 1_100,
        ctime: 1_100,
      });
      upsert(baseDb, baseRow);

      const planBeta = planThreeWayMerge({
        alphaDb: r.aDb,
        betaDb: r.bDb,
        baseDb: r.baseDb,
        prefer: "alpha",
        strategyName: "last-write-wins",
      });
      expect(planBeta.operations).toEqual([
        { op: "copy", from: "beta", to: "alpha", path: "conf.txt" },
      ]);

      upsert(alphaDb, {
        ...baseRow,
        hash: "alpha-new|1a4",
        change_start: 1_100,
        change_end: 1_200,
        confirmed_at: 1_200,
        updated: 1_200,
        mtime: 1_200,
        ctime: 1_200,
      });
      upsert(betaDb, baseRow);
      upsert(baseDb, baseRow);

      const planAlpha = planThreeWayMerge({
        alphaDb: r.aDb,
        betaDb: r.bDb,
        baseDb: r.baseDb,
        prefer: "alpha",
        strategyName: "last-write-wins",
      });
      expect(planAlpha.operations).toEqual([
        { op: "copy", from: "alpha", to: "beta", path: "conf.txt" },
      ]);
    } finally {
      alphaDb.close();
      betaDb.close();
      baseDb.close();
    }
  });

  test("prefer strategy overrides default order when requested", async () => {
    const r = await mkCase(tmp, "t-lww-equal");
    const a = join(r.aRoot, "conf.txt");
    const b = join(r.bRoot, "conf.txt");

    await fsp.writeFile(a, "seed");
    await sync(r, "alpha");

    await fsp.writeFile(a, "alpha-data");
    await fsp.writeFile(b, "beta-data");

    // prefer=beta should decide
    await syncPrefer(r, "beta", undefined, {
      scanOrder: ["alpha", "beta"],
    });
    expect(await fsp.readFile(a, "utf8")).toBe("beta-data");
    expect(await fsp.readFile(b, "utf8")).toBe("beta-data");

    // do it again the other way
    await fsp.writeFile(a, "alpha-next");
    await fsp.writeFile(b, "beta-next");
    await syncPrefer(r, "alpha", undefined, {
      scanOrder: ["beta", "alpha"],
    });
    expect(await fsp.readFile(a, "utf8")).toBe("alpha-next");
    expect(await fsp.readFile(b, "utf8")).toBe("alpha-next");
  });
});
