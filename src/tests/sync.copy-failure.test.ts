import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Database } from "../db.js";
import { executeThreeWayMerge } from "../three-way-merge.js";
import { createLogicalClock } from "../logical-clock.js";
import { syncConfirmedCopiesToBase } from "../copy-pending.js";
import { dirExists, mkCase, runDist, type Roots } from "./util.js";

describe("three-way merge copy tracking", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "copy-failure-"));
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  async function scanSide(
    r: Roots,
    side: "alpha" | "beta",
    clock: Awaited<ReturnType<typeof createLogicalClock>>,
  ) {
    const root = side === "alpha" ? r.aRoot : r.bRoot;
    const db = side === "alpha" ? r.aDb : r.bDb;
    const scanTick = clock.next();
    await runDist("scan.js", [
      "--root",
      root,
      "--db",
      db,
      "--clock-base",
      r.aDb,
      "--clock-base",
      r.bDb,
      "--clock-base",
      r.baseDb,
      "--scan-tick",
      String(scanTick),
    ]);
  }

  function readNode(
    dbPath: string,
    rel: string,
  ): { deleted: number; copy_pending: number } | undefined {
    const db = new Database(dbPath);
    try {
      return db
        .prepare(
          `SELECT deleted, copy_pending FROM nodes WHERE path = ?`,
        )
        .get(rel) as { deleted: number; copy_pending: number } | undefined;
    } finally {
      db.close();
    }
  }

  test("vanished source is not recorded as a successful copy", async () => {
    const r = await mkCase(tmp, "vanished-source");
    const rel = "var/tmp/foo.bin";
    const betaPath = path.join(r.bRoot, rel);
    await fsp.mkdir(path.dirname(betaPath), { recursive: true });
    await fsp.writeFile(betaPath, "beta-data");

    const clock = await createLogicalClock([r.aDb, r.bDb, r.baseDb]);
    await scanSide(r, "alpha", clock);
    await scanSide(r, "beta", clock);

    // Remove the source before merge so rsync reports a vanished file.
    await fsp.rm(betaPath);

    syncConfirmedCopiesToBase(r.aDb, r.baseDb);
    syncConfirmedCopiesToBase(r.bDb, r.baseDb);

    await executeThreeWayMerge({
      alphaDb: r.aDb,
      betaDb: r.bDb,
      baseDb: r.baseDb,
      prefer: "alpha",
      alphaRoot: r.aRoot,
      betaRoot: r.bRoot,
      logicalClock: clock,
    });

    const alphaRow = readNode(r.aDb, rel);
    expect(alphaRow).toBeUndefined();

    const baseRow = readNode(r.baseDb, rel);
    expect(baseRow).toBeUndefined();
  });

  test("vanished directory is not recorded as a successful copy", async () => {
    const r = await mkCase(tmp, "vanished-dir");
    const rel = "var/tmp/foo-dir";
    const betaDir = path.join(r.bRoot, rel);
    await fsp.mkdir(betaDir, { recursive: true });
    await fsp.writeFile(path.join(betaDir, "child.txt"), "beta-data");

    const clock = await createLogicalClock([r.aDb, r.bDb, r.baseDb]);
    await scanSide(r, "alpha", clock);
    await scanSide(r, "beta", clock);

    await fsp.rm(betaDir, { recursive: true, force: true });

    syncConfirmedCopiesToBase(r.aDb, r.baseDb);
    syncConfirmedCopiesToBase(r.bDb, r.baseDb);

    await executeThreeWayMerge({
      alphaDb: r.aDb,
      betaDb: r.bDb,
      baseDb: r.baseDb,
      prefer: "alpha",
      alphaRoot: r.aRoot,
      betaRoot: r.bRoot,
      logicalClock: clock,
    });

    expect(await dirExists(path.join(r.aRoot, rel))).toBe(false);
    const alphaRow = readNode(r.aDb, rel);
    expect(alphaRow).toBeUndefined();
  });
});
