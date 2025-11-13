import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Database } from "../db.js";
import { syncConfirmedCopiesToBase } from "../copy-pending.js";
import { mkCase, runDist, sync } from "./util";

describe("restricted scan confirmation", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "restricted-confirm-"));
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  async function runRestrictedScan(root: string, db: string, rel: string) {
    await runDist("scan.js", [
      "--root",
      root,
      "--db",
      db,
      "--restricted-path",
      rel,
    ]);
  }

  function setCopyPending(dbPath: string, rel: string) {
    const db = new Database(dbPath);
    try {
      db.prepare(
        `UPDATE nodes SET copy_pending = 1, hash_pending = 0 WHERE path = ?`,
      ).run(rel);
    } finally {
      db.close();
    }
  }

  function readNode(dbPath: string, rel: string) {
    const db = new Database(dbPath);
    try {
      return db
        .prepare(
          `SELECT path, hash, deleted, copy_pending FROM nodes WHERE path = ?`,
        )
        .get(rel) as
        | {
            path: string;
            hash: string;
            deleted: number;
            copy_pending: number;
          }
        | undefined;
    } finally {
      db.close();
    }
  }

  test("restricted scan confirms existing pending copies", async () => {
    const r = await mkCase(tmp, "confirm-existing");
    const rel = "foo.bin";
    const bPath = path.join(r.bRoot, rel);
    await fsp.mkdir(path.dirname(bPath), { recursive: true });
    await fsp.writeFile(bPath, "beta-contents");

    await sync(r, "alpha");

    // simulate a copy that has not been confirmed yet
    setCopyPending(r.aDb, rel);

    await runRestrictedScan(r.aRoot, r.aDb, rel);
    syncConfirmedCopiesToBase(r.aDb, r.baseDb);

    const alphaRow = readNode(r.aDb, rel);
    expect(alphaRow?.deleted).toBe(0);
    expect(alphaRow?.copy_pending).toBe(0);
    expect(alphaRow?.hash?.includes("|")).toBe(true);

    const baseRow = readNode(r.baseDb, rel);
    expect(baseRow?.hash).toBe(alphaRow?.hash);
    expect(baseRow?.deleted).toBe(0);
  });

  test("restricted scan records deletion when pending copy target vanished", async () => {
    const r = await mkCase(tmp, "confirm-missing");
    const rel = "bar.bin";
    const aPath = path.join(r.aRoot, rel);
    const bPath = path.join(r.bRoot, rel);
    await fsp.mkdir(path.dirname(bPath), { recursive: true });
    await fsp.writeFile(bPath, "beta-contents");

    await sync(r, "alpha");

    // remove the file before confirming to simulate user delete
    await fsp.rm(aPath);
    setCopyPending(r.aDb, rel);

    await runRestrictedScan(r.aRoot, r.aDb, rel);
    syncConfirmedCopiesToBase(r.aDb, r.baseDb);

    const alphaRow = readNode(r.aDb, rel);
    expect(alphaRow?.deleted).toBe(1);
    expect(alphaRow?.copy_pending).toBe(0);
    expect(alphaRow?.hash).toBe("");

    const baseRow = readNode(r.baseDb, rel);
    expect(baseRow?.hash).not.toBe("");
  });
});
