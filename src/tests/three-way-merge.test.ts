import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { getDb } from "../db.js";
import { planThreeWayMerge } from "../three-way-merge.js";

function insertNode(
  dbPath: string,
  row: {
    path: string;
    kind: "f" | "d" | "l";
    hash: string;
    mtime: number;
    updated: number;
    size?: number;
    deleted?: number;
  },
) {
  const db = getDb(dbPath);
  try {
    db.prepare(
      `INSERT INTO nodes(path, kind, hash, mtime, ctime, hashed_ctime, updated, size, deleted, last_seen, link_target, last_error)
       VALUES(@path, @kind, @hash, @mtime, @mtime, NULL, @updated, @size, @deleted, NULL, NULL, NULL)`,
    ).run({
      ...row,
      size: row.size ?? 0,
      deleted: row.deleted ?? 0,
    });
  } finally {
    db.close();
  }
}

describe("three-way merge planner", () => {
  let tmp: string;
  let alphaDb: string;
  let betaDb: string;
  let baseDb: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "merge-plan-"));
    alphaDb = path.join(tmp, "alpha.db");
    betaDb = path.join(tmp, "beta.db");
    baseDb = path.join(tmp, "base.db");
    // touch DBs to create schema
    getDb(alphaDb).close();
    getDb(betaDb).close();
    getDb(baseDb).close();
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("ignores paths deleted on both sides", () => {
    const target = "double/deleted.txt";
    insertNode(alphaDb, {
      path: target,
      kind: "f",
      hash: "",
      mtime: Date.now(),
      updated: Date.now(),
      deleted: 1,
    });
    insertNode(betaDb, {
      path: target,
      kind: "f",
      hash: "somehash",
      mtime: Date.now(),
      updated: Date.now(),
      deleted: 1,
    });
    insertNode(baseDb, {
      path: target,
      kind: "f",
      hash: "somehash",
      mtime: Date.now() - 1000,
      updated: Date.now() - 1000,
      deleted: 1,
    });

    const plan = planThreeWayMerge({
      alphaDb,
      betaDb,
      baseDb,
      prefer: "alpha",
    });

    expect(plan.diffs.find((row) => row.path === target)).toBeUndefined();
    expect(
      plan.operations.find((op) => "path" in op && op.path === target),
    ).toBeUndefined();
  });
});
