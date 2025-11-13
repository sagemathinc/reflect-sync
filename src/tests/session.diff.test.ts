// src/tests/session.diff.test.ts

import fsp from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { getDb } from "../db.js";
import { diffSession } from "../session-diff.js";
import type { SessionRow } from "../session-db.js";

const now = Date.now();

function insertFile(dbPath: string, relPath: string, hash: string) {
  const db = getDb(dbPath);
  try {
    db.prepare(
      `INSERT OR REPLACE INTO nodes(path, kind, hash, mtime, ctime, hashed_ctime, updated, size, deleted, last_seen, link_target, last_error)
       VALUES(?, 'f', ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)`,
    ).run(relPath, hash, now, now, now, now, 0, 0, now);
  } finally {
    db.close();
  }
}

function baseSessionRow(
  alphaDb: string,
  betaDb: string,
  overrides: Partial<SessionRow> = {},
): SessionRow {
  const stamp = Date.now();
  const defaultBaseDb = path.join(path.dirname(alphaDb), "base.db");
  return {
    id: 1,
    created_at: stamp,
    updated_at: stamp,
    name: null,
    alpha_root: "/alpha",
    beta_root: "/beta",
    prefer: "alpha",
    alpha_host: null,
    alpha_port: null,
    beta_host: "localhost",
    beta_port: null,
    alpha_remote_db: null,
    beta_remote_db:
      "~/.local/share/reflect-sync/by-origin/test/sessions/1/beta.db",
    remote_scan_cmd: null,
    remote_watch_cmd: null,
    base_db: defaultBaseDb,
    alpha_db: alphaDb,
    beta_db: betaDb,
    events_db: null,
    hash_alg: "sha256",
    desired_state: "running",
    actual_state: "running",
    last_heartbeat: null,
    scheduler_pid: null,
    compress: "auto",
    ignore_rules: null,
    disable_hot_sync: 0,
    enable_reflink: 0,
    disable_full_sync: 0,
    merge_strategy: null,
    last_clean_sync_at: null,
    ...overrides,
  };
}

describe("session diff restrictions", () => {
  const tmpBase = path.join(os.tmpdir(), "reflect-diff-test-");
  let workDir: string;
  let alphaDb: string;
  let betaDb: string;

  beforeEach(async () => {
    workDir = await fsp.mkdtemp(tmpBase);
    alphaDb = path.join(workDir, "alpha.db");
    betaDb = path.join(workDir, "beta.db");
    const baseDb = path.join(workDir, "base.db");
    // initialize schemas
    getDb(alphaDb).close();
    getDb(betaDb).close();
    getDb(baseDb).close();
    insertFile(alphaDb, "include.txt", "alpha");
    insertFile(betaDb, "include.txt", "beta");
    insertFile(alphaDb, "dirA/file.txt", "alpha-a");
    insertFile(betaDb, "dirA/file.txt", "beta-a");
    insertFile(alphaDb, "dirB/other.txt", "alpha-b");
    insertFile(betaDb, "dirB/other.txt", "beta-b");
  });

  afterEach(async () => {
    await fsp.rm(workDir, { recursive: true, force: true });
  });

  it("filters diffs by restricted paths", () => {
    const session = baseSessionRow(alphaDb, betaDb);
    const allDiffs = diffSession(session)
      .map((entry) => entry.path)
      .sort();
    expect(allDiffs).toEqual([
      "dirA/file.txt",
      "dirB/other.txt",
      "include.txt",
    ]);

    const pathRestricted = diffSession(session, {
      restrictedPaths: ["include.txt"],
    }).map((entry) => entry.path);
    expect(pathRestricted).toEqual(["include.txt"]);
  });
});
