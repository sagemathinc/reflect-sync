import { spawn } from "node:child_process";
import fsp from "node:fs/promises";
import { join, resolve } from "node:path";
import { Database } from "../db";
import { executeThreeWayMerge } from "../three-way-merge.js";
import { createLogicalClock } from "../logical-clock.js";
import { syncConfirmedCopiesToBase } from "../copy-pending.js";

const DIST = resolve(__dirname, "../../dist");

export function wait(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

export function runDist(
  scriptRel: string,
  args: string[] = [],
  envExtra: Record<string, string> = {},
): Promise<void> {
  return new Promise((resolve, reject) => {
    const script = join(DIST, scriptRel);
    const p = spawn(process.execPath, [script, ...args], {
      stdio: "inherit",
      env: { ...process.env, ...envExtra },
    });
    p.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error(`${scriptRel} exited ${code}`)),
    );
    p.on("error", reject);
  });
}

export async function fileExists(p: string) {
  try {
    await fsp.stat(p);
    return true;
  } catch {
    return false;
  }
}

export type Roots = {
  aRoot: string;
  bRoot: string;
  aDb: string;
  bDb: string;
  baseDb: string;
};

export async function mkCase(tmpBase: string, name: string): Promise<Roots> {
  const base = join(tmpBase, name);
  const aRoot = join(base, "alpha");
  const bRoot = join(base, "beta");
  const aDb = join(base, "alpha.db");
  const bDb = join(base, "beta.db");
  const baseDb = join(base, "base.db");
  await fsp.mkdir(aRoot, { recursive: true });
  await fsp.mkdir(bRoot, { recursive: true });
  return { aRoot, bRoot, aDb, bDb, baseDb };
}

type SyncOptions = {
  scanOrder?: ("alpha" | "beta")[];
  extraCycles?: number;
  traceDbPath?: string;
  traceLabel?: string;
};

export async function sync(
  r: Roots,
  prefer: "alpha" | "beta" = "alpha",
  _verbose: boolean | undefined = undefined,
  args?: string[],
  options: SyncOptions = {},
  strategy = "last-write-wins",
) {
  const cycles = Math.max(1, (options.extraCycles ?? 0) + 1);
  const logicalClock = await createLogicalClock([r.aDb, r.bDb, r.baseDb]);
  for (let cycle = 0; cycle < cycles; cycle += 1) {
    const order = options.scanOrder ?? ["alpha", "beta"];
    for (const side of order) {
      const root = side === "alpha" ? r.aRoot : r.bRoot;
      const db = side === "alpha" ? r.aDb : r.bDb;
      const scanTick = logicalClock.next();
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
        ...(args ?? []),
      ]);
    }
    syncConfirmedCopiesToBase(r.aDb, r.baseDb);
    syncConfirmedCopiesToBase(r.bDb, r.baseDb);
    await executeThreeWayMerge({
      alphaDb: r.aDb,
      betaDb: r.bDb,
      baseDb: r.baseDb,
      prefer,
      strategyName: strategy,
      alphaRoot: r.aRoot,
      betaRoot: r.bRoot,
      logicalClock,
      traceDbPath: options.traceDbPath,
      traceLabel: options.traceLabel,
    });
  }
}

export async function syncPrefer(
  r: Roots,
  prefer: "alpha" | "beta" = "alpha",
  args?: string[],
  options?: SyncOptions,
) {
  await sync(r, prefer, undefined, args, options ?? {}, "prefer");
}

export async function waitFor<T>(
  fn: () => Promise<T> | T,
  predicate: ((v: T) => Promise<boolean>) | ((v: T) => boolean),
  timeoutMs = 10_000,
  intervalMs = 50,
): Promise<T> {
  const t0 = Date.now();
  while (true) {
    const v = await fn();
    if (await predicate(v)) return v;
    if (Date.now() - t0 > timeoutMs) throw new Error("waitFor: timeout");
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

export function countSchedulerCycles(baseDb: string): number {
  const db = new Database(baseDb);
  try {
    db.pragma("busy_timeout = 1000");
    db.exec(`
      CREATE TABLE IF NOT EXISTS events(
        id INTEGER PRIMARY KEY,
        ts INTEGER,
        level TEXT,
        source TEXT,
        msg TEXT,
        details TEXT
      );
    `);
    const row = db
      .prepare(
        `SELECT COUNT(*) AS n FROM events WHERE source='scheduler' AND msg LIKE 'cycle complete %'`,
      )
      .get() as { n?: number };
    return row?.n ?? 0;
  } finally {
    db.close();
  }
}

export function hasEventLog(
  baseDb: string,
  source: string,
  msg: string,
): boolean {
  const db = new Database(baseDb);
  try {
    db.pragma("busy_timeout = 1000");
    db.exec(`
      CREATE TABLE IF NOT EXISTS events(
        id INTEGER PRIMARY KEY,
        ts INTEGER,
        level TEXT,
        source TEXT,
        msg TEXT,
        details TEXT
      );
    `);
    const row = db
      .prepare(`SELECT COUNT(*) AS n FROM events WHERE source=? AND msg=?`)
      .get(source, msg) as { n?: number };
    return (row?.n ?? 0) > 0;
  } finally {
    db.close();
  }
}

export async function dirExists(p: string) {
  return !!(await fsp
    .stat(p)
    .then((st) => st.isDirectory())
    .catch(() => false));
}

export async function linkExists(p: string) {
  return !!(await fsp
    .lstat(p)
    .then((st) => st.isSymbolicLink())
    .catch(() => false));
}
