import { spawn } from "node:child_process";
import fsp from "node:fs/promises";
import { join, resolve } from "node:path";
import Database from "better-sqlite3";

// Toggle for noisy child output while debugging, by default.
const VERBOSE = false;

const DIST = resolve(__dirname, "../../dist");

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

export async function sync(
  r: Roots,
  prefer: "alpha" | "beta" = "alpha",
  verbose: boolean | undefined = undefined,
) {
  const verboseArg = verbose || VERBOSE ? ["--verbose"] : [];

  await runDist("scan.js", ["--root", r.aRoot, "--db", r.aDb, ...verboseArg]);
  await runDist("scan.js", ["--root", r.bRoot, "--db", r.bDb, ...verboseArg]);
  await runDist("merge-rsync.js", [
    "--alpha-root",
    r.aRoot,
    "--beta-root",
    r.bRoot,
    "--alpha-db",
    r.aDb,
    "--beta-db",
    r.bDb,
    "--base-db",
    r.baseDb,
    "--prefer",
    prefer,
    ...verboseArg,
  ]);
}

export async function waitFor<T>(
  fn: () => Promise<T> | T,
  predicate: ((v: T) => Promise<boolean>) | ((v: T) => boolean),
  timeoutMs = 10_000,
  intervalMs = 50,
): Promise<T> {
  const t0 = Date.now();
  // eslint-disable-next-line no-constant-condition
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

