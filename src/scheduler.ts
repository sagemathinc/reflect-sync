// Minimal orchestrator with adaptive watching:
// - Shallow root watchers (depth-limited) to discover candidate subtrees
// - Bounded HotWatchManager adds depth-limited watchers on hot subtrees
// - Escalates deeper on activity at the depth frontier
// - Seeds hot subtrees from recent_touch after each scan
// - Reuses your microSync + full cycle merge-rsync flow

import { spawn } from "node:child_process";
import chokidar, { FSWatcher } from "chokidar";
import Database from "better-sqlite3";
import path from "node:path";
import { tmpdir } from "node:os";
import {
  mkdtemp,
  writeFile,
  stat as fsStat,
  lstat as fsLstat,
} from "node:fs/promises";

// ---------- args ----------
type Args = Record<string, string | boolean>;
function parseArgs(): Args {
  const out: Args = {};
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const [k, maybeV] = a.slice(2).split("=");
      if (maybeV !== undefined) out[k] = parseBool(maybeV);
      else {
        const v = argv[i + 1];
        if (!v || v.startsWith("--")) out[k] = true;
        else {
          out[k] = parseBool(v);
          i++;
        }
      }
    }
  }
  return out;
}
function parseBool(v: string): any {
  if (/^(true|false)$/i.test(v)) return /^true$/i.test(v);
  if (/^(y|n)$/i.test(v)) return /^y$/i.test(v);
  if (/^(1|0)$/i.test(v)) return v === "1";
  return v;
}
const args = parseArgs();
const alphaRoot = String(args["alpha-root"] ?? "");
const betaRoot = String(args["beta-root"] ?? "");
const alphaDb = String(args["alpha-db"] ?? "alpha.db");
const betaDb = String(args["beta-db"] ?? "beta.db");
const baseDb = String(args["base-db"] ?? "base.db");
const prefer = String(args["prefer"] ?? "alpha").toLowerCase();
const verbose = Boolean(args["verbose"] ?? false);
const dryRun = Boolean(args["dry-run"] ?? false);
if (!alphaRoot || !betaRoot) {
  console.error("Need --alpha-root and --beta-root");
  process.exit(1);
}

// ---------- adaptive watcher config ----------
const MAX_HOT_WATCHERS = Number(process.env.MAX_HOT_WATCHERS ?? 256);
const HOT_TTL_MS = Number(process.env.HOT_TTL_MS ?? 3 * 60_000);
const SHALLOW_DEPTH = Number(process.env.SHALLOW_DEPTH ?? 1); // 0 or 1 recommended
const HOT_DEPTH = Number(process.env.HOT_DEPTH ?? 2); // depth under hot anchor

// ---------- logging ----------
const db = new Database(baseDb);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");
db.exec(`
  CREATE TABLE IF NOT EXISTS events(
    id INTEGER PRIMARY KEY,
    ts INTEGER,             -- Date.now()
    level TEXT,             -- info|warn|error
    source TEXT,            -- scheduler|scan|merge|rsync
    msg TEXT,
    details TEXT            -- JSON
  );
`);
const logStmt = db.prepare(
  `INSERT INTO events(ts,level,source,msg,details) VALUES (?,?,?,?,?)`,
);

function log(
  level: "info" | "warn" | "error",
  source: string,
  msg: string,
  details?: any,
) {
  try {
    logStmt.run(
      Date.now(),
      level,
      source,
      msg,
      details ? JSON.stringify(details) : null,
    );
  } catch {}
  if (verbose || level !== "info")
    console.log(
      `${level === "error" ? "⛔" : level === "warn" ? "⚠️" : "ℹ️"} [${source}] ${msg}`,
      details ? JSON.stringify(details) : "",
    );
}

// ---------- helpers ----------
function spawnTask(
  cmd: string,
  args: string[],
  envExtra: Record<string, string> = {},
  okCodes: number[] = [0],
): Promise<{
  code: number | null;
  ms: number;
  ok: boolean;
  lastZero: boolean;
}> {
  return new Promise((resolve) => {
    const t0 = Date.now();
    let lastZero = false;
    const p = spawn(cmd, args, {
      stdio: verbose ? "inherit" : "ignore",
      env: { ...process.env, ...envExtra },
    });
    p.on("exit", (code) => {
      lastZero = code === 0;
      const ok = code !== null && okCodes.includes(code);
      resolve({ code, ms: Date.now() - t0, ok, lastZero });
    });
    p.on("error", () => {
      resolve({
        code: 1,
        ms: Date.now() - t0,
        ok: okCodes.includes(1),
        lastZero: false,
      });
    });
  });
}

const clamp = (x: number, lo: number, hi: number) =>
  Math.max(lo, Math.min(hi, x));
async function fileNonEmpty(p: string) {
  try {
    return (await fsStat(p)).size > 0;
  } catch {
    return false;
  }
}
const join0 = (items: string[]) =>
  Buffer.from(items.filter(Boolean).join("\0") + (items.length ? "\0" : ""));

const norm = (p: string) =>
  path.sep === "/" ? p : p.split(path.sep).join("/");
function rel(root: string, full: string): string {
  let r = path.relative(root, full);
  if (r === "" || r === ".") return "";
  if (path.sep !== "/") r = r.split(path.sep).join("/");
  return r;
}
const parentDir = (r: string) => norm(path.posix.dirname(r || ".")); // "" -> "."

function relDepth(rootAbs: string, absPath: string): number {
  const r = norm(path.relative(rootAbs, absPath));
  if (!r || r === ".") return 0;
  return r.split("/").length - 1;
}

// collapse many siblings under a parent if possible
function minimalCover(dirs: string[]): string[] {
  const sorted = Array.from(new Set(dirs)).sort((a, b) => a.length - b.length);
  const out: string[] = [];
  for (const d of sorted) {
    if (
      !out.some((p) => d === p || d.startsWith(p.endsWith("/") ? p : p + "/"))
    )
      out.push(d);
  }
  return out;
}

// ---------- scheduler state ----------
let running = false,
  pending = false,
  lastCycleMs = 0,
  nextDelayMs = 10_000,
  backoffMs = 0;
const MIN_INTERVAL_MS = 7_500,
  MAX_INTERVAL_MS = 60_000,
  MAX_BACKOFF_MS = 600_000,
  JITTER_MS = 500;

function requestSoon(reason: string) {
  pending = true;
  nextDelayMs = clamp(
    Math.min(nextDelayMs, 3000),
    MIN_INTERVAL_MS,
    MAX_INTERVAL_MS,
  );
  log("info", "scheduler", `event-triggered rescan scheduled: ${reason}`);
}

// ---------- hot (realtime) sets ----------
const hotAlpha = new Set<string>(); // rpaths relative to alphaRoot
const hotBeta = new Set<string>(); // rpaths relative to betaRoot
let hotTimer: NodeJS.Timeout | null = null;

function scheduleHotFlush() {
  if (hotTimer) return;
  hotTimer = setTimeout(async () => {
    hotTimer = null;
    if (hotAlpha.size === 0 && hotBeta.size === 0) return;
    const rpathsAlpha = Array.from(hotAlpha);
    const rpathsBeta = Array.from(hotBeta);
    hotAlpha.clear();
    hotBeta.clear();
    try {
      await microSync(rpathsAlpha, rpathsBeta);
    } catch (e: any) {
      log("warn", "realtime", "microSync failed", {
        err: String(e?.message || e),
      });
    } finally {
      if (hotAlpha.size || hotBeta.size) {
        scheduleHotFlush(); // run another micro pass if more landed
      }
      requestSoon("micro-sync complete");
    }
  }, 200); // short debounce
}

const cooldownMs = 300;
const lastPush = new Map<string, number>(); // rpath -> ts
function keepFresh(rpaths: string[]) {
  const now = Date.now();
  const out: string[] = [];
  for (const r of rpaths) {
    const t = lastPush.get(r) || 0;
    if (now - t >= cooldownMs) {
      out.push(r);
      lastPush.set(r, now);
    }
  }
  return out;
}

// ---------- HotWatchManager (bounded, depth-limited, with escalation) ----------
class HotWatchManager {
  private map = new Map<string, { watcher: FSWatcher; expiresAt: number }>();
  private lru: string[] = []; // oldest first

  constructor(
    private side: "alpha" | "beta",
    private root: string,
    private onHotEvent: (abs: string, ev: string) => void,
  ) {
    console.log("created hot watch manager", this.side);
  }

  size() {
    return this.map.size;
  }

  async add(rdir: string) {
    rdir = rdir === "" ? "." : rdir;
    const anchorAbs = norm(path.join(this.root, rdir));
    const now = Date.now();

    if (this.map.has(anchorAbs)) {
      this.bump(anchorAbs);
      this.map.get(anchorAbs)!.expiresAt = now + HOT_TTL_MS;
      return;
    }

    const watcher = chokidar.watch(anchorAbs, {
      persistent: true,
      ignoreInitial: true,
      depth: HOT_DEPTH, // limit depth under this anchor
      awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
    });

    const handler = async (ev: string, abs: string) => {
      // realtime push for the concrete file/dir
      this.onHotEvent(abs, ev);

      // escalate deeper when event is at frontier depth
      const d = relDepth(anchorAbs, abs);
      if (d >= HOT_DEPTH && this.map.size < MAX_HOT_WATCHERS) {
        const deeperDir = norm(path.dirname(abs));
        const r = norm(path.relative(this.root, deeperDir));
        if (r && r !== ".") await this.add(r);
      }

      // refresh TTL & LRU
      const st = this.map.get(anchorAbs);
      if (st) st.expiresAt = Date.now() + HOT_TTL_MS;
      this.bump(anchorAbs);
    };

    ["add", "change", "unlink", "addDir", "unlinkDir"].forEach((evt) => {
      watcher.on(evt as any, (p: string) => handler(evt, p));
    });

    this.map.set(anchorAbs, { watcher, expiresAt: now + HOT_TTL_MS });
    this.lru.push(anchorAbs);
    await this.evictIfNeeded();
  }

  private bump(abs: string) {
    const i = this.lru.indexOf(abs);
    if (i >= 0) {
      this.lru.splice(i, 1);
      this.lru.push(abs);
    }
  }

  private async evictIfNeeded() {
    const now = Date.now();
    for (const [abs, st] of Array.from(this.map)) {
      if (st.expiresAt <= now) await this.drop(abs);
    }
    while (this.map.size > MAX_HOT_WATCHERS) {
      const victim = this.lru.shift();
      if (!victim) break;
      if (this.map.has(victim)) await this.drop(victim);
    }
  }

  private async drop(abs: string) {
    const st = this.map.get(abs);
    if (!st) return;
    await st.watcher.close().catch(() => {});
    this.map.delete(abs);
    const i = this.lru.indexOf(abs);
    if (i >= 0) this.lru.splice(i, 1);
  }

  async closeAll() {
    await Promise.all(
      Array.from(this.map.values()).map((s) =>
        s.watcher.close().catch(() => {}),
      ),
    );
    this.map.clear();
    this.lru = [];
  }
}

// ---------- shallow root watchers + hot managers ----------
function onAlphaHot(abs: string, evt: string) {
  const r = rel(alphaRoot, abs);
  if (r && (evt === "change" || evt === "add" || evt === "unlink")) {
    hotAlpha.add(r);
    scheduleHotFlush();
  }
}
function onBetaHot(abs: string, evt: string) {
  const r = rel(betaRoot, abs);
  if (r && (evt === "change" || evt === "add" || evt === "unlink")) {
    hotBeta.add(r);
    scheduleHotFlush();
  }
}

const hotAlphaMgr = new HotWatchManager("alpha", alphaRoot, onAlphaHot);
const hotBetaMgr = new HotWatchManager("beta", betaRoot, onBetaHot);

// very cheap root watchers
const shallowAlpha = chokidar.watch(alphaRoot, {
  persistent: true,
  ignoreInitial: true,
  depth: SHALLOW_DEPTH,
  awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
});
const shallowBeta = chokidar.watch(betaRoot, {
  persistent: true,
  ignoreInitial: true,
  depth: SHALLOW_DEPTH,
  awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
});

// any shallow signal -> start (or refresh) a hot subtree watcher on its parent
["add", "change", "unlink", "addDir", "unlinkDir"].forEach((evt) => {
  shallowAlpha.on(evt as any, async (p: string) => {
    const rdir = parentDir(rel(alphaRoot, p));
    if (rdir) await hotAlphaMgr.add(rdir);
  });
  shallowBeta.on(evt as any, async (p: string) => {
    const rdir = parentDir(rel(betaRoot, p));
    if (rdir) await hotBetaMgr.add(rdir);
  });
});

// ---------- seed hot watchers from DB recent_touch ----------
function seedHotFromDb(
  dbPath: string,
  root: string,
  mgr: HotWatchManager,
  sinceTs: number | null,
  maxDirs = 256,
) {
  const sdb = new Database(dbPath);
  try {
    const rows = sinceTs
      ? sdb
          .prepare(
            `SELECT path FROM recent_touch WHERE ts >= ? ORDER BY ts DESC LIMIT ?`,
          )
          .all(sinceTs, maxDirs * 8)
      : sdb
          .prepare(`SELECT path FROM recent_touch ORDER BY ts DESC LIMIT ?`)
          .all(maxDirs * 8);

    const dirs = rows
      .map((r: any) => parentDir(norm(path.relative(root, r.path))))
      .filter(Boolean);
    const covered = minimalCover(dirs).slice(0, maxDirs);
    covered.forEach((d) => mgr.add(d));
    if (verbose)
      log("info", "watch", `seeded ${covered.length} hot dirs from ${dbPath}`);
  } catch {
    // table may not exist yet; ignore
  } finally {
    sdb.close();
  }
}

// ---------- realtime micro-sync (unchanged from your version) ----------
async function microSync(rpathsAlpha: string[], rpathsBeta: string[]) {
  // Decide direction per rpath using just the event sets
  const setA = new Set(rpathsAlpha);
  const setB = new Set(rpathsBeta);

  const toBeta: string[] = [];
  const toAlpha: string[] = [];

  const touched = new Set<string>([...setA, ...setB]);
  for (const r of touched) {
    const aTouched = setA.has(r);
    const bTouched = setB.has(r);
    if (aTouched && bTouched) {
      if (prefer === "alpha") toBeta.push(r);
      else toAlpha.push(r);
    } else if (aTouched) {
      toBeta.push(r);
    } else {
      toAlpha.push(r);
    }
  }

  // Filter: only regular files for now (skip dirs/links)
  async function keepFiles(root: string, rpaths: string[]) {
    const out: string[] = [];
    for (const r of rpaths) {
      try {
        const st = await fsLstat(path.join(root, r));
        if (st.isFile()) out.push(r);
      } catch {
        /* file might have vanished; ignore in micro pass */
      }
    }
    return out;
  }
  const [toBetaFiles, toAlphaFiles] = (
    await Promise.all([
      keepFiles(alphaRoot, toBeta),
      keepFiles(betaRoot, toAlpha),
    ])
  ).map(keepFresh);

  if (toBetaFiles.length === 0 && toAlphaFiles.length === 0) return;

  const tmp = await mkdtemp(path.join(tmpdir(), "micro-plan-"));
  const listToBeta = path.join(tmp, "toBeta.list");
  const listToAlpha = path.join(tmp, "toAlpha.list");

  await writeFile(listToBeta, join0(toBetaFiles));
  await writeFile(listToAlpha, join0(toAlphaFiles));

  // Run small rsyncs now. Accept partial codes (23/24) so we don't blow up on edits-in-flight.
  if (await fileNonEmpty(listToBeta)) {
    log("info", "realtime", `alpha→beta ${toBetaFiles.length} paths`);
    await spawnTask(
      "rsync",
      [
        ...(dryRun ? ["-n"] : []),
        "-a",
        "--inplace",
        "--relative",
        "--from0",
        `--files-from=${listToBeta}`,
        alphaRoot.endsWith("/") ? alphaRoot : alphaRoot + "/",
        betaRoot.endsWith("/") ? betaRoot : betaRoot + "/",
      ],
      {},
      [0, 23, 24],
    );
  }

  if (await fileNonEmpty(listToAlpha)) {
    log("info", "realtime", `beta→alpha ${toAlphaFiles.length} paths`);
    await spawnTask(
      "rsync",
      [
        ...(dryRun ? ["-n"] : []),
        "-a",
        "--inplace",
        "--relative",
        "--from0",
        `--files-from=${listToAlpha}`,
        betaRoot.endsWith("/") ? betaRoot : betaRoot + "/",
        alphaRoot.endsWith("/") ? alphaRoot : alphaRoot + "/",
      ],
      {},
      [0, 23, 24],
    );
  }
  // No base updates here — the full cycle will verify and finalize.
}

// ---------- full cycle ----------
async function oneCycle(): Promise<void> {
  running = true;
  const t0 = Date.now();

  // Scan alpha & beta (record start times so we can seed recent touches since each scan)
  const tAlphaStart = Date.now();
  log("info", "scan", `alpha: ${alphaRoot}`);
  const a = await spawnTask("tsx", ["src/scan.ts", alphaRoot], {
    DB_PATH: alphaDb,
  });
  seedHotFromDb(alphaDb, alphaRoot, hotAlphaMgr, tAlphaStart, 256);

  const tBetaStart = Date.now();
  log("info", "scan", `beta: ${betaRoot}`);
  const b = await spawnTask("tsx", ["src/scan.ts", betaRoot], {
    DB_PATH: betaDb,
  });
  seedHotFromDb(betaDb, betaRoot, hotBetaMgr, tBetaStart, 256);

  // Merge/rsync (full)
  log("info", "merge", `prefer=${prefer} dryRun=${dryRun}`);
  const mArgs = [
    "src/merge-rsync.ts",
    "--alpha-root",
    alphaRoot,
    "--beta-root",
    betaRoot,
    "--alpha-db",
    alphaDb,
    "--beta-db",
    betaDb,
    "--base-db",
    baseDb,
    "--prefer",
    prefer,
  ];
  if (dryRun) mArgs.push("--dry-run", "true");
  if (verbose) mArgs.push("--verbose", "true");
  const m = await spawnTask("tsx", mArgs);

  const ms = Date.now() - t0;
  lastCycleMs = ms;
  log("info", "scheduler", `cycle complete in ${ms} ms`, {
    scanAlphaMs: a.ms,
    scanBetaMs: b.ms,
    mergeMs: m.ms,
    codes: { a: a.code, b: b.code, m: m.code },
  });

  // Backoff on merge errors
  if (m.code && m.code !== 0) {
    const code = m.code ?? -1;
    const warn = code === 23 || code === 24;
    const enospc = code === 28;
    if (enospc) {
      log("error", "rsync", "ENOSPC; backoff", { code });
      backoffMs = Math.min(backoffMs ? backoffMs * 2 : 10_000, MAX_BACKOFF_MS);
    } else if (warn) {
      log("warn", "rsync", "partial; backoff a bit", { code });
      backoffMs = Math.min(backoffMs ? backoffMs + 5_000 : 5_000, 60_000);
    } else {
      log("error", "rsync", "unexpected error; backoff", { code });
      backoffMs = Math.min(backoffMs ? backoffMs * 2 : 10_000, MAX_BACKOFF_MS);
    }
  } else backoffMs = 0;

  running = false;
}

async function loop() {
  while (true) {
    if (!running) {
      pending = false;
      await oneCycle();
      // Dynamic interval: aim for ~2x last cycle, clamped.
      const baseNext = clamp(lastCycleMs * 2, MIN_INTERVAL_MS, MAX_INTERVAL_MS);
      // If there was backoff (errors), add it; otherwise a small jitter.
      nextDelayMs =
        baseNext + (backoffMs || Math.floor(Math.random() * JITTER_MS));
    }

    // If something changed while we were running, run again sooner.
    if (pending) {
      nextDelayMs = clamp(1500, MIN_INTERVAL_MS, MAX_INTERVAL_MS);
      pending = false;
      continue; // loop immediately
    }

    log("info", "scheduler", "watching", `next full scan in ${nextDelayMs} ms`);
    await new Promise((r) => setTimeout(r, nextDelayMs));
  }
}

log("info", "scheduler", "starting", {
  alphaRoot,
  betaRoot,
  alphaDb,
  betaDb,
  baseDb,
  prefer,
  dryRun,
  verbose,
  MAX_HOT_WATCHERS,
  HOT_TTL_MS,
  SHALLOW_DEPTH,
  HOT_DEPTH,
});

// cheap root watchers start immediately; hot managers add anchors on demand
loop().catch((e) => {
  log("error", "scheduler", "fatal", { err: String(e?.stack || e) });
  process.exit(1);
});
