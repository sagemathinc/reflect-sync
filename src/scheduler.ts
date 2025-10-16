// Minimal orchestrator:
// - Runs scan(alpha), scan(beta), then merge-rsync
// - Dynamic interval: next = clamp(2x lastCycleMs, min=3s, max=120s)
// - chokidar watches both roots; any change debounces a "run soon"
// - Logs events to base.db (table 'events')
// - Simple backoff on rsync failures (up to MAX_BACKOFF_MS)
//
// Usage example:
//   tsx src/scheduler.ts \
//     --alpha-root /srv/alpha \
//     --beta-root  /srv/beta  \
//     --alpha-db   alpha.db   \
//     --beta-db    beta.db    \
//     --base-db    base.db    \
//     --prefer     alpha      \
//     --verbose    true
//
// Optional env to pass scan tuning (you already support DB_PATH):
//   SCAN_CONCURRENCY=128 SCAN_DISPATCH_BATCH=256 SCAN_DB_BATCH=2000

import { spawn } from "node:child_process";
import chokidar from "chokidar";
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

// ---------- scheduler state ----------
let running = false,
  pending = false,
  lastCycleMs = 0,
  nextDelayMs = 10_000,
  backoffMs = 0;
const MIN_INTERVAL_MS = 7_500,
  MAX_INTERVAL_MS = 120_000,
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
function rel(root: string, full: string): string {
  let r = path.relative(root, full);
  if (r === "" || r === ".") return "";
  if (path.sep !== "/") r = r.split(path.sep).join("/");
  return r;
}
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
      // If more events landed while we were syncing, run another micro batch right away.
      if (hotAlpha.size || hotBeta.size) {
        scheduleHotFlush(); // run another micro pass
      }
      // ask main loop to run soon to verify and update base
      requestSoon("micro-sync complete");
    }
  }, 200); // short debounce
}

// ---------- chokidar ----------
const watcher = chokidar.watch([alphaRoot, betaRoot], {
  persistent: true,
  ignoreInitial: true,
  depth: 2,
  awaitWriteFinish: { stabilityThreshold: 150, pollInterval: 50 },
});
["add", "change", "unlink"].forEach((evt) => {
  watcher.on(evt as any, (p) => {
    if (p.startsWith(alphaRoot)) {
      const r = rel(alphaRoot, p);
      if (r) {
        hotAlpha.add(r);
        scheduleHotFlush();
      }
    } else if (p.startsWith(betaRoot)) {
      const r = rel(betaRoot, p);
      if (r) {
        hotBeta.add(r);
        scheduleHotFlush();
      }
    }
  });
});

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
      // conflict right now: push prefer side
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

  // No base updates here — the normal full cycle will verify and finalize.
}

// ---------- full cycle ----------
async function oneCycle(): Promise<void> {
  running = true;
  const t0 = Date.now();

  // Scan alpha & beta
  log("info", "scan", `alpha: ${alphaRoot}`);
  const a = await spawnTask("tsx", ["src/scan.ts", alphaRoot], {
    DB_PATH: alphaDb,
  });
  log("info", "scan", `beta: ${betaRoot}`);
  const b = await spawnTask("tsx", ["src/scan.ts", betaRoot], {
    DB_PATH: betaDb,
  });

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
      // Dynamic interval: aim for ~4x last cycle, clamped.
      const baseNext = clamp(lastCycleMs * 4, MIN_INTERVAL_MS, MAX_INTERVAL_MS);
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
});
loop().catch((e) => {
  log("error", "scheduler", "fatal", { err: String(e?.stack || e) });
  process.exit(1);
});
