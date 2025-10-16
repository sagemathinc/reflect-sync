// src/scheduler.ts
//
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
//
// Requires deps: better-sqlite3, chokidar

import { spawn } from "node:child_process";
import chokidar from "chokidar";
import Database from "better-sqlite3";

type Args = Record<string, string | boolean>;
function parseArgs(): Args {
  const out: Args = {};
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const [k, maybeV] = a.slice(2).split("=");
      if (maybeV !== undefined) {
        out[k] = parseBool(maybeV);
      } else {
        const v = argv[i + 1];
        if (!v || v.startsWith("--")) {
          out[k] = true;
        } else {
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

// ------------ event log in base.db -------------
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
  } catch {
    /* ignore logging failures */
  }
  if (verbose || level !== "info") {
    const tag = level === "error" ? "⛔" : level === "warn" ? "⚠️ " : "ℹ️ ";
    console.log(
      tag,
      `[${source}]`,
      msg,
      details ? JSON.stringify(details) : "",
    );
  }
}

// ------------ helpers -------------
function spawnTask(
  cmd: string,
  args: string[],
  envExtra: Record<string, string> = {},
): Promise<{ code: number | null; ms: number }> {
  return new Promise((resolve) => {
    const t0 = Date.now();
    const p = spawn(cmd, args, {
      stdio: verbose ? "inherit" : "ignore",
      env: { ...process.env, ...envExtra },
    });
    p.on("exit", (code) => resolve({ code, ms: Date.now() - t0 }));
    p.on("error", () => resolve({ code: 1, ms: Date.now() - t0 }));
  });
}
const clamp = (x: number, lo: number, hi: number) =>
  Math.max(lo, Math.min(hi, x));

// ------------ scheduler state -------------
let running = false;
let pending = false;
let lastCycleMs = 0;
let nextDelayMs = 5000; // initial
let backoffMs = 0;
const MIN_INTERVAL_MS = 3000;
const MAX_INTERVAL_MS = 60_000;
const MAX_BACKOFF_MS = 10 * 60_000;
const JITTER_MS = 500;

// Triggered by fs events to pull next run earlier
function requestSoon(reason: string) {
  pending = true;
  // bring next run forward, but never below MIN_INTERVAL_MS
  nextDelayMs = clamp(
    Math.min(nextDelayMs, 3000),
    MIN_INTERVAL_MS,
    MAX_INTERVAL_MS,
  );
  log("info", "scheduler", `event-triggered rescan scheduled: ${reason}`);
}

// ------------ chokidar watchers -------------
// Simple version: watch both roots; rely on ignore patterns later.
const watcher = chokidar.watch([alphaRoot, betaRoot], {
  persistent: true,
  ignoreInitial: true,
  awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
  depth: undefined, // full depth
});
["add", "change", "unlink", "addDir", "unlinkDir"].forEach((evt) => {
  watcher.on(evt as any, (p: string) => {
    // Only react to likely file changes (add/change/unlink)
    if (evt === "change" || evt === "add" || evt === "unlink") {
      requestSoon(`${evt}:${p}`);
    }
  });
});

// ------------ main loop -------------
async function oneCycle(): Promise<void> {
  running = true;
  const t0 = Date.now();

  // 1) scan alpha
  log("info", "scan", `scanning alpha: ${alphaRoot}`);
  const a = await spawnTask("tsx", ["src/scan.ts", alphaRoot], {
    DB_PATH: alphaDb,
  });

  // 2) scan beta
  log("info", "scan", `scanning beta: ${betaRoot}`);
  const b = await spawnTask("tsx", ["src/scan.ts", betaRoot], {
    DB_PATH: betaDb,
  });

  // 3) merge + rsync
  log("info", "merge", `planning/rsync prefer=${prefer} dryRun=${dryRun}`);
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

  // Basic failure handling
  if (m.code && m.code !== 0) {
    // Common rsync exit codes: 23/24 partial, 28 ENOSPC, treat as warnings with backoff
    const code = m.code ?? -1;
    const warn = code === 23 || code === 24;
    const enospc = code === 28;
    if (enospc) {
      log("error", "rsync", "ENOSPC detected; backing off", { code });
      backoffMs = Math.min(backoffMs ? backoffMs * 2 : 10_000, MAX_BACKOFF_MS);
    } else if (warn) {
      log("warn", "rsync", "partial transfer; backing off a bit", { code });
      backoffMs = Math.min(backoffMs ? backoffMs + 5_000 : 5_000, 60_000);
    } else {
      log("error", "rsync", "unexpected error; backing off", { code });
      backoffMs = Math.min(backoffMs ? backoffMs * 2 : 10_000, MAX_BACKOFF_MS);
    }
  } else {
    backoffMs = 0;
  }

  running = false;
}

async function loop() {
  while (true) {
    if (!running) {
      pending = false;
      await oneCycle();
      // Dynamic interval: aim for ~5x last cycle, clamped.
      const baseNext = clamp(lastCycleMs * 5, MIN_INTERVAL_MS, MAX_INTERVAL_MS);
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
