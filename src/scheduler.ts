#!/usr/bin/env node
// - Commander-based CLI
// - Exported runScheduler(opts) API for programmatic use
// - Optional SSH micro-sync: remote watch -> tee to ingest + realtime push
//
// Notes:
// * Local watchers only; remote changes arrive via remote watch stream.
// * Full-cycle still uses "ccsync merge" (see merge.ts).

import { spawn, SpawnOptions, ChildProcess } from "node:child_process";
import chokidar from "chokidar";
import path from "node:path";
import { Command, Option } from "commander";
import readline from "node:readline";
import { cliEntrypoint } from "./cli-util.js";
import {
  HotWatchManager,
  minimalCover,
  norm,
  parentDir,
  handleWatchErrors,
  isRecent,
} from "./hotwatch.js";
import { ensureSessionDb, SessionWriter } from "./session-db.js";
import { MAX_WATCHERS } from "./defaults.js";
import { makeMicroSync } from "./micro-sync.js";
import { PassThrough } from "node:stream";
import { getBaseDb, getDb } from "./db.js";

export type SchedulerOptions = {
  alphaRoot: string;
  betaRoot: string;
  alphaDb: string;
  betaDb: string;
  baseDb: string;
  prefer: "alpha" | "beta";
  verbose: boolean;
  dryRun: boolean;

  alphaHost?: string;
  betaHost?: string;
  alphaRemoteDb: string;
  betaRemoteDb: string;

  remoteScanCmd: string; // e.g. "ccsync scan"
  remoteWatchCmd: string; // e.g. "ccsync watch"
  disableHotWatch: boolean;

  sessionDb?: string;
  sessionId?: number;
};

// ---------- CLI ----------
function buildProgram(): Command {
  const program = new Command()
    .name("ccsync-scheduler")
    .description("Orchestration of scanning, watching, syncing");

  program
    .requiredOption("--alpha-root <path>", "path to root of alpha sync tree")
    .requiredOption("--beta-root <path>", "path to root of beta sync tree")
    .option("--alpha-db <file>", "path to alpha sqlite database", "alpha.db")
    .option("--beta-db <file>", "path to beta sqlite database", "beta.db")
    .option("--base-db <file>", "path to base sqlite database", "base.db")
    .addOption(
      new Option(
        "--prefer <side>",
        "conflict preference: all conflicts are resolved in favor of this side",
      )
        .choices(["alpha", "beta"])
        .default("alpha"),
    )
    .option("--verbose", "enable verbose logging", false)
    .option("--dry-run", "simulate without changing files", false)
    // optional SSH endpoints (only one side may be remote)
    .option("--alpha-host <ssh>", "SSH host for alpha (e.g. user@host)")
    .option("--beta-host <ssh>", "SSH host for beta (e.g. user@host)")
    .option(
      "--alpha-remote-db <file>",
      "remote path to alpha sqlite db (on the SSH host)",
      `${process.env.HOME ?? ""}/.cache/cocalc-sync/alpha.db`,
    )
    .option(
      "--beta-remote-db <file>",
      "remote path to beta sqlite db (on the SSH host)",
      `${process.env.HOME ?? ""}/.cache/cocalc-sync/beta.db`,
    )
    // commands to run on remote for scan/micro-watch
    .option("--remote-scan-cmd <cmd>", "remote scan command", "ccsync scan")
    .option(
      "--remote-watch-cmd <cmd>",
      "remote watch command for micro-sync (emits NDJSON lines)",
      "ccsync watch",
    )
    .option(
      "--disable-hot-watch",
      "only sync during the full sync cycle",
      false,
    )
    .option("--session-id <id>", "optional session id to enable heartbeats")
    .option("--session-db <path>", "path to session database");

  return program;
}

function cliOptsToSchedulerOptions(opts): SchedulerOptions {
  const out: SchedulerOptions = {
    alphaRoot: String(opts.alphaRoot),
    betaRoot: String(opts.betaRoot),
    alphaDb: String(opts.alphaDb),
    betaDb: String(opts.betaDb),
    baseDb: String(opts.baseDb),
    prefer: String(opts.prefer).toLowerCase() as "alpha" | "beta",
    verbose: !!opts.verbose,
    dryRun: !!opts.dryRun,
    alphaHost: opts.alphaHost?.trim() || undefined,
    betaHost: opts.betaHost?.trim() || undefined,
    alphaRemoteDb: String(opts.alphaRemoteDb),
    betaRemoteDb: String(opts.betaRemoteDb),
    remoteScanCmd: String(opts.remoteScanCmd),
    remoteWatchCmd: String(opts.remoteWatchCmd),
    disableHotWatch: !!opts.disableHotWatch,
    sessionId: opts.sessionId != null ? Number(opts.sessionId) : undefined,
    sessionDb: opts.sessionDb,
  };

  if (out.alphaHost && out.betaHost) {
    console.error("Both sides remote is not supported yet (rsync two-remote).");
    process.exit(1);
  }

  return out;
}

// ---------- env/test knobs ----------
const envNum = (k: string, def: number) =>
  process.env[k] ? Number(process.env[k]) : def;

const MAX_HOT_WATCHERS = envNum("MAX_HOT_WATCHERS", 16);
const HOT_TTL_MS = envNum("HOT_TTL_MS", 30 * 60_000);
const SHALLOW_DEPTH = envNum("SHALLOW_DEPTH", 1);
const HOT_DEPTH = envNum("HOT_DEPTH", 2);

const MICRO_DEBOUNCE_MS = envNum("MICRO_DEBOUNCE_MS", 200);

const MIN_INTERVAL_MS = envNum("SCHED_MIN_MS", 7_500);
const MAX_INTERVAL_MS = envNum("SCHED_MAX_MS", 60_000);
const MAX_BACKOFF_MS = envNum("SCHED_MAX_BACKOFF_MS", 600_000);
const JITTER_MS = envNum("SCHED_JITTER_MS", 500);

// ---------- core (exported) ----------
export async function runScheduler({
  alphaRoot,
  betaRoot,
  alphaDb,
  betaDb,
  baseDb,
  prefer,
  verbose,
  dryRun,
  alphaHost,
  betaHost,
  alphaRemoteDb,
  betaRemoteDb,
  remoteScanCmd,
  remoteWatchCmd,
  disableHotWatch,
  sessionDb,
  sessionId,
}: SchedulerOptions): Promise<void> {
  if (!alphaRoot || !betaRoot)
    throw new Error("Need --alpha-root and --beta-root");
  if (alphaHost && betaHost)
    throw new Error(
      "Both sides remote is not supported yet (rsync two-remote).",
    );

  // ---------- scheduler state ----------
  let running = false,
    pending = false,
    lastCycleMs = 0,
    nextDelayMs = 10_000,
    backoffMs = 0;

  const alphaIsRemote = !!alphaHost;
  const betaIsRemote = !!betaHost;

  // heartbeat interval (ms)
  const HEARTBEAT_MS = Number(process.env.HEARTBEAT_MS ?? 2000);
  let sessionWriter: SessionWriter | null = null;
  let hbTimer: NodeJS.Timeout | null = null;
  if (sessionDb && Number.isFinite(sessionId)) {
    sessionWriter = SessionWriter.open(sessionDb, sessionId!);
    sessionWriter.start();
    hbTimer = setInterval(() => {
      sessionWriter!.heartbeat(running, pending, lastCycleMs, backoffMs);
    }, HEARTBEAT_MS);
  }

  // ---------- logging ----------
  const db = getBaseDb(baseDb);
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
    okCodes: number[] = [0],
    extra: SpawnOptions = {},
  ): Promise<{
    code: number | null;
    ms: number;
    ok: boolean;
    lastZero: boolean;
  }> {
    if (verbose) console.log(`${cmd} ${args.join(" ")}`);
    return new Promise((resolve) => {
      const t0 = Date.now();
      let lastZero = false;
      const p = spawn(cmd, args, {
        stdio: verbose ? "inherit" : "ignore",
        ...extra,
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

  function rel(root: string, full: string): string {
    let r = path.relative(root, full);
    if (r === "" || r === ".") return "";
    if (path.sep !== "/") r = r.split(path.sep).join("/");
    return r;
  }

  // Pipe: ssh scan --emit-delta  →  ccsync ingest --db <local.db>
  function splitCmd(s: string): string[] {
    return s.trim().split(/\s+/);
  }

  // ssh to a remote, run a scan, and writing the resulting
  // data into our local database.
  const lastRemoteScan = { start: 0, ok: false };
  async function sshScanIntoMirror(params: {
    host: string;
    remoteScanCmd: string;
    root: string;
    localDb: string;
  }): Promise<{
    code: number | null;
    ms: number;
    ok: boolean;
    lastZero: boolean;
  }> {
    const t0 = Date.now();
    const sshArgs = [
      "-C",
      params.host,
      ...splitCmd(params.remoteScanCmd),
      "--root",
      params.root,
      "--emit-delta",
      // use same path as local DB, but on remote:
      // [ ] TODO: this isn't going to be right in general!
      "--db",
      params.localDb,
      "--vacuum",
    ];
    if (lastRemoteScan.ok && lastRemoteScan.start) {
      sshArgs.push("--prune-ms");
      sshArgs.push(`${Date.now() - lastRemoteScan.start}`);
    }
    lastRemoteScan.start = Date.now();
    lastRemoteScan.ok = false;

    if (verbose) console.log("$ ssh", sshArgs.join(" "));

    const sshP = spawn("ssh", sshArgs, {
      stdio: ["ignore", "pipe", verbose ? "inherit" : "ignore"],
    });

    const ingestArgs = ["ingest", "--db", params.localDb];
    if (verbose) console.log("ccsync", ingestArgs.join(" "));
    const ingestP = spawn("ccsync", ingestArgs, {
      stdio: [
        "pipe",
        verbose ? "inherit" : "ignore",
        verbose ? "inherit" : "ignore",
      ],
    });

    if (sshP.stdout && ingestP.stdin) sshP.stdout.pipe(ingestP.stdin);

    const wait = (p: ChildProcess) =>
      new Promise<number | null>((resolve) => p.on("exit", (c) => resolve(c)));
    const [sshCode, ingestCode] = await Promise.all([
      wait(sshP),
      wait(ingestP),
    ]);
    const ok = sshCode === 0 && ingestCode === 0;
    lastRemoteScan.ok = ok;
    return {
      code: ok ? 0 : (sshCode ?? ingestCode),
      ms: Date.now() - t0,
      ok,
      lastZero: ok,
    };
  }

  // Remote delta stream (watch): ssh "<remoteWatchCmd> --root <root>"
  // tee stdout to: (a) local ingest, and (b) our line-reader for microSync cues.
  function startRemoteDeltaStream(side: "alpha" | "beta") {
    if (disableHotWatch) return null;
    const host = side === "alpha" ? alphaHost : betaHost;
    if (!host) return null;
    const root = side === "alpha" ? alphaRoot : betaRoot;
    const localDb = side === "alpha" ? alphaDb : betaDb;

    const sshArgs = [
      "-C",
      "-T",
      "-o",
      "BatchMode=yes",
      host,
      ...splitCmd(remoteWatchCmd),
      "--root",
      root,
    ];
    if (verbose) console.log("$ ssh", sshArgs.join(" "));

    // stdin=pipe so we can send EOF to make remote `watch` exit
    const sshP = spawn("ssh", sshArgs, {
      stdio: ["pipe", "pipe", "inherit"],
    });

    const ingestArgs = ["ingest", "--db", localDb];
    const ingestP = spawn("ccsync", ingestArgs, {
      stdio: [
        "pipe",
        verbose ? "inherit" : "ignore",
        verbose ? "inherit" : "ignore",
      ],
    });

    const tee = new PassThrough();
    if (sshP.stdout) {
      sshP.stdout.pipe(tee);
      tee.pipe(ingestP.stdin!);
    }

    const rl = readline.createInterface({ input: tee });
    rl.on("line", (line) => {
      if (!line) return;
      try {
        const evt = JSON.parse(line);
        if (!evt.path) return;
        let r = String(evt.path);
        if (r.startsWith(root)) r = r.slice(root.length);
        if (r.startsWith("/")) r = r.slice(1);
        if (!r) return;
        (side === "alpha" ? hotAlpha : hotBeta).add(r);
        scheduleHotFlush();
      } catch {
        /* ignore malformed */
      }
    });

    const kill = () => {
      try {
        rl.close();
      } catch {}
      try {
        ingestP.stdin?.end();
      } catch {}
      try {
        sshP.stdin?.end();
      } catch {} // <— send EOF to remote watch
      // Give it a moment to exit cleanly on EOF
      setTimeout(() => {
        try {
          ingestP.kill("SIGTERM");
        } catch {}
        try {
          sshP.kill("SIGTERM");
        } catch {}
      }, 300);
    };

    sshP.on("exit", () => {
      kill();
      if (side === "alpha") alphaStream = null;
      else betaStream = null;
    });

    ingestP.on("exit", () => {
      kill();
      if (side === "alpha") alphaStream = null;
      else betaStream = null;
    });

    const add = (dirs: string[]) => {
      if (!dirs?.length) return;
      try {
        sshP.stdin?.write(JSON.stringify({ op: "add", dirs }) + "\n");
      } catch {}
    };

    return { add, kill };
  }

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
  const hotAlpha = new Set<string>();
  const hotBeta = new Set<string>();
  let hotTimer: NodeJS.Timeout | null = null;

  // create the microSync closure
  const microSync = makeMicroSync({
    alphaRoot,
    betaRoot,
    alphaHost,
    betaHost,
    prefer,
    dryRun,
    verbose,
    spawnTask,
    log,
  });

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
        // run another micro pass if more landed
        if (hotAlpha.size || hotBeta.size) scheduleHotFlush();
        requestSoon("micro-sync complete");
      }
    }, MICRO_DEBOUNCE_MS);
  }

  // ---------- root watchers (locals only) ----------
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

  const hotAlphaMgr =
    alphaIsRemote || disableHotWatch
      ? null
      : new HotWatchManager(alphaRoot, onAlphaHot, {
          maxWatchers: MAX_HOT_WATCHERS,
          ttlMs: HOT_TTL_MS,
          hotDepth: HOT_DEPTH,
          awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
          verbose,
        });

  const hotBetaMgr =
    betaIsRemote || disableHotWatch
      ? null
      : new HotWatchManager(betaRoot, onBetaHot, {
          maxWatchers: MAX_HOT_WATCHERS,
          ttlMs: HOT_TTL_MS,
          hotDepth: HOT_DEPTH,
          awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
          verbose,
        });

  const shallowAlpha =
    alphaIsRemote || disableHotWatch
      ? null
      : chokidar.watch(alphaRoot, {
          persistent: true,
          ignoreInitial: true,
          depth: SHALLOW_DEPTH,
          awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
          followSymlinks: false,
          alwaysStat: false,
        });
  const shallowBeta =
    betaIsRemote || disableHotWatch
      ? null
      : chokidar.watch(betaRoot, {
          persistent: true,
          ignoreInitial: true,
          depth: SHALLOW_DEPTH,
          awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
          followSymlinks: false,
          alwaysStat: false,
        });

  function enableWatch({ watcher, root, mgr, hot }) {
    handleWatchErrors(watcher);
    ["add", "change", "unlink", "addDir", "unlinkDir"].forEach((evt) => {
      watcher.on(evt as any, async (p: string, stats) => {
        const r = rel(root, p);
        if (mgr.isIgnored(r, (evt as string)?.endsWith("Dir"))) return;
        if (await isRecent(p, stats)) {
          const rdir = parentDir(r);
          if (rdir) {
            await mgr.add(rdir);
          }
        }
        if (r && (evt === "add" || evt === "change" || evt === "unlink")) {
          hot.add(r);
          scheduleHotFlush();
        }
      });
    });
  }

  if (shallowAlpha && hotAlphaMgr) {
    enableWatch({
      watcher: shallowAlpha,
      root: alphaRoot,
      mgr: hotAlphaMgr,
      hot: hotAlpha,
    });
  }
  if (shallowBeta && hotBetaMgr) {
    enableWatch({
      watcher: shallowBeta,
      root: betaRoot,
      mgr: hotBetaMgr,
      hot: hotBeta,
    });
  }

  // ---------- seed hot watchers from DB recent_touch ----------
  function seedHotFromDb(
    dbPath: string,
    mgr: HotWatchManager | null,
    remoteAdd: null | ((dirs: string[]) => void),
    sinceTs: number,
    maxDirs = MAX_WATCHERS,
  ) {
    if (disableHotWatch) return;
    const sdb = getDb(dbPath);
    try {
      const rows = sdb
        .prepare(
          `SELECT path FROM recent_touch WHERE ts >= ? ORDER BY ts DESC LIMIT ?`,
        )
        .all(sinceTs, maxDirs * 8);
      // clear the recently touched table to save space
      sdb.prepare("DELETE FROM recent_touch").run();
      const dirs = rows
        .map((r: any) => parentDir(norm(r.path)))
        .filter(Boolean);
      const covered = minimalCover(dirs).slice(0, maxDirs);
      if (mgr != null) covered.forEach((d) => mgr.add(d));
      remoteAdd?.(covered);
      if (verbose)
        log(
          "info",
          "watch",
          `seeded ${covered.length} hot dirs from ${dbPath}`,
        );
    } catch {
      // may not exist yet
    } finally {
      sdb.close();
    }
  }

  let addRemoteAlphaHotDirs: null | ((dirs: string[]) => void) = null;
  let addRemoteBetaHotDirs: null | ((dirs: string[]) => void) = null;

  // ---------- full cycle ----------
  async function oneCycle(): Promise<void> {
    running = true;
    const t0 = Date.now();

    // Scan alpha & beta in parallel
    let a: any, b: any;
    const scanAlpha = async () => {
      const tAlphaStart = Date.now();
      log(
        "info",
        "scan",
        `alpha: ${alphaRoot}${alphaHost ? ` @ ${alphaHost}` : ""}`,
      );
      a = alphaIsRemote
        ? await sshScanIntoMirror({
            host: alphaHost!,
            remoteScanCmd,
            root: alphaRoot,
            localDb: alphaDb,
          })
        : await spawnTask(
            "ccsync",
            ["scan", "--root", alphaRoot, "--db", alphaDb].concat(
              verbose ? ["--verbose"] : [],
            ),
          );
      seedHotFromDb(
        alphaDb,
        hotAlphaMgr,
        addRemoteAlphaHotDirs,
        tAlphaStart,
        MAX_WATCHERS,
      );
    };
    const scanBeta = async () => {
      const tBetaStart = Date.now();
      log(
        "info",
        "scan",
        `beta: ${betaRoot}${betaHost ? ` @ ${betaHost}` : ""}`,
      );
      b = betaIsRemote
        ? await sshScanIntoMirror({
            host: betaHost!,
            remoteScanCmd,
            root: betaRoot,
            localDb: betaDb,
          })
        : await spawnTask(
            "ccsync",
            ["scan", "--root", betaRoot, "--db", betaDb].concat(
              verbose ? ["--verbose"] : [],
            ),
          );
      seedHotFromDb(
        betaDb,
        hotBetaMgr,
        addRemoteBetaHotDirs,
        tBetaStart,
        MAX_WATCHERS,
      );
    };
    await Promise.all([scanAlpha(), scanBeta()]);

    // Merge/rsync (full)
    log("info", "merge", `prefer=${prefer} dryRun=${dryRun}`);
    const mArgs = [
      "merge",
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
    if (alphaHost) mArgs.push("--alpha-host", alphaHost);
    if (betaHost) mArgs.push("--beta-host", betaHost);
    if (dryRun) mArgs.push("--dry-run");
    if (verbose) mArgs.push("--verbose");

    const m = await spawnTask("ccsync", mArgs);

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
      sessionWriter?.error(`merge/rsync exit code ${m.code}`);
      const code = m.code ?? -1;
      const warn = code === 23 || code === 24;
      const enospc = code === 28;
      if (enospc) {
        log("error", "rsync", "ENOSPC; backoff", { code });
        backoffMs = Math.min(
          backoffMs ? backoffMs * 2 : 10_000,
          MAX_BACKOFF_MS,
        );
      } else if (warn) {
        log("warn", "rsync", "partial; backoff a bit", { code });
        backoffMs = Math.min(backoffMs ? backoffMs + 5_000 : 5_000, 60_000);
      } else {
        log("error", "rsync", "unexpected error; backoff", { code });
        backoffMs = Math.min(
          backoffMs ? backoffMs * 2 : 10_000,
          MAX_BACKOFF_MS,
        );
      }
    } else backoffMs = 0;

    running = false;

    sessionWriter?.cycleDone({
      lastCycleMs,
      scanAlphaMs: a.ms ?? 0,
      scanBetaMs: b.ms ?? 0,
      mergeMs: m.ms ?? 0,
      backoffMs,
    });
  }

  // --- Flush helpers ---
  async function microFlushOnce(): Promise<boolean> {
    if (hotAlpha.size === 0 && hotBeta.size === 0) return false;
    const a = Array.from(hotAlpha);
    const b = Array.from(hotBeta);
    hotAlpha.clear();
    hotBeta.clear();
    try {
      await microSync(a, b);
    } catch (err) {
      log("warn", "flush", "microSync during flush failed", { err: `${err}` });
    }
    return true;
  }

  async function drainMicro(timeoutMs = 1500): Promise<void> {
    const t0 = Date.now();
    scheduleHotFlush();
    while (Date.now() - t0 < timeoutMs) {
      const did = await microFlushOnce();
      if (!did) {
        await new Promise((r) => setTimeout(r, 50));
        if (hotAlpha.size === 0 && hotBeta.size === 0) break;
      }
    }
  }

  async function performFlush(): Promise<void> {
    const startedAt = Date.now();
    try {
      if (sessionDb && sessionId) {
        ensureSessionDb(sessionDb)
          .prepare(
            `
        UPDATE session_state
           SET flushing=1,
               last_flush_started_at=?,
               last_flush_ok=0,
               last_flush_error=NULL
         WHERE session_id=?
      `,
          )
          .run(startedAt, sessionId);
      }
    } catch {}

    try {
      await drainMicro(1500);
      await oneCycle();
      await drainMicro(500);
      await oneCycle();

      if (sessionDb && sessionId) {
        ensureSessionDb(sessionDb)
          .prepare(
            `
        UPDATE session_state
           SET flushing=0,
               last_flush_ok=1
         WHERE session_id=?
      `,
          )
          .run(sessionId);
      }
    } catch (e: any) {
      if (sessionDb && sessionId) {
        ensureSessionDb(sessionDb)
          .prepare(
            `
        UPDATE session_state
           SET flushing=0,
               last_flush_ok=0,
               last_flush_error=?
         WHERE session_id=?
      `,
          )
          .run(String(e?.stack || e), sessionId);
      }
      throw e;
    }
  }

  async function processSessionCommands(): Promise<boolean> {
    if (!sessionDb || !sessionId) return false;
    let executed = false;
    const db = ensureSessionDb(sessionDb);
    try {
      const rows = db
        .prepare(
          `
      SELECT id, cmd, payload
        FROM session_commands
       WHERE session_id=? AND acked=0
       ORDER BY ts ASC
    `,
        )
        .all(sessionId) as { id: number; cmd: string; payload?: string }[];

      for (const row of rows) {
        if (row.cmd === "flush") {
          executed = true;
          log("info", "scheduler", "flush command received");
          try {
            await performFlush();
            log("info", "scheduler", "flush ok");
          } catch (e: any) {
            log("error", "scheduler", "flush failed", {
              err: String(e?.message || e),
            });
          }
        }
        db.prepare(
          `
        UPDATE session_commands
           SET acked=1, acked_at=?
         WHERE id=?
      `,
        ).run(Date.now(), row.id);
        db.exec(`
  DELETE FROM session_commands
   WHERE acked = 1
     AND ts < strftime('%s','now')*1000 - 7*24*60*60*1000
`);
      }
    } catch (e: any) {
      log("warn", "scheduler", "failed processing session commands", {
        err: String(e?.message || e),
      });
    }
    return executed;
  }

  const CMD_POLL_MS = envNum("SESSION_CMD_POLL_MS", 500);
  async function idleWaitWithCommandPolling(totalMs: number) {
    let remaining = totalMs;
    while (remaining > 0) {
      const executed = await processSessionCommands();
      if (executed) return;
      const slice = Math.min(CMD_POLL_MS, remaining);
      await new Promise((r) => setTimeout(r, slice));
      remaining -= slice;
    }
  }

  let remoteStreams: Array<{ kill: () => void }> = [];

  let alphaStream: ReturnType<typeof startRemoteDeltaStream> | null = null;
  let betaStream: ReturnType<typeof startRemoteDeltaStream> | null = null;

  function ensureRemoteStreams() {
    if (alphaIsRemote && !alphaStream) {
      alphaStream = startRemoteDeltaStream("alpha");
    }
    if (betaIsRemote && !betaStream) {
      betaStream = startRemoteDeltaStream("beta");
    }
  }

  async function loop() {
    ensureRemoteStreams();

    // capture add() so seedHotFromDb can push hot dirs to the ONE watcher
    addRemoteAlphaHotDirs = alphaStream?.add ?? null;
    addRemoteBetaHotDirs = betaStream?.add ?? null;

    remoteStreams = [alphaStream, betaStream].filter(Boolean) as any[];

    while (true) {
      if (!running) {
        pending = false;
        await oneCycle();
        ensureRemoteStreams(); // recreate if they died during the cycle
        const baseNext = clamp(
          lastCycleMs * 2,
          MIN_INTERVAL_MS,
          MAX_INTERVAL_MS,
        );
        nextDelayMs =
          baseNext + (backoffMs || Math.floor(Math.random() * JITTER_MS));
      }

      if (pending) {
        nextDelayMs = clamp(1500, MIN_INTERVAL_MS, MAX_INTERVAL_MS);
        pending = false;
        continue;
      }

      log(
        "info",
        "scheduler",
        "watching",
        `next full scan in ${nextDelayMs} ms`,
      );
      await idleWaitWithCommandPolling(nextDelayMs);
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
    alphaHost,
    betaHost,
    alphaRemoteDb,
    betaRemoteDb,
    remoteScanCmd,
    remoteWatchCmd,
    MAX_HOT_WATCHERS,
    HOT_TTL_MS,
    SHALLOW_DEPTH,
    HOT_DEPTH,
  });

  const cleanup = async () => {
    try {
      await hotAlphaMgr?.closeAll();
    } catch {}
    try {
      await hotBetaMgr?.closeAll();
    } catch {}
    for (const s of remoteStreams) {
      try {
        s.kill();
      } catch {}
    }
    try {
      db.close();
    } catch {}

    if (hbTimer) {
      clearInterval(hbTimer);
      hbTimer = null;
    }
    try {
      sessionWriter?.stop();
    } catch {}
  };
  const onSig = async () => {
    await cleanup();
    process.exit(0);
  };
  process.once("SIGINT", onSig);
  process.once("SIGTERM", onSig);

  await loop();
}

cliEntrypoint<SchedulerOptions>(
  import.meta.url,
  buildProgram,
  async (opts) => await runScheduler(cliOptsToSchedulerOptions(opts)),
  { label: "scheduler" },
);
