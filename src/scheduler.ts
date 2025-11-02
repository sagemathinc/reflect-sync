#!/usr/bin/env node
// - Commander-based CLI
// - Exported runScheduler(opts) API for programmatic use
// - Optional SSH micro-sync: remote watch -> tee to ingest + realtime push
//
// Notes:
// * Local watchers only; remote changes arrive via remote watch stream.
// * Full-cycle still uses "reflect merge" (see merge.ts).

import { spawn, ChildProcess } from "node:child_process";
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
import { makeMicroSync } from "./micro-sync.js";
import { PassThrough } from "node:stream";
import { getBaseDb, getDb } from "./db.js";
import { expandHome, isRoot, remoteWhich } from "./remote.js";
import { CLI_NAME, MAX_WATCHERS } from "./constants.js";
import { listSupportedHashes, defaultHashAlg } from "./hash.js";
import { resolveCompression } from "./rsync-compression.js";
import { argsJoin } from "./remote.js";
import { ConsoleLogger, type Logger, parseLogLevel } from "./logger.js";
import {
  createSessionLogger,
  type SessionLoggerHandle,
} from "./session-logs.js";
import { runMerge } from "./merge.js";
import { runIngestDelta } from "./ingest-delta.js";
import { runScan } from "./scan.js";
import {
  autoIgnoreForRoot,
  normalizeIgnorePatterns,
  collectIgnoreOption,
} from "./ignore.js";
import { getReflectSyncHome } from "./session-db.js";

export type SchedulerOptions = {
  alphaRoot: string;
  betaRoot: string;
  alphaDb: string;
  betaDb: string;
  baseDb: string;
  prefer: "alpha" | "beta";
  dryRun: boolean;

  hash: string;
  alphaHost?: string;
  alphaPort?: number;
  betaHost?: string;
  betaPort?: number;
  alphaRemoteDb: string;
  betaRemoteDb: string;
  remoteCommand?: string;

  disableHotWatch: boolean;

  compress?: string;
  ignoreRules: string[];

  sessionDb?: string;
  sessionId?: number;
  logger?: Logger;
};

// ---------- CLI ----------
function buildProgram(): Command {
  const program = new Command()
    .name(`${CLI_NAME}-scheduler`)
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
    .addOption(
      new Option("--hash <algorithm>", "content hash algorithm")
        .choices(listSupportedHashes())
        .default(defaultHashAlg()),
    )
    .option("--dry-run", "simulate without changing files", false)
    // optional SSH endpoints (only one side may be remote)
    .option("--alpha-host <ssh>", "SSH host for alpha (e.g. user@host)")
    .option("--alpha-port <n>", "SSH port for alpha", (v) => Number.parseInt(v, 10))
    .option("--beta-host <ssh>", "SSH host for beta (e.g. user@host)")
    .option("--beta-port <n>", "SSH port for beta", (v) => Number.parseInt(v, 10))
    .option(
      "--alpha-remote-db <file>",
      "remote path to alpha sqlite db (on the SSH host)",
      `~/.local/share/${CLI_NAME}/alpha.db`,
    )
    .option(
      "--beta-remote-db <file>",
      "remote path to beta sqlite db (on the SSH host)",
      `~/.local/share/${CLI_NAME}/beta.db`,
    )
    .option(
      "--remote-command <cmd>",
      "absolute path to remote reflect-sync command",
    )
    .option(
      "--disable-hot-watch",
      "only sync during the full sync cycle",
      false,
    )
    .option(
      "-i, --ignore <pattern>",
      "gitignore-style ignore rule (repeat or comma-separated)",
      collectIgnoreOption,
      [] as string[],
    )
    .option("--compress", "[auto|zstd|lz4|zlib|zlibx|none][:level]", "auto")
    .option(
      "--session-id <id>",
      "optional session id to enable heartbeats, report state, etc",
    )
    .option("--session-db <path>", "path to session database");

  return program;
}

export function cliOptsToSchedulerOptions(opts): SchedulerOptions {
  const out: SchedulerOptions = {
    alphaRoot: String(opts.alphaRoot),
    betaRoot: String(opts.betaRoot),
    alphaDb: String(opts.alphaDb),
    betaDb: String(opts.betaDb),
    baseDb: String(opts.baseDb),
    prefer: String(opts.prefer).toLowerCase() as "alpha" | "beta",
    dryRun: !!opts.dryRun,
    hash: String(opts.hash),
    alphaHost: opts.alphaHost?.trim() || undefined,
    alphaPort:
      opts.alphaPort != null && !Number.isNaN(Number(opts.alphaPort))
        ? Number(opts.alphaPort)
        : undefined,
    betaHost: opts.betaHost?.trim() || undefined,
    betaPort:
      opts.betaPort != null && !Number.isNaN(Number(opts.betaPort))
        ? Number(opts.betaPort)
        : undefined,
    alphaRemoteDb: String(opts.alphaRemoteDb),
    betaRemoteDb: String(opts.betaRemoteDb),
    remoteCommand: opts.remoteCommand,
    disableHotWatch: !!opts.disableHotWatch,
    compress: opts.compress,
    ignoreRules: normalizeIgnorePatterns(opts.ignore ?? []),
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

const HOT_TTL_MS = envNum("HOT_TTL_MS", 30 * 60_000);
const SHALLOW_DEPTH = envNum("SHALLOW_DEPTH", 1);
const HOT_DEPTH = envNum("HOT_DEPTH", 2);

const MICRO_DEBOUNCE_MS = envNum("MICRO_DEBOUNCE_MS", 200);

const MIN_INTERVAL_MS = envNum("SCHED_MIN_MS", 7_500);
const MAX_INTERVAL_MS = envNum("SCHED_MAX_MS", 60_000);
const MAX_BACKOFF_MS = envNum("SCHED_MAX_BACKOFF_MS", 600_000);
const JITTER_MS = envNum("SCHED_JITTER_MS", 500);

const DEFAULT_CONSOLE_LEVEL = parseLogLevel(
  process.env.REFLECT_LOG_LEVEL,
  "info",
);

const DEFAULT_SESSION_ECHO_LEVEL = parseLogLevel(
  process.env.REFLECT_SESSION_ECHO_LEVEL ?? process.env.REFLECT_LOG_LEVEL,
  "info",
);

// ---------- core (exported) ----------
export async function runScheduler({
  alphaRoot,
  betaRoot,
  alphaDb,
  betaDb,
  baseDb,
  prefer,
  dryRun,
  hash,
  alphaHost,
  alphaPort,
  betaHost,
  betaPort,
  alphaRemoteDb,
  betaRemoteDb,
  remoteCommand,
  disableHotWatch,
  compress,
  ignoreRules: schedulerIgnoreRules,
  sessionDb,
  sessionId,
  logger: providedLogger,
}: SchedulerOptions): Promise<void> {
  if (!alphaRoot || !betaRoot)
    throw new Error("Need --alpha-root and --beta-root");
  if (alphaHost && betaHost)
    throw new Error(
      "Both sides remote is not supported yet (rsync two-remote).",
    );

  let sessionLogHandle: SessionLoggerHandle | null = null;
  let logger: Logger;
  if (providedLogger) {
    logger = providedLogger.child("scheduler");
  } else if (sessionDb && Number.isFinite(sessionId)) {
    sessionLogHandle = createSessionLogger(sessionDb, sessionId!, {
      scope: "scheduler",
      echoLevel: DEFAULT_SESSION_ECHO_LEVEL,
    });
    logger = sessionLogHandle.logger;
  } else {
    logger = new ConsoleLogger(DEFAULT_CONSOLE_LEVEL).child("scheduler");
  }

  const scopeCache = new Map<string, Logger>();
  const scoped = (scope: string) => {
    const existing = scopeCache.get(scope);
    if (existing) return existing;
    const next = logger.child(scope);
    scopeCache.set(scope, next);
    return next;
  };

  const syncHome = getReflectSyncHome();
  const baseIgnore = normalizeIgnorePatterns(schedulerIgnoreRules ?? []);
  const ignoreRules = normalizeIgnorePatterns([
    ...baseIgnore,
    ...autoIgnoreForRoot(alphaRoot, syncHome),
    ...autoIgnoreForRoot(betaRoot, syncHome),
  ]);

  // ---------- scheduler state ----------
  let running = false,
    pending = false,
    lastCycleMs = 0,
    nextDelayMs = 10_000,
    backoffMs = 0;

  const alphaIsRemote = !!alphaHost;
  const betaIsRemote = !!betaHost;

  const remotePort = alphaHost ? alphaPort : betaPort;
  compress = await resolveCompression(alphaHost || betaHost, compress, remotePort);

  // Resolve ~ for any remote paths once up-front
  const remoteLogConfig = { logger };

  alphaRoot = await expandHome(alphaRoot, alphaHost, remoteLogConfig, alphaPort);
  betaRoot = await expandHome(betaRoot, betaHost, remoteLogConfig, betaPort);
  alphaRemoteDb = await expandHome(
    alphaRemoteDb || `~/.local/share/${CLI_NAME}/alpha.db`,
    alphaHost,
    remoteLogConfig,
    alphaPort,
  );
  betaRemoteDb = await expandHome(
    betaRemoteDb || `~/.local/share/${CLI_NAME}/beta.db`,
    betaHost,
    remoteLogConfig,
    betaPort,
  );
  const numericIds =
    (await isRoot(alphaHost, remoteLogConfig, alphaPort)) &&
    (await isRoot(betaHost, remoteLogConfig, betaPort));

  // heartbeat interval (ms)
  const HEARTBEAT_MS = Number(process.env.REFLECT_HEARTBEAT_MS ?? 2000);
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
    details?: Record<string, unknown> | undefined,
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
    const scopeLogger = scoped(source);
    scopeLogger[level](msg, details ?? undefined);
  }

  const clamp = (x: number, lo: number, hi: number) =>
    Math.max(lo, Math.min(hi, x));

  function rel(root: string, full: string): string {
    let r = path.relative(root, full);
    if (r === "" || r === ".") return "";
    if (path.sep !== "/") r = r.split(path.sep).join("/");
    return r;
  }

  // Pipe: ssh scan --emit-delta  →  CLI_NAME ingest --db <local.db>
  // ssh to a remote, run a scan, and writing the resulting
  // data into our local database.
  const lastRemoteScan = { start: 0, ok: false, whenOk: 0 };
  async function sshScanIntoMirror(params: {
    host: string;
    port?: number;
    root: string; // remote root
    remoteDb: string; // remote DB path (on remote host)
    localDb: string; // local mirror DB for ingest
    numericIds?: boolean;
    ignoreRules: string[];
  }): Promise<{
    code: number | null;
    ms: number;
    ok: boolean;
    lastZero: boolean;
  }> {
    const t0 = Date.now();
    // if remote command not known, determine it from the PATH
    remoteCommand ??= await remoteWhich(params.host, CLI_NAME, {
      logger,
      port: params.port,
    });
    const sshArgs = ["-C"];
    if (params.port != null) {
      sshArgs.push("-p", String(params.port));
    }
    sshArgs.push(
      params.host,
      `${remoteCommand} scan`,
      "--root",
      params.root,
      "--emit-delta",
      "--db",
      params.remoteDb,
      "--hash",
      hash,
      "--vacuum",
    );
    for (const pattern of params.ignoreRules) {
      sshArgs.push("--ignore", pattern);
    }
    if (numericIds) {
      sshArgs.push("--numeric-ids");
    }
    if (lastRemoteScan.ok && lastRemoteScan.start) {
      sshArgs.push("--prune-ms");
      sshArgs.push(`${Date.now() - lastRemoteScan.start}`);
    }
    if (!lastRemoteScan.ok && lastRemoteScan.whenOk) {
      // last time wasn't ok, so emit *everything* since last time it worked,
      // so we do not miss data.
      sshArgs.push("--emit-since-ts");
      sshArgs.push(`${Date.now() - lastRemoteScan.whenOk}`);
    }
    lastRemoteScan.start = Date.now();
    lastRemoteScan.ok = false;

    const remoteLog = scoped("remote");
    remoteLog.debug("ssh scan", {
      host: params.host,
      args: argsJoin(sshArgs),
    });

    const sshP = spawn("ssh", sshArgs, {
      stdio: ["ignore", "pipe", "pipe"],
    });

    sshP.stderr?.on("data", (chunk) => {
      remoteLog.debug("ssh stderr", {
        host: params.host,
        data: chunk.toString().trim(),
      });
    });
    const stdout = sshP.stdout;
    if (!stdout) {
      sshP.kill("SIGTERM");
      lastRemoteScan.ok = false;
      return {
        code: 1,
        ms: Date.now() - t0,
        ok: false,
        lastZero: false,
      };
    }

    const abortController = new AbortController();
    const ingestPromise = runIngestDelta({
      db: params.localDb,
      logger: remoteLog.child("ingest"),
      input: stdout,
      abortSignal: abortController.signal,
    });

    const wait = (p: ChildProcess) =>
      new Promise<number | null>((resolve) => p.on("exit", (c) => resolve(c)));

    let sshCode: number | null = null;
    let ingestError: unknown = null;
    try {
      await Promise.all([
        wait(sshP).then((code) => {
          sshCode = code;
          return code;
        }),
        ingestPromise,
      ]);
    } catch (err) {
      ingestError = err;
      abortController.abort();
      sshCode = await wait(sshP);
    }

    const ok = sshCode === 0 && !ingestError;
    lastRemoteScan.ok = ok;
    if (lastRemoteScan.ok) {
      lastRemoteScan.whenOk = lastRemoteScan.start;
    }

    if (ingestError) {
      remoteLog.error("ingest error", {
        error: ingestError instanceof Error ? ingestError.message : String(ingestError),
      });
    }

    return {
      code: ok ? 0 : sshCode ?? 1,
      ms: Date.now() - t0,
      ok,
      lastZero: ok,
    };
  }

  // Remote delta stream (watch)
  // tee stdout to: (a) local ingest, and (b) our line-reader for microSync cues.
  type StreamControl = { add: (dirs: string[]) => void; kill: () => void };

  async function startRemoteDeltaStream(
    side: "alpha" | "beta",
  ): Promise<null | StreamControl> {
    if (disableHotWatch) return null;
    const host = side === "alpha" ? alphaHost : betaHost;
    if (!host) return null;
    const root = side === "alpha" ? alphaRoot : betaRoot;
    const localDb = side === "alpha" ? alphaDb : betaDb;
    const port = side === "alpha" ? alphaPort : betaPort;
    remoteCommand ??= await remoteWhich(host, CLI_NAME, {
      logger,
      port,
    });
    const sshArgs = ["-C", "-T", "-o", "BatchMode=yes"];
    if (port != null) {
      sshArgs.push("-p", String(port));
    }
    sshArgs.push(host, `${remoteCommand} watch`, "--root", root);
    for (const pattern of ignoreRules) {
      sshArgs.push("--ignore", pattern);
    }
    const remoteLog = scoped(`remote.${side}`);
    remoteLog.debug("ssh watch", { args: argsJoin(sshArgs) });

    // stdin=pipe so we can send EOF to make remote `watch` exit
    const sshP = spawn("ssh", sshArgs, {
      stdio: ["pipe", "pipe", "pipe"],
    });

    sshP.stderr?.on("data", (chunk) => {
      remoteLog.debug("ssh stderr", { data: chunk.toString().trim() });
    });

    const stdout = sshP.stdout;
    if (!stdout) {
      remoteLog.error("ssh watch missing stdout");
      sshP.kill("SIGTERM");
      return null;
    }

    const tee = new PassThrough();
    const ingestStream = new PassThrough();
    const watchStream = new PassThrough();

    // ensure errors on the source propagate
    stdout.on("error", (err) => {
      remoteLog.error("ssh stdout error", {
        error: err instanceof Error ? err.message : String(err),
      });
      tee.destroy(err as Error);
    });

    tee.on("error", (err) => {
      ingestStream.destroy(err as Error);
      watchStream.destroy(err as Error);
    });

    ingestStream.on("error", (err) => {
      remoteLog.error("ingest stream error", {
        error: err instanceof Error ? err.message : String(err),
      });
      watchStream.destroy(err as Error);
    });

    watchStream.on("error", (err) => {
      remoteLog.error("watch stream error", {
        error: err instanceof Error ? err.message : String(err),
      });
      ingestStream.destroy(err as Error);
    });

    stdout.pipe(tee);
    tee.pipe(ingestStream);
    tee.pipe(watchStream);

    const ingestAbort = new AbortController();
    runIngestDelta({
      db: localDb,
      logger: remoteLog.child("ingest"),
      input: ingestStream,
      abortSignal: ingestAbort.signal,
    }).catch((err) => {
      remoteLog.error("ingest watch error", {
        error: err instanceof Error ? err.message : String(err),
      });
    });

    const rl = readline.createInterface({ input: watchStream });
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
      ingestAbort.abort();
      try {
        rl.close();
      } catch {}
      try {
        sshP.stdin?.end();
      } catch {} // <— send EOF to remote watch
      // Give it a moment to exit cleanly on EOF
      setTimeout(() => {
        try {
          sshP.kill("SIGTERM");
        } catch {}
      }, 300);
      try {
        ingestStream.destroy();
      } catch {}
      try {
        watchStream.destroy();
      } catch {}
      try {
        tee.destroy();
      } catch {}
      try {
        stdout.destroy();
      } catch {}
    };

    sshP.on("exit", (code) => {
      kill();
      remoteLog.info("ssh watch exited", { code });
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
    alphaPort,
    betaHost,
    betaPort,
    prefer,
    dryRun,
    compress,
    log,
    logger,
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
          maxWatchers: MAX_WATCHERS,
          ttlMs: HOT_TTL_MS,
          hotDepth: HOT_DEPTH,
          awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
          logger: scoped("hot.alpha"),
          ignoreRules,
        });

  const hotBetaMgr =
    betaIsRemote || disableHotWatch
      ? null
      : new HotWatchManager(betaRoot, onBetaHot, {
          maxWatchers: MAX_WATCHERS,
          ttlMs: HOT_TTL_MS,
          hotDepth: HOT_DEPTH,
          awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
          logger: scoped("hot.beta"),
          ignoreRules,
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
        if (r) {
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
      log("info", "watch", `seeded ${covered.length} hot dirs from ${dbPath}`);
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
      const scanLogger = scoped("scan.alpha");
      log(
        "info",
        "scan",
        `alpha: ${alphaRoot}${alphaHost ? ` @ ${alphaHost}` : ""}`,
      );
      a = alphaIsRemote
        ? await sshScanIntoMirror({
            host: alphaHost!,
            port: alphaPort,
            root: alphaRoot,
            localDb: alphaDb,
            remoteDb: alphaRemoteDb!,
            numericIds,
            ignoreRules,
          })
        : await (async () => {
            try {
              await runScan({
                root: alphaRoot,
                db: alphaDb,
                emitDelta: false,
                hash,
                vacuum: false,
                numericIds,
                logger: scanLogger,
                ignoreRules,
              });
              return {
                code: 0,
                ok: true,
                lastZero: true,
                ms: Date.now() - tAlphaStart,
              };
            } catch (err) {
              const duration = Date.now() - tAlphaStart;
              scanLogger.error("scan failed", {
                error: err instanceof Error ? err.message : String(err),
              });
              return {
                code: 1,
                ok: false,
                lastZero: false,
                ms: duration,
                error: err,
              };
            }
          })();
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
      const scanLogger = scoped("scan.beta");
      log(
        "info",
        "scan",
        `beta: ${betaRoot}${betaHost ? ` @ ${betaHost}` : ""}`,
      );
      b = betaIsRemote
        ? await sshScanIntoMirror({
            host: betaHost!,
            port: betaPort,
            root: betaRoot,
            localDb: betaDb,
            remoteDb: betaRemoteDb!,
            numericIds,
            ignoreRules,
          })
        : await (async () => {
            try {
              await runScan({
                root: betaRoot,
                db: betaDb,
                emitDelta: false,
                hash,
                vacuum: false,
                numericIds,
                logger: scanLogger,
                ignoreRules,
              });
              return {
                code: 0,
                ok: true,
                lastZero: true,
                ms: Date.now() - tBetaStart,
              };
            } catch (err) {
              const duration = Date.now() - tBetaStart;
              scanLogger.error("scan failed", {
                error: err instanceof Error ? err.message : String(err),
              });
              return {
                code: 1,
                ok: false,
                lastZero: false,
                ms: duration,
                error: err,
              };
            }
          })();
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
    const mergeLogger = scoped("merge");
    const mergeStart = Date.now();
    let mergeOk = false;
    let mergeError: unknown = null;
    try {
      await runMerge({
        alphaRoot,
        betaRoot,
        alphaDb,
        betaDb,
        baseDb,
        alphaHost,
        alphaPort,
        betaHost,
        betaPort,
        prefer,
        dryRun,
        compress,
        sessionDb,
        sessionId,
        logger: mergeLogger,
        ignoreRules,
        // if the session logger (to database) is enabled, then
        // ensure merge logs everything to our logger.
        verbose: !!sessionLogHandle,
      });
      mergeOk = true;
    } catch (err) {
      mergeError = err;
      mergeLogger.error("merge failed", {
        error: err instanceof Error ? err.message : String(err),
      });
    }
    const mergeMs = Date.now() - mergeStart;

    const ms = Date.now() - t0;
    lastCycleMs = ms;
    log("info", "scheduler", `cycle complete in ${ms} ms`, {
      scanAlphaMs: a.ms,
      scanBetaMs: b.ms,
      mergeMs,
      mergeOk,
    });

    // Backoff on merge errors
    if (!mergeOk) {
      const message =
        mergeError instanceof Error
          ? mergeError.message
          : mergeError
            ? String(mergeError)
            : "merge failed";
      sessionWriter?.error(`merge failed: ${message}`);
      const lower = message.toLowerCase();
      const enospc = lower.includes("enospc") || lower.includes("no space");
      const warn =
        lower.includes("partial") ||
        lower.includes("some files could not be transferred");
      if (enospc) {
        log("error", "merge", "ENOSPC; backoff", { message });
        backoffMs = Math.min(
          backoffMs ? backoffMs * 2 : 10_000,
          MAX_BACKOFF_MS,
        );
      } else if (warn) {
        log("warn", "merge", "partial transfer; backoff", { message });
        backoffMs = Math.min(backoffMs ? backoffMs + 5_000 : 5_000, 60_000);
      } else {
        log("error", "merge", "unexpected error; backoff", { message });
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
      mergeMs,
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
        } else if (row.cmd === "sync") {
          executed = true;
          let attempt: number | undefined;
          if (row.payload) {
            try {
              const parsed = JSON.parse(row.payload) as {
                attempt?: number;
              };
              if (typeof parsed.attempt === "number") {
                attempt = parsed.attempt;
              }
            } catch {
              // ignore malformed payloads
            }
          }
          const msg =
            attempt && Number.isFinite(attempt)
              ? `sync command received (attempt ${attempt})`
              : "sync command received";
          log("info", "scheduler", msg);
          try {
            await oneCycle();
            log("info", "scheduler", "sync cycle complete");
          } catch (e: any) {
            log("error", "scheduler", "sync cycle error", {
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
  let alphaStream: StreamControl | null = null;
  let betaStream: StreamControl | null = null;

  async function ensureRemoteStreams() {
    if (alphaIsRemote && !alphaStream) {
      alphaStream = await startRemoteDeltaStream("alpha");
    }
    if (betaIsRemote && !betaStream) {
      betaStream = await startRemoteDeltaStream("beta");
    }
  }

  async function loop() {
    await ensureRemoteStreams();

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

      log("info", "scheduler", `watching: next full scan in ${nextDelayMs} ms`);
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
    alphaHost,
    betaHost,
    alphaRemoteDb,
    betaRemoteDb,
    MAX_WATCHERS,
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
    if (sessionLogHandle) {
      try {
        sessionLogHandle.close();
      } catch {}
      sessionLogHandle = null;
    }

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
  await cleanup();
}

cliEntrypoint<SchedulerOptions>(
  import.meta.url,
  buildProgram,
  async (opts) => await runScheduler(cliOptsToSchedulerOptions(opts)),
  { label: "scheduler" },
);
