#!/usr/bin/env node
// - Commander-based CLI
// - Exported runScheduler(opts) API for programmatic use
// - Optional SSH watcher: remote watch -> demux remote stdout into ingest +
//   realtime events
//
// Notes:
// * Local watchers only; remote changes arrive via remote watch stream.
// * Full-cycle still uses "reflect merge" (see merge.ts).

import { spawn, ChildProcess } from "node:child_process";
import chokidar, { FSWatcher } from "chokidar";
import path from "node:path";
import { stat } from "node:fs/promises";
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
import { recordHotEvent } from "./hot-events.js";
import {
  ensureSessionDb,
  SessionWriter,
  setDesiredState,
  setActualState,
  getReflectSyncHome,
} from "./session-db.js";
import { PassThrough } from "node:stream";
import { getBaseDb, getDb } from "./db.js";
import {
  expandHome,
  isRoot,
  remoteDirExists,
  remoteWhich,
  RemoteConnectionError,
} from "./remote.js";
import type { SendSignature } from "./recent-send.js";
import { applySignaturesToDb, type SignatureEntry } from "./signature-store.js";
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
import { DiskFullError } from "./rsync.js";
import { DeviceBoundary } from "./device-boundary.js";
import { waitForStableFile, DEFAULT_STABILITY_OPTIONS } from "./stability.js";
import { dedupeRestrictedList } from "./restrict.js";

class FatalSchedulerError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FatalSchedulerError";
  }
}

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
  disableHotSync?: boolean;
  disableFullCycle?: boolean;
  enableReflink?: boolean;

  compress?: string;
  ignoreRules: string[];

  sessionDb?: string;
  sessionId?: number;
  logger?: Logger;
};

type CycleOptions = {
  restrictedPaths?: string[];
  restrictedDirs?: string[];
  label?: string;
};

// ---------- CLI ----------
export function configureSchedulerCommand(
  command: Command,
  { standalone = false }: { standalone?: boolean } = {},
): Command {
  if (standalone) {
    command.name(`${CLI_NAME}-scheduler`);
  }
  return (
    command
      .description("Orchestration of scanning, watching, syncing")
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
      .option("--alpha-port <n>", "SSH port for alpha", (v) =>
        Number.parseInt(v, 10),
      )
      .option("--beta-host <ssh>", "SSH host for beta (e.g. user@host)")
      .option("--beta-port <n>", "SSH port for beta", (v) =>
        Number.parseInt(v, 10),
      )
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
      .option("--disable-hot-sync", "disable realtime hot-sync cycles")
      .option(
        "--disable-full-cycle",
        "disable automatic periodic full sync cycles",
      )
      .option(
        "--enable-reflink",
        "use reflink copies when both roots are local",
        false,
      )
      .option(
        "-i, --ignore <pattern>",
        "gitignore-style ignore rule (repeat or comma-separated)",
        collectIgnoreOption,
        [] as string[],
      )
      .option(
        "--compress <algo>",
        "[auto|zstd|lz4|zlib|zlibx|none][:level]",
        "auto",
      )
      .option(
        "--session-id <id>",
        "optional session id to enable heartbeats, report state, etc",
      )
      .option("--session-db <path>", "path to session database")
  );
}

function buildProgram(): Command {
  return configureSchedulerCommand(new Command(), { standalone: true });
}

export function cliOptsToSchedulerOptions(opts): SchedulerOptions {
  let disableHotSync =
    opts.disableHotSync !== undefined ? !!opts.disableHotSync : false;
  if (opts.enableHotSync === true) {
    disableHotSync = false;
  }
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
    disableHotWatch: !!opts.disableHotWatch || disableHotSync,
    disableHotSync,
    disableFullCycle: !!opts.disableFullCycle,
    compress: opts.compress,
    ignoreRules: normalizeIgnorePatterns(opts.ignore ?? []),
    sessionId: opts.sessionId != null ? Number(opts.sessionId) : undefined,
    sessionDb: opts.sessionDb,
    enableReflink: opts.enableReflink === true,
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

const HOT_DEBOUNCE_MS = envNum(
  "HOT_DEBOUNCE_MS",
  envNum("MICRO_DEBOUNCE_MS", 200),
);

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
  disableHotWatch: disableHotWatchOption,
  disableHotSync = false,
  disableFullCycle = false,
  enableReflink = false,
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
  if (enableReflink && (alphaHost || betaHost)) {
    throw new Error(
      "Reflink can only be enabled when both alpha and beta are local paths.",
    );
  }

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
  const disableHotWatch = disableHotWatchOption || disableHotSync;
  const baseIgnore = normalizeIgnorePatterns(schedulerIgnoreRules ?? []);
  const autoIgnores: string[] = [];
  if (!alphaHost) autoIgnores.push(...autoIgnoreForRoot(alphaRoot, syncHome));
  if (!betaHost) autoIgnores.push(...autoIgnoreForRoot(betaRoot, syncHome));
  const ignoreRules = normalizeIgnorePatterns([...baseIgnore, ...autoIgnores]);

  // ---------- scheduler state ----------
  let running = false,
    pending = false,
    lastCycleMs = 0,
    nextDelayMs = 10_000,
    backoffMs = 0;

  let fatalTriggered = false;
  let initialCycleDone = false;

  const DISK_FULL_PATTERNS = [
    "enospc",
    "no space",
    "disk is full",
    "disk full",
    "filesystem full",
    "file system full",
    "out of space",
  ];

  function describeError(err: unknown): string {
    if (!err) return "";
    if (err instanceof Error) {
      return err.message || err.toString();
    }
    if (typeof err === "object" && err) {
      const maybeMessage = (err as any).message;
      if (typeof maybeMessage === "string" && maybeMessage) return maybeMessage;
    }
    return String(err);
  }

  function messageHasDiskFull(message?: string | null): boolean {
    if (!message) return false;
    const lower = message.toLowerCase();
    return DISK_FULL_PATTERNS.some((pattern) => lower.includes(pattern));
  }

  function isDiskFullCause(err: unknown): boolean {
    if (!err) return false;
    if (err instanceof DiskFullError) return true;
    if (err instanceof Error) {
      const anyErr = err as any;
      if (typeof anyErr.code === "number" && anyErr.code === 28) return true;
      if (typeof anyErr.errno === "number" && anyErr.errno === 28) return true;
      if (typeof anyErr.code === "string" && messageHasDiskFull(anyErr.code)) {
        return true;
      }
      if (messageHasDiskFull(err.message)) return true;
      if (anyErr.cause && isDiskFullCause(anyErr.cause)) return true;
      return false;
    }
    if (typeof err === "object") {
      const anyErr = err as any;
      if (typeof anyErr?.code === "number" && anyErr.code === 28) return true;
      if (typeof anyErr?.errno === "number" && anyErr.errno === 28) return true;
      if (typeof anyErr?.code === "string" && messageHasDiskFull(anyErr.code)) {
        return true;
      }
      if (
        typeof anyErr?.message === "string" &&
        messageHasDiskFull(anyErr.message)
      ) {
        return true;
      }
    }
    if (typeof err === "string") return messageHasDiskFull(err);
    return false;
  }

  function stopForDiskFull(reason: string, err?: unknown): string {
    const detail = describeError(err);
    const message = detail ? `${reason}: ${detail}` : reason;
    if (!fatalTriggered) {
      fatalTriggered = true;
      log("error", "scheduler", message, {
        code: (err as any)?.code ?? null,
      });
      sessionWriter?.error(message);
      if (sessionDb && sessionId) {
        try {
          setDesiredState(sessionDb, sessionId, "stopped");
        } catch {}
        try {
          setActualState(sessionDb, sessionId, "error");
        } catch {}
      }
    }
    running = false;
    pending = false;
    return message;
  }

  const alphaIsRemote = !!alphaHost;
  const betaIsRemote = !!betaHost;

  const stabilityOptions = { ...DEFAULT_STABILITY_OPTIONS };
  const stabilityEnabled =
    Number.isFinite(stabilityOptions.windowMs) && stabilityOptions.windowMs > 0;

  const remotePort = alphaHost ? alphaPort : betaPort;
  compress = await resolveCompression(
    alphaHost || betaHost,
    compress,
    remotePort,
  );

  // Resolve ~ for any remote paths once up-front
  const remoteLogConfig = { logger };

  alphaRoot = await expandHome(
    alphaRoot,
    alphaHost,
    remoteLogConfig,
    alphaPort,
  );
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

  const alphaDeviceBoundary =
    !alphaIsRemote && !disableHotWatch
      ? await DeviceBoundary.create(alphaRoot)
      : null;
  const betaDeviceBoundary =
    !betaIsRemote && !disableHotWatch
      ? await DeviceBoundary.create(betaRoot)
      : null;
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

  class MissingRootError extends Error {
    constructor(
      readonly side: "alpha" | "beta",
      message: string,
    ) {
      super(message);
      this.name = "MissingRootError";
    }
  }

  class RemoteRootUnavailableError extends Error {
    constructor(
      readonly side: "alpha" | "beta",
      message: string,
      readonly cause?: unknown,
    ) {
      super(message);
      this.name = "RemoteRootUnavailableError";
    }
  }

  let missingRootError: MissingRootError | null = null;

  function formatRootLocation(side: "alpha" | "beta"): string {
    const root = side === "alpha" ? alphaRoot : betaRoot;
    const host = side === "alpha" ? alphaHost : betaHost;
    const port = side === "alpha" ? alphaPort : betaPort;
    if (!host) return root;
    const hostPart = port != null ? `${host}:${port}` : host;
    return `${hostPart}:${root}`;
  }

  async function dirExistsLocal(pathToCheck: string): Promise<boolean> {
    try {
      const st = await stat(pathToCheck);
      return st.isDirectory();
    } catch {
      return false;
    }
  }

  function failMissingRoot(side: "alpha" | "beta"): never {
    if (missingRootError) throw missingRootError;
    const location = formatRootLocation(side);
    const message = `sync root missing: ${side} root '${location}' does not exist`;
    log("error", "scheduler", message);
    sessionWriter?.error(message);
    missingRootError = new MissingRootError(side, message);
    throw missingRootError;
  }

  async function ensureRootsExist({
    localOnly = false,
  }: { localOnly?: boolean } = {}): Promise<void> {
    if (missingRootError) throw missingRootError;

    if (!alphaIsRemote) {
      if (!(await dirExistsLocal(alphaRoot))) failMissingRoot("alpha");
    } else if (!localOnly) {
      try {
        const ok = await remoteDirExists({
          host: alphaHost!,
          path: alphaRoot,
          port: alphaPort,
          logger,
        });
        if (!ok) failMissingRoot("alpha");
      } catch (err) {
        if (err instanceof RemoteConnectionError) {
          const message = `remote alpha root unreachable (${formatRootLocation("alpha")}): ${err.message}`;
          throw new RemoteRootUnavailableError("alpha", message, err);
        }
        throw err;
      }
    }

    if (!betaIsRemote) {
      if (!(await dirExistsLocal(betaRoot))) failMissingRoot("beta");
    } else if (!localOnly) {
      try {
        const ok = await remoteDirExists({
          host: betaHost!,
          path: betaRoot,
          port: betaPort,
          logger,
        });
        if (!ok) failMissingRoot("beta");
      } catch (err) {
        if (err instanceof RemoteConnectionError) {
          const message = `remote beta root unreachable (${formatRootLocation("beta")}): ${err.message}`;
          throw new RemoteRootUnavailableError("beta", message, err);
        }
        throw err;
      }
    }
  }

  await ensureRootsExist();

  function rel(root: string, full: string): string {
    let r = path.relative(root, full);
    if (r === "" || r === ".") return "";
    if (path.sep !== "/") r = r.split(path.sep).join("/");
    return r;
  }

  const crossDeviceLogged = {
    alpha: new Set<string>(),
    beta: new Set<string>(),
  };

  function noteCrossDevice(side: "alpha" | "beta", abs: string): void {
    const root = side === "alpha" ? alphaRoot : betaRoot;
    const relPath = rel(root, abs) || ".";
    const seen = crossDeviceLogged[side];
    if (seen.has(relPath)) return;
    seen.add(relPath);
    log("info", "watch", `${side} watcher ignoring cross-device path`, {
      path: relPath,
    });
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
    restrictedPaths?: string[];
    restrictedDirs?: string[];
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
      sshArgs.push("--emit-since-age");
      sshArgs.push(`${Date.now() - lastRemoteScan.whenOk}`);
    }
    if (params.restrictedPaths?.length) {
      for (const rel of params.restrictedPaths) {
        if (!rel) continue;
        sshArgs.push("--restricted-path", rel);
      }
    }
    if (params.restrictedDirs?.length) {
      for (const rel of params.restrictedDirs) {
        if (!rel) continue;
        sshArgs.push("--restricted-dir", rel);
      }
    }
    lastRemoteScan.start = Date.now();
    lastRemoteScan.ok = false;

    const remoteLog = scoped("remote");
    const summarizeStderr = (chunk: Buffer | string) => {
      const raw = chunk.toString();
      let trimmed = raw.trim();
      const MAX_LEN = 4096;
      if (trimmed.length > MAX_LEN) {
        trimmed = `${trimmed.slice(0, MAX_LEN)}… [${trimmed.length} chars truncated]`;
      }
      return trimmed;
    };
    remoteLog.debug(`ssh scan: "ssh ${argsJoin(sshArgs)}"`);

    const sshP = spawn("ssh", sshArgs, {
      stdio: ["ignore", "pipe", "pipe"],
    });

    sshP.stderr?.on("data", (chunk) => {
      remoteLog.debug("ssh stderr", {
        host: params.host,
        data: summarizeStderr(chunk),
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
        error:
          ingestError instanceof Error
            ? ingestError.message
            : String(ingestError),
      });
      if (isDiskFullCause(ingestError)) {
        const fatalMessage = stopForDiskFull(
          `disk full during remote scan (${params.host})`,
          ingestError,
        );
        throw new FatalSchedulerError(fatalMessage);
      }
    }

    return {
      code: ok ? 0 : (sshCode ?? 1),
      ms: Date.now() - t0,
      ok,
      lastZero: ok,
    };
  }

  // Remote delta stream (watch)
  // Demultiplex stdout lines: NDJSON deltas feed ingest, others drive control events.
  type ReleaseAckEntry = {
    path: string;
    watermark?: number | null;
  };

  type StreamControl = {
    add: (dirs: string[]) => void;
    stat: (
      paths: string[],
      opts: { ignore: boolean; stable?: boolean },
    ) => Promise<SignatureEntry[]>;
    lock?: (paths: string[]) => Promise<void>;
    release?: (entries: ReleaseAckEntry[]) => Promise<void>;
    unlock?: (paths: string[]) => Promise<void>;
    kill: () => void;
  };

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
    if (numericIds) {
      sshArgs.push("--numeric-ids");
    }
    for (const pattern of ignoreRules) {
      sshArgs.push("--ignore", pattern);
    }
    const remoteLog = scoped(`remote.${side}`);
    const summarizeStderr = (chunk: Buffer | string) => {
      const raw = chunk.toString();
      let trimmed = raw.trim();
      const MAX_LEN = 1024;
      if (trimmed.length > MAX_LEN) {
        trimmed = `${trimmed.slice(0, MAX_LEN)}… [${trimmed.length} chars truncated]`;
      }
      return trimmed;
    };
    remoteLog.debug("ssh watch", { args: argsJoin(sshArgs) });

    // stdin=pipe so we can send EOF to make remote `watch` exit
    const sshP = spawn("ssh", sshArgs, {
      stdio: ["pipe", "pipe", "pipe"],
    });

    sshP.stderr?.on("data", (chunk) => {
      remoteLog.debug("ssh stderr", { data: summarizeStderr(chunk) });
    });

    const stdout = sshP.stdout;
    if (!stdout) {
      remoteLog.error("ssh watch missing stdout");
      sshP.kill("SIGTERM");
      return null;
    }

    const ingestStream = new PassThrough();

    ingestStream.on("error", (err) => {
      remoteLog.error("ingest stream error", {
        error: err instanceof Error ? err.message : String(err),
      });
    });

    const ingestAbort = new AbortController();
    runIngestDelta({
      db: localDb,
      logger: remoteLog.child("ingest"),
      input: ingestStream,
      abortSignal: ingestAbort.signal,
    }).catch((err) => {
      if (isDiskFullCause(err)) {
        stopForDiskFull(`disk full during remote ${side} watch ingest`, err);
        return;
      }
      remoteLog.error("ingest watch error", {
        error: err instanceof Error ? err.message : String(err),
      });
    });

    const rl = readline.createInterface({ input: stdout });

    interface PendingStat {
      resolve: (entries: SignatureEntry[]) => void;
      reject: (err: Error) => void;
    }
    interface PendingControl {
      resolve: (payload?: any) => void;
      reject: (err: Error) => void;
    }

    let nextRequestId = 1;
    const pendingStats = new Map<number, PendingStat>();
    const pendingControls = new Map<number, PendingControl>();
    let statsInFlight = 0;
    let bufferedEvents: any[] = [];

    const flushBuffered = () => {
      if (statsInFlight > 0 || bufferedEvents.length === 0) return;
      const events = bufferedEvents;
      bufferedEvents = [];
      for (const evt of events) {
        processEvent(evt);
      }
    };

    const processEvent = (evt: any) => {
      if (!evt?.path) return;
      let r = String(evt.path);
      if (r.startsWith(root)) r = r.slice(root.length);
      if (r.startsWith("/")) r = r.slice(1);
      if (!r) return;
      (side === "alpha" ? hotAlpha : hotBeta).add(r);
      if (side === "alpha") {
        recordHotEvent(alphaDb, "alpha", r, "remote-watch");
      } else {
        recordHotEvent(betaDb, "beta", r, "remote-watch");
      }
      scheduleHotFlush();
    };

    const failPending = (err: Error) => {
      for (const pending of pendingStats.values()) {
        pending.reject(err);
      }
      pendingStats.clear();
      for (const pending of pendingControls.values()) {
        pending.reject(err);
      }
      pendingControls.clear();
      bufferedEvents = [];
      statsInFlight = 0;
    };

    const forwardToIngest = (line: string) => {
      if (ingestStream.destroyed || ingestStream.writableEnded) return;
      ingestStream.write(`${line}\n`);
    };

    const handleControlAck = (evt: any) => {
      const requestId = Number(evt?.requestId ?? 0);
      if (!requestId) return;
      const pending = pendingControls.get(requestId);
      if (!pending) return;
      pendingControls.delete(requestId);
      if (evt?.error) {
        const message =
          typeof evt.error === "string"
            ? evt.error
            : String(evt.error ?? "unknown error");
        pending.reject(new Error(message));
      } else {
        pending.resolve(evt);
      }
    };

    rl.on("line", (line) => {
      if (!line) return;
      const raw = line;
      const trimmed = raw.trim();
      if (!trimmed) return;
      let evt: any;
      try {
        evt = JSON.parse(trimmed);
      } catch {
        forwardToIngest(raw);
        return;
      }
      if (evt?.op === "stat") {
        const requestId = Number(evt.requestId ?? 0);
        const pending = pendingStats.get(requestId);
        if (pending) {
          pendingStats.delete(requestId);
          statsInFlight = Math.max(0, statsInFlight - 1);
          if (evt.error) {
            pending.reject(
              new Error(
                typeof evt.error === "string" ? evt.error : String(evt.error),
              ),
            );
            flushBuffered();
            return;
          }
          const entries: SignatureEntry[] = [];
          if (Array.isArray(evt.entries)) {
            for (const entry of evt.entries) {
              if (!entry?.path || !entry?.signature) continue;
              entries.push({
                path: String(entry.path),
                signature: entry.signature as SendSignature,
                target:
                  entry.target === undefined
                    ? undefined
                    : (entry.target ?? null),
              });
            }
          }
          pending.resolve(entries);
        }
        flushBuffered();
        return;
      }
      if (
        evt?.op === "lockAck" ||
        evt?.op === "releaseAck" ||
        evt?.op === "unlockAck"
      ) {
        handleControlAck(evt);
        return;
      }
      if (evt?.ev && evt?.path) {
        if (statsInFlight > 0) {
          bufferedEvents.push(evt);
          return;
        }
        processEvent(evt);
        return;
      }
      if (typeof evt?.kind === "string") {
        forwardToIngest(raw);
        return;
      }
      // Unknown message: forward to ingest so it can decide (or ignore downstream).
      forwardToIngest(raw);
    });

    const requestStat = (
      paths: string[],
      opts: { ignore: boolean; stable?: boolean },
    ): Promise<SignatureEntry[]> => {
      const unique = Array.from(new Set(paths.filter(Boolean)));
      if (!unique.length) {
        return Promise.resolve([]);
      }
      const requestId = nextRequestId++;
      statsInFlight += 1;
      return new Promise((resolve, reject) => {
        pendingStats.set(requestId, { resolve, reject });
        try {
          sshP.stdin?.write(
            JSON.stringify({
              op: "stat",
              requestId,
              paths: unique,
              ignore: !!opts.ignore,
              stable: opts.stable === false ? false : true,
            }) + "\n",
          );
        } catch (err) {
          pendingStats.delete(requestId);
          statsInFlight = Math.max(0, statsInFlight - 1);
          reject(err as Error);
          flushBuffered();
        }
      });
    };

    const sendControl = (
      op: "lock" | "release" | "unlock",
      payload: Record<string, unknown>,
    ): Promise<any> => {
      const requestId = nextRequestId++;
      return new Promise((resolve, reject) => {
        pendingControls.set(requestId, { resolve, reject });
        try {
          sshP.stdin?.write(
            JSON.stringify({
              op,
              requestId,
              ...payload,
            }) + "\n",
          );
        } catch (err) {
          pendingControls.delete(requestId);
          reject(err as Error);
        }
      });
    };

    const requestLock = (paths: string[]) => {
      if (!paths.length) return Promise.resolve();
      return sendControl("lock", { paths });
    };

    const requestRelease = (entries: ReleaseAckEntry[]) => {
      if (!entries.length) return Promise.resolve();
      return sendControl("release", { entries });
    };

    const requestUnlock = (paths: string[]) => {
      if (!paths.length) return Promise.resolve();
      return sendControl("unlock", { paths });
    };

    let killed = false;
    const kill = () => {
      if (killed) return;
      killed = true;
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
        ingestStream.end();
      } catch {}
      try {
        stdout.destroy();
      } catch {}
      failPending(new Error("remote watch closed"));
    };

    stdout?.once("close", () => {
      try {
        ingestStream.end();
      } catch {}
    });

    stdout?.on("error", (err) => {
      remoteLog.error("watch stdout error", {
        error: err instanceof Error ? err.message : String(err),
      });
      ingestStream.destroy(err as Error);
      failPending(err as Error);
    });

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

    return {
      add,
      stat: requestStat,
      lock: requestLock,
      release: requestRelease,
      unlock: requestUnlock,
      kill,
    };
  }

  function requestSoon(reason: string) {
    if (disableFullCycle) return;
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
  let hotFlushPromise: Promise<void> | null = null;
  let flushQueuedWhileRunning = false;

  let alphaStream: StreamControl | null = null;
  let betaStream: StreamControl | null = null;

  const chunkArray = <T>(arr: T[], size: number): T[][] => {
    if (arr.length === 0) return [];
    const out: T[][] = [];
    for (let i = 0; i < arr.length; i += size) {
      out.push(arr.slice(i, i + size));
    }
    return out;
  };

  const fetchAlphaRemoteSignatures = async (
    paths: string[],
    opts: { ignore: boolean; stable?: boolean },
  ): Promise<SignatureEntry[]> => {
    if (!paths.length) return [];
    if (!alphaStream?.stat) {
      await ensureRemoteStreams();
    }
    if (!alphaStream?.stat) return [];
    const unique = Array.from(new Set(paths.filter(Boolean)));
    const collected: SignatureEntry[] = [];
    for (const batch of chunkArray(unique, 1000)) {
      const entries = await alphaStream.stat(batch, opts);
      if (entries.length) collected.push(...entries);
    }
    if (collected.length) {
      applySignaturesToDb(alphaDb, collected, { numericIds });
    }
    return collected;
  };

  const fetchBetaRemoteSignatures = async (
    paths: string[],
    opts: { ignore: boolean; stable?: boolean },
  ): Promise<SignatureEntry[]> => {
    if (!paths.length) return [];
    if (!betaStream?.stat) {
      await ensureRemoteStreams();
    }
    if (!betaStream?.stat) return [];
    const unique = Array.from(new Set(paths.filter(Boolean)));
    const collected: SignatureEntry[] = [];
    for (const batch of chunkArray(unique, 1000)) {
      const entries = await betaStream.stat(batch, opts);
      if (entries.length) collected.push(...entries);
    }
    if (collected.length) {
      applySignaturesToDb(betaDb, collected, { numericIds });
    }
    return collected;
  };

  const lockRemoteAlphaPaths = async (paths: string[]) => {
    if (!paths.length) return;
    if (!alphaStream?.lock) {
      await ensureRemoteStreams();
    }
    if (!alphaStream?.lock) return;
    const unique = Array.from(new Set(paths.filter(Boolean)));
    for (const batch of chunkArray(unique, 1000)) {
      await alphaStream.lock!(batch);
    }
  };

  const releaseRemoteAlphaPaths = async (entries: ReleaseAckEntry[]) => {
    if (!entries.length) return;
    if (!alphaStream?.release) {
      await ensureRemoteStreams();
    }
    if (!alphaStream?.release) return;
    for (const batch of chunkArray(entries, 500)) {
      await alphaStream.release!(batch);
    }
  };

  const unlockRemoteAlphaPaths = async (paths: string[]) => {
    if (!paths.length) return;
    if (!alphaStream?.unlock) {
      await ensureRemoteStreams();
    }
    if (!alphaStream?.unlock) return;
    const unique = Array.from(new Set(paths.filter(Boolean)));
    for (const batch of chunkArray(unique, 1000)) {
      await alphaStream.unlock!(batch);
    }
  };

  const lockRemoteBetaPaths = async (paths: string[]) => {
    if (!paths.length) return;
    if (!betaStream?.lock) {
      await ensureRemoteStreams();
    }
    if (!betaStream?.lock) return;
    const unique = Array.from(new Set(paths.filter(Boolean)));
    for (const batch of chunkArray(unique, 1000)) {
      await betaStream.lock!(batch);
    }
  };

  const releaseRemoteBetaPaths = async (entries: ReleaseAckEntry[]) => {
    if (!entries.length) return;
    if (!betaStream?.release) {
      await ensureRemoteStreams();
    }
    if (!betaStream?.release) return;
    for (const batch of chunkArray(entries, 500)) {
      await betaStream.release!(batch);
    }
  };

  const unlockRemoteBetaPaths = async (paths: string[]) => {
    if (!paths.length) return;
    if (!betaStream?.unlock) {
      await ensureRemoteStreams();
    }
    if (!betaStream?.unlock) return;
    const unique = Array.from(new Set(paths.filter(Boolean)));
    for (const batch of chunkArray(unique, 1000)) {
      await betaStream.unlock!(batch);
    }
  };

  const alphaLockHandle = alphaIsRemote
    ? {
        lock: lockRemoteAlphaPaths,
        release: releaseRemoteAlphaPaths,
        unlock: unlockRemoteAlphaPaths,
      }
    : undefined;
  const betaLockHandle = betaIsRemote
    ? {
        lock: lockRemoteBetaPaths,
        release: releaseRemoteBetaPaths,
        unlock: unlockRemoteBetaPaths,
      }
    : undefined;

  async function partitionStablePaths(side: "alpha" | "beta", paths: string[]) {
    if (!stabilityEnabled || !paths.length) {
      return { ready: paths, pending: [] as string[] };
    }
    const isRemote = side === "alpha" ? alphaIsRemote : betaIsRemote;
    if (isRemote) {
      return { ready: paths, pending: [] as string[] };
    }
    const root = side === "alpha" ? alphaRoot : betaRoot;
    const ready: string[] = [];
    const pending: string[] = [];
    for (const rel of paths) {
      if (!rel) continue;
      const abs = path.join(root, rel);
      try {
        const res = await waitForStableFile(abs, stabilityOptions);
        if (res.stable) {
          ready.push(rel);
        } else {
          pending.push(rel);
          logger.debug("deferring unstable path", {
            side,
            path: rel,
            reason: res.reason ?? "unstable",
          });
        }
      } catch (err: any) {
        log("warn", "realtime", "stability check failed; proceeding", {
          side,
          path: rel,
          error: String(err?.message || err),
        });
        ready.push(rel);
      }
    }
    return { ready, pending };
  }

  function scheduleHotFlush() {
    if (hotFlushPromise) {
      flushQueuedWhileRunning = true;
      return;
    }
    if (hotTimer) return;
    hotTimer = setTimeout(() => {
      hotTimer = null;
      startHotFlush();
    }, HOT_DEBOUNCE_MS);
  }

  function startHotFlush() {
    if (hotFlushPromise) {
      flushQueuedWhileRunning = true;
      return;
    }
    hotFlushPromise = (async () => {
      await flushHotOnce();
    })();
    hotFlushPromise
      ?.then(() => {
        finalizeHotFlush();
      })
      .catch((err) => {
        log("warn", "realtime", "hot flush failed", {
          err: String(err?.message || err),
        });
        finalizeHotFlush();
      });
  }

  function finalizeHotFlush() {
    hotFlushPromise = null;
    if (fatalTriggered) return;
    requestSoon("restricted sync complete");
    const needsAnother =
      flushQueuedWhileRunning || hotAlpha.size > 0 || hotBeta.size > 0;
    flushQueuedWhileRunning = false;
    if (needsAnother) {
      scheduleHotFlush();
    }
  }

  async function flushHotOnce(): Promise<void> {
    if (fatalTriggered) return;
    if (hotAlpha.size === 0 && hotBeta.size === 0) return;
    const rpathsAlpha = Array.from(hotAlpha);
    const rpathsBeta = Array.from(hotBeta);
    hotAlpha.clear();
    hotBeta.clear();
    try {
      const [alphaPartition, betaPartition] = await Promise.all([
        partitionStablePaths("alpha", rpathsAlpha),
        partitionStablePaths("beta", rpathsBeta),
      ]);
      for (const p of alphaPartition.pending) {
        hotAlpha.add(p);
      }
      for (const p of betaPartition.pending) {
        hotBeta.add(p);
      }
      const readyAlpha = alphaPartition.ready;
      const readyBeta = betaPartition.ready;
      if (!readyAlpha.length && !readyBeta.length) {
        logger.debug("waiting for files to settle", {
          pendingAlpha: alphaPartition.pending.length,
          pendingBeta: betaPartition.pending.length,
        });
        return;
      }
      const readySet = new Set<string>([...readyAlpha, ...readyBeta]);
      const readyPaths = Array.from(readySet);
      await runCycle({
        restrictedPaths: readyPaths,
        label: "hot",
      });
      if (readyPaths.length) {
        log("info", "realtime", "restricted sync complete", {
          paths: readyPaths.length,
          alphaPaths: readyAlpha.length,
          betaPaths: readyBeta.length,
        });
      }
    } catch (e: any) {
      for (const rel of rpathsAlpha) {
        if (rel) hotAlpha.add(rel);
      }
      for (const rel of rpathsBeta) {
        if (rel) hotBeta.add(rel);
      }
      if (isDiskFullCause(e)) {
        stopForDiskFull("disk full during realtime sync", e);
      } else {
        log("warn", "realtime", "restricted cycle failed", {
          err: String(e?.message || e),
        });
      }
    }
  }

  // ---------- root watchers (locals only) ----------
  async function onAlphaHot(abs: string, evt: string) {
    if (
      alphaDeviceBoundary &&
      !(await alphaDeviceBoundary.isOnRootDevice(abs, {
        isDir: evt === "addDir" || evt === "unlinkDir",
      }))
    ) {
      noteCrossDevice("alpha", abs);
      return;
    }
    const r = rel(alphaRoot, abs);
    if (r && (evt === "change" || evt === "add" || evt === "unlink")) {
      hotAlpha.add(r);
      recordHotEvent(alphaDb, "alpha", r, "hotwatch");
      scheduleHotFlush();
    }
  }
  async function onBetaHot(abs: string, evt: string) {
    if (
      betaDeviceBoundary &&
      !(await betaDeviceBoundary.isOnRootDevice(abs, {
        isDir: evt === "addDir" || evt === "unlinkDir",
      }))
    ) {
      noteCrossDevice("beta", abs);
      return;
    }
    const r = rel(betaRoot, abs);
    if (r && (evt === "change" || evt === "add" || evt === "unlink")) {
      hotBeta.add(r);
      recordHotEvent(betaDb, "beta", r, "hotwatch");
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

  function enableWatch({
    watcher,
    root,
    mgr,
    hot,
    boundary,
    side,
  }: {
    watcher: FSWatcher;
    root: string;
    mgr: HotWatchManager;
    hot: Set<string>;
    boundary?: DeviceBoundary | null;
    side: "alpha" | "beta";
  }) {
    handleWatchErrors(watcher);
    ["add", "change", "unlink", "addDir", "unlinkDir"].forEach((evt) => {
      watcher.on(evt as any, async (p: string, stats) => {
        if (
          boundary &&
          !(await boundary.isOnRootDevice(p, {
            isDir: (evt as string)?.endsWith("Dir"),
            stats,
          }))
        ) {
          noteCrossDevice(side, p);
          return;
        }
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
      boundary: alphaDeviceBoundary,
      side: "alpha",
    });
  }
  if (shallowBeta && hotBetaMgr) {
    enableWatch({
      watcher: shallowBeta,
      root: betaRoot,
      mgr: hotBetaMgr,
      hot: hotBeta,
      boundary: betaDeviceBoundary,
      side: "beta",
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
  async function runCycle(options: CycleOptions = {}): Promise<void> {
    try {
      await ensureRootsExist();
    } catch (err) {
      if (err instanceof RemoteRootUnavailableError) {
        log("warn", "scheduler", err.message);
        sessionWriter?.error(err.message);
        backoffMs = Math.min(
          backoffMs ? backoffMs * 2 : 10_000,
          MAX_BACKOFF_MS,
        );
      }
      throw err;
    }

    const restrictedPaths = dedupeRestrictedList(options.restrictedPaths);
    const restrictedDirs = dedupeRestrictedList(options.restrictedDirs);
    const hasRestrictions =
      restrictedPaths.length > 0 || restrictedDirs.length > 0;

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
            restrictedPaths: hasRestrictions ? restrictedPaths : undefined,
            restrictedDirs: hasRestrictions ? restrictedDirs : undefined,
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
                restrictedPaths: hasRestrictions ? restrictedPaths : undefined,
                restrictedDirs: hasRestrictions ? restrictedDirs : undefined,
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
      if (!hasRestrictions) {
        seedHotFromDb(
          alphaDb,
          hotAlphaMgr,
          addRemoteAlphaHotDirs,
          tAlphaStart,
          MAX_WATCHERS,
        );
      }
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
            restrictedPaths: hasRestrictions ? restrictedPaths : undefined,
            restrictedDirs: hasRestrictions ? restrictedDirs : undefined,
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
                restrictedPaths: hasRestrictions ? restrictedPaths : undefined,
                restrictedDirs: hasRestrictions ? restrictedDirs : undefined,
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
      if (!hasRestrictions) {
        seedHotFromDb(
          betaDb,
          hotBetaMgr,
          addRemoteBetaHotDirs,
          tBetaStart,
          MAX_WATCHERS,
        );
      }
    };
    await Promise.all([scanAlpha(), scanBeta()]);

    const scanFailures: Array<{ side: "alpha" | "beta"; error: unknown }> = [];
    if (!a?.ok) scanFailures.push({ side: "alpha", error: a?.error });
    if (!b?.ok) scanFailures.push({ side: "beta", error: b?.error });
    if (scanFailures.length) {
      const diskFail = scanFailures.find((failure) =>
        isDiskFullCause(failure.error),
      );
      if (diskFail) {
        const fatalMessage = stopForDiskFull(
          `disk full during ${diskFail.side} scan`,
          diskFail.error,
        );
        throw new FatalSchedulerError(fatalMessage);
      }
      const message =
        scanFailures
          .map((failure) =>
            failure.error
              ? `${failure.side} scan failed: ${describeError(failure.error)}`
              : `${failure.side} scan failed`,
          )
          .join("; ") || "scan failed";
      log("warn", "scan", message);
      sessionWriter?.error(message);
      backoffMs = Math.min(backoffMs ? backoffMs * 2 : 10_000, MAX_BACKOFF_MS);
      lastCycleMs = MIN_INTERVAL_MS;
      running = false;
      return;
    }

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
        restrictedPaths: hasRestrictions ? restrictedPaths : undefined,
        restrictedDirs: hasRestrictions ? restrictedDirs : undefined,
        enableReflink,
        fetchRemoteAlphaSignatures: alphaIsRemote
          ? fetchAlphaRemoteSignatures
          : undefined,
        fetchRemoteBetaSignatures: betaIsRemote
          ? fetchBetaRemoteSignatures
          : undefined,
        alphaRemoteLock: alphaLockHandle,
        betaRemoteLock: betaLockHandle,
      });
      mergeOk = true;
    } catch (err) {
      mergeError = err;
      mergeLogger.error("merge failed", {
        error: err instanceof Error ? err.message : String(err),
        stack: err.stack,
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
      if (isDiskFullCause(mergeError) || messageHasDiskFull(message)) {
        const fatalMessage = stopForDiskFull(
          "disk full during merge",
          mergeError,
        );
        throw new FatalSchedulerError(fatalMessage);
      }
      const lower = message.toLowerCase();
      const warn =
        lower.includes("partial") ||
        lower.includes("some files could not be transferred");
      if (warn) {
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
  async function processHotQueueOnce(): Promise<boolean> {
    if (hotAlpha.size === 0 && hotBeta.size === 0) return false;
    await flushHotOnce();
    return true;
  }

  async function drainHotQueue(timeoutMs = 1500): Promise<void> {
    const t0 = Date.now();
    scheduleHotFlush();
    while (Date.now() - t0 < timeoutMs) {
      const did = await processHotQueueOnce();
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
      await drainHotQueue(1500);
      await runCycle();
      await drainHotQueue(500);
      await runCycle();

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
            await runCycle();
            log("info", "scheduler", "sync cycle complete");
          } catch (e: any) {
            if (e instanceof FatalSchedulerError) throw e;
            if (isDiskFullCause(e)) {
              const fatalMessage = stopForDiskFull(
                "disk full during manual sync",
                e,
              );
              throw new FatalSchedulerError(fatalMessage);
            }
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
      if (fatalTriggered) return;
      await ensureRootsExist({ localOnly: true });
      const executed = await processSessionCommands();
      if (executed) return;
      const slice = Math.min(CMD_POLL_MS, remaining);
      await new Promise((r) => setTimeout(r, slice));
      remaining -= slice;
    }
  }

  let remoteStreams: Array<{ kill: () => void }> = [];

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
      if (fatalTriggered) {
        throw new FatalSchedulerError("disk full detected");
      }
      if (!running) {
        pending = false;
        const shouldRunCycle = !disableFullCycle || !initialCycleDone;
        if (shouldRunCycle) {
          try {
            await runCycle();
            initialCycleDone = true;
          } catch (err) {
            if (err instanceof RemoteRootUnavailableError) {
              const delay = clamp(
                backoffMs || MIN_INTERVAL_MS,
                MIN_INTERVAL_MS,
                MAX_INTERVAL_MS,
              );
              log(
                "info",
                "scheduler",
                `remote root unavailable; retrying in ${delay} ms`,
              );
              await idleWaitWithCommandPolling(delay);
              continue;
            }
            if (err instanceof FatalSchedulerError) {
              throw err;
            }
            throw err;
          }
          ensureRemoteStreams(); // recreate if they died during the cycle
          const baseNext = clamp(
            lastCycleMs * 2,
            MIN_INTERVAL_MS,
            MAX_INTERVAL_MS,
          );
          nextDelayMs =
            baseNext + (backoffMs || Math.floor(Math.random() * JITTER_MS));
        } else if (disableFullCycle) {
          await ensureRemoteStreams();
        }
      }

      if (pending) {
        if (!disableFullCycle) {
          nextDelayMs = clamp(1500, MIN_INTERVAL_MS, MAX_INTERVAL_MS);
          pending = false;
          continue;
        }
        pending = false;
      }

      if (disableFullCycle) {
        log(
          "info",
          "scheduler",
          "watching: automatic full cycles disabled; awaiting commands",
        );
      } else {
        log(
          "info",
          "scheduler",
          `watching: next full scan in ${nextDelayMs} ms`,
        );
      }
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

  try {
    await loop();
  } catch (err) {
    if (err instanceof FatalSchedulerError) {
      process.exitCode = process.exitCode ?? 1;
    } else {
      throw err;
    }
  } finally {
    await cleanup();
  }
}

cliEntrypoint<SchedulerOptions>(
  import.meta.url,
  buildProgram,
  async (opts) => await runScheduler(cliOptsToSchedulerOptions(opts)),
  { label: "scheduler" },
);
