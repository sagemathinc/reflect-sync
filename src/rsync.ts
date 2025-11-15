// src/rsync.ts
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { mkdtemp, rm, stat as fsStat, mkdir, readFile } from "node:fs/promises";
import path from "node:path";
import { createWriteStream, readFileSync } from "node:fs";
import { finished } from "node:stream/promises";
import {
  isCompressing,
  type RsyncCompressSpec,
  rsyncCompressionArgs,
} from "./rsync-compression.js";
import { argsJoin } from "./remote.js";
import { ConsoleLogger, type LogLevel, type Logger } from "./logger.js";
import { maybeRestartSshControl } from "./ssh-control.js";

// if true, logs every file sent over rsync
const REFLECT_RSYNC_VERY_VERBOSE = false;
const REFLECT_RSYNC_VERY_VERBOSE_MAX_FILES = 1000;

const REFLECT_COPY_CHUNK = Number(process.env.REFLECT_COPY_CHUNK ?? 10_000);
const REFLECT_COPY_CONCURRENCY = Number(
  process.env.REFLECT_COPY_CONCURRENCY ?? 2,
);
const REFLECT_DIR_CHUNK = Number(process.env.REFLECT_DIR_CHUNK ?? 20_000);

const DEFAULT_FIELD_DELIM = "\x1F"; // US
const DEFAULT_RECORD_DELIM = "\x1E"; // RS
const DELIM_FALLBACK_START = 0xe000;
const DELIM_FALLBACK_END = 0xf8ff;

export const RSYNC_TEMP_DIR =
  process.env.REFLECT_RSYNC_TEMP_DIR ?? ".reflect-rsync-tmp";

export async function ensureTempDir(root: string): Promise<string> {
  if (!root) return "";
  const dir = path.join(root, RSYNC_TEMP_DIR);
  try {
    await mkdir(dir, { recursive: true });
  } catch (err: any) {
    if (err?.code !== "EEXIST") {
      throw err;
    }
  }
  return dir;
}

const PROGRESS_ARGS = [
  "--outbuf=L",
  "--no-inc-recursive",
  "--info=progress2",
  "--no-human-readable",
];

const MAX_PROGRESS_UPDATES_PER_SEC = Number(
  process.env.REFLECT_PROGRESS_MAX_HZ ?? 3,
);

export class RsyncError extends Error {
  constructor(
    message: string,
    public readonly code: number | null,
    public readonly context?: Record<string, unknown>,
  ) {
    super(message);
    this.name = "RsyncError";
  }
}

export class DiskFullError extends RsyncError {
  constructor(
    message: string,
    code: number | null,
    context?: Record<string, unknown>,
  ) {
    super(message, code, context);
    this.name = "DiskFullError";
  }
}

type RsyncLogOptions = {
  logger?: Logger;
  verbose?: boolean | string;
  logLevel?: LogLevel;
  sshPort?: number;
};

type RsyncRunOptions = RsyncLogOptions & {
  dryRun?: boolean | string;
  onProgress?: (event: RsyncProgressEvent) => void;
  tempDir?: string;
  direction?: "alpha->beta" | "beta->alpha" | "alpha->alpha" | "beta->beta";
  captureTransfers?: TransferCaptureSpec | false;
};

export type RsyncTransferRecord = {
  path: string;
  itemized: string;
  mode?: string;
  user?: string;
  group?: string;
};

type TransferCaptureSpec = {
  paths?: readonly string[];
};

export type RsyncProgressEvent = {
  transferredBytes: number;
  percent: number;
  totalBytes?: number;
  speed?: string;
  etaMilliseconds?: number;
};

function toBoolVerbose(v?: boolean | string): boolean {
  if (typeof v === "string") {
    const trimmed = v.trim().toLowerCase();
    if (!trimmed) return false;
    if (trimmed === "false" || trimmed === "0" || trimmed === "off") {
      return false;
    }
    return true;
  }
  return !!v;
}

function resolveLogContext(opts: RsyncLogOptions) {
  const level = opts.logLevel ?? "info";
  const logger = opts.logger ?? new ConsoleLogger(level);
  const debug =
    REFLECT_RSYNC_VERY_VERBOSE ||
    toBoolVerbose(opts.verbose) ||
    level === "debug";
  return { logger, debug, level };
}

function isDebugEnabled(opts: RsyncLogOptions): boolean {
  return toBoolVerbose(opts.verbose) || opts.logLevel === "debug";
}

// ----------------------- Helpers -----------------------

async function parallelMapLimit<T>(
  items: T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<void>,
): Promise<void> {
  if (items.length === 0) return;
  const k = Math.max(1, Math.min(concurrency, items.length));
  let i = 0;
  const workers = Array.from({ length: k }, async () => {
    while (true) {
      const idx = i++;
      if (idx >= items.length) break;
      await fn(items[idx], idx);
    }
  });
  await Promise.all(workers);
}

function parentDirsOf(rpaths: string[]): string[] {
  const s = new Set<string>();
  for (const p of rpaths) {
    const d = path.posix.dirname(p);
    if (d && d !== ".") s.add(d);
  }
  // Sort shallowest-first so mkdir walk is cache-friendly
  return Array.from(s).sort((a, b) => a.length - b.length || (a < b ? -1 : 1));
}

export function sortChildFirst(rpaths: string[]): string[] {
  const depth = (rel: string): number =>
    rel === "" ? 0 : rel.split("/").length;
  rpaths.sort((a, b) => {
    const da = depth(a);
    const db = depth(b);
    if (da !== db) {
      return db - da;
    }
    return a < b ? -1 : a > b ? 1 : 0;
  });
  return rpaths;
}

export async function rsyncCopyDirsChunked(
  workDir: string,
  fromRoot: string,
  toRoot: string,
  dirRpaths: string[],
  label: string,
  opts: {
    direction?;
    chunkSize?: number;
    concurrency?: number; // dirs usually OK at 2–4 as well
    dryRun?: boolean | string;
    verbose?: boolean | string;
    logger?: Logger;
    logLevel?: LogLevel;
    sshPort?: number;
    tempDir?: string;
    progressScope?: string;
    progressMeta?: Record<string, unknown>;
    captureTransfers?: boolean;
  } = {},
): Promise<string[]> {
  if (!dirRpaths.length) return [];
  const { logger, debug } = resolveLogContext(opts);
  const chunkSize = opts.chunkSize ?? REFLECT_DIR_CHUNK;
  const concurrency = opts.concurrency ?? Math.min(4, REFLECT_COPY_CONCURRENCY);

  const sorted = Array.from(new Set(dirRpaths)).sort(); // stable, deduped
  const batches = chunk(sorted, chunkSize);

  if (debug) {
    logger.debug(
      `>>> rsync mkdir ${label}: ${sorted.length} dirs in ${batches.length} batches (chunk=${chunkSize}, conc=${concurrency})`,
    );
  }

  // Prepare list files first to reduce interleaving on disk
  const listFiles: string[] = [];
  for (let i = 0; i < batches.length; i++) {
    const lf = path.join(
      workDir,
      `${label.replace(/[\s\(\)]+/g, "-")}.dirs.${i}.list`,
    );
    await writeNulList(lf, batches[i]);
    listFiles.push(lf);
  }

  const { captureTransfers, ...forwardOpts } = opts;
  const { progressScope, progressMeta } = forwardOpts;
  const scope = progressScope ?? label;
  const transferredSet = captureTransfers ? new Set<string>() : null;
  await parallelMapLimit(listFiles, concurrency, async (lf, idx) => {
    if (debug) {
      logger.debug(`>>> rsync mkdir ${label} [${idx + 1}/${listFiles.length}]`);
    }
    const chunkPaths = batches[idx];
    const res = await rsyncCopyDirs(
      fromRoot,
      toRoot,
      lf,
      `${label} (dirs chunk ${idx + 1})`,
      {
        direction: forwardOpts.direction,
        dryRun: forwardOpts.dryRun,
        verbose: forwardOpts.verbose,
        logger,
        logLevel: forwardOpts.logLevel,
        sshPort: forwardOpts.sshPort,
        tempDir: forwardOpts.tempDir,
        progressScope: scope,
        progressMeta: {
          ...(progressMeta ?? {}),
          label,
          chunkIndex: idx + 1,
          chunkCount: listFiles.length,
        },
        captureDirs: captureTransfers ? chunkPaths : undefined,
      },
    );
    if (captureTransfers && res.transferred.length) {
      for (const entry of res.transferred) transferredSet!.add(entry);
    }
  });
  if (!captureTransfers) {
    return [];
  }
  return Array.from(transferredSet ?? []);
}

// rsync does not reliably update directory metadata (perms/uid/gid/mtime)
// once the directory already exists; we run a second metadata-only pass to
// ensure the destination dir reflects the source immediately after a copy.
export async function rsyncFixMetaDirsChunked(
  workDir: string,
  fromRoot: string,
  toRoot: string,
  dirRpaths: string[],
  label: string,
  opts: {
    chunkSize?: number;
    concurrency?: number;
    dryRun?: boolean | string;
    verbose?: boolean | string;
    logger?: Logger;
    logLevel?: LogLevel;
    sshPort?: number;
    direction?;
    captureTransfers?: boolean;
  } = {},
): Promise<RsyncTransferRecord[]> {
  if (!dirRpaths.length) return [];
  const { logger, debug } = resolveLogContext(opts);
  const chunkSize = opts.chunkSize ?? REFLECT_DIR_CHUNK;
  const concurrency = opts.concurrency ?? Math.min(4, REFLECT_COPY_CONCURRENCY);
  const sorted = Array.from(new Set(dirRpaths)).sort();
  const batches = chunk(sorted, chunkSize);
  const listFiles: string[] = [];
  for (let i = 0; i < batches.length; i++) {
    const lf = path.join(
      workDir,
      `${label.replace(/[\s\(\)]+/g, "-")}.meta-dirs.${i}.list`,
    );
    await writeNulList(lf, batches[i]);
    listFiles.push(lf);
  }
  const captured: RsyncTransferRecord[] = [];
  await parallelMapLimit(listFiles, concurrency, async (lf, idx) => {
    if (debug) {
      logger.debug(
        `>>> rsync fixmeta ${label} [${idx + 1}/${listFiles.length}]`,
      );
    }
    const res = await rsyncFixMetaDirs(
      fromRoot,
      toRoot,
      lf,
      `${label} (dirs chunk ${idx + 1})`,
      {
        ...opts,
        direction: opts.direction,
        captureDirs: opts.captureTransfers ? batches[idx] : undefined,
      },
    );
    if (opts.captureTransfers && res.transfers?.length) {
      for (const entry of res.transfers) {
        captured.push({
          ...entry,
          path: normalizeTransferPath(entry.path),
        });
      }
    }
  });
  return opts.captureTransfers ? captured : [];
}

export async function rsyncCopyChunked(
  workDir: string,
  fromRoot: string,
  toRoot: string,
  fileRpaths: string[],
  label: string,
  opts: {
    direction?;
    chunkSize?: number;
    concurrency?: number;
    precreateDirs?: boolean; // precreate parent dirs with -d
    dryRun?: boolean | string;
    verbose?: boolean | string;
    compress?: RsyncCompressSpec;
    logger?: Logger;
    logLevel?: LogLevel;
    sshPort?: number;
    tempDir?: string;
    progressScope?: string;
    progressMeta?: Record<string, unknown>;
    onChunkResult?: (
      chunkPaths: string[],
      result: {
        ok: boolean;
        zero: boolean;
        code: number | null;
        stderr?: string;
      },
    ) => void | Promise<void>;
    captureTransfers?: boolean;
  } = {},
): Promise<{ ok: boolean; transferred: string[] }> {
  if (!fileRpaths.length) return { ok: true, transferred: [] };
  const { logger, debug } = resolveLogContext(opts);

  const chunkSize = opts.chunkSize ?? REFLECT_COPY_CHUNK;
  const concurrency = opts.concurrency ?? REFLECT_COPY_CONCURRENCY;

  // Sort for locality; dedupe
  const sorted = Array.from(new Set(fileRpaths)).sort();
  const batches = chunk(sorted, chunkSize);
  const { progressScope, progressMeta } = opts;
  const scope = progressScope ?? label;

  if (debug) {
    logger.debug(
      `>>> rsync copy ${label}: ${sorted.length} files in ${batches.length} batches (chunk=${chunkSize}, conc=${concurrency})`,
    );
  }

  // Optional: precreate parent directories once (chunked) to avoid per-chunk mkdir pressure.
  if (opts.precreateDirs !== false) {
    const dirs = parentDirsOf(sorted);
    if (dirs.length) {
      await rsyncCopyDirsChunked(workDir, fromRoot, toRoot, dirs, `${label}`, {
        direction: opts.direction,
        dryRun: opts.dryRun,
        verbose: opts.verbose,
        logger,
        logLevel: opts.logLevel,
        sshPort: opts.sshPort,
        tempDir: opts.tempDir,
        progressScope: scope,
        progressMeta: {
          ...(progressMeta ?? {}),
          label,
          phase: "precreate-dirs",
        },
      });
    }
  }

  // Pre-write list files to disk to keep worker bodies small
  const listFiles: string[] = [];
  for (let i = 0; i < batches.length; i++) {
    const lf = path.join(
      workDir,
      `${label.replace(/[\s\(\)]+/g, "-")}.files.${i}.list`,
    );
    await writeNulList(lf, batches[i]);
    listFiles.push(lf);
  }

  let allOk = true;
  const transferredSet = new Set<string>();

  await parallelMapLimit(listFiles, concurrency, async (lf, idx) => {
    if (debug) {
      logger.debug(`>>> rsync copy ${label} [${idx + 1}/${listFiles.length}]`);
    }
    const chunkMeta = {
      ...(progressMeta ?? {}),
      label,
      chunkIndex: idx + 1,
      chunkCount: listFiles.length,
    };
    const plannedChunk = opts.captureTransfers ? batches[idx] : undefined;
    const plannedSet = plannedChunk ? new Set(plannedChunk) : undefined;
    const result = await rsyncCopy(
      fromRoot,
      toRoot,
      lf,
      `${label} (chunk ${idx + 1})`,
      {
        direction: opts.direction,
        dryRun: opts.dryRun,
        verbose: opts.verbose,
        compress: opts.compress,
        logger,
        logLevel: opts.logLevel,
        sshPort: opts.sshPort,
        tempDir: opts.tempDir,
        progressScope: scope,
        progressMeta: chunkMeta,
        captureTransfers: plannedChunk
          ? {
              paths: plannedChunk,
            }
          : undefined,
      },
    );
    if (opts.captureTransfers && result.transfers?.length) {
      for (const entry of result.transfers) {
        const normalized = normalizeTransferPath(entry.path);
        if (!normalized) continue;
        if (
          REFLECT_RSYNC_VERY_VERBOSE &&
          plannedSet &&
          !plannedSet.has(normalized) &&
          debug
        ) {
          logger.debug("rsync transfer outside planned chunk", {
            label,
            path: entry.path,
          });
        }
        transferredSet.add(normalized);
      }
    }
    if (opts.onChunkResult) {
      await opts.onChunkResult(batches[idx], result);
    }
    if (!result.ok) allOk = false;
  });

  return { ok: allOk, transferred: Array.from(transferredSet) };
}

// ---------- helpers that used to be local to merge ----------
export function ensureTrailingSlash(root: string): string {
  return root.endsWith("/") ? root : root + "/";
}

export async function fileNonEmpty(p: string, logger?: Logger) {
  try {
    return (await fsStat(p)).size > 0;
  } catch (err) {
    logger?.warn("fileNonEmpty failed", { path: p, error: String(err) });
    return false;
  }
}

function isLocal(p: string) {
  // crude but good enough: user@host:/path or host:/path patterns
  return !/^[^:/]+@[^:/]+:|^[^:/]+:/.test(p);
}

function applySshTransport(
  args: string[],
  from: string,
  to: string,
  opts: RsyncLogOptions,
  compress?: RsyncCompressSpec,
) {
  const remote = !isLocal(from) || !isLocal(to);
  if (!remote) return;
  const pieces = ["ssh"];
  if (opts.sshPort != null) {
    pieces.push("-p", String(opts.sshPort));
  }
  const controlPath = process.env.REFLECT_SSH_CONTROL_PATH;
  if (controlPath) {
    pieces.push("-S", controlPath);
  }
  const disableCompression =
    compress && compress !== "none" && isCompressing(compress);
  if (disableCompression) {
    pieces.push("-oCompression=no");
  }
  if (pieces.length > 1) {
    args.push("-e", pieces.join(" "));
  }
}

function numericIdsFlag(): string[] {
  try {
    return process.geteuid?.() === 0 ? ["--numeric-ids"] : [];
  } catch {
    return [];
  }
}

export function rsyncArgsBase(opts: RsyncRunOptions, from: string, to: string) {
  const a = ["-a", "-I", "--relative", ...numericIdsFlag()];
  if (opts.dryRun) a.unshift("-n");
  if (isLocal(from) && isLocal(to)) {
    // don't use the rsync delta algorithm
    a.push("--whole-file");
  }
  if (!isDebugEnabled(opts)) a.push("--quiet");
  return a;
  // NOTE: -I disables rsync's quick-check so listed files always copy.
}

export function rsyncArgsDirs(opts: RsyncRunOptions) {
  // -d: transfer directories themselves (no recursion) — needed for empty dirs
  const a = [
    "-d",
    "--relative",
    "--from0",
    "--perms",
    "--times",
    "--group",
    "--owner",
    "--devices",
    ...numericIdsFlag(),
  ];
  if (opts.dryRun) a.unshift("-n");
  if (!isDebugEnabled(opts)) a.push("--quiet");
  return a;
}

// Metadata-only fixers (no content copy)
export function rsyncArgsFixMeta(opts: RsyncRunOptions) {
  // -a includes -pgo (perms, owner, group); --no-times prevents touching mtimes
  const a = ["-a", "--no-times", "--relative", "--from0", ...numericIdsFlag()];
  if (opts.dryRun) a.unshift("-n");
  if (!isDebugEnabled(opts)) a.push("--quiet");
  return a;
}
export function rsyncArgsFixMetaDirs(opts: RsyncRunOptions) {
  const a = [
    "-a",
    "-d",
    "--no-times",
    "--relative",
    "--from0",
    ...numericIdsFlag(),
  ];
  if (opts.dryRun) a.unshift("-n");
  if (!isDebugEnabled(opts)) a.push("--quiet");
  return a;
}

function isRsyncDiskFull(code: number | null, stderr?: string): boolean {
  if (code === 28) return true;
  if (!stderr) return false;
  const lower = stderr.toLowerCase();
  return (
    lower.includes("no space") ||
    lower.includes("disk is full") ||
    lower.includes("filesystem full") ||
    lower.includes("file system full")
  );
}

export function assertRsyncOk(
  label: string,
  res: { code: number | null; ok: boolean; zero: boolean; stderr?: string },
  context?: Record<string, unknown>,
) {
  if (res.ok) return;
  const base =
    typeof res.code === "number"
      ? `exit code ${res.code}`
      : "unknown exit code";
  const message = `rsync ${label} failed (${base})`;
  const errorContext = { label, ...context, code: res.code };
  if (isRsyncDiskFull(res.code, res.stderr)) {
    throw new DiskFullError(message, res.code ?? null, errorContext);
  }
  throw new RsyncError(message, res.code ?? null, errorContext);
}

function containsDelimiterChar(
  paths: readonly string[] | undefined,
  token: string,
): boolean {
  if (!paths?.length) return false;
  for (const p of paths) {
    if (p.includes(token)) return true;
  }
  return false;
}

function pickDelimiterChar(
  paths: readonly string[] | undefined,
  preferred: string,
  avoid: Set<string>,
): string {
  if (!avoid.has(preferred) && !containsDelimiterChar(paths, preferred)) {
    return preferred;
  }
  for (let code = DELIM_FALLBACK_START; code <= DELIM_FALLBACK_END; code++) {
    const candidate = String.fromCharCode(code);
    if (avoid.has(candidate)) continue;
    if (!containsDelimiterChar(paths, candidate)) {
      return candidate;
    }
  }
  throw new Error("unable to find safe delimiter for rsync transfer capture");
}

function chooseDelimiters(paths?: readonly string[]) {
  const avoid = new Set<string>();
  const field = pickDelimiterChar(paths, DEFAULT_FIELD_DELIM, avoid);
  avoid.add(field);
  const record = pickDelimiterChar(paths, DEFAULT_RECORD_DELIM, avoid);
  if (record === field) {
    avoid.add(record);
    const next = pickDelimiterChar(paths, DEFAULT_RECORD_DELIM, avoid);
    return { field, record: next };
  }
  return { field, record };
}

function stripRecordPreamble(segment: string): string {
  if (!segment) return segment;
  return segment.replace(/^\r?\n/, "");
}

function parseTransferLog(
  data: string,
  field: string,
  record: string,
): RsyncTransferRecord[] {
  if (!data) return [];
  const entries = data.split(record);
  const records: RsyncTransferRecord[] = [];
  for (const raw of entries) {
    if (!raw) continue;
    const normalized = stripRecordPreamble(raw);
    if (!normalized) continue;
    const parts = normalized.split(field);
    if (parts.length < 2) continue;
    const [itemized, mode, user, group, rawPath] = parts;
    const path = rawPath?.trim();
    if (!path) continue;
    records.push({
      path,
      itemized,
      mode: mode?.trim() || undefined,
      user: user?.trim() || undefined,
      group: group?.trim() || undefined,
    });
  }
  return records;
}

function normalizeTransferPath(p: string): string {
  if (!p) return p;
  let normalized = p.startsWith("./") ? p.slice(2) : p;
  if (normalized.endsWith("/") && normalized.length > 1) {
    normalized = normalized.slice(0, -1);
  }
  return normalized;
}

export async function run(
  cmd: string,
  args: string[],
  okCodes: number[] = [0],
  opts: RsyncRunOptions = {},
): Promise<{
  code: number | null;
  ok: boolean;
  zero: boolean;
  stderr?: string;
  transfers: RsyncTransferRecord[];
}> {
  const t = Date.now();
  const { logger, debug } = resolveLogContext(opts);
  const wantProgress = typeof opts.onProgress === "function";
  const finalArgs = wantProgress ? [...PROGRESS_ARGS, ...args] : [...args];
  const tempDirArg =
    opts.tempDir && opts.tempDir.trim() ? opts.tempDir.trim() : undefined;
  const filesFromIndex = finalArgs.findIndex((arg) =>
    arg.startsWith("--files-from="),
  );
  let files;
  if (REFLECT_RSYNC_VERY_VERBOSE && filesFromIndex >= 0) {
    files = logFiles(finalArgs[filesFromIndex]);
  } else {
    files = "...";
  }
  if (tempDirArg && !finalArgs.some((arg) => arg.startsWith("--temp-dir="))) {
    const insertIndex =
      filesFromIndex >= 0 ? filesFromIndex : Math.max(finalArgs.length - 2, 0);
    finalArgs.splice(insertIndex, 0, `--temp-dir=${tempDirArg}`);
  }
  if (process.env.REFLECT_RSYNC_BWLIMIT) {
    // This can be very useful for testing/debugging to simulate a slower network:
    finalArgs.push(`--bwlimit=${process.env.REFLECT_RSYNC_BWLIMIT}`);
  }
  const captureSpec = opts.captureTransfers ? opts.captureTransfers : undefined;
  const runOnce = async () => {
    const argsForRun = [...finalArgs];
    let captureDir: string | undefined;
    let captureLogPath: string | undefined;
    let captureDelims: { field: string; record: string } | undefined;
    if (captureSpec) {
      captureDelims = chooseDelimiters(captureSpec.paths);
      captureDir = await mkdtemp(path.join(tmpdir(), "reflect-rsync-log-"));
      captureLogPath = path.join(captureDir, "transfers.log");
      argsForRun.push(`--log-file=${captureLogPath}`);
      argsForRun.push(
        `--log-file-format=%i${captureDelims.field}%B${captureDelims.field}%U${captureDelims.field}%G${captureDelims.field}%n${captureDelims.record}`,
      );
    } else {
      captureDir = undefined;
      captureLogPath = undefined;
      captureDelims = undefined;
    }
    if (debug)
      logger.debug(
        `rsync.run ${opts.direction ?? ""}: '${cmd} ${argsJoin(argsForRun)}'`,
        { files },
      );
    const throttleMs =
      wantProgress && MAX_PROGRESS_UPDATES_PER_SEC > 0
        ? Math.max(10, Math.floor(1000 / MAX_PROGRESS_UPDATES_PER_SEC))
        : 0;

    const res = await new Promise<{
      code: number | null;
      ok: boolean;
      zero: boolean;
      stderr?: string;
    }>((resolve) => {
    const stdio: ("ignore" | "pipe" | "inherit")[] | "inherit" = wantProgress
      ? ["ignore", "pipe", "pipe"]
      : ["ignore", "ignore", "ignore"];

    const child = spawn(cmd, argsForRun, {
      stdio,
    });

    let lastEmit = 0;
    let lastPercent = -1;
    let stdoutBuffer = "";
    let stderrBuffer = "";

    if (wantProgress && child.stdout) {
      child.stdout.setEncoding("utf8");
      child.stdout.on("data", (chunk: string) => {
        stdoutBuffer += chunk;
        const pieces = stdoutBuffer.split(/[\r\n]+/);
        stdoutBuffer = pieces.pop() ?? "";
        for (const piece of pieces) {
          const event = parseProgressLine(piece);
          if (!event) continue;
          const now = Date.now();
          if (event.percent < lastPercent) {
            continue;
          }
          if (event.percent === lastPercent && now - lastEmit < throttleMs) {
            continue;
          }
          lastPercent = event.percent;
          lastEmit = now;
          try {
            opts.onProgress?.(event);
          } catch (err) {
            if (debug) {
              logger.warn("rsync onProgress handler failed", {
                error: err instanceof Error ? err.message : String(err),
              });
            }
          }
        }
      });
    }

    if (wantProgress && child.stderr) {
      child.stderr.setEncoding("utf8");
      child.stderr.on("data", (chunk: string) => {
        stderrBuffer += chunk;
      });
    }

    child.on("exit", (code) => {
      const zero = code === 0;
      const ok = code !== null && okCodes.includes(code!);
      if (wantProgress && lastPercent < 100 && ok) {
        try {
          opts.onProgress?.({
            transferredBytes: NaN,
            percent: 100,
          });
        } catch {}
      }
      resolve({ code, ok, zero, stderr: stderrBuffer });
      if (debug) {
        logger.debug("rsync exit", {
          cmd,
          code,
          ok,
          elapsedMs: Date.now() - t,
        });
      }
      if (!ok && stderrBuffer && !debug) {
        logger.warn("rsync stderr", {
          stderr: truncateMiddle(stderrBuffer, 400),
        });
      }
    });

    child.on("error", (err) => {
      if (wantProgress && stderrBuffer) {
        logger.warn("rsync stderr", {
          stderr: truncateMiddle(stderrBuffer, 400),
        });
      }
      logger.error("rsync spawn error", {
        error: err instanceof Error ? err.message : String(err),
      });
      resolve({ code: 1, ok: okCodes.includes(1), zero: false });
    });
  });

    let transfers: RsyncTransferRecord[] = [];
    if (captureSpec && captureLogPath && captureDelims) {
      try {
        const raw = await readFile(captureLogPath, "utf8");
        transfers = parseTransferLog(
          raw,
          captureDelims.field,
          captureDelims.record,
        );
      } catch (err) {
        if (debug) {
          logger.warn("failed to read rsync transfer log", {
            error: err instanceof Error ? err.message : String(err),
          });
        }
      }
    }
    if (captureDir) {
      try {
        await rm(captureDir, { recursive: true, force: true });
      } catch {}
    }
    if (REFLECT_RSYNC_VERY_VERBOSE && transfers) {
      logger.debug(`rsync.run ${opts.direction ?? ""}`, {
        transfers: transfers.map(({ path }) => path),
      });
    }
    return { ...res, transfers };
  };

  for (let attempt = 0; attempt < 2; attempt += 1) {
    const result = await runOnce();
    if (
      result.ok ||
      !(await maybeRestartSshControl(result.stderr ?? "")) ||
      attempt === 1
    ) {
      return result;
    }
    logger.info("rsync retry after restarting ssh control master", {
      direction: opts.direction,
    });
  }
  return await runOnce();
}

// write a NUL-separated list memory efficiently
export async function writeNulList(file: string, items: string[]) {
  const ws = createWriteStream(file);
  for (const it of items) {
    if (!ws.write(it + "\0")) {
      await new Promise<void>((resolve) => ws.once("drain", resolve));
    }
  }
  ws.end();
  await finished(ws);
}

// divide arr up into chunks of size at most n.
export function chunk<T>(arr: T[], n: number): T[][] {
  if (arr.length <= n) {
    return [arr];
  }
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += n) {
    out.push(arr.slice(i, i + n));
  }
  return out;
}

function parseProgressLine(line: string): RsyncProgressEvent | null {
  const trimmed = line.trim();
  if (!trimmed) return null;
  const parts = trimmed.split(/\s+/);
  if (parts.length < 2) return null;
  const percentToken = parts[1];
  if (!percentToken.endsWith("%")) return null;
  const transferred = Number(parts[0].replace(/,/g, ""));
  if (!Number.isFinite(transferred)) return null;
  const percent = Number(percentToken.slice(0, -1));
  if (!Number.isFinite(percent)) return null;
  const speed = parts[2];
  const eta = parts[3];
  const totalBytes =
    percent > 0 ? Math.round((transferred * 100) / percent) : undefined;
  return {
    transferredBytes: transferred,
    percent,
    totalBytes,
    speed,
    etaMilliseconds: parseEta(eta),
  };
}

function parseEta(token?: string): number | undefined {
  if (!token) return undefined;
  const segments = token.split(":").map((s) => Number(s));
  if (segments.some((n) => Number.isNaN(n))) return undefined;
  if (segments.length === 3) {
    const [h, m, s] = segments;
    return (h * 3600 + m * 60 + s) * 1000;
  }
  if (segments.length === 2) {
    const [m, s] = segments;
    return (m * 60 + s) * 1000;
  }
  if (segments.length === 1) {
    return segments[0] * 1000;
  }
  return undefined;
}

function truncateMiddle(input: string, max = 200): string {
  if (input.length <= max) return input;
  const half = Math.floor((max - 3) / 2);
  return `${input.slice(0, half)}...${input.slice(-half)}`;
}

async function rsyncCopy(
  fromRoot: string,
  toRoot: string,
  listFile: string,
  label: string,
  opts: {
    dryRun?: boolean | string;
    verbose?: boolean | string;
    compress?: RsyncCompressSpec;
    logger?: Logger;
    logLevel?: LogLevel;
    sshPort?: number;
    tempDir?: string;
    progressScope?: string;
    progressMeta?: Record<string, unknown>;
    direction?;
    captureTransfers?: TransferCaptureSpec;
  } = {},
): Promise<{
  ok: boolean;
  zero: boolean;
  code: number | null;
  stderr?: string;
  transfers: RsyncTransferRecord[];
}> {
  const { logger, debug } = resolveLogContext(opts);
  if (!(await fileNonEmpty(listFile, debug ? logger : undefined))) {
    if (debug) logger.debug(`>>> rsync ${label}: nothing to do`);
    return { ok: true, zero: false, code: 0, transfers: [] };
  }
  if (debug) {
    logger.debug(`>>> rsync ${label} (${fromRoot} -> ${toRoot})`);
  }
  const from = ensureTrailingSlash(fromRoot);
  const to = ensureTrailingSlash(toRoot);
  const args = [
    ...rsyncArgsBase(opts, fromRoot, toRoot),
    ...rsyncCompressionArgs(opts.compress),
    "--from0",
    `--files-from=${listFile}`,
    from,
    to,
  ];
  applySshTransport(args, from, to, opts, opts.compress);
  const { progressScope, progressMeta, ...runOpts } = opts;
  const scope = progressScope ?? label;
  const progressHandler = runOpts.logger
    ? (event: RsyncProgressEvent) => {
        runOpts.logger!.info("progress", {
          scope,
          label,
          transferredBytes: event.transferredBytes,
          totalBytes: event.totalBytes ?? null,
          percent: event.percent,
          speed: event.speed ?? null,
          etaMilliseconds: event.etaMilliseconds ?? null,
          ...progressMeta,
        });
      }
    : undefined;
  const res = await run("rsync", args, [0, 23, 24], {
    ...runOpts,
    onProgress: progressHandler,
    tempDir: opts.tempDir,
    captureTransfers: opts.captureTransfers,
  }); // accept partials
  if (debug) {
    logger.debug(`>>> rsync ${label}: done`, {
      code: res.code,
      ok: res.ok,
    });
  }
  return {
    ok: res.ok,
    zero: res.zero,
    code: res.code,
    stderr: res.stderr,
    transfers: res.transfers ?? [],
  };
}

export async function rsyncCopyDirs(
  fromRoot: string,
  toRoot: string,
  listFile: string,
  label: string,
  opts: {
    direction?;
    dryRun?: boolean | string;
    verbose?: boolean | string;
    logger?: Logger;
    logLevel?: LogLevel;
    sshPort?: number;
    tempDir?: string;
    progressScope?: string;
    progressMeta?: Record<string, unknown>;
    captureDirs?: readonly string[];
  } = {},
): Promise<{ ok: boolean; zero: boolean; transferred: string[] }> {
  const { logger, debug } = resolveLogContext(opts);
  if (!(await fileNonEmpty(listFile, debug ? logger : undefined))) {
    if (debug) logger.debug(`>>> rsync ${label} (dirs): nothing to do`);
    return { ok: true, zero: false, transferred: [] };
  }
  if (debug) {
    logger.debug(`>>> rsync ${label} (dirs) (${fromRoot} -> ${toRoot})`);
  }
  const from = ensureTrailingSlash(fromRoot);
  const to = ensureTrailingSlash(toRoot);
  const args = [...rsyncArgsDirs(opts), `--files-from=${listFile}`, from, to];
  applySshTransport(args, from, to, opts);
  const { progressScope, progressMeta, ...runOpts } = opts;
  const scope = progressScope ?? label;
  const progressHandler = runOpts.logger
    ? (event: RsyncProgressEvent) => {
        runOpts.logger!.info("progress", {
          scope,
          label,
          transferredBytes: event.transferredBytes,
          totalBytes: event.totalBytes ?? null,
          percent: event.percent,
          speed: event.speed ?? null,
          etaMilliseconds: event.etaMilliseconds ?? null,
          ...progressMeta,
        });
      }
    : undefined;
  const captureList =
    Array.isArray(opts.captureDirs) && opts.captureDirs.length
      ? opts.captureDirs
      : undefined;
  const plannedSet = captureList
    ? new Set(captureList.map((p) => normalizeTransferPath(p)))
    : undefined;
  const res = await run("rsync", args, [0, 23, 24], {
    ...runOpts,
    onProgress: progressHandler,
    tempDir: opts.tempDir,
    direction: opts.direction,
    captureTransfers: captureList ? { paths: captureList } : undefined,
  });
  assertRsyncOk(`${label} (dirs)`, res, { from: fromRoot, to: toRoot });
  if (debug) {
    logger.debug(`>>> rsync ${label} (dirs): done`, {
      code: res.code,
      ok: res.ok,
    });
  }
  const transferred =
    captureList && res.transfers?.length
      ? res.transfers
          .map((entry) => normalizeTransferPath(entry.path))
          .filter((p) => (plannedSet ? plannedSet.has(p) : true))
      : [];
  return { ok: true, zero: res.zero, transferred };
}

export async function rsyncFixMeta(
  fromRoot: string,
  toRoot: string,
  listFile: string,
  label: string,
  opts: {
    dryRun?: boolean | string;
    verbose?: boolean | string;
    logger?: Logger;
    logLevel?: LogLevel;
    sshPort?: number;
    direction?;
  } = {},
): Promise<{ ok: boolean; zero: boolean }> {
  const { logger, debug } = resolveLogContext(opts);
  if (!(await fileNonEmpty(listFile, debug ? logger : undefined))) {
    if (debug) logger.debug(`>>> rsync ${label}: nothing to do`);
    return { ok: true, zero: false };
  }
  if (debug) {
    logger.debug(`>>> rsync ${label} (meta) (${fromRoot} -> ${toRoot})`);
  }
  const from = ensureTrailingSlash(fromRoot);
  const to = ensureTrailingSlash(toRoot);
  const args = [
    ...rsyncArgsFixMeta(opts),
    `--files-from=${listFile}`,
    from,
    to,
  ];
  applySshTransport(args, from, to, opts);
  const res = await run("rsync", args, [0, 23, 24], opts);
  assertRsyncOk(`${label} (meta)`, res, { from: fromRoot, to: toRoot });
  if (debug) {
    logger.debug(`>>> rsync ${label} (meta): done`, {
      code: res.code,
      ok: res.ok,
    });
  }
  return { ok: true, zero: res.zero };
}

export async function rsyncFixMetaDirs(
  fromRoot: string,
  toRoot: string,
  listFile: string,
  label: string,
  opts: {
    dryRun?: boolean | string;
    verbose?: boolean | string;
    logger?: Logger;
    logLevel?: LogLevel;
    sshPort?: number;
    direction?;
    captureDirs?: string[];
  } = {},
): Promise<{ ok: boolean; zero: boolean; transfers: RsyncTransferRecord[] }> {
  const { logger, debug } = resolveLogContext(opts);
  if (!(await fileNonEmpty(listFile, debug ? logger : undefined))) {
    if (debug) logger.debug(`>>> rsync ${label} (meta dirs): nothing to do`);
    return { ok: true, zero: false, transfers: [] };
  }
  if (debug) {
    logger.debug(`>>> rsync ${label} (meta dirs) (${fromRoot} -> ${toRoot})`);
  }
  const from = ensureTrailingSlash(fromRoot);
  const to = ensureTrailingSlash(toRoot);
  const args = [
    ...rsyncArgsFixMetaDirs(opts),
    `--files-from=${listFile}`,
    from,
    to,
  ];
  applySshTransport(args, from, to, opts);
  const captureSpec = opts.captureDirs ? { paths: opts.captureDirs } : undefined;
  const res = await run("rsync", args, [0, 23, 24], {
    ...opts,
    captureTransfers: captureSpec,
  });
  assertRsyncOk(`${label} (meta dirs)`, res, { from: fromRoot, to: toRoot });
  if (debug) {
    logger.debug(`>>> rsync ${label} (meta dirs): done`, {
      code: res.code,
      ok: res.ok,
    });
  }
  return { ok: true, zero: res.zero, transfers: res.transfers ?? [] };
}

// -- DELETE --



function logFiles(listFile: string) {
  if (!REFLECT_RSYNC_VERY_VERBOSE || !listFile) {
    return "";
  }
  try {
    const path = listFile.split("=").slice(-1)[0];
    let v = readFileSync(path, "utf8")
      .split("\0")
      .filter((x) => x);
    if (v.length >= REFLECT_RSYNC_VERY_VERBOSE_MAX_FILES) {
      v = v.slice(0, REFLECT_RSYNC_VERY_VERBOSE_MAX_FILES);
      v.push("TRUNCATED");
    }
    return v;
  } catch (err) {
    return `${err}`;
  }
}
