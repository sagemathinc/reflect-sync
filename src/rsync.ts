// src/rsync.ts
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { mkdtemp, rm, stat as fsStat, mkdir } from "node:fs/promises";
import path from "node:path";
import { createWriteStream } from "node:fs";
import { finished } from "node:stream/promises";
import {
  isCompressing,
  type RsyncCompressSpec,
  rsyncCompressionArgs,
} from "./rsync-compression.js";
import { argsJoin } from "./remote.js";
import { ConsoleLogger, type LogLevel, type Logger } from "./logger.js";

// extremely verbose -- showing all output of rsync, which
// can be massive, of course.
const verbose2 = !!process.env.REFLECT_VERBOSE2;

const REFLECT_COPY_CHUNK = Number(process.env.REFLECT_COPY_CHUNK ?? 10_000);
const REFLECT_COPY_CONCURRENCY = Number(
  process.env.REFLECT_COPY_CONCURRENCY ?? 2,
);
const REFLECT_DIR_CHUNK = Number(process.env.REFLECT_DIR_CHUNK ?? 20_000);

export const RSYNC_TEMP_DIR =
  process.env.REFLECT_RSYNC_TEMP_DIR ?? ".reflect-rsync-tmp";

export async function ensureTempDir(root: string): Promise<void> {
  if (!root) return;
  const dir = path.join(root, RSYNC_TEMP_DIR);
  try {
    await mkdir(dir, { recursive: true });
  } catch (err: any) {
    if (err?.code !== "EEXIST") {
      throw err;
    }
  }
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
  const debug = toBoolVerbose(opts.verbose) || level === "debug";
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

export async function rsyncCopyDirsChunked(
  workDir: string,
  fromRoot: string,
  toRoot: string,
  dirRpaths: string[],
  label: string,
  opts: {
    chunkSize?: number;
    concurrency?: number; // dirs usually OK at 2–4 as well
    dryRun?: boolean | string;
    verbose?: boolean | string;
    logger?: Logger;
    logLevel?: LogLevel;
    sshPort?: number;
    progressScope?: string;
    progressMeta?: Record<string, unknown>;
  } = {},
): Promise<void> {
  if (!dirRpaths.length) return;
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

  const { progressScope, progressMeta } = opts;
  const scope = progressScope ?? label;
  await parallelMapLimit(listFiles, concurrency, async (lf, idx) => {
    if (debug) {
      logger.debug(`>>> rsync mkdir ${label} [${idx + 1}/${listFiles.length}]`);
    }
    await rsyncCopyDirs(
      fromRoot,
      toRoot,
      lf,
      `${label} (dirs chunk ${idx + 1})`,
      {
        dryRun: opts.dryRun,
        verbose: opts.verbose,
        logger,
        logLevel: opts.logLevel,
        sshPort: opts.sshPort,
        progressScope: scope,
        progressMeta: {
          ...(progressMeta ?? {}),
          label,
          chunkIndex: idx + 1,
          chunkCount: listFiles.length,
        },
      },
    );
  });
}

export async function rsyncCopyChunked(
  workDir: string,
  fromRoot: string,
  toRoot: string,
  fileRpaths: string[],
  label: string,
  opts: {
    chunkSize?: number;
    concurrency?: number;
    precreateDirs?: boolean; // precreate parent dirs with -d
    dryRun?: boolean | string;
    verbose?: boolean | string;
    compress?: RsyncCompressSpec;
    logger?: Logger;
    logLevel?: LogLevel;
    sshPort?: number;
    progressScope?: string;
    progressMeta?: Record<string, unknown>;
  } = {},
): Promise<{ ok: boolean }> {
  if (!fileRpaths.length) return { ok: true };
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
        dryRun: opts.dryRun,
        verbose: opts.verbose,
        logger,
        logLevel: opts.logLevel,
        sshPort: opts.sshPort,
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
    const { ok } = await rsyncCopy(
      fromRoot,
      toRoot,
      lf,
      `${label} (chunk ${idx + 1})`,
      {
        dryRun: opts.dryRun,
        verbose: opts.verbose,
        compress: opts.compress,
        logger,
        logLevel: opts.logLevel,
        sshPort: opts.sshPort,
        progressScope: scope,
        progressMeta: chunkMeta,
      },
    );
    if (!ok) allOk = false;
  });

  return { ok: allOk };
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
  if (verbose2) a.push("-v");
  if (!isDebugEnabled(opts) && !verbose2) a.push("--quiet");
  return a;
  // NOTE: -I disables rsync's quick-check so listed files always copy.
}

export function rsyncArgsDirs(opts: RsyncRunOptions) {
  // -d: transfer directories themselves (no recursion) — needed for empty dirs
  const a = ["-a", "-d", "--relative", "--from0", ...numericIdsFlag()];
  if (opts.dryRun) a.unshift("-n");
  if (verbose2) a.push("-v");
  if (!isDebugEnabled(opts) && !verbose2) a.push("--quiet");
  return a;
}

export function rsyncArgsDelete(opts: RsyncRunOptions) {
  const a = [
    "-a",
    "--relative",
    "--from0",
    "--ignore-missing-args",
    "--delete-missing-args",
    "--force",
    ...numericIdsFlag(),
  ];
  if (opts.dryRun) {
    a.unshift("-n");
  }
  if (verbose2) a.push("-v");
  if (!isDebugEnabled(opts) && !verbose2) a.push("--quiet");
  return a;
}

// Metadata-only fixers (no content copy)
export function rsyncArgsFixMeta(opts: RsyncRunOptions) {
  // -a includes -pgo (perms, owner, group); --no-times prevents touching mtimes
  const a = ["-a", "--no-times", "--relative", "--from0", ...numericIdsFlag()];
  if (opts.dryRun) a.unshift("-n");
  if (verbose2) a.push("-v");
  if (!isDebugEnabled(opts) && !verbose2) a.push("--quiet");
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
  if (verbose2) a.push("-v");
  if (!isDebugEnabled(opts) && !verbose2) a.push("--quiet");
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

export function run(
  cmd: string,
  args: string[],
  okCodes: number[] = [0],
  opts: RsyncRunOptions = {},
): Promise<{
  code: number | null;
  ok: boolean;
  zero: boolean;
  stderr?: string;
}> {
  const t = Date.now();
  const { logger, debug } = resolveLogContext(opts);
  const wantProgress = typeof opts.onProgress === "function";
  const finalArgs = wantProgress ? [...PROGRESS_ARGS, ...args] : [...args];
  if (!finalArgs.some((arg) => arg.startsWith("--temp-dir="))) {
    const filesFromIndex = finalArgs.findIndex((arg) =>
      arg.startsWith("--files-from="),
    );
    const insertIndex =
      filesFromIndex >= 0 ? filesFromIndex : Math.max(finalArgs.length - 2, 0);
    finalArgs.splice(insertIndex, 0, `--temp-dir=${RSYNC_TEMP_DIR}`);
  }
  if (process.env.REFLECT_RSYNC_BWLIMIT) {
    // This can be very useful for testing/debugging to simulate a slower network:
    finalArgs.push(`--bwlimit=${process.env.REFLECT_RSYNC_BWLIMIT}`);
  }
  if (debug)
    logger.debug("rsync exec", {
      cmd,
      args: argsJoin(finalArgs),
    });

  const throttleMs =
    wantProgress && MAX_PROGRESS_UPDATES_PER_SEC > 0
      ? Math.max(10, Math.floor(1000 / MAX_PROGRESS_UPDATES_PER_SEC))
      : 0;

  return new Promise((resolve) => {
    const stdio: ("ignore" | "pipe" | "inherit")[] | "inherit" = wantProgress
      ? ["ignore", "pipe", "pipe"]
      : verbose2
        ? "inherit"
        : ["ignore", "ignore", "ignore"];

    const child = spawn(cmd, finalArgs, {
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

export async function rsyncCopy(
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
    progressScope?: string;
    progressMeta?: Record<string, unknown>;
  } = {},
): Promise<{ ok: boolean; zero: boolean }> {
  const { logger, debug } = resolveLogContext(opts);
  if (!(await fileNonEmpty(listFile, debug ? logger : undefined))) {
    if (debug) logger.debug(`>>> rsync ${label}: nothing to do`);
    return { ok: true, zero: false };
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
  }); // accept partials
  if (debug) {
    logger.debug(`>>> rsync ${label}: done`, {
      code: res.code,
      ok: res.ok,
    });
  }
  return { ok: res.ok, zero: res.zero };
}

export async function rsyncCopyDirs(
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
    progressScope?: string;
    progressMeta?: Record<string, unknown>;
  } = {},
): Promise<{ ok: boolean; zero: boolean }> {
  const { logger, debug } = resolveLogContext(opts);
  if (!(await fileNonEmpty(listFile, debug ? logger : undefined))) {
    if (debug) logger.debug(`>>> rsync ${label} (dirs): nothing to do`);
    return { ok: true, zero: false };
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
  const res = await run("rsync", args, [0, 23, 24], {
    ...runOpts,
    onProgress: progressHandler,
  });
  assertRsyncOk(`${label} (dirs)`, res, { from: fromRoot, to: toRoot });
  if (debug) {
    logger.debug(`>>> rsync ${label} (dirs): done`, {
      code: res.code,
      ok: res.ok,
    });
  }
  return { ok: true, zero: res.zero };
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
  } = {},
): Promise<{ ok: boolean; zero: boolean }> {
  const { logger, debug } = resolveLogContext(opts);
  if (!(await fileNonEmpty(listFile, debug ? logger : undefined))) {
    if (debug) logger.debug(`>>> rsync ${label} (meta dirs): nothing to do`);
    return { ok: true, zero: false };
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
  const res = await run("rsync", args, [0, 23, 24], opts);
  assertRsyncOk(`${label} (meta dirs)`, res, { from: fromRoot, to: toRoot });
  if (debug) {
    logger.debug(`>>> rsync ${label} (meta dirs): done`, {
      code: res.code,
      ok: res.ok,
    });
  }
  return { ok: true, zero: res.zero };
}

export async function rsyncDelete(
  fromRoot: string,
  toRoot: string,
  listFile: string,
  label: string,
  opts: {
    forceEmptySource?: boolean;
    dryRun?: boolean | string;
    verbose?: boolean | string;
    logger?: Logger;
    logLevel?: LogLevel;
    sshPort?: number;
  } = {},
): Promise<void> {
  const { logger, debug } = resolveLogContext(opts);
  if (!(await fileNonEmpty(listFile, debug ? logger : undefined))) {
    if (debug) logger.debug(`>>> rsync delete ${label}: nothing to do`);
    return;
  }

  // Force all listed paths to be "missing" by using an empty temp source dir
  let sourceRoot = ensureTrailingSlash(fromRoot);
  let tmpEmptyDir: string | null = null;
  try {
    if (opts.forceEmptySource) {
      tmpEmptyDir = await mkdtemp(path.join(tmpdir(), "rsync-empty-"));
      sourceRoot = ensureTrailingSlash(tmpEmptyDir);
    }

    if (debug) {
      logger.debug(
        `>>> rsync delete ${label} (missing in ${sourceRoot} => delete in ${toRoot})`,
      );
    }
    const to = ensureTrailingSlash(toRoot);
    const args = [
      ...rsyncArgsDelete(opts), // includes --delete-missing-args --force
      `--files-from=${listFile}`,
      sourceRoot,
      to,
    ];
    applySshTransport(args, sourceRoot, to, opts);
    const res = await run("rsync", args, [0, 24], opts);
    assertRsyncOk(`delete ${label}`, res, { from: sourceRoot, to });
  } finally {
    if (tmpEmptyDir) {
      await rm(tmpEmptyDir, { recursive: true, force: true });
    }
  }
}

export async function rsyncDeleteChunked(
  workDir: string,
  fromRoot: string,
  toRoot: string,
  rpaths: string[],
  label: string,
  opts: {
    forceEmptySource?: boolean;
    chunkSize?: number;
    dryRun?: boolean | string;
    verbose?: boolean | string;
    logger?: Logger;
    logLevel?: LogLevel;
    sshPort?: number;
  } = {},
) {
  if (!rpaths.length) return;
  const { logger, debug } = resolveLogContext(opts);
  const { chunkSize = 50_000 } = opts;
  const batches = chunk(rpaths, chunkSize);
  if (debug) {
    logger.debug(
      `>>> rsync delete ${label}: ${rpaths.length} in ${batches.length} batches of size at most ${chunkSize}`,
    );
  }

  for (let i = 0; i < batches.length; i++) {
    if (debug) {
      logger.debug(`>>> rsync delete ${label} [${i + 1}/${batches.length}]`);
    }
    const listFile = path.join(
      workDir,
      `${label.replace(/[\s\(\)]+/g, "-")}.${i}.list`,
    );
    await writeNulList(listFile, batches[i]);
    await rsyncDelete(
      fromRoot,
      toRoot,
      listFile,
      `${label} [${i + 1}/${batches.length}]`,
      {
        forceEmptySource: opts.forceEmptySource,
        dryRun: opts.dryRun,
        verbose: opts.verbose,
        logger,
        logLevel: opts.logLevel,
        sshPort: opts.sshPort,
      },
    );
  }
}
