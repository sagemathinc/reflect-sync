// src/rsync.ts
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { mkdtemp, rm, stat as fsStat } from "node:fs/promises";
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

type RsyncLogOptions = {
  logger?: Logger;
  verbose?: boolean | string;
  logLevel?: LogLevel;
};

type RsyncRunOptions = RsyncLogOptions & {
  dryRun?: boolean | string;
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
  } = {},
): Promise<{ ok: boolean }> {
  if (!fileRpaths.length) return { ok: true };
  const { logger, debug } = resolveLogContext(opts);

  const chunkSize = opts.chunkSize ?? REFLECT_COPY_CHUNK;
  const concurrency = opts.concurrency ?? REFLECT_COPY_CONCURRENCY;

  // Sort for locality; dedupe
  const sorted = Array.from(new Set(fileRpaths)).sort();
  const batches = chunk(sorted, chunkSize);

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

function numericIdsFlag(): string[] {
  try {
    return process.geteuid?.() === 0 ? ["--numeric-ids"] : [];
  } catch {
    return [];
  }
}

export function rsyncArgsBase(
  opts: RsyncRunOptions,
  from: string,
  to: string,
) {
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

export function run(
  cmd: string,
  args: string[],
  okCodes: number[] = [0],
  opts: RsyncLogOptions = {},
): Promise<{ code: number | null; ok: boolean; zero: boolean }> {
  const t = Date.now();
  const { logger, debug } = resolveLogContext(opts);
  if (debug) logger.debug("rsync exec", { cmd, args: argsJoin(args) });
  return new Promise((resolve) => {
    // ignore is critical for stdio since we don't read the output
    // and there is a lot, so it would otherwise DEADLOCK.
    const p = spawn(cmd, args, {
      stdio: verbose2 ? "inherit" : ["ignore", "ignore", "ignore"],
    });
    p.on("exit", (code) => {
      const zero = code === 0;
      const ok = code !== null && okCodes.includes(code!);
      resolve({ code, ok, zero });
      if (debug) {
        logger.debug("rsync exit", {
          cmd,
          code,
          ok,
          elapsedMs: Date.now() - t,
        });
      }
    });
    p.on("error", () =>
      resolve({ code: 1, ok: okCodes.includes(1), zero: false }),
    );
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
  const args = [
    ...rsyncArgsBase(opts, fromRoot, toRoot),
    ...rsyncCompressionArgs(opts.compress),
    "--from0",
    `--files-from=${listFile}`,
    ensureTrailingSlash(fromRoot),
    ensureTrailingSlash(toRoot),
  ];

  if (
    (!fromRoot.startsWith("/") || !toRoot.startsWith("/")) &&
    opts.compress &&
    opts.compress != "none" &&
    isCompressing(opts.compress)
  ) {
    args.push("-e", "ssh -oCompression=no");
  }

  const res = await run("rsync", args, [0, 23, 24], opts); // accept partials
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
  } = {},
): Promise<{ ok: boolean; zero: boolean }> {
  const { logger, debug } = resolveLogContext(opts);
  if (!(await fileNonEmpty(listFile, debug ? logger : undefined))) {
    if (debug)
      logger.debug(`>>> rsync ${label} (dirs): nothing to do`);
    return { ok: true, zero: false };
  }
  if (debug) {
    logger.debug(`>>> rsync ${label} (dirs) (${fromRoot} -> ${toRoot})`);
  }
  const args = [
    ...rsyncArgsDirs(opts),
    `--files-from=${listFile}`,
    ensureTrailingSlash(fromRoot),
    ensureTrailingSlash(toRoot),
  ];
  const res = await run("rsync", args, [0, 23, 24], opts);
  if (debug) {
    logger.debug(`>>> rsync ${label} (dirs): done`, {
      code: res.code,
      ok: res.ok,
    });
  }
  return { ok: res.ok, zero: res.zero };
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
  const args = [
    ...rsyncArgsFixMeta(opts),
    `--files-from=${listFile}`,
    ensureTrailingSlash(fromRoot),
    ensureTrailingSlash(toRoot),
  ];
  const res = await run("rsync", args, [0, 23, 24], opts);
  if (debug) {
    logger.debug(`>>> rsync ${label} (meta): done`, {
      code: res.code,
      ok: res.ok,
    });
  }
  return { ok: res.ok, zero: res.zero };
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
  } = {},
): Promise<{ ok: boolean; zero: boolean }> {
  const { logger, debug } = resolveLogContext(opts);
  if (!(await fileNonEmpty(listFile, debug ? logger : undefined))) {
    if (debug)
      logger.debug(`>>> rsync ${label} (meta dirs): nothing to do`);
    return { ok: true, zero: false };
  }
  if (debug) {
    logger.debug(`>>> rsync ${label} (meta dirs) (${fromRoot} -> ${toRoot})`);
  }
  const args = [
    ...rsyncArgsFixMetaDirs(opts),
    `--files-from=${listFile}`,
    ensureTrailingSlash(fromRoot),
    ensureTrailingSlash(toRoot),
  ];
  const res = await run("rsync", args, [0, 23, 24], opts);
  if (debug) {
    logger.debug(`>>> rsync ${label} (meta dirs): done`, {
      code: res.code,
      ok: res.ok,
    });
  }
  return { ok: res.ok, zero: res.zero };
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
    const args = [
      ...rsyncArgsDelete(opts), // includes --delete-missing-args --force
      `--files-from=${listFile}`,
      sourceRoot,
      ensureTrailingSlash(toRoot),
    ];
    await run("rsync", args, [0, 24], opts);
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
      },
    );
  }
}
