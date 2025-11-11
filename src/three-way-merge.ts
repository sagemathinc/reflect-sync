import { getDb } from "./db.js";
import type { Logger, LogLevel } from "./logger.js";
import { tmpdir } from "node:os";
import { mkdtemp, rm } from "node:fs/promises";
import path from "node:path";
import {
  rsyncCopyChunked,
  rsyncCopyDirsChunked,
  rsyncDeleteChunked,
  ensureTempDir,
} from "./rsync.js";
import type { RsyncCompressSpec } from "./rsync-compression.js";
import {
  resolveMergeStrategy,
  type MergeDiffRow,
  type PlannedOperation,
  type MergeStrategyContext,
  type MergeSide,
} from "./merge-strategies.js";

export type ThreeWayMergeOptions = {
  alphaDb: string;
  betaDb: string;
  baseDb: string;
  prefer: "alpha" | "beta";
  strategyName?: string | null;
  logger?: Logger;
};

export type ThreeWayMergeResult = {
  diffs: MergeDiffRow[];
  operations: PlannedOperation[];
};

export type ExecuteThreeWayMergeOptions = ThreeWayMergeOptions & {
  alphaRoot: string;
  betaRoot: string;
  alphaHost?: string;
  alphaPort?: number;
  betaHost?: string;
  betaPort?: number;
  dryRun?: boolean | string;
  verbose?: boolean | string;
  logLevel?: LogLevel;
  compress?: RsyncCompressSpec;
};

export type ExecuteThreeWayMergeResult = {
  plan: ThreeWayMergeResult;
  ok: boolean;
};

export function planThreeWayMerge(
  opts: ThreeWayMergeOptions,
): ThreeWayMergeResult {
  const {
    alphaDb,
    betaDb,
    baseDb,
    prefer,
    strategyName,
    logger,
  } = opts;

  const db = getDb(baseDb);
  try {
    attachDb(db, "alpha", alphaDb);
    attachDb(db, "beta", betaDb);

    const rows = queryDiffs(db);
    const strategy = resolveMergeStrategy(strategyName);
    const ctx: MergeStrategyContext = { prefer };
    const operations = strategy(rows, ctx);
    logger?.debug("three-way merge plan", {
      diffs: rows.length,
      operations: operations.length,
      strategy: strategyName ?? "default",
    });
    return { diffs: rows, operations };
  } finally {
    try {
      db.exec("DETACH DATABASE alpha");
    } catch {}
    try {
      db.exec("DETACH DATABASE beta");
    } catch {}
    db.close();
  }
}

export async function executeThreeWayMerge(
  opts: ExecuteThreeWayMergeOptions,
): Promise<ExecuteThreeWayMergeResult> {
  const plan = planThreeWayMerge(opts);
  const { logger } = opts;
  if (!plan.operations.length) {
    logger?.debug("node-merge: no operations to perform");
    return { plan, ok: true };
  }

  const tmpWork = await mkdtemp(path.join(tmpdir(), "node-merge-"));
  let alphaTempArg: string | undefined;
  let betaTempArg: string | undefined;
  if (opts.alphaHost) {
    alphaTempArg = ".reflect-rsync-tmp";
  } else {
    alphaTempArg = await ensureTempDir(opts.alphaRoot);
  }
  if (opts.betaHost) {
    betaTempArg = ".reflect-rsync-tmp";
  } else {
    betaTempArg = await ensureTempDir(opts.betaRoot);
  }

  const alphaSpec = opts.alphaHost
    ? `${opts.alphaHost}:${opts.alphaRoot}`
    : opts.alphaRoot;
  const betaSpec = opts.betaHost
    ? `${opts.betaHost}:${opts.betaRoot}`
    : opts.betaRoot;
  const sshPort = opts.alphaHost ? opts.alphaPort : opts.betaPort;
  const rsyncBase = {
    dryRun: opts.dryRun,
    verbose: opts.verbose,
    logger,
    logLevel: opts.logLevel,
    sshPort,
    compress: opts.compress,
  } as const;

  const alphaConn = getDb(opts.alphaDb);
  const betaConn = getDb(opts.betaDb);
  const baseConn = getDb(opts.baseDb);

  try {
    const buckets = categorizeOperations(plan);

    await performDeletes({
      paths: buckets.deleteAlpha,
      fromRoot: betaSpec,
      toRoot: alphaSpec,
      workDir: tmpWork,
      tempDir: alphaTempArg,
      direction: "beta->alpha",
      label: "alpha deletes",
      rsyncOpts: rsyncBase,
      targetDb: alphaConn,
      baseDb: baseConn,
    });
    await performDeletes({
      paths: buckets.deleteBeta,
      fromRoot: alphaSpec,
      toRoot: betaSpec,
      workDir: tmpWork,
      tempDir: betaTempArg,
      direction: "alpha->beta",
      label: "beta deletes",
      rsyncOpts: rsyncBase,
      targetDb: betaConn,
      baseDb: baseConn,
    });

    await performDirCopies({
      paths: buckets.copyAlphaBetaDirs,
      workDir: tmpWork,
      fromRoot: alphaSpec,
      toRoot: betaSpec,
      direction: "alpha->beta",
      tempDir: betaTempArg,
      rsyncOpts: rsyncBase,
      sourceDb: alphaConn,
      destDb: betaConn,
      baseDb: baseConn,
    });
    await performDirCopies({
      paths: buckets.copyBetaAlphaDirs,
      workDir: tmpWork,
      fromRoot: betaSpec,
      toRoot: alphaSpec,
      direction: "beta->alpha",
      tempDir: alphaTempArg,
      rsyncOpts: rsyncBase,
      sourceDb: betaConn,
      destDb: alphaConn,
      baseDb: baseConn,
    });

    await performFileCopies({
      paths: buckets.copyAlphaBetaFiles,
      workDir: tmpWork,
      fromRoot: alphaSpec,
      toRoot: betaSpec,
      direction: "alpha->beta",
      tempDir: betaTempArg,
      rsyncOpts: rsyncBase,
      sourceDb: alphaConn,
      destDb: betaConn,
      baseDb: baseConn,
      sourceSide: "alpha",
      logger,
    });
    await performFileCopies({
      paths: buckets.copyBetaAlphaFiles,
      workDir: tmpWork,
      fromRoot: betaSpec,
      toRoot: alphaSpec,
      direction: "beta->alpha",
      tempDir: alphaTempArg,
      rsyncOpts: rsyncBase,
      sourceDb: betaConn,
      destDb: alphaConn,
      baseDb: baseConn,
      sourceSide: "beta",
      logger,
    });

    return { plan, ok: true };
  } finally {
    await rm(tmpWork, { recursive: true, force: true });
    alphaConn.close();
    betaConn.close();
    baseConn.close();
  }
}

function attachDb(db: ReturnType<typeof getDb>, alias: string, file: string) {
  const escaped = file.replace(/'/g, "''");
  db.exec(`ATTACH DATABASE '${escaped}' AS ${alias}`);
}

function queryDiffs(
  db: ReturnType<typeof getDb>,
): MergeDiffRow[] {
  const sql = `
WITH
pairs AS (
  SELECT
    a.path,
    a.kind    AS a_kind,
    a.hash    AS a_hash,
    a.mtime   AS a_mtime,
    a.updated AS a_updated,
    a.size    AS a_size,
    a.deleted AS a_deleted,
    a.last_error AS a_error,
    b.kind    AS b_kind,
    b.hash    AS b_hash,
    b.mtime   AS b_mtime,
    b.updated AS b_updated,
    b.size    AS b_size,
    b.deleted AS b_deleted,
    b.last_error AS b_error
  FROM alpha.nodes a
  LEFT JOIN beta.nodes b ON b.path = a.path
  WHERE (b.path IS NULL
     OR a.kind    <> b.kind
     OR a.hash    <> b.hash
     OR a.deleted <> b.deleted)
),

beta_only AS (
  SELECT
    b.path,
    NULL AS a_kind,
    NULL AS a_hash,
    NULL AS a_mtime,
    NULL AS a_updated,
    NULL AS a_size,
    1    AS a_deleted,
    NULL AS a_error,
    b.kind    AS b_kind,
    b.hash    AS b_hash,
    b.mtime   AS b_mtime,
    b.updated AS b_updated,
    b.size    AS b_size,
    b.deleted AS b_deleted,
    b.last_error AS b_error
  FROM beta.nodes b
  LEFT JOIN alpha.nodes a ON a.path = b.path
  WHERE a.path IS NULL
),

diff AS (
  SELECT * FROM pairs
  UNION ALL
  SELECT * FROM beta_only
)

SELECT
  diff.path,
  diff.a_kind,
  diff.a_hash,
  diff.a_mtime,
  diff.a_updated,
  diff.a_size,
  diff.a_deleted,
  diff.a_error,
  diff.b_kind,
  diff.b_hash,
  diff.b_mtime,
  diff.b_updated,
  diff.b_size,
  diff.b_deleted,
  diff.b_error,
  base.kind       AS base_kind,
  base.hash       AS base_hash,
  base.mtime      AS base_mtime,
  base.updated    AS base_updated,
  base.size       AS base_size,
  base.deleted    AS base_deleted,
  base.last_error AS base_error
FROM diff
LEFT JOIN nodes AS base ON base.path = diff.path
ORDER BY diff.path;
`;

  const stmt = db.prepare(sql);
  const results = stmt.all() as MergeDiffRow[];
  return results;
}

type OperationBuckets = {
  copyAlphaBetaFiles: string[];
  copyAlphaBetaDirs: string[];
  copyBetaAlphaFiles: string[];
  copyBetaAlphaDirs: string[];
  deleteAlpha: string[];
  deleteBeta: string[];
};

function categorizeOperations(plan: ThreeWayMergeResult): OperationBuckets {
  const diffMap = new Map(plan.diffs.map((row) => [row.path, row]));
  const buckets = {
    copyAlphaBetaFiles: new Set<string>(),
    copyAlphaBetaDirs: new Set<string>(),
    copyBetaAlphaFiles: new Set<string>(),
    copyBetaAlphaDirs: new Set<string>(),
    deleteAlpha: new Set<string>(),
    deleteBeta: new Set<string>(),
  } as const;

  for (const op of plan.operations) {
    if (op.op === "copy") {
      const row = diffMap.get(op.path);
      const kind = row
        ? op.from === "alpha"
          ? row.a_kind
          : row.b_kind
        : null;
      if (op.from === "alpha") {
        if (kind === "d") buckets.copyAlphaBetaDirs.add(op.path);
        else buckets.copyAlphaBetaFiles.add(op.path);
      } else {
        if (kind === "d") buckets.copyBetaAlphaDirs.add(op.path);
        else buckets.copyBetaAlphaFiles.add(op.path);
      }
    } else if (op.op === "delete") {
      if (op.side === "alpha") buckets.deleteAlpha.add(op.path);
      else buckets.deleteBeta.add(op.path);
    }
  }

  return {
    copyAlphaBetaFiles: Array.from(buckets.copyAlphaBetaFiles),
    copyAlphaBetaDirs: Array.from(buckets.copyAlphaBetaDirs),
    copyBetaAlphaFiles: Array.from(buckets.copyBetaAlphaFiles),
    copyBetaAlphaDirs: Array.from(buckets.copyBetaAlphaDirs),
    deleteAlpha: Array.from(buckets.deleteAlpha),
    deleteBeta: Array.from(buckets.deleteBeta),
  };
}

type RsyncBaseOptions = {
  dryRun?: boolean | string;
  verbose?: boolean | string;
  logger?: Logger;
  logLevel?: LogLevel;
  sshPort?: number;
  compress?: RsyncCompressSpec;
};

async function performDeletes(params: {
  paths: string[];
  fromRoot: string;
  toRoot: string;
  workDir: string;
  tempDir?: string;
  direction: "alpha->beta" | "beta->alpha";
  label: string;
  rsyncOpts: RsyncBaseOptions;
  targetDb: ReturnType<typeof getDb>;
  baseDb: ReturnType<typeof getDb>;
}) {
  const unique = uniquePaths(params.paths);
  if (!unique.length) return;
  const res = await rsyncDeleteChunked(
    params.workDir,
    params.fromRoot,
    params.toRoot,
    unique,
    params.label,
    {
      ...params.rsyncOpts,
      direction: params.direction,
      tempDir: params.tempDir,
      forceEmptySource: true,
      captureDeletes: true,
    },
  );
  const deleted = res.deleted?.length ? res.deleted : unique;
  for (const path of deleted) {
    markNodeDeleted(params.targetDb, path);
    markNodeDeleted(params.baseDb, path);
  }
}

async function performDirCopies(params: {
  paths: string[];
  workDir: string;
  fromRoot: string;
  toRoot: string;
  direction: "alpha->beta" | "beta->alpha";
  tempDir?: string;
  rsyncOpts: RsyncBaseOptions;
  sourceDb: ReturnType<typeof getDb>;
  destDb: ReturnType<typeof getDb>;
  baseDb: ReturnType<typeof getDb>;
}) {
  const unique = uniquePaths(params.paths);
  if (!unique.length) return;
  try {
    await rsyncCopyDirsChunked(
      params.workDir,
      params.fromRoot,
      params.toRoot,
      unique,
      `${params.direction} dirs`,
      {
        ...params.rsyncOpts,
        direction: params.direction,
        tempDir: params.tempDir,
      },
    );
    for (const path of unique) {
      mirrorNodeFromSource(
        params.sourceDb,
        params.destDb,
        params.baseDb,
        path,
      );
    }
  } catch (err) {
    const message = `rsync dir copy failed: ${err instanceof Error ? err.message : String(err)}`;
    for (const path of unique) {
      recordNodeError(params.destDb, path, message);
      recordNodeError(params.baseDb, path, message);
    }
    throw err;
  }
}

async function performFileCopies(params: {
  paths: string[];
  workDir: string;
  fromRoot: string;
  toRoot: string;
  direction: "alpha->beta" | "beta->alpha";
  tempDir?: string;
  rsyncOpts: RsyncBaseOptions;
  sourceDb: ReturnType<typeof getDb>;
  destDb: ReturnType<typeof getDb>;
  baseDb: ReturnType<typeof getDb>;
  sourceSide: MergeSide;
  logger?: Logger;
}) {
  const unique = uniquePaths(params.paths);
  if (!unique.length) return;
  const failed = new Set<string>();
  const onChunkResult = async (
    chunk: string[],
    result: { ok: boolean; code: number | null; stderr?: string },
  ) => {
    if (!result.ok && result.code !== null && result.code !== 0) {
      if (result.code === 23 && isVanishedWarning(result.stderr)) return;
      chunk.forEach((p) => failed.add(p));
    }
  };
  try {
    await rsyncCopyChunked(
      params.workDir,
      params.fromRoot,
      params.toRoot,
      unique,
      `${params.direction} files`,
      {
        ...params.rsyncOpts,
        direction: params.direction,
        tempDir: params.tempDir,
        captureTransfers: true,
        onChunkResult,
      },
    );
  } catch (err) {
    unique.forEach((p) => failed.add(p));
    const message = `rsync file copy failed: ${err instanceof Error ? err.message : String(err)}`;
    for (const path of failed) {
      recordNodeError(params.destDb, path, message);
      recordNodeError(params.baseDb, path, message);
    }
    throw err;
  }

  const succeeded = failed.size
    ? unique.filter((p) => !failed.has(p))
    : unique;
  for (const path of succeeded) {
    mirrorNodeFromSource(
      params.sourceDb,
      params.destDb,
      params.baseDb,
      path,
    );
  }
  if (failed.size) {
    const message = `rsync reported partial failures for ${params.direction}`;
    for (const path of failed) {
      recordNodeError(params.destDb, path, message);
      recordNodeError(params.baseDb, path, message);
    }
    throw new Error(message);
  }
}

type NodeRecord = {
  path: string;
  kind: string;
  hash: string;
  mtime: number;
  updated: number;
  size: number;
  deleted: number;
  last_error: string | null;
};

function uniquePaths(paths: string[]): string[] {
  return Array.from(new Set(paths));
}

function fetchNode(db: ReturnType<typeof getDb>, path: string): NodeRecord | null {
  return (
    (db
      .prepare(
        `SELECT path, kind, hash, mtime, updated, size, deleted, last_error FROM nodes WHERE path = ?`,
      )
      .get(path) as NodeRecord | undefined) ?? null
  );
}

function upsertNode(db: ReturnType<typeof getDb>, row: NodeRecord): void {
  db.prepare(
    `INSERT INTO nodes(path, kind, hash, mtime, updated, size, deleted, last_error)
     VALUES (@path,@kind,@hash,@mtime,@updated,@size,@deleted,@last_error)
     ON CONFLICT(path) DO UPDATE SET
       kind=excluded.kind,
       hash=excluded.hash,
       mtime=excluded.mtime,
       updated=excluded.updated,
       size=excluded.size,
       deleted=excluded.deleted,
       last_error=excluded.last_error`,
  ).run(row);
}

function mirrorNodeFromSource(
  sourceDb: ReturnType<typeof getDb>,
  destDb: ReturnType<typeof getDb>,
  baseDb: ReturnType<typeof getDb>,
  path: string,
): void {
  const row = fetchNode(sourceDb, path);
  if (!row) return;
  const clean: NodeRecord = { ...row, last_error: null };
  upsertNode(destDb, clean);
  upsertNode(baseDb, clean);
}

function markNodeDeleted(db: ReturnType<typeof getDb>, path: string) {
  const existing = fetchNode(db, path);
  const now = Date.now();
  const row: NodeRecord = existing
    ? { ...existing, deleted: 1, updated: now, last_error: null }
    : {
        path,
        kind: "f",
        hash: "",
        mtime: now,
        updated: now,
        size: 0,
        deleted: 1,
        last_error: null,
      };
  upsertNode(db, row);
}

function recordNodeError(
  db: ReturnType<typeof getDb>,
  path: string,
  message: string,
) {
  const existing = fetchNode(db, path);
  const now = Date.now();
  const row: NodeRecord = existing
    ? { ...existing }
    : {
        path,
        kind: "f",
        hash: "",
        mtime: now,
        updated: now,
        size: 0,
        deleted: 0,
        last_error: null,
      };
  row.updated = now;
  row.last_error = JSON.stringify({ message, at: now });
  upsertNode(db, row);
}

function isVanishedWarning(stderr?: string | null): boolean {
  if (!stderr) return false;
  return stderr.toLowerCase().includes("vanished");
}
