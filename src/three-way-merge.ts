import { getDb } from "./db.js";
import type { Logger, LogLevel } from "./logger.js";
import { tmpdir } from "node:os";
import { mkdtemp, rm } from "node:fs/promises";
import path from "node:path";
import {
  rsyncCopyChunked,
  rsyncCopyDirsChunked,
  rsyncDeleteChunked,
  rsyncFixMetaDirsChunked,
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
import { dedupeRestrictedList, dirnameRel } from "./restrict.js";
import {
  TraceWriter,
  describeOperation,
  type TracePlanEntry,
} from "./trace.js";
import { deletionMtimeFromMeta } from "./nodes-util.js";

const VERY_VERBOSE = false;

export type ThreeWayMergeOptions = {
  alphaDb: string;
  betaDb: string;
  baseDb: string;
  prefer: "alpha" | "beta";
  strategyName?: string | null;
  restrictedPaths?: string[];
  logger?: Logger;
  traceLabel?: string;
  traceDbPath?: string;
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
  traceLabel?: string;
  traceDbPath?: string;
};

export type ExecuteThreeWayMergeResult = {
  plan: ThreeWayMergeResult;
  ok: boolean;
};

export function planThreeWayMerge(
  opts: ThreeWayMergeOptions,
): ThreeWayMergeResult {
  const { alphaDb, betaDb, baseDb, prefer, strategyName, logger } = opts;

  const db = getDb(baseDb);
  const restrictedPaths = dedupeRestrictedList(
    opts.restrictedPaths ?? [],
  ).filter(Boolean);
  const restrictionActive = restrictedPaths.length > 0;

  try {
    if (restrictionActive) {
      db.exec("DROP TABLE IF EXISTS __three_way_paths;");
      db.exec(
        "CREATE TEMP TABLE __three_way_paths(path TEXT PRIMARY KEY) WITHOUT ROWID;",
      );
      const insert = db.prepare(
        `INSERT OR IGNORE INTO __three_way_paths(path) VALUES (?)`,
      );
      const txInsert = db.transaction((paths: string[]) => {
        for (const rel of paths) {
          let current = rel;
          while (current && !insert.run(current)) {
            const parent = dirnameRel(current);
            if (!parent || parent === current) break;
            current = parent;
          }
        }
      });
      txInsert(restrictedPaths);
    }

    attachDb(db, "alpha", alphaDb);
    attachDb(db, "beta", betaDb);

    const rows = queryDiffs(db, restrictionActive);
    const strategy = resolveMergeStrategy(strategyName);
    const ctx: MergeStrategyContext = { prefer };
    const operations = strategy(rows, ctx);
    logger?.debug("three-way merge plan", {
      diffs: rows.length,
      operations: operations.length,
      strategy: strategyName ?? "default",
    });
    if (VERY_VERBOSE) {
      logger?.debug(
        "three merge details" +
          JSON.stringify({ rows, operations }, undefined, 2),
      );
    }
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
  const tracer =
    TraceWriter.maybeCreate({
      baseDb: opts.baseDb,
      traceDb: opts.traceDbPath,
      context: {
        label:
          opts.traceLabel ??
          (opts.restrictedPaths && opts.restrictedPaths.length
            ? "restricted"
            : "full"),
        strategy: opts.strategyName ?? "lww-mtime",
        prefer: opts.prefer,
        restrictedCount: opts.restrictedPaths?.length ?? 0,
      },
    }) ?? null;
  if (tracer) {
    const entries = buildTraceEntries(plan.diffs, plan.operations);
    tracer.recordPlan(entries, plan.diffs.length);
  }
  const { logger } = opts;
  if (!plan.operations.length) {
    logger?.debug("node-merge: no operations to perform");
    tracer?.close();
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
      tracer,
      opName: operationName({ op: "delete", side: "alpha" }),
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
      tracer,
      opName: operationName({ op: "delete", side: "beta" }),
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
      tracer,
      opName: operationName({ op: "copy", from: "alpha", to: "beta" }),
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
      tracer,
      opName: operationName({ op: "copy", from: "beta", to: "alpha" }),
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
      tracer,
      opName: operationName({ op: "copy", from: "alpha", to: "beta" }),
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
      tracer,
      opName: operationName({ op: "copy", from: "beta", to: "alpha" }),
    });

    return { plan, ok: true };
  } finally {
    await rm(tmpWork, { recursive: true, force: true });
    alphaConn.close();
    betaConn.close();
    baseConn.close();
    tracer?.close();
  }
}

function attachDb(db: ReturnType<typeof getDb>, alias: string, file: string) {
  const escaped = file.replace(/'/g, "''");
  db.exec(`ATTACH DATABASE '${escaped}' AS ${alias}`);
}

function queryDiffs(
  db: ReturnType<typeof getDb>,
  restricted: boolean,
): MergeDiffRow[] {
  const clausePairs = restricted
    ? " AND EXISTS (SELECT 1 FROM __three_way_paths r WHERE r.path = a.path)"
    : "";
  const clauseBetaOnly = restricted
    ? " AND EXISTS (SELECT 1 FROM __three_way_paths r WHERE r.path = b.path)"
    : "";
  const deletedFilter =
    "NOT (COALESCE(diff.a_deleted, 0) = 1 AND COALESCE(diff.b_deleted, 0) = 1)";
  const finalWhere = restricted
    ? `WHERE diff.path IN (SELECT path FROM __three_way_paths) AND ${deletedFilter}`
    : `WHERE ${deletedFilter}`;

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
     OR a.deleted <> b.deleted)${clausePairs}
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
  WHERE a.path IS NULL${clauseBetaOnly}
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
${finalWhere}
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
      const kind = row ? (op.from === "alpha" ? row.a_kind : row.b_kind) : null;
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
  tracer?: TraceWriter | null;
  opName?: string;
}) {
  const unique = uniquePaths(params.paths);
  if (!unique.length) return;
  let res;
  try {
    res = await rsyncDeleteChunked(
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
  } catch (err) {
    if (params.tracer && params.opName) {
      for (const path of unique) {
        params.tracer.recordResult(path, params.opName, "failure", {
          error: err instanceof Error ? err.message : String(err),
          phase: "delete",
        });
      }
    }
    throw err;
  }
  const deleted = res.deleted?.length ? res.deleted : unique;
  markNodesDeletedBatch(params.targetDb, deleted);
  markNodesDeletedBatch(params.baseDb, deleted);
  if (params.tracer && params.opName) {
    for (const path of deleted) {
      params.tracer.recordResult(path, params.opName, "success");
    }
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
  tracer?: TraceWriter | null;
  opName?: string;
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
    await rsyncFixMetaDirsChunked(
      params.workDir,
      params.fromRoot,
      params.toRoot,
      unique,
      `${params.direction} dirs meta`,
      {
        ...params.rsyncOpts,
        direction: params.direction,
      },
    );
    mirrorNodesFromSourceBatch(
      params.sourceDb,
        params.destDb,
        params.baseDb,
        unique,
      );
      if (params.tracer && params.opName) {
        for (const path of unique) {
          params.tracer.recordResult(path, params.opName, "success");
        }
      }
    } catch (err) {
      const message = `rsync dir copy failed: ${err instanceof Error ? err.message : String(err)}`;
      recordNodeErrorsBatch(params.destDb, unique, message);
      recordNodeErrorsBatch(params.baseDb, unique, message);
      if (params.tracer && params.opName) {
        for (const path of unique) {
          params.tracer.recordResult(path, params.opName, "failure", {
            error: err instanceof Error ? err.message : String(err),
            phase: "dir-copy",
          });
        }
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
  tracer?: TraceWriter | null;
  opName?: string;
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
    const failedList = Array.from(failed);
    recordNodeErrorsBatch(params.destDb, failedList, message);
    recordNodeErrorsBatch(params.baseDb, failedList, message);
    throw err;
  }

  const succeeded = failed.size ? unique.filter((p) => !failed.has(p)) : unique;
  mirrorNodesFromSourceBatch(
    params.sourceDb,
    params.destDb,
    params.baseDb,
    succeeded,
  );
  if (params.tracer && params.opName) {
    for (const path of succeeded) {
      params.tracer.recordResult(path, params.opName, "success");
    }
  }
  if (failed.size) {
    const message = `rsync reported partial failures for ${params.direction}`;
    const failedList = Array.from(failed);
    recordNodeErrorsBatch(params.destDb, failedList, message);
    recordNodeErrorsBatch(params.baseDb, failedList, message);
    if (params.tracer && params.opName) {
      for (const path of failedList) {
        params.tracer.recordResult(path, params.opName, "failure", {
          error: message,
          phase: "file-copy",
        });
      }
    }
    throw new Error(message);
  }
}

type NodeRecord = {
  path: string;
  kind: string;
  hash: string;
  mtime: number;
  ctime: number;
  hashed_ctime: number | null;
  updated: number;
  size: number;
  deleted: number;
  last_seen: number | null;
  link_target: string | null;
  last_error: string | null;
};

function uniquePaths(paths: string[]): string[] {
  return Array.from(new Set(paths));
}

function fetchNode(
  db: ReturnType<typeof getDb>,
  path: string,
): NodeRecord | null {
  return (
    (db
      .prepare(
        `SELECT path, kind, hash, mtime, ctime, hashed_ctime, updated, size, deleted, last_seen, link_target, last_error FROM nodes WHERE path = ?`,
      )
      .get(path) as NodeRecord | undefined) ?? null
  );
}

function upsertNode(db: ReturnType<typeof getDb>, row: NodeRecord): void {
  db.prepare(
    `INSERT INTO nodes(path, kind, hash, mtime, ctime, hashed_ctime, updated, size, deleted, last_seen, link_target, last_error)
     VALUES (@path,@kind,@hash,@mtime,@ctime,@hashed_ctime,@updated,@size,@deleted,@last_seen,@link_target,@last_error)
     ON CONFLICT(path) DO UPDATE SET
       kind=excluded.kind,
       hash=excluded.hash,
       mtime=excluded.mtime,
       ctime=excluded.ctime,
       hashed_ctime=excluded.hashed_ctime,
       updated=excluded.updated,
       size=excluded.size,
       deleted=excluded.deleted,
       last_seen=excluded.last_seen,
       link_target=excluded.link_target,
       last_error=excluded.last_error`,
  ).run(row);
}

function mirrorNodesFromSourceBatch(
  sourceDb: ReturnType<typeof getDb>,
  destDb: ReturnType<typeof getDb>,
  baseDb: ReturnType<typeof getDb>,
  paths: string[],
): void {
  const rows: NodeRecord[] = [];
  for (const path of paths) {
    const row = fetchNode(sourceDb, path);
    if (row) rows.push({ ...row, last_error: null });
  }
  if (!rows.length) return;
  const applyDest = destDb.transaction((entries: NodeRecord[]) => {
    for (const entry of entries) upsertNode(destDb, entry);
  });
  const applyBase = baseDb.transaction((entries: NodeRecord[]) => {
    for (const entry of entries) upsertNode(baseDb, entry);
  });
  applyDest(rows);
  applyBase(rows);
}

function markNodesDeletedBatch(
  db: ReturnType<typeof getDb>,
  paths: string[],
): void {
  if (!paths.length) return;
  const rows: NodeRecord[] = [];
  const now = Date.now();
  for (const path of paths) {
    const existing = fetchNode(db, path);
    const deleteMtime = deletionMtimeFromMeta(
      existing ?? {},
      now,
    );
    const row: NodeRecord = existing
      ? {
          ...existing,
          deleted: 1,
          mtime: deleteMtime,
          hash: "",
          hashed_ctime: null,
          size: 0,
          updated: now,
          last_error: null,
        }
        : {
            path,
            kind: "f",
            hash: "",
            mtime: deleteMtime,
            ctime: deleteMtime,
            hashed_ctime: null,
            updated: now,
            size: 0,
            deleted: 1,
            last_seen: null,
            link_target: null,
            last_error: null,
          };
    rows.push(row);
  }
  const apply = db.transaction((entries: NodeRecord[]) => {
    for (const entry of entries) upsertNode(db, entry);
  });
  apply(rows);
}

function recordNodeErrorsBatch(
  db: ReturnType<typeof getDb>,
  paths: string[],
  message: string,
) {
  if (!paths.length) return;
  const now = Date.now();
  const rows: NodeRecord[] = [];
  for (const path of paths) {
    const existing = fetchNode(db, path);
    const row: NodeRecord = existing
      ? { ...existing }
      : {
          path,
          kind: "f",
          hash: "",
          mtime: now,
          ctime: now,
          hashed_ctime: null,
          updated: now,
          size: 0,
          deleted: 0,
          last_seen: now,
          link_target: null,
          last_error: null,
        };
    row.updated = now;
    row.last_error = JSON.stringify({ message, at: now });
    rows.push(row);
  }
  const apply = db.transaction((entries: NodeRecord[]) => {
    for (const entry of entries) upsertNode(db, entry);
  });
  apply(rows);
}

function operationName(op: { op: "copy" | "delete" | "noop"; from?: string; to?: string; side?: string }) {
  return describeOperation(op);
}

function buildTraceEntries(
  rows: MergeDiffRow[],
  operations: PlannedOperation[],
): TracePlanEntry[] {
  const opsByPath = new Map<string, PlannedOperation[]>();
  for (const op of operations) {
    const arr = opsByPath.get(op.path);
    if (arr) arr.push(op);
    else opsByPath.set(op.path, [op]);
  }
  const entries: TracePlanEntry[] = [];
  for (const row of rows) {
    const alpha = snapshotState(row, "a");
    const beta = snapshotState(row, "b");
    const base = snapshotState(row, "base");
    const ops = opsByPath.get(row.path);
    if (!ops || !ops.length) {
      entries.push({
        path: row.path,
        operation: "noop",
        alpha,
        beta,
        base,
      });
      continue;
    }
    for (const op of ops) {
      entries.push({
        path: row.path,
        operation: describeOperation(op),
        alpha,
        beta,
        base,
      });
    }
  }
  return entries;
}

function snapshotState(
  row: MergeDiffRow,
  prefix: "a" | "b" | "base",
): Record<string, unknown> | null {
  const suffix = (key: string) =>
    prefix === "base" ? `base_${key}` : `${prefix}_${key}`;
  const kind = (row as any)[suffix("kind")] ?? null;
  const hash = (row as any)[suffix("hash")] ?? null;
  const mtime = (row as any)[suffix("mtime")] ?? null;
  const updated = (row as any)[suffix("updated")] ?? null;
  const size = (row as any)[suffix("size")] ?? null;
  const deleted = (row as any)[suffix("deleted")] ?? null;
  const error = (row as any)[suffix("error")] ?? null;
  if (
    kind === null &&
    hash === null &&
    mtime === null &&
    updated === null &&
    size === null &&
    deleted === null &&
    error === null
  ) {
    return null;
  }
  return { kind, hash, mtime, updated, size, deleted, error };
}

function isVanishedWarning(stderr?: string | null): boolean {
  if (!stderr) return false;
  return stderr.toLowerCase().includes("vanished");
}
