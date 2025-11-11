import { getDb } from "./db.js";
import type { Logger } from "./logger.js";
import {
  resolveMergeStrategy,
  type MergeDiffRow,
  type PlannedOperation,
  type MergeStrategyContext,
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
    logger?.debug?.("three-way merge plan", {
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
