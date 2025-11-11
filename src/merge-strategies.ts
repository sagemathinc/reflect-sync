export type MergeSide = "alpha" | "beta";

export type MergeDiffRow = {
  path: string;
  a_kind?: string | null;
  a_hash?: string | null;
  a_mtime?: number | null;
  a_updated?: number | null;
  a_size?: number | null;
  a_deleted?: number | null;
  a_error?: string | null;
  b_kind?: string | null;
  b_hash?: string | null;
  b_mtime?: number | null;
  b_updated?: number | null;
  b_size?: number | null;
  b_deleted?: number | null;
  b_error?: string | null;
  base_kind?: string | null;
  base_hash?: string | null;
  base_mtime?: number | null;
  base_updated?: number | null;
  base_size?: number | null;
  base_deleted?: number | null;
  base_error?: string | null;
};

export type PlannedOperation =
  | { op: "copy"; from: MergeSide; to: MergeSide; path: string }
  | { op: "delete"; side: MergeSide; path: string }
  | { op: "noop"; path: string };

export interface MergeStrategyContext {
  prefer: MergeSide;
}

export type MergeStrategy = (
  rows: MergeDiffRow[],
  ctx: MergeStrategyContext,
) => PlannedOperation[];

export function resolveMergeStrategy(name?: string | null): MergeStrategy {
  switch (name?.trim().toLowerCase()) {
    case "prefer-beta":
      return (rows) => planPreferSide(rows, "beta");
    case "prefer-alpha":
    case "lww-mtime":
    case "lww-updated":
      // TODO: implement these semantics properly
      return (rows, ctx) => planPreferSide(rows, ctx.prefer);
    default:
      return (rows, ctx) => planPreferSide(rows, ctx.prefer);
  }
}

function planPreferSide(rows: MergeDiffRow[], prefer: MergeSide) {
  const operations: PlannedOperation[] = [];
  for (const row of rows) {
    const aDeleted = !!row.a_deleted;
    const bDeleted = !!row.b_deleted;
    const baseDeleted = !!row.base_deleted;
    const aActive = !aDeleted && !!row.a_hash;
    const bActive = !bDeleted && !!row.b_hash;

    if (aActive && !bActive) {
      if (prefer === "alpha") {
        operations.push({
          op: "copy",
          from: "alpha",
          to: "beta",
          path: row.path,
        });
      } else {
        operations.push({ op: "delete", side: "alpha", path: row.path });
      }
      continue;
    }
    if (bActive && !aActive) {
      if (prefer === "beta") {
        operations.push({
          op: "copy",
          from: "beta",
          to: "alpha",
          path: row.path,
        });
      } else {
        operations.push({ op: "delete", side: "beta", path: row.path });
      }
      continue;
    }
    if (!aActive && !bActive) {
      if (!baseDeleted) {
        operations.push({ op: "delete", side: "alpha", path: row.path });
        operations.push({ op: "delete", side: "beta", path: row.path });
      }
      continue;
    }
    const sameHash = row.a_hash && row.a_hash === row.b_hash;
    if (sameHash) {
      operations.push({ op: "noop", path: row.path });
      continue;
    }
    const from = prefer === "alpha" ? "alpha" : "beta";
    const to = from === "alpha" ? "beta" : "alpha";
    operations.push({ op: "copy", from, to, path: row.path });
  }
  return operations;
}
