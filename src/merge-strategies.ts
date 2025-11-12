/**
 * Merge strategies operate on rows from the node tables (alpha/beta/base).
 *
 * Each row reflects our latest knowledge of that path after scan/watch/ingest:
 *
 * - `kind/hash/size` describe the current object when `deleted = 0`.
 * - `mtime` is the operating system stat mtime, except when the file
 *   is deleted, when it is instead the timestamp when we observed the deletion.
 * - `updated` always tracks the last time we touched the row, allowing merge
 *    strategies to use either `mtime` or `updated` as their comparison clock.
 *    Thus updated is closer to "ctime" in POSIX (though not quite the same).
 *
 * With those guarantees, last-writer-wins strategies can compare timestamps
 * directly without special-casing deletions.
 */
export type MergeSide = "alpha" | "beta";

export type MergeDiffRow = {
  path: string;
  a_kind?: string | null;
  a_hash?: string | null;
  a_ctime?: number | null;
  a_mtime?: number | null;
  a_updated?: number | null;
  a_size?: number | null;
  a_deleted?: number | null;
  a_error?: string | null;
  b_kind?: string | null;
  b_hash?: string | null;
  b_ctime?: number | null;
  b_mtime?: number | null;
  b_updated?: number | null;
  b_size?: number | null;
  b_deleted?: number | null;
  b_error?: string | null;
  base_kind?: string | null;
  base_hash?: string | null;
  base_ctime?: number | null;
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

export const MERGE_STRATEGY_NAMES = [
  "last-write-wins",
  "lww-mtime",
  "lww-updated",
  "mirror-to-beta",
  "mirror-to-alpha",
  "prefer",
] as const;

export function resolveMergeStrategy(name?: string | null): MergeStrategy {
  switch (name?.trim().toLowerCase()) {
    case "last-write-wins":
      return (rows, ctx) => planLww(rows, "updated", ctx.prefer);
    case "mirror-to-alpha":
      return (rows) => planMirror(rows, "alpha");
    case "mirror-to-beta":
      return (rows) => planMirror(rows, "beta");
    case "prefer":
      return (rows, ctx) =>
        planMirror(rows, ctx.prefer === "alpha" ? "beta" : "alpha");
    case "lww-mtime":
      return (rows, ctx) => planLww(rows, "mtime", ctx.prefer);
    case "lww-updated":
      return (rows, ctx) => planLww(rows, "updated", ctx.prefer);
    default:
      return (rows, ctx) => planLww(rows, "updated", ctx.prefer);
  }
}

function planMirror(rows: MergeDiffRow[], target: MergeSide) {
  const operations: PlannedOperation[] = [];
  for (const row of rows) {
    const source = target === "alpha" ? "beta" : "alpha";
    const srcActive = source === "alpha" ? !row.a_deleted : !row.b_deleted;
    const dstActive = target === "alpha" ? !row.a_deleted : !row.b_deleted;

    if (srcActive) {
      operations.push({ op: "copy", from: source, to: target, path: row.path });
    } else if (dstActive) {
      operations.push({ op: "delete", side: target, path: row.path });
    }
  }
  return operations;
}

type TimestampMode = "mtime" | "updated";

type TimestampVector = [number, number, number];

type SideState = {
  exists: boolean;
  deleted: boolean;
  hash?: string | null;
  kind?: string | null;
  ts: TimestampVector;
};

type Candidate = {
  side: MergeSide;
  ts: TimestampVector;
  type: "present" | "deleted";
};

function planLww(
  rows: MergeDiffRow[],
  mode: TimestampMode,
  prefer: MergeSide,
): PlannedOperation[] {
  const operations: PlannedOperation[] = [];
  for (const row of rows) {
    const alpha = extractState(row, "alpha", mode);
    const beta = extractState(row, "beta", mode);

    const bothMissing =
      !alpha.exists && !beta.exists && !alpha.deleted && !beta.deleted;
    if (bothMissing) continue;

    const sameHash =
      alpha.exists &&
      beta.exists &&
      !!alpha.hash &&
      alpha.hash === beta.hash &&
      alpha.kind === beta.kind;
    if (sameHash) {
      operations.push({ op: "noop", path: row.path });
      continue;
    }

    const alphaCand = toCandidate(alpha, "alpha");
    const betaCand = toCandidate(beta, "beta");
    const winner = pickWinner(alphaCand, betaCand, prefer);
    if (!winner) continue;

    if (winner.type === "present") {
      const from = winner.side;
      const to = from === "alpha" ? "beta" : "alpha";
      operations.push({ op: "copy", from, to, path: row.path });
    } else {
      const target = winner.side === "alpha" ? "beta" : "alpha";
      operations.push({ op: "delete", side: target, path: row.path });
    }
  }
  return operations;
}

function extractState(
  row: MergeDiffRow,
  side: MergeSide,
  mode: TimestampMode,
): SideState {
  const prefix = side === "alpha" ? "a" : "b";
  const deleted = !!(row as any)[`${prefix}_deleted`];
  const kind = (row as any)[`${prefix}_kind`] ?? null;
  const hash = (row as any)[`${prefix}_hash`] ?? null;
  const size = (row as any)[`${prefix}_size`];
  const exists = !deleted && (kind != null || hash != null || size != null);
  const mtime = Number((row as any)[`${prefix}_mtime`]) || 0;
  const ctime = Number((row as any)[`${prefix}_ctime`]) || 0;
  const updated = Number((row as any)[`${prefix}_updated`]) || 0;
  // Default vector: updated → mtime → ctime. Legacy lww-mtime still leads with mtime.
  const ts: TimestampVector =
    mode === "mtime"
      ? [mtime, updated, ctime]
      : [updated, mtime, ctime];
  return {
    exists,
    deleted,
    hash,
    kind,
    ts,
  };
}

function toCandidate(state: SideState, side: MergeSide): Candidate | null {
  if (state.exists) {
    return { side, ts: state.ts, type: "present" };
  }
  if (state.deleted) {
    return { side, ts: state.ts, type: "deleted" };
  }
  return null;
}

function pickWinner(
  alpha: Candidate | null,
  beta: Candidate | null,
  prefer: MergeSide,
): Candidate | null {
  if (alpha && beta) {
    const cmp = compareVectors(alpha.ts, beta.ts);
    if (cmp > 0) return alpha;
    if (cmp < 0) return beta;
    return prefer === "alpha" ? alpha : beta;
  }
  return alpha ?? beta ?? null;
}

function compareVectors(a: TimestampVector, b: TimestampVector): number {
  for (let i = 0; i < Math.max(a.length, b.length); i += 1) {
    const av = a[i] ?? 0;
    const bv = b[i] ?? 0;
    if (av > bv) return 1;
    if (av < bv) return -1;
  }
  return 0;
}
