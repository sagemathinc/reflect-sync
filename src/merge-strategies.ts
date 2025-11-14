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
  a_hash_pending?: number | null;
  a_copy_pending?: number | null;
  a_ctime?: number | null;
  a_change_start?: number | null;
  a_change_end?: number | null;
  a_mtime?: number | null;
  a_updated?: number | null;
  a_size?: number | null;
  a_deleted?: number | null;
  a_error?: string | null;
  b_kind?: string | null;
  b_hash?: string | null;
  b_hash_pending?: number | null;
  b_copy_pending?: number | null;
  b_ctime?: number | null;
  b_change_start?: number | null;
  b_change_end?: number | null;
  b_mtime?: number | null;
  b_updated?: number | null;
  b_size?: number | null;
  b_deleted?: number | null;
  b_error?: string | null;
  base_kind?: string | null;
  base_hash?: string | null;
  base_hash_pending?: number | null;
  base_copy_pending?: number | null;
  base_ctime?: number | null;
  base_change_start?: number | null;
  base_change_end?: number | null;
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
  "lww-updated", // not sure we should keep this
  "mirror-to-beta",
  "mirror-to-alpha",
  "prefer",
] as const;

// set to true to make it so when there is a merge conflict
// and both files exist but one is deleted, then the deleted
// one is the winner. Just an experiment...
const LWW_DELETE_WINS = false;

export function resolveMergeStrategy(name?: string | null): MergeStrategy {
  switch (name?.trim().toLowerCase()) {
    case "last-write-wins":
      return (rows, ctx) => planLww(rows, "mtime", ctx.prefer);
    case "mirror-to-alpha":
      return (rows) => planMirror(rows, "alpha");
    case "mirror-to-beta":
      return (rows) => planMirror(rows, "beta");
    case "prefer":
      return (rows, ctx) =>
        planMirror(rows, ctx.prefer === "alpha" ? "beta" : "alpha");
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
  pending: boolean;
  start: number | null;
  end: number | null;
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
    if (alpha.pending || beta.pending) {
      operations.push({ op: "noop", path: row.path });
      continue;
    }

    const alphaCand = toCandidate(alpha, "alpha");
    const betaCand = toCandidate(beta, "beta");

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

    const alphaMatchesBase = matchesBase(row, "a");
    const betaMatchesBase = matchesBase(row, "b");
    if (alphaMatchesBase && !betaMatchesBase && betaCand) {
      operations.push(candidateToOperation(betaCand, row.path));
      continue;
    }
    if (betaMatchesBase && !alphaMatchesBase && alphaCand) {
      operations.push(candidateToOperation(alphaCand, row.path));
      continue;
    }

    let intervalWinner: MergeSide | null = null;
    if (alphaCand && betaCand) {
      intervalWinner = pickIntervalWinner(alpha, beta);
      if (intervalWinner === "alpha") {
        operations.push(candidateToOperation(alphaCand, row.path));
        continue;
      }
      if (intervalWinner === "beta") {
        operations.push(candidateToOperation(betaCand, row.path));
        continue;
      }
    }

    if (alpha.exists && beta.exists) {
      const overlapTieBreak =
        intervalWinner === null &&
        alpha.start != null &&
        alpha.end != null &&
        beta.start != null &&
        beta.end != null;
      if (overlapTieBreak) {
        if (LWW_DELETE_WINS) {
          if (alphaCand && alpha.deleted && !beta.deleted) {
            operations.push(candidateToOperation(alphaCand, row.path));
            continue;
          }
          if (betaCand && !alpha.deleted && beta.deleted) {
            operations.push(candidateToOperation(betaCand, row.path));
            continue;
          }
        }
        const mtimeWinner = pickByTimestamp(row, "mtime");
        if (mtimeWinner) {
          pushCopyOperation(operations, mtimeWinner, row.path);
          continue;
        }
        const hashWinner = pickByBaseHash(row);
        if (hashWinner) {
          pushCopyOperation(operations, hashWinner, row.path);
          continue;
        }
        const ctimeWinner = pickByTimestamp(row, "ctime");
        if (ctimeWinner) {
          pushCopyOperation(operations, ctimeWinner, row.path);
          continue;
        }
      }
    }

    const winner = pickWinner(alphaCand, betaCand, prefer);
    if (!winner) continue;

    operations.push(candidateToOperation(winner, row.path));
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
  const hashPending = Boolean((row as any)[`${prefix}_hash_pending`]);
  const copyPending = Boolean((row as any)[`${prefix}_copy_pending`]);
  const pending = hashPending || copyPending;
  const start = (row as any)[`${prefix}_change_start`] ?? null;
  const end = (row as any)[`${prefix}_change_end`] ?? null;
  // Default vector: prioritise mtime, then ctime.
  const ts: TimestampVector =
    mode === "mtime" ? [mtime, ctime, updated] : [updated, mtime, ctime];
  return {
    exists,
    deleted,
    hash,
    kind,
    ts,
    pending,
    start: typeof start === "number" ? start : null,
    end: typeof end === "number" ? end : null,
  };
}

function toCandidate(state: SideState, side: MergeSide): Candidate | null {
  if (state.pending) return null;
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

function pickIntervalWinner(
  alpha: SideState,
  beta: SideState,
): MergeSide | null {
  if (
    alpha.start == null ||
    alpha.end == null ||
    beta.start == null ||
    beta.end == null
  ) {
    return null;
  }
  if (alpha.end <= beta.start) {
    return "beta";
  }
  if (beta.end <= alpha.start) {
    return "alpha";
  }
  return null;
}

function pickByTimestamp(
  row: MergeDiffRow,
  field: "mtime" | "ctime",
): MergeSide | null {
  const alphaVal = readNumericField(row, `a_${field}`);
  const betaVal = readNumericField(row, `b_${field}`);
  if (alphaVal == null || betaVal == null) return null;
  if (alphaVal > betaVal) return "alpha";
  if (betaVal > alphaVal) return "beta";
  return null;
}

function pickByBaseHash(row: MergeDiffRow): MergeSide | null {
  if (row.base_deleted) return null;
  const baseHash = row.base_hash ?? null;
  if (!baseHash) return null;
  const alphaHash = row.a_hash ?? null;
  const betaHash = row.b_hash ?? null;
  const alphaDiffers = alphaHash != null && alphaHash !== baseHash;
  const betaDiffers = betaHash != null && betaHash !== baseHash;
  if (alphaDiffers === betaDiffers) return null;
  return alphaDiffers ? "alpha" : "beta";
}

function readNumericField(
  row: MergeDiffRow,
  key: keyof MergeDiffRow,
): number | null {
  const value = row[key];
  if (typeof value !== "number") return null;
  if (!Number.isFinite(value)) return null;
  return value;
}

function pushCopyOperation(
  ops: PlannedOperation[],
  from: MergeSide,
  path: string,
) {
  ops.push({
    op: "copy",
    from,
    to: from === "alpha" ? "beta" : "alpha",
    path,
  });
}

function candidateToOperation(
  candidate: Candidate,
  path: string,
): PlannedOperation {
  if (candidate.type === "present") {
    const from = candidate.side;
    const to = from === "alpha" ? "beta" : "alpha";
    return { op: "copy", from, to, path };
  }
  const target = candidate.side === "alpha" ? "beta" : "alpha";
  return { op: "delete", side: target, path };
}

function matchesBase(row: MergeDiffRow, prefix: "a" | "b"): boolean {
  const baseKind = row.base_kind ?? null;
  const baseHash = row.base_hash ?? null;
  const baseSize = row.base_size ?? null;
  const baseDeleted = !!row.base_deleted;
  const sideDeleted = !!(row as any)[`${prefix}_deleted`];
  const sideHash = (row as any)[`${prefix}_hash`] ?? null;
  const sideKind = (row as any)[`${prefix}_kind`] ?? null;
  const sideSize = (row as any)[`${prefix}_size`] ?? null;

  if (baseDeleted || sideDeleted) {
    return baseDeleted === sideDeleted;
  }

  if (baseHash && sideHash) {
    return baseHash === sideHash;
  }
  if (baseKind !== sideKind) return false;
  if (baseSize != null && sideSize != null) {
    return baseSize === sideSize;
  }
  return baseKind === sideKind;
}
