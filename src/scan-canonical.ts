import path from "node:path";
import { lstat as lstatAsync } from "node:fs/promises";
import type { FilesystemCapabilities } from "./fs-capabilities.js";
import { canonicalizePath } from "./fs-capabilities.js";
import type { Logger } from "./logger.js";
import type { Database } from "./db.js";

type CanonicalWinnerState = {
  winner: string | null;
  winnerChecked: boolean;
  winnerMissing: boolean;
};

export type CanonicalTracker = {
  enabled: boolean;
  tracking: boolean;
  canonicalKeyFor(relPath: string): string;
  hasConflictAncestor(relPath: string): boolean;
  markConflictDir(relPath: string): void;
  noteConflict(key: string, variant: string): void;
  classifyEntry(key: string, relPath: string): Promise<number>;
  shouldPreferExisting(key: string, relPath: string): Promise<boolean>;
  flushReports(logger: Logger): void;
};

type CanonicalTrackerOptions = {
  db: Database;
  absRoot: string;
  filesystemCaps: FilesystemCapabilities;
  enableCanonicalization: boolean;
  trackConflicts: boolean;
};

export function createCanonicalTracker(
  opts: CanonicalTrackerOptions,
): CanonicalTracker {
  const {
    db,
    absRoot,
    filesystemCaps,
    enableCanonicalization,
    trackConflicts,
  } = opts;

  const canonicalMap =
    enableCanonicalization && trackConflicts ? new Map<string, string>() : null;
  const conflictDirSet =
    enableCanonicalization && trackConflicts ? new Set<string>() : null;
  const conflictReports =
    enableCanonicalization && trackConflicts
      ? new Map<string, Set<string>>()
      : null;
  const canonicalState =
    enableCanonicalization && trackConflicts
      ? new Map<string, CanonicalWinnerState>()
      : null;

  if (canonicalState && canonicalMap) {
    const canonicalRows = db
      .prepare(
        `SELECT path, canonical_key, case_conflict, deleted FROM nodes WHERE canonical_key IS NOT NULL`,
      )
      .all() as {
      path: string;
      canonical_key: string | null;
      case_conflict: number | null;
      deleted: number | null;
    }[];
    for (const row of canonicalRows) {
      const canonicalKey = row.canonical_key;
      if (!canonicalKey) continue;
      let state = canonicalState.get(canonicalKey);
      if (!state) {
        state = {
          winner: null,
          winnerChecked: true,
          winnerMissing: true,
        };
        canonicalState.set(canonicalKey, state);
      }
      if (row.case_conflict === 0 && row.deleted === 0 && !state.winner) {
        state.winner = row.path;
        state.winnerChecked = false;
        state.winnerMissing = false;
        canonicalMap.set(canonicalKey, row.path);
      }
    }
  }

  const canonicalKeyFor = (relPath: string): string => {
    if (!enableCanonicalization) return relPath;
    return canonicalizePath(relPath, filesystemCaps);
  };

  const hasConflictAncestor = (relPath: string): boolean => {
    if (!conflictDirSet || conflictDirSet.size === 0) {
      return false;
    }
    let current = relPath;
    while (true) {
      const idx = current.lastIndexOf("/");
      if (idx === -1) break;
      current = current.slice(0, idx);
      if (conflictDirSet.has(current)) return true;
    }
    return conflictDirSet.has("");
  };

  const noteConflict = (key: string, variant: string): void => {
    if (!conflictReports || !canonicalMap) return;
    let set = conflictReports.get(key);
    if (!set) {
      set = new Set<string>();
      const winner = canonicalMap.get(key);
      if (winner) set.add(winner);
      conflictReports.set(key, set);
    }
    set.add(variant);
  };

  const markConflictDir = (relPath: string): void => {
    if (!conflictDirSet) return;
    conflictDirSet.add(relPath);
  };

  const classifyEntry = async (
    key: string,
    relPath: string,
  ): Promise<number> => {
    if (!canonicalState || !canonicalMap) return 0;
    let state = canonicalState.get(key);
    if (!state) {
      state = {
        winner: null,
        winnerChecked: true,
        winnerMissing: true,
      };
      canonicalState.set(key, state);
    }
    if (!state.winner) {
      state.winner = relPath;
      state.winnerChecked = true;
      state.winnerMissing = false;
      canonicalMap.set(key, relPath);
      return 0;
    }
    if (state.winner === relPath) {
      state.winnerChecked = true;
      state.winnerMissing = false;
      return 0;
    }
    if (state.winnerMissing) {
      state.winner = relPath;
      state.winnerMissing = false;
      canonicalMap.set(key, relPath);
      return 0;
    }
    if (!state.winnerChecked) {
      state.winnerChecked = true;
      try {
        await lstatAsync(path.join(absRoot, state.winner));
        state.winnerMissing = false;
      } catch {
        state.winnerMissing = true;
      }
      if (state.winnerMissing) {
        state.winner = relPath;
        state.winnerMissing = false;
        canonicalMap.set(key, relPath);
        return 0;
      }
    }
    noteConflict(key, relPath);
    return 1;
  };

  const shouldPreferExisting = async (
    key: string,
    relPath: string,
  ): Promise<boolean> => {
    if (!trackConflicts || !enableCanonicalization) return false;
    if (!key || key === relPath) return false;
    try {
      await lstatAsync(path.join(absRoot, key));
      return true;
    } catch {
      return false;
    }
  };

  const flushReports = (logger: Logger): void => {
    if (!conflictReports || conflictReports.size === 0) return;
    for (const [canonicalName, paths] of conflictReports.entries()) {
      logger.warn(
        "case-insensitive name conflict; only the first observed variant is synchronized",
        {
          canonical: canonicalName,
          paths: Array.from(paths).sort(),
        },
      );
    }
  };

  return {
    enabled: enableCanonicalization,
    tracking: enableCanonicalization && trackConflicts,
    canonicalKeyFor,
    hasConflictAncestor,
    markConflictDir,
    noteConflict,
    classifyEntry,
    shouldPreferExisting,
    flushReports,
  };
}
