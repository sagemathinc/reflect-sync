// src/session-diff.ts

import path from "node:path";
import { deriveSessionPaths, type SessionRow } from "./session-db.js";
import { dedupeRestrictedList } from "./restrict.js";
import { planThreeWayMerge } from "./three-way-merge.js";

export type DiffEntryType = "file" | "dir" | "link";

export interface DiffEntry {
  path: string;
  type: DiffEntryType;
  mtime: number | null;
}

export interface SessionDiffOptions {
  limit?: number;
  restrictedPaths?: string[];
}

export function diffSession(
  session: SessionRow,
  { limit, restrictedPaths }: SessionDiffOptions = {},
): DiffEntry[] {
  const alphaDb = resolveDbPath(session, "alpha");
  const betaDb = resolveDbPath(session, "beta");
  const baseDb = resolveDbPath(session, "base");
  const prefer =
    session.prefer && session.prefer.toLowerCase() === "beta"
      ? "beta"
      : "alpha";
  const plan = planThreeWayMerge({
    alphaDb,
    betaDb,
    baseDb,
    prefer,
  });
  const allowed = buildRestrictionFilter(restrictedPaths);
  const entries = plan.diffs
    .map((row) => {
      if (!row.path) return null;
      const type = resolveKind(row);
      const mtime =
        row.a_mtime ??
        row.b_mtime ??
        row.base_mtime ??
        row.a_updated ??
        row.b_updated ??
        row.base_updated ??
        null;
      return {
        path: normalizePath(row.path),
        type,
        mtime,
      };
    })
    .filter((entry): entry is DiffEntry => !!entry && allowed(entry.path))
    .sort((a, b) => (b.mtime ?? 0) - (a.mtime ?? 0));
  return typeof limit === "number" && limit > 0
    ? entries.slice(0, limit)
    : entries;
}

function resolveKind(row: any): DiffEntryType {
  const kind = row.a_kind ?? row.b_kind ?? row.base_kind ?? "f";
  if (kind === "d") return "dir";
  if (kind === "l") return "link";
  return "file";
}

function buildRestrictionFilter(restrictedPaths?: string[]) {
  const pathList = dedupeRestrictedList(restrictedPaths);
  if (!pathList.length) {
    return () => true;
  }
  const pathSet = new Set(pathList);
  return (relPath: string) => pathSet.has(relPath);
}

function resolveDbPath(
  session: SessionRow,
  side: "alpha" | "beta" | "base",
): string {
  const explicit =
    side === "alpha"
      ? session.alpha_db?.trim()
      : side === "beta"
        ? session.beta_db?.trim()
        : session.base_db?.trim();
  if (explicit) {
    return path.resolve(explicit);
  }
  const derived = deriveSessionPaths(session.id);
  if (side === "alpha") return derived.alpha_db;
  if (side === "beta") return derived.beta_db;
  return derived.base_db;
}

function normalizePath(p: string): string {
  return p.replace(/\\/g, "/");
}
