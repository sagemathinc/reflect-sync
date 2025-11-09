// src/session-diff.ts

import path from "node:path";
import { getDb } from "./db.js";
import { deriveSessionPaths, type SessionRow } from "./session-db.js";
import { dedupeRestrictedList } from "./restrict.js";

export type DiffEntryType = "file" | "dir" | "link";

export interface DiffEntry {
  path: string;
  type: DiffEntryType;
  mtime: number | null;
}

export interface SessionDiffOptions {
  limit?: number;
  restrictedPaths?: string[];
  restrictedDirs?: string[];
}

export function diffSession(
  session: SessionRow,
  { limit, restrictedPaths, restrictedDirs }: SessionDiffOptions = {},
): DiffEntry[] {
  const alphaDb = resolveDbPath(session, "alpha");
  const betaDb = resolveDbPath(session, "beta");
  const betaHandle = getDb(betaDb);
  betaHandle.close();
  const db = getDb(alphaDb);
  try {
    db.prepare("ATTACH DATABASE ? AS beta").run(betaDb);
    try {
      const sql = `
        WITH file_union AS (
          SELECT path FROM files
          UNION
          SELECT path FROM beta.files
        ),
        file_diff AS (
          SELECT
            p.path AS path,
            'file' AS kind,
            MAX(COALESCE(a.mtime, b.mtime)) AS mtime
          FROM file_union p
          LEFT JOIN files a ON a.path = p.path
          LEFT JOIN beta.files b ON b.path = p.path
          WHERE
            COALESCE(a.deleted, 1) != COALESCE(b.deleted, 1)
            OR (
              COALESCE(a.deleted, 1) = 0
              AND COALESCE(b.deleted, 1) = 0
              AND (
                COALESCE(a.hash, '') != COALESCE(b.hash, '')
                OR COALESCE(a.size, 0) != COALESCE(b.size, 0)
              )
            )
          GROUP BY p.path
        ),
        dir_union AS (
          SELECT path FROM dirs
          UNION
          SELECT path FROM beta.dirs
        ),
        dir_diff AS (
          SELECT
            p.path AS path,
            MAX(COALESCE(a.mtime, b.mtime)) AS mtime
          FROM dir_union p
          LEFT JOIN dirs a ON a.path = p.path
          LEFT JOIN beta.dirs b ON b.path = p.path
          WHERE
            COALESCE(a.deleted, 1) != COALESCE(b.deleted, 1)
            OR (
              COALESCE(a.deleted, 1) = 0
              AND COALESCE(b.deleted, 1) = 0
              AND COALESCE(a.hash, '') != COALESCE(b.hash, '')
            )
          GROUP BY p.path
        ),
        link_union AS (
          SELECT path FROM links
          UNION
          SELECT path FROM beta.links
        ),
        link_diff AS (
          SELECT
            p.path AS path,
            MAX(COALESCE(a.mtime, b.mtime)) AS mtime
          FROM link_union p
          LEFT JOIN links a ON a.path = p.path
          LEFT JOIN beta.links b ON b.path = p.path
          WHERE
            COALESCE(a.deleted, 1) != COALESCE(b.deleted, 1)
            OR (
              COALESCE(a.deleted, 1) = 0
              AND COALESCE(b.deleted, 1) = 0
              AND (
                COALESCE(a.target, '') != COALESCE(b.target, '')
                OR COALESCE(a.hash, '') != COALESCE(b.hash, '')
              )
            )
          GROUP BY p.path
        )
        SELECT path, kind, mtime
        FROM (
          SELECT path, kind, mtime FROM file_diff
          UNION ALL
          SELECT path, 'dir' AS kind, mtime FROM dir_diff
          UNION ALL
          SELECT path, 'link' AS kind, mtime FROM link_diff
        )
        ORDER BY mtime desc
        ${limit ? "LIMIT ?" : ""}
      `;
      const r = db.prepare(sql);
      const rows = (limit ? r.all(Math.max(1, limit)) : r.all()) as {
        path: string | null;
        kind: DiffEntryType;
        mtime: number | null;
      }[];
      const allowed = buildRestrictionFilter(restrictedPaths, restrictedDirs);
      return rows
        .filter((row) => typeof row.path === "string")
        .map((row) => ({
          path: normalizePath(row.path as string),
          type: row.kind,
          mtime: typeof row.mtime === "number" ? row.mtime : null,
        }))
        .filter((entry) => allowed(entry.path));
    } finally {
      db.exec("DETACH DATABASE beta");
    }
  } finally {
    db.close();
  }
}

function buildRestrictionFilter(
  restrictedPaths?: string[],
  restrictedDirs?: string[],
) {
  const pathList = dedupeRestrictedList(restrictedPaths);
  const dirList = dedupeRestrictedList(restrictedDirs);
  const hasRestrictions = pathList.length > 0 || dirList.length > 0;
  const pathSet = new Set(pathList);
  const dirSet = new Set(dirList);
  const dirPrefixes = dirList.filter(Boolean).map((dir) => `${dir}/`);

  if (!hasRestrictions) {
    return () => true;
  }

  return (relPath: string) => {
    if (pathSet.has(relPath)) return true;
    if (dirSet.has(relPath)) return true;
    for (const prefix of dirPrefixes) {
      if (relPath.startsWith(prefix)) return true;
    }
    return false;
  };
}

function resolveDbPath(session: SessionRow, side: "alpha" | "beta"): string {
  const explicit =
    side === "alpha" ? session.alpha_db?.trim() : session.beta_db?.trim();
  if (explicit) {
    return path.resolve(explicit);
  }
  const derived = deriveSessionPaths(session.id);
  return side === "alpha" ? derived.alpha_db : derived.beta_db;
}

function normalizePath(p: string): string {
  return p.replace(/\\/g, "/");
}
