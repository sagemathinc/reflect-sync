import { getDb } from "./db.js";

export type EntryKind = "file" | "dir" | "link" | "missing";

export interface OpStamp {
  kind: EntryKind;
  opTs: number | null;
  deleted: boolean;
  size?: number | null;
  mtime?: number | null;
  ctime?: number | null;
  hash?: string | null;
}

const SQLITE_MAX_VARIABLE_NUMBER = 999;

function chunkArray<T>(items: T[], size: number): T[][] {
  if (items.length <= size) return [items];
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    out.push(items.slice(i, i + size));
  }
  return out;
}

export function fetchOpStamps(
  dbPath: string,
  paths: string[],
): Map<string, OpStamp> {
  const result = new Map<string, OpStamp>();
  if (!paths.length) return result;
  const db = getDb(dbPath);
  try {
    const batches = chunkArray(paths, SQLITE_MAX_VARIABLE_NUMBER - 1);
    for (const batch of batches) {
      const placeholders = batch.map(() => "?").join(",");

      const filesStmt = db.prepare(
        `SELECT path, op_ts, deleted, size, mtime, ctime, hash FROM files WHERE path IN (${placeholders})`,
      );
      for (const row of filesStmt.iterate(...batch)) {
        const deleted = Boolean(row.deleted);
        const entry = result.get(row.path as string);
        if (!entry || entry.deleted) {
          result.set(row.path as string, {
            kind: deleted ? "missing" : "file",
            opTs: row.op_ts as number | null,
            deleted,
            size: row.size as number | null,
            mtime: row.mtime as number | null,
            ctime: row.ctime as number | null,
            hash: row.hash as string | null,
          });
        }
      }

      const dirsStmt = db.prepare(
        `SELECT path, op_ts, deleted, mtime, ctime, hash FROM dirs WHERE path IN (${placeholders})`,
      );
      for (const row of dirsStmt.iterate(...batch)) {
        const deleted = Boolean(row.deleted);
        const existing = result.get(row.path as string);
        if (!existing || existing.deleted) {
          result.set(row.path as string, {
            kind: deleted ? "missing" : "dir",
            opTs: row.op_ts as number | null,
            deleted,
            mtime: row.mtime as number | null,
            ctime: row.ctime as number | null,
            hash: row.hash as string | null,
          });
        }
      }

      const linksStmt = db.prepare(
        `SELECT path, op_ts, deleted, hash FROM links WHERE path IN (${placeholders})`,
      );
      for (const row of linksStmt.iterate(...batch)) {
        const deleted = Boolean(row.deleted);
        const existing = result.get(row.path as string);
        if (!existing || existing.deleted) {
          result.set(row.path as string, {
            kind: deleted ? "missing" : "link",
            opTs: row.op_ts as number | null,
            deleted,
            hash: row.hash as string | null,
          });
        }
      }
    }
  } finally {
    db.close();
  }
  return result;
}
