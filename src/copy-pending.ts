import { getDb } from "./db.js";

export type CopyPendingRow = {
  path: string;
  kind: string;
  hash: string;
  mtime: number;
  ctime: number;
  change_start: number | null;
  change_end: number | null;
  confirmed_at: number | null;
  hashed_ctime: number | null;
  updated: number;
  size: number;
  deleted: number;
  hash_pending: number;
  copy_pending: number;
  last_seen: number | null;
  link_target: string | null;
  last_error: string | null;
};

/**
 * Synchronize any nodes that completed a copy (copy_pending = 2) into the base DB
 * and clear the pending flag in the source DB.
 *
 * Returns the number of rows that were synchronized.
 */
export function syncConfirmedCopiesToBase(
  sourceDbPath: string,
  baseDbPath: string,
): number {
  const sourceDb = getDb(sourceDbPath);
  let rows: CopyPendingRow[] = [];
  try {
    rows = sourceDb
      .prepare(
        `SELECT path, kind, hash, mtime, ctime, change_start, change_end, confirmed_at,
                hashed_ctime, updated, size, deleted, hash_pending, copy_pending,
                last_seen, link_target, last_error
           FROM nodes
          WHERE copy_pending = 2`,
      )
      .all() as CopyPendingRow[];
    if (!rows.length) return 0;

    const clearStmt = sourceDb.prepare(
      `UPDATE nodes SET copy_pending = 0 WHERE path = ?`,
    );
    const clearTx = sourceDb.transaction((entries: CopyPendingRow[]) => {
      for (const entry of entries) {
        clearStmt.run(entry.path);
      }
    });
    clearTx(rows);
  } finally {
    sourceDb.close();
  }

  const baseDb = getDb(baseDbPath);
  try {
    const upsert = baseDb.prepare(
      `INSERT INTO nodes(path, kind, hash, mtime, ctime, change_start, change_end, confirmed_at,
                         hashed_ctime, updated, size, deleted, hash_pending, copy_pending,
                         last_seen, link_target, last_error)
       VALUES (@path,@kind,@hash,@mtime,@ctime,@change_start,@change_end,@confirmed_at,
               @hashed_ctime,@updated,@size,@deleted,@hash_pending,0,@last_seen,@link_target,@last_error)
       ON CONFLICT(path) DO UPDATE SET
         kind=excluded.kind,
         hash=excluded.hash,
         mtime=excluded.mtime,
         ctime=excluded.ctime,
         change_start=excluded.change_start,
         change_end=excluded.change_end,
         confirmed_at=excluded.confirmed_at,
         hashed_ctime=excluded.hashed_ctime,
         updated=excluded.updated,
         size=excluded.size,
         deleted=excluded.deleted,
         hash_pending=excluded.hash_pending,
         copy_pending=0,
         last_seen=excluded.last_seen,
         link_target=excluded.link_target,
         last_error=excluded.last_error`,
    );
    const upsertTx = baseDb.transaction((entries: CopyPendingRow[]) => {
      for (const entry of entries) {
        const {
          copy_pending: _ignored,
          ...bindings
        } = entry;
        upsert.run(bindings);
      }
    });
    upsertTx(rows);
  } finally {
    baseDb.close();
  }

  return rows.length;
}
