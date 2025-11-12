// src/op-stamp.ts
import { getDb } from "./db.js";
import {
  nodeKindToEntry,
  type EntryKind as NodeEntryKind,
} from "./nodes-util.js";

export type EntryKind = NodeEntryKind;

export type OpStampEntryKind = EntryKind | "missing";

export interface OpStamp {
  kind: OpStampEntryKind;
  opTs: number | null;
  deleted: boolean;
  size?: number | null;
  mtime?: number | null;
  ctime?: number | null;
  hash?: string | null;
  target?: string | null;
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
      const stmt = db.prepare(
        `SELECT path, kind, hash, mtime, ctime, updated, size, deleted, link_target
           FROM nodes
          WHERE path IN (${placeholders})`,
      );
      for (const row of stmt.iterate(...batch)) {
        const deleted = !!row.deleted;
        const path = row.path as string;
        const entryKind = nodeKindToEntry(row.kind as string);
        if (result.has(path) && !result.get(path)!.deleted) continue;
        result.set(path, {
          kind: deleted ? "missing" : entryKind,
          opTs: row.updated != null ? Number(row.updated) : null,
          deleted,
          size: row.size != null ? Number(row.size) : null,
          mtime: row.mtime != null ? Number(row.mtime) : null,
          ctime: row.ctime != null ? Number(row.ctime) : null,
          hash: row.hash != null ? String(row.hash) : null,
          target: row.link_target != null ? String(row.link_target) : null,
        });
      }
      for (const path of batch) {
        if (!result.has(path)) {
          result.set(path, {
            kind: "missing",
            opTs: null,
            deleted: true,
          });
        }
      }
    }
  } finally {
    db.close();
  }
  return result;
}
