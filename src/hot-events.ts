import { getDb } from "./db.js";
import type { Database } from "./db.js";

export type HotEvent = {
  id: number;
  path: string;
  side: "alpha" | "beta";
  opTs: number;
  source: string | null;
};

const HOT_EVENT_TTL_MS = Number(
  process.env.REFLECT_HOT_EVENT_TTL_MS ?? 5 * 60_000,
);

const dbCache = new Map<string, Database>();
const lastCleanup = new Map<string, number>();

function getHandle(dbPath: string): Database {
  let handle = dbCache.get(dbPath);
  if (!handle) {
    handle = getDb(dbPath);
    dbCache.set(dbPath, handle);
  }
  return handle;
}

export function recordHotEvent(
  dbPath: string,
  side: "alpha" | "beta",
  path: string,
  source: string,
): void {
  if (!path) return;
  const db = getHandle(dbPath);
  const now = Date.now();
  db
    .prepare(
      `INSERT INTO hot_events(path, side, op_ts, source) VALUES(?, ?, ?, ?);`,
    )
    .run(path, side, now, source);
  const last = lastCleanup.get(dbPath) ?? 0;
  if (now - last > HOT_EVENT_TTL_MS) {
    db.prepare(`DELETE FROM hot_events WHERE op_ts < ?`).run(
      now - HOT_EVENT_TTL_MS,
    );
    lastCleanup.set(dbPath, now);
  }
}

export function fetchHotEvents(
  dbPath: string,
  sinceOpTs: number,
  opts: { side?: "alpha" | "beta"; limit?: number } = {},
): HotEvent[] {
  const db = getHandle(dbPath);
  const limit = opts.limit ?? 500;
  const stmt = db.prepare(
    `SELECT id, path, side, op_ts as opTs, source
       FROM hot_events
       WHERE op_ts > ? ${opts.side ? "AND side = ?" : ""}
       ORDER BY op_ts ASC
       LIMIT ?`,
  );
  const params = opts.side
    ? [sinceOpTs, opts.side, limit]
    : [sinceOpTs, limit];
  const rows = stmt.all(...params) as HotEvent[];
  if (rows.length) {
    const ids = rows.map((row) => row.id);
    const placeholders = ids.map(() => "?").join(",");
    db.prepare(`DELETE FROM hot_events WHERE id IN (${placeholders})`).run(
      ...ids,
    );
  }
  return rows;
}

export function getMaxOpTs(dbPath: string): number {
  const db = getHandle(dbPath);
  const row = db
    .prepare(
      `SELECT MAX(op_ts) as m FROM (
         SELECT op_ts FROM files
         UNION ALL
         SELECT op_ts FROM dirs
         UNION ALL
         SELECT op_ts FROM links
       )`,
    )
    .get() as { m: number | null };
  return row?.m ?? 0;
}
