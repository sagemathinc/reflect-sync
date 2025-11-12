import { getDb } from "./db.js";

export interface LogicalClock {
  next(): number;
  current(): number;
}

export async function createLogicalClock(
  dbPaths: string[],
): Promise<LogicalClock> {
  const seeds: number[] = [];
  for (const dbPath of dbPaths) {
    if (!dbPath) continue;
    try {
      const db = getDb(dbPath);
      try {
        const row = db
          .prepare(`SELECT MAX(updated) AS max_updated FROM nodes`)
          .get() as { max_updated?: number };
        if (row && Number.isFinite(row.max_updated)) {
          seeds.push(Number(row.max_updated));
        }
      } finally {
        db.close();
      }
    } catch {
      // ignore missing/corrupt dbs when seeding
    }
  }
  let current = Math.max(Date.now(), ...seeds, 0);
  return {
    next() {
      current += 1;
      return current;
    },
    current() {
      return current;
    },
  };
}
