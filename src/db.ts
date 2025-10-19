import Database from "better-sqlite3";

export function getDb(dbPath: string): Database {
  // ----------------- SQLite setup -----------------
  const db = new Database(dbPath);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("temp_store = MEMORY");

  db.exec(`
CREATE TABLE IF NOT EXISTS files (
  path TEXT PRIMARY KEY,
  size INTEGER,
  ctime INTEGER,
  mtime INTEGER,
  hash TEXT,
  deleted INTEGER DEFAULT 0,
  last_seen INTEGER,
  hashed_ctime INTEGER
);
`);

  db.exec(`
CREATE TABLE IF NOT EXISTS dirs (
  path       TEXT PRIMARY KEY,
  ctime      INTEGER,
  mtime      INTEGER,
  deleted    INTEGER DEFAULT 0,
  last_seen  INTEGER
);
`);

  db.exec(`
  CREATE TABLE IF NOT EXISTS recent_touch (
    path TEXT PRIMARY KEY,
    ts   INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_recent_touch_ts ON recent_touch(ts);
`);

  return db;
}
