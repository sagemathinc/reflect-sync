import Database from "better-sqlite3";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

export function getDb(dbPath: string): Database {
  // ----------------- SQLite setup -----------------
  mkdirSync(dirname(dbPath), { recursive: true });
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
  -- op_ts = operation timestamp: adjusted timestamp in ms used for last
  --         write wins = normalized mtime for creates/modifies; for deletes,
  --         set to observed delete time
  op_ts INTEGER,
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
    op_ts      INTEGER,  -- operation timestamp for last write wins
    hash       TEXT DEFAULT '',     -- hash used for *metadata* only for directories.
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

  db.exec(`
  CREATE TABLE IF NOT EXISTS links (
    path       TEXT PRIMARY KEY,
    target     TEXT,
    ctime      INTEGER,
    mtime      INTEGER,
    op_ts      INTEGER,   -- LWW op timestamp
    hash       TEXT,
    deleted    INTEGER DEFAULT 0,
    last_seen  INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_links_path ON links(path);
  `);

  return db;
}
