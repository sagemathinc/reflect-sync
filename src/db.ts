import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";

export class Database extends DatabaseSync {
  transaction = <A extends any[]>(fn: (...args: A) => void) => {
    return (...args: A) => {
      this.exec("BEGIN IMMEDIATE");
      try {
        fn(...args);
        this.exec("COMMIT");
      } catch (e) {
        try {
          this.exec("ROLLBACK");
        } catch {}
        throw e;
      }
    };
  };

  pragma = (s: string) => {
    this.exec(`PRAGMA ${s}`);
  };
}

const PRAGMAS = [
  "auto_vacuum = INCREMENTAL",
  "temp_store = MEMORY",
  "journal_mode = WAL",
  "synchronous = NORMAL",
];

export function getDb(dbPath: string): Database {
  // ----------------- SQLite setup -----------------
  mkdirSync(dirname(dbPath), { recursive: true });
  const db = new Database(dbPath);
  for (const pragma of PRAGMAS) {
    try {
      db.pragma(pragma);
    } catch {
      // if two at once loading db it may lock so we skip setting a pragma in that case.
    }
  }

  db.exec(`
CREATE TABLE IF NOT EXISTS files (
  path TEXT PRIMARY KEY NOT NULL,
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

CREATE INDEX IF NOT EXISTS files_live_idx ON files(deleted, path);
CREATE INDEX IF NOT EXISTS files_pdo ON files(path, deleted, op_ts);
`);

  db.exec(`
  CREATE TABLE IF NOT EXISTS dirs (
    path       TEXT PRIMARY KEY NOT NULL,
    ctime      INTEGER,
    mtime      INTEGER,
    op_ts      INTEGER,  -- operation timestamp for last write wins
    hash       TEXT DEFAULT '',     -- hash used for *metadata* only for directories.
    deleted    INTEGER DEFAULT 0,
    last_seen  INTEGER
  );

  CREATE INDEX IF NOT EXISTS dirs_live_idx  ON dirs(deleted, path);
`);

  db.exec(`
  CREATE TABLE IF NOT EXISTS recent_touch (
    path TEXT PRIMARY KEY NOT NULL,
    ts   INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_recent_touch_ts ON recent_touch(ts);
`);

  db.exec(`
  CREATE TABLE IF NOT EXISTS links (
    path       TEXT PRIMARY KEY NOT NULL,
    target     TEXT,
    ctime      INTEGER,
    mtime      INTEGER,
    op_ts      INTEGER,   -- LWW op timestamp
    hash       TEXT,
    deleted    INTEGER DEFAULT 0,
    last_seen  INTEGER
  );
CREATE INDEX IF NOT EXISTS links_live_idx ON links(deleted, path);
CREATE INDEX IF NOT EXISTS links_pdo ON files(path, deleted, op_ts);
  `);

  return db;
}

export function getBaseDb(dbPath: string): Database {
  mkdirSync(dirname(dbPath), { recursive: true });
  const db = new Database(dbPath);
  db.pragma("auto_vacuum = INCREMENTAL");
  db.pragma("temp_store = MEMORY");
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");

  // base (files) and base_dirs (directories) — relative paths; include op_ts for LWW
  db.exec(`
      CREATE TABLE IF NOT EXISTS base (
        path    TEXT PRIMARY KEY,  -- RELATIVE file path
        hash    TEXT,
        deleted INTEGER DEFAULT 0,
        op_ts   INTEGER
      );

      CREATE TABLE IF NOT EXISTS base_dirs (
        path    TEXT PRIMARY KEY,  -- RELATIVE dir path
        hash    TEXT DEFAULT '',
        deleted INTEGER DEFAULT 0,
        op_ts   INTEGER
      );

     CREATE TABLE IF NOT EXISTS events(
      id INTEGER PRIMARY KEY,
      ts INTEGER,
      level TEXT,
      source TEXT,
      msg TEXT,
      details TEXT
    );
    `);

  return db;
}
