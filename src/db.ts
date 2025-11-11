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
  "busy_timeout = 5000",
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
  CREATE TABLE IF NOT EXISTS hot_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL,
    side TEXT NOT NULL,
    op_ts INTEGER,
    source TEXT
  );
  CREATE INDEX IF NOT EXISTS hot_events_ts_idx ON hot_events(op_ts);
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

  db.exec(`
  CREATE TABLE IF NOT EXISTS recent_send (
    path TEXT NOT NULL,
    direction TEXT NOT NULL,
    op_ts INTEGER,
    signature TEXT,
    sent_at INTEGER NOT NULL,
    PRIMARY KEY(path, direction)
  );
  CREATE INDEX IF NOT EXISTS recent_send_dir_idx ON recent_send(direction);
  `);
  try {
    db.exec(`ALTER TABLE recent_send ADD COLUMN signature TEXT`);
  } catch {}

  // --- Node schema (new implementation) -----------------------------------
  db.exec(`
  CREATE TABLE IF NOT EXISTS nodes (
    path     TEXT PRIMARY KEY NOT NULL,
    kind     TEXT NOT NULL,        -- 'f', 'd', or 'l'
    hash     TEXT NOT NULL,        -- sha256 (files), symlink target, or '' for dirs
    mtime    REAL NOT NULL,
    updated  REAL NOT NULL,        -- logical timestamp we control
    size     INTEGER NOT NULL DEFAULT 0,
    deleted  INTEGER NOT NULL DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS nodes_deleted_path_idx ON nodes(deleted, path);
  CREATE INDEX IF NOT EXISTS nodes_updated_idx ON nodes(updated);
  `);

  return db;
}

export function getBaseDb(dbPath: string): Database {
  mkdirSync(dirname(dbPath), { recursive: true });
  const db = new Database(dbPath);
  db.pragma("busy_timeout = 5000");
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

     -- Node schema for base db mirrors alpha/beta
     CREATE TABLE IF NOT EXISTS nodes (
       path     TEXT PRIMARY KEY NOT NULL,
       kind     TEXT NOT NULL,
       hash     TEXT NOT NULL,
       mtime    REAL NOT NULL,
       updated  REAL NOT NULL,
       size     INTEGER NOT NULL DEFAULT 0,
       deleted  INTEGER NOT NULL DEFAULT 0
     );
     CREATE INDEX IF NOT EXISTS nodes_deleted_path_idx ON nodes(deleted, path);
     CREATE INDEX IF NOT EXISTS nodes_updated_idx ON nodes(updated);
    `);

  return db;
}
