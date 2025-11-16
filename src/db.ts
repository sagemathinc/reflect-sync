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
  // Use default temp storage (usually on-disk) so large temp tables
  // like tmp_scan don't consume heap memory.
  "temp_store = DEFAULT",
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
  CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT
  );
`);

  // --- Node schema (new implementation) -----------------------------------
  db.exec(`
  CREATE TABLE IF NOT EXISTS nodes (
    path        TEXT PRIMARY KEY NOT NULL,
    kind        TEXT NOT NULL,        -- 'f', 'd', or 'l'
    hash        TEXT NOT NULL,        -- sha256 (files), symlink target, or '' for dirs
    mtime       REAL NOT NULL,
    ctime       REAL NOT NULL,
    change_start REAL,
    change_end REAL,
    confirmed_at REAL,
    hashed_ctime REAL,
    updated     REAL NOT NULL,        -- logical timestamp we control
    size        INTEGER NOT NULL DEFAULT 0,
    deleted     INTEGER NOT NULL DEFAULT 0,
    hash_pending INTEGER NOT NULL DEFAULT 0,
    copy_pending INTEGER NOT NULL DEFAULT 0,
    case_conflict INTEGER NOT NULL DEFAULT 0,
    canonical_key TEXT,
    last_seen   REAL,
    link_target TEXT,
    last_error  TEXT
  );
  CREATE INDEX IF NOT EXISTS nodes_deleted_path_idx ON nodes(deleted, path);
  CREATE INDEX IF NOT EXISTS nodes_updated_idx ON nodes(updated);
  CREATE INDEX IF NOT EXISTS nodes_canonical_idx ON nodes(canonical_key);
  `);

  ensureCaseConflictColumns(db);

  return db;
}

export function getBaseDb(dbPath: string): Database {
  mkdirSync(dirname(dbPath), { recursive: true });
  const db = new Database(dbPath);
  db.pragma("busy_timeout = 5000");
  db.pragma("auto_vacuum = INCREMENTAL");
  // Keep temp tables off-heap for the base DB as well.
  db.pragma("temp_store = DEFAULT");
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");

  db.exec(`
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
       path       TEXT PRIMARY KEY NOT NULL,
       kind       TEXT NOT NULL,
       hash       TEXT NOT NULL,
       mtime      REAL NOT NULL,
       ctime      REAL NOT NULL,
       change_start REAL,
       change_end REAL,
       confirmed_at REAL,
       hashed_ctime REAL,
       updated    REAL NOT NULL,
       size       INTEGER NOT NULL DEFAULT 0,
       deleted    INTEGER NOT NULL DEFAULT 0,
       hash_pending INTEGER NOT NULL DEFAULT 0,
       copy_pending INTEGER NOT NULL DEFAULT 0,
       case_conflict INTEGER NOT NULL DEFAULT 0,
       canonical_key TEXT,
       last_seen  REAL,
       link_target TEXT,
       last_error TEXT
     );
     CREATE INDEX IF NOT EXISTS nodes_deleted_path_idx ON nodes(deleted, path);
     CREATE INDEX IF NOT EXISTS nodes_updated_idx ON nodes(updated);
     CREATE INDEX IF NOT EXISTS nodes_canonical_idx ON nodes(canonical_key);
    `);
  ensureCaseConflictColumns(db);
  db.exec(`
    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY NOT NULL,
      value TEXT
    );
  `);
  return db;
}

function ensureCaseConflictColumns(db: Database) {
  try {
    db.exec(
      `ALTER TABLE nodes ADD COLUMN case_conflict INTEGER NOT NULL DEFAULT 0;`,
    );
  } catch {
    // column already exists
  }
  try {
    db.exec(`ALTER TABLE nodes ADD COLUMN canonical_key TEXT;`);
  } catch {
    // column already exists
  }
  try {
    db.exec(
      `CREATE INDEX IF NOT EXISTS nodes_canonical_idx ON nodes(canonical_key);`,
    );
  } catch {
    // index may already exist or table missing
  }
}
