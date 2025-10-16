// src/ingest-delta.ts
//
// Read NDJSON deltas from stdin and mirror them into a local files table.
// Usage:
//   node dist/ingest-delta.js --db alpha.db --root /remote/alpha/root
//
// Example over SSH:
//   ssh -C user@alpha 'env DB_PATH=~/.cache/cocalc-sync/alpha.db node /path/dist/scan.js /alpha/root --emit-delta' \
//     | node dist/ingest-delta.js --db alpha.db --root /alpha/root

import Database from "better-sqlite3";
import readline from "node:readline";

// ---------- args ----------
const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) {
  const k = process.argv[i], v = process.argv[i + 1];
  if (!k) break;
  args.set(k.replace(/^--/, ""), v ?? "");
}
const dbPath = args.get("db") || "alpha.db";
const root = (args.get("root") || "").replace(/\/+$/, ""); // optional safety

// ---------- db ----------
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
CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
`);

const upsert = db.prepare(`
INSERT INTO files(path,size,ctime,mtime,hash,deleted,last_seen,hashed_ctime)
VALUES (@path,@size,@ctime,@mtime,@hash,@deleted,@now,@hashed_ctime)
ON CONFLICT(path) DO UPDATE SET
  size=COALESCE(excluded.size, files.size),
  ctime=COALESCE(excluded.ctime, files.ctime),
  mtime=COALESCE(excluded.mtime, files.mtime),
  -- Only update hash/hashed_ctime when a hash is provided
  hash=COALESCE(excluded.hash, files.hash),
  hashed_ctime=COALESCE(excluded.hashed_ctime, files.hashed_ctime),
  deleted=excluded.deleted,
  last_seen=excluded.last_seen
`);

const tx = db.transaction((rows: any[]) => {
  const now = Date.now();
  for (const r of rows) {
    // shape: {path, size?, ctime?, mtime?, hash?, deleted?}
    // normalize/guard
    if (root && !r.path.startsWith(root + "/") && r.path !== root) continue;
    const isDelete = r.deleted === 1;
    upsert.run({
      path: r.path,
      size: isDelete ? null : r.size ?? null,
      ctime: isDelete ? null : r.ctime ?? null,
      mtime: isDelete ? null : r.mtime ?? null,
      hash: isDelete ? null : r.hash ?? null,
      deleted: isDelete ? 1 : 0,
      now,
      hashed_ctime: isDelete ? null : r.ctime ?? null,
    });
  }
});

let buf: any[] = [];
const BATCH = 5000;
function flush() {
  if (!buf.length) return;
  tx(buf);
  buf = [];
}

const rl = readline.createInterface({ input: process.stdin });
rl.on("line", (line) => {
  if (!line) return;
  try {
    const r = JSON.parse(line);
    buf.push(r);
    if (buf.length >= BATCH) flush();
  } catch {
    // ignore malformed lines
  }
});
rl.on("close", () => {
  flush();
  db.close();
});
process.on("SIGINT", () => {
  flush();
  db.close();
  process.exit(0);
});
