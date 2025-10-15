import fs from "node:fs/promises";
import path from "node:path";
import Database from "better-sqlite3";
import { Worker } from "node:worker_threads";
import { cpus } from "node:os";
import * as walk from "@nodelib/fs.walk";

const db = new Database("alpha.db");
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");
db.exec(`
  CREATE TABLE IF NOT EXISTS files (
    path TEXT PRIMARY KEY,
    size INTEGER,
    ctime INTEGER,
    mtime INTEGER,
    hash TEXT,
    deleted INTEGER DEFAULT 0,
    last_seen INTEGER
  )
`);

const insert = db.prepare(`
  INSERT INTO files (path,size,ctime,mtime,hash,deleted,last_seen)
  VALUES (@path,@size,@ctime,@mtime,@hash,0,@last_seen)
  ON CONFLICT(path) DO UPDATE SET
    size=excluded.size,
    ctime=excluded.ctime,
    mtime=excluded.mtime,
    last_seen=excluded.last_seen,
    -- keep prior hash unless we provide a new one later:
    deleted=0
`);

// Batch transaction wrapper
const batchInsert = db.transaction((rows: any[]) => {
  for (const r of rows) insert.run(r);
});

// Flush a batch of rows to the DB
function flushBatch(rows: any[], force = false) {
  if (rows.length === 0) return;
  batchInsert(rows);
  rows.length = 0;
}

// ---------------- worker pool -----------------
const CPU_COUNT = Math.max(1, Math.min(cpus().length, 8));
const workers = Array.from(
  { length: CPU_COUNT },
  () => new Worker(new URL("./hash-worker.ts", import.meta.url)),
);
const freeWorkers: Worker[] = [...workers];
const pending: (() => void)[] = [];

const BATCH_SIZE = 250;
let last_seen = 0;
let received = 0;
for (const w of workers) {
  w.on("message", (msg) => {
    received++;
    freeWorkers.push(w);
    pending.shift()?.(); // wake next waiters
    if (msg.error) {
      //console.error("Worker error for", msg.path, msg.error);
      return;
    }
    results.push({ ...msg, last_seen });
    if (results.length >= BATCH_SIZE) {
      flushBatch(results);
    }
  });
}

const results: any[] = [];
function nextWorker(): Promise<Worker> {
  return new Promise((resolve) => {
    if (freeWorkers.length) {
      return resolve(freeWorkers.pop()!);
    }
    pending.push(() => resolve(freeWorkers.pop()!));
  });
}

// ---------------- walker (streaming) -----------
type Entry = {
  path: string; // full path
  dirent: import("fs").Dirent;
  stats?: import("fs").Stats;
};

async function scan(root: string) {
  let dispatched = 0;
  const start = Date.now();
  const scan_id = Date.now();
  last_seen = scan_id;

  // Stream entries; request Stats to avoid a second stat() in workers
  const stream = walk.walkStream(root, {
    stats: true,
    followSymbolicLinks: false,
    concurrency: 64, // tune: 32–256 are typical
    entryFilter: (e) => e.dirent.isFile(),
    errorFilter: () => true,
  });

  // small periodic flush so results don’t sit too long
  const flushTimer = setInterval(() => flushBatch(results), 500).unref();

  const rowBuf: any[] = [];
  for await (const entry of stream as AsyncIterable<Entry>) {
    const st = entry.stats!;
    // record metadata immediately (no hash yet)
    rowBuf.push({
      path: entry.path,
      size: st.size,
      ctime: st.ctimeMs ?? st.ctime.getTime(),
      mtime: st.mtimeMs ?? st.mtime.getTime(),
      hash: null,
      last_seen: scan_id,
    });
    if (rowBuf.length >= 2000) flushBatch(rowBuf);

    // dispatch hashing job (worker uses provided size to choose stream vs readFile)
    const w = await nextWorker();
    w.postMessage({
      path: entry.path,
      size: st.size,
      ctime: st.ctimeMs ?? st.ctime.getTime(),
      mtime: st.mtimeMs ?? st.mtime.getTime(),
    });
    dispatched++;
  }
  flushBatch(rowBuf);

  // wait for all hashes to complete
  while (received < dispatched) {
    await new Promise((r) => setTimeout(r, 30));
  }
  flushBatch(results);
  clearInterval(flushTimer);

  // mark deletions (anything not seen this pass)
  db.prepare(`UPDATE files SET deleted=1 WHERE last_seen <> ?`).run(scan_id);

  await Promise.all(workers.map((w) => w.terminate()));
  console.log(`Done in ${Date.now() - start} ms (files: ${dispatched})`);
}

(async () => {
  await scan(process.argv[2] ?? ".");
  console.log("Scan complete");
})();
