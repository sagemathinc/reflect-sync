import Database from "better-sqlite3";
import { Worker } from "node:worker_threads";
import os from "node:os";
import * as walk from "@nodelib/fs.walk";

type Row = {
  path: string;
  size: number;
  ctime: number;
  mtime: number;
  hash: string | null;
  last_seen: number;
  hashed_ctime: number | null;
};

const DB_PATH = process.env.DB_PATH ?? "alpha.db";
const CPU_COUNT = Math.min(os.cpus().length, 8);
const DB_BATCH_SIZE = 2000;
const DISPATCH_BATCH = 256; // files per worker message

// ----------------- SQLite setup -----------------
const db = new Database(DB_PATH);
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
  last_seen INTEGER,
  hashed_ctime INTEGER
);
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS recent_touch (
    path TEXT PRIMARY KEY,
    ts   INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_recent_touch_ts ON recent_touch(ts);
`);
const insTouch = db.prepare(
  `INSERT OR REPLACE INTO recent_touch(path, ts) VALUES (?, ?)`,
);

// and whenever you flush your DB batch, also flush touches:
const touchTx = db.transaction((rows: [string, number][]) => {
  for (const [p, t] of rows) insTouch.run(p, t);
});
function flushTouchBatch(touchBatch: [string, number][]) {
  if (!touchBatch.length) return;
  touchTx(touchBatch);
  touchBatch.length = 0;
}

function ensureColumn(col: string, def: string) {
  try {
    db.prepare(`ALTER TABLE files ADD COLUMN ${col} ${def}`).run();
  } catch {
    // ignore if it already exists
  }
}
// For existing DBs that predate these:
ensureColumn("last_seen", "INTEGER");
ensureColumn("hashed_ctime", "INTEGER");

const upsertMeta = db.prepare(`
INSERT INTO files (path,size,ctime,mtime,hash,deleted,last_seen,hashed_ctime)
VALUES (@path,@size,@ctime,@mtime,@hash,0,@last_seen,@hashed_ctime)
ON CONFLICT(path) DO UPDATE SET
  size=excluded.size,
  ctime=excluded.ctime,
  mtime=excluded.mtime,
  last_seen=excluded.last_seen,
  deleted=0
-- NOTE: we intentionally DO NOT overwrite hash or hashed_ctime here.
`);

const applyMetaBatch = db.transaction((rows: Row[]) => {
  for (const r of rows) upsertMeta.run(r);
});

const applyHashBatch = db.transaction(
  (rows: { path: string; hash: string; ctime: number }[]) => {
    const stmt = db.prepare(
      `UPDATE files
       SET hash = ?, hashed_ctime = ?, deleted = 0
       WHERE path = ?`,
    );
    for (const r of rows) stmt.run(r.hash, r.ctime, r.path);
  },
);

// ----------------- Worker pool ------------------
type Job = { path: string; size: number; ctime: number; mtime: number };
type JobBatch = { jobs: Job[] };
type Result =
  | { path: string; hash: string; ctime: number }
  | { path: string; error: string };

const workers = Array.from(
  { length: CPU_COUNT },
  () => new Worker(new URL("./hash-worker.ts", import.meta.url)),
);
const freeWorkers: Worker[] = [...workers];
const waiters: Array<() => void> = [];

function nextWorker(): Promise<Worker> {
  return new Promise((resolve) => {
    const w = freeWorkers.pop();
    if (w) return resolve(w);
    waiters.push(() => resolve(freeWorkers.pop()!));
  });
}

// Buffer for hash results (to batch DB writes)
const hashResults: { path: string; hash: string; ctime: number }[] = [];
const touchBatch: [string, number][] = [];

let dispatched = 0;
let received = 0;

// Handle worker replies (batched)
for (const w of workers) {
  w.on("message", (msg: { done?: Result[] }) => {
    freeWorkers.push(w);
    waiters.shift()?.();

    const arr = msg.done || [];
    received += arr.length;

    // Collect successful ones for batched DB write
    for (const r of arr) {
      if ("error" in r) {
        // Log and continue
        // console.error("hash error:", r.path, r.error);
      } else {
        hashResults.push({ path: r.path, hash: r.hash, ctime: r.ctime });
        touchBatch.push([r.path, Date.now()]);
        if (hashResults.length >= DB_BATCH_SIZE) {
          applyHashBatch(hashResults);
          hashResults.length = 0;
          flushTouchBatch(touchBatch);
        }
      }
    }
  });
}

// --------------- Walk + incremental logic ---------------
async function scan(root: string) {
  const t0 = Date.now();
  const scan_id = Date.now();

  // stream entries with stats so we avoid a second stat in main thread
  const stream = walk.walkStream(root, {
    stats: true,
    followSymbolicLinks: false,
    concurrency: 128,
    entryFilter: (e) => e.dirent.isFile(),
    errorFilter: () => true,
  });

  // Periodic flush so we don't hold large arrays in RAM too long
  const periodicFlush = setInterval(() => {
    if (hashResults.length) {
      applyHashBatch(hashResults);
      hashResults.length = 0;
      flushTouchBatch(touchBatch);
    }
  }, 500).unref();

  // Mini-buffers
  const metaBuf: Row[] = [];
  let jobBuf: Job[] = [];

  // Prepare a fast fetch of existing meta to check ctime change
  const getExisting = db.prepare<
    [string],
    | {
        size: number;
        ctime: number;
        mtime: number;
        hashed_ctime: number | null;
      }
    | undefined
  >(`SELECT size, ctime, mtime, hashed_ctime FROM files WHERE path = ?`);

  for await (const entry of stream as AsyncIterable<{
    path: string;
    stats: import("fs").Stats;
  }>) {
    const full = entry.path; // already full path
    const st = entry.stats!;
    const size = st.size;
    const ctime = (st as any).ctimeMs ?? st.ctime.getTime();
    const mtime = (st as any).mtimeMs ?? st.mtime.getTime();

    // Upsert *metadata only* (no hash/hashed_ctime change here)
    metaBuf.push({
      path: full,
      size,
      ctime,
      mtime,
      hash: null,
      last_seen: scan_id,
      hashed_ctime: null,
    });
    if (metaBuf.length >= DB_BATCH_SIZE) {
      applyMetaBatch(metaBuf);
      metaBuf.length = 0;
    }

    // Decide if we need to hash: only when ctime changed since last time
    const row = getExisting.get(full);
    const needsHash = !row || row.hashed_ctime !== ctime;

    if (needsHash) {
      //console.log(full, !row, row?.hashed_ctime, ctime);
      jobBuf.push({ path: full, size, ctime, mtime });
      // Dispatch in batches to minimize IPC
      if (jobBuf.length >= DISPATCH_BATCH) {
        const w = await nextWorker();
        (w as any).postMessage({ jobs: jobBuf } as JobBatch);
        dispatched += jobBuf.length;
        jobBuf = [];
      }
    }
  }

  // Flush remaining meta
  if (metaBuf.length) {
    applyMetaBatch(metaBuf);
    metaBuf.length = 0;
  }

  // Flush remaining job batch
  if (jobBuf.length) {
    const w = await nextWorker();
    (w as any).postMessage({ jobs: jobBuf } as JobBatch);
    dispatched += jobBuf.length;
    jobBuf = [];
  }

  // Wait for all hashes to finish
  while (received < dispatched) {
    await new Promise((r) => setTimeout(r, 20));
  }

  // Final flush of hash results and meta
  if (hashResults.length) {
    applyHashBatch(hashResults);
    hashResults.length = 0;
    flushTouchBatch(touchBatch);
  }
  // Mark deletions (anything not seen this pass)
  db.prepare(`UPDATE files SET deleted=1 WHERE last_seen <> ?`).run(scan_id);

  clearInterval(periodicFlush);
  await Promise.all(workers.map((w) => w.terminate()));

  console.log(
    `Scan done: ${dispatched} hashed / ${received} results in ${Date.now() - t0} ms`,
  );
}

await scan(process.argv[2] ?? ".");
