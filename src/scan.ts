// src/scan.ts
import fs from "node:fs/promises";
import path from "node:path";
import Database from "better-sqlite3";
import { fdir } from "fdir";
import { Worker } from "node:worker_threads";
import { cpus } from "node:os";

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
    deleted INTEGER DEFAULT 0
  )
`);

const insert = db.prepare(`
  INSERT INTO files (path, size, ctime, mtime, hash, deleted)
  VALUES (@path, @size, @ctime, @mtime, @hash, 0)
  ON CONFLICT(path) DO UPDATE SET
    size=excluded.size,
    ctime=excluded.ctime,
    mtime=excluded.mtime,
    hash=excluded.hash,
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
    results.push(msg);
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

async function listFiles(dir: string): Promise<string[]> {
  const api = new fdir()
    .withBasePath()
    .withFullPaths()
    .withMaxDepth(999) // 0 = no limit
    .crawl(dir);

  // Run asynchronously
  const files = await api.withPromise();
  return files;
}
async function walk(root: string) {
  const t0 = Date.now();
  const files = await listFiles(root);
  console.log(`Found ${files.length} files`, Date.now() - t0, "ms");

  let processed = 0;
  received = 0;
  for (const p of files) {
    let stat;
    try {
      stat = await fs.stat(p);
    } catch {
      continue;
    }
    const worker = await nextWorker();
    worker.postMessage({
      path: p,
      size: stat.size,
      ctime: stat.ctimeMs,
      mtime: stat.mtimeMs,
    });
    processed++;
  }

  // Wait for all to finish
  while (received < processed) {
    await new Promise((r) => setTimeout(r, 100));
  }
  flushBatch(results);

  for (const w of workers) {
    w.terminate();
  }
  console.log("Done", Date.now() - t0, "ms");
}

(async () => {
  await walk(process.argv[2] ?? ".");
  console.log("Scan complete");
})();
