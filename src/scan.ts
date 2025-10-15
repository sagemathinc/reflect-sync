// src/scan.ts
import fs from "node:fs/promises";
import { createReadStream } from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import Database from "better-sqlite3";
import { pipeline } from "node:stream/promises";
import { fdir } from "fdir";

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

const HASH_STREAM_CUTOFF = 10_000_000;

async function hashFile(filename: string, size: number): Promise<string> {
  const hash = createHash("sha256");
  if (size <= HASH_STREAM_CUTOFF) {
    // test non-streaming version:
    const x = await fs.readFile(filename);
    hash.update(x);
  } else {
    const stream = createReadStream(filename, {
      highWaterMark: 8 * 1024 * 1024,
    });
    // pipeline() handles back-pressure and avoids manual 'data' events.
    await pipeline(stream, async function* (source) {
      for await (const chunk of source) {
        hash.update(chunk);
      }
    });
  }
  return hash.digest("hex");
}

async function walk(root: string) {
  const t = Date.now();
  const files = await listFiles(root);
  console.log(`Found ${files.length} files`, Date.now() - t, "ms");

  const batch: any[] = [];
  for (const path of files) {
    let stat;
    try {
      stat = await fs.stat(path);
    } catch {
      continue;
    }
    const hash = await hashFile(path, stat.size);
    batch.push({
      path,
      size: stat.size,
      ctime: stat.ctimeMs,
      mtime: stat.mtimeMs,
      hash,
    });
    if (batch.length >= 2000) flushBatch(batch);
  }
  flushBatch(batch, true);

  console.log("Done", Date.now() - t, "ms");
}

(async () => {
  await walk(process.argv[2] ?? ".");
  console.log("Scan complete");
})();
