#!/usr/bin/env node
// src/scan.ts
import { Worker } from "node:worker_threads";
import os from "node:os";
import * as walk from "@nodelib/fs.walk";
import { readlink } from "node:fs/promises";
import { getDb } from "./db.js";
import { Command } from "commander";
import { cliEntrypoint } from "./cli-util.js";
import { xxh128String } from "./hash.js";
import path from "node:path";
import { loadIgnoreFile, normalizeR } from "./ignore.js";

function buildProgram(): Command {
  const program = new Command();

  return program
    .name("ccsync-scan")
    .description("Run a local scan writing to sqlite database")
    .requiredOption("--root <path>", "directory to scan")
    .requiredOption("--db <file>", "path to sqlite database")
    .option("--emit-delta", "emit NDJSON deltas to stdout for ingest", false)
    .option("--verbose", "enable verbose logging", false);
}

type ScanOptions = {
  db: string;
  emitDelta: boolean;
  verbose: boolean;
  root: string;
};

export async function runScan(opts: ScanOptions): Promise<void> {
  const { root, db: DB_PATH, emitDelta, verbose } = opts;

  type Row = {
    path: string;
    size: number;
    ctime: number;
    mtime: number;
    mode: number;
    uid: number;
    gid: number;
    op_ts: number;
    hash: string | null;
    last_seen: number;
    hashed_ctime: number | null;
  };

  if (verbose) {
    console.log("running scan with database = ", DB_PATH);
  }

  if (emitDelta) {
    process.stdout.write(
      JSON.stringify({ kind: "time", remote_now_ms: Date.now() }) + "\n",
    );
  }

  const CPU_COUNT = Math.min(os.cpus().length, 8);
  const DB_BATCH_SIZE = 2000;
  const DISPATCH_BATCH = 256; // files per worker message

  // ----------------- SQLite setup -----------------
  const db = getDb(DB_PATH);

  const insTouch = db.prepare(
    `INSERT OR REPLACE INTO recent_touch(path, ts) VALUES (?, ?)`,
  );

  const touchTx = db.transaction((rows: [string, number][]) => {
    for (const [p, t] of rows) insTouch.run(p, t);
  });

  function flushTouchBatch(touchBatch: [string, number][]) {
    if (!touchBatch.length) return;
    touchTx(touchBatch);
    touchBatch.length = 0;
  }

  // for directory metadata
  const upsertDir = db.prepare(`
INSERT INTO dirs(path, ctime, mtime, mode, uid, gid, op_ts, deleted, last_seen)
VALUES (@path, @ctime, @mtime, @mode, @uid, @gid, @op_ts, 0, @scan_id)
ON CONFLICT(path) DO UPDATE SET
  ctime     = excluded.ctime,
  mtime     = excluded.mtime,
  mode      = excluded.mode,
  uid       = excluded.uid,
  gid       = excluded.gid,
  deleted   = 0,
  last_seen = excluded.last_seen,
  -- Update op_ts if resurrecting OR if metadata changed since last time
  op_ts     = CASE
                WHEN dirs.deleted = 1
                     OR dirs.mode <> excluded.mode
                     OR dirs.uid  <> excluded.uid
                     OR dirs.gid  <> excluded.gid
                     OR dirs.mtime <> excluded.mtime
                     OR dirs.ctime <> excluded.ctime
                THEN excluded.op_ts
                ELSE dirs.op_ts
              END
`);

  type DirRow = {
    path: string;
    ctime: number;
    mtime: number;
    mode: number;
    uid: number;
    gid: number;
    op_ts: number;
    scan_id: number;
  };

  const applyDirBatch = db.transaction((rows: DirRow[]) => {
    for (const r of rows) {
      upsertDir.run(r);
    }
  });

  const upsertMeta = db.prepare(`
INSERT INTO files (path, size, ctime, mtime, mode, uid, gid, op_ts, hash, deleted, last_seen, hashed_ctime)
VALUES (@path, @size, @ctime, @mtime, @mode, @uid, @gid, @op_ts, @hash, 0, @last_seen, @hashed_ctime)
ON CONFLICT(path) DO UPDATE SET
  size=excluded.size,
  ctime=excluded.ctime,
  mtime=excluded.mtime,
  mode=excluded.mode,
  uid=excluded.uid,
  gid=excluded.gid,
  op_ts=excluded.op_ts,
  last_seen=excluded.last_seen,
  deleted=0
-- NOTE: we intentionally DO NOT overwrite hash or hashed_ctime here.
`);

  const applyMetaBatch = db.transaction((rows: Row[]) => {
    for (const r of rows) {
      upsertMeta.run(r);
    }
  });

  const applyHashBatch = db.transaction(
    (rows: { path: string; hash: string; ctime: number }[]) => {
      const stmt = db.prepare(
        `UPDATE files
       SET hash = ?, hashed_ctime = ?, deleted = 0
       WHERE path = ?`,
      );
      for (const r of rows) {
        stmt.run(r.hash, r.ctime, r.path);
      }
    },
  );

  type LinkRow = {
    path: string;
    target: string;
    ctime: number;
    mtime: number;
    op_ts: number;
    hash: string;
    scan_id: number;
  };

  const upsertLink = db.prepare(`
INSERT INTO links(path, target, ctime, mtime, op_ts, hash, deleted, last_seen)
VALUES (@path, @target, @ctime, @mtime, @op_ts, @hash, 0, @scan_id)
ON CONFLICT(path) DO UPDATE SET
target=excluded.target,
ctime=excluded.ctime,
mtime=excluded.mtime,
op_ts=excluded.op_ts,
hash=excluded.hash,
deleted=0,
last_seen=excluded.last_seen
`);

  const applyLinksBatch = db.transaction((rows: LinkRow[]) => {
    for (const r of rows) {
      upsertLink.run(r);
    }
  });

  // ----------------- Worker pool ------------------
  type Job = { path: string; size: number; ctime: number; mtime: number };
  type JobBatch = { jobs: Job[] };
  type Result =
    | { path: string; hash: string; ctime: number }
    | { path: string; error: string };

  const workers = Array.from(
    { length: CPU_COUNT },
    () => new Worker(new URL("./hash-worker", import.meta.url)),
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

  // Emit buffering (fewer stdout writes)
  const deltaBuf: string[] = [];
  const emitObj = (o: {
    kind?: "dir" | "link";
    path: string;
    op_ts: number;
    deleted: number;
    size?: number;
    ctime?: number;
    mtime?: number;
    hash?: string;
    target?: string; // for links
  }) => {
    if (!emitDelta) return;
    deltaBuf.push(JSON.stringify(o));
    if (deltaBuf.length >= 1000) flushDeltaBuf();
  };
  function flushDeltaBuf() {
    if (!emitDelta || deltaBuf.length === 0) return;
    process.stdout.write(deltaBuf.join("\n") + "\n");
    deltaBuf.length = 0;
  }

  // Keep minimal metadata for paths that will be hashed so we can emit a full row
  const pendingMeta = new Map<
    string,
    { size: number; ctime: number; mtime: number }
  >();

  let dispatched = 0;
  let received = 0;

  // Handle worker replies (batched)
  for (const w of workers) {
    w.on("message", (msg: { done?: Result[] }) => {
      freeWorkers.push(w);
      waiters.shift()?.();

      const arr = msg.done || [];
      received += arr.length;

      // Collect successful ones for batched DB write and emit deltas
      for (const r of arr) {
        if ("error" in r) {
          // ignore per-file hash errors in this pass
        } else {
          hashResults.push({ path: r.path, hash: r.hash, ctime: r.ctime });
          touchBatch.push([r.path, Date.now()]);
          // Emit delta line with full metadata if we have it
          const meta = pendingMeta.get(r.path);
          if (meta) {
            emitObj({
              path: r.path,
              size: meta.size,
              ctime: meta.ctime,
              mtime: meta.mtime,
              op_ts: meta.mtime,
              hash: r.hash,
              deleted: 0,
            });
            pendingMeta.delete(r.path);
          }
          if (hashResults.length >= DB_BATCH_SIZE) {
            applyHashBatch(hashResults);
            hashResults.length = 0;
            flushTouchBatch(touchBatch);
            flushDeltaBuf();
          }
        }
      }
    });
  }

  // --------------- Walk + incremental logic ---------------
  async function scan(root: string) {
    const t0 = Date.now();
    const scan_id = Date.now();

    const absRoot = path.resolve(root);
    const toR = (abs: string) => normalizeR(path.relative(absRoot, abs));

    // Load per-root ignore matcher (gitignore semantics)
    const ig = await loadIgnoreFile(absRoot);

    // stream entries with stats so we avoid a second stat in main thread
    const stream = walk.walkStream(absRoot, {
      stats: true,
      followSymbolicLinks: false,
      concurrency: 128,
      // Do not descend into ignored directories
      deepFilter: (e) => {
        if (e.dirent.isDirectory()) {
          const r = toR(e.path);
          return !ig.ignoresDir(r);
        }
        return true;
      },
      // Do not emit ignored directories/files/links as entries
      entryFilter: (e) => {
        const r = toR(e.path);
        if (e.dirent.isDirectory()) return !ig.ignoresDir(r);
        // For files & symlinks, ignore file-style rules
        return !ig.ignoresFile(r);
      },
      errorFilter: () => true,
    });

    // Periodic flush so we don't hold large arrays in RAM too long
    const periodicFlush = setInterval(() => {
      if (hashResults.length) {
        applyHashBatch(hashResults);
        hashResults.length = 0;
        flushTouchBatch(touchBatch);
      }
      flushDeltaBuf();
    }, 500).unref();

    // Mini-buffers
    const metaBuf: Row[] = [];
    const dirMetaBuf: DirRow[] = [];
    const linksBuf: LinkRow[] = [];
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
      dirent;
      path: string;
      stats: import("fs").Stats;
    }>) {
      const full = entry.path; // absolute
      const st = entry.stats!;
      const ctime = (st as any).ctimeMs ?? st.ctime.getTime();
      const mtime = (st as any).mtimeMs ?? st.mtime.getTime();
      const mode = (st as any).mode ?? 0;
      const uid = (st as any).uid ?? 0;
      const gid = (st as any).gid ?? 0;

      if (entry.dirent.isDirectory()) {
        // op_ts reflects metadata changes for dirs as well
        const op_ts = Date.now();
        dirMetaBuf.push({ path: full, ctime, mtime, mode, uid, gid, scan_id, op_ts });

        // emit-delta: dir ensure
        if (emitDelta) {
          // keep delta format minimal; we don't need to emit mode/uid/gid here
          // since merge uses DB state.
          // (Add if you want later.)
        }

        if (dirMetaBuf.length >= DB_BATCH_SIZE) {
          applyDirBatch(dirMetaBuf);
          dirMetaBuf.length = 0;
        }
      } else if (entry.dirent.isFile()) {
        const size = st.size;
        // Use max(mtime, ctime) so chmod/chown bumps LWW
        const op_ts = Math.max(mtime, ctime);
        // Upsert *metadata only* (no hash/hashed_ctime change here)
        metaBuf.push({
          path: full,
          size,
          ctime,
          mtime,
          mode,
          uid,
          gid,
          op_ts,
          hash: null,
          last_seen: scan_id,
          hashed_ctime: null,
        });

        if (metaBuf.length >= DB_BATCH_SIZE) {
          applyMetaBatch(metaBuf);
          metaBuf.length = 0;
        }

        // Decide if we need to hash: only when ctime changed since last time (or brand new)
        const row = getExisting.get(full);
        const needsHash = !row || row.hashed_ctime !== ctime;

        if (needsHash) {
          // remember minimal meta so we can emit a full delta row when the hash arrives
          pendingMeta.set(full, { size, ctime, mtime });
          jobBuf.push({ path: full, size, ctime, mtime });
          // Dispatch in batches to minimize IPC
          if (jobBuf.length >= DISPATCH_BATCH) {
            const w = await nextWorker();
            (w as any).postMessage({ jobs: jobBuf } as JobBatch);
            dispatched += jobBuf.length;
            jobBuf = [];
          }
        }
      } else if (entry.dirent.isSymbolicLink()) {
        let target = "";
        try {
          target = await readlink(full);
        } catch {}
        const ctime = (st as any).ctimeMs ?? st.ctime.getTime();
        const mtime = (st as any).mtimeMs ?? st.mtime.getTime();
        const op_ts = mtime; // LWW uses op_ts consistently
        const hash = xxh128String(target);

        linksBuf.push({
          path: full,
          target,
          ctime,
          mtime,
          op_ts,
          hash,
          scan_id,
        });

        // emit-delta for remote ingestion (optional)
        if (emitDelta) {
          // keep minimal; no need to include mode/uid/gid for links
        }

        if (linksBuf.length >= DB_BATCH_SIZE) {
          applyLinksBatch(linksBuf);
          linksBuf.length = 0;
        }
        continue;
      }
    }

    // Flush remaining meta
    if (metaBuf.length) {
      applyMetaBatch(metaBuf);
      metaBuf.length = 0;
    }

    // Flush remaining directories
    if (dirMetaBuf.length) {
      applyDirBatch(dirMetaBuf);
      dirMetaBuf.length = 0;
    }

    // Flush remaining links
    if (linksBuf.length) {
      applyLinksBatch(linksBuf);
      linksBuf.length = 0;
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

    // Final flush of hash results and touches
    if (hashResults.length) {
      applyHashBatch(hashResults);
      hashResults.length = 0;
      flushTouchBatch(touchBatch);
    }
    flushDeltaBuf();

    // Compute deletions (anything not seen this pass and not already deleted)
    const toDelete = db
      .prepare(`SELECT path FROM files WHERE last_seen <> ? AND deleted = 0`)
      .all(scan_id) as { path: string }[];

    const op_ts = Date.now();
    db.prepare(
      `UPDATE files
      SET deleted = 1, op_ts = ?
      WHERE last_seen <> ? AND deleted = 0`,
    ).run(op_ts, scan_id);

    // emit-delta: Emit deletions (optional/minimal)
    if (emitDelta && toDelete.length) {
      for (const _ of toDelete) {
        // minimal
      }
      flushDeltaBuf();
    }

    // Mark deletions: dirs
    db.prepare(
      `UPDATE dirs SET deleted=1, op_ts=? WHERE last_seen <> ? AND deleted = 0`,
    ).run(op_ts, scan_id);

    // Mark deletions: links
    const toDeleteLinks = db
      .prepare(`SELECT path FROM links WHERE last_seen <> ? AND deleted = 0`)
      .all(scan_id) as { path: string }[];

    const op_ts_links = Date.now();
    db.prepare(
      `UPDATE links SET deleted=1, op_ts=? WHERE last_seen <> ? AND deleted = 0`,
    ).run(op_ts_links, scan_id);

    if (emitDelta && toDeleteLinks.length) {
      flushDeltaBuf();
    }

    clearInterval(periodicFlush);
    await Promise.all(workers.map((w) => w.terminate()));

    if (verbose) {
      console.log(
        `Scan done: ${dispatched} hashed / ${received} results in ${Date.now() - t0} ms`,
      );
    }
  }

  await scan(root);
}

// ---------- CLI entry (preserved) ----------
cliEntrypoint<ScanOptions>(import.meta.url, buildProgram, runScan, {
  label: "scan",
});
