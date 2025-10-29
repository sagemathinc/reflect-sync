#!/usr/bin/env node
// src/scan.ts
import { Worker } from "node:worker_threads";
import os from "node:os";
import * as walk from "@nodelib/fs.walk";
import { readlink } from "node:fs/promises";
import { getDb } from "./db.js";
import { Command } from "commander";
import { cliEntrypoint } from "./cli-util.js";
import { modeHash, xxh128String } from "./hash.js";
import path from "node:path";
import { loadIgnoreFile } from "./ignore.js";
import { toRel } from "./path-rel.js";
import { isRecent } from "./hotwatch.js";

function buildProgram(): Command {
  const program = new Command();

  return program
    .name("ccsync-scan")
    .description("Run a local scan writing to sqlite database")
    .requiredOption("--root <path>", "directory to scan")
    .requiredOption("--db <file>", "path to sqlite database")
    .option("--emit-delta", "emit NDJSON deltas to stdout for ingest", false)
    .option(
      "--emit-since-ts <milliseconds>",
      "when used with --emit-delta, first replay all rows (files/dirs/links) with op_ts >= this timestamp",
    )
    .option("--verbose", "enable verbose logging", false)
    .option("--vacuum", "vacuum the database after doing the scan", false)
    .option(
      "--prune-ms <milliseconds>",
      "prune deleted entries at least this old *before* doing the scan",
    );
}

type ScanOptions = {
  db: string;
  emitDelta: boolean;
  emitRecentMs?: string;
  verbose: boolean;
  root: string;
  vacuum?: boolean;
  pruneMs?: string;
};

export async function runScan(opts: ScanOptions): Promise<void> {
  const {
    root,
    db: DB_PATH,
    emitDelta,
    emitRecentMs,
    verbose,
    vacuum,
    pruneMs,
  } = opts;

  // Rows written to DB always use rpaths now.
  type Row = {
    path: string; // rpath
    size: number;
    ctime: number;
    mtime: number;
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

  const isRoot = process.geteuid?.() === 0;

  // ----------------- SQLite setup -----------------
  const db = getDb(DB_PATH);

  if (pruneMs) {
    const olderThanTs = Date.now() - Number(pruneMs);
    for (const table of ["files", "dirs", "links"]) {
      db.prepare(`DELETE FROM ${table} WHERE deleted=1 AND op_ts < ?`).run(
        olderThanTs,
      );
    }
  }

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

  // for directory metadata (paths are rpaths)
  const upsertDir = db.prepare(`
INSERT INTO dirs(path, ctime, mtime, op_ts, hash, deleted, last_seen)
VALUES (@path, @ctime, @mtime, @op_ts, @hash, 0, @scan_id)
ON CONFLICT(path) DO UPDATE SET
  ctime     = excluded.ctime,
  mtime     = excluded.mtime,
  deleted   = 0,
  last_seen = excluded.last_seen,
  hash      = excluded.hash,
  -- Preserve current op_ts unless we are resurrecting a previously-deleted dir
  op_ts     = CASE
                WHEN dirs.deleted = 1 THEN excluded.op_ts
                ELSE dirs.op_ts
              END
`);

  type DirRow = {
    path: string; // rpath
    ctime: number;
    mtime: number;
    op_ts: number;
    scan_id: number;
    hash: string;
  };

  const applyDirBatch = db.transaction((rows: DirRow[]) => {
    for (const r of rows) upsertDir.run(r);
  });

  // Files meta (paths are rpaths)
  const upsertMeta = db.prepare(`
INSERT INTO files (path, size, ctime, mtime, op_ts, hash, deleted, last_seen, hashed_ctime)
VALUES (@path, @size, @ctime, @mtime, @op_ts, @hash, 0, @last_seen, @hashed_ctime)
ON CONFLICT(path) DO UPDATE SET
  size=excluded.size,
  ctime=excluded.ctime,
  mtime=excluded.mtime,
  op_ts=excluded.op_ts,
  last_seen=excluded.last_seen,
  deleted=0
-- NOTE: we intentionally DO NOT overwrite hash or hashed_ctime here.
`);

  const applyMetaBatch = db.transaction((rows: Row[]) => {
    for (const r of rows) upsertMeta.run(r);
  });

  // Hashes (paths are rpaths)
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

  // Links (paths are rpaths)
  type LinkRow = {
    path: string; // rpath
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
    for (const r of rows) upsertLink.run(r);
  });

  // ----------------- Worker pool ------------------
  // Worker accepts ABS paths for hashing; we convert the results to rpaths here.
  type Job = { path: string; size: number; ctime: number; mtime: number }; // ABS path
  type JobBatch = { jobs: Job[] };
  type Result =
    | { path: string; hash: string; ctime: number; mtime: number } // ABS path echoed back
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

  // Buffer for hash results (rpaths) to batch DB writes
  const hashResults: { path: string; hash: string; ctime: number }[] = [];
  const touchBatch: [string, number][] = [];

  // Emit buffering (fewer stdout writes)
  const deltaBuf: string[] = [];
  const emitObj = (o: {
    kind?: "dir" | "link";
    path: string; // rpath
    op_ts: number;
    deleted: number;
    size?: number;
    ctime?: number;
    mtime?: number;
    hash?: string;
    target?: string; // for links
  }) => {
    if (!emitDelta) {
      throw Error("do not call emitObj if emitDelta isn't enabled");
    }
    deltaBuf.push(JSON.stringify(o));
    if (deltaBuf.length >= 1000) {
      flushDeltaBuf();
    }
  };

  function flushDeltaBuf() {
    if (!emitDelta || deltaBuf.length === 0) {
      return;
    }
    process.stdout.write(deltaBuf.join("\n") + "\n");
    deltaBuf.length = 0;
  }

  async function emitReplaySinceTs(dbSince: number) {
    if (!emitDelta) return;

    // Files (changed or deleted)
    const files = db
      .prepare(
        `
    SELECT path, size, ctime, mtime, op_ts, hash, deleted
    FROM files
    WHERE op_ts >= ?
    ORDER BY op_ts ASC, path ASC
  `,
      )
      .all(dbSince) as {
      path: string;
      size: number;
      ctime: number;
      mtime: number;
      op_ts: number;
      hash: string | null;
      deleted: number;
    }[];

    for (const r of files) {
      emitObj({
        path: r.path,
        size: r.size,
        ctime: r.ctime,
        mtime: r.mtime,
        op_ts: r.op_ts,
        hash: r.hash ?? undefined,
        deleted: r.deleted,
      });
    }
    flushDeltaBuf();

    // Dirs
    const dirs = db
      .prepare(
        `
    SELECT path, ctime, mtime, op_ts, hash, deleted
    FROM dirs
    WHERE op_ts >= ?
    ORDER BY op_ts ASC, path ASC
  `,
      )
      .all(dbSince) as {
      path: string;
      ctime: number;
      mtime: number;
      op_ts: number;
      hash: string;
      deleted: number;
    }[];

    for (const r of dirs) {
      emitObj({
        kind: "dir",
        path: r.path,
        ctime: r.ctime,
        mtime: r.mtime,
        op_ts: r.op_ts,
        hash: r.hash,
        deleted: r.deleted,
      });
    }
    flushDeltaBuf();

    // Links
    const links = db
      .prepare(
        `
    SELECT path, target, ctime, mtime, op_ts, hash, deleted
    FROM links
    WHERE op_ts >= ?
    ORDER BY op_ts ASC, path ASC
  `,
      )
      .all(dbSince) as {
      path: string;
      target: string;
      ctime: number;
      mtime: number;
      op_ts: number;
      hash: string;
      deleted: number;
    }[];

    for (const r of links) {
      emitObj({
        kind: "link",
        path: r.path,
        target: r.target,
        ctime: r.ctime,
        mtime: r.mtime,
        op_ts: r.op_ts,
        hash: r.hash,
        deleted: r.deleted,
      });
    }
    flushDeltaBuf();
  }

  // We keep meta keyed by ABS path (because worker replies with ABS),
  // then translate to rpath when emitting/applying results.
  const pendingMeta = new Map<
    string, // ABS
    { size: number; ctime: number; mtime: number }
  >();

  let dispatched = 0;
  let received = 0;

  const absRoot = path.resolve(root);

  // Handle worker replies (batched)
  for (const w of workers) {
    w.on("message", async (msg: { done?: Result[] }) => {
      freeWorkers.push(w);
      waiters.shift()?.();

      const arr = msg.done || [];
      received += arr.length;

      for (const r of arr) {
        if ("error" in r) {
          // ignore per-file hash errors in this pass
        } else {
          const rpath = toRel(r.path, absRoot);
          hashResults.push({ path: rpath, hash: r.hash, ctime: r.ctime });
          if (await isRecent(r.path, undefined, r.mtime)) {
            touchBatch.push([rpath, Date.now()]);
          }
          const meta = pendingMeta.get(r.path); // ABS key
          if (meta) {
            if (emitDelta) {
              emitObj({
                path: rpath,
                size: meta.size,
                ctime: meta.ctime,
                mtime: meta.mtime,
                op_ts: meta.mtime,
                hash: r.hash,
                deleted: 0,
              });
            }
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
  async function scan() {
    const t0 = Date.now();
    const scan_id = Date.now();

    // If requested, first replay a bounded window from the DB,
    // then proceed to the actual filesystem walk (which will emit new changes).
    if (emitDelta && emitRecentMs) {
      const ms = Number(emitRecentMs);
      if (Number.isFinite(ms) && ms > 0) {
        const since = Date.now() - ms;
        await emitReplaySinceTs(since);
      }
    }

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
          const r = toRel(e.path, absRoot);
          return !ig.ignoresDir(r);
        }
        return true;
      },
      // Do not emit ignored directories/files/links as entries
      entryFilter: (e) => {
        const r = toRel(e.path, absRoot);
        if (e.dirent.isDirectory()) {
          return !ig.ignoresDir(r);
        }
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

    // Existing-meta lookup by rpath
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

    // Existing-dir lookup by rpath (to decide whether to emit NDJSON)
    const getExistingDir = db.prepare<
      [string],
      | { ctime: number; mtime: number; hash: string; deleted: number }
      | undefined
    >(`SELECT ctime, mtime, hash, deleted FROM dirs WHERE path = ?`);

    // Existing-link lookup by rpath (to decide whether to emit NDJSON)
    const getExistingLink = db.prepare<
      [string],
      | {
          ctime: number;
          mtime: number;
          hash: string;
          target: string;
          deleted: number;
        }
      | undefined
    >(`SELECT ctime, mtime, hash, target, deleted FROM links WHERE path = ?`);

    for await (const entry of stream as AsyncIterable<{
      dirent;
      path: string; // ABS
      stats: import("fs").Stats;
    }>) {
      const abs = entry.path; // absolute on filesystem
      const rpath = toRel(abs, absRoot);
      const st = entry.stats!;
      const ctime = (st as any).ctimeMs ?? st.ctime.getTime();
      const mtime = (st as any).mtimeMs ?? st.mtime.getTime();

      if (entry.dirent.isDirectory()) {
        // directory ops don't bump mtime reliably, so we use op time.
        const op_ts = Date.now();
        let hash = modeHash(st.mode);
        if (isRoot) {
          hash += `|${st.uid}:${st.gid}`;
        }

        // Decide whether to emit NDJSON for this dir (delta-only)
        let dirChanged = true;
        if (emitDelta) {
          const prev = getExistingDir.get(rpath);
          dirChanged =
            !prev ||
            prev.deleted === 1 ||
            prev.mtime !== mtime ||
            prev.ctime !== ctime ||
            prev.hash !== hash;
        }

        // Always upsert to bump last_seen (so deletion detection works)
        dirMetaBuf.push({ path: rpath, ctime, mtime, hash, scan_id, op_ts });
        if (dirMetaBuf.length >= DB_BATCH_SIZE) {
          applyDirBatch(dirMetaBuf);
          dirMetaBuf.length = 0;
        }

        // Only emit if changed/new/resurrected
        if (emitDelta && dirChanged) {
          emitObj({
            kind: "dir",
            path: rpath,
            ctime,
            mtime,
            hash,
            op_ts,
            deleted: 0,
          });
        }

        continue;
      } else if (entry.dirent.isFile()) {
        const size = st.size;
        const op_ts = mtime;

        // Upsert *metadata only* (no hash/hashed_ctime change here)
        metaBuf.push({
          path: rpath,
          size,
          ctime,
          mtime,
          op_ts,
          hash: null,
          last_seen: scan_id,
          hashed_ctime: null,
        });

        if (metaBuf.length >= DB_BATCH_SIZE) {
          applyMetaBatch(metaBuf);
          metaBuf.length = 0;
        }

        // Decide if we need to hash: only when ctime changed (or brand new)
        const row = getExisting.get(rpath);
        const needsHash = !row || row.hashed_ctime !== ctime;

        if (needsHash) {
          // keep ABS key here; worker replies with ABS and we’ll map to rpath
          pendingMeta.set(abs, { size, ctime, mtime });
          jobBuf.push({ path: abs, size, ctime, mtime }); // ABS for worker
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
          target = await readlink(abs);
        } catch {}
        const op_ts = mtime; // LWW uses op_ts consistently
        const hash = xxh128String(target);

        // Decide whether to emit NDJSON for this link (delta-only)
        let linkChanged = true;
        if (emitDelta) {
          const prev = getExistingLink.get(rpath);
          linkChanged =
            !prev ||
            prev.deleted === 1 ||
            prev.mtime !== mtime ||
            prev.ctime !== ctime ||
            prev.hash !== hash ||
            prev.target !== target;
        }

        // Always upsert to bump last_seen
        linksBuf.push({
          path: rpath,
          target,
          ctime,
          mtime,
          op_ts,
          hash,
          scan_id,
        });
        if (linksBuf.length >= DB_BATCH_SIZE) {
          applyLinksBatch(linksBuf);
          linksBuf.length = 0;
        }

        // Only emit if changed/new/resurrected
        if (emitDelta && linkChanged) {
          emitObj({
            kind: "link",
            path: rpath,
            ctime,
            mtime,
            op_ts,
            hash,
            target,
            deleted: 0,
          });
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

    // emit-delta: file deletions
    if (emitDelta && toDelete.length) {
      for (const r of toDelete) {
        emitObj({ path: r.path, deleted: 1, op_ts });
      }
      flushDeltaBuf();
    }

    // Mark deletions: dirs
    const toDeleteDirs = db
      .prepare(`SELECT path FROM dirs WHERE last_seen <> ? AND deleted = 0`)
      .all(scan_id) as { path: string }[];

    const op_ts_dirs = Date.now();

    db.prepare(
      `UPDATE dirs SET deleted=1, op_ts=? WHERE last_seen <> ? AND deleted = 0`,
    ).run(op_ts_dirs, scan_id);

    // emit-delta: Emit dir deletions (use the snapshot we captured BEFORE the update)
    if (emitDelta && toDeleteDirs.length) {
      for (const r of toDeleteDirs) {
        emitObj({ kind: "dir", path: r.path, deleted: 1, op_ts: op_ts_dirs });
      }
      flushDeltaBuf();
    }

    // Mark deletions: links
    const toDeleteLinks = db
      .prepare(`SELECT path FROM links WHERE last_seen <> ? AND deleted = 0`)
      .all(scan_id) as { path: string }[];

    const op_ts_links = Date.now();
    db.prepare(
      `UPDATE links SET deleted=1, op_ts=? WHERE last_seen <> ? AND deleted = 0`,
    ).run(op_ts_links, scan_id);

    if (emitDelta && toDeleteLinks.length) {
      for (const r of toDeleteLinks) {
        emitObj({ kind: "link", path: r.path, deleted: 1, op_ts: op_ts_links });
      }
      flushDeltaBuf();
    }

    clearInterval(periodicFlush);
    await Promise.all(workers.map((w) => w.terminate()));

    if (vacuum) {
      db.exec("vacuum");
    }

    if (verbose) {
      console.log(
        `Scan done: ${dispatched} hashed / ${received} results in ${Date.now() - t0} ms`,
      );
    }
  }

  await scan();
}

// ---------- CLI entry (preserved) ----------
cliEntrypoint<ScanOptions>(import.meta.url, buildProgram, runScan, {
  label: "scan",
});
