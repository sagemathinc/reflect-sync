#!/usr/bin/env node
// src/ingest-delta.ts
//
// Read NDJSON deltas from stdin and mirror them into a local files table.
// Usage:
//   node dist/ingest-delta.js --db alpha.db --root /remote/alpha/root
//
// Example over SSH:
//   ssh -C user@alpha 'env DB_PATH=~/.cache/cocalc-sync/alpha.db node /path/dist/scan.js /alpha/root --emit-delta' \
//     | node dist/ingest-delta.js --db alpha.db --root /alpha/root
//
// NOTE: this looks like code duplication with scan.ts, but it's not, since
// there's a number of things that are completely different about how
// we're inserting data into the database here.

import readline from "node:readline";
import { getDb } from "./db.js";
import { Command } from "commander";
import { cliEntrypoint } from "./cli-util.js";

function buildProgram(): Command {
  const program = new Command()
    .name("ccsync-ingest-delta")
    .description("Ingest NDJSON deltas from stdin into a local files table");

  program
    .requiredOption("--db <path>", "sqlite db file")
    .option("--root <path>", "absolute root to accept")
    .option("--verbose", "enable verbose logging", false);

  return program;
}

export type IngestDeltaOptions = {
  db: string;
  verbose: boolean;
  root: string;
};

export async function runIngestDelta(opts: IngestDeltaOptions): Promise<void> {
  const { db: dbPath, verbose, root } = opts;

  // ---------- db ----------
  const db = getDb(dbPath);

  // used to update data about files
  const upsertFile = db.prepare(`
INSERT INTO files(path, size, ctime, mtime, op_ts, hash, deleted, last_seen, hashed_ctime)
VALUES (@path, @size, @ctime, @mtime, @op_ts, @hash, @deleted, @now, @hashed_ctime)
ON CONFLICT(path) DO UPDATE SET
  size=COALESCE(excluded.size, files.size),
  ctime=COALESCE(excluded.ctime, files.ctime),
  mtime=COALESCE(excluded.mtime, files.mtime),
  op_ts=COALESCE(excluded.op_ts, files.op_ts),
  -- Only update hash/hashed_ctime when a hash is provided
  hash=COALESCE(excluded.hash, files.hash),
  hashed_ctime=COALESCE(excluded.hashed_ctime, files.hashed_ctime),
  deleted=excluded.deleted,
  last_seen=excluded.last_seen
`);

  // used to update data about directories
  const upsertDir = db.prepare(`
INSERT INTO dirs(path, ctime, mtime, op_ts, deleted, last_seen)
VALUES (@path, @ctime, @mtime, @op_ts, @deleted, @now)
ON CONFLICT(path) DO UPDATE SET
  ctime=COALESCE(excluded.ctime, dirs.ctime),
  mtime=COALESCE(excluded.mtime, dirs.mtime),
  op_ts=COALESCE(excluded.op_ts, dirs.op_ts),
  deleted=excluded.deleted,
  last_seen=excluded.last_seen
`);

  const insTouch = db.prepare(
    `INSERT OR REPLACE INTO recent_touch(path, ts) VALUES (?, ?)`,
  );

  const tx = db.transaction((rows: any[]) => {
    if (verbose) {
      console.log(`ingest-delta: ${rows.length} rows`);
    }
    const now = Date.now();
    for (const r of rows) {
      // shape: {path, size?, ctime?, mtime?, hash?, deleted?}
      // normalize/guard
      if (root && !r.path.startsWith(root + "/") && r.path !== root) {
        continue;
      }
      const isDelete = r.deleted === 1;

      // [ ] TODO: need to adjust op_ts by clock skew:
      const op_ts = r.op_ts;

      if (r.kind === "dir") {
        upsertDir.run({
          path: r.path,
          ctime: isDelete ? null : (r.ctime ?? null),
          mtime: isDelete ? null : (r.mtime ?? null),
          op_ts,
          deleted: isDelete ? 1 : 0,
          now,
        });
      } else {
        // default to file (no kind), since most entries are files
        // so this saves space
        upsertFile.run({
          path: r.path,
          size: isDelete ? null : (r.size ?? null),
          ctime: isDelete ? null : (r.ctime ?? null),
          mtime: isDelete ? null : (r.mtime ?? null),
          op_ts,
          hash: isDelete ? null : (r.hash ?? null),
          deleted: isDelete ? 1 : 0,
          now,
          hashed_ctime: isDelete ? null : (r.ctime ?? null),
        });
        // consider this path as active
        insTouch.run(r.path, Date.now());
      }
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
}

cliEntrypoint<IngestDeltaOptions>(
  import.meta.url,
  buildProgram,
  runIngestDelta,
  {
    label: "ingest",
  },
);
