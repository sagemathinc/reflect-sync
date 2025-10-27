#!/usr/bin/env node
// src/ingest-delta.ts
//
// Read NDJSON deltas (with RELATIVE paths) from stdin and mirror them into a
// local files/dirs/links tables.
//
// Usage:
//   node dist/ingest-delta.js --db alpha.db
//
// Example over SSH:
//   ssh -C user@alpha 'node /path/dist/scan.js --root /alpha/root --emit-delta' \
//     | node dist/ingest-delta.js --db alpha.db
//
// NOTE: Deltas must contain relative paths now. No --root filtering is needed.

import readline from "node:readline";
import { getDb } from "./db.js";
import { Command } from "commander";
import { cliEntrypoint } from "./cli-util.js";

// SAFETY_MS, FUTURE_SLACK_MS and CAP_BACKOFF_MS are for skew/future handling.
// See comments in the original version for rationale.
const SAFETY_MS = Number(process.env.CLOCK_SKEW_SAFETY_MS ?? 100);
const FUTURE_SLACK_MS = Number(process.env.FUTURE_SLACK_MS ?? 400);
const CAP_BACKOFF_MS = Number(process.env.FUTURE_CAP_BACKOFF_MS ?? 1);

function buildProgram(): Command {
  const program = new Command()
    .name("ccsync-ingest-delta")
    .description("Ingest NDJSON deltas from stdin into a local sqlite db");

  program.requiredOption("--db <path>", "sqlite db file");
  program.option("--verbose", "enable verbose logging", false);

  return program;
}

export type IngestDeltaOptions = {
  db: string;
  verbose: boolean;
};

export async function runIngestDelta(opts: IngestDeltaOptions): Promise<void> {
  const { db: dbPath, verbose } = opts;

  let skew: number | null = null; // local time = remote time + skew
  const nowLocal = () => Date.now();

  // Per-path monotonic timestamp (prevents going backwards when lines reorder)
  const lastByPath = new Map<string, number>();
  function monotonicFor(path: string, t: number) {
    const last = lastByPath.get(path) ?? -Infinity;
    const next = t <= last ? last + 1 : t;
    lastByPath.set(path, next);
    return next;
  }

  const adjustRemoteTime = (ts: number | undefined) => {
    const n = nowLocal();
    const base = Number.isFinite(ts as any) ? Number(ts) : n;
    let t = base + (skew ?? 0);
    // Cap far-future timestamps (allow a little slack)
    if (t > n + FUTURE_SLACK_MS) {
      t = n - CAP_BACKOFF_MS;
    }
    return t;
  };

  // ---------- db ----------
  const db = getDb(dbPath);

  // Files: upsert minimal metadata; only overwrite hash/hashed_ctime when provided
  const upsertFile = db.prepare(`
INSERT INTO files(path, size, ctime, mtime, op_ts, hash, deleted, last_seen, hashed_ctime)
VALUES (@path, @size, @ctime, @mtime, @op_ts, @hash, @deleted, @now, @hashed_ctime)
ON CONFLICT(path) DO UPDATE SET
  size=COALESCE(excluded.size, files.size),
  ctime=COALESCE(excluded.ctime, files.ctime),
  mtime=COALESCE(excluded.mtime, files.mtime),
  op_ts=COALESCE(excluded.op_ts, files.op_ts),
  hash=COALESCE(excluded.hash, files.hash),
  hashed_ctime=COALESCE(excluded.hashed_ctime, files.hashed_ctime),
  deleted=excluded.deleted,
  last_seen=excluded.last_seen
`);

  // Directories: presence/meta only (hash typically carries mode bits etc.)
  const upsertDir = db.prepare(`
INSERT INTO dirs(path, ctime, mtime, op_ts, hash, deleted, last_seen)
VALUES (@path, @ctime, @mtime, @op_ts, @hash, @deleted, @now)
ON CONFLICT(path) DO UPDATE SET
  ctime=COALESCE(excluded.ctime, dirs.ctime),
  mtime=COALESCE(excluded.mtime, dirs.mtime),
  op_ts=COALESCE(excluded.op_ts, dirs.op_ts),
  hash=excluded.hash,
  deleted=excluded.deleted,
  last_seen=excluded.last_seen
`);

  // Symlinks: store target + hash of target string for change detection
  const upsertLink = db.prepare(`
INSERT INTO links(path, target, ctime, mtime, op_ts, hash, deleted, last_seen)
VALUES (@path, @target, @ctime, @mtime, @op_ts, @hash, @deleted, @now)
ON CONFLICT(path) DO UPDATE SET
  target=COALESCE(excluded.target, links.target),
  ctime=COALESCE(excluded.ctime, links.ctime),
  mtime=COALESCE(excluded.mtime, links.mtime),
  op_ts=COALESCE(excluded.op_ts, links.op_ts),
  hash=COALESCE(excluded.hash, links.hash),
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
      if (r.kind === "time") {
        // special event conveying the remote "now" for skew calc
        const remoteNow = Number(r.remote_now_ms);
        if (Number.isFinite(remoteNow)) {
          // Upper-bound skew (includes network delay), minus safety if remote is behind
          let s = nowLocal() - remoteNow;
          if (s > 0) {
            s = Math.max(0, s - SAFETY_MS);
          }
          skew = s;
        }
        continue;
      }

      // All paths are RELATIVE now; just ingest.
      const isDelete = r.deleted === 1;
      const op_ts = monotonicFor(r.path, adjustRemoteTime(r.op_ts));
      if (r.path == null) {
        throw Error(`invalid data -- ${JSON.stringify(r)}`);
      }

      if (r.kind === "dir") {
        upsertDir.run({
          path: r.path,
          ctime: isDelete ? null : (r.ctime ?? null),
          mtime: isDelete ? null : (r.mtime ?? null),
          hash: r.hash ?? null,
          op_ts,
          deleted: isDelete ? 1 : 0,
          now,
        });
      } else if (r.kind === "link") {
        upsertLink.run({
          path: r.path,
          target: isDelete ? null : (r.target ?? null),
          ctime: isDelete ? null : (r.ctime ?? null),
          mtime: isDelete ? null : (r.mtime ?? null),
          op_ts,
          hash: isDelete ? null : (r.hash ?? null),
          deleted: isDelete ? 1 : 0,
          now,
        });
      } else {
        // default: file row
        upsertFile.run({
          path: r.path,
          size: isDelete ? null : (r.size ?? null),
          ctime: isDelete ? null : (r.ctime ?? null),
          mtime: isDelete ? null : (r.mtime ?? null),
          op_ts,
          hash: isDelete ? null : (r.hash ?? null),
          deleted: isDelete ? 1 : 0,
          now,
          // When scan emits file deltas, it includes a hash; we can safely set hashed_ctime.
          // If hash is missing, hashed_ctime should be null so COALESCE keeps the old one.
          hashed_ctime: isDelete ? null : r.hash ? (r.ctime ?? null) : null,
        });
        // consider this path as active
        insTouch.run(r.path, now);
      }
    }
  });

  let buf: any[] = [];
  const BATCH = 5000;
  function flush() {
    if (!buf.length) return;
    tx(buf);
    buf.length = 0;
  }

  const rl = readline.createInterface({ input: process.stdin });
  rl.on("line", (line) => {
    if (!line) return;
    try {
      const r = JSON.parse(line);
      buf.push(r);
      if (buf.length >= BATCH) {
        flush();
      }
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
  { label: "ingest" },
);
