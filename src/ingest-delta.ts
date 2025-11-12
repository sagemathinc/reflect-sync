#!/usr/bin/env node
// src/ingest-delta.ts
//
// Read NDJSON deltas (with RELATIVE paths) from stdin and mirror them into the
// local nodes table.
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
import { CLI_NAME } from "./constants.js";
import { ConsoleLogger, type Logger, type LogLevel } from "./logger.js";
import { deletionMtimeFromMeta } from "./nodes-util.js";
import type { LogicalClock } from "./logical-clock.js";

// SAFETY_MS, FUTURE_SLACK_MS and CAP_BACKOFF_MS are for skew/future handling.
// See comments in the original version for rationale.
const SAFETY_MS = Number(process.env.REFLECT_CLOCK_SKEW_SAFETY_MS ?? 100);
const FUTURE_SLACK_MS = Number(process.env.REFLECT_FUTURE_SLACK_MS ?? 400);
const CAP_BACKOFF_MS = Number(process.env.REFLECT_FUTURE_CAP_BACKOFF_MS ?? 1);

function buildProgram(): Command {
  const program = new Command()
    .name(`${CLI_NAME}-ingest-delta`)
    .description("Ingest NDJSON deltas from stdin into a local sqlite db");

  program.requiredOption("--db <path>", "sqlite db file");
  return program;
}

export interface IngestDeltaOptions {
  db: string;
  logger?: Logger;
  logLevel?: LogLevel;
  input?: NodeJS.ReadableStream;
  abortSignal?: AbortSignal;
  logicalClock?: LogicalClock;
  batchTick?: number;
}

export async function runIngestDelta(opts: IngestDeltaOptions): Promise<void> {
  const {
    db: dbPath,
    logger: providedLogger,
    logLevel = "info",
    input = process.stdin,
    abortSignal,
    logicalClock,
    batchTick,
  } = opts;
  const logger = providedLogger ?? new ConsoleLogger(logLevel);
  const clock = logicalClock;
  let standaloneClock = Date.now();
  const fixedTick = batchTick ?? null;
  const nextUpdatedTick = () => {
    if (fixedTick != null) return fixedTick;
    if (clock) return clock.next();
    const now = Date.now();
    const next = Math.max(now, standaloneClock + 1);
    standaloneClock = next;
    return next;
  };

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
  if (!clock) {
    try {
      const row = db
        .prepare(`SELECT MAX(updated) AS max_updated FROM nodes`)
        .get() as { max_updated?: number };
      if (row && Number.isFinite(row.max_updated)) {
        standaloneClock = Math.max(standaloneClock, Number(row.max_updated));
      }
    } catch {
      // ignore
    }
  }
  type NodeKind = "f" | "d" | "l";
  type NodeWriteParams = {
    path: string;
    kind: NodeKind;
    hash: string;
    mtime: number;
    ctime?: number;
    hashed_ctime?: number | null;
    size: number;
    deleted: 0 | 1;
    last_seen?: number | null;
    link_target?: string | null;
    last_error?: string | null;
    updated?: number;
    hash_pending?: number;
    change_start?: number | null;
    change_end?: number | null;
  };
  const nodeUpsertStmt = db.prepare(`
        INSERT INTO nodes(path, kind, hash, mtime, ctime, hashed_ctime, updated, size, deleted, hash_pending, change_start, change_end, last_seen, link_target, last_error)
        VALUES (@path, @kind, @hash, @mtime, @ctime, @hashed_ctime, @updated, @size, @deleted, @hash_pending, @change_start, @change_end, @last_seen, @link_target, @last_error)
        ON CONFLICT(path) DO UPDATE SET
          kind=excluded.kind,
          hash=excluded.hash,
          mtime=excluded.mtime,
          ctime=excluded.ctime,
          hashed_ctime=excluded.hashed_ctime,
          updated=excluded.updated,
          size=excluded.size,
          deleted=excluded.deleted,
          hash_pending=excluded.hash_pending,
          change_start=excluded.change_start,
          change_end=excluded.change_end,
          last_seen=excluded.last_seen,
          link_target=excluded.link_target,
          last_error=excluded.last_error
      `);
  const writeNode = (params: NodeWriteParams) => {
    const updated = params.updated ?? nextUpdatedTick();
    const ctime = params.ctime ?? params.mtime;
    nodeUpsertStmt.run({
      path: params.path,
      kind: params.kind,
      hash: params.hash,
      mtime: params.mtime,
      ctime,
      hashed_ctime: params.hashed_ctime ?? null,
      updated,
      size: params.size,
      deleted: params.deleted,
      hash_pending: params.hash_pending ?? 0,
      change_start:
        params.change_start === undefined ? null : params.change_start,
      change_end: params.change_end === undefined ? null : params.change_end,
      last_seen: params.last_seen ?? null,
      link_target: params.link_target ?? null,
      last_error: params.last_error === undefined ? null : params.last_error,
    });
  };
  type ExistingMetaRow = {
    kind: NodeKind;
    last_seen: number | null;
    updated: number | null;
    mtime: number | null;
    change_start?: number | null;
    change_end?: number | null;
  };
  const selectMetaStmt = db.prepare(
    `SELECT kind, last_seen, updated, mtime, change_start, change_end FROM nodes WHERE path = ?`,
  );
  const buildDeleteParams = (
    path: string,
    kindHint: NodeKind,
    fallbackNow: number,
  ): NodeWriteParams => {
    const existing = selectMetaStmt.get(path) as ExistingMetaRow | undefined;
    const deleteMtime = deletionMtimeFromMeta(existing ?? {}, fallbackNow);
    const logicalUpdated = nextUpdatedTick();
    const rawStart =
      existing?.change_start ??
      existing?.change_end ??
      existing?.updated ??
      fallbackNow;
    const changeStart =
      rawStart == null ? logicalUpdated : Math.min(rawStart, logicalUpdated);
    return {
      path,
      kind: existing?.kind ?? kindHint,
      hash: "",
      mtime: deleteMtime,
      ctime: deleteMtime,
      hashed_ctime: null,
      size: 0,
      deleted: 1,
      last_seen: existing?.last_seen ?? null,
      updated: logicalUpdated,
      link_target: null,
      last_error: null,
      hash_pending: 0,
      change_start: changeStart,
      change_end: logicalUpdated,
    };
  };
  let processedRows = 0;
  let closed = false;
  logger.info("ingest start", { db: dbPath });

  const insTouch = db.prepare(
    `INSERT OR REPLACE INTO recent_touch(path, ts) VALUES (?, ?)`,
  );

  const tx = db.transaction((rows: any[]) => {
    if (!rows.length) return;
    processedRows += rows.length;
    logger.debug("ingest batch", { rows: rows.length });
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
        if (isDelete) {
          writeNode(buildDeleteParams(r.path, "d", now));
        } else {
          const updatedTick = nextUpdatedTick();
          writeNode({
            path: r.path,
            kind: "d",
            hash: r.hash ?? "",
            mtime: r.mtime ?? op_ts,
            ctime: r.ctime ?? r.mtime ?? op_ts,
            size: 0,
            deleted: 0,
            last_seen: now,
            updated: updatedTick,
            link_target: null,
            last_error: null,
            hash_pending: 0,
            change_start: op_ts,
            change_end: op_ts,
          });
        }
      } else if (r.kind === "link") {
        const target = r.target ?? "";
        if (isDelete) {
          writeNode(buildDeleteParams(r.path, "l", now));
        } else {
          const updatedTick = nextUpdatedTick();
          writeNode({
            path: r.path,
            kind: "l",
            hash: r.hash ?? "",
            mtime: r.mtime ?? op_ts,
            ctime: r.ctime ?? r.mtime ?? op_ts,
            size: Buffer.byteLength(target, "utf8"),
            deleted: 0,
            last_seen: now,
            updated: updatedTick,
            link_target: target,
            last_error: null,
            hash_pending: 0,
            change_start: op_ts,
            change_end: op_ts,
          });
        }
      } else {
        if (isDelete) {
          writeNode({
            ...buildDeleteParams(r.path, "f", now),
          });
          insTouch.run(r.path, now);
        } else if (r.hash == null) {
          insTouch.run(r.path, now);
        } else {
          const updatedTick = nextUpdatedTick();
          writeNode({
            path: r.path,
            kind: "f",
            hash: r.hash,
            mtime: r.mtime ?? op_ts,
            ctime: r.ctime ?? r.mtime ?? op_ts,
            hashed_ctime: r.ctime ?? null,
            size: r.size ?? 0,
            deleted: 0,
            last_seen: now,
            updated: updatedTick,
            link_target: null,
            last_error: null,
            hash_pending: 0,
            change_start: op_ts,
            change_end: updatedTick,
          });
          insTouch.run(r.path, now);
        }
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

  const finalize = () => {
    if (closed) return;
    closed = true;
    logger.info("ingest complete", { rows: processedRows, db: dbPath });
    db.close();
  };

  await new Promise<void>((resolve, reject) => {
    const rl = readline.createInterface({ input });

    function handleLine(line: string) {
      if (!line) return;
      try {
        const r = JSON.parse(line);
        buf.push(r);
        if (buf.length >= BATCH) {
          flush();
        }
      } catch (err) {
        logger.warn("ingest skipped malformed line", {
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    function handleError(err: unknown) {
      rl.removeListener("line", handleLine);
      rl.removeAllListeners();
      cleanup();
      finalize();
      reject(err instanceof Error ? err : new Error(String(err)));
    }

    function handleClose() {
      rl.removeListener("line", handleLine);
      rl.removeAllListeners();
      cleanup();
      try {
        flush();
        finalize();
        resolve();
      } catch (err) {
        reject(err instanceof Error ? err : new Error(String(err)));
      }
    }

    function handleSigint() {
      cleanup();
      flush();
      finalize();
      process.exit(0);
    }

    function onAbort() {
      cleanup();
      rl.close();
      reject(new Error("ingest aborted"));
    }

    function cleanup() {
      input.off("error", handleError);
      rl.off("close", handleClose);
      if (input === process.stdin) {
        process.off("SIGINT", handleSigint);
      }
      if (abortSignal) {
        abortSignal.removeEventListener("abort", onAbort);
      }
    }

    rl.on("line", handleLine);
    rl.once("close", handleClose);
    input.once("error", handleError);

    if (input === process.stdin) {
      process.on("SIGINT", handleSigint);
    }

    if (abortSignal) {
      if (abortSignal.aborted) {
        onAbort();
      } else {
        abortSignal.addEventListener("abort", onAbort);
      }
    }
  });
}

cliEntrypoint<IngestDeltaOptions>(
  import.meta.url,
  buildProgram,
  runIngestDelta,
  { label: "ingest" },
);
