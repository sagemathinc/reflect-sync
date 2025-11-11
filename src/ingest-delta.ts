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
}

export async function runIngestDelta(opts: IngestDeltaOptions): Promise<void> {
  const {
    db: dbPath,
    logger: providedLogger,
    logLevel = "info",
    input = process.stdin,
    abortSignal,
  } = opts;
  const logger = providedLogger ?? new ConsoleLogger(logLevel);

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
  };
  const nodeUpsertStmt = db.prepare(`
        INSERT INTO nodes(path, kind, hash, mtime, ctime, hashed_ctime, updated, size, deleted, last_seen, link_target, last_error)
        VALUES (@path, @kind, @hash, @mtime, @ctime, @hashed_ctime, @updated, @size, @deleted, @last_seen, @link_target, @last_error)
        ON CONFLICT(path) DO UPDATE SET
          kind=excluded.kind,
          hash=excluded.hash,
          mtime=excluded.mtime,
          ctime=excluded.ctime,
          hashed_ctime=excluded.hashed_ctime,
          updated=excluded.updated,
          size=excluded.size,
          deleted=excluded.deleted,
          last_seen=excluded.last_seen,
          link_target=excluded.link_target,
          last_error=excluded.last_error
      `);
  const writeNode = (params: NodeWriteParams) => {
    const updated = params.updated ?? Date.now();
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
      last_seen: params.last_seen ?? null,
      link_target: params.link_target ?? null,
      last_error: params.last_error === undefined ? null : params.last_error,
    });
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
        writeNode({
          path: r.path,
          kind: "d",
          hash: r.hash ?? "",
          mtime: isDelete ? now : r.mtime ?? op_ts,
          ctime: r.ctime ?? r.mtime ?? op_ts,
          size: 0,
          deleted: isDelete ? 1 : 0,
          last_seen: now,
          updated: op_ts,
          link_target: null,
          last_error: null,
        });
      } else if (r.kind === "link") {
        const target = r.target ?? "";
        writeNode({
          path: r.path,
          kind: "l",
          hash: r.hash ?? "",
          mtime: isDelete ? now : r.mtime ?? op_ts,
          ctime: r.ctime ?? r.mtime ?? op_ts,
          size: Buffer.byteLength(isDelete ? "" : target, "utf8"),
          deleted: isDelete ? 1 : 0,
          last_seen: now,
          updated: op_ts,
          link_target: isDelete ? null : target,
          last_error: null,
        });
      } else {
        if (isDelete) {
          writeNode({
            path: r.path,
            kind: "f",
            hash: "",
            mtime: now,
            ctime: r.ctime ?? now,
            hashed_ctime: null,
            size: 0,
            deleted: 1,
            last_seen: now,
            updated: op_ts,
            link_target: null,
            last_error: null,
          });
          insTouch.run(r.path, now);
        } else if (r.hash == null) {
          insTouch.run(r.path, now);
        } else {
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
            updated: op_ts,
            link_target: null,
            last_error: null,
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
