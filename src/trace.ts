// src/trace.ts
import path from "node:path";
import { mkdirSync } from "node:fs";
import { Database } from "./db.js";

const TRACE_ENABLED = Boolean(process.env.REFLECT_TRACE_ALL);

export type TraceContext = {
  label?: string;
  strategy?: string | null;
  prefer?: string;
  restrictedCount?: number;
  diffCount?: number;
};

export type TracePlanEntry = {
  path: string;
  operation: string;
  alpha?: Record<string, unknown> | null;
  beta?: Record<string, unknown> | null;
  base?: Record<string, unknown> | null;
};

export type TraceResultStatus = "success" | "failure";

export class TraceWriter {
  private db: Database;
  private insertStmt;
  private updateStmt;
  private pending = new Map<string, number>();
  private context: TraceContext;

  static maybeCreate(opts: {
    baseDb: string;
    traceDb?: string;
    context?: TraceContext;
  }): TraceWriter | null {
    if (!TRACE_ENABLED) return null;
    const traceDb =
      opts.traceDb ??
      path.join(path.dirname(opts.baseDb), "trace.db");
    mkdirSync(path.dirname(traceDb), { recursive: true });
    const db = new Database(traceDb);
    db.pragma("busy_timeout = 5000");
    db.pragma("journal_mode = WAL");
    db.pragma("synchronous = NORMAL");
    db.exec(`
      CREATE TABLE IF NOT EXISTS trace_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        label TEXT,
        strategy TEXT,
        prefer TEXT,
        restricted_count INTEGER,
        diff_count INTEGER,
        path TEXT NOT NULL,
        operation TEXT NOT NULL,
        status TEXT NOT NULL,
        alpha_state TEXT,
        beta_state TEXT,
        base_state TEXT,
        details TEXT
      );
      CREATE INDEX IF NOT EXISTS trace_entries_path_ts
        ON trace_entries(path, ts);
    `);
    return new TraceWriter(db, opts.context ?? {});
  }

  private constructor(db: Database, context: TraceContext) {
    this.db = db;
    this.context = context;
    this.insertStmt = this.db.prepare(`
      INSERT INTO trace_entries(
        ts, label, strategy, prefer, restricted_count, diff_count,
        path, operation, status, alpha_state, beta_state, base_state, details
      ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
    `);
    this.updateStmt = this.db.prepare(
      `UPDATE trace_entries SET status=?, details=? WHERE id=?`,
    );
  }

  recordPlan(entries: TracePlanEntry[], diffCount: number) {
    this.context.diffCount = diffCount;
    const now = Date.now();
    for (const entry of entries) {
      const key = this.makeKey(entry.path, entry.operation);
      const info = this.insertStmt.run(
        now,
        this.context.label ?? null,
        this.context.strategy ?? null,
        this.context.prefer ?? null,
        this.context.restrictedCount ?? null,
        diffCount,
        entry.path,
        entry.operation,
        "planned",
        entry.alpha ? JSON.stringify(entry.alpha) : null,
        entry.beta ? JSON.stringify(entry.beta) : null,
        entry.base ? JSON.stringify(entry.base) : null,
        null,
      );
      this.pending.set(key, Number(info.lastInsertRowid));
    }
  }

  recordResult(
    path: string,
    operation: string,
    status: TraceResultStatus,
    details?: Record<string, unknown>,
  ) {
    const key = this.makeKey(path, operation);
    const rowId = this.pending.get(key);
    if (!rowId) {
      // If we somehow missed plan entry, insert minimal row.
      this.insertStmt.run(
        Date.now(),
        this.context.label ?? null,
        this.context.strategy ?? null,
        this.context.prefer ?? null,
        this.context.restrictedCount ?? null,
        this.context.diffCount ?? null,
        path,
        operation,
        status,
        null,
        null,
        null,
        details ? JSON.stringify(details) : null,
      );
      return;
    }
    this.updateStmt.run(
      status,
      details ? JSON.stringify(details) : null,
      rowId,
    );
  }

  close() {
    this.db.close();
    this.pending.clear();
  }

  private makeKey(pathValue: string, op: string) {
    return `${pathValue}::${op}`;
  }
}

export function describeOperation(op: {
  op: "copy" | "delete" | "noop";
  from?: string;
  to?: string;
  side?: string;
}): string {
  if (op.op === "copy" && op.from && op.to) {
    return `copy ${op.from}->${op.to}`;
  }
  if (op.op === "delete" && op.side) {
    return `delete ${op.side}`;
  }
  if (op.op === "noop") return "noop";
  return op.op;
}

export function isTraceEnabled(): boolean {
  return TRACE_ENABLED;
}
