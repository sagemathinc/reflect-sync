import {
  ConsoleLogger,
  StructuredLogger,
  levelsAtOrAbove,
  type LogEntry,
  type LogLevel,
  type Logger,
} from "./logger.js";
import { ensureSessionDb, openSessionDb } from "./session-db.js";
import type { Database } from "./db.js";

const DEFAULT_KEEP_MS = envNumber(
  "DAEMON_LOG_KEEP_MS",
  7 * 24 * 60 * 60 * 1000,
);
const DEFAULT_KEEP_ROWS = envNumber("DAEMON_LOG_KEEP_ROWS", 100_000);

function envNumber(key: string, fallback: number): number {
  const raw = process.env[key];
  if (!raw) return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

export interface DaemonLogRow {
  id: number;
  ts: number;
  level: LogLevel;
  scope: string | null;
  message: string;
  meta: Record<string, unknown> | null;
}

export interface DaemonLogQuery {
  afterId?: number;
  sinceTs?: number;
  limit?: number;
  minLevel?: LogLevel;
  order?: "asc" | "desc";
  scope?: string;
  message?: string;
}

export interface DaemonLogStoreOptions {
  keepMs?: number;
  keepRows?: number;
}

class DaemonLogStore {
  private readonly insertStmt;
  private readonly pruneByTimeStmt;
  private readonly pruneByRowsStmt;
  private readonly keepMs: number;
  private readonly keepRows: number;

  private constructor(
    private readonly db: Database,
    { keepMs, keepRows }: DaemonLogStoreOptions = {},
  ) {
    this.keepMs = keepMs ?? DEFAULT_KEEP_MS;
    this.keepRows = keepRows ?? DEFAULT_KEEP_ROWS;

    this.insertStmt = this.db.prepare(
      `INSERT INTO daemon_logs(ts, level, scope, message, meta)
       VALUES (?, ?, ?, ?, ?)`,
    );
    this.pruneByTimeStmt = this.db.prepare(
      `DELETE FROM daemon_logs
        WHERE ts < ?`,
    );
    this.pruneByRowsStmt = this.db.prepare(
      `DELETE FROM daemon_logs
        WHERE id < COALESCE((
          SELECT id FROM daemon_logs
           ORDER BY id DESC
           LIMIT 1 OFFSET ?
        ), -1)`,
    );
  }

  static open(
    sessionDbPath: string,
    options?: DaemonLogStoreOptions,
  ): DaemonLogStore {
    return new DaemonLogStore(ensureSessionDb(sessionDbPath), options);
  }

  append(entry: LogEntry): void {
    const ts = entry.ts ?? Date.now();
    let metaJson: string | null = null;
    if (entry.meta) {
      metaJson = safeStringify(entry.meta);
    }
    this.insertStmt.run(
      ts,
      entry.level,
      entry.scope ?? null,
      entry.message,
      metaJson,
    );
    this.prune(ts);
  }

  close(): void {
    this.db.close();
  }

  private prune(now: number): void {
    if (this.keepMs > 0) {
      const cutoff = now - this.keepMs;
      this.pruneByTimeStmt.run(cutoff);
    }
    if (this.keepRows > 0) {
      const offset = Math.max(0, this.keepRows - 1);
      this.pruneByRowsStmt.run(offset);
    }
  }
}

export interface DaemonLoggerHandle {
  logger: Logger;
  close: () => void;
  store: DaemonLogStore;
}

export interface DaemonLoggerOptions {
  scope?: string;
  echoLevel?: LogLevel;
  keepMs?: number;
  keepRows?: number;
}

export function createDaemonLogger(
  sessionDbPath: string,
  options: DaemonLoggerOptions = {},
): DaemonLoggerHandle {
  const store = DaemonLogStore.open(sessionDbPath, {
    keepMs: options.keepMs,
    keepRows: options.keepRows,
  });
  const sink = (entry: LogEntry) => {
    try {
      store.append(entry);
    } catch (err) {
      new ConsoleLogger("warn").warn("failed to persist daemon log entry", {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  };
  const logger = new StructuredLogger({
    scope: options.scope,
    sink,
    echo: options.echoLevel
      ? {
          minLevel: options.echoLevel,
        }
      : undefined,
  });
  return {
    logger,
    store,
    close: () => store.close(),
  };
}

export function fetchDaemonLogs(
  sessionDbPath: string,
  {
    afterId,
    sinceTs,
    limit = 200,
    minLevel,
    order = "asc",
    scope,
    message,
  }: DaemonLogQuery = {},
): DaemonLogRow[] {
  const db = openSessionDb(sessionDbPath);
  try {
    const where: string[] = ["1 = 1"];
    const params: any[] = [];
    if (typeof afterId === "number" && Number.isFinite(afterId)) {
      where.push("id > ?");
      params.push(afterId);
    }
    if (typeof sinceTs === "number" && Number.isFinite(sinceTs)) {
      where.push("ts >= ?");
      params.push(sinceTs);
    }
    if (minLevel) {
      const levels = levelsAtOrAbove(minLevel);
      where.push(`level IN (${levels.map(() => "?").join(",")})`);
      params.push(...levels);
    }
    if (scope) {
      where.push("scope = ?");
      params.push(scope);
    }
    if (message) {
      where.push("message = ?");
      params.push(message);
    }

    const stmt = db.prepare(
      `SELECT id, ts, level, scope, message, meta
         FROM daemon_logs
        WHERE ${where.join(" AND ")}
        ORDER BY id ${order === "desc" ? "DESC" : "ASC"}
        LIMIT ?`,
    );
    const rows = stmt.all(...params, Math.max(1, limit)) as {
      id: number;
      ts: number;
      level: LogLevel;
      scope: string | null;
      message: string;
      meta: string | null;
    }[];
    return rows.map((row) => ({
      ...row,
      meta: parseMeta(row.meta),
    }));
  } finally {
    db.close();
  }
}

function safeStringify(obj: Record<string, unknown>): string {
  try {
    return JSON.stringify(obj);
  } catch {
    return JSON.stringify({ __error: "failed to serialize meta" });
  }
}

function parseMeta(meta: string | null): Record<string, unknown> | null {
  if (!meta) return null;
  try {
    return JSON.parse(meta);
  } catch {
    return { __error: "failed to parse meta JSON" };
  }
}
