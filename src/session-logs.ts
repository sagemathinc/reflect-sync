import {
  ConsoleLogger,
  type LogEntry,
  type LogLevel,
  type Logger,
  StructuredLogger,
  levelsAtOrAbove,
} from "./logger.js";
import { ensureSessionDb, openSessionDb } from "./session-db.js";
import type { Database } from "./db.js";

const DEFAULT_KEEP_MS = envNumber("SESSION_LOG_KEEP_MS", 7 * 24 * 60 * 60 * 1000);
const DEFAULT_KEEP_ROWS = envNumber("SESSION_LOG_KEEP_ROWS", 100_000);

function envNumber(key: string, fallback: number): number {
  const raw = process.env[key];
  if (!raw) return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

export interface SessionLogRow {
  id: number;
  session_id: number;
  ts: number;
  level: LogLevel;
  scope: string | null;
  message: string;
  meta: Record<string, unknown> | null;
}

export interface SessionLogQuery {
  afterId?: number;
  sinceTs?: number;
  limit?: number;
  minLevel?: LogLevel;
  order?: "asc" | "desc";
  scope?: string;
  message?: string;
}

export interface SessionLogStoreOptions {
  keepMs?: number;
  keepRows?: number;
}

export class SessionLogStore {
  private readonly db: Database;
  private readonly insertStmt;
  private readonly pruneByTimeStmt;
  private readonly pruneByRowsStmt;
  private readonly keepMs: number;
  private readonly keepRows: number;

  private constructor(
    db: Database,
    private readonly sessionId: number,
    { keepMs, keepRows }: SessionLogStoreOptions = {},
  ) {
    this.db = db;
    this.keepMs = keepMs ?? DEFAULT_KEEP_MS;
    this.keepRows = keepRows ?? DEFAULT_KEEP_ROWS;

    this.insertStmt = this.db.prepare(
      `INSERT INTO session_logs(session_id, ts, level, scope, message, meta)
       VALUES (?, ?, ?, ?, ?, ?)`,
    );
    this.pruneByTimeStmt = this.db.prepare(
      `DELETE FROM session_logs
        WHERE session_id = ?
          AND ts < ?`,
    );
    this.pruneByRowsStmt = this.db.prepare(
      `DELETE FROM session_logs
        WHERE session_id = ?
          AND id < COALESCE((
            SELECT id FROM session_logs
             WHERE session_id = ?
             ORDER BY id DESC
             LIMIT 1 OFFSET ?
          ), -1)`,
    );
  }

  static open(
    sessionDbPath: string,
    sessionId: number,
    options?: SessionLogStoreOptions,
  ): SessionLogStore {
    return new SessionLogStore(ensureSessionDb(sessionDbPath), sessionId, options);
  }

  append(entry: LogEntry): void {
    const ts = entry.ts ?? Date.now();
    let metaJson: string | null = null;
    if (entry.meta) {
      metaJson = safeStringify(entry.meta);
    }
    this.insertStmt.run(
      this.sessionId,
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
      this.pruneByTimeStmt.run(this.sessionId, cutoff);
    }
    if (this.keepRows > 0) {
      const offset = Math.max(0, this.keepRows - 1);
      this.pruneByRowsStmt.run(this.sessionId, this.sessionId, offset);
    }
  }
}

export interface SessionLoggerHandle {
  logger: Logger;
  close: () => void;
  store: SessionLogStore;
}

export interface SessionLoggerOptions {
  scope?: string;
  echoLevel?: LogLevel;
  keepMs?: number;
  keepRows?: number;
}

export function createSessionLogger(
  sessionDbPath: string,
  sessionId: number,
  options: SessionLoggerOptions = {},
): SessionLoggerHandle {
  const store = SessionLogStore.open(sessionDbPath, sessionId, {
    keepMs: options.keepMs,
    keepRows: options.keepRows,
  });
  const sink = (entry: LogEntry) => {
    try {
      store.append(entry);
    } catch (err) {
      // As a last resort, output to console; we do not rethrow to avoid cascading failures.
      new ConsoleLogger("warn").warn("failed to persist session log entry", {
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

export function fetchSessionLogs(
  sessionDbPath: string,
  sessionId: number,
  {
    afterId,
    sinceTs,
    limit = 200,
    minLevel,
    order = "asc",
    scope,
    message,
  }: SessionLogQuery = {},
): SessionLogRow[] {
  const db = openSessionDb(sessionDbPath);
  try {
    const where: string[] = ["session_id = ?"];
    const params: any[] = [sessionId];
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
      `SELECT id, session_id, ts, level, scope, message, meta
         FROM session_logs
        WHERE ${where.join(" AND ")}
        ORDER BY id ${order === "desc" ? "DESC" : "ASC"}
        LIMIT ?`,
    );
    const rows = stmt.all(...params, Math.max(1, limit)) as {
      id: number;
      session_id: number;
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
