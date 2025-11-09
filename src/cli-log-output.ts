import { fmtAgo } from "./session-status.js";
import { LOG_LEVELS, type LogLevel } from "./logger.js";

export interface LogCliRow {
  id: number;
  ts: number;
  level: LogLevel;
  scope: string | null;
  message: string;
  meta: Record<string, unknown> | null;
}

export function parseLogLevelOption(raw?: string): LogLevel | undefined {
  if (!raw) return undefined;
  const lvl = raw.toLowerCase();
  if (!LOG_LEVELS.includes(lvl as LogLevel)) {
    console.error(
      `invalid level '${raw}', expected one of ${LOG_LEVELS.join(", ")}`,
    );
    process.exit(1);
  }
  return lvl as LogLevel;
}

export function renderLogRows(
  rows: LogCliRow[],
  { json, absolute }: { json: boolean; absolute: boolean },
) {
  for (const row of rows) {
    if (json) {
      const payload = {
        id: row.id,
        ts: row.ts,
        level: row.level,
        scope: row.scope,
        message: row.message,
        meta: row.meta ?? null,
      };
      console.log(JSON.stringify(payload));
      continue;
    }
    const scope = row.scope ? ` [${row.scope}]` : "";
    const meta =
      row.meta && Object.keys(row.meta).length
        ? ` ${JSON.stringify(row.meta)}`
        : "";
    console.log(
      `(${absolute ? new Date(row.ts).toISOString() : fmtAgo(row.ts)}) ${row.level.toUpperCase()}${scope} ${row.message}${meta}`,
    );
  }
}
