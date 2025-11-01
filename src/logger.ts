import { inspect } from "node:util";

export type LogLevel = "debug" | "info" | "warn" | "error";
export const LOG_LEVELS: LogLevel[] = ["debug", "info", "warn", "error"];

const LEVEL_ORDER: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

export function levelsAtOrAbove(minLevel: LogLevel): LogLevel[] {
  const threshold = LEVEL_ORDER[minLevel];
  return LOG_LEVELS.filter((lvl) => LEVEL_ORDER[lvl] >= threshold);
}

export interface LogEntry {
  ts: number;
  level: LogLevel;
  scope?: string;
  message: string;
  meta?: Record<string, unknown> | null;
}

export interface Logger {
  child(scope: string): Logger;
  log(level: LogLevel, message: string, meta?: Record<string, unknown>): void;
  debug(message: string, meta?: Record<string, unknown>): void;
  info(message: string, meta?: Record<string, unknown>): void;
  warn(message: string, meta?: Record<string, unknown>): void;
  error(message: string, meta?: Record<string, unknown>): void;
  isLevelEnabled(level: LogLevel): boolean;
}

type Sink = (entry: LogEntry) => void;
type EchoWriter = (entry: LogEntry) => void;

export interface LoggerOptions {
  scope?: string;
  sink?: Sink;
  echo?: {
    minLevel?: LogLevel;
    writer?: EchoWriter;
  };
  clock?: () => number;
  isLevelEnabled?: (level: LogLevel) => boolean;
}

const defaultClock = () => Date.now();

function isEchoSuppressed(): boolean {
  const raw = process.env.REFLECT_DISABLE_LOG_ECHO;
  if (!raw) return false;
  const normalized = raw.trim().toLowerCase();
  if (!normalized) return false;
  return normalized !== "0" && normalized !== "false";
}

const defaultEchoWriter: EchoWriter = (entry) => {
  if (isEchoSuppressed()) return;
  const { level, scope, message, meta } = entry;
  const prefix =
    level === "error"
      ? "⛔"
      : level === "warn"
        ? "⚠️"
        : level === "info"
          ? "ℹ️"
          : "·";
  const scopeText = scope ? `[${scope}] ` : "";
  if (meta && Object.keys(meta).length) {
    console.error(`${prefix} ${scopeText}${message}`, serializeMeta(meta));
  } else {
    console.error(`${prefix} ${scopeText}${message}`);
  }
};

function serializeMeta(meta: Record<string, unknown>): string {
  try {
    return JSON.stringify(meta);
  } catch {
    return inspect(meta, { depth: 4 });
  }
}

export class StructuredLogger implements Logger {
  private readonly sink: Sink;
  private readonly echoMinLevel?: LogLevel;
  private readonly echoWriter: EchoWriter;
  private readonly clock: () => number;
  private readonly scope?: string;
  private readonly enabled?: (level: LogLevel) => boolean;

  constructor({
    scope,
    sink,
    echo,
    clock,
    isLevelEnabled,
  }: LoggerOptions = {}) {
    this.scope = scope;
    this.sink = sink ?? (() => {});
    this.echoMinLevel = echo?.minLevel;
    this.echoWriter = echo?.writer ?? defaultEchoWriter;
    this.clock = clock ?? defaultClock;
    this.enabled = isLevelEnabled;
  }

  child(scope: string): Logger {
    const childScope = this.scope ? `${this.scope}.${scope}` : scope;
    return new StructuredLogger({
      scope: childScope,
      sink: this.sink,
      echo: this.echoMinLevel
        ? { minLevel: this.echoMinLevel, writer: this.echoWriter }
        : undefined,
      clock: this.clock,
      isLevelEnabled: this.enabled,
    });
  }

  log(level: LogLevel, message: string, meta?: Record<string, unknown>): void {
    const entry: LogEntry = {
      ts: this.clock(),
      level,
      scope: this.scope,
      message,
      meta: meta && Object.keys(meta).length ? meta : undefined,
    };
    this.sink(entry);
    if (this.echoMinLevel && shouldEcho(level, this.echoMinLevel)) {
      this.echoWriter(entry);
    }
  }

  debug(message: string, meta?: Record<string, unknown>): void {
    this.log("debug", message, meta);
  }

  info(message: string, meta?: Record<string, unknown>): void {
    this.log("info", message, meta);
  }

  warn(message: string, meta?: Record<string, unknown>): void {
    this.log("warn", message, meta);
  }

  error(message: string, meta?: Record<string, unknown>): void {
    this.log("error", message, meta);
  }

  isLevelEnabled(_level: LogLevel): boolean {
    return this.enabled ? this.enabled(_level) : true;
  }
}

function shouldEcho(level: LogLevel, minLevel: LogLevel): boolean {
  return LEVEL_ORDER[level] >= LEVEL_ORDER[minLevel];
}

export class NullLogger implements Logger {
  child(): Logger {
    return this;
  }
  log(): void {}
  debug(): void {}
  info(): void {}
  warn(): void {}
  error(): void {}
  isLevelEnabled(): boolean {
    return false;
  }
}

export class ConsoleLogger extends StructuredLogger {
  private readonly minLevel: LogLevel;

  constructor(minLevel: LogLevel = "info") {
    super({
      echo: { minLevel },
      isLevelEnabled: (level) => shouldEcho(level, minLevel),
    });
    this.minLevel = minLevel;
  }

  isLevelEnabled(level: LogLevel): boolean {
    return shouldEcho(level, this.minLevel);
  }
}

export function parseLogLevel(
  raw: string | undefined,
  fallback: LogLevel = "info",
): LogLevel {
  if (!raw) return fallback;
  const normalized = raw.trim().toLowerCase();
  return LOG_LEVELS.includes(normalized as LogLevel)
    ? (normalized as LogLevel)
    : fallback;
}

export function loggerFromLevel(level?: string): ConsoleLogger {
  return new ConsoleLogger(parseLogLevel(level));
}

export function levelAtOrAbove(
  desired: LogLevel,
  candidate: LogLevel,
): boolean {
  return LEVEL_ORDER[candidate] >= LEVEL_ORDER[desired];
}

export function minLevelEnabled(
  logger: Logger | undefined,
  level: LogLevel,
): boolean {
  if (!logger) return false;
  return logger.isLevelEnabled(level);
}

export function childOrSelf(
  logger: Logger | undefined,
  scope: string,
): Logger | undefined {
  if (!logger) return undefined;
  return logger.child(scope);
}
