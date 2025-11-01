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
}

const defaultClock = () => Date.now();

const defaultEchoWriter: EchoWriter = (entry) => {
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

  constructor({
    scope,
    sink,
    echo,
    clock,
  }: LoggerOptions = {}) {
    this.scope = scope;
    this.sink = sink ?? (() => {});
    this.echoMinLevel = echo?.minLevel;
    this.echoWriter = echo?.writer ?? defaultEchoWriter;
    this.clock = clock ?? defaultClock;
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
}

export class ConsoleLogger extends StructuredLogger {
  constructor(minLevel: LogLevel = "info") {
    super({
      echo: { minLevel },
    });
  }
}
