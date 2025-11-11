#!/usr/bin/env node
// src/cli.ts (ESM, TypeScript)
import fs from "node:fs";
import path from "node:path";
import { Command } from "commander";
import { registerSessionCommands } from "./session-cli.js";
import { registerForwardCommands } from "./forward-cli.js";
import { CLI_NAME } from "./constants.js";
import { getSessionDbPath } from "./session-db.js";
import {
  ConsoleLogger,
  LOG_LEVELS,
  parseLogLevel,
  type LogLevel,
} from "./logger.js";
import pkg from "../package.json" with { type: "json" };
import { registerInstallCommand } from "./install-cli.js";
import { configureScanCommand } from "./scan.js";
import { configureSchedulerCommand } from "./scheduler.js";
import { configureWatchCommand } from "./watch.js";

if (!process.env.REFLECT_ENTRY) {
  const entry = process.argv[1];
  if (entry) {
    try {
      if (fs.existsSync(entry)) {
        process.env.REFLECT_ENTRY = path.resolve(entry);
      }
    } catch {
      // ignore fs errors; we will fall back to process.execPath when spawning
    }
  }
}

function resolveLogLevel(command: Command): LogLevel {
  const raw = (command.optsWithGlobals() as any)?.logLevel as
    | string
    | undefined;
  return parseLogLevel(raw, "info");
}

function mergeOptsWithLogger<T extends Record<string, unknown>>(
  command: Command,
  opts: T,
) {
  const globals = command.optsWithGlobals() as Record<string, unknown> & {
    logLevel?: string;
    advanced?: boolean;
  };
  const { logLevel: _, advanced: __, ...restGlobals } = globals;
  const level = resolveLogLevel(command);
  const logger = new ConsoleLogger(level);
  return { ...restGlobals, ...opts, logger, logLevel: level };
}

const program = new Command()
  .name(CLI_NAME)
  .description("Fast rsync-powered two-way sync with SQLite metadata and SSH")
  .version(pkg.version);

// Global flags you want available everywhere
program
  .option(
    "--log-level <level>",
    `log verbosity (${LOG_LEVELS.join(", ")})`,
    "info",
  )
  .option(
    "--session-db <file>",
    "override path to sessions.db",
    getSessionDbPath(),
  )
  .option(
    "-A, --advanced",
    "show advanced plumbing commands in help output",
    false,
  )
  .option("--dry-run", "do not modify files", false);

registerSessionCommands(program);
registerForwardCommands(program);
registerInstallCommand(program);

const ADVANCED_COMMANDS = new Set([
  "scan",
  "ingest",
  "merge",
  "scheduler",
  "watch",
]);

const shouldShowAdvanced = () => {
  const val = program.getOptionValue("advanced");
  if (typeof val === "boolean") return val;
  const raw = (program as any).rawArgs as string[] | undefined;
  return (raw ?? []).some((arg) => arg === "--advanced" || arg === "-A");
};

program.configureHelp({
  visibleCommands(cmd) {
    const showAdvanced = shouldShowAdvanced();
    return cmd.commands.filter(
      (c) => showAdvanced || !ADVANCED_COMMANDS.has(c.name()),
    );
  },
});

program.addHelpText(
  "after",
  "\nAdvanced plumbing commands are hidden by default. Use `reflect --help --advanced` to show them.\n",
);

configureScanCommand(program.command("scan")).action(
  async (
    opts: {
      root: string;
      db: string;
      emitDelta: boolean;
      emitSinceTs: string;
      hash: string;
      listHashes: boolean;
      vacume: boolean;
      pruneMs: string;
      numericIds: boolean;
      ignore?: string[];
    },
    command,
  ) => {
    const { runScan } = await import("./scan.js");
    const params = mergeOptsWithLogger(command, opts);
    await runScan(params as any);
  },
);

program
  .command("ingest")
  .description("Ingest NDJSON deltas from stdin into a local files table")
  .requiredOption("--db <path>", "sqlite db file")
  .action(async (opts: { db: string }, command) => {
    const { runIngestDelta } = await import("./ingest-delta.js");
    const params = mergeOptsWithLogger(command, opts);
    await runIngestDelta(params as any);
  });

configureSchedulerCommand(program.command("scheduler")).action(
  async (opts, command) => {
    // Import and run in-process so we can manage lifecycle cleanly
    const { runScheduler, cliOptsToSchedulerOptions } = await import(
      "./scheduler.js"
    );
    const params = mergeOptsWithLogger(command, opts);
    await runScheduler(cliOptsToSchedulerOptions(params));
  },
);

configureWatchCommand(program.command("watch")).action(
  async (opts, command) => {
    const { runWatch } = await import("./watch.js");
    const merged = {
      ...command.optsWithGlobals(),
      ...opts,
    } as any;
    if (Array.isArray(merged.ignore)) {
      merged.ignoreRules = merged.ignore;
    }
    await runWatch(merged);
  },
);

// Default help when no subcommand given
if (process.argv.length <= 2) {
  program.outputHelp();
  process.exit(0);
}

// Parse user args
const normalizedArgv = process.argv.map((arg) =>
  arg === "--no-ignore" ? "--clear-ignore" : arg,
);
program.parse(normalizedArgv);
