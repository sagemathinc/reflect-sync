#!/usr/bin/env node
// src/cli.ts (ESM, TypeScript)
import fs from "node:fs";
import path from "node:path";
import { Command, Option } from "commander";
import { registerSessionCommands } from "./session-cli.js";
import { registerForwardCommands } from "./forward-cli.js";
import { CLI_NAME, MAX_WATCHERS } from "./constants.js";
import { listSupportedHashes, defaultHashAlg } from "./hash.js";
import { getSessionDbPath } from "./session-db.js";
import {
  ConsoleLogger,
  LOG_LEVELS,
  parseLogLevel,
  type LogLevel,
} from "./logger.js";
import pkg from "../package.json" with { type: "json" };
import { collectIgnoreOption } from "./ignore.js";
import { registerInstallCommand } from "./install-cli.js";
import { configureScanCommand } from "./scan.js";

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

program
  .command("merge")
  .description("3-way plan + rsync between alpha/beta; updates base snapshot")
  .requiredOption("--alpha-root <path>", "alpha filesystem root")
  .requiredOption("--beta-root <path>", "beta filesystem root")
  .option("--alpha-db <path>", "alpha sqlite", "alpha.db")
  .option("--beta-db <path>", "beta sqlite", "beta.db")
  .option("--base-db <path>", "base sqlite", "base.db")
  .option("--alpha-host <ssh>", "SSH host for alpha (e.g. user@host)")
  .option("--alpha-port <n>", "SSH port for alpha", (v) =>
    Number.parseInt(v, 10),
  )
  .option("--beta-host <ssh>", "SSH host for beta (e.g. user@host)")
  .option("--beta-port <n>", "SSH port for beta", (v) => Number.parseInt(v, 10))
  .option(
    "--compress <algo>",
    "[auto|zstd|lz4|zlib|zlibx|none][:level]",
    "auto",
  )
  .option(
    "--session-id <id>",
    "optional session id to enable heartbeats, report state, etc",
  )
  .option("--session-db <path>", "path to session database")
  .addOption(
    new Option("--prefer <side>", "conflict winner")
      .choices(["alpha", "beta"])
      .default("alpha"),
  )
  .action(async (opts, command) => {
    const { runMerge } = await import("./merge.js");
    const params = mergeOptsWithLogger(command, opts);
    params.verbose = params.logLevel === "debug";
    await runMerge(params as any);
  });

program
  .command("scheduler")
  .description("Orchestration of scanning, watching, and syncing")
  .requiredOption("--alpha-root <path>", "path to alpha root")
  .requiredOption("--beta-root <path>", "path to beta root")
  .option("--alpha-db <file>", "alpha sqlite database", "alpha.db")
  .option("--beta-db <file>", "beta sqlite database", "beta.db")
  .option("--base-db <file>", "base sqlite database", "base.db")
  .addOption(
    new Option("--prefer <side>", "conflict preference")
      .choices(["alpha", "beta"])
      .default("alpha"),
  )
  .addOption(
    new Option("--hash <algorithm>", "content hash algorithm")
      .choices(listSupportedHashes())
      .default(defaultHashAlg()),
  )
  // optional SSH endpoints (only one side may be remote)
  .option("--alpha-host <ssh>", "SSH host for alpha (e.g. user@host)")
  .option("--alpha-port <n>", "SSH port for alpha", (v) =>
    Number.parseInt(v, 10),
  )
  .option("--beta-host <ssh>", "SSH host for beta (e.g. user@host)")
  .option("--beta-port <n>", "SSH port for beta", (v) => Number.parseInt(v, 10))
  .option(
    "--alpha-remote-db <file>",
    "remote alpha sqlite DB path (on SSH host)",
    `~/.local/share/${CLI_NAME}/alpha.db`,
  )
  .option(
    "--beta-remote-db <file>",
    "remote beta sqlite DB path (on SSH host)",
    `~/.local/share/${CLI_NAME}/beta.db`,
  )
  .option(
    "--remote-command <cmd>",
    "absolute path to remote reflect-sync command",
  )
  .option(
    "--compress <algo>",
    "[auto|zstd|lz4|zlib|zlibx|none][:level]",
    "auto",
  )
  .option("--session-id <id>", "optional session id to enable heartbeats")
  .option("--session-db <path>", "path to session database")
  .option("--disable-micro-sync", "disable realtime micro-sync watchers")
  .option("--disable-full-cycle", "disable automatic periodic full sync cycles")
  .option(
    "-i, --ignore <pattern>",
    "gitignore-style ignore rule (repeat or comma-separated)",
    collectIgnoreOption,
    [] as string[],
  )
  .action(async (opts, command) => {
    // Safety: disallow both sides remote (two-remote rsync not yet supported)
    if (opts.alphaHost && opts.betaHost) {
      console.error(
        "Both sides remote is not supported yet (rsync two-remote).",
      );
      process.exit(1);
    }
    // Import and run in-process so we can manage lifecycle cleanly
    const { runScheduler, cliOptsToSchedulerOptions } = await import(
      "./scheduler.js"
    );
    const params = mergeOptsWithLogger(command, opts);
    await runScheduler(cliOptsToSchedulerOptions(params));
  });

program
  .command("watch")
  .description("Watch a root and emit NDJSON events on stdout")
  .requiredOption("--root <path>", "root directory to watch")
  .option("--shallow-depth <n>", "root watcher depth", "1")
  .option("--hot-depth <n>", "hot anchor depth", "2")
  .option("--hot-ttl-ms <ms>", "TTL for hot anchors", String(30 * 60_000))
  .option(
    "--max-hot-watchers <n>",
    "max concurrent hot watchers",
    String(MAX_WATCHERS),
  )
  .option(
    "-i, --ignore <pattern>",
    "gitignore-style ignore rule (repeat or comma-separated)",
    collectIgnoreOption,
    [] as string[],
  )
  .option(
    "--numeric-ids",
    "include uid:gid metadata in hashes (requires root on both sides)",
    false,
  )
  .action(async (opts, command) => {
    const { runWatch } = await import("./watch.js");
    const merged = {
      ...command.optsWithGlobals(),
      ...opts,
    } as any;
    if (Array.isArray(merged.ignore)) {
      merged.ignoreRules = merged.ignore;
    }
    await runWatch(merged);
  });

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
