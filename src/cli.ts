#!/usr/bin/env node
// src/cli.ts (ESM, TypeScript)
import { Command, Option } from "commander";
import { createRequire } from "node:module";
import { registerSessionCommands } from "./session-cli.js";
import { MAX_WATCHERS } from "./defaults.js";

const require = createRequire(import.meta.url);
const { version } = require("../package.json");

const program = new Command()
  .name("ccsync")
  .description("Fast rsync-powered two-way sync with SQLite metadata and SSH")
  .version(version);

// Global flags you want available everywhere
program
  .option("--verbose", "verbose output", false)
  .option("--dry-run", "do not modify files", false);

registerSessionCommands(program);

program
  .command("scan")
  .description(
    "Run a local scan writing to sqlite database and optionally stdout",
  )
  .requiredOption("--root <path>", "directory to scan")
  .option("--db <path>", "sqlite db file", "alpha.db")
  .option("--emit-delta", "emit NDJSON deltas to stdout for ingest", false)
  .action(
    async (opts: { root: string; db: string; emitDelta: boolean }, command) => {
      const { runScan } = await import("./scan.js");
      await runScan({ ...command.optsWithGlobals(), ...opts });
    },
  );

program
  .command("ingest")
  .description("Ingest NDJSON deltas from stdin into a local files table")
  .requiredOption("--db <path>", "sqlite db file")
  .option("--root <path>", "absolute root to accept")
  .action(async (opts: { db: string; root: string }, command) => {
    const { runIngestDelta } = await import("./ingest-delta.js");
    await runIngestDelta({ ...command.optsWithGlobals(), ...opts });
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
  .option("--beta-host <ssh>", "SSH host for beta (e.g. user@host)")
  .addOption(
    new Option("--prefer <side>", "conflict winner")
      .choices(["alpha", "beta"])
      .default("alpha"),
  )
  .action(async (opts, command) => {
    const { runMergeRsync } = await import("./merge-rsync.js");
    await runMergeRsync({ ...command.optsWithGlobals(), ...opts });
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
  // optional SSH endpoints (only one side may be remote)
  .option("--alpha-host <ssh>", "SSH host for alpha (e.g. user@host)")
  .option("--beta-host <ssh>", "SSH host for beta (e.g. user@host)")
  .option(
    "--alpha-remote-db <file>",
    "remote alpha sqlite DB path (on SSH host)",
    `${process.env.HOME ?? ""}/.cache/cocalc-sync/alpha.db`,
  )
  .option(
    "--beta-remote-db <file>",
    "remote beta sqlite DB path (on SSH host)",
    `${process.env.HOME ?? ""}/.cache/cocalc-sync/beta.db`,
  )
  // commands to run remotely
  .option("--remote-scan-cmd <cmd>", "remote scan command", "ccsync scan")
  .option(
    "--remote-watch-cmd <cmd>",
    "remote watch command for micro-sync (emits NDJSON)",
    "ccsync watch",
  )
  .option("--disable-hot-watch", "only sync during the full sync cycle", false)
  .action(async (opts, command) => {
    // Safety: disallow both sides remote (two-remote rsync not yet supported)
    if (opts.alphaHost && opts.betaHost) {
      console.error(
        "Both sides remote is not supported yet (rsync two-remote).",
      );
      process.exit(1);
    }
    // Import and run in-process so we can manage lifecycle cleanly
    const { runScheduler } = await import("./scheduler.js");
    await runScheduler({ ...command.optsWithGlobals(), ...opts });
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
  .action(async (opts, command) => {
    const { runWatch } = await import("./watch.js");
    await runWatch({
      ...command.optsWithGlobals(),
      ...opts,
    });
  });

// Default help when no subcommand given
if (process.argv.length <= 2) {
  program.outputHelp();
  process.exit(0);
}

// Parse user args
program.parse();
