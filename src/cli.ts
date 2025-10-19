#!/usr/bin/env node
// src/cli.ts (ESM, TypeScript)
import { Command, Option } from "commander";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { version } = require("../package.json");

// --- tiny helper to run our compiled sub-scripts ---
function run(scriptRel: string, args: string[]) {
  const here = fileURLToPath(import.meta.url); // dist/cli.js at runtime
  const script = path.resolve(path.dirname(here), scriptRel);
  if (true || args.includes("--verbose")) {
    console.log(
      `${process.execPath} ${script} ${args.map((x) => (x.includes(" ") ? '"' + x + '"' : x)).join(" ")}`,
    );
  }
  const p = spawn(process.execPath, [script, ...args], {
    stdio: ["ignore", "inherit", "inherit"],
    detached: true, // group leader
  });
  p.on("exit", (code) => process.exit(code ?? 1));
}

// --- Commander program ---
const program = new Command()
  .name("ccsync")
  .description("Fast rsync-powered two-way sync with SQLite metadata and SSH")
  .version(version);

// Global flags you want available everywhere
program
  .option("--verbose", "verbose output", false)
  .option("--dry-run", "do not modify files", false);

// ---------- scan ----------
program
  .command("scan")
  .description(
    "Run a local scan writing to sqlite database and optionally stdout",
  )
  .argument("<root>", "directory to scan")
  .option("--db", "sqlite db file", "alpha.db")
  .option("--emit-delta", "emit NDJSON deltas to stdout for ingest", false)
  .action(
    async (root: string, opts: { db: string; emitDelta: boolean }, command) => {
      const { runScan } = await import("./scan.js");
      await runScan({ ...command.optsWithGlobals(), ...opts, root });
    },
  );

// ---------- ingest ----------
program
  .command("ingest")
  .description("Ingest NDJSON deltas from stdin into a local files table")
  .requiredOption("--db <path>", "sqlite db file")
  .option("--root <path>", "absolute root to accept")
  .action(async (opts: { db: string; root: string }, command) => {
    const { runIngestDelta } = await import("./ingest-delta");
    await runIngestDelta({ ...command.optsWithGlobals(), ...opts });
  });

// ---------- merge ----------
program
  .command("merge")
  .description("3-way plan + rsync between alpha/beta; updates base snapshot")
  .requiredOption("--alpha-root <path>", "alpha filesystem root")
  .requiredOption("--beta-root <path>", "beta filesystem root")
  .option("--alpha-db <path>", "alpha sqlite", "alpha.db")
  .option("--beta-db <path>", "beta sqlite", "beta.db")
  .option("--base-db <path>", "base sqlite", "base.db")
  .addOption(
    new Option("--prefer <side>", "conflict winner")
      .choices(["alpha", "beta"])
      .default("alpha"),
  )
  .action((opts, command) => {
    const { verbose, dryRun } = command.optsWithGlobals();
    const args = [
      "--alpha-root",
      opts.alphaRoot,
      "--beta-root",
      opts.betaRoot,
      "--alpha-db",
      opts.alphaDb,
      "--beta-db",
      opts.betaDb,
      "--base-db",
      opts.baseDb,
      "--prefer",
      opts.prefer,
    ];
    if (verbose) args.push("--verbose");
    if (dryRun) args.push("--dry-run");
    run("./merge-rsync.js", args);
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

// Default help when no subcommand given
if (process.argv.length <= 2) {
  program.outputHelp();
  process.exit(0);
}

// Parse user args
program.parse();
