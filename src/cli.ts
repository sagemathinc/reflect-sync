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
    "Run a local scan (writes to DB_PATH or --db); supports --emit-delta for SSH piping",
  )
  .argument("<root>", "directory to scan")
  .addOption(new Option("--db <path>", "sqlite db file").default("alpha.db"))
  .option("--emit-delta", "emit NDJSON deltas to stdout for ingest", false)
  .action((root: string, opts: { db: string; emitDelta: boolean }) => {
    const args: string[] = [root];
    if (opts.emitDelta) args.push("--emit-delta");
    if (opts.db) args.push("--db", opts.db);
    run("./scan.js", args);
  });

// ---------- ingest ----------
program
  .command("ingest")
  .description("Ingest NDJSON deltas from stdin into a local files table")
  .addOption(new Option("--db <path>", "sqlite db file").makeOptionMandatory())
  .addOption(
    new Option("--root <path>", "absolute root to accept (optional)").default(
      "",
    ),
  )
  .action((opts: { db: string; root: string }) => {
    const args = ["--db", opts.db, "--root", opts.root];
    run("./ingest-delta.js", args);
  });

// ---------- merge ----------
program
  .command("merge")
  .description("3-way plan + rsync between alpha/beta; updates base snapshot")
  .requiredOption("--alpha-root <path>", "alpha filesystem root")
  .requiredOption("--beta-root <path>", "beta filesystem root")
  .addOption(
    new Option("--alpha-db <path>", "alpha sqlite").default("alpha.db"),
  )
  .addOption(new Option("--beta-db <path>", "beta sqlite").default("beta.db"))
  .addOption(new Option("--base-db <path>", "base sqlite").default("base.db"))
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

// ---------- scheduler ----------
program
  .command("scheduler")
  .description("Watch/scan/merge loop; supports SSH on either side")
  .option("--verbose")
  .requiredOption("--alpha-root <path>")
  .requiredOption("--beta-root <path>")
  .addOption(new Option("--alpha-db <path>").default("alpha.db"))
  .addOption(new Option("--beta-db <path>").default("beta.db"))
  .addOption(new Option("--base-db <path>").default("base.db"))
  .addOption(
    new Option("--prefer <side>").choices(["alpha", "beta"]).default("alpha"),
  )
  .addOption(
    new Option(
      "--alpha-host <target>",
      "ssh target for alpha (e.g. user@host)",
    ).default(""),
  )
  .addOption(
    new Option("--beta-host <target>", "ssh target for beta").default(""),
  )
  .addOption(
    new Option("--remote-scan-cmd <cmd>", "remote scan command").default(
      "ccsync scan",
    ),
  )
  .option("--dry-run", "rsync -n dry run")
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
    if (opts.alphaHost) args.push("--alpha-host", opts.alphaHost);
    if (opts.betaHost) args.push("--beta-host", opts.betaHost);
    if (opts.remoteScanCmd) args.push("--remote-scan-cmd", opts.remoteScanCmd);
    if (verbose) args.push("--verbose");
    if (dryRun) args.push("--dry-run");
    run("./scheduler.js", args);
  });

// Default help when no subcommand given
if (process.argv.length <= 2) {
  program.outputHelp();
  process.exit(0);
}

// Parse user args
program.parse();
