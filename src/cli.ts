#!/usr/bin/env node
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";

function run(scriptRel: string, args: string[]) {
  // When compiled, cli.js sits next to scan.js/merge.js/etc in dist/
  const here = fileURLToPath(import.meta.url);
  const script = path.resolve(path.dirname(here), scriptRel);
  const p = spawn(process.execPath, [script, ...args], { stdio: "inherit" });
  p.on("exit", (code) => process.exit(code ?? 1));
}

function main() {
  const argv = process.argv.slice(2);
  const invoked = path.basename(process.argv[1] || "");

  // Support alias executables like `ccsync-scan`
  const alias = invoked.startsWith("ccsync-")
    ? invoked.slice("ccsync-".length)
    : null;
  const [cmd, ...rest] = alias ? [alias, ...argv] : argv;

  switch ((cmd || "").toLowerCase()) {
    case "scan":
      return run("./scan.js", rest);
    case "ingest":
      return run("./ingest-delta.js", rest);
    case "merge":
      return run("./merge-rsync.js", rest);
    case "scheduler":
      return run("./scheduler.js", rest);
    case "help":
    case "":
      console.log(`ccsync <cmd> [...args]

Commands:
  scan        Run a local scan (writes to DB_PATH or --db)
  ingest      Ingest NDJSON deltas from stdin into --db
  merge       3-way plan + rsync between alpha/beta (updates base)
  scheduler   Watch/scan/merge loop; supports SSH on either side

Examples:
  ccsync scan /path/to/root
  ssh host 'env DB_PATH=~/.cache/cocalc-sync/alpha.db ccsync scan /root --emit-delta' | ccsync ingest --db alpha.db --root /root
  ccsync scheduler --alpha-root /a --beta-root /b --prefer alpha --verbose true
`);
      process.exit(0);
    default:
      console.error(`Unknown command: ${cmd}. Try "ccsync help".`);
      process.exit(2);
  }
}

main();
