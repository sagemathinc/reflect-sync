#!/usr/bin/env node
// src/watch.ts
import chokidar from "chokidar";
import path from "node:path";
import { Command, Option } from "commander";
import { cliEntrypoint } from "./cli-util.js";

const norm = (p: string) =>
  path.sep === "/" ? p : p.split(path.sep).join("/");
const rel = (root: string, full: string): string => {
  let r = path.relative(root, full);
  if (r === "" || r === ".") return "";
  return norm(r);
};

/** Event shape we emit over stdout as NDJSON */
type WatchEvent =
  | { type: "file"; op: "add" | "change" | "unlink"; path: string; ts: number }
  | { type: "dir"; op: "addDir" | "unlinkDir"; path: string; ts: number };

export type WatchOpts = {
  root: string;
  depth?: number; // chokidar depth; undefined => full
  batchMs?: number; // debounce for flushing grouped events
  awaitWriteFinish?: number; // ms stability threshold (0 disables)
  verbose?: boolean;
};

export async function runWatch({
  root,
  depth,
  batchMs = 100,
  awaitWriteFinish = 200,
  verbose = false,
}: WatchOpts): Promise<void> {
  if (!root) {
    console.error("watch: --root is required");
    process.exit(1);
  }

  const awf =
    awaitWriteFinish && awaitWriteFinish > 0
      ? { stabilityThreshold: awaitWriteFinish, pollInterval: 50 }
      : undefined;

  const watcher = chokidar.watch(root, {
    persistent: true,
    ignoreInitial: true,
    depth: typeof depth === "number" ? depth : undefined,
    awaitWriteFinish: awf,
  });

  // Simple coalescing: last-op-wins per (type, path)
  const pending = new Map<string, WatchEvent>();
  let flushTimer: NodeJS.Timeout | null = null;

  const queue = (evt: WatchEvent) => {
    const key = `${evt.type}:${evt.path}`;
    pending.set(key, evt);
    if (!flushTimer) {
      flushTimer = setTimeout(flush, batchMs);
      // keep tests from hanging: not strictly required in production
      flushTimer.unref?.();
    }
  };

  const flush = () => {
    flushTimer = null;
    if (pending.size === 0) return;
    const out = Array.from(pending.values());
    pending.clear();
    for (const row of out) {
      // NDJSON on stdout
      process.stdout.write(JSON.stringify(row) + "\n");
    }
    // Do not log to stdout – use stderr if verbose
    if (verbose) {
      console.error(`watch: flushed ${out.length} event(s)`);
    }
  };

  const now = () => Date.now();

  // Wire chokidar events
  watcher.on("add", (p) => {
    const r = rel(root, p);
    if (r) queue({ type: "file", op: "add", path: r, ts: now() });
  });
  watcher.on("change", (p) => {
    const r = rel(root, p);
    if (r) queue({ type: "file", op: "change", path: r, ts: now() });
  });
  watcher.on("unlink", (p) => {
    const r = rel(root, p);
    if (r) queue({ type: "file", op: "unlink", path: r, ts: now() });
  });

  watcher.on("addDir", (p) => {
    const r = rel(root, p);
    if (r) queue({ type: "dir", op: "addDir", path: r, ts: now() });
  });
  watcher.on("unlinkDir", (p) => {
    const r = rel(root, p);
    if (r) queue({ type: "dir", op: "unlinkDir", path: r, ts: now() });
  });

  watcher.on("error", (e) => {
    console.error("watch: chokidar error:", e);
  });

  // Graceful shutdown
  const stop = async () => {
    try {
      await watcher.close();
    } catch {}
    flush();
    process.exit(0);
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);

  if (verbose) {
    console.error(
      `watch: started on ${root} (depth=${depth ?? "∞"}, batchMs=${batchMs}, awf=${awaitWriteFinish})`,
    );
  }
}

export function buildProgram(): Command {
  const program = new Command()
    .name("ccsync-watch")
    .description("Watch a root and emit NDJSON file/dir events on stdout")
    .requiredOption("--root <path>", "root directory to watch")
    .addOption(
      new Option("--depth <n>", "limit watch recursion depth").argParser((v) =>
        Number(v),
      ),
    )
    .addOption(
      new Option("--batch-ms <n>", "debounce time in ms before flushing events")
        .argParser((v) => Number(v))
        .default(100),
    )
    .addOption(
      new Option(
        "--await-write-finish <ms>",
        "require ms of quiet time before firing events (0 disables)",
      )
        .argParser((v) => Number(v))
        .default(200),
    )
    .option("--verbose", "log to stderr", false)
    .action(async (opts) => {
      await runWatch({
        ...opts,
        depth: opts.depth ? Number(opts.depth) : undefined,
        batchMs: Number(opts.batchMs),
        awaitWriteFinish: Number(opts.awaitWriteFinish),
      });
    });
  return program;
}

cliEntrypoint<WatchOpts>(import.meta.url, buildProgram, runWatch, {
  label: "watch",
});
