#!/usr/bin/env node
// watch.ts — remote/local watcher that emits NDJSON events to stdout
//            and accepts JSON control messages on stdin.

import { Command } from "commander";
import chokidar, { FSWatcher } from "chokidar";
import path from "node:path";
import readline from "node:readline";
import { HotWatchManager, HOT_EVENTS, type HotWatchEvent } from "./hotwatch.js";
import { isDirectRun } from "./cli-util.js";

// ---------- types ----------
type WatchOpts = {
  root: string;
  shallowDepth: number;
  hotDepth: number;
  hotTtlMs: number;
  maxHotWatchers: number;
  verbose?: boolean;
};

// ---------- helpers ----------
const norm = (p: string) =>
  path.sep === "/" ? p : p.split(path.sep).join("/");
function relR(root: string, abs: string): string {
  let r = path.relative(root, abs);
  if (!r || r === ".") return ""; // root itself
  if (path.sep !== "/") r = r.split(path.sep).join("/");
  return r;
}
const parentDir = (r: string) => norm(path.posix.dirname(r || ".")); // "" -> "."
const isPlainRel = (r: string) =>
  !!r && !r.startsWith("/") && !r.startsWith("../") && !r.includes("..");

// STDERR logging only when verbose
function vlog(verbose: boolean | undefined, ...args: any[]) {
  if (verbose) {
    console.error(...args);
  }
}

function emitEvent(abs: string, ev: HotWatchEvent, root: string) {
  const rpath = relR(root, abs);
  if (!rpath) return;
  const isDir = ev.endsWith("Dir");
  const rec = { ev, rpath, kind: isDir ? "dir" : ("file" as const) };
  process.stdout.write(JSON.stringify(rec) + "\n");
}

// ---------- JSON control channel on STDIN ----------
function serveJsonControl(mgr: HotWatchManager, onClose: () => Promise<void>) {
  const rl = readline.createInterface({ input: process.stdin });

  rl.on("line", async (line) => {
    const s = line.trim();
    if (!s) return;
    let msg: any;
    try {
      msg = JSON.parse(s);
    } catch {
      console.error(`watch: ignoring invalid JSON: ${s}`);
      return;
    }

    const op = String(msg.op || "").toLowerCase();
    try {
      if (op === "add") {
        const dirs: string[] = Array.isArray(msg.dirs) ? msg.dirs : [];
        for (const r of dirs) {
          const clean = String(r || "").replace(/^\.\/+/, "");
          if (isPlainRel(clean) && clean !== ".") {
            await mgr.add(clean);
          }
        }
      } else if (op === "ping") {
        const id = msg.id ?? "";
        console.error(JSON.stringify({ pong: String(id) }));
      } else if (op === "close") {
        await onClose();
      } else {
        console.error(`watch: unknown op: ${op}`);
      }
    } catch (e: any) {
      console.error(`watch: control op failed: ${op}`, e);
    }
  });

  rl.on("close", () => {
    // Controller closed stdin; keep watching (do not exit).
  });
}

// ---------- core ----------
export async function runWatch(opts: WatchOpts): Promise<void> {
  const { root, shallowDepth, hotDepth, hotTtlMs, maxHotWatchers, verbose } =
    opts;

  const rootAbs = path.resolve(root);

  const hotMgr = new HotWatchManager(
    rootAbs,
    (abs, ev: HotWatchEvent) => {
      emitEvent(abs, ev, rootAbs);
    },
    {
      hotDepth,
      ttlMs: hotTtlMs,
      maxWatchers: maxHotWatchers,
    },
  );

  const shallow: FSWatcher = chokidar.watch(rootAbs, {
    persistent: true,
    ignoreInitial: true,
    depth: shallowDepth,
    awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
  });

  function wireShallow() {
    const handle = async (evt: HotWatchEvent, abs: string) => {
      // Send the event immediately (so microSync can act quickly)
      emitEvent(abs, evt, rootAbs);

      // Escalate: add a bounded hot watcher anchored at the parent dir
      const r = relR(rootAbs, abs);
      const rdir = parentDir(r);
      if (rdir && rdir !== ".") {
        try {
          await hotMgr.add(rdir);
        } catch (e) {
          console.error(
            `watch: hot add failed for '${rdir}': ${String((e as any)?.message || e)}`,
          );
        }
      }
    };

    for (const evt of HOT_EVENTS) {
      shallow.on(evt, (p) => handle(evt, p));
    }
    shallow.on("error", (err) => console.error("watch: shallow error", err));
  }

  wireShallow();
  vlog(
    verbose,
    `watch: root=${rootAbs} shallowDepth=${shallowDepth} hotDepth=${hotDepth} ttlMs=${hotTtlMs} max=${maxHotWatchers}`,
  );

  // Control channel
  serveJsonControl(hotMgr, async () => {
    try {
      await shallow.close();
    } catch {}
    try {
      await hotMgr.closeAll();
    } catch {}
    process.exit(0);
  });

  // Clean shutdown on signals
  const exit = async () => {
    try {
      await shallow.close();
    } catch {}
    try {
      await hotMgr.closeAll();
    } catch {}
    process.exit(0);
  };
  process.on("SIGINT", exit);
  process.on("SIGTERM", exit);

  // Keep alive forever
  await new Promise<void>(() => {});
}

// ---------- CLI ----------
function buildProgram() {
  const program = new Command()
    .name("ccsync watch")
    .description(
      "Watch a tree and emit NDJSON events to stdout; control via JSON on stdin.",
    );

  program
    .requiredOption("--root <path>", "root directory to watch")
    .option("--shallow-depth <n>", "root watcher depth", "1")
    .option("--hot-depth <n>", "hot anchor depth", "2")
    .option("--hot-ttl-ms <ms>", "TTL for hot anchors", String(30 * 60_000))
    .option("--max-hot-watchers <n>", "max concurrent hot watchers", "256")
    .option("--verbose", "log to stderr", false);

  return program;
}

async function mainFromCli() {
  const program = buildProgram();
  const opts = program.parse(process.argv).opts() as {
    root: string;
    shallowDepth: string;
    hotDepth: string;
    hotTtlMs: string;
    maxHotWatchers: string;
    verbose?: boolean;
  };

  await runWatch({
    root: opts.root,
    shallowDepth: Number(opts.shallowDepth),
    hotDepth: Number(opts.hotDepth),
    hotTtlMs: Number(opts.hotTtlMs),
    maxHotWatchers: Number(opts.maxHotWatchers),
    verbose: !!opts.verbose,
  });
}

if (isDirectRun(import.meta.url)) {
  mainFromCli().catch((e) => {
    console.error("watch fatal:", e?.stack || e);
    process.exit(1);
  });
}
