#!/usr/bin/env node
// watch.ts — remote/local watcher that emits NDJSON events to stdout
//            and accepts JSON control messages on stdin.

import { Command } from "commander";
import chokidar, { FSWatcher } from "chokidar";
import path from "node:path";
import readline from "node:readline";
import { lstat, readlink } from "node:fs/promises";
import {
  HotWatchManager,
  HOT_EVENTS,
  type HotWatchEvent,
  handleWatchErrors,
  isRecent,
} from "./hotwatch.js";
import { isDirectRun } from "./cli-util.js";
import { CLI_NAME, MAX_WATCHERS } from "./constants.js";
import {
  collectIgnoreOption,
  normalizeIgnorePatterns,
  autoIgnoreForRoot,
} from "./ignore.js";
import { getReflectSyncHome } from "./session-db.js";
import { ensureTempDir } from "./rsync.js";
import {
  type SendSignature,
  signatureEquals,
} from "./recent-send.js";
import { modeHash, stringDigest, defaultHashAlg } from "./hash.js";

const HASH_ALG = defaultHashAlg();
const IGNORE_TTL_MS = Number(
  process.env.REFLECT_REMOTE_IGNORE_TTL_MS ?? 60_000,
);

// ---------- types ----------
type WatchOpts = {
  root: string;
  shallowDepth: number;
  hotDepth: number;
  hotTtlMs: number;
  maxHotWatchers: number;
  ignoreRules: string[];
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

function emitEvent(abs: string, ev: HotWatchEvent, root: string) {
  const path = relR(root, abs);
  if (!path) {
    return;
  }
  const rec = { ev, path };
  process.stdout.write(JSON.stringify(rec) + "\n");
  // vlog(true, rec);
}

// ---------- JSON control channel on STDIN ----------
function serveJsonControl(
  mgr: HotWatchManager,
  handleStat: (
    requestId: number,
    paths: string[],
    ignore: boolean,
  ) => Promise<void>,
  onClose: () => Promise<void>,
) {
  const rl = readline.createInterface({ input: process.stdin });

  rl.on("line", async (line) => {
    // vlog(true, { line });
    const s = line.trim();
    if (!s) return;
    let msg: any;
    try {
      msg = JSON.parse(s);
    } catch (err) {
      console.error(`watch: ignoring invalid JSON: ${s}`, err);
      return;
    }
    // vlog(true, { msg });

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
      } else if (op === "stat") {
        const requestId = Number(msg.requestId ?? 0);
        const ignore = Boolean(msg.ignore);
        const paths: string[] = Array.isArray(msg.paths)
          ? msg.paths.map((p) =>
              norm(String(p || "").replace(/^\.\/+/, "")),
            )
          : [];
        await handleStat(requestId, paths, ignore);
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
  const {
    root,
    shallowDepth,
    hotDepth,
    hotTtlMs,
    maxHotWatchers,
    ignoreRules: rawIgnoreRules,
  } = opts;
  const rootAbs = path.resolve(root);
  await ensureTempDir(rootAbs);
  const ignoreRaw = Array.isArray(rawIgnoreRules) ? [...rawIgnoreRules] : [];
  ignoreRaw.push(...autoIgnoreForRoot(rootAbs, getReflectSyncHome()));
  const ignoreRules = normalizeIgnorePatterns(ignoreRaw);

  const pendingIgnores = new Map<
    string,
    { signature: SendSignature; expiresAt: number }
  >();
  const pruneIgnores = () => {
    const now = Date.now();
    for (const [rel, entry] of pendingIgnores) {
      if (entry.expiresAt < now) {
        pendingIgnores.delete(rel);
      }
    }
  };

  // Ensure process terminate when stdin closes (so this also closes when
  // used via ssh)
  (function bindShutdownHooks() {
    const exit = () => process.exit(0);
    process.on("SIGTERM", exit);
    process.on("SIGHUP", exit);
    process.on("SIGINT", exit);

    // If launched via ssh with a pipe, stdin will get EOF when the local side ends.
    if (!process.stdin.isTTY) {
      process.stdin.on("end", exit);
      // Make sure 'end' can fire even if we never read from stdin.
      process.stdin.resume();
    }
  })();

  const hotMgr = new HotWatchManager(
    rootAbs,
    async (abs, ev: HotWatchEvent) => {
      const rel = relR(rootAbs, abs);
      if (await shouldSuppress(rel)) {
        return;
      }
      emitEvent(abs, ev, rootAbs);
    },
    {
      hotDepth,
      ttlMs: hotTtlMs,
      maxWatchers: maxHotWatchers,
      ignoreRules,
    },
  );

  async function computeSignature(
    rel: string,
  ): Promise<{ signature: SendSignature; target?: string | null }> {
    const abs = path.join(rootAbs, rel);
    try {
      const st = await lstat(abs);
      const mtime = (st as any).mtimeMs ?? st.mtime.getTime();
      if (st.isSymbolicLink()) {
        const target = await readlink(abs);
        return {
          signature: {
            kind: "link",
            opTs: mtime,
            mtime,
            hash: stringDigest(HASH_ALG, target),
          },
          target,
        };
      }
      if (st.isDirectory()) {
        return {
          signature: {
            kind: "dir",
            opTs: mtime,
            mtime,
            hash: modeHash(st.mode),
          },
        };
      }
      if (st.isFile()) {
        return {
          signature: {
            kind: "file",
            opTs: mtime,
            mtime,
            size: st.size,
          },
        };
      }
      return {
        signature: {
          kind: "file",
          opTs: mtime,
          mtime,
          size: st.size,
        },
      };
    } catch (err: any) {
      if (err?.code === "ENOENT") {
        return {
          signature: {
            kind: "missing",
            opTs: Date.now(),
          },
        };
      }
      throw err;
    }
  }

  async function handleStatRequest(
    requestId: number,
    paths: string[],
    ignore: boolean,
  ) {
    const now = Date.now();
    pruneIgnores();
    const unique = Array.from(new Set(paths.filter(Boolean)));
    const entries: Array<{
      path: string;
      signature: SendSignature;
      target?: string | null;
    }> = [];
    for (const rel of unique) {
      try {
        const { signature, target } = await computeSignature(rel);
        entries.push({ path: rel, signature, target });
        if (ignore) {
          pendingIgnores.set(rel, {
            signature,
            expiresAt: now + IGNORE_TTL_MS,
          });
        }
      } catch (err: any) {
        console.error(
          `watch: stat failed for '${rel}': ${String(err?.message || err)}`,
        );
        const signature: SendSignature = { kind: "missing", opTs: Date.now() };
        entries.push({ path: rel, signature });
        if (ignore) {
          pendingIgnores.set(rel, {
            signature,
            expiresAt: now + IGNORE_TTL_MS,
          });
        }
      }
    }
    const response = {
      op: "stat",
      requestId,
      entries,
    };
    process.stdout.write(JSON.stringify(response) + "\n");
  }

  async function shouldSuppress(rel: string): Promise<boolean> {
    if (!rel) return false;
    const entry = pendingIgnores.get(rel);
    if (!entry) return false;
    if (Date.now() > entry.expiresAt) {
      pendingIgnores.delete(rel);
      return false;
    }
    try {
      const { signature: currentSig } = await computeSignature(rel);
      if (signatureEquals(currentSig, entry.signature)) {
        entry.expiresAt = Date.now() + IGNORE_TTL_MS;
        return true;
      }
      pendingIgnores.delete(rel);
      return false;
    } catch (err: any) {
      console.error(
        `watch: suppress check failed for '${rel}': ${String(
          err?.message || err,
        )}`,
      );
      pendingIgnores.delete(rel);
      return false;
    }
  }

  const shallow: FSWatcher = chokidar.watch(rootAbs, {
    persistent: true,
    ignoreInitial: true,
    depth: shallowDepth,
    awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
    followSymlinks: false,
    alwaysStat: false,
  });

  function wireShallow() {
    const handle = async (evt: HotWatchEvent, abs: string, stats?) => {
      // Send the event immediately (so microSync can act quickly)
      const r = relR(rootAbs, abs);
      if (await shouldSuppress(r)) {
        return;
      }
      emitEvent(abs, evt, rootAbs);

      if (!(await isRecent(abs, stats))) {
        return;
      }

      // Escalate: add a bounded hot watcher anchored at the parent dir
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
      shallow.on(evt, (p, stats) => handle(evt, p, stats));
    }
    handleWatchErrors(shallow);
  }

  wireShallow();

  // Control channel
  serveJsonControl(hotMgr, handleStatRequest, async () => {
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
    .name(`${CLI_NAME}-watch`)
    .description(
      "Watch a tree and emit NDJSON events to stdout; control via JSON on stdin.",
    );

  program
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
    );

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
    ignore?: string[];
  };

  await runWatch({
    root: opts.root,
    shallowDepth: Number(opts.shallowDepth),
    hotDepth: Number(opts.hotDepth),
    hotTtlMs: Number(opts.hotTtlMs),
    maxHotWatchers: Number(opts.maxHotWatchers),
    ignoreRules: opts.ignore ?? [],
  });
}

if (isDirectRun(import.meta.url)) {
  mainFromCli().catch((e) => {
    console.error("watch fatal:", e?.stack || e);
    process.exit(1);
  });
}
