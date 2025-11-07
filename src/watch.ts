#!/usr/bin/env node
// watch.ts — remote/local watcher that emits NDJSON events to stdout
//            and accepts JSON control messages on stdin.

import { Command } from "commander";
import chokidar, { FSWatcher } from "chokidar";
import path from "node:path";
import readline from "node:readline";
import { lstat, readlink } from "node:fs/promises";
import type { Stats } from "node:fs";
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
import { waitForStableFile } from "./stability.js";
import {
  type SendSignature,
  signatureEquals,
} from "./recent-send.js";
import { stringDigest, defaultHashAlg, modeHash } from "./hash.js";
import {
  DeviceBoundary,
  type DeviceCheckOptions,
} from "./device-boundary.js";

const HASH_ALG = defaultHashAlg();
const IGNORE_TTL_MS = Number(
  process.env.REFLECT_REMOTE_IGNORE_TTL_MS ?? 60_000,
);
const LOCK_TTL_MS = Number(process.env.REFLECT_WATCH_LOCK_TTL_MS ?? 120_000);
const REMOTE_STABILITY_MS = Number(
  process.env.REFLECT_REMOTE_STABILITY_MS ?? 250,
);
const REMOTE_STABILITY_POLL_MS = Number(
  process.env.REFLECT_REMOTE_STABILITY_POLL_MS ?? 50,
);
const REMOTE_STABILITY_OPTIONS = {
  windowMs: REMOTE_STABILITY_MS,
  pollMs: REMOTE_STABILITY_POLL_MS,
  maxWaitMs: Math.max(
    Number.isFinite(REMOTE_STABILITY_MS) ? REMOTE_STABILITY_MS * 4 : 1000,
    1000,
  ),
};
const REMOTE_STABILITY_ENABLED =
  REMOTE_STABILITY_OPTIONS.windowMs > 0 &&
  Number.isFinite(REMOTE_STABILITY_OPTIONS.windowMs);

// ---------- types ----------
type WatchOpts = {
  root: string;
  shallowDepth: number;
  hotDepth: number;
  hotTtlMs: number;
  maxHotWatchers: number;
  ignoreRules: string[];
  numericIds: boolean;
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
type ReleaseControlEntry = {
  path: string;
  watermark?: number | null;
  ttlMs?: number;
};

type ControlHandlers = {
  addHotDir: (dir: string) => Promise<void>;
  handleStat: (
    requestId: number,
    paths: string[],
    ignore: boolean,
  ) => Promise<void>;
  handleLock?: (requestId: number, paths: string[]) => Promise<void>;
  handleRelease?: (
    requestId: number,
    entries: ReleaseControlEntry[],
  ) => Promise<void>;
  handleUnlock?: (requestId: number, paths: string[]) => Promise<void>;
  onClose: () => Promise<void>;
};

function serveJsonControl(handlers: ControlHandlers) {
  const rl = readline.createInterface({ input: process.stdin });

  const sendAck = (op: string, requestId: number, payload?: any) => {
    const body = { op, requestId, ...(payload ?? {}) };
    process.stdout.write(JSON.stringify(body) + "\n");
  };

  const respond = async (
    ackOp: string,
    requestId: number,
    fn: () => Promise<void>,
  ) => {
    try {
      await fn();
      sendAck(ackOp, requestId);
    } catch (err: any) {
      sendAck(ackOp, requestId, {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  };

  const normalizeRel = (value: any) => {
    const clean = String(value || "").replace(/^\.\/+/, "");
    if (!isPlainRel(clean) || clean === ".") {
      return "";
    }
    return clean;
  };

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
          const clean = normalizeRel(r);
          if (clean) {
            await handlers.addHotDir(clean);
          }
        }
      } else if (op === "stat") {
        const requestId = Number(msg.requestId ?? 0);
        const ignore = Boolean(msg.ignore);
        const paths: string[] = Array.isArray(msg.paths)
          ? msg.paths
              .map((p) => norm(String(p || "").replace(/^\.\/+/, "")))
          : [];
        await handlers.handleStat(requestId, paths, ignore);
      } else if (op === "lock" && handlers.handleLock) {
        const requestId = Number(msg.requestId ?? 0);
        const paths: string[] = Array.isArray(msg.paths)
          ? msg.paths
              .map((p) => norm(normalizeRel(p)))
              .filter((p) => !!p)
          : [];
        await respond("lockAck", requestId, () =>
          handlers.handleLock!(requestId, paths),
        );
      } else if (op === "release" && handlers.handleRelease) {
        const requestId = Number(msg.requestId ?? 0);
        const entries: ReleaseControlEntry[] = Array.isArray(msg.entries)
          ? msg.entries
              .map((entry: any) => ({
                path: norm(normalizeRel(entry?.path)),
                watermark:
                  entry?.watermark == null
                    ? null
                    : Number(entry.watermark),
                ttlMs:
                  entry?.ttlMs == null ? undefined : Number(entry.ttlMs),
              }))
              .filter((e) => !!e.path)
          : [];
        await respond("releaseAck", requestId, () =>
          handlers.handleRelease!(requestId, entries),
        );
      } else if (op === "unlock" && handlers.handleUnlock) {
        const requestId = Number(msg.requestId ?? 0);
        const paths: string[] = Array.isArray(msg.paths)
          ? msg.paths
              .map((p) => norm(normalizeRel(p)))
              .filter((p) => !!p)
          : [];
        await respond("unlockAck", requestId, () =>
          handlers.handleUnlock!(requestId, paths),
        );
      } else if (op === "close") {
        await handlers.onClose();
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
    numericIds,
  } = opts;
  const rootAbs = path.resolve(root);
  await ensureTempDir(rootAbs);
  const deviceBoundary = await DeviceBoundary.create(rootAbs);
  const loggedCrossDevice = new Set<string>();

  const logCrossDevice = (abs: string) => {
    const rel = relR(rootAbs, abs) || ".";
    if (loggedCrossDevice.has(rel)) return;
    loggedCrossDevice.add(rel);
    console.warn(
      `watch: ignoring cross-device path '${rel}' under root '${rootAbs}'`,
    );
  };

  const allowPath = async (
    abs: string,
    opts: DeviceCheckOptions & { isDir: boolean },
  ): Promise<boolean> => {
    const ok = await deviceBoundary.isOnRootDevice(abs, opts);
    if (!ok) logCrossDevice(abs);
    return ok;
  };
  const ignoreRaw = Array.isArray(rawIgnoreRules) ? [...rawIgnoreRules] : [];
  ignoreRaw.push(...autoIgnoreForRoot(rootAbs, getReflectSyncHome()));
  const ignoreRules = normalizeIgnorePatterns(ignoreRaw);

  const pendingIgnores = new Map<
    string,
    { signature: SendSignature; expiresAt: number }
  >();
  type StabilityEntry = {
    ev: HotWatchEvent;
    abs: string;
    stats?: Stats;
    timeout?: NodeJS.Timeout;
  };
  const pendingStability = new Map<string, StabilityEntry>();
  const pruneIgnores = () => {
    const now = Date.now();
    for (const [rel, entry] of pendingIgnores) {
      if (entry.expiresAt < now) {
        pendingIgnores.delete(rel);
      }
    }
  };
  const clearStabilityTimers = () => {
    for (const entry of pendingStability.values()) {
      if (entry.timeout) {
        clearTimeout(entry.timeout);
      }
    }
    pendingStability.clear();
  };
  type ForcedEntry = {
    state: "locked" | "released";
    watermark?: number | null;
    expiresAt: number;
  };
  const forcedLocks = new Map<string, ForcedEntry>();
  const pruneForcedLocks = () => {
    if (!forcedLocks.size) return;
    const now = Date.now();
    for (const [rel, entry] of forcedLocks) {
      if (entry.expiresAt <= now) {
        forcedLocks.delete(rel);
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
      const isDirEvent = ev === "addDir" || ev === "unlinkDir";
      if (!(await allowPath(abs, { isDir: isDirEvent }))) {
        return;
      }
      const rel = relR(rootAbs, abs);
      scheduleStableEmit(rel, abs, ev);
    },
    {
      hotDepth,
      ttlMs: hotTtlMs,
      maxWatchers: maxHotWatchers,
      ignoreRules,
    },
  );

  async function addHotAnchor(rdir: string): Promise<void> {
    if (!rdir || rdir === ".") return;
    const absAnchor = path.join(rootAbs, rdir);
    if (!(await allowPath(absAnchor, { isDir: true }))) {
      return;
    }
    await hotMgr.add(rdir);
  }

  async function computeSignature(
    rel: string,
  ): Promise<{ signature: SendSignature; target?: string | null }> {
    const abs = path.join(rootAbs, rel);
    try {
      const st = await lstat(abs);
      const isDir = st.isDirectory();
      if (!(await allowPath(abs, { isDir, stats: st }))) {
        return {
          signature: {
            kind: "missing",
            opTs: Date.now(),
          },
        };
      }
      const mtime = (st as any).mtimeMs ?? st.mtime.getTime();
      const ctime = (st as any).ctimeMs ?? st.ctime.getTime();
      if (st.isSymbolicLink()) {
        const target = await readlink(abs);
        return {
          signature: {
            kind: "link",
            opTs: mtime,
            mtime,
            ctime,
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
            ctime,
            hash: modeHash(st.mode),
          },
        };
      }
      if (st.isFile()) {
        const signature: SendSignature = {
          kind: "file",
          opTs: mtime,
          mtime,
          ctime,
          size: st.size,
          mode: st.mode,
        };
        if (numericIds) {
          signature.uid = st.uid;
          signature.gid = st.gid;
        }
        return {
          signature,
        };
      }
      return {
        signature: {
          kind: "file",
          opTs: mtime,
          mtime,
          ctime,
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
        if (REMOTE_STABILITY_ENABLED) {
          await ensureStable(path.join(rootAbs, rel));
        }
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

  const statsCtimeMs = (st?: Stats): number | null => {
    if (!st) return null;
    return (st as any).ctimeMs ?? (st.ctime ? st.ctime.getTime() : null);
  };

  async function ensureStable(abs: string): Promise<boolean> {
    if (!REMOTE_STABILITY_ENABLED) return true;
    try {
      const res = await waitForStableFile(abs, REMOTE_STABILITY_OPTIONS);
      return res.stable;
    } catch (err) {
      console.warn(`watch: stability check failed for '${abs}':`, err);
      return false;
    }
  }

  async function currentCtimeMs(abs?: string): Promise<number | null> {
    if (!abs) return null;
    try {
      const st = await lstat(abs);
      return statsCtimeMs(st);
    } catch {
      return null;
    }
  }

  async function emitWithEscalation(
    rel: string,
    abs: string,
    ev: HotWatchEvent,
    stats?: Stats,
  ) {
    emitEvent(abs, ev, rootAbs);
    if (!(await isRecent(abs, stats))) {
      return;
    }
    const rdir = parentDir(rel);
    if (rdir && rdir !== ".") {
      try {
        await addHotAnchor(rdir);
      } catch (e: any) {
        console.error(
          `watch: hot add failed for '${rdir}': ${String(
            e?.message || e,
          )}`,
        );
      }
    }
  }

  async function emitImmediate(
    rel: string,
    abs: string,
    ev: HotWatchEvent,
    stats?: Stats,
  ) {
    if (await shouldSuppress(rel, abs, stats)) {
      return;
    }
    await emitWithEscalation(rel, abs, ev, stats);
  }

  async function shouldSuppress(
    rel: string,
    abs?: string,
    stats?: Stats,
  ): Promise<boolean> {
    if (!rel) return false;
    pruneForcedLocks();
    const forced = forcedLocks.get(rel);
    if (forced) {
      const expired = forced.expiresAt <= Date.now();
      if (expired) {
        forcedLocks.delete(rel);
      } else if (forced.state === "locked") {
        return true;
      } else if (forced.watermark != null) {
        const ctime =
          statsCtimeMs(stats) ?? (await currentCtimeMs(abs ?? path.join(rootAbs, rel)));
        if (ctime != null && ctime <= forced.watermark) {
          return true;
        }
        forcedLocks.delete(rel);
      } else {
        forcedLocks.delete(rel);
      }
    }
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

  const dedupe = <T>(items: T[]) => Array.from(new Set(items));

  function scheduleStableEmit(
    rel: string,
    abs: string,
    ev: HotWatchEvent,
    stats?: Stats,
  ) {
    if (!rel) {
      emitImmediate(rel, abs, ev, stats);
      return;
    }
    if (
      !REMOTE_STABILITY_ENABLED ||
      ev === "addDir" ||
      ev === "unlinkDir"
    ) {
      emitImmediate(rel, abs, ev, stats);
      return;
    }
    const key = rel;
    const existing = pendingStability.get(key);
    if (existing?.timeout) {
      clearTimeout(existing.timeout);
    }
    const timeout = setTimeout(async () => {
      pendingStability.delete(key);
      const stable = await ensureStable(abs);
      if (!stable) {
        scheduleStableEmit(rel, abs, ev, stats);
        return;
      }
      await emitImmediate(rel, abs, ev, stats);
    }, REMOTE_STABILITY_OPTIONS.windowMs);
    pendingStability.set(key, { ev, abs, stats, timeout });
  }

  async function handleLockControl(
    _requestId: number,
    paths: string[],
  ): Promise<void> {
    if (!paths.length) return;
    pruneForcedLocks();
    const now = Date.now();
    for (const rel of dedupe(paths)) {
      if (!rel) continue;
      forcedLocks.set(rel, {
        state: "locked",
        expiresAt: now + LOCK_TTL_MS,
      });
    }
  }

  async function handleReleaseControl(
    _requestId: number,
    entries: ReleaseControlEntry[],
  ): Promise<void> {
    if (!entries.length) return;
    pruneForcedLocks();
    const now = Date.now();
    for (const entry of entries) {
      const rel = entry.path;
      if (!rel) continue;
      forcedLocks.set(rel, {
        state: "released",
        watermark: entry.watermark ?? null,
        expiresAt: now + (entry.ttlMs && entry.ttlMs > 0 ? entry.ttlMs : LOCK_TTL_MS),
      });
    }
  }

  async function handleUnlockControl(
    _requestId: number,
    paths: string[],
  ): Promise<void> {
    if (!paths.length) return;
    for (const rel of dedupe(paths)) {
      forcedLocks.delete(rel);
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
    const handle = async (evt: HotWatchEvent, abs: string, stats?: Stats) => {
      const isDirEvent = evt === "addDir" || evt === "unlinkDir";
      if (!(await allowPath(abs, { isDir: isDirEvent, stats }))) {
        return;
      }
      // Send the event immediately (so microSync can act quickly)
      const r = relR(rootAbs, abs);
      scheduleStableEmit(r, abs, evt, stats);
    };

    for (const evt of HOT_EVENTS) {
      shallow.on(evt, (p, stats) => handle(evt, p, stats));
    }
    handleWatchErrors(shallow);
  }

  wireShallow();

  // Control channel
  serveJsonControl({
    addHotDir: addHotAnchor,
    handleStat: handleStatRequest,
    handleLock: handleLockControl,
    handleRelease: handleReleaseControl,
    handleUnlock: handleUnlockControl,
    onClose: async () => {
      try {
        await shallow.close();
      } catch {}
      try {
        await hotMgr.closeAll();
      } catch {}
      clearStabilityTimers();
      process.exit(0);
    },
  });

  // Clean shutdown on signals
  const exit = async () => {
    try {
      await shallow.close();
    } catch {}
    try {
      await hotMgr.closeAll();
    } catch {}
    clearStabilityTimers();
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
    )
    .option(
      "--numeric-ids",
      "include uid:gid metadata in hashes (requires root on both sides)",
      false,
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
    db?: string;
    numericIds?: boolean;
  };

  await runWatch({
    root: opts.root,
    shallowDepth: Number(opts.shallowDepth),
    hotDepth: Number(opts.hotDepth),
    hotTtlMs: Number(opts.hotTtlMs),
    maxHotWatchers: Number(opts.maxHotWatchers),
    ignoreRules: opts.ignore ?? [],
    numericIds: Boolean(opts.numericIds),
  });
}

if (isDirectRun(import.meta.url)) {
  mainFromCli().catch((e) => {
    console.error("watch fatal:", e?.stack || e);
    process.exit(1);
  });
}
