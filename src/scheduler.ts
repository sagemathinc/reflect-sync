#!/usr/bin/env node
// scheduler.ts
//
// - Commander-based CLI
// - Exported runScheduler(opts) API for programmatic use
// - Optional SSH micro-sync: remote watch -> tee to ingest + realtime push
//
// Notes:
// * Local watchers only; remote changes arrive via remote watch stream.
// * Full-cycle still uses "ccsync merge" (your merge-rsync.ts).
// * Test knobs are env-based: SCHED_MIN_MS, MICRO_DEBOUNCE_MS, etc.

import { spawn, SpawnOptions, ChildProcess } from "node:child_process";
import chokidar, { FSWatcher } from "chokidar";
import Database from "better-sqlite3";
import path from "node:path";
import { tmpdir } from "node:os";
import {
  mkdtemp,
  rm,
  writeFile,
  stat as fsStat,
  lstat as fsLstat,
} from "node:fs/promises";
import { Command, Option } from "commander";
import readline from "node:readline";
import { PassThrough } from "node:stream";
import { cliEntrypoint } from "./cli-util.js";

export type SchedulerOptions = {
  alphaRoot: string;
  betaRoot: string;
  alphaDb: string;
  betaDb: string;
  baseDb: string;
  prefer: "alpha" | "beta";
  verbose: boolean;
  dryRun: boolean;

  alphaHost?: string;
  betaHost?: string;
  alphaRemoteDb: string;
  betaRemoteDb: string;

  remoteScanCmd: string; // e.g. "ccsync scan"
  remoteWatchCmd: string; // e.g. "ccsync watch"
};

// ---------- CLI ----------
function buildProgram(): Command {
  const program = new Command()
    .name("ccsync-scheduler")
    .description("Orchestration of scanning, watching, syncing");

  program
    .requiredOption("--alpha-root <path>", "path to root of alpha sync tree")
    .requiredOption("--beta-root <path>", "path to root of beta sync tree")
    .option("--alpha-db <file>", "path to alpha sqlite database", "alpha.db")
    .option("--beta-db <file>", "path to beta sqlite database", "beta.db")
    .option("--base-db <file>", "path to base sqlite database", "base.db")
    .addOption(
      new Option(
        "--prefer <side>",
        "conflict preference: all conflicts are resolved in favor of this side",
      )
        .choices(["alpha", "beta"])
        .default("alpha"),
    )
    .option("--verbose", "enable verbose logging", false)
    .option("--dry-run", "simulate without changing files", false)
    // optional SSH endpoints (only one side may be remote)
    .option("--alpha-host <ssh>", "SSH host for alpha (e.g. user@host)")
    .option("--beta-host <ssh>", "SSH host for beta (e.g. user@host)")
    .option(
      "--alpha-remote-db <file>",
      "remote path to alpha sqlite db (on the SSH host)",
      `${process.env.HOME ?? ""}/.cache/cocalc-sync/alpha.db`,
    )
    .option(
      "--beta-remote-db <file>",
      "remote path to beta sqlite db (on the SSH host)",
      `${process.env.HOME ?? ""}/.cache/cocalc-sync/beta.db`,
    )
    // commands to run on remote for scan/micro-watch
    .option("--remote-scan-cmd <cmd>", "remote scan command", "ccsync scan")
    .option(
      "--remote-watch-cmd <cmd>",
      "remote watch command for micro-sync (emits NDJSON lines)",
      "ccsync watch",
    );

  return program;
}

function cliOptsToSchedulerOptions(opts: any): SchedulerOptions {
  const out: SchedulerOptions = {
    alphaRoot: String(opts.alphaRoot),
    betaRoot: String(opts.betaRoot),
    alphaDb: String(opts.alphaDb),
    betaDb: String(opts.betaDb),
    baseDb: String(opts.baseDb),
    prefer: String(opts.prefer).toLowerCase() as "alpha" | "beta",
    verbose: !!opts.verbose,
    dryRun: !!opts.dryRun,
    alphaHost: opts.alphaHost?.trim() || undefined,
    betaHost: opts.betaHost?.trim() || undefined,
    alphaRemoteDb: String(opts.alphaRemoteDb),
    betaRemoteDb: String(opts.betaRemoteDb),
    remoteScanCmd: String(opts.remoteScanCmd),
    remoteWatchCmd: String(opts.remoteWatchCmd),
  };

  if (out.alphaHost && out.betaHost) {
    console.error("Both sides remote is not supported yet (rsync two-remote).");
    process.exit(1);
  }

  return out;
}

// ---------- env/test knobs ----------
const envNum = (k: string, def: number) =>
  process.env[k] ? Number(process.env[k]) : def;

const MAX_HOT_WATCHERS = envNum("MAX_HOT_WATCHERS", 256);
const HOT_TTL_MS = envNum("HOT_TTL_MS", 30 * 60_000);
const SHALLOW_DEPTH = envNum("SHALLOW_DEPTH", 1);
const HOT_DEPTH = envNum("HOT_DEPTH", 2);

const MICRO_DEBOUNCE_MS = envNum("MICRO_DEBOUNCE_MS", 200);
const COOLDOWN_MS = envNum("COOLDOWN_MS", 300);

const MIN_INTERVAL_MS = envNum("SCHED_MIN_MS", 7_500);
const MAX_INTERVAL_MS = envNum("SCHED_MAX_MS", 60_000);
const MAX_BACKOFF_MS = envNum("SCHED_MAX_BACKOFF_MS", 600_000);
const JITTER_MS = envNum("SCHED_JITTER_MS", 500);

// ---------- core (exported) ----------
export async function runScheduler(opts: SchedulerOptions): Promise<void> {
  const {
    alphaRoot,
    betaRoot,
    alphaDb,
    betaDb,
    baseDb,
    prefer,
    verbose,
    dryRun,
    alphaHost,
    betaHost,
    alphaRemoteDb,
    betaRemoteDb,
    remoteScanCmd,
    remoteWatchCmd,
  } = opts;

  if (!alphaRoot || !betaRoot) {
    throw new Error("Need --alpha-root and --beta-root");
  }
  if (alphaHost && betaHost) {
    throw new Error(
      "Both sides remote is not supported yet (rsync two-remote).",
    );
  }

  // ---------- scheduler state ----------
  let running = false,
    pending = false,
    lastCycleMs = 0,
    nextDelayMs = 10_000,
    backoffMs = 0;

  const alphaIsRemote = !!alphaHost;
  const betaIsRemote = !!betaHost;

  // ---------- logging ----------
  const db = new Database(baseDb);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.exec(`
    CREATE TABLE IF NOT EXISTS events(
      id INTEGER PRIMARY KEY,
      ts INTEGER,
      level TEXT,
      source TEXT,
      msg TEXT,
      details TEXT
    );
  `);
  const logStmt = db.prepare(
    `INSERT INTO events(ts,level,source,msg,details) VALUES (?,?,?,?,?)`,
  );

  function log(
    level: "info" | "warn" | "error",
    source: string,
    msg: string,
    details?: any,
  ) {
    try {
      logStmt.run(
        Date.now(),
        level,
        source,
        msg,
        details ? JSON.stringify(details) : null,
      );
    } catch {}
    if (verbose || level !== "info")
      console.log(
        `${level === "error" ? "⛔" : level === "warn" ? "⚠️" : "ℹ️"} [${source}] ${msg}`,
        details ? JSON.stringify(details) : "",
      );
  }

  // ---------- helpers ----------
  function spawnTask(
    cmd: string,
    args: string[],
    okCodes: number[] = [0],
    extra: SpawnOptions = {},
  ): Promise<{
    code: number | null;
    ms: number;
    ok: boolean;
    lastZero: boolean;
  }> {
    if (verbose) console.log(`${cmd} ${args.join(" ")}`);
    return new Promise((resolve) => {
      const t0 = Date.now();
      let lastZero = false;
      const p = spawn(cmd, args, {
        stdio: verbose ? "inherit" : "ignore",
        ...extra,
      });
      p.on("exit", (code) => {
        lastZero = code === 0;
        const ok = code !== null && okCodes.includes(code);
        resolve({ code, ms: Date.now() - t0, ok, lastZero });
      });
      p.on("error", () => {
        resolve({
          code: 1,
          ms: Date.now() - t0,
          ok: okCodes.includes(1),
          lastZero: false,
        });
      });
    });
  }

  const clamp = (x: number, lo: number, hi: number) =>
    Math.max(lo, Math.min(hi, x));
  async function fileNonEmpty(p: string) {
    try {
      return (await fsStat(p)).size > 0;
    } catch {
      return false;
    }
  }
  const join0 = (items: string[]) =>
    Buffer.from(items.filter(Boolean).join("\0") + (items.length ? "\0" : ""));

  const norm = (p: string) =>
    path.sep === "/" ? p : p.split(path.sep).join("/");
  function rel(root: string, full: string): string {
    let r = path.relative(root, full);
    if (r === "" || r === ".") return "";
    if (path.sep !== "/") r = r.split(path.sep).join("/");
    return r;
  }
  const parentDir = (r: string) => norm(path.posix.dirname(r || ".")); // "" -> "."

  function relDepth(rootAbs: string, absPath: string): number {
    const r = norm(path.relative(rootAbs, absPath));
    if (!r || r === ".") return 0;
    return r.split("/").length - 1;
  }

  function minimalCover(dirs: string[]): string[] {
    const sorted = Array.from(new Set(dirs)).sort(
      (a, b) => a.length - b.length,
    );
    const out: string[] = [];
    for (const d of sorted) {
      if (
        !out.some((p) => d === p || d.startsWith(p.endsWith("/") ? p : p + "/"))
      )
        out.push(d);
    }
    return out;
  }

  // ---------- Remote watch (SSH) ----------
  type WatchSide = "alpha" | "beta";

  function parseWatchLine(line: string): { rpaths: string[] } | null {
    const s = line.trim();
    if (!s) return null;

    // Prefer NDJSON { r: "...", e: "add|change|unlink" } or { path: "...", event: "..." }.
    // Also accept arrays: [{r:...}, ...]
    try {
      const obj = JSON.parse(s);
      const toR = (o: any): string | null =>
        (o?.r as string) ??
        (o?.path as string) ??
        (typeof o === "string" ? o : null);

      if (Array.isArray(obj)) {
        const rpaths = obj.map((o) => toR(o)).filter(Boolean) as string[];
        return rpaths.length ? { rpaths } : null;
      } else {
        const r = toR(obj);
        return r ? { rpaths: [r] } : null;
      }
    } catch {
      // Fallback: plain path per line
      return { rpaths: [s] };
    }
  }

  function startSshRemoteWatch(params: {
    side: WatchSide;
    host: string;
    root: string;
    remoteWatchCmd: string; // e.g. "ccsync watch"
    verbose?: boolean;
  }) {
    const { side, host, root, remoteWatchCmd, verbose } = params;

    // pick target set and logger tag
    const targetSet = side === "alpha" ? hotAlpha : hotBeta;
    const tag = side === "alpha" ? "remote-watch-alpha" : "remote-watch-beta";

    // Split the configured command and append args
    const parts = remoteWatchCmd.trim().split(/\s+/);
    const cmd = parts.shift()!;
    const cmdArgs = [...parts, "--root", root];

    let restarting = false;
    let proc: import("node:child_process").ChildProcess | null = null;

    const launch = () => {
      const sshArgs = [
        "-o",
        "BatchMode=yes",
        host,
        // exec remote program
        cmd,
        ...cmdArgs,
      ];
      if (verbose) console.log("$ ssh", sshArgs.join(" "));

      proc = spawn("ssh", sshArgs, {
        stdio: ["ignore", "pipe", "inherit"],
      });

      // Read NDJSON lines from stdout
      if (proc.stdout) {
        const rl = readline.createInterface({ input: proc.stdout });
        rl.on("line", (line) => {
          const parsed = parseWatchLine(line);
          if (!parsed) return;
          for (const r of parsed.rpaths) {
            // Treat each rpath as "touched" on that side
            targetSet.add(r);
          }
          // Run a micro pass soon
          scheduleHotFlush();
        });
        rl.on("close", () => {
          // no-op; exit handler will decide restart
        });
      }

      proc.on("exit", (code, sig) => {
        if (verbose) console.warn(`ssh ${tag} exited: code=${code} sig=${sig}`);
        if (!restarting) {
          restarting = true;
          setTimeout(() => {
            restarting = false;
            launch();
          }, 1500);
        }
      });

      proc.on("error", (err) => {
        if (verbose) console.warn(`ssh ${tag} error:`, err?.message || err);
      });
    };

    launch();

    return {
      stop: () => {
        restarting = false;
        if (proc?.pid) {
          try {
            proc.kill("SIGINT");
          } catch {}
        }
      },
    };
  }

  // Pipe: ssh scan --emit-delta  →  ccsync ingest --db <local.db>
  function splitCmd(s: string): string[] {
    return s.trim().split(/\s+/);
  }

  async function sshScanIntoMirror(params: {
    host: string;
    remoteScanCmd: string;
    root: string;
    localDb: string;
  }): Promise<{
    code: number | null;
    ms: number;
    ok: boolean;
    lastZero: boolean;
  }> {
    const t0 = Date.now();
    const sshArgs = [
      "-C",
      params.host,
      ...splitCmd(params.remoteScanCmd),
      "--root",
      params.root,
      "--emit-delta",
    ];
    if (verbose) {
      console.log("$ ssh", sshArgs.join(" "));
    }

    const sshP = spawn("ssh", sshArgs, {
      stdio: ["ignore", "pipe", verbose ? "inherit" : "ignore"],
    });

    const ingestArgs = [
      "ingest",
      "--db",
      params.localDb,
      "--root",
      params.root,
    ];
    if (verbose) console.log("ccsync", ingestArgs.join(" "));
    const ingestP = spawn("ccsync", ingestArgs, {
      stdio: [
        "pipe",
        verbose ? "inherit" : "ignore",
        verbose ? "inherit" : "ignore",
      ],
    });

    if (sshP.stdout && ingestP.stdin) {
      sshP.stdout.pipe(ingestP.stdin);
    }

    const wait = (p: ChildProcess) =>
      new Promise<number | null>((resolve) => p.on("exit", (c) => resolve(c)));

    const [sshCode, ingestCode] = await Promise.all([
      wait(sshP),
      wait(ingestP),
    ]);
    const ok = sshCode === 0 && ingestCode === 0;
    return {
      code: ok ? 0 : (sshCode ?? ingestCode),
      ms: Date.now() - t0,
      ok,
      lastZero: ok,
    };
  }

  // Remote delta stream (watch): ssh "<remoteWatchCmd> --root <root> --emit-delta"
  // tee stdout to: (a) local ingest, and (b) our line-reader for microSync cues.
  function startRemoteDeltaStream(side: "alpha" | "beta") {
    const host = side === "alpha" ? alphaHost : betaHost;
    if (!host) return null; // only for remote sides
    const root = side === "alpha" ? alphaRoot : betaRoot;
    const localDb = side === "alpha" ? alphaDb : betaDb;

    const sshArgs = [
      "-C",
      host,
      ...splitCmd(remoteWatchCmd),
      "--root",
      root,
      "--emit-delta",
    ];
    if (verbose) console.log("$ ssh", sshArgs.join(" "));
    const sshP = spawn("ssh", sshArgs, {
      stdio: ["ignore", "pipe", "inherit"],
    });

    const ingestArgs = ["ingest", "--db", localDb, "--root", root];
    const ingestP = spawn("ccsync", ingestArgs, {
      stdio: [
        "pipe",
        verbose ? "inherit" : "ignore",
        verbose ? "inherit" : "ignore",
      ],
    });

    // tee
    const tee = new PassThrough();
    if (sshP.stdout) {
      sshP.stdout.pipe(tee);
      tee.pipe(ingestP.stdin!);
    }

    // Also parse NDJSON to kick microSync:
    const rl = readline.createInterface({ input: tee });
    rl.on("line", (line) => {
      if (!line) return;
      try {
        const evt = JSON.parse(line);
        // Expect {path, deleted? ...}; convert to rpath for this side.
        if (!evt.path) return;
        const p = String(evt.path);
        // event path might be absolute; normalize to relative under `root`
        let r = p.startsWith(root) ? p.slice(root.length) : p;
        if (r.startsWith("/")) r = r.slice(1);
        if (!r) return;
        if (side === "alpha") {
          hotAlpha.add(r);
        } else {
          hotBeta.add(r);
        }
        scheduleHotFlush();
      } catch {
        /* ignore malformed */
      }
    });

    const kill = () => {
      try {
        rl.close();
      } catch {}
      try {
        ingestP.stdin?.end();
      } catch {}
      try {
        ingestP.kill("SIGINT");
      } catch {}
      try {
        sshP.kill("SIGINT");
      } catch {}
    };

    // If either dies, kill the other.
    sshP.on("exit", () => kill());
    ingestP.on("exit", () => kill());

    return { sshP, ingestP, kill };
  }

  // Build rsync endpoints + transport
  function rsyncRoots(
    fromRoot: string,
    fromHost: string | undefined,
    toRoot: string,
    toHost: string | undefined,
  ) {
    const slash = (s: string) => (s.endsWith("/") ? s : s + "/");
    const from = fromHost ? `${fromHost}:${slash(fromRoot)}` : slash(fromRoot);
    const to = toHost ? `${toHost}:${slash(toRoot)}` : slash(toRoot);
    const transport = fromHost || toHost ? (["-e", "ssh"] as string[]) : [];
    return { from, to, transport };
  }

  function requestSoon(reason: string) {
    pending = true;
    nextDelayMs = clamp(
      Math.min(nextDelayMs, 3000),
      MIN_INTERVAL_MS,
      MAX_INTERVAL_MS,
    );
    log("info", "scheduler", `event-triggered rescan scheduled: ${reason}`);
  }

  // ---------- hot (realtime) sets ----------
  const hotAlpha = new Set<string>(); // rpaths relative to alphaRoot
  const hotBeta = new Set<string>(); // rpaths relative to betaRoot
  let hotTimer: NodeJS.Timeout | null = null;

  function scheduleHotFlush() {
    if (hotTimer) return;
    hotTimer = setTimeout(async () => {
      hotTimer = null;
      if (hotAlpha.size === 0 && hotBeta.size === 0) return;
      const rpathsAlpha = Array.from(hotAlpha);
      const rpathsBeta = Array.from(hotBeta);
      hotAlpha.clear();
      hotBeta.clear();
      try {
        await microSync(rpathsAlpha, rpathsBeta);
      } catch (e: any) {
        log("warn", "realtime", "microSync failed", {
          err: String(e?.message || e),
        });
      } finally {
        if (hotAlpha.size || hotBeta.size) {
          scheduleHotFlush(); // run another micro pass if more landed
        }
        requestSoon("micro-sync complete");
      }
    }, MICRO_DEBOUNCE_MS);
  }

  const lastPush = new Map<string, number>(); // rpath -> ts
  function keepFresh(rpaths: string[]) {
    const now = Date.now();
    const out: string[] = [];
    for (const r of rpaths) {
      const t = lastPush.get(r) || 0;
      if (now - t >= COOLDOWN_MS) {
        out.push(r);
        lastPush.set(r, now);
      }
    }
    return out;
  }

  // ---------- HotWatchManager ----------
  class HotWatchManager {
    private map = new Map<string, { watcher: FSWatcher; expiresAt: number }>();
    private lru: string[] = []; // oldest first

    constructor(
      // @ts-ignore
      private side: "alpha" | "beta",
      private root: string,
      private onHotEvent: (abs: string, ev: string) => void,
    ) {}

    size() {
      return this.map.size;
    }

    async add(rdir: string) {
      rdir = rdir === "" ? "." : rdir;
      const anchorAbs = norm(path.join(this.root, rdir));
      const now = Date.now();

      if (this.map.has(anchorAbs)) {
        this.bump(anchorAbs);
        this.map.get(anchorAbs)!.expiresAt = now + HOT_TTL_MS;
        return;
      }

      const watcher = chokidar.watch(anchorAbs, {
        persistent: true,
        ignoreInitial: true,
        depth: HOT_DEPTH,
        awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
      });

      const handler = async (ev: string, abs: string) => {
        this.onHotEvent(abs, ev);
        const d = relDepth(anchorAbs, abs);
        if (d >= HOT_DEPTH && this.map.size < MAX_HOT_WATCHERS) {
          const deeperDir = norm(path.dirname(abs));
          const r = norm(path.relative(this.root, deeperDir));
          if (r && r !== ".") await this.add(r);
        }
        const st = this.map.get(anchorAbs);
        if (st) st.expiresAt = Date.now() + HOT_TTL_MS;
        this.bump(anchorAbs);
      };

      ["add", "change", "unlink", "addDir", "unlinkDir"].forEach((evt) => {
        watcher.on(evt as any, (p: string) => handler(evt, p));
      });

      this.map.set(anchorAbs, { watcher, expiresAt: now + HOT_TTL_MS });
      this.lru.push(anchorAbs);
      await this.evictIfNeeded();
    }

    private bump(abs: string) {
      const i = this.lru.indexOf(abs);
      if (i >= 0) {
        this.lru.splice(i, 1);
        this.lru.push(abs);
      }
    }

    private async evictIfNeeded() {
      const now = Date.now();
      for (const [abs, st] of Array.from(this.map)) {
        if (st.expiresAt <= now) await this.drop(abs);
      }
      while (this.map.size > MAX_HOT_WATCHERS) {
        const victim = this.lru.shift();
        if (!victim) break;
        if (this.map.has(victim)) await this.drop(victim);
      }
    }

    private async drop(abs: string) {
      const st = this.map.get(abs);
      if (!st) return;
      await st.watcher.close().catch(() => {});
      this.map.delete(abs);
      const i = this.lru.indexOf(abs);
      if (i >= 0) this.lru.splice(i, 1);
    }

    async closeAll() {
      await Promise.all(
        Array.from(this.map.values()).map((s) =>
          s.watcher.close().catch(() => {}),
        ),
      );
      this.map.clear();
      this.lru = [];
    }
  }

  // ---------- root watchers (locals only) ----------
  function onAlphaHot(abs: string, evt: string) {
    const r = rel(alphaRoot, abs);
    if (r && (evt === "change" || evt === "add" || evt === "unlink")) {
      hotAlpha.add(r);
      scheduleHotFlush();
    }
  }
  function onBetaHot(abs: string, evt: string) {
    const r = rel(betaRoot, abs);
    if (r && (evt === "change" || evt === "add" || evt === "unlink")) {
      hotBeta.add(r);
      scheduleHotFlush();
    }
  }

  const hotAlphaMgr = alphaIsRemote
    ? null
    : new HotWatchManager("alpha", alphaRoot, onAlphaHot);
  const hotBetaMgr = betaIsRemote
    ? null
    : new HotWatchManager("beta", betaRoot, onBetaHot);

  const shallowAlpha = alphaIsRemote
    ? null
    : chokidar.watch(alphaRoot, {
        persistent: true,
        ignoreInitial: true,
        depth: SHALLOW_DEPTH,
        awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
      });
  const shallowBeta = betaIsRemote
    ? null
    : chokidar.watch(betaRoot, {
        persistent: true,
        ignoreInitial: true,
        depth: SHALLOW_DEPTH,
        awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
      });

  function enableWatch({ watcher, root, mgr, hot }) {
    ["add", "change", "unlink", "addDir", "unlinkDir"].forEach((evt) => {
      watcher.on(evt as any, async (p: string) => {
        const r = rel(root, p);
        const rdir = parentDir(r);

        // Seed/bump a hot watcher for the directory
        if (rdir) await mgr.add(rdir);

        // Promote the *first* event directly to microSync too
        if (r && (evt === "add" || evt === "change" || evt === "unlink")) {
          hot.add(r);
          scheduleHotFlush();
        }
      });
    });
  }

  if (shallowAlpha && hotAlphaMgr) {
    enableWatch({
      watcher: shallowAlpha,
      root: alphaRoot,
      mgr: hotAlphaMgr,
      hot: hotAlpha,
    });
  }

  if (shallowBeta && hotBetaMgr) {
    enableWatch({
      watcher: shallowBeta,
      root: betaRoot,
      mgr: hotBetaMgr,
      hot: hotBeta,
    });
  }

  // Start remote watch agent(s) over SSH (if any side is remote)
  let stopRemoteAlphaWatch: null | (() => void) = null;
  let stopRemoteBetaWatch: null | (() => void) = null;

  if (alphaIsRemote && alphaHost) {
    const h = startSshRemoteWatch({
      side: "alpha",
      host: alphaHost,
      root: alphaRoot,
      remoteWatchCmd, // from your parsed options, e.g. "ccsync watch"
      verbose,
    });
    stopRemoteAlphaWatch = h.stop;
  }

  if (betaIsRemote && betaHost) {
    const h = startSshRemoteWatch({
      side: "beta",
      host: betaHost,
      root: betaRoot,
      remoteWatchCmd, // from your parsed options, e.g. "ccsync watch"
      verbose,
    });
    stopRemoteBetaWatch = h.stop;
  }

  // ---------- seed hot watchers from DB recent_touch (locals only) ----------
  function seedHotFromDb(
    dbPath: string,
    root: string,
    mgr: HotWatchManager | null,
    sinceTs: number | null,
    maxDirs = 256,
  ) {
    if (!mgr) return;
    const sdb = new Database(dbPath);
    try {
      const rows = sinceTs
        ? sdb
            .prepare(
              `SELECT path FROM recent_touch WHERE ts >= ? ORDER BY ts DESC LIMIT ?`,
            )
            .all(sinceTs, maxDirs * 8)
        : sdb
            .prepare(`SELECT path FROM recent_touch ORDER BY ts DESC LIMIT ?`)
            .all(maxDirs * 8);

      const dirs = rows
        .map((r: any) => parentDir(norm(path.relative(root, r.path))))
        .filter(Boolean);
      const covered = minimalCover(dirs).slice(0, maxDirs);
      covered.forEach((d) => mgr.add(d));
      if (verbose)
        log(
          "info",
          "watch",
          `seeded ${covered.length} hot dirs from ${dbPath}`,
        );
    } catch {
      // table may not exist yet; ignore
    } finally {
      sdb.close();
    }
  }

  // ---------- realtime micro-sync (SSH-aware endpoints) ----------
  async function microSync(rpathsAlpha: string[], rpathsBeta: string[]) {
    const setA = new Set(rpathsAlpha);
    const setB = new Set(rpathsBeta);

    const toBeta: string[] = [];
    const toAlpha: string[] = [];

    const touched = new Set<string>([...setA, ...setB]);
    for (const r of touched) {
      const aTouched = setA.has(r);
      const bTouched = setB.has(r);
      if (aTouched && bTouched) {
        if (prefer === "alpha") toBeta.push(r);
        else toAlpha.push(r);
      } else if (aTouched) {
        toBeta.push(r);
      } else {
        toAlpha.push(r);
      }
    }

    async function keepFilesLocal(root: string, rpaths: string[]) {
      const out: string[] = [];
      for (const r of rpaths) {
        try {
          const st = await fsLstat(path.join(root, r));
          if (st.isFile()) out.push(r);
        } catch {
          /* file might have vanished; ignore */
        }
      }
      return out;
    }

    // For remote side we cannot lstat; push as-is (rsync will handle).
    const toBetaFiles = alphaIsRemote
      ? keepFresh(toBeta)
      : keepFresh(await keepFilesLocal(alphaRoot, toBeta));
    const toAlphaFiles = betaIsRemote
      ? keepFresh(toAlpha)
      : keepFresh(await keepFilesLocal(betaRoot, toAlpha));

    if (toBetaFiles.length === 0 && toAlphaFiles.length === 0) return;

    const tmp = await mkdtemp(path.join(tmpdir(), "micro-plan-"));
    try {
      const listToBeta = path.join(tmp, "toBeta.list");
      const listToAlpha = path.join(tmp, "toAlpha.list");

      await writeFile(listToBeta, join0(toBetaFiles));
      await writeFile(listToAlpha, join0(toAlphaFiles));

      if (await fileNonEmpty(listToBeta)) {
        log("info", "realtime", `alpha→beta ${toBetaFiles.length} paths`);
        const { from, to, transport } = rsyncRoots(
          alphaRoot,
          alphaHost,
          betaRoot,
          betaHost,
        );
        await spawnTask(
          "rsync",
          [
            ...(dryRun ? ["-n"] : []),
            ...transport,
            "-a",
            "-I",
            "--relative",
            "--from0",
            `--files-from=${listToBeta}`,
            from,
            to,
          ],
          [0, 23, 24],
        );
      }

      if (await fileNonEmpty(listToAlpha)) {
        log("info", "realtime", `beta→alpha ${toAlphaFiles.length} paths`);
        const { from, to, transport } = rsyncRoots(
          betaRoot,
          betaHost,
          alphaRoot,
          alphaHost,
        );
        await spawnTask(
          "rsync",
          [
            ...(dryRun ? ["-n"] : []),
            ...transport,
            "-a",
            "-I",
            "--relative",
            "--from0",
            `--files-from=${listToAlpha}`,
            from,
            to,
          ],
          [0, 23, 24],
        );
      }
    } finally {
      await rm(tmp, { recursive: true, force: true });
    }
  }

  // ---------- full cycle ----------
  async function oneCycle(): Promise<void> {
    running = true;
    const t0 = Date.now();

    // Scan alpha
    const tAlphaStart = Date.now();
    log(
      "info",
      "scan",
      `alpha: ${alphaRoot}${alphaHost ? ` @ ${alphaHost}` : ""}`,
    );
    const a = alphaIsRemote
      ? await sshScanIntoMirror({
          host: alphaHost!,
          remoteScanCmd,
          root: alphaRoot,
          localDb: alphaDb,
        })
      : await spawnTask("ccsync", [
          "scan",
          "--root",
          alphaRoot,
          "--db",
          alphaDb,
        ]);

    seedHotFromDb(alphaDb, alphaRoot, hotAlphaMgr, tAlphaStart, 256);

    // Scan beta
    const tBetaStart = Date.now();
    log("info", "scan", `beta: ${betaRoot}${betaHost ? ` @ ${betaHost}` : ""}`);
    const b = betaIsRemote
      ? await sshScanIntoMirror({
          host: betaHost!,
          remoteScanCmd,
          root: betaRoot,
          localDb: betaDb,
        })
      : await spawnTask("ccsync", ["scan", "--root", betaRoot, "--db", betaDb]);

    seedHotFromDb(betaDb, betaRoot, hotBetaMgr, tBetaStart, 256);

    // Merge/rsync (full)
    log("info", "merge", `prefer=${prefer} dryRun=${dryRun}`);
    const mArgs = [
      "merge",
      "--alpha-root",
      alphaRoot,
      "--beta-root",
      betaRoot,
      "--alpha-db",
      alphaDb,
      "--beta-db",
      betaDb,
      "--base-db",
      baseDb,
      "--prefer",
      prefer,
    ];
    if (alphaHost) mArgs.push("--alpha-host", alphaHost);
    if (betaHost) mArgs.push("--beta-host", betaHost);
    if (dryRun) mArgs.push("--dry-run");
    if (verbose) mArgs.push("--verbose");

    const m = await spawnTask("ccsync", mArgs);

    const ms = Date.now() - t0;
    lastCycleMs = ms;
    log("info", "scheduler", `cycle complete in ${ms} ms`, {
      scanAlphaMs: a.ms,
      scanBetaMs: b.ms,
      mergeMs: m.ms,
      codes: { a: a.code, b: b.code, m: m.code },
    });

    // Backoff on merge errors
    if (m.code && m.code !== 0) {
      const code = m.code ?? -1;
      const warn = code === 23 || code === 24;
      const enospc = code === 28;
      if (enospc) {
        log("error", "rsync", "ENOSPC; backoff", { code });
        backoffMs = Math.min(
          backoffMs ? backoffMs * 2 : 10_000,
          MAX_BACKOFF_MS,
        );
      } else if (warn) {
        log("warn", "rsync", "partial; backoff a bit", { code });
        backoffMs = Math.min(backoffMs ? backoffMs + 5_000 : 5_000, 60_000);
      } else {
        log("error", "rsync", "unexpected error; backoff", { code });
        backoffMs = Math.min(
          backoffMs ? backoffMs * 2 : 10_000,
          MAX_BACKOFF_MS,
        );
      }
    } else backoffMs = 0;

    running = false;
  }

  let remoteStreams: Array<{ kill: () => void }> = [];

  async function loop() {
    // Start remote micro-streams (if any) once, before the loop
    const alphaStream = alphaIsRemote ? startRemoteDeltaStream("alpha") : null;
    const betaStream = betaIsRemote ? startRemoteDeltaStream("beta") : null;
    remoteStreams = [alphaStream, betaStream].filter(Boolean) as any[];

    while (true) {
      if (!running) {
        pending = false;
        await oneCycle();
        const baseNext = clamp(
          lastCycleMs * 2,
          MIN_INTERVAL_MS,
          MAX_INTERVAL_MS,
        );
        nextDelayMs =
          baseNext + (backoffMs || Math.floor(Math.random() * JITTER_MS));
      }

      if (pending) {
        nextDelayMs = clamp(1500, MIN_INTERVAL_MS, MAX_INTERVAL_MS);
        pending = false;
        continue; // loop immediately
      }

      log(
        "info",
        "scheduler",
        "watching",
        `next full scan in ${nextDelayMs} ms`,
      );
      await new Promise((r) => setTimeout(r, nextDelayMs));
    }
  }

  log("info", "scheduler", "starting", {
    alphaRoot,
    betaRoot,
    alphaDb,
    betaDb,
    baseDb,
    prefer,
    dryRun,
    verbose,
    alphaHost,
    betaHost,
    alphaRemoteDb,
    betaRemoteDb,
    remoteScanCmd,
    remoteWatchCmd,
    MAX_HOT_WATCHERS,
    HOT_TTL_MS,
    SHALLOW_DEPTH,
    HOT_DEPTH,
  });

  // Exit/cleanup
  const cleanup = async () => {
    try {
      await hotAlphaMgr?.closeAll();
    } catch {}
    try {
      await hotBetaMgr?.closeAll();
    } catch {}
    for (const s of remoteStreams) {
      try {
        s.kill();
      } catch {}
    }
    try {
      db.close();
    } catch {}
    try {
      stopRemoteAlphaWatch?.();
    } catch {}
    try {
      stopRemoteBetaWatch?.();
    } catch {}
  };
  const onSig = async () => {
    await cleanup();
    process.exit(0);
  };
  process.once("SIGINT", onSig);
  process.once("SIGTERM", onSig);

  await loop();
}

cliEntrypoint<SchedulerOptions>(
  import.meta.url,
  buildProgram,
  async (opts) => await runScheduler(cliOptsToSchedulerOptions(opts)),
  {
    label: "scheduler",
  },
);
