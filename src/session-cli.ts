// --- imports near the top of cli.ts (add if not already present)
import { Command, Option } from "commander";
import path from "node:path";
import os from "node:os";
import fs from "node:fs";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  ensureSessionDb,
  getSessionDbPath,
  getCcsyncHome,
  createSession,
  selectSessions,
  parseSelectorTokens,
  materializeSessionPaths,
  loadSessionById,
  setDesiredState,
  setActualState,
  deriveSessionPaths,
  recordHeartbeat,
  deleteSessionById,
} from "./session-db.js";
import { registerSessionStatus } from "./session-status.js";

// ---------- small utils ----------
function expandHome(p: string): string {
  if (!p) return p;
  if (p.startsWith("~")) return path.join(os.homedir(), p.slice(1));
  return p;
}

type Endpoint = { root: string; host?: string };

// Minimal mutagen-like endpoint parsing.
// - local: "~/work" or "/abs/path" -> {root: absolute path, host: undefined}
// - remote: "user@host:~/work" or "host:/abs" -> {root: as-given (keep ~ for remote), host}
// Windows drive-letter paths like "C:\x" won’t be mistaken as remote (colon rule).
function parseEndpoint(spec: string): Endpoint {
  // treat as remote if there is a colon AND (contains "@", or starts with something that clearly looks like a host)
  const looksLikeWindows = /^[A-Za-z]:[\\/]/.test(spec);
  const colon = spec.indexOf(":");
  if (!looksLikeWindows && colon > 0) {
    const host = spec.slice(0, colon);
    const root = spec.slice(colon + 1);
    if (host.includes("@") || /^[a-z0-9_.-]+$/.test(host)) {
      // keep ~ unexpanded for remote; resolve relative pieces a bit
      return { host, root: root || "." };
    }
  }
  // local
  const abs = path.resolve(expandHome(spec));
  return { root: abs };
}

// Collect `-l/--label k=v` repeatables
function collectLabels(val: string, acc: string[]) {
  acc.push(val);
  return acc;
}

function parseLabelPairs(pairs: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const p of pairs || []) {
    const i = p.indexOf("=");
    if (i <= 0) throw new Error(`Invalid label "${p}", expected k=v`);
    const k = p.slice(0, i).trim();
    const v = p.slice(i + 1).trim();
    if (!k) throw new Error(`Invalid label key in "${p}"`);
    out[k] = v;
  }
  return out;
}

// Find dist/scheduler.js next to the compiled CLI
const SCHED = fileURLToPath(new URL("./scheduler.js", import.meta.url));

// Spawn scheduler for a session row
function spawnSchedulerForSession(sessionDb: string, row: any): number {
  // Ensure DB file paths exist (materialize if needed)
  const home = getCcsyncHome();
  const sessDir = deriveSessionPaths(row.id, home).dir;
  fs.mkdirSync(sessDir, { recursive: true });

  const args: string[] = [
    SCHED,
    "--alpha-root",
    row.alpha_root,
    "--beta-root",
    row.beta_root,
    "--alpha-db",
    row.alpha_db,
    "--beta-db",
    row.beta_db,
    "--base-db",
    row.base_db,
    "--prefer",
    row.prefer,
  ];

  if (row.alpha_host) {
    args.push("--alpha-host", row.alpha_host);
  }
  if (row.beta_host) {
    args.push("--beta-host", row.beta_host);
  }
  if (row.alpha_remote_db) {
    args.push("--alpha-remote-db", row.alpha_remote_db);
  }
  if (row.beta_remote_db) {
    args.push("--beta-remote-db", row.beta_remote_db);
  }
  if (row.remote_scan_cmd) {
    args.push("--remote-scan-cmd", row.remote_scan_cmd);
  }
  if (row.remote_watch_cmd) {
    args.push("--remote-watch-cmd", row.remote_watch_cmd);
  }
  args.push("--session-id", String(row.id));
  args.push("--session-db", sessionDb);

  console.log(`${process.execPath} ${args.join(" ")}`);
  // Important: keep stdio detached so it runs in background (daemon-esque)
  const child = spawn(process.execPath, args, {
    stdio: "ignore",
    detached: true,
    env: process.env,
  });
  child.unref();
  return child.pid ?? 0;
}

// Attempt to stop a scheduler by PID
function stopPid(pid: number): boolean {
  if (!pid) return false;
  try {
    process.kill(pid, "SIGTERM");
    return true;
  } catch {
    return false;
  }
}

// ---------- add the "session" subtree ----------
export function registerSessionCommands(program: Command) {
  program.option(
    "--session-db <file>",
    "override path to sessions.db",
    getSessionDbPath(),
  );

  const session = program
    .command("session")
    .description("Manage sync sessions");

  // Global override for the sessions DB location (optional)
  session.hook("preAction", (thisCmd) => {
    const custom = (thisCmd.parent?.getOptionValue("sessionDb") ??
      program.getOptionValue("sessionDb")) as string | undefined;
    if (custom) ensureSessionDb(custom);
    else ensureSessionDb(getSessionDbPath());
  });

  // `ccsync session create [options] <alpha> <beta>`
  session
    .command("create")
    .description("Create a new sync session (mutagen-like endpoint syntax)")
    .argument("<alpha>", "alpha endpoint (local path or user@host:path)")
    .argument("<beta>", "beta endpoint (local path or user@host:path)")
    .option("--name <name>", "human-friendly session name")
    .addOption(
      new Option("--prefer <side>", "conflict preference")
        .choices(["alpha", "beta"])
        .default("alpha"),
    )
    .option("-l, --label <k=v>", "add a label", collectLabels, [] as string[])
    .option("--start", "start scheduler immediately", true)
    .action((alphaSpec: string, betaSpec: string, opts: any) => {
      const sessionDb =
        program.getOptionValue("sessionDb") || getSessionDbPath();
      ensureSessionDb(sessionDb);

      const a = parseEndpoint(alphaSpec);
      const b = parseEndpoint(betaSpec);

      // Only expand ~ locally; leave ~ as-is for remote paths (expanded remotely).
      const alpha_root = a.host
        ? a.root || "."
        : path.resolve(expandHome(a.root));
      const beta_root = b.host
        ? b.root || "."
        : path.resolve(expandHome(b.root));

      const id = createSession(
        sessionDb,
        {
          name: opts.name ?? null,
          alpha_root,
          beta_root,
          prefer: (opts.prefer || "alpha").toLowerCase(),
          alpha_host: a.host ?? null,
          beta_host: b.host ?? null,
          // Remote DB defaults; allow overrides later via patch command if desired
          alpha_remote_db: a.host ? "~/.cache/cocalc-sync/alpha.db" : null,
          beta_remote_db: b.host ? "~/.cache/cocalc-sync/beta.db" : null,
          remote_scan_cmd: "ccsync scan",
          remote_watch_cmd: "ccsync watch",
        },
        parseLabelPairs(opts.label || []),
      );

      // Create per-session dir & DB files; write back into row
      const paths = materializeSessionPaths(sessionDb, id);
      // (No need to echo here; start will read from DB)

      console.log(`created session ${id}${opts.name ? ` (${opts.name})` : ""}`);

      if (opts.start) {
        const row = loadSessionById(sessionDb, id)!;
        // merge materialized DBs into row for spawn
        row.base_db = paths.base_db;
        row.alpha_db = paths.alpha_db;
        row.beta_db = paths.beta_db;
        const pid = spawnSchedulerForSession(sessionDb, row);
        setDesiredState(sessionDb, id, "running");
        setActualState(sessionDb, id, pid ? "running" : "error");
        // Also persist PID
        // (minor convenience; you may add a dedicated update if you like)
        // @ts-ignore
        row.scheduler_pid = pid;
        console.log(`started session ${id} (pid ${pid})`);
      }
    });

  // `ccsync session list [-s/--selector <expr>]…`
  session
    .command("list")
    .description("List sessions (filterable by label selectors)")
    .option(
      "-s, --selector <expr>",
      "label selector (k=v | k!=v | k | !k); repeatable",
      (val, acc) => {
        acc.push(val);
        return acc;
      },
      [] as string[],
    )
    .action((opts: any) => {
      const sessionDb =
        program.getOptionValue("sessionDb") || getSessionDbPath();
      const sels = parseSelectorTokens(opts.selector || []);
      const rows = selectSessions(sessionDb, sels);

      if (!rows.length) {
        console.log("no sessions");
        return;
      }

      for (const r of rows) {
        const left = r.alpha_host
          ? `${r.alpha_host}:${r.alpha_root}`
          : r.alpha_root;
        const right = r.beta_host
          ? `${r.beta_host}:${r.beta_root}`
          : r.beta_root;
        console.log(
          [
            `id=${r.id}`,
            r.name ? `name=${r.name}` : "",
            `state=${r.actual_state}/${r.desired_state}`,
            `prefer=${r.prefer}`,
            `alpha=${left}`,
            `beta=${right}`,
            r.scheduler_pid ? `pid=${r.scheduler_pid}` : "",
          ]
            .filter(Boolean)
            .join("  "),
        );
      }
    });

  // `ccsync session start <id...>`
  session
    .command("start")
    .description("Start scheduler for one or more sessions")
    .argument("<id...>", "session id(s)")
    .action((ids: string[]) => {
      const sessionDb =
        program.getOptionValue("sessionDb") || getSessionDbPath();

      ids.map(Number).forEach((id) => {
        const row = loadSessionById(sessionDb, id);
        if (!row) {
          console.error(`session ${id} not found`);
          return;
        }
        // Ensure per-session DB paths are present
        materializeSessionPaths(sessionDb, id);
        const fresh = loadSessionById(sessionDb, id)!;
        const pid = spawnSchedulerForSession(sessionDb, fresh);
        setDesiredState(sessionDb, id, "running");
        setActualState(sessionDb, id, pid ? "running" : "error");
        if (pid) {
          recordHeartbeat(sessionDb, id, "running", pid);
        }
        console.log(
          pid
            ? `started session ${id} (pid ${pid})`
            : `failed to start session ${id}`,
        );
      });
    });

  // `ccsync session stop <id...>`
  session
    .command("stop")
    .description("Stop scheduler for one or more sessions")
    .argument("<id...>", "session id(s)")
    .action((ids: string[]) => {
      const sessionDb =
        program.getOptionValue("sessionDb") || getSessionDbPath();

      ids.map(Number).forEach((id) => {
        const row = loadSessionById(sessionDb, id);
        if (!row) {
          console.error(`session ${id} not found`);
          return;
        }
        const ok = row.scheduler_pid ? stopPid(row.scheduler_pid) : false;
        setDesiredState(sessionDb, id, "stopped");
        setActualState(sessionDb, id, "stopped");
        console.log(
          ok
            ? `stopped session ${id} (pid ${row.scheduler_pid})`
            : `session ${id} was not running`,
        );
      });
    });

  // `ccsync session terminate <id...>` (stop + remove all session state)
  session
    .command("terminate")
    .description("Stop and remove all session state")
    .argument("<id...>", "session id(s)")
    .action((ids: string[]) => {
      const sessionDb =
        program.getOptionValue("sessionDb") || getSessionDbPath();

      ids.map(Number).forEach((id) => {
        const row = loadSessionById(sessionDb, id);
        if (!row) {
          console.error(`session ${id} not found`);
          return;
        }
        if (row.scheduler_pid) {
          stopPid(row.scheduler_pid);
        }
        setDesiredState(sessionDb, id, "stopped");
        setActualState(sessionDb, id, "stopped");
        const dir = deriveSessionPaths(id, getCcsyncHome()).dir;
        try {
          fs.rmSync(dir, { recursive: true, force: true });
        } catch {}
        deleteSessionById(sessionDb, id);
        console.log(`terminated session ${id}`);
      });
    });

  registerSessionStatus(session);
}
