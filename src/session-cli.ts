// --- imports near the top of cli.ts (add if not already present)
import { Command, Option } from "commander";
import fs from "node:fs";
import { spawn } from "node:child_process";
import {
  ensureSessionDb,
  getSessionDbPath,
  getReflectSyncHome,
  getOrCreateEngineId,
  createSession,
  updateSession,
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
import { registerSessionMonitor } from "./session-monitor.js";
import { registerSessionFlush } from "./session-flush.js";
import { ensureRemoteParentDir } from "./remote.js";
import { CLI_NAME } from "./constants.js";
import { defaultHashAlg, listSupportedHashes } from "./hash.js";

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
      return { host, root: root || "~" };
    }
  }
  return { root: spec };
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

// Spawn scheduler for a session row
function spawnSchedulerForSession(sessionDb: string, row: any): number {
  // Ensure DB file paths exist (materialize if needed)
  const home = getReflectSyncHome();
  const sessDir = deriveSessionPaths(row.id, home).dir;
  fs.mkdirSync(sessDir, { recursive: true });

  const args: string[] = process.env.RFSYNC_BUNDLED ? [] : [process.argv[1]];
  args.push(
    "scheduler",
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
    "--hash",
    row.hash_alg,
  );

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

  // `reflect-sync session create [options] <alpha> <beta>`
  session
    .command("create")
    .description("Create a new sync session (mutagen-like endpoint syntax)")
    .argument("<alpha>", "alpha endpoint (local path or user@host:path)")
    .argument("<beta>", "beta endpoint (local path or user@host:path)")
    .option("-n, --name <name>", "human-friendly session name")
    .addOption(
      new Option("--prefer <side>", "conflict preference")
        .choices(["alpha", "beta"])
        .default("alpha"),
    )
    .option("-l, --label <k=v>", "add a label", collectLabels, [] as string[])
    .option("-p, --paused", "leave session paused (do not sync)", false)
    .addOption(
      new Option("--hash <algorithm>", "content hash algorithm")
        .choices(listSupportedHashes())
        .default(defaultHashAlg()),
    )
    .action(async (alphaSpec: string, betaSpec: string, opts: any) => {
      if (opts.listHashes) {
        console.log(listSupportedHashes().join("\n"));
        return;
      }
      const sessionDb =
        program.getOptionValue("sessionDb") || getSessionDbPath();
      ensureSessionDb(sessionDb);

      const a = parseEndpoint(alphaSpec);
      const b = parseEndpoint(betaSpec);

      const id = createSession(
        sessionDb,
        {
          name: opts.name ?? null,
          alpha_root: a.root,
          beta_root: b.root,
          prefer: (opts.prefer || "alpha").toLowerCase(),
          alpha_host: a.host ?? null,
          beta_host: b.host ?? null,
          hash_alg: opts.hash,
        },
        parseLabelPairs(opts.label || []),
      );

      const engineId = getOrCreateEngineId(getReflectSyncHome()); // persistent local origin id
      // [ ] TODO: should instead use a remote version of getReflectSyncHome here:
      const nsBase = `~/.local/share/${CLI_NAME}/by-origin/${engineId}/sessions/${id}`;

      let alphaRemoteDb: string | null = null;
      let betaRemoteDb: string | null = null;

      if (a.host) alphaRemoteDb = `${nsBase}/alpha.db`;
      if (b.host) betaRemoteDb = `${nsBase}/beta.db`;

      if (alphaRemoteDb || betaRemoteDb) {
        updateSession(sessionDb, id, {
          alpha_remote_db: alphaRemoteDb,
          beta_remote_db: betaRemoteDb,
        });

        // proactively create parent dirs on remotes
        if (a.host && alphaRemoteDb)
          await ensureRemoteParentDir(a.host, alphaRemoteDb, opts.verbose);
        if (b.host && betaRemoteDb)
          await ensureRemoteParentDir(b.host, betaRemoteDb, opts.verbose);
      }

      const paths = materializeSessionPaths(id);
      updateSession(sessionDb, id, paths);

      console.log(`created session ${id}${opts.name ? ` (${opts.name})` : ""}`);
      if (!opts.paused) {
        const row = loadSessionById(sessionDb, id)!;
        const pid = spawnSchedulerForSession(sessionDb, row);
        setDesiredState(sessionDb, id, "running");
        setActualState(sessionDb, id, pid ? "running" : "error");
        // Also persist PID
        row.scheduler_pid = pid;
        console.log(`started session ${id} (pid ${pid})`);
      }
    });

  // `${CLI_NAME} session list [-s/--selector <expr>]…`
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

  registerSessionMonitor(session);

  registerSessionFlush(session);

  // `$CLI_NAME} session pause <id...>`
  session
    .command("pause")
    .description("Pause sync for one or more sessions")
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
        setDesiredState(sessionDb, id, "paused");
        setActualState(sessionDb, id, "paused");
        console.log(
          ok
            ? `paused session ${id} (pid ${row.scheduler_pid})`
            : `session ${id} was not running`,
        );
      });
    });

  // `${CLI_NAME} session resume <id...>`
  session
    .command("resume")
    .description("Resume sync for one or more sessions")
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
        const pid = spawnSchedulerForSession(sessionDb, row);
        setDesiredState(sessionDb, id, "running");
        setActualState(sessionDb, id, pid ? "running" : "error");
        if (pid) {
          recordHeartbeat(sessionDb, id, "running", pid);
        }
        console.log(
          pid
            ? `resumed session ${id} (pid ${pid})`
            : `failed to resume session ${id}`,
        );
      });
    });

  // `${CLI_NAME} session resume <id...>`
  session
    .command("reset")
    .description("Reset sync for one or more sessions")
    .argument("<id...>", "session id(s)")
    .action((ids: string[]) => {
      //const sessionDb =
      //  program.getOptionValue("sessionDb") || getSessionDbPath();

      // [ ] TODO: need to basically do a terminate then create of exactly the session,
      // without code duplication, so might requiore some refactor, or just
      // call this script as a subprocess?
      console.log("reset: TODO", ids);
    });

  // `${CLI_NAME} session terminate <id...>` (stop + remove all session state)
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
        const dir = deriveSessionPaths(id, getReflectSyncHome()).dir;
        try {
          fs.rmSync(dir, { recursive: true, force: true });
        } catch {}
        // [ ] TODO: need to get and delete the REMOTE session via ssh
        // and if that fails, give an error.  We could allow it only
        // with termiante --force.  The point is that otherwise a potentially
        // huge sqlite3 database could be left on the other side, wasting
        // space forever, and the user needs to be aware of this.
        deleteSessionById(sessionDb, id);
        console.log(`terminated session ${id}`);
      });
    });

  registerSessionStatus(session);
}
