// --- imports near the top of cli.ts (add if not already present)
import { Command, Option } from "commander";
import fs from "node:fs";
import { spawn } from "node:child_process";
import {
  ensureSessionDb,
  getSessionDbPath,
  getReflectSyncHome,
  selectSessions,
  parseSelectorTokens,
  loadSessionById,
  setDesiredState,
  setActualState,
  deriveSessionPaths,
  recordHeartbeat,
} from "./session-db.js";
import { stopPid, terminateSession, newSession } from "./session-manage.js";
import { registerSessionStatus } from "./session-status.js";
import { registerSessionMonitor } from "./session-monitor.js";
import { registerSessionFlush } from "./session-flush.js";
import { argsJoin } from "./remote.js";
import { defaultHashAlg, listSupportedHashes } from "./hash.js";

// Collect `-l/--label k=v` repeatables
function collectLabels(val: string, acc: string[]) {
  acc.push(val);
  return acc;
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
    "--compress",
    row.compress ?? "auto",
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

  console.log(`${process.execPath} ${argsJoin(args)}`);
  // Important: keep stdio detached so it runs in background (daemon-esque)
  const child = spawn(process.execPath, args, {
    stdio: "ignore",
    detached: true,
    env: process.env,
  });
  child.unref();
  return child.pid ?? 0;
}

// ---------- add the "session" subtree ----------
export function registerSessionCommands(program: Command) {
  const session = program
    .command("session")
    .description("Manage sync sessions")
    .option("--verbose")
    .option(
      "--session-db <file>",
      "override path to sessions.db",
      getSessionDbPath(),
    );

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
    .addOption(
      new Option("--compress <algorithm>", "rsync compression algorithm")
        .choices(["auto", "zstd", "lz4", "zlib", "zlibx", "none"])
        .default("auto"),
    )
    .option(
      "--compress-level <level>",
      "options -- zstd: -131072..22 (3 default), zlib/zlibx: 1..9 (6 default), lz4: 0",
      "",
    )
    .action(async (alphaSpec: string, betaSpec: string, opts: any) => {
      const sessionDb =
        program.getOptionValue("sessionDb") || getSessionDbPath();
      let id;
      try {
        id = await newSession({ alphaSpec, betaSpec, ...opts, sessionDb });
      } catch (err) {
        console.error("failed to create session", err);
        return;
      }
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
    .option("--force", "terminate even if can't delete remote db")
    .argument("<id...>", "session id(s)")
    .action(async (ids: string[], options: { force?: boolean }, command) => {
      const sessionDb =
        program.getOptionValue("sessionDb") || getSessionDbPath();
      const opts = { ...command.optsWithGlobals(), ...options };

      for (const id0 of ids) {
        const id = Number(id0);
        await terminateSession({
          id,
          verbose: opts.verbose,
          force: opts.force,
          sessionDb,
        });
        console.log(`terminated session ${id}`);
      }
    });

  registerSessionStatus(session);
}
