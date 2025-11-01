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
import { fmtAgo, registerSessionStatus } from "./session-status.js";
import { registerSessionMonitor } from "./session-monitor.js";
import { registerSessionFlush } from "./session-flush.js";
import { argsJoin } from "./remote.js";
import { defaultHashAlg, listSupportedHashes } from "./hash.js";
import {
  ConsoleLogger,
  LOG_LEVELS,
  parseLogLevel,
  type LogLevel,
} from "./logger.js";
import { fetchSessionLogs, type SessionLogRow } from "./session-logs.js";
import { AsciiTable3, AlignmentEnum } from "ascii-table3";
import { fmtLocalPath } from "./session-status.js";

// Collect `-l/--label k=v` repeatables
function collectLabels(val: string, acc: string[]) {
  acc.push(val);
  return acc;
}

function clampPositive(value: number | undefined, fallback: number): number {
  const n = Number(value);
  if (Number.isFinite(n) && n > 0) return n;
  return fallback;
}

function parseLogLevelOption(raw?: string): LogLevel | undefined {
  if (!raw) return undefined;
  const lvl = raw.toLowerCase();
  if (!LOG_LEVELS.includes(lvl as LogLevel)) {
    console.error(
      `invalid level '${raw}', expected one of ${LOG_LEVELS.join(", ")}`,
    );
    process.exit(1);
  }
  return lvl as LogLevel;
}

function renderRows(
  rows: SessionLogRow[],
  { json, absolute }: { json: boolean; absolute: boolean },
) {
  for (const row of rows) {
    if (json) {
      const payload = {
        id: row.id,
        session_id: row.session_id,
        ts: row.ts,
        level: row.level,
        scope: row.scope,
        message: row.message,
        meta: row.meta ?? null,
      };
      console.log(JSON.stringify(payload));
      continue;
    }
    const scope = row.scope ? ` [${row.scope}]` : "";
    const meta =
      row.meta && Object.keys(row.meta).length
        ? ` ${JSON.stringify(row.meta)}`
        : "";
    console.log(
      `(${absolute ? new Date(row.ts).toISOString() : fmtAgo(row.ts)}) ${row.level.toUpperCase()}${scope} ${row.message}${meta}`,
    );
  }
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

export function registerSessionCommands(program: Command) {
  const ensureDbPath = (path: string) => {
    const db = ensureSessionDb(path);
    db.close();
    return path;
  };

  const addSessionDbOption = (cmd: Command) =>
    cmd.option(
      "--session-db <file>",
      "override path to sessions.db",
      getSessionDbPath(),
    );

  const resolveSessionDb = (
    opts: { sessionDb?: string },
    command: Command,
  ): string => {
    if (opts.sessionDb) return ensureDbPath(opts.sessionDb);
    const globals = command.optsWithGlobals() as { sessionDb?: string };
    if (globals.sessionDb) return ensureDbPath(globals.sessionDb);
    return ensureDbPath(getSessionDbPath());
  };

  const getLogLevel = () =>
    parseLogLevel(program.getOptionValue("logLevel") as string | undefined, "info");

  addSessionDbOption(
    program
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
      .action(
        async (
          alphaSpec: string,
          betaSpec: string,
          opts: any,
          command: Command,
        ) => {
          const sessionDb = resolveSessionDb(opts, command);
          const cliLogger = new ConsoleLogger(getLogLevel());
          try {
            const id = await newSession({
              alphaSpec,
              betaSpec,
              ...opts,
              sessionDb,
              logger: cliLogger,
            });
            console.log(
              `created session ${id}${opts.name ? ` (${opts.name})` : ""}`,
            );
            if (!opts.paused) {
              const row = loadSessionById(sessionDb, id)!;
              const pid = spawnSchedulerForSession(sessionDb, row);
              setDesiredState(sessionDb, id, "running");
              setActualState(sessionDb, id, pid ? "running" : "error");
              row.scheduler_pid = pid;
              console.log(`started session ${id} (pid ${pid})`);
            }
          } catch (err) {
            console.error("failed to create session", err);
          }
        },
      ),
  );

  addSessionDbOption(
    program
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
      .action((opts: any, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        const sels = parseSelectorTokens(opts.selector || []);
        const rows = selectSessions(sessionDb, sels);

        if (!rows.length) {
          console.log("no sessions");
          return;
        }

        const table = new AsciiTable3("Sessions")
          .setHeading(
            "ID",
            "Name",
            "State (actual/desired)",
            "Prefer",
            "Alpha",
            "Beta",
            "PID",
          )
          .setStyle("unicode-round");

        const alignments = [
          AlignmentEnum.LEFT,
          AlignmentEnum.LEFT,
          AlignmentEnum.LEFT,
          AlignmentEnum.LEFT,
          AlignmentEnum.LEFT,
          AlignmentEnum.LEFT,
          AlignmentEnum.LEFT,
        ];
        alignments.forEach((align, idx) => table.setAlign(idx, align));

        for (const r of rows) {
          const alphaPath = r.alpha_host
            ? `${r.alpha_host}:${r.alpha_root}`
            : fmtLocalPath(r.alpha_root);
          const betaPath = r.beta_host
            ? `${r.beta_host}:${r.beta_root}`
            : fmtLocalPath(r.beta_root);

          table.addRow(
            String(r.id),
            r.name ?? "-",
            `${r.actual_state}/${r.desired_state}`,
            r.prefer,
            alphaPath,
            betaPath,
            r.scheduler_pid ? String(r.scheduler_pid) : "-",
          );
        }

        console.log(table.toString());
      }),
  );

  addSessionDbOption(
    program
      .command("logs")
      .description("Show recent logs for a session")
      .argument("<id>", "session id")
      .option("--tail <n>", "number of log entries to display", (v: string) =>
        Number.parseInt(v, 10),
      )
      .option("--since <ms>", "only show logs with ts >= ms since epoch", (v) =>
        Number.parseInt(v, 10),
      )
      .option("--absolute", "show times as absolute timestamps", false)
      .option(
        "--level <level>",
        `minimum log level (${LOG_LEVELS.join(", ")})`,
        (v: string) => v.trim().toLowerCase(),
      )
      .option("-f, --follow", "follow log output", false)
      .option("--json", "emit newline-delimited JSON", false)
      .action(async (idStr: string, opts: any, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        const id = Number(idStr);
        if (!Number.isFinite(id) || id <= 0) {
          console.error("invalid session id", idStr);
          return;
        }
        const minLevel = parseLogLevelOption(opts.level);
        const tail = clampPositive(opts.tail, 200);
        const sinceTs =
          opts.since != null && Number.isFinite(Number(opts.since))
            ? Number(opts.since)
            : undefined;
        let rows = fetchSessionLogs(sessionDb, id, {
          limit: tail,
          minLevel,
          sinceTs,
          order: "desc",
        }).reverse();

        let lastId = 0;
        if (!rows.length) {
          if (!opts.follow) {
            console.log("no logs");
            return;
          }
        } else {
          renderRows(rows, { json: !!opts.json, absolute: !!opts.absolute });
          lastId = rows[rows.length - 1].id;
        }

        if (!opts.follow) return;

        const intervalMs = clampPositive(
          Number(process.env.RFSYNC_LOG_FOLLOW_INTERVAL ?? 1000),
          1000,
        );

        await new Promise<void>((resolve) => {
          const tick = () => {
            rows = fetchSessionLogs(sessionDb, id, {
              afterId: lastId,
              minLevel,
              order: "asc",
            });
            if (rows.length) {
              renderRows(rows, { json: !!opts.json, absolute: !!opts.absolute });
              lastId = rows[rows.length - 1].id;
            }
          };
          const timer = setInterval(tick, intervalMs);
          const stop = () => {
            clearInterval(timer);
            resolve();
          };
          process.once("SIGINT", stop);
          process.once("SIGTERM", stop);
        });
      }),
  );

  addSessionDbOption(
    program
      .command("pause")
      .description("Pause sync for one or more sessions")
      .argument("<id...>", "session id(s)")
      .action((ids: string[], opts: { sessionDb?: string }, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
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
      }),
  );

  addSessionDbOption(
    program
      .command("resume")
      .description("Resume sync for one or more sessions")
      .argument("<id...>", "session id(s)")
      .action((ids: string[], opts: { sessionDb?: string }, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
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
      }),
  );

  addSessionDbOption(
    program
      .command("reset")
      .description("Reset sync for one or more sessions")
      .argument("<id...>", "session id(s)")
      .action((ids: string[]) => {
        console.log("reset: TODO", ids);
      }),
  );

  addSessionDbOption(
    program
      .command("terminate")
      .description("Stop and remove all session state")
      .option("--force", "terminate even if can't delete remote db")
      .argument("<id...>", "session id(s)")
      .action(
        async (
          ids: string[],
          options: { force?: boolean; sessionDb?: string },
          command: Command,
        ) => {
          const sessionDb = resolveSessionDb(options, command);
          const cliLogger = new ConsoleLogger(getLogLevel());
          for (const id0 of ids) {
            const id = Number(id0);
            await terminateSession({
              id,
              logger: cliLogger,
              force: options.force,
              sessionDb,
            });
            console.log(`terminated session ${id}`);
          }
        },
      ),
  );

  registerSessionMonitor(program);
  registerSessionFlush(program);
  registerSessionStatus(program);
}
