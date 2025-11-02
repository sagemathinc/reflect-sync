// --- imports near the top of cli.ts (add if not already present)
import { Command, Option } from "commander";
import {
  ensureSessionDb,
  getSessionDbPath,
  selectSessions,
  parseSelectorTokens,
  loadSessionById,
  resolveSessionRow,
  setDesiredState,
  setActualState,
  recordHeartbeat,
  type SessionRow,
} from "./session-db.js";
import {
  stopPid,
  terminateSession,
  newSession,
  resetSession,
  editSession,
} from "./session-manage.js";
import { fmtAgo, registerSessionStatus } from "./session-status.js";
import { registerSessionMonitor } from "./session-monitor.js";
import { registerSessionSync } from "./session-sync.js";
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
import { spawnSchedulerForSession } from "./session-runner.js";
import { registerSessionDaemon } from "./session-daemon.js";
import { collectIgnoreOption, deserializeIgnoreRules } from "./ignore.js";
import { queryRecent, querySize } from "./session-query.js";
import { diffSession } from "./session-diff.js";

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

function requireSessionRow(sessionDb: string, ref: string): SessionRow {
  const row = resolveSessionRow(sessionDb, ref);
  if (!row) {
    throw new Error(`session '${ref}' not found`);
  }
  return row;
}

function fmtRemoteEndpoint(host: string, port: number | null, root: string) {
  const hostPart = port != null ? `${host}:${port}` : host;
  return `${hostPart}:${root}`;
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
    parseLogLevel(
      program.getOptionValue("logLevel") as string | undefined,
      "info",
    );

  addSessionDbOption(
    program
      .command("create")
      .description(
        "Create a new sync session (mutagen-like endpoints; remote specs accept host[:port]:/path)",
      )
      .argument(
        "<alpha>",
        "alpha endpoint (local path or user@host:[port:]path)",
      )
      .argument("<beta>", "beta endpoint (local path or user@host:[port:]path)")
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
        "-i, --ignore <pattern>",
        "gitignore-style ignore rule (repeat or comma-separated)",
        collectIgnoreOption,
        [] as string[],
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
            const message = err instanceof Error ? err.message : String(err);
            console.error(`failed to create session: ${message}`);
            process.exitCode = 1;
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
      .option("--json", "output JSON instead of a table", false)
      .action((opts: any, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        const sels = parseSelectorTokens(opts.selector || []);
        const rows = selectSessions(sessionDb, sels);

        if (!rows.length) {
          console.log("no sessions");
          return;
        }

        if (opts.json) {
          console.log(JSON.stringify(rows, null, 2));
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
            "Ignores",
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
          AlignmentEnum.LEFT,
        ];
        alignments.forEach((align, idx) => table.setAlign(idx, align));

        for (const r of rows) {
          const alphaPath = r.alpha_host
            ? fmtRemoteEndpoint(r.alpha_host, r.alpha_port, r.alpha_root)
            : fmtLocalPath(r.alpha_root);
          const betaPath = r.beta_host
            ? fmtRemoteEndpoint(r.beta_host, r.beta_port, r.beta_root)
            : fmtLocalPath(r.beta_root);
          const ignoreRules = deserializeIgnoreRules(
            (r as any).ignore_rules ?? null,
          );
          const ignoreSummary = ignoreRules.length
            ? ignoreRules.join(", ")
            : "-";

          table.addRow(
            String(r.id),
            r.name ?? "-",
            `${r.actual_state}/${r.desired_state}`,
            r.prefer,
            alphaPath,
            betaPath,
            r.scheduler_pid ? String(r.scheduler_pid) : "-",
            ignoreSummary,
          );
        }

        console.log(table.toString());
      }),
  );

  const sideOption = () =>
    new Option("--side <side>", "which session side to inspect")
      .choices(["alpha", "beta"])
      .default("alpha");

  const numberFormatter = new Intl.NumberFormat("en-US");

  const humanFileSize = (bytes: number): string => {
    if (!Number.isFinite(bytes)) return "-";
    const sign = bytes < 0 ? -1 : 1;
    let value = Math.abs(bytes);
    const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    let unitIndex = 0;
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex += 1;
    }
    const formatted = value >= 10 ? value.toFixed(1) : value.toFixed(2);
    return `${sign < 0 ? "-" : ""}${formatted.replace(/\.0+$/, "")} ${units[unitIndex]}`;
  };

  const query = program
    .command("query")
    .description("Run analytic queries against session metadata");

  addSessionDbOption(
    query
      .command("size")
      .description("Sum file sizes tracked by the session")
      .argument("<id-or-name>", "session id or name")
      .addOption(sideOption())
      .option(
        "--path <path>",
        "limit to a file or directory within the session",
      )
      .option("--json", "emit JSON output", false)
      .action(async (ref: string, opts: any, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        let row: SessionRow;
        try {
          row = requireSessionRow(sessionDb, ref);
        } catch (err) {
          console.error((err as Error).message);
          process.exitCode = 1;
          return;
        }
        const side = (opts.side ?? "alpha") as "alpha" | "beta";
        try {
          const result = await querySize({
            session: row,
            side,
            path: opts.path,
          });
          if (opts.json) {
            console.log(JSON.stringify(result, null, 2));
            return;
          }
          const table = new AsciiTable3("Session Size")
            .setHeading("Field", "Value")
            .setStyle("unicode-round");
          table.setAlign(0, AlignmentEnum.LEFT);
          table.setAlign(1, AlignmentEnum.LEFT);
          table.addRow("Side", result.side);
          table.addRow("Root", result.rootPath);
          if (result.pathProvided) {
            table.addRow("Path", result.targetPath);
          }
          table.addRow("Kind", result.kind);
          table.addRow("Files", numberFormatter.format(result.fileCount));
          table.addRow(
            "Total bytes",
            `${numberFormatter.format(result.totalBytes)} (${humanFileSize(result.totalBytes)})`,
          );
          console.log(table.toString());
        } catch (err) {
          console.error(`query size failed: ${(err as Error).message}`);
          process.exitCode = 1;
        }
      }),
  );

  addSessionDbOption(
    query
      .command("recent")
      .description("Show most recently modified files")
      .argument("<id-or-name>", "session id or name")
      .addOption(sideOption())
      .option(
        "--path <path>",
        "limit to a directory or file within the session",
      )
      .option("--json", "emit JSON output", false)
      .option("--max <n>", "maximum number of files to return (default 50)")
      .action(async (ref: string, opts: any, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        let row: SessionRow;
        try {
          row = requireSessionRow(sessionDb, ref);
        } catch (err) {
          console.error((err as Error).message);
          process.exitCode = 1;
          return;
        }
        const limitRaw =
          opts.max !== undefined && opts.max !== null ? Number(opts.max) : 50;
        if (!Number.isInteger(limitRaw) || limitRaw <= 0) {
          console.error(
            "reflect query recent: --max must be a positive integer",
          );
          process.exitCode = 1;
          return;
        }
        const side = (opts.side ?? "alpha") as "alpha" | "beta";
        try {
          const result = await queryRecent({
            session: row,
            side,
            path: opts.path,
            limit: limitRaw,
          });
          if (opts.json) {
            console.log(JSON.stringify(result, null, 2));
            return;
          }
          console.log(`Side: ${result.side}`);
          console.log(`Root: ${result.rootPath}`);
          if (result.pathProvided) {
            console.log(`Path: ${result.targetPath}`);
          }
          if (!result.files.length) {
            console.log("No files found.");
            return;
          }
          const table = new AsciiTable3("Recent Files")
            .setHeading("Path", "Size", "Modified", "Age")
            .setStyle("unicode-round");
          [0, 1, 2, 3].forEach((idx) =>
            table.setAlign(idx, AlignmentEnum.LEFT),
          );
          for (const file of result.files) {
            table.addRow(
              file.relativePath,
              `${numberFormatter.format(file.size)} (${humanFileSize(file.size)})`,
              new Date(file.mtime).toLocaleString(),
              fmtAgo(file.mtime),
            );
          }
          console.log(table.toString());
          if (result.hasMore) {
            console.log(
              `Showing ${result.files.length} of ${result.limit} records (more available).`,
            );
          }
        } catch (err) {
          console.error(`query recent failed: ${(err as Error).message}`);
          process.exitCode = 1;
        }
      }),
  );

  addSessionDbOption(
    program
      .command("edit")
      .description("Modify an existing session")
      .argument("<id-or-name>", "session id or name")
      .option("-n, --name <name>", "rename the session")
      .option("--compress <algorithm>", "set rsync compression algorithm")
      .option(
        "--compress-level <level>",
        "set compression level (e.g. zstd: -131072..22)",
      )
      .option(
        "-i, --ignore <pattern>",
        "gitignore-style ignore rule (repeat or comma-separated)",
        collectIgnoreOption,
        [] as string[],
      )
      .addOption(
        new Option(
          "--clear-ignore",
          "reset ignore rules before applying updates",
        )
          .preset(true)
          .default(false),
      )
      .option(
        "-l, --label <k=v>",
        "upsert a session label",
        collectLabels,
        [] as string[],
      )
      .option("--hash <algorithm>", "change hash algorithm (requires --reset)")
      .option("--alpha <endpoint>", "update alpha endpoint (requires --reset)")
      .option("--beta <endpoint>", "update beta endpoint (requires --reset)")
      .option("--reset", "reset session state after applying changes", false)
      .action(async (ref: string, opts: any, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        let row: SessionRow;
        try {
          row = requireSessionRow(sessionDb, ref);
        } catch (err) {
          console.error((err as Error).message);
          process.exitCode = 1;
          return;
        }

        const cliLogger = new ConsoleLogger(getLogLevel());
        try {
          const result = await editSession({
            sessionDb,
            id: row.id,
            name: opts.name,
            compress: opts.compress,
            compressLevel: opts.compressLevel,
            ignoreAdd: Array.isArray(opts.ignore) ? opts.ignore : [],
            resetIgnore: !!opts.clearIgnore,
            labels: opts.label || [],
            hash: opts.hash,
            alphaSpec: opts.alpha,
            betaSpec: opts.beta,
            reset: !!opts.reset,
            logger: cliLogger,
          });

          const updatedRow = loadSessionById(sessionDb, row.id) ?? row;
          const label = updatedRow.name ?? String(updatedRow.id);
          if (!result.changes.length) {
            console.log(`no changes applied to session ${label}`);
          } else {
            console.log(
              `updated session ${label}: ${result.changes.join(", ")}`,
            );
          }
          if (result.restarted) {
            console.log("session restarted");
          }
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          console.error(
            `failed to edit session ${row.name ?? row.id}: ${message}`,
          );
          process.exitCode = 1;
        }
      }),
  );

  addSessionDbOption(
    program
      .command("logs")
      .description("Show recent logs for a session")
      .argument("<id-or-name>", "session id or name")
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
      .option("--scope <scope>", "only include logs with matching scope")
      .option("--message <message>", "only include logs with matching message")
      .action(async (ref: string, opts: any, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        let row: SessionRow;
        try {
          row = requireSessionRow(sessionDb, ref);
        } catch (err) {
          console.error((err as Error).message);
          return;
        }
        const id = row.id;
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
          scope: opts.scope ? String(opts.scope) : undefined,
          message: opts.message ? String(opts.message) : undefined,
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
          Number(process.env.REFLECT_LOG_FOLLOW_INTERVAL ?? 1000),
          1000,
        );

        await new Promise<void>((resolve) => {
          const tick = () => {
            rows = fetchSessionLogs(sessionDb, id, {
              afterId: lastId,
              minLevel,
              order: "asc",
              scope: opts.scope ? String(opts.scope) : undefined,
              message: opts.message ? String(opts.message) : undefined,
            });
            if (rows.length) {
              renderRows(rows, {
                json: !!opts.json,
                absolute: !!opts.absolute,
              });
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
      .command("diff")
      .description("Show paths that differ between alpha and beta")
      .argument("<id-or-name>", "session id or name")
      .option("--max-paths <n>", "maximum number of paths to display")
      .option("--json", "emit JSON output", false)
      .action(async (ref: string, opts: any, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        let row: SessionRow;
        try {
          row = requireSessionRow(sessionDb, ref);
        } catch (err) {
          console.error((err as Error).message);
          process.exitCode = 1;
          return;
        }
        try {
          const differences = diffSession(row, {
            limit: opts.maxPaths ? Number(opts.maxPaths) : undefined,
          });
          if (opts.json) {
            console.log(JSON.stringify(differences, null, 2));
            return;
          }
          if (!differences.length) {
            console.log("alpha and beta are synchronized (no differences)");
            return;
          }
          const table = new AsciiTable3("Session Differences")
            .setHeading("Path", "Type", "Modified", "Age")
            .setStyle("unicode-round");
          [0, 1, 2, 3].forEach((idx) =>
            table.setAlign(idx, AlignmentEnum.LEFT),
          );
          for (const entry of differences) {
            const mtime = entry.mtime
              ? new Date(entry.mtime).toLocaleString()
              : "-";
            const age = entry.mtime ? fmtAgo(entry.mtime) : "-";
            table.addRow(entry.path, entry.type, mtime, age);
          }
          console.log(table.toString());
          if (opts.maxPaths && differences.length === opts.maxPaths) {
            console.log(
              `Showing first ${opts.maxPaths} paths. Use --max-paths to adjust the limit.`,
            );
          }
        } catch (err) {
          console.error(`diff failed: ${(err as Error).message}`);
          process.exitCode = 1;
        }
      }),
  );

  addSessionDbOption(
    program
      .command("pause")
      .description("Pause sync for one or more sessions")
      .argument("<id-or-name...>", "session id(s) or name(s)")
      .action(
        (refs: string[], opts: { sessionDb?: string }, command: Command) => {
          const sessionDb = resolveSessionDb(opts, command);
          for (const ref of refs) {
            let row: SessionRow;
            try {
              row = requireSessionRow(sessionDb, ref);
            } catch (err) {
              console.error((err as Error).message);
              continue;
            }
            const ok = row.scheduler_pid ? stopPid(row.scheduler_pid) : false;
            setDesiredState(sessionDb, row.id, "paused");
            setActualState(sessionDb, row.id, "paused");
            console.log(
              ok
                ? `paused session ${row.name ?? row.id} (pid ${row.scheduler_pid})`
                : `session ${row.name ?? row.id} was not running`,
            );
          }
        },
      ),
  );

  addSessionDbOption(
    program
      .command("resume")
      .description("Resume sync for one or more sessions")
      .argument("<id-or-name...>", "session id(s) or name(s)")
      .action(
        (refs: string[], opts: { sessionDb?: string }, command: Command) => {
          const sessionDb = resolveSessionDb(opts, command);
          for (const ref of refs) {
            let row: SessionRow;
            try {
              row = requireSessionRow(sessionDb, ref);
            } catch (err) {
              console.error((err as Error).message);
              continue;
            }
            const pid = spawnSchedulerForSession(sessionDb, row);
            setDesiredState(sessionDb, row.id, "running");
            setActualState(sessionDb, row.id, pid ? "running" : "error");
            if (pid) {
              recordHeartbeat(sessionDb, row.id, "running", pid);
            }
            console.log(
              pid
                ? `resumed session ${row.name ?? row.id} (pid ${pid})`
                : `failed to resume session ${row.name ?? row.id}`,
            );
          }
        },
      ),
  );

  addSessionDbOption(
    program
      .command("reset")
      .description("Reset sync for one or more sessions")
      .argument("<id-or-name...>", "session id(s) or name(s)")
      .action(
        async (
          refs: string[],
          opts: { sessionDb?: string },
          command: Command,
        ) => {
          const sessionDb = resolveSessionDb(opts, command);
          const cliLogger = new ConsoleLogger(getLogLevel());
          let hadError = false;
          for (const ref of refs) {
            let row: SessionRow;
            try {
              row = requireSessionRow(sessionDb, ref);
            } catch (err) {
              console.error((err as Error).message);
              hadError = true;
              continue;
            }
            const label = row.name ?? String(row.id);
            try {
              await resetSession({ sessionDb, id: row.id, logger: cliLogger });
              console.log(
                `reset session ${label}; run 'reflect resume ${label}' to restart`,
              );
            } catch (err) {
              hadError = true;
              const msg = err instanceof Error ? err.message : String(err);
              console.error(`failed to reset session ${label}: ${msg}`);
            }
          }
          if (hadError) {
            process.exitCode = 1;
          }
        },
      ),
  );

  addSessionDbOption(
    program
      .command("terminate")
      .description("Stop and remove all session state")
      .option("--force", "terminate even if can't delete remote db")
      .argument("<id-or-name...>", "session id(s) or name(s)")
      .action(
        async (
          refs: string[],
          options: { force?: boolean; sessionDb?: string },
          command: Command,
        ) => {
          const sessionDb = resolveSessionDb(options, command);
          const cliLogger = new ConsoleLogger(getLogLevel());
          for (const ref of refs) {
            let row: SessionRow;
            try {
              row = requireSessionRow(sessionDb, ref);
            } catch (err) {
              console.error((err as Error).message);
              continue;
            }
            await terminateSession({
              id: row.id,
              logger: cliLogger,
              force: options.force,
              sessionDb,
            });
            console.log(`terminated session ${row.name ?? row.id}`);
          }
        },
      ),
  );

  registerSessionMonitor(program);
  registerSessionSync(program);
  registerSessionStatus(program);
  registerSessionDaemon(program);
}
