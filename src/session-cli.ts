// src/session-cli.ts

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
  deriveSessionPaths,
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
import { registerSessionSync } from "./session-sync.js";
import { defaultHashAlg, listSupportedHashes } from "./hash.js";
import { ConsoleLogger, LOG_LEVELS, parseLogLevel } from "./logger.js";
import { fetchSessionLogs } from "./session-logs.js";
import { AsciiTable3, AlignmentEnum } from "ascii-table3";
import { fmtLocalPath } from "./session-status.js";
import { spawnSchedulerForSession } from "./session-runner.js";
import {
  registerSessionDaemon,
  ensureDaemonRunning,
} from "./session-daemon.js";
import { collectIgnoreOption, deserializeIgnoreRules } from "./ignore.js";
import { queryRecent, querySize } from "./session-query.js";
import { diffSession } from "./session-diff.js";
import { getDb } from "./db.js";
import type { Database } from "./db.js";
import { fetchHotEvents, getMaxOpTs } from "./hot-events.js";
import { MERGE_STRATEGY_NAMES } from "./merge-strategies.js";
import { nodeKindToEntry } from "./nodes-util.js";
import { parseLogLevelOption, renderLogRows } from "./cli-log-output.js";
import { collectListOption, dedupeRestrictedList } from "./restrict.js";
import { wait } from "./util.js";

type StopResult = "stopped" | "failed" | "not-running";

const DEFAULT_LOG_LINES = 2_500;

const COLOR_OK =
  Boolean(process.stdout.isTTY) && process.env.NO_COLOR === undefined;
const CHECK_CHAR = "✓";
const GREEN_CHECK = COLOR_OK ? `\x1b[32m${CHECK_CHAR}\x1b[0m` : CHECK_CHAR;

function fmtCleanMarker(ts?: number | null, useColor = COLOR_OK): string {
  if (!ts) return "-";
  const marker = useColor ? GREEN_CHECK : CHECK_CHAR;
  return `${marker} ${fmtAgo(ts)}`;
}

function stopSessionRow(sessionDb: string, row: SessionRow): StopResult {
  let status: StopResult;
  if (row.scheduler_pid) {
    const ok = stopPid(row.scheduler_pid);
    status = ok ? "stopped" : "failed";
  } else {
    status = "not-running";
  }
  setDesiredState(sessionDb, row.id, "stopped");
  setActualState(sessionDb, row.id, "stopped");
  return status;
}

function startSessionRow(sessionDb: string, row: SessionRow): number | null {
  const pid = spawnSchedulerForSession(sessionDb, row);
  setDesiredState(sessionDb, row.id, "running");
  setActualState(sessionDb, row.id, pid ? "running" : "error");
  if (pid) {
    recordHeartbeat(sessionDb, row.id, "running", pid);
    return pid;
  }
  return null;
}

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
      .option("-s, --stopped", "leave session stopped (do not sync)", false)
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
      .option(
        "-i, --ignore <pattern>",
        "gitignore-style ignore rule (repeat or comma-separated)",
        collectIgnoreOption,
        [] as string[],
      )
      .option(
        "--disable-hot-sync",
        "disable realtime hot-sync cycles for this session",
      )
      .option(
        "--disable-full-sync",
        "disable automatic periodic full sync cycles",
      )
      .option(
        "--enable-reflink",
        "copy files via cp --reflink when both roots are local",
        false,
      )
      .addOption(
        new Option("--merge-strategy <name>", "merge strategy")
          .choices(Array.from(MERGE_STRATEGY_NAMES) as string[])
          .default("last-write-wins"),
      )
      .action(
        async (
          alphaSpec: string,
          betaSpec: string,
          opts: any,
          command: Command,
        ) => {
          const sessionDb = resolveSessionDb(opts, command);
          if (typeof opts.disableHotSync !== "boolean") {
            if (typeof opts.enableHotSync === "boolean" && opts.enableHotSync) {
              opts.disableHotSync = false;
            }
          } else if (
            typeof opts.enableHotSync === "boolean" &&
            opts.enableHotSync
          ) {
            opts.disableHotSync = false;
          }
          const cliLogger = new ConsoleLogger(getLogLevel());
          try {
            const id = await newSession({
              alphaSpec,
              betaSpec,
              ...opts,
              sessionDb,
              logger: cliLogger,
            });
            const daemonPid = ensureDaemonRunning(
              sessionDb,
              cliLogger.child("daemon"),
            );
            console.log(
              `created session ${id}${opts.name ? ` (${opts.name})` : ""}`,
            );
            if (!opts.stopped) {
              setDesiredState(sessionDb, id, "running");
              console.log(
                daemonPid
                  ? `queued session ${id} for daemon start (daemon pid ${daemonPid})`
                  : `queued session ${id} for daemon start`,
              );
            } else {
              console.log(`session ${id} left stopped`);
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
      .argument("[id-or-name...]", "session id(s) or name(s) to list")
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
      .option(
        "-l, --long",
        "show detailed columns (ignores/flags/paths)",
        false,
      )
      .action((refs: string[], opts: any, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        const selectors = opts.selector || [];
        const explicitRefs = Array.isArray(refs) ? refs.filter(Boolean) : [];

        if (explicitRefs.length && selectors.length) {
          console.error("cannot combine id-or-name arguments with selectors");
          process.exitCode = 1;
          return;
        }

        let rows: SessionRow[];
        if (explicitRefs.length) {
          const seen = new Set<string>();
          const selected: SessionRow[] = [];
          let hadError = false;
          for (const ref of explicitRefs) {
            if (!ref || seen.has(ref)) continue;
            seen.add(ref);
            try {
              const row = requireSessionRow(sessionDb, ref);
              selected.push(row);
            } catch (err) {
              console.error((err as Error).message);
              process.exitCode = 1;
              hadError = true;
            }
          }
          if (hadError && !selected.length) return;
          rows = selected;
        } else {
          const sels = parseSelectorTokens(selectors);
          rows = selectSessions(sessionDb, sels);
        }

        if (!rows.length) {
          if (opts.json) {
            console.log("[]");
          } else {
            console.log("no sessions");
          }
          return;
        }

        if (opts.json) {
          const payload = rows.map((r) => ({
            ...r,
            disable_hot_sync: !!r.disable_hot_sync,
            disable_full_sync: !!r.disable_full_sync,
          }));
          console.log(JSON.stringify(payload, null, 2));
          return;
        }

        const longView = Boolean(opts.long);
        const headings = longView
          ? [
              "ID",
              "Name",
              "State/Desired",
              "Sync",
              "PID",
              "Ignores",
              "Flags",
              "Paths",
            ]
          : ["ID", "Name", "State/Desired", "Sync", "PID"];
        const table = new AsciiTable3("Sync Sessions")
          .setHeading(...headings)
          .setStyle("unicode-round");

        const alignments = headings.map(() => AlignmentEnum.LEFT);
        alignments.forEach((align, idx) => table.setAlign(idx, align));

        for (const r of rows) {
          const alphaPath = r.alpha_host
            ? fmtRemoteEndpoint(r.alpha_host, r.alpha_port, r.alpha_root)
            : fmtLocalPath(r.alpha_root);
          const betaPath = r.beta_host
            ? fmtRemoteEndpoint(r.beta_host, r.beta_port, r.beta_root)
            : fmtLocalPath(r.beta_root);
          if (!longView) {
            table.addRow(
              String(r.id),
              r.name ?? "-",
              `${r.actual_state}/${r.desired_state}`,
              fmtCleanMarker(r.last_clean_sync_at),
              r.scheduler_pid ? String(r.scheduler_pid) : "-",
            );
            continue;
          }

          const ignoreRules = deserializeIgnoreRules(
            (r as any).ignore_rules ?? null,
          );
          const ignoreSummary = ignoreRules.length
            ? ignoreRules.join(", ")
            : "-";
          const flags: string[] = [];
          if (r.disable_hot_sync) flags.push("hot-sync off");
          if (r.enable_reflink) flags.push("reflink");
          if (r.disable_full_sync) flags.push("full-sync off");
          const flagsSummary = flags.length ? flags.join(", ") : "-";

          table.addRow(
            String(r.id),
            r.name ?? "-",
            `${r.actual_state}/${r.desired_state}`,
            fmtCleanMarker(r.last_clean_sync_at),
            r.scheduler_pid ? String(r.scheduler_pid) : "-",
            ignoreSummary,
            flagsSummary,
            `alpha${r.prefer == "alpha" ? "*" : " "}: ${alphaPath}`,
          );
          table.addRow(
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            `beta${r.prefer == "beta" ? "*" : " "} : ${betaPath}`,
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
      .addOption(
        new Option(
          "--disable-hot-sync",
          "disable realtime hot-sync cycles",
        ).conflicts("enableHotSync"),
      )
      .addOption(
        new Option(
          "--enable-hot-sync",
          "enable realtime hot-sync cycles",
        ).conflicts("disableHotSync"),
      )
      .addOption(
        new Option(
          "--disable-full-sync",
          "disable automatic periodic full sync cycles",
        ).conflicts("enableFullSync"),
      )
      .addOption(
        new Option(
          "--enable-full-sync",
          "enable automatic periodic full sync cycles",
        ).conflicts("disableFullSync"),
      )
      .addOption(
        new Option("--merge-strategy <name>", "set merge strategy")
          .choices(Array.from(MERGE_STRATEGY_NAMES) as string[])
          .conflicts("clearMergeStrategy"),
      )
      .option("--clear-merge-strategy", "remove merge strategy override", false)
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
        const disableHotSyncUpdate =
          opts.disableHotSync === true
            ? true
            : opts.enableHotSync === true
              ? false
              : undefined;
        const disableFullSyncUpdate =
          opts.disableFullSync === true
            ? true
            : opts.enableFullSync === true
              ? false
              : undefined;
        const mergeStrategyUpdate =
          opts.clearMergeStrategy === true
            ? null
            : opts.mergeStrategy !== undefined
              ? opts.mergeStrategy
              : undefined;
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
            disableHotSync: disableHotSyncUpdate,
            disableFullSync: disableFullSyncUpdate,
            mergeStrategy: mergeStrategyUpdate,
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
        const tail = clampPositive(
          opts.tail,
          opts.follow ? 100 : DEFAULT_LOG_LINES,
        );
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
          renderLogRows(rows, { json: !!opts.json, absolute: !!opts.absolute });
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
              renderLogRows(rows, {
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
      .command("monitor <id-or-name>")
      .description("Stream change events for a session")
      .option("--json", "emit JSON output", false)
      .option("--alpha-only", "only watch alpha", false)
      .option("--beta-only", "only watch beta", false)
      .option(
        "--poll-interval <ms>",
        "scan poll interval in milliseconds",
        (v: string) => Number.parseInt(v, 10),
      )
      .option(
        "--hot-interval <ms>",
        "hot events poll interval in milliseconds",
        (v: string) => Number.parseInt(v, 10),
      )
      .option(
        "--since <ms>",
        "look back this many milliseconds before start",
        (v: string) => Number.parseInt(v, 10),
      )
      .action(async (ref: string, opts: any, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        try {
          await runMonitorCommand(sessionDb, ref, opts);
        } catch (err) {
          console.error((err as Error).message);
          process.exitCode = 1;
        }
      }),
  );

  addSessionDbOption(
    program
      .command("diff")
      .description("Show paths that differ between alpha and beta")
      .argument("<id-or-name>", "session id or name")
      .option("--max-paths <n>", "maximum number of paths to display")
      .option("--json", "emit JSON output", false)
      .option(
        "--restricted-path <path>",
        "restrict diff to a specific relative path (repeat or comma-separated)",
        collectListOption,
        [] as string[],
      )
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
            restrictedPaths: dedupeRestrictedList(opts.restrictedPath),
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
            .setHeading("Path", "Type", "Modified", "Age", "Conflicts")
            .setStyle("unicode-round");
          [0, 1, 2, 3, 4].forEach((idx) =>
            table.setAlign(idx, AlignmentEnum.LEFT),
          );
          for (const entry of differences) {
            const mtime = entry.mtime
              ? new Date(entry.mtime).toLocaleString()
              : "-";
            const age = entry.mtime ? fmtAgo(entry.mtime) : "-";
            const conflicts =
              entry.conflicts.length > 0 ? entry.conflicts.join(",") : "-";
            table.addRow(entry.path, entry.type, mtime, age, conflicts);
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
      .command("stop")
      .description("Stop one or more sync sessions")
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
            const status = stopSessionRow(sessionDb, row);
            const label = row.name ?? row.id;
            let message: string;
            if (status === "stopped") {
              message = `stopped session ${label} (pid ${row.scheduler_pid})`;
            } else if (status === "failed") {
              message = `failed to stop session ${label} (pid ${row.scheduler_pid})`;
            } else {
              message = `session ${label} was not running`;
            }
            console.log(message);
          }
        },
      ),
  );

  addSessionDbOption(
    program
      .command("start")
      .description("Start one or more sync sessions")
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
            const pid = startSessionRow(sessionDb, row);
            console.log(
              pid
                ? `started session ${row.name ?? row.id} (pid ${pid})`
                : `failed to start session ${row.name ?? row.id}`,
            );
          }
        },
      ),
  );

  addSessionDbOption(
    program
      .command("restart")
      .description("Restart one or more sync sessions")
      .argument("<id-or-name...>", "session id(s) or name(s)")
      .action(
        async (
          refs: string[],
          opts: { sessionDb?: string },
          command: Command,
        ) => {
          const sessionDb = resolveSessionDb(opts, command);
          for (const ref of refs) {
            let row: SessionRow;
            try {
              row = requireSessionRow(sessionDb, ref);
            } catch (err) {
              console.error((err as Error).message);
              continue;
            }
            const label = row.name ?? String(row.id);
            const status = stopSessionRow(sessionDb, row);
            if (status === "stopped") {
              console.log(
                `stopped session ${label} (pid ${row.scheduler_pid})`,
              );
            } else if (status === "failed") {
              console.log(
                `failed to stop session ${label} (pid ${row.scheduler_pid}); attempting restart`,
              );
            } else {
              console.log(`session ${label} was not running`);
            }

            let latestRow: SessionRow;
            try {
              latestRow = requireSessionRow(sessionDb, String(row.id));
            } catch (err) {
              console.error(
                `unable to reload session ${label} for restart: ${(err as Error).message}`,
              );
              continue;
            }

            const pid = startSessionRow(sessionDb, latestRow);
            console.log(
              pid
                ? `restarted session ${label} (pid ${pid})`
                : `failed to restart session ${label}`,
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
              const pid = startSessionRow(sessionDb, row);
              console.log(
                pid
                  ? `started session ${row.name ?? row.id} (pid ${pid})`
                  : `failed to start session ${row.name ?? row.id}`,
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

  registerSessionSync(program);
  registerSessionStatus(program);
  registerSessionDaemon(program);
}

type WatchCommandOptions = {
  json?: boolean;
  alphaOnly?: boolean;
  betaOnly?: boolean;
  pollInterval?: number;
  hotInterval?: number;
  since?: number;
};

type WatchEventPayload = {
  side: "alpha" | "beta";
  path: string;
  source: string;
  opTs: number;
  kind?: string | null;
  size?: number | null;
  mtime?: number | null;
  hash?: string | null;
  target?: string | null;
  deleted?: boolean;
};

async function runMonitorCommand(
  sessionDbPath: string,
  ref: string,
  opts: WatchCommandOptions,
) {
  const row = requireSessionRow(sessionDbPath, ref);
  const derived = deriveSessionPaths(row.id);
  const alphaDbPath = row.alpha_db ?? derived.alpha_db;
  const betaDbPath = row.beta_db ?? derived.beta_db;
  if (!alphaDbPath || !betaDbPath) {
    throw new Error("session is missing alpha/beta database paths");
  }
  let watchAlpha = !opts.betaOnly;
  let watchBeta = !opts.alphaOnly;
  if (!watchAlpha && !watchBeta) {
    watchAlpha = true;
    watchBeta = true;
  }

  const pollInterval = clampPositive(opts.pollInterval, 2000);
  const hotInterval = clampPositive(
    opts.hotInterval,
    Math.min(1000, Math.max(250, Math.floor(pollInterval / 2))),
  );
  const sinceOpTs =
    opts.since != null && Number.isFinite(Number(opts.since))
      ? Math.max(0, Date.now() - Number(opts.since))
      : undefined;

  const alphaHandle = watchAlpha ? getDb(alphaDbPath) : null;
  const betaHandle = watchBeta ? getDb(betaDbPath) : null;

  try {
    let lastAlphaScanOpTs = watchAlpha
      ? (sinceOpTs ?? getMaxOpTs(alphaDbPath))
      : 0;
    let lastBetaScanOpTs = watchBeta
      ? (sinceOpTs ?? getMaxOpTs(betaDbPath))
      : 0;
    let lastAlphaHotOpTs = sinceOpTs ?? 0;
    let lastBetaHotOpTs = sinceOpTs ?? 0;
    let lastAlphaScanPoll = 0;
    let lastBetaScanPoll = 0;

    let running = true;
    const stop = () => {
      running = false;
    };
    process.once("SIGINT", stop);
    process.once("SIGTERM", stop);

    while (running) {
      const loopStart = Date.now();
      if (watchAlpha) {
        lastAlphaHotOpTs = await drainHotEvents(
          alphaDbPath,
          "alpha",
          lastAlphaHotOpTs,
          alphaHandle,
          !!opts.json,
        );
      }
      if (watchBeta) {
        lastBetaHotOpTs = await drainHotEvents(
          betaDbPath,
          "beta",
          lastBetaHotOpTs,
          betaHandle,
          !!opts.json,
        );
      }

      const now = Date.now();
      if (watchAlpha && now - lastAlphaScanPoll >= pollInterval) {
        lastAlphaScanOpTs = await drainScanEvents(
          alphaHandle!,
          "alpha",
          lastAlphaScanOpTs,
          !!opts.json,
        );
        lastAlphaScanPoll = now;
      }
      if (watchBeta && now - lastBetaScanPoll >= pollInterval) {
        lastBetaScanOpTs = await drainScanEvents(
          betaHandle!,
          "beta",
          lastBetaScanOpTs,
          !!opts.json,
        );
        lastBetaScanPoll = now;
      }

      const elapsed = Date.now() - loopStart;
      const waitMs = Math.max(hotInterval - elapsed, 100);
      if (waitMs > 0 && running) {
        await wait(waitMs);
      }
    }
  } finally {
    alphaHandle?.close();
    betaHandle?.close();
  }
}

async function drainHotEvents(
  dbPath: string,
  side: "alpha" | "beta",
  since: number,
  handle: Database | null,
  json: boolean,
): Promise<number> {
  let max = since;
  const limit = 500;
  while (true) {
    const rows = fetchHotEvents(dbPath, max, { side, limit });
    if (!rows.length) break;
    for (const row of rows) {
      max = Math.max(max, row.opTs);
      const meta = handle ? lookupLatestMeta(handle, row.path) : null;
      outputWatchEvent(
        {
          side,
          source: row.source ?? "hotwatch",
          path: row.path,
          opTs: row.opTs,
          kind: meta?.kind ?? null,
          size: meta?.size ?? null,
          mtime: meta?.mtime ?? null,
          hash: meta?.hash ?? null,
          target: meta?.target ?? null,
          deleted: meta?.deleted ?? false,
        },
        json,
      );
    }
    if (rows.length < limit) break;
  }
  return max;
}

async function drainScanEvents(
  handle: Database,
  side: "alpha" | "beta",
  since: number,
  json: boolean,
): Promise<number> {
  let max = since;
  const limit = 500;
  while (true) {
    const rows = fetchScanEvents(handle, max, limit);
    if (!rows.length) break;
    for (const row of rows) {
      max = Math.max(max, row.opTs);
      outputWatchEvent({ side, source: "scan", ...row }, json);
    }
    if (rows.length < limit) break;
  }
  return max;
}

type ScanRow = {
  path: string;
  opTs: number;
  kind: string;
  size: number | null;
  mtime: number | null;
  hash: string | null;
  target?: string | null;
  deleted: boolean;
};

function fetchScanEvents(
  handle: Database,
  since: number,
  limit: number,
): ScanRow[] {
  const stmt = handle.prepare(
    `SELECT path,
            updated AS opTs,
            kind,
            size,
            mtime,
            hash,
            link_target AS target,
            deleted
       FROM nodes
      WHERE updated > ?
      ORDER BY opTs ASC, path ASC
      LIMIT ?`,
  );
  const raw = stmt.all(since, limit) as Record<string, any>[];
  return raw.map((row) => ({
    path: row.path as string,
    opTs: Number(row.opTs),
    kind: nodeKindToEntry(row.kind as string),
    size: row.size != null ? Number(row.size) : null,
    mtime: row.mtime != null ? Number(row.mtime) : null,
    hash: row.hash != null ? String(row.hash) : null,
    target: row.target != null ? String(row.target) : null,
    deleted: !!row.deleted,
  }));
}

function lookupLatestMeta(
  handle: Database,
  path: string,
): (Omit<ScanRow, "opTs"> & { path: string }) | null {
  const row = handle
    .prepare(
      `SELECT kind, size, mtime, hash, link_target AS target, deleted
         FROM nodes
        WHERE path = ?
        LIMIT 1`,
    )
    .get(path) as
    | {
        kind: string;
        size: number | null;
        mtime: number | null;
        hash: string | null;
        target: string | null;
        deleted: number;
      }
    | undefined;
  if (!row) return null;
  return {
    path,
    kind: nodeKindToEntry(row.kind as string),
    size: row.size ?? null,
    mtime: row.mtime ?? null,
    hash: row.hash ?? null,
    target: row.target ?? null,
    deleted: !!row.deleted,
  };
}

function outputWatchEvent(event: WatchEventPayload, json: boolean) {
  if (json) {
    console.log(JSON.stringify(event));
    return;
  }
  const parts = [
    new Date(event.opTs).toISOString(),
    `[${event.side}]`,
    event.source,
    event.path,
  ];
  if (event.kind) {
    parts.push(`(${event.kind}${event.deleted ? ", deleted" : ""})`);
  } else if (event.deleted) {
    parts.push("(deleted)");
  }
  if (event.hash) {
    parts.push(`hash=${event.hash}`);
  }
  console.log(parts.join(" "));
}
