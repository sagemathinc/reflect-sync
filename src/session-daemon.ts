import { Command } from "commander";
import fs from "node:fs";
import { join } from "node:path";
import os from "node:os";
import { spawn } from "node:child_process";
import {
  ensureSessionDb,
  getReflectSyncHome,
  getSessionDbPath,
  selectSessions,
  updateSession,
  setActualState,
  recordHeartbeat,
  type SessionRow,
  type ActualState,
  selectForwardSessions,
  updateForwardSession,
  type ForwardRow,
  loadForwardById,
} from "./session-db.js";
import { spawnSchedulerForSession } from "./session-runner.js";
import { launchForwardProcess } from "./forward-runner.js";
import { stopPid } from "./session-manage.js";
import {
  ConsoleLogger,
  LOG_LEVELS,
  parseLogLevel,
  type LogLevel,
  type Logger,
} from "./logger.js";
import { resolveSelfLaunch } from "./self-launch.js";
import { createDaemonLogger, fetchDaemonLogs } from "./daemon-logs.js";
import { parseLogLevelOption, renderLogRows } from "./cli-log-output.js";

const DAEMON_DIR = join(getReflectSyncHome(), "daemon");
const PID_FILE = join(DAEMON_DIR, "reflect.pid");
const SERVICE_NAME = "reflect-sync";

function ensureDir() {
  fs.mkdirSync(DAEMON_DIR, { recursive: true });
}

function readPidSync(): number | null {
  try {
    const raw = fs.readFileSync(PID_FILE, "utf8").trim();
    if (!raw) return null;
    const pid = Number(raw);
    return Number.isFinite(pid) ? pid : null;
  } catch {
    return null;
  }
}

function writePidSync(pid: number) {
  ensureDir();
  fs.writeFileSync(PID_FILE, String(pid));
}

function removePidSync() {
  try {
    fs.rmSync(PID_FILE);
  } catch {}
}

function isPidAlive(pid: number | null | undefined): boolean {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function resolveSessionDb(
  opts: { sessionDb?: string },
  command: Command,
): string {
  const ensure = (path: string) => {
    const db = ensureSessionDb(path);
    db.close();
    return path;
  };
  if (opts.sessionDb) return ensure(opts.sessionDb);
  const globals = command.optsWithGlobals() as { sessionDb?: string };
  if (globals.sessionDb) return ensure(globals.sessionDb);
  return ensure(getSessionDbPath());
}

async function superviseSessions(sessionDb: string, logger: Logger) {
  const rows = selectSessions(sessionDb, []);
  for (const row of rows as SessionRow[]) {
    const shouldRun = row.desired_state === "running";
    const alive = isPidAlive(row.scheduler_pid);

    if (shouldRun) {
      if (!alive) {
        if (row.scheduler_pid) {
          updateSession(sessionDb, row.id, { scheduler_pid: null });
        }
        const pid = spawnSchedulerForSession(sessionDb, row, logger);
        if (pid) {
          updateSession(sessionDb, row.id, { scheduler_pid: pid });
          setActualState(sessionDb, row.id, "running");
          recordHeartbeat(sessionDb, row.id, "running", pid);
          if (logger.isLevelEnabled("debug")) {
            logger.debug("launched scheduler", { session: row.id, pid });
          }
        } else {
          setActualState(sessionDb, row.id, "error");
          logger.warn("failed to launch scheduler", { session: row.id });
        }
      } else if (row.actual_state !== "running") {
        setActualState(sessionDb, row.id, "running");
      }
    } else {
      if (alive) {
        const stopped = stopPid(row.scheduler_pid!);
        if (!stopped) {
          logger.warn("failed to stop scheduler", { session: row.id });
        }
      }
      if (row.scheduler_pid) {
        updateSession(sessionDb, row.id, { scheduler_pid: null });
      }
      const targetState: ActualState = "stopped";
      if (row.actual_state !== targetState) {
        setActualState(sessionDb, row.id, targetState);
      }
    }
  }
}

function isForwardAlive(row: ForwardRow): boolean {
  return isPidAlive(row.monitor_pid);
}

async function superviseForwards(sessionDb: string, logger: Logger) {
  const forwards = selectForwardSessions(sessionDb);
  for (const row of forwards as ForwardRow[]) {
    const shouldRun = row.desired_state === "running";
    const alive = isForwardAlive(row);

    if (shouldRun) {
      if (!alive) {
        if (row.monitor_pid) {
          updateForwardSession(sessionDb, row.id, { monitor_pid: null });
        }
        const forwardRow = loadForwardById(sessionDb, row.id);
        if (!forwardRow) continue;
        const pid = await launchForwardProcess(sessionDb, forwardRow);
        if (pid) {
          logger.debug?.("launched forward ssh", { id: row.id, pid });
        } else {
          logger.warn("failed to launch forward ssh", { id: row.id });
        }
      } else if (row.actual_state !== "running") {
        updateForwardSession(sessionDb, row.id, { actual_state: "running" });
      }
    } else {
      if (alive && row.monitor_pid) {
        stopPid(row.monitor_pid);
      }
      if (row.monitor_pid) {
        updateForwardSession(sessionDb, row.id, { monitor_pid: null });
      }
      if (row.actual_state !== "stopped") {
        updateForwardSession(sessionDb, row.id, { actual_state: "stopped" });
      }
    }
  }
}

function wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clampPositive(value: number | undefined, fallback: number): number {
  const n = Number(value);
  if (Number.isFinite(n) && n > 0) return n;
  return fallback;
}

async function runSupervisorLoop(
  sessionDb: string,
  intervalMs: number,
  logger: Logger,
  abortSignal: { stopped: boolean },
) {
  while (!abortSignal.stopped) {
    try {
      await superviseSessions(sessionDb, logger);
      await superviseForwards(sessionDb, logger);
    } catch (err) {
      logger.error("daemon supervise error", {
        error: err instanceof Error ? err.message : String(err),
      });
    }
    for (
      let elapsed = 0;
      elapsed < intervalMs && !abortSignal.stopped;
      elapsed += 250
    ) {
      await wait(250);
    }
  }
}

function spawnDetachedDaemon(sessionDb: string) {
  ensureDir();
  const launcher = resolveSelfLaunch();
  const args: string[] = [
    ...launcher.args,
    "daemon",
    "run",
    "--session-db",
    sessionDb,
  ];
  const child = spawn(launcher.command, args, {
    stdio: "ignore",
    detached: true,
    env: process.env,
  });
  child.unref();
  return child.pid ?? 0;
}

export function ensureDaemonRunning(
  sessionDb: string,
  logger?: Logger,
): number | null {
  if (process.env.REFLECT_DISABLE_DAEMON === "1") {
    return null;
  }
  const existing = readPidSync();
  if (isPidAlive(existing)) {
    return existing ?? null;
  }
  if (existing) {
    removePidSync();
  }
  const pid = spawnDetachedDaemon(sessionDb);
  if (pid) {
    logger?.debug?.("daemon auto-started", { pid });
    return pid;
  }
  logger?.warn?.("failed to start daemon automatically");
  return null;
}

async function stopDaemonProcess(pid: number, logger: Logger) {
  try {
    process.kill(pid, "SIGTERM");
  } catch (err) {
    const code = (err as any)?.code;
    if (code === "ESRCH") return;
    logger.warn("failed to send SIGTERM", { pid, error: String(err) });
    return;
  }
  const timeoutMs = 5000;
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (!isPidAlive(pid)) return;
    await wait(200);
  }
  try {
    process.kill(pid, "SIGKILL");
  } catch {}
}

type ForegroundOptions = {
  stopExisting?: boolean;
  logLevel?: LogLevel;
};

async function runDaemonForeground(
  sessionDb: string,
  options: ForegroundOptions = {},
) {
  const logHandle = createDaemonLogger(sessionDb, {
    scope: "daemon",
    echoLevel: options.logLevel ?? "info",
  });
  const daemonLogger = logHandle.logger;
  const stopSignal = { stopped: false };
  const existing = readPidSync();
  let wrotePid = false;
  let cleaned = false;
  const cleanUp = () => {
    if (cleaned) return;
    if (wrotePid) {
      removePidSync();
    }
    logHandle.close();
    cleaned = true;
  };
  try {
    if (isPidAlive(existing)) {
      if (options.stopExisting) {
        daemonLogger.info("stopping existing daemon before foreground start", {
          pid: existing,
        });
        if (existing) await stopDaemonProcess(existing, daemonLogger);
      } else {
        throw new Error(`daemon already running (pid ${existing})`);
      }
    }
    if (existing) {
      removePidSync();
    }
    writePidSync(process.pid);
    wrotePid = true;
    const handleSignal = (sig: string) => {
      if (!stopSignal.stopped) {
        daemonLogger.info(`received ${sig}, stopping daemon`);
        stopSignal.stopped = true;
      }
    };
    const onSigint = () => handleSignal("SIGINT");
    const onSigterm = () => handleSignal("SIGTERM");
    process.on("SIGINT", onSigint);
    process.on("SIGTERM", onSigterm);
    process.on("exit", cleanUp);
    process.on("uncaughtException", (err) => {
      daemonLogger.error("uncaught exception", {
        error: err instanceof Error ? (err.stack ?? err.message) : String(err),
      });
      stopSignal.stopped = true;
      cleanUp();
      process.exit(1);
    });
    daemonLogger.info("daemon started", { sessionDb, mode: "foreground" });
    await runSupervisorLoop(sessionDb, 3000, daemonLogger, stopSignal);
    daemonLogger.info("daemon stopped");
  } finally {
    cleanUp();
  }
}

function formatStatus(pid: number | null, running: boolean) {
  if (!pid) return "stopped";
  return running ? `running (pid ${pid})` : `stale (pid ${pid})`;
}

function shellEscape(input: string): string {
  return `'${input.replace(/'/g, "'\\''")}'`;
}

function writeSystemdUnit(execArgs: string[]) {
  const unitDir = join(os.homedir(), ".config", "systemd", "user");
  fs.mkdirSync(unitDir, { recursive: true });
  const unitPath = join(unitDir, `${SERVICE_NAME}.service`);
  const execLine = execArgs.map(shellEscape).join(" ");
  const content = `[Unit]\nDescription=Reflect Sync Daemon\nAfter=default.target\n\n[Service]\nType=simple\nExecStart=${execLine}\nRestart=on-failure\nEnvironment=REFLECT_DISABLE_LOG_ECHO=1\n\n[Install]\nWantedBy=default.target\n`;
  fs.writeFileSync(unitPath, content);
  return unitPath;
}

function writeLaunchAgent(execArgs: string[]) {
  const agentDir = join(os.homedir(), "Library", "LaunchAgents");
  fs.mkdirSync(agentDir, { recursive: true });
  const plistPath = join(agentDir, "com.reflect.sync.plist");
  const programArguments = execArgs
    .map((arg) => `        <string>${arg}</string>`)
    .join("\n");
  const content = `<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE plist PUBLIC "-//Apple Computer//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n<plist version="1.0">\n  <dict>\n    <key>Label</key>\n    <string>com.reflect.sync</string>\n    <key>ProgramArguments</key>\n    <array>\n${programArguments}\n    </array>\n    <key>RunAtLoad</key>\n    <true/>\n    <key>KeepAlive</key>\n    <true/>\n    <key>EnvironmentVariables</key>\n    <dict>\n      <key>REFLECT_DISABLE_LOG_ECHO</key>\n      <string>1</string>\n    </dict>\n  </dict>\n</plist>\n`;
  fs.writeFileSync(plistPath, content);
  return plistPath;
}

function defaultExecArgs(sessionDb: string): string[] {
  const args: string[] = [];
  args.push(process.execPath);
  if (!process.env.REFLECT_BUNDLED && process.argv[1]) {
    args.push(process.argv[1]);
  }
  args.push("daemon", "run", "--session-db", sessionDb);
  return args;
}

export function registerSessionDaemon(program: Command) {
  const addSessionDbOption = (cmd: Command) =>
    cmd.option(
      "--session-db <file>",
      "override path to sessions.db",
      getSessionDbPath(),
    );

  const daemon = program
    .command("daemon")
    .description("manage the reflect-sync background daemon");

  addSessionDbOption(daemon);

  addSessionDbOption(
    daemon
      .command("start")
      .description("start the daemon if it is not already running")
      .action(async (opts: { sessionDb?: string }, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        const existing = readPidSync();
        if (isPidAlive(existing)) {
          console.log(`daemon already running (pid ${existing})`);
          return;
        }
        if (existing && !isPidAlive(existing)) {
          removePidSync();
        }
        const pid = spawnDetachedDaemon(sessionDb);
        console.log(`daemon starting (child pid ${pid})`);
      }),
  );

  addSessionDbOption(
    daemon
      .command("stop")
      .description("stop the daemon if running")
      .action(async (_opts: { sessionDb?: string }, _command: Command) => {
        const logger = new ConsoleLogger("info");
        const existing = readPidSync();
        if (!existing || !isPidAlive(existing)) {
          console.log("daemon not running");
          removePidSync();
          return;
        }
        await stopDaemonProcess(existing, logger);
        removePidSync();
        console.log("daemon stopped");
      }),
  );

  addSessionDbOption(
    daemon
      .command("status")
      .description("show daemon status")
      .action((_opts: { sessionDb?: string }, _command: Command) => {
        const pid = readPidSync();
        const running = isPidAlive(pid);
        console.log(formatStatus(pid, running));
      }),
  );

  addSessionDbOption(
    daemon
      .command("logs")
      .description("show recent daemon logs")
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
      .action(async (opts: any, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        const minLevel = parseLogLevelOption(opts.level);
        const tail = clampPositive(opts.tail, opts.follow ? 100 : 10000);
        const sinceTs =
          opts.since != null && Number.isFinite(Number(opts.since))
            ? Number(opts.since)
            : undefined;
        const scope = opts.scope ? String(opts.scope) : undefined;
        const message = opts.message ? String(opts.message) : undefined;
        let rows = fetchDaemonLogs(sessionDb, {
          limit: tail,
          minLevel,
          sinceTs,
          order: "desc",
          scope,
          message,
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
            rows = fetchDaemonLogs(sessionDb, {
              afterId: lastId,
              minLevel,
              order: "asc",
              scope,
              message,
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
    daemon
      .command("run")
      .description("run the daemon in the foreground (if not already running)")
      .action(async (opts: { sessionDb?: string }, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        const globals = command.optsWithGlobals() as { logLevel?: string };
        const logLevel = parseLogLevel(globals.logLevel, "debug");
        try {
          await runDaemonForeground(sessionDb, {
            stopExisting: false,
            logLevel,
          });
        } catch (err) {
          console.error(err instanceof Error ? err.message : String(err));
          process.exit(1);
        }
      }),
  );

  addSessionDbOption(
    daemon
      .command("install")
      .description("install the daemon as a user service (systemd/launchd)")
      .action(async (opts: { sessionDb?: string }, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        const execArgs = defaultExecArgs(sessionDb);
        const platform = process.platform;
        if (platform === "linux") {
          const unitPath = writeSystemdUnit(execArgs);
          console.log(`wrote systemd unit to ${unitPath}`);
          try {
            await new Promise<void>((resolve, reject) => {
              const child = spawn("systemctl", ["--user", "daemon-reload"], {
                stdio: "inherit",
              });
              child.on("exit", (code) =>
                code === 0 ? resolve() : reject(code),
              );
              child.on("error", reject);
            });
            await new Promise<void>((resolve, reject) => {
              const child = spawn(
                "systemctl",
                ["--user", "enable", "--now", `${SERVICE_NAME}.service`],
                { stdio: "inherit" },
              );
              child.on("exit", (code) =>
                code === 0 ? resolve() : reject(code),
              );
              child.on("error", reject);
            });
            console.log("systemd user service enabled (reflect-sync)");
          } catch (err) {
            console.warn(
              "failed to enable systemd service; you may need to run the commands manually",
              err,
            );
          }
        } else if (platform === "darwin") {
          const plistPath = writeLaunchAgent(execArgs);
          console.log(`wrote launch agent to ${plistPath}`);
          try {
            await new Promise<void>((resolve, reject) => {
              const child = spawn("launchctl", ["load", "-w", plistPath], {
                stdio: "inherit",
              });
              child.on("exit", (code) =>
                code === 0 ? resolve() : reject(code),
              );
              child.on("error", reject);
            });
            console.log("launchctl agent loaded");
          } catch (err) {
            console.warn(
              "failed to load launch agent; run 'launchctl load -w <plist>' manually",
              err,
            );
          }
        } else {
          console.log(
            "automatic install is only supported on macOS and Linux. Please invoke 'reflect daemon run' via your preferred service manager.",
          );
        }
      }),
  );
}
