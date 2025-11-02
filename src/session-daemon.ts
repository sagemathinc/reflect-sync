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
import { spawnForwardMonitor } from "./forward-runner.js";
import { stopPid } from "./session-manage.js";
import { ConsoleLogger, type Logger } from "./logger.js";

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
        const pid = spawnSchedulerForSession(sessionDb, row);
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
      const targetState: ActualState = "paused";
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
        const pid = spawnForwardMonitor(sessionDb, forwardRow);
        if (pid) {
          updateForwardSession(sessionDb, row.id, {
            monitor_pid: pid,
            actual_state: "running",
            last_error: null,
          });
          logger.debug?.("launched forward monitor", { id: row.id, pid });
        } else {
          updateForwardSession(sessionDb, row.id, { actual_state: "error" });
          logger.warn("failed to launch forward monitor", { id: row.id });
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
    for (let elapsed = 0; elapsed < intervalMs && !abortSignal.stopped; elapsed += 250) {
      await wait(250);
    }
  }
}

function spawnDetachedDaemon(sessionDb: string) {
  ensureDir();
  const args: string[] = process.env.REFLECT_BUNDLED ? [] : [process.argv[1]];
  args.push("daemon", "run", "--session-db", sessionDb);
  const child = spawn(process.execPath, args, {
    stdio: "ignore",
    detached: true,
    env: process.env,
  });
  child.unref();
  return child.pid ?? 0;
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
  const programArguments = execArgs.map((arg) => `        <string>${arg}</string>`).join("\n");
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
      .command("run")
      .description("run the daemon supervisor loop (internal)")
      .action(async (opts: { sessionDb?: string }, command: Command) => {
        const sessionDb = resolveSessionDb(opts, command);
        const existing = readPidSync();
        if (isPidAlive(existing)) {
          console.error(`daemon already running (pid ${existing})`);
          process.exit(1);
        }
        if (existing) {
          removePidSync();
        }
        writePidSync(process.pid);
        const baseLogger = new ConsoleLogger("info");
        const logger = baseLogger.child("daemon");
        const stopSignal = { stopped: false };
        const handleSignal = (sig: string) => {
          if (!stopSignal.stopped) {
            logger.info(`received ${sig}, stopping daemon`);
            stopSignal.stopped = true;
          }
        };
        process.on("SIGINT", () => handleSignal("SIGINT"));
        process.on("SIGTERM", () => handleSignal("SIGTERM"));
        const cleanUp = () => {
          removePidSync();
        };
        process.on("exit", cleanUp);
        process.on("uncaughtException", (err) => {
          logger.error("uncaught exception", {
            error: err instanceof Error ? err.stack ?? err.message : String(err),
          });
          stopSignal.stopped = true;
          cleanUp();
          process.exit(1);
        });
        logger.info("daemon started", { sessionDb });
        await runSupervisorLoop(sessionDb, 3000, logger, stopSignal);
        removePidSync();
        logger.info("daemon stopped");
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
              child.on("exit", (code) => (code === 0 ? resolve() : reject(code)));
              child.on("error", reject);
            });
            await new Promise<void>((resolve, reject) => {
              const child = spawn(
                "systemctl",
                ["--user", "enable", "--now", `${SERVICE_NAME}.service`],
                { stdio: "inherit" },
              );
              child.on("exit", (code) => (code === 0 ? resolve() : reject(code)));
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
              child.on("exit", (code) => (code === 0 ? resolve() : reject(code)));
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
