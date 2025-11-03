#!/usr/bin/env node
import { Command } from "commander";
import { spawn, type ChildProcess } from "node:child_process";
import { setTimeout as wait } from "node:timers/promises";
import { argsJoin } from "./remote.js";
import {
  ensureSessionDb,
  loadForwardById,
  updateForwardSession,
  type ForwardRow,
  type ForwardPatch,
} from "./session-db.js";

function buildProgram(): Command {
  return new Command()
    .name("forward-monitor")
    .requiredOption("--session-db <file>", "path to sessions database")
    .requiredOption("--id <id>", "forward session id");
}

function buildSshArgs(row: ForwardRow): string[] {
  const args: string[] = ["-N"]; // no shell, just forward
  if (row.ssh_port) {
    args.push("-p", String(row.ssh_port));
  }
  if (row.ssh_compress) {
    args.push("-C");
  }
  if (row.direction === "local_to_remote") {
    const bindHost = row.local_host || "127.0.0.1";
    const binding = `${bindHost}:${row.local_port}:${row.remote_host}:${row.remote_port}`;
    args.push("-L", binding);
  } else {
    const bindHost = row.remote_host || "0.0.0.0";
    const binding = bindHost
      ? `${bindHost}:${row.remote_port}:${row.local_host}:${row.local_port}`
      : `${row.remote_port}:${row.local_host}:${row.local_port}`;
    args.push("-R", binding);
  }
  args.push(row.ssh_host);
  return args;
}

export async function runForwardMonitor(sessionDb: string, id: number) {
  const db = ensureSessionDb(sessionDb);
  db.close();

  let backoffMs = 1000;
  const maxBackoff = 30_000;
  let shuttingDown = false;
  let currentChild: ChildProcess | null = null;

  const stopChild = () => {
    if (currentChild && !currentChild.killed) {
      try {
        currentChild.kill("SIGTERM");
      } catch {}
    }
  };

  const handleSignal = () => {
    shuttingDown = true;
    stopChild();
  };
  const signals: NodeJS.Signals[] = ["SIGINT", "SIGTERM", "SIGQUIT"];
  for (const sig of signals) {
    process.on(sig, handleSignal);
  }

  try {
    while (!shuttingDown) {
      const row = loadForwardById(sessionDb, id);
      if (!row) {
        break;
      }
      if (row.desired_state !== "running") {
        updateForwardSession(sessionDb, id, {
          actual_state: "stopped",
          monitor_pid: null,
        });
        break;
      }

      updateForwardSession(sessionDb, id, {
        monitor_pid: process.pid,
        actual_state: "running",
        last_error: null,
      });

      const args = buildSshArgs(row);
      updateForwardSession(sessionDb, id, {
        ssh_args: argsJoin(["ssh", ...args]),
      });
      const child = spawn("ssh", args, {
        stdio: "inherit",
      });
      currentChild = child;

      const exitCode: number | null = await new Promise((resolve) => {
        child.once("exit", (code) => resolve(code));
        child.once("error", () => resolve(1));
      });

      currentChild = null;

      if (shuttingDown) {
        break;
      }

      const latest = loadForwardById(sessionDb, id);
      if (!latest) {
        break;
      }

      if (latest.desired_state !== "running") {
        updateForwardSession(sessionDb, id, {
          actual_state: "stopped",
          monitor_pid: null,
        });
        break;
      }

      updateForwardSession(sessionDb, id, {
        actual_state: "error",
        last_error: exitCode === 0 ? null : `ssh exited with code ${exitCode}`,
        monitor_pid: null,
      });

      await wait(backoffMs);
      if (shuttingDown) {
        break;
      }
      backoffMs = Math.min(backoffMs * 2, maxBackoff);
    }
  } finally {
    stopChild();
    const row = loadForwardById(sessionDb, id);
    if (row) {
      const patch: ForwardPatch = {
        monitor_pid: null,
      };
      if (shuttingDown || row.desired_state !== "running") {
        patch.actual_state = "stopped";
      }
      updateForwardSession(sessionDb, id, patch);
    }
    for (const sig of signals) {
      process.off(sig, handleSignal);
    }
  }
}

export async function runForwardMonitorCli(argv = process.argv) {
  const program = buildProgram();
  const opts = program.parse(argv).opts<{
    sessionDb: string;
    id: string;
  }>();
  const id = Number(opts.id);
  if (!Number.isInteger(id) || id <= 0) {
    throw new Error("invalid forward id");
  }
  await runForwardMonitor(opts.sessionDb, id);
}

if (process.argv[1] && process.argv[1].includes("forward-monitor")) {
  runForwardMonitorCli().catch((err) => {
    console.error("forward monitor fatal", err);
    process.exit(1);
  });
}
