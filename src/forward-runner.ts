import { spawn } from "node:child_process";
import { argsJoin } from "./remote.js";
import { updateForwardSession, type ForwardRow } from "./session-db.js";

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
    // use 0.0.0.0 to align behavior with mutagen.  This remote ssh server
    // should have "GatewayPorts clientspecified" set.
    const bindHost = row.remote_host || "0.0.0.0";
    const binding = bindHost
      ? `${bindHost}:${row.remote_port}:${row.local_host}:${row.local_port}`
      : `${row.remote_port}:${row.local_host}:${row.local_port}`;
    args.push("-R", binding);
  }
  args.push(row.ssh_host);
  return args;
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

export async function launchForwardProcess(
  sessionDb: string,
  forward: ForwardRow,
): Promise<number | null> {
  const args = buildSshArgs(forward);
  updateForwardSession(sessionDb, forward.id, {
    ssh_args: argsJoin(["ssh", ...args]),
  });

  return await new Promise((resolve) => {
    const child = spawn("ssh", args, {
      stdio: "ignore",
      detached: true,
      env: process.env,
    });

    const fail = (message: string) => {
      try {
        child.unref();
      } catch {}
      updateForwardSession(sessionDb, forward.id, {
        monitor_pid: null,
        actual_state: "error",
        last_error: message,
      });
      resolve(null);
    };

    child.once("error", (err) => {
      const message = err instanceof Error ? err.message : String(err);
      fail(message || "failed to start ssh");
    });

    const pid = child.pid;
    if (!pid) {
      fail("ssh did not provide a pid");
      return;
    }

    // Give ssh a brief moment to ensure it is running.
    setTimeout(() => {
      if (!isPidAlive(pid)) {
        fail("ssh exited immediately");
        return;
      }
      updateForwardSession(sessionDb, forward.id, {
        monitor_pid: pid,
        actual_state: "running",
        last_error: null,
      });
      try {
        child.unref();
      } catch {}
      resolve(pid);
    }, 200);
  });
}
