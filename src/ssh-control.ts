import { spawn } from "node:child_process";
import fsp from "node:fs/promises";
import path from "node:path";
import type { Logger } from "./logger.js";

export type SshControlOptions = {
  host: string;
  port?: number | null;
  socketPath: string;
  persistSeconds?: number;
  logger?: Logger;
};

export type SshControlHandle = {
  socketPath: string;
  close: () => Promise<void>;
};

async function runSsh(args: string[]): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const p = spawn("ssh", args, { stdio: "ignore" });
    p.once("error", reject);
    p.once("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`ssh ${args.join(" ")} exited ${code}`));
    });
  });
}

function buildBaseArgs(socketPath: string, port?: number | null): string[] {
  const args = ["-S", socketPath, "-o", "BatchMode=yes"];
  if (port != null) {
    args.push("-p", String(port));
  }
  return args;
}

function sanitizeSocketPath(p: string): string {
  if (process.platform === "win32") {
    return p;
  }
  if (p.length < 100) return p;
  // shrink path but keep directory
  const dir = path.dirname(p);
  const hash = Buffer.from(p).toString("base64url").slice(0, 16);
  return path.join(dir, `ssh-${hash}.sock`);
}

export async function createSshControlMaster(
  opts: SshControlOptions,
): Promise<SshControlHandle | null> {
  const socketPath = sanitizeSocketPath(opts.socketPath);
  try {
    await fsp.mkdir(path.dirname(socketPath), { recursive: true });
  } catch {}
  try {
    await fsp.unlink(socketPath);
  } catch {}

  const baseArgs = buildBaseArgs(socketPath, opts.port);
  const host = opts.host;

  const persistSeconds = Math.max(5, opts.persistSeconds ?? 60);

  try {
    await runSsh([
      ...baseArgs,
      "-M",
      "-o",
      `ControlPersist=${persistSeconds}s`,
      "-fNT",
      host,
    ]);
  } catch (err) {
    opts.logger?.warn?.("ssh control master unavailable", {
      error: err instanceof Error ? err.message : String(err),
      host,
    });
    try {
      await fsp.unlink(socketPath);
    } catch {}
    return null;
  }

  const close = async () => {
    try {
      await runSsh([...baseArgs, "-O", "exit", host]);
    } catch (err) {
      opts.logger?.debug?.("ssh control master exit failed", {
        error: err instanceof Error ? err.message : String(err),
        host,
      });
    }
    try {
      await fsp.unlink(socketPath);
    } catch {}
  };

  return { socketPath, close };
}

type RestartFn = () => Promise<boolean>;
let restartHandler: RestartFn | null = null;

export function registerSshControlRestart(fn: RestartFn | null) {
  restartHandler = fn;
}

const CONTROL_SOCKET_PATTERNS = [
  "control socket connect",
  "control socket open",
  "muxclient_request_session",
  "master is dead",
  "connection refused",
  "no such file",
];

export async function maybeRestartSshControl(message?: string): Promise<boolean> {
  if (!restartHandler || !message) return false;
  const lower = message.toLowerCase();
  if (!CONTROL_SOCKET_PATTERNS.some((p) => lower.includes(p))) return false;
  return restartHandler();
}
