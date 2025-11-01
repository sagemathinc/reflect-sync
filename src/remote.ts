// --- Remote HOME resolver + path expansion (with cache) ---
// remote.ts

import { homedir } from "node:os";
import { join, posix } from "node:path";
import { spawn } from "node:child_process";
import type { Logger } from "./logger.js";

type RemoteLogOptions = {
  logger?: Logger;
  verbose?: boolean;
};

// single-quote safe escape for sh -lc
function shellEscape(s: string): string {
  return `'${String(s).replace(/'/g, `'\\''`)}'`;
}

export function argsJoin(args: string[]): string {
  return args.map((x) => (x.includes(" ") ? `'${x}'` : x)).join(" ");
}

const remoteWhichCache = new Map<string, string>();

// timeout in seconds for any ssh command attempt
const TIMEOUT = 5;

async function ssh(
  host: string,
  args: string[],
  log: RemoteLogOptions | undefined,
  what: string | undefined,
): Promise<{ stdout: string; stderr: string }> {
  log?.logger?.debug("ssh exec", { host, command: argsJoin(args), label: what });
  if (log?.verbose) console.log("$ ssh", argsJoin(args));
  let stdout = "",
    stderr = "";
  await new Promise<void>((resolve, reject) => {
    const p = spawn("ssh", args, { stdio: ["ignore", "pipe", "pipe"] });
    p.stdout!.on("data", (c) => (stdout += c));
    p.stderr!.on("data", (c) => (stderr += c));
    p.once("exit", (c) =>
      !c
        ? resolve()
        : reject(
            new Error(
              `${what ?? argsJoin(args)} failed on ${host} (exit ${c}) -- ${stderr}`,
            ),
          ),
    );
    p.once("error", reject);
  });
  return { stdout, stderr };
}

export async function remoteWhich(
  host: string,
  cmd: string,
  {
    logger,
    verbose,
    noCache,
  }: { logger?: Logger; verbose?: boolean; noCache?: boolean } = {},
): Promise<string> {
  const key = `${host}:${cmd}`;
  if (!noCache && remoteWhichCache.has(key)) {
    return remoteWhichCache.get(key)!;
  }
  const args = [
    "-o",
    `ConnectTimeout=${TIMEOUT}`,
    "-C",
    "-T",
    "-o",
    "BatchMode=yes",
    host,
    `sh -lc 'which ${cmd}'`,
  ];
  const logCfg: RemoteLogOptions | undefined = logger || verbose ? { logger, verbose } : undefined;
  let { stdout: out } = await ssh(host, args, logCfg, `which ${cmd}`);
  out = out.trim();
  remoteWhichCache.set(key, out);
  return out;
}

const remoteHomeCache = new Map<string, string>();

export async function resolveRemoteHome(
  host: string,
  log?: RemoteLogOptions,
): Promise<string> {
  if (remoteHomeCache.has(host)) return remoteHomeCache.get(host)!;

  // Use shell's own tilde expansion instead of $HOME:
  // 1) cd ~ (expands ~)
  // 2) pwd -P (resolve symlinks; canonical)
  // Fallback: getent passwd <user> | cut -d: -f6
  const cmd =
    'cd ~ 2>/dev/null && pwd -P || (getent passwd "$(id -un)" | cut -d: -f6)';
  const args = [
    "-o",
    `ConnectTimeout=${TIMEOUT}`,
    "-C",
    "-T",
    "-o",
    "BatchMode=yes",
    host,
    `sh -lc ${cmd}`,
  ];
  const { stdout } = await ssh(host, args, log, cmd);
  const home = stdout.trim();
  if (!home) throw new Error(`Empty HOME from ${host}`);
  remoteHomeCache.set(host, home);
  return home;
}

export async function isRoot(host?: string, log?: RemoteLogOptions) {
  if (!host) {
    return process.geteuid?.() === 0;
  }
  // this isn't technically the same as being root, but
  // we cache it and implemented it above, so are going with it for now
  return (await resolveRemoteHome(host, log)) == "/root";
}

export async function expandHome(
  p: string,
  host?: string,
  log?: RemoteLogOptions,
): Promise<string> {
  if (!p || p[0] !== "~") return p;
  if (p === "~") {
    const base = host ? await resolveRemoteHome(host, log) : homedir();
    return base;
  }
  if (p.startsWith("~/")) {
    const base = host ? await resolveRemoteHome(host, log) : homedir();
    return join(base, p.slice(2));
  }
  // "~user" not handled intentionally
  return p;
}

export async function ensureRemoteParentDir(
  host: string,
  path: string,
  log?: RemoteLogOptions,
): Promise<void> {
  if (path.startsWith("~/")) {
    // relative to HOME
    path = path.slice(2);
  }
  const dirname = posix.dirname(path);
  const cmd = `mkdir -p -- ${shellEscape(dirname)}`;
  const args = [
    "-o",
    `ConnectTimeout=${TIMEOUT}`,
    "-C",
    "-T",
    "-o",
    "BatchMode=yes",
    host,
    "--",
    cmd,
  ];
  await ssh(host, args, log, cmd);
}

export async function sshDeleteDirectory({
  host,
  path,
  logger,
  verbose,
}: {
  host: string;
  path: string;
  logger?: Logger;
  verbose?: boolean;
}) {
  const logCfg: RemoteLogOptions | undefined = logger || verbose ? { logger, verbose } : undefined;
  path = await expandHome(path, host, logCfg);
  const cmd = `rm -rf -- ${shellEscape(path)}`;
  const args = [
    "-o",
    `ConnectTimeout=${TIMEOUT}`,
    "-C",
    "-T",
    "-o",
    "BatchMode=yes",
    host,
    "--",
    cmd,
  ];
  await ssh(host, args, logCfg, cmd);
}
