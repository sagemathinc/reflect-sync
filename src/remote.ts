// --- Remote HOME resolver + path expansion (with cache) ---
// remote.ts

import { homedir } from "node:os";
import { join, posix } from "node:path";
import { spawn } from "node:child_process";
import type { Logger } from "./logger.js";
import { maybeRestartSshControl } from "./ssh-control.js";
import type { FilesystemCapabilities } from "./fs-capabilities.js";

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

const remoteCacheKey = (host: string, port?: number) =>
  port != null ? `${host}:${port}` : host;

// timeout in seconds for any ssh command attempt
const TIMEOUT = 5;

function withControlPath(args: string[]): string[] {
  const controlPath = process.env.REFLECT_SSH_CONTROL_PATH;
  if (!controlPath) return args;
  return ["-S", controlPath, ...args];
}

async function runSshCommand(
  host: string,
  args: string[],
  log: RemoteLogOptions | undefined,
  what: string | undefined,
): Promise<{ stdout: string; stderr: string }> {
  const finalArgs = withControlPath(args);
  log?.logger?.debug("ssh exec", {
    host,
    command: argsJoin(finalArgs),
    label: what,
  });
  if (log?.verbose) console.log("$ ssh", argsJoin(finalArgs));
  let stdout = "",
    stderr = "";
  await new Promise<void>((resolve, reject) => {
    const p = spawn("ssh", finalArgs, { stdio: ["ignore", "pipe", "pipe"] });
    p.stdout!.on("data", (c) => (stdout += c));
    p.stderr!.on("data", (c) => (stderr += c));
    p.once("exit", (c) =>
      !c
        ? resolve()
        : reject(
            new Error(
              `${what ?? argsJoin(finalArgs)} failed on ${host} (exit ${c}) -- ${stderr}`,
            ),
          ),
    );
    p.once("error", reject);
  });
  return { stdout, stderr };
}

async function ssh(
  host: string,
  args: string[],
  log: RemoteLogOptions | undefined,
  what: string | undefined,
): Promise<{ stdout: string; stderr: string }> {
  let attempt = 0;
  while (true) {
    try {
      return await runSshCommand(host, args, log, what);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err ?? "");
      if (attempt === 0 && (await maybeRestartSshControl(message))) {
        attempt += 1;
        continue;
      }
      throw err;
    }
  }
}

export async function remoteWhich(
  host: string,
  cmd: string,
  {
    logger,
    verbose,
    noCache,
    port,
  }: {
    logger?: Logger;
    verbose?: boolean;
    noCache?: boolean;
    port?: number;
  } = {},
): Promise<string> {
  const key = `${remoteCacheKey(host, port)}:${cmd}`;
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
  ];
  if (port != null) {
    args.push("-p", String(port));
  }
  args.push(host, `sh -lc 'which ${cmd}'`);
  const logCfg: RemoteLogOptions | undefined =
    logger || verbose ? { logger, verbose } : undefined;
  let { stdout: out } = await ssh(host, args, logCfg, `which ${cmd}`);
  out = out.trim();
  remoteWhichCache.set(key, out);
  return out;
}

const remoteHomeCache = new Map<string, string>();

export async function resolveRemoteHome(
  host: string,
  log?: RemoteLogOptions,
  port?: number,
): Promise<string> {
  const key = remoteCacheKey(host, port);
  if (remoteHomeCache.has(key)) return remoteHomeCache.get(key)!;

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
  ];
  if (port != null) {
    args.push("-p", String(port));
  }
  args.push(host, `sh -lc ${cmd}`);
  const { stdout } = await ssh(host, args, log, cmd);
  const home = stdout.trim();
  if (!home) throw new Error(`Empty HOME from ${host}`);
  remoteHomeCache.set(key, home);
  return home;
}

export async function isRoot(
  host?: string,
  log?: RemoteLogOptions,
  port?: number,
) {
  if (!host) {
    return process.geteuid?.() === 0;
  }
  // this isn't technically the same as being root, but
  // we cache it and implemented it above, so are going with it for now
  return (await resolveRemoteHome(host, log, port)) == "/root";
}

export async function expandHome(
  p: string,
  host?: string,
  log?: RemoteLogOptions,
  port?: number,
): Promise<string> {
  if (!p || p[0] !== "~") return p;
  if (p === "~") {
    const base = host ? await resolveRemoteHome(host, log, port) : homedir();
    return base;
  }
  if (p.startsWith("~/")) {
    const base = host ? await resolveRemoteHome(host, log, port) : homedir();
    return join(base, p.slice(2));
  }
  // "~user" not handled intentionally
  return p;
}

export async function ensureRemoteParentDir({
  host,
  path,
  logger,
  verbose,
  port,
}: {
  host: string;
  path: string;
  logger?: Logger;
  verbose?: boolean;
  port?: number;
}): Promise<void> {
  let target = path;
  if (target.startsWith("~/")) {
    target = target.slice(2);
  }
  const dirname = posix.dirname(target);
  const cmd = `mkdir -p -- ${shellEscape(dirname)}`;
  const logCfg: RemoteLogOptions | undefined =
    logger || verbose ? { logger, verbose } : undefined;
  const args = [
    "-o",
    `ConnectTimeout=${TIMEOUT}`,
    "-C",
    "-T",
    "-o",
    "BatchMode=yes",
  ];
  if (port != null) {
    args.push("-p", String(port));
  }
  args.push(host, "--", cmd);
  await ssh(host, args, logCfg, cmd);
}

export async function detectRemoteFilesystemCapabilities({
  host,
  port,
  root,
  remoteCommand,
  logger,
}: {
  host: string;
  port?: number;
  root: string;
  remoteCommand: string;
  logger?: Logger;
}): Promise<FilesystemCapabilities> {
  const args = [
    "-o",
    `ConnectTimeout=${TIMEOUT}`,
    "-C",
    "-T",
    "-o",
    "BatchMode=yes",
  ];
  if (port != null) {
    args.push("-p", String(port));
  }
  args.push(host, `${remoteCommand} fs-capabilities`, "--root", root);
  const logCfg = logger ? { logger } : undefined;
  let stdout: string;
  try {
    const result = await ssh(host, args, logCfg, "fs-capabilities");
    stdout = result.stdout;
  } catch (err) {
    throw new Error(
      `remote filesystem capability probe failed for ${host}:${root}`,
      { cause: err },
    );
  }
  try {
    return JSON.parse(stdout.trim()) as FilesystemCapabilities;
  } catch (err) {
    throw new Error(
      `failed to parse remote filesystem capabilities for ${host}:${root}: ${stdout}`,
      { cause: err },
    );
  }
}

export class RemoteConnectionError extends Error {
  override readonly cause?: unknown;
  constructor(message: string, options?: { cause?: unknown }) {
    super(message);
    this.name = "RemoteConnectionError";
    this.cause = options?.cause;
  }
}

export async function remoteDirExists({
  host,
  path,
  logger,
  verbose,
  port,
}: {
  host: string;
  path: string;
  logger?: Logger;
  verbose?: boolean;
  port?: number;
}): Promise<boolean> {
  const args = [
    "-o",
    `ConnectTimeout=${TIMEOUT}`,
    "-C",
    "-T",
    "-o",
    "BatchMode=yes",
  ];
  if (port != null) {
    args.push("-p", String(port));
  }
  const escapedPath = shellEscape(path);
  const command = `if test -d ${escapedPath}; then printf yes; else printf no; fi`;
  args.push(host, `sh -lc ${shellEscape(command)}`);
  const logCfg: RemoteLogOptions | undefined =
    logger || verbose ? { logger, verbose } : undefined;
  try {
    const { stdout } = await ssh(host, args, logCfg, `test -d ${path}`);
    const out = stdout.trim();
    return out === "yes";
  } catch (err) {
    throw new RemoteConnectionError(
      `failed to check directory '${path}' on ${host}`,
      { cause: err },
    );
  }
}

export async function sshDeleteDirectory({
  host,
  path,
  logger,
  verbose,
  port,
}: {
  host: string;
  path: string;
  logger?: Logger;
  verbose?: boolean;
  port?: number;
}) {
  const logCfg: RemoteLogOptions | undefined =
    logger || verbose ? { logger, verbose } : undefined;
  path = await expandHome(path, host, logCfg, port);
  const cmd = `rm -rf -- ${shellEscape(path)}`;
  const args = [
    "-o",
    `ConnectTimeout=${TIMEOUT}`,
    "-C",
    "-T",
    "-o",
    "BatchMode=yes",
  ];
  if (port != null) {
    args.push("-p", String(port));
  }
  args.push(host, "--", cmd);
  await ssh(host, args, logCfg, cmd);
}
