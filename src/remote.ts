// --- Remote HOME resolver + path expansion (with cache) ---
// remote.ts

import { homedir } from "node:os";
import { join, posix } from "node:path";
import { spawn } from "node:child_process";

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
  verbose: boolean | undefined,
  what: string | undefined,
): Promise<{ stdout: string; stderr: string }> {
  if (verbose) console.log("$ ssh", argsJoin(args));
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
  { verbose, noCache }: { verbose?: boolean; noCache?: boolean } = {},
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
  let { stdout: out } = await ssh(host, args, verbose, `which ${cmd}`);
  out = out.trim();
  remoteWhichCache.set(key, out);
  return out;
}

const remoteHomeCache = new Map<string, string>();

export async function resolveRemoteHome(
  host: string,
  verbose?: boolean,
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
  const { stdout } = await ssh(host, args, verbose, cmd);
  const home = stdout.trim();
  if (!home) throw new Error(`Empty HOME from ${host}`);
  remoteHomeCache.set(host, home);
  return home;
}

export async function isRoot(host?: string, verbose?: boolean) {
  if (!host) {
    return process.geteuid?.() === 0;
  }
  // this isn't technically the same as being root, but
  // we cache it and implemented it above, so are going with it for now
  return (await resolveRemoteHome(host, verbose)) == "/root";
}

export async function expandHome(
  p: string,
  host?: string,
  verbose?: boolean,
): Promise<string> {
  if (!p || p[0] !== "~") return p;
  if (p === "~") {
    const base = host ? await resolveRemoteHome(host, verbose) : homedir();
    return base;
  }
  if (p.startsWith("~/")) {
    const base = host ? await resolveRemoteHome(host, verbose) : homedir();
    return join(base, p.slice(2));
  }
  // "~user" not handled intentionally
  return p;
}

export async function ensureRemoteParentDir(
  host: string,
  path: string,
  verbose?: boolean,
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
  await ssh(host, args, verbose, cmd);
}

export async function sshDeleteDirectory({
  host,
  path,
  verbose,
}: {
  host: string;
  path: string;
  verbose?: boolean;
}) {
  path = await expandHome(path, host, verbose);
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
  await ssh(host, args, verbose, cmd);
}
