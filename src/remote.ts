// --- Remote HOME resolver + path expansion (with cache) ---
// remote.ts

import { homedir } from "node:os";
import { join, posix } from "node:path";
import { spawn } from "node:child_process";

// single-quote safe escape for sh -lc
function shellEscape(s: string): string {
  return `'${String(s).replace(/'/g, `'\\''`)}'`;
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
  const args = ["-C", "-T", "-o", "BatchMode=yes", host, "sh", "-lc", cmd];

  if (verbose) console.log("$ ssh", args.join(" "));

  const p = spawn("ssh", args, { stdio: ["ignore", "pipe", "inherit"] });

  let out = "";
  p.stdout!.on("data", (c) => (out += c));

  const code: number | null = await new Promise((res) =>
    p.on("exit", (c) => res(c)),
  );
  if (code !== 0)
    throw new Error(`Failed to resolve remote HOME on ${host} (exit ${code})`);

  const home = out.trim();
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
  absPath: string,
  verbose?: boolean,
): Promise<void> {
  const dirname = posix.dirname(absPath);
  const cmd = `mkdir -p -- ${shellEscape(dirname)}`;
  const args = ["-C", "-T", "-o", "BatchMode=yes", host, "sh", "-lc", cmd];
  if (verbose) console.log("$ ssh", args.join(" "));
  await new Promise<void>((resolve, reject) => {
    const p = spawn("ssh", args, { stdio: ["ignore", "ignore", "inherit"] });
    p.on("exit", (c) =>
      c === 0
        ? resolve()
        : reject(new Error(`mkdir -p failed on ${host} (exit ${c})`)),
    );
    p.on("error", reject);
  });
}
