import { ChildProcess, spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fsp from "node:fs/promises";
import { createReadStream } from "node:fs";
import { dirname, join, normalize, resolve } from "node:path";

// Resolve scheduler entrypoint directly to avoid CLI multi-proc trees
export const SCHED = resolve(__dirname, "../../dist/scheduler.js");

export const SSH_ENABLED =
  (() => {
    try {
      const res = spawnSync("ssh", [
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=2",
        "localhost",
        "true",
      ]);
      return res.status === 0;
    } catch {
      return false;
    }
  })() === true;

export const describeIfSsh: typeof describe = SSH_ENABLED
  ? describe
  : describe.skip;

type SchedulerOpts = {
  alphaRoot: string;
  betaRootRemote: string;
  alphaDb: string;
  betaDb: string;
  baseDb: string;
  prefer?: "alpha" | "beta";
};

export function startSchedulerRemote(opts: SchedulerOpts): ChildProcess {
  const betaRemoteDb = join(
    opts.betaRootRemote,
    "..",
    "beta.remote.db",
  );
  const args = [
    SCHED,
    "--alpha-root",
    opts.alphaRoot,
    "--beta-root",
    opts.betaRootRemote,
    "--alpha-db",
    opts.alphaDb,
    "--beta-db",
    opts.betaDb,
    "--beta-remote-db",
    betaRemoteDb,
    "--base-db",
    opts.baseDb,
    "--prefer",
    opts.prefer ?? "alpha",
    "--disable-hot-watch",
    "--beta-host",
    "localhost",
  ];
  const env = {
    ...process.env,
    SCHED_MIN_MS: "100",
    SCHED_MAX_MS: "200",
    SCHED_MAX_BACKOFF_MS: "50",
    SCHED_JITTER_MS: "0",
    HOT_DEBOUNCE_MS: "0",
    COOLDOWN_MS: "10",
    SHALLOW_DEPTH: "1",
    HOT_DEPTH: "1",
    MAX_HOT_WATCHERS: "32",
  };

  return spawn(process.execPath, args, {
    stdio: ["ignore", "ignore", "inherit"],
    env,
  });
}

export async function stopScheduler(p: ChildProcess) {
  if (!p.pid) return;
  try {
    process.kill(p.pid, "SIGINT");
  } catch {}
  await Promise.race([
    new Promise<void>((resolve) => p.once("exit", () => resolve())),
    new Promise<void>((resolve) => setTimeout(resolve, 500)),
  ]);
  if (p.pid) {
    try {
      process.kill(p.pid, "SIGKILL");
    } catch {}
  }
}

export const wait = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));

export async function writeLongPack(
  file: string,
  {
    iterations,
    chunkSize,
    delayMs,
  }: { iterations: number; chunkSize: number; delayMs: number },
) {
  await fsp.mkdir(normalize(dirname(file)), { recursive: true });
  const handle = await fsp.open(file, "w");
  try {
    for (let i = 0; i < iterations; i++) {
      const chunk = Buffer.alloc(chunkSize, i % 251);
      await handle.write(chunk);
      if (delayMs > 0) {
        await wait(delayMs);
      }
    }
  } finally {
    await handle.close();
  }
}

export async function hashFile(file: string): Promise<string> {
  const hash = createHash("sha256");
  await new Promise<void>((resolve, reject) => {
    const rs = createReadStream(file);
    rs.on("data", (chunk) => hash.update(chunk));
    rs.on("error", reject);
    rs.on("end", resolve);
  });
  return hash.digest("hex");
}

export async function listDatasetFiles(root: string): Promise<string[]> {
  try {
    const entries = await fsp.readdir(root, { withFileTypes: true });
    const out: string[] = [];
    for (const entry of entries) {
      const full = join(root, entry.name);
      if (entry.isDirectory()) {
        const nested = await listDatasetFiles(full);
        for (const child of nested) {
          out.push(join(entry.name, child));
        }
      } else if (entry.isFile()) {
        out.push(entry.name);
      }
    }
    return out.sort();
  } catch {
    return [];
  }
}

export async function hashDirectory(root: string): Promise<string> {
  const hash = createHash("sha256");
  const entries = await fsp
    .readdir(root, { withFileTypes: true })
    .catch(() => []);
  const sorted = entries.map((e) => e.name).sort();
  for (const name of sorted) {
    const entry = entries.find((e: any) => e.name === name)!;
    const full = join(root, entry.name);
    if (entry.isDirectory()) {
      const subHash = await hashDirectory(full);
      hash.update(entry.name);
      hash.update(subHash);
    } else if (entry.isFile()) {
      const data = await fsp.readFile(full);
      hash.update(entry.name);
      hash.update(data);
    }
  }
  return hash.digest("hex");
}
