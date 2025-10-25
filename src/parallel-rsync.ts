/*
Copy a list of files using rsync in parallel, which can
be ~ 2x-4x faster than just using a single rsync in some cases.
*/

import { promises as fsp } from "node:fs";
import { spawn } from "node:child_process";
import * as os from "node:os";
import * as path from "node:path";

type CopyItem = { rpath: string; size: number };

function chooseLaneCount(requested?: number): number {
  if (requested && requested > 0) return requested;
  const cores = os.cpus()?.length ?? 4;
  return Math.min(Math.max(2, Math.floor(cores / 2)), 8);
}

function shardBySize(items: CopyItem[], lanes: number): CopyItem[][] {
  // Largest-first greedy bin pack
  const sorted = [...items].sort((a, b) => b.size - a.size);
  const buckets: { total: number; items: CopyItem[] }[] = Array.from(
    { length: lanes },
    () => ({ total: 0, items: [] }),
  );
  for (const it of sorted) {
    buckets.sort((a, b) => a.total - b.total); // cheapest bucket first
    buckets[0].items.push(it);
    buckets[0].total += Math.max(1, it.size); // symlinks/dirs weight >= 1
  }
  return buckets.map((b) => b.items);
}

async function writeFrom0List(
  tmpDir: string,
  items: CopyItem[],
): Promise<string> {
  const p = path.join(
    tmpDir,
    `rsync-list-${Math.random().toString(36).slice(2)}.list`,
  );
  // Null-delimited, relative paths
  const buf = Buffer.concat(items.map((i) => Buffer.from(i.rpath + "\0")));
  await fsp.writeFile(p, buf);
  return p;
}

function runRsync(
  srcRoot: string,
  dstRoot: string,
  filesFrom: string,
  baseArgs: string[],
  env?: NodeJS.ProcessEnv,
): Promise<void> {
  // Ensure --from0 and --files-from are included
  const args = [
    ...baseArgs,
    "--from0",
    `--files-from=${filesFrom}`,
    srcRoot.endsWith(path.sep) ? srcRoot : srcRoot + path.sep,
    dstRoot,
  ];
  return new Promise((resolve, reject) => {
    const child = spawn("rsync", args, { stdio: "inherit", env });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0 || code === 23 || code === 24) resolve();
      else reject(new Error(`rsync exit ${code}`));
    });
  });
}

/**
 * Parallel copy phase. DO NOT pass --delete in baseArgs; do deletes in a serial phase.
 */
export async function parallelCopyPhase(
  toCopy: CopyItem[],
  srcRoot: string,
  dstRoot: string,
  baseArgs: string[],
  lanesRequested?: number,
  sshControlPath?: string, // optional: enable ssh multiplexing
): Promise<void> {
  if (toCopy.length === 0) return;
  const lanes = chooseLaneCount(lanesRequested);
  if (lanes === 1 || toCopy.length < 128) {
    // Small list? single rsync is often faster.
    const list = await writeFrom0List(os.tmpdir(), toCopy);
    await runRsync(srcRoot, dstRoot, list, baseArgs, process.env);
    await fsp.rm(list).catch(() => {});
    return;
  }

  // Prepare environment with SSH multiplexing (optional)
  let env = process.env;
  if (sshControlPath) {
    env = {
      ...process.env,
      RSYNC_RSH: `ssh -S ${sshControlPath} -o ControlMaster=auto -o ControlPersist=60s`,
    };
    // You can pre-open the control master here if desired:
    // spawn("ssh", ["-MNf", "-S", sshControlPath, "<user@host>"]);
  }

  const shards = shardBySize(toCopy, lanes);
  const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "ccsync-rsync-"));
  try {
    const lists = await Promise.all(shards.map((s) => writeFrom0List(tmp, s)));
    // Launch lanes with a modest cap so we don't melt disks; cap == lanes is fine on SSD/NVMe
    const promises = lists.map((list) =>
      runRsync(srcRoot, dstRoot, list, baseArgs, env),
    );
    // Fail-fast if any lane errors
    await Promise.all(promises);
  } finally {
    await fsp.rm(tmp, { recursive: true, force: true }).catch(() => {});
  }
}
