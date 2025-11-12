// src/hash-worker.ts
import { parentPort, workerData } from "node:worker_threads";
import { stat as fsStat } from "node:fs/promises";
import { modeHash, fileDigest } from "./hash.js";

type Job = {
  path: string;
  size: number;
  ctime: number;
  mtime: number;
};

type JobBatch = {
  jobs: Job[];
  /**
   * If true, append "|uid:gid" so ownership changes force re-hash equality changes.
   * If omitted, falls back to the batch default, then to the worker default (false).
   */
  numericIds?: boolean;
};

type Out =
  | {
      path: string;
      hash: string;
      ctime: number;
      mtime: number;
      size: number;
    }
  | { path: string; error: string }
  | { path: string; skipped: true };

if (!parentPort) {
  throw new Error("hash-worker must be run as a worker");
}

// ---- Worker state (mutable via config messages) ----
const hashAlgorithm = workerData.alg; // e.g., "sha256"

// ---- Message handler ----
parentPort.on("message", async (payload: JobBatch) => {
  const { jobs = [], numericIds } = payload as JobBatch;
  const out: Out[] = [];

  for (const j of jobs) {
    try {
      const before = await fsStat(j.path);
      const beforeMtime = (before as any).mtimeMs ?? before.mtime.getTime();
      const beforeCtime = (before as any).ctimeMs ?? before.ctime.getTime();
      if (
        before.size !== j.size ||
        Math.abs(beforeMtime - j.mtime) > 0.5 ||
        Math.abs(beforeCtime - j.ctime) > 0.5
      ) {
        out.push({ path: j.path, skipped: true });
        continue;
      }

      // Content digest (fast path for small files handled inside fileDigest)
      const content = await fileDigest(hashAlgorithm, j.path, before.size);

      // Mode/uid/gid snapshot (to fold into the final hash string)
      const after = await fsStat(j.path);
      const afterMtime = (after as any).mtimeMs ?? after.mtime.getTime();
      const afterCtime = (after as any).ctimeMs ?? after.ctime.getTime();
      if (
        after.size !== before.size ||
        Math.abs(afterMtime - beforeMtime) > 0.5 ||
        Math.abs(afterCtime - beforeCtime) > 0.5
      ) {
        out.push({ path: j.path, skipped: true });
        continue;
      }

      let h = `${content}|${modeHash(after.mode)}`;
      if (numericIds) {
        h += `|${after.uid}:${after.gid}`;
      }

      out.push({
        path: j.path,
        hash: h,
        ctime: afterCtime,
        mtime: afterMtime,
        size: after.size,
      });
    } catch (e: any) {
      // Common case: file vanished mid-hash or permission error
      out.push({ path: j.path, error: e?.message || String(e) });
    }
  }

  parentPort!.postMessage({ done: out });
});
