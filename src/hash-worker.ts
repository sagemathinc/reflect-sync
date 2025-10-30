// src/hash-worker.ts
import { parentPort, workerData } from "node:worker_threads";
import { stat as fsStat } from "node:fs/promises";
import { modeHash, fileDigest } from "./hash.js";

type Job = {
  path: string;
  size: number;
  ctime: number;
  mtime: number;
  /**
   * If true, append "|uid:gid" so ownership changes force re-hash equality changes.
   * If omitted, falls back to the batch default, then to the worker default (false).
   */
  numericIds?: boolean;
};

type JobBatch = {
  jobs: Job[];
  numericIds?: boolean;
};

type Out =
  | { path: string; hash: string; ctime: number; mtime: number }
  | { path: string; error: string };

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
      // Content digest (fast path for small files handled inside fileDigest)
      const content = await fileDigest(hashAlgorithm, j.path, j.size);

      // Mode/uid/gid snapshot (to fold into the final hash string)
      const st = await fsStat(j.path);
      let h = `${content}|${modeHash(st.mode)}`;
      if (numericIds) {
        h += `|${st.uid}:${st.gid}`;
      }

      out.push({ path: j.path, hash: h, ctime: j.ctime, mtime: j.mtime });
    } catch (e: any) {
      // Common case: file vanished mid-hash or permission error
      out.push({ path: j.path, error: e?.message || String(e) });
    }
  }

  parentPort!.postMessage({ done: out });
});
