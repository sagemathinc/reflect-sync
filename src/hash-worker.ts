// src/hash-worker.ts
import { parentPort } from "node:worker_threads";
import { modeHash, xxh128File } from "./hash.js";
import { stat } from "node:fs/promises";

type Job = { path: string; size: number; ctime: number; mtime: number };
type JobBatch = { jobs: Job[] };

if (!parentPort) {
  throw Error("hash-worker must be run as a worker");
}

parentPort?.on("message", async (payload: JobBatch) => {
  const isRoot = process.geteuid?.() === 0;
  const jobs = payload?.jobs ?? [];
  const out: Array<
    | { path: string; hash: string; ctime: number }
    | { path: string; error: string }
  > = [];

  for (const j of jobs) {
    try {
      const hashContents = await xxh128File(j.path, j.size);
      const st = await stat(j.path);
      let hash = `${hashContents}|${modeHash(st.mode)}`;
      if (isRoot) {
        // so that uid and gid changes cause an update:
        hash += `|${st.uid}:${st.gid}`;
      }
      out.push({ path: j.path, hash, ctime: j.ctime });
    } catch (e: any) {
      // Most common issue: file vanished mid-hash; surface as an error entry
      out.push({ path: j.path, error: e?.message || String(e) });
    }
  }

  parentPort!.postMessage({ done: out });
});
