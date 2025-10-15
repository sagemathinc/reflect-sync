import { parentPort } from "node:worker_threads";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import fs from "node:fs/promises";

const HASH_STREAM_CUTOFF = 10_000_000;

async function sha256(path: string, size: number): Promise<string> {
  const h = createHash("sha256");
  if (size <= HASH_STREAM_CUTOFF) {
    const buf = await fs.readFile(path);
    h.update(buf);
  } else {
    const rs = createReadStream(path, { highWaterMark: 8 * 1024 * 1024 });
    await pipeline(rs, async function* (src) {
      for await (const chunk of src) h.update(chunk);
    });
  }
  return h.digest("hex");
}

type Job = { path: string; size: number; ctime: number; mtime: number };
type JobBatch = { jobs: Job[] };

if (!parentPort) throw new Error("worker only");

parentPort.on("message", async (payload: JobBatch) => {
  const jobs = payload.jobs || [];
  const out: Array<
    | { path: string; hash: string; ctime: number }
    | { path: string; error: string }
  > = [];

  for (const j of jobs) {
    try {
      const hash = await sha256(j.path, j.size);
      out.push({ path: j.path, hash, ctime: j.ctime });
    } catch (e: any) {
      out.push({ path: j.path, error: e?.message || String(e) });
    }
  }

  parentPort!.postMessage({ done: out });
});
