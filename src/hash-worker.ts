// src/hash-worker.ts
import { parentPort } from "node:worker_threads";
import { createReadStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import fs from "node:fs/promises";
import { xxh3 } from "@node-rs/xxhash";

const HASH_STREAM_CUTOFF = 10_000_000; // ~10MB; small files do one-shot hashing
const STREAM_HWM = 8 * 1024 * 1024; // 8MB read chunks

// Convert unsigned 128-bit BigInt to 32-char hex (lowercase)
const TWO128 = 1n << 128n;
function hex128(x: bigint): string {
  if (x < 0n) x += TWO128; // normalize (defensive)
  return x.toString(16).padStart(32, "0");
}

export async function xxh128File(path: string, size: number): Promise<string> {
  // Fast path for small files
  if (size <= HASH_STREAM_CUTOFF) {
    const buf = await fs.readFile(path);
    const dig = xxh3.xxh128(buf); // BigInt
    return hex128(dig);
  }

  // Streaming path for large files
  const hasher = xxh3.Xxh3.withSeed(); // default seed=0
  const rs = createReadStream(path, { highWaterMark: STREAM_HWM });

  // Use pipeline to get proper backpressure/error propagation
  await pipeline(rs, async function* (src) {
    for await (const chunk of src) {
      hasher.update(chunk);
      // we must yield something to satisfy the generator signature
      // but we don't actually need to pass data downstream
    }
  });

  const dig = hasher.digest(); // BigInt
  return hex128(dig);
}

type Job = { path: string; size: number; ctime: number; mtime: number };
type JobBatch = { jobs: Job[] };

if (!parentPort) {
  console.warn("WARNING: hash-worker must be run as a worker");
}

parentPort?.on("message", async (payload: JobBatch) => {
  const jobs = payload?.jobs ?? [];
  const out: Array<
    | { path: string; hash: string; ctime: number }
    | { path: string; error: string }
  > = [];

  for (const j of jobs) {
    try {
      const hash = await xxh128File(j.path, j.size);
      out.push({ path: j.path, hash, ctime: j.ctime });
    } catch (e: any) {
      // Most common issue: file vanished mid-hash; surface as an error entry
      out.push({ path: j.path, error: e?.message || String(e) });
    }
  }

  parentPort!.postMessage({ done: out });
});
