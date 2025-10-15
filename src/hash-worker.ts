import { parentPort } from "node:worker_threads";
import { createReadStream } from "node:fs";
import { createHash } from "node:crypto";
import { pipeline } from "node:stream/promises";
import fs from "node:fs/promises";

const HASH_STREAM_CUTOFF = 10_000_000;

async function hashFile(filename: string, size: number): Promise<string> {
  const hash = createHash("sha256");
  if (size <= HASH_STREAM_CUTOFF) {
    const buf = await fs.readFile(filename);
    hash.update(buf);
  } else {
    const stream = createReadStream(filename, {
      highWaterMark: 8 * 1024 * 1024,
    });
    await pipeline(stream, async function* (source) {
      for await (const chunk of source) hash.update(chunk);
    });
  }
  return hash.digest("hex");
}

if (!parentPort) throw new Error("Must be run as a worker");

parentPort.on("message", async ({ path, size, ctime, mtime }) => {
  try {
    const hash = await hashFile(path, size);
    parentPort!.postMessage({ path, size, ctime, mtime, hash });
  } catch (err) {
    parentPort!.postMessage({ path, error: (err as Error).message });
  }
});
