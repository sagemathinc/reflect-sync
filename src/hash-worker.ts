import { parentPort } from "node:worker_threads";
import { createReadStream, lstatSync } from "node:fs";
import { createHash } from "node:crypto";
import { pipeline } from "node:stream/promises";
import { readFile } from "node:fs/promises";

const HASH_STREAM_CUTOFF = 10_000_000;

async function hashFile(filename: string, size: number): Promise<string> {
  const hash = createHash("sha256");
  if (size <= HASH_STREAM_CUTOFF) {
    const buf = await readFile(filename);
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

parentPort.on("message", async ({ path }: { path: string }) => {
  try {
    const stat = lstatSync(path);
    let hash;
    if (stat.isSymbolicLink()) {
      hash = "";
    } else {
      hash = await hashFile(path, stat.size);
    }
    parentPort!.postMessage({
      path,
      size: stat.size,
      ctime: stat.ctimeMs,
      mtime: stat.mtimeMs,
      hash,
    });
  } catch (err) {
    parentPort!.postMessage({ path, error: (err as Error).message });
  }
});
