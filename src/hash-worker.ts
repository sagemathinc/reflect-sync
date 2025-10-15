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

const updateHashStmt = `
  UPDATE files SET hash=?, deleted=0, size=?, ctime=?, mtime=? WHERE path=?
`;

import Database from "better-sqlite3"; // optional: write hashes from worker
// If you prefer all DB writes on main thread, postMessage(...) and let main flush.
// If you want to avoid one big results array, you can have workers write directly:
const db = new Database("alpha.db", { readonly: false });

if (!parentPort) throw new Error("worker only");

parentPort.on("message", async ({ path, size, ctime, mtime }) => {
  try {
    const hash = await sha256(path, size);
    // Option A: send to main to batch (what you have today)
    parentPort!.postMessage({ path, size, ctime, mtime, hash });

    // Option B (optional): write directly, no big results buffer:
    // db.prepare(updateHashStmt).run(hash, size, ctime, mtime, path);
  } catch (err: any) {
    parentPort!.postMessage({ path, error: err.message || String(err) });
  }
});
