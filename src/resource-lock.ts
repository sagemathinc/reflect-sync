import { mkdir } from "node:fs/promises";
import { dirname, resolve, join } from "node:path";
import { createHash } from "node:crypto";
import lockfile from "proper-lockfile";

export async function withResourceLock<T>(
  db: string,
  domain: "sync" | "forward",
  id: number,
  operation: () => Promise<T>,
): Promise<T> {
  const dir = join(
    dirname(db),
    ".reflect-locks",
    createHash("sha256").update(resolve(db)).digest("hex"),
  );
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const release = await lockfile.lock(join(dir, `${domain}-${id}`), {
    realpath: false,
    stale: 30000,
    update: 5000,
    retries: { retries: 60, minTimeout: 100, maxTimeout: 500 },
  });
  try {
    return await operation();
  } finally {
    await release();
  }
}
