import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import lockfile from "proper-lockfile";

// Serialize admission/removal across independent kernelspec launcher processes.
// Heartbeat-backed locks recover after process death without trusting reused PIDs.
export async function withJupyterTargetLock<T>(
  root: string,
  name: string,
  f: () => Promise<T>,
): Promise<T> {
  const dir = join(root, "locks");
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const release = await lockfile.lock(join(dir, name), {
    realpath: false,
    stale: 30000,
    update: 5000,
    retries: { retries: 60, minTimeout: 500, maxTimeout: 1000 },
  });
  try {
    return await f();
  } finally {
    await release();
  }
}
