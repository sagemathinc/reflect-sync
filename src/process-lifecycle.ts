import { setTimeout as sleep } from "node:timers/promises";
import { readFileSync } from "node:fs";

export function isProcessAlive(pid: number | null | undefined): boolean {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    if (process.platform === "linux") {
      const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
      if (stat.slice(stat.lastIndexOf(")") + 2).startsWith("Z ")) return false;
    }
    return true;
  } catch (err) {
    if (["ESRCH", "ENOENT"].includes((err as NodeJS.ErrnoException).code ?? ""))
      return false;
    throw err;
  }
}

export async function stopProcess(
  pid: number | null | undefined,
): Promise<void> {
  if (!pid || !isProcessAlive(pid)) return;
  try {
    process.kill(pid, "SIGTERM");
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ESRCH") throw err;
  }
  const deadline = Date.now() + 10000;
  while (isProcessAlive(pid)) {
    if (Date.now() >= deadline)
      throw Error(`Process ${pid} has not stopped; configuration retained`);
    await sleep(50);
  }
}
