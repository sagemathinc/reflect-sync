import { spawn } from "node:child_process";
import fsp from "node:fs/promises";
import path from "node:path";

const DIST = path.resolve(__dirname, "../../dist");

export function runDist(
  scriptRel: string,
  args: string[] = [],
  envExtra: Record<string, string> = {},
): Promise<void> {
  return new Promise((resolve, reject) => {
    const script = path.join(DIST, scriptRel);
    const p = spawn(process.execPath, [script, ...args], {
      stdio: "inherit",
      env: { ...process.env, ...envExtra },
    });
    p.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error(`${scriptRel} exited ${code}`)),
    );
    p.on("error", reject);
  });
}

export async function fileExists(p: string) {
  try {
    await fsp.stat(p);
    return true;
  } catch {
    return false;
  }
}
