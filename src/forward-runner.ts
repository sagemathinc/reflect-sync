import { spawn } from "node:child_process";
import { type ForwardRow } from "./session-db.js";

export function spawnForwardMonitor(
  sessionDb: string,
  forward: ForwardRow,
): number {
  const args: string[] = process.env.REFLECT_BUNDLED ? [] : [process.argv[1]];
  args.push(
    "forward-monitor",
    "--session-db",
    sessionDb,
    "--id",
    String(forward.id),
  );
  const child = spawn(process.execPath, args, {
    stdio: "ignore",
    detached: true,
    env: process.env,
  });
  child.unref();
  return child.pid ?? 0;
}
