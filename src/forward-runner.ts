import { spawn } from "node:child_process";
import { type ForwardRow } from "./session-db.js";

export function spawnForwardMonitor(
  sessionDb: string,
  forward: ForwardRow,
): number {
  const args: string[] = [
    process.argv[1] ?? "",
    "forward-monitor",
    "--session-db",
    sessionDb,
    "--id",
    String(forward.id),
  ];
  const filteredArgs = args.filter((arg) => arg !== "");
  const child = spawn(process.execPath, filteredArgs, {
    stdio: "ignore",
    detached: true,
    env: process.env,
  });
  child.unref();
  return child.pid ?? 0;
}
