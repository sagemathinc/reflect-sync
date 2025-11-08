import fs from "node:fs";
import { spawn } from "node:child_process";
import {
  deriveSessionPaths,
  getReflectSyncHome,
  type SessionRow,
} from "./session-db.js";
import { deserializeIgnoreRules } from "./ignore.js";
import { resolveSelfLaunch } from "./self-launch.js";
import { type Logger } from "./logger.js";
import { argsJoin } from "./remote.js";

export function spawnSchedulerForSession(
  sessionDb: string,
  row: SessionRow,
  logger?: Logger,
): number {
  const home = getReflectSyncHome();
  const sessionPaths = deriveSessionPaths(row.id, home);
  fs.mkdirSync(sessionPaths.dir, { recursive: true });

  const alphaDb: string = row.alpha_db ?? sessionPaths.alpha_db;
  const betaDb: string = row.beta_db ?? sessionPaths.beta_db;
  const baseDb: string = row.base_db ?? sessionPaths.base_db;
  const hashAlg: string = row.hash_alg ?? "sha256";

  const launcher = resolveSelfLaunch();
  const args: string[] = [
    ...launcher.args,
    "scheduler",
    "--alpha-root",
    row.alpha_root,
    "--beta-root",
    row.beta_root,
    "--alpha-db",
    alphaDb,
    "--beta-db",
    betaDb,
    "--base-db",
    baseDb,
    "--prefer",
    row.prefer,
    "--hash",
    hashAlg,
    "--compress",
    row.compress ?? "auto",
  ];

  const ignorePatterns = deserializeIgnoreRules(row.ignore_rules ?? null);
  for (const pattern of ignorePatterns) {
    args.push("--ignore", pattern);
  }

  if (row.alpha_host) {
    args.push("--alpha-host", row.alpha_host);
    if (row.alpha_port != null) {
      args.push("--alpha-port", String(row.alpha_port));
    }
  }
  if (row.beta_host) {
    args.push("--beta-host", row.beta_host);
    if (row.beta_port != null) {
      args.push("--beta-port", String(row.beta_port));
    }
  }

  if (row.disable_micro_sync) {
    args.push("--disable-micro-sync");
  }
  if (row.disable_full_cycle) {
    args.push("--disable-full-cycle");
  }

  args.push("--session-id", String(row.id));
  args.push("--session-db", sessionDb);

  // Debug output suppressed to avoid polluting CLI stdout contracts.
  logger?.debug(`start session: '${process.execPath} ${argsJoin(args)}'`);
  const child = spawn(launcher.command, args, {
    stdio: "ignore",
    detached: true,
    env: process.env,
  });
  child.unref();
  return child.pid ?? 0;
}
