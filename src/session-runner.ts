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
    if (row.alpha_remote_db) {
      args.push("--alpha-remote-db", row.alpha_remote_db);
    }
  }
  if (row.beta_host) {
    args.push("--beta-host", row.beta_host);
    if (row.beta_port != null) {
      args.push("--beta-port", String(row.beta_port));
    }
    if (row.beta_remote_db) {
      args.push("--beta-remote-db", row.beta_remote_db);
    }
  }

  if (row.disable_hot_sync) {
    args.push("--disable-hot-sync");
  }
  if (row.enable_reflink) {
    args.push("--enable-reflink");
  }
  if (row.disable_full_cycle) {
    args.push("--disable-full-cycle");
  }

  args.push("--session-id", String(row.id));
  args.push("--session-db", sessionDb);

  logger?.info(`start session: '${process.execPath} ${argsJoin(args)}'`);
  const child = spawn(launcher.command, args, {
    stdio: "ignore",
    detached: true,
    env: process.env,
  });
  child.unref();

  if (logger) {
    const earlyMs = Number(process.env.REFLECT_SCHEDULER_EARLY_EXIT_MS ?? 3000);
    if (Number.isFinite(earlyMs) && earlyMs > 0) {
      let timer: NodeJS.Timeout | null = null;
      const onEarlyExit = (
        code: number | null,
        signal: NodeJS.Signals | null,
      ) => {
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
        logger.warn("scheduler exited shortly after launch", {
          session: row.id,
          pid: child.pid,
          code,
          signal,
        });
      };
      timer = setTimeout(() => {
        child.removeListener("exit", onEarlyExit);
        timer = null;
      }, earlyMs);
      if (timer.unref) timer.unref();
      child.once("exit", onEarlyExit);
    }
  }

  return child.pid ?? 0;
}
