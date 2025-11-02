import fs from "node:fs";
import { spawn } from "node:child_process";
import {
  deriveSessionPaths,
  getReflectSyncHome,
  type SessionRow,
} from "./session-db.js";
import { deserializeIgnoreRules } from "./ignore.js";

export function spawnSchedulerForSession(
  sessionDb: string,
  row: SessionRow,
): number {
  const home = getReflectSyncHome();
  const sessionPaths = deriveSessionPaths(row.id, home);
  fs.mkdirSync(sessionPaths.dir, { recursive: true });

  const alphaDb: string = row.alpha_db ?? sessionPaths.alpha_db;
  const betaDb: string = row.beta_db ?? sessionPaths.beta_db;
  const baseDb: string = row.base_db ?? sessionPaths.base_db;
  const hashAlg: string = row.hash_alg ?? "sha256";

  const args: string[] = process.env.REFLECT_BUNDLED ? [] : [process.argv[1]];
  args.push(
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
  );

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

  args.push("--session-id", String(row.id));
  args.push("--session-db", sessionDb);

  // Debug output suppressed to avoid polluting CLI stdout contracts.
  // console.log(`${process.execPath} ${argsJoin(args)}`);
  const child = spawn(process.execPath, args, {
    stdio: "ignore",
    detached: true,
    env: process.env,
  });
  child.unref();
  return child.pid ?? 0;
}
