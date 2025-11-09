import {
  loadSessionById,
  loadSessionByName,
  createSession,
  deleteSessionById,
  deriveSessionPaths,
  ensureSessionDb,
  getOrCreateEngineId,
  getReflectSyncHome,
  materializeSessionPaths,
  updateSession,
  clearSessionRuntime,
  upsertLabels,
  setActualState,
  setDesiredState,
  recordHeartbeat,
  type SessionPatch,
} from "./session-db.js";
import { ensureRemoteParentDir, sshDeleteDirectory } from "./remote.js";
import path from "node:path";
import os from "node:os";
import { CLI_NAME } from "./constants.js";
import { rm } from "node:fs/promises";
import type { Logger } from "./logger.js";
import {
  deserializeIgnoreRules,
  normalizeIgnorePatterns,
  serializeIgnoreRules,
} from "./ignore.js";
import { spawnSchedulerForSession } from "./session-runner.js";

// Attempt to stop a scheduler by PID
export function stopPid(pid: number): boolean {
  if (!pid) return false;
  try {
    process.kill(pid, "SIGTERM");
    return true;
  } catch {
    return false;
  }
}

export async function terminateSession({
  sessionDb,
  id,
  logger,
  force,
}: {
  sessionDb: string;
  id: number;
  logger?: Logger;
  force?: boolean;
}) {
  const row = loadSessionById(sessionDb, id);
  if (!row) {
    console.error(`session ${id} not found`);
    return;
  }
  logger?.info("terminating session", { sessionId: id });
  if (row.scheduler_pid) {
    stopPid(row.scheduler_pid);
  }
  const host = row.alpha_host ?? row.beta_host;
  const port =
    row.alpha_host != null
      ? (row.alpha_port ?? undefined)
      : (row.beta_port ?? undefined);
  if (host) {
    const remotePath = row.alpha_remote_db || row.beta_remote_db;
    if (remotePath) {
      const dir = path.dirname(remotePath);
      // includes is just a sanity check...
      if (dir.includes(CLI_NAME)) {
        // delete it over ssh
        try {
          await sshDeleteDirectory({
            host,
            path: dir,
            logger,
            port,
          });
        } catch (err) {
          if (!force) {
            console.error(
              `session ${id} -- unable to delete ${host}:${remotePath} -- you could use --force and manually delete`,
              err,
            );
            return;
          } else {
            // non-fatal
            console.warn(
              `session ${id} -- failed to delete ${host}:${remotePath}`,
              err,
            );
          }
        }
      }
    }
  }

  const { dir } = deriveSessionPaths(id, getReflectSyncHome());
  try {
    await rm(dir, { recursive: true, force: true });
  } catch (err) {
    if (force) {
      console.warn(
        `session ${id} -- failed to delete ${dir} (you should manually clean up)`,
        err,
      );
    } else {
      console.error(
        `session ${id} -- failed to delete ${dir} (consider using --force)`,
        err,
      );
      return;
    }
  }
  deleteSessionById(sessionDb, id);
}

export async function resetSession({
  sessionDb,
  id,
  logger,
}: {
  sessionDb: string;
  id: number;
  logger?: Logger;
}) {
  const row = loadSessionById(sessionDb, id);
  if (!row) {
    throw new Error(`session ${id} not found`);
  }

  logger?.info("resetting session", { sessionId: id });
  if (row.scheduler_pid) {
    stopPid(row.scheduler_pid);
  }

  const remoteDirs = new Map<
    string,
    { host: string; port?: number; dirs: Set<string> }
  >();
  const remoteDbPaths = new Map<
    string,
    { host: string; port?: number; paths: Set<string> }
  >();
  const remoteKey = (host: string, port?: number) =>
    port != null ? `${host}:${port}` : host;
  const addRemote = (
    host?: string | null,
    port?: number | null,
    remotePath?: string | null,
  ) => {
    if (!host || !remotePath) return;
    const dir = path.dirname(remotePath);
    const key = remoteKey(host, port ?? undefined);
    if (!remoteDirs.has(key)) {
      remoteDirs.set(key, { host, port: port ?? undefined, dirs: new Set() });
    }
    remoteDirs.get(key)!.dirs.add(dir);
    if (!remoteDbPaths.has(key)) {
      remoteDbPaths.set(key, {
        host,
        port: port ?? undefined,
        paths: new Set(),
      });
    }
    remoteDbPaths.get(key)!.paths.add(remotePath);
  };

  addRemote(row.alpha_host, row.alpha_port, row.alpha_remote_db);
  addRemote(row.beta_host, row.beta_port, row.beta_remote_db);

  for (const { host, port, dirs } of remoteDirs.values()) {
    for (const dir of dirs) {
      if (!dir.includes(CLI_NAME)) {
        const msg = `skipping remote cleanup for ${host}:${dir} (outside ${CLI_NAME})`;
        if (logger) {
          logger.warn(msg, { sessionId: id });
        } else {
          console.warn(msg);
        }
        continue;
      }
      await sshDeleteDirectory({
        host,
        path: dir,
        logger,
        port,
      });
    }
  }

  const { dir } = deriveSessionPaths(id, getReflectSyncHome());
  await rm(dir, { recursive: true, force: true });

  const paths = materializeSessionPaths(id);
  updateSession(sessionDb, id, {
    ...paths,
    scheduler_pid: null,
    last_heartbeat: null,
    last_digest: null,
    alpha_digest: null,
    beta_digest: null,
    actual_state: "stopped",
    desired_state: "stopped",
  });

  clearSessionRuntime(sessionDb, id);

  for (const { host, port, paths } of remoteDbPaths.values()) {
    for (const path of paths) {
      await ensureRemoteParentDir({
        host,
        port,
        path,
        logger,
      });
    }
  }

  logger?.info("session reset complete", { sessionId: id });
}

type Endpoint = { root: string; host?: string; port?: number };

// Minimal mutagen-like endpoint parsing.
// - local: "~/work" or "/abs/path" -> {root: absolute path, host: undefined}
// - remote: "user@host:~/work" or "host:/abs" -> {root: as-given (keep ~ for remote), host}
// Windows drive-letter paths like "C:\x" won’t be mistaken as remote (colon rule).
export function parseEndpoint(spec: string): Endpoint {
  const trimmed = spec.trim();
  if (!trimmed) return { root: "~" };
  const looksLikeWindows = /^[A-Za-z]:/.test(trimmed);
  if (looksLikeWindows && trimmed.length >= 3) {
    const sep = trimmed[2];
    if (sep === "\\" || (sep === "/" && process.platform === "win32")) {
      return { root: trimmed };
    }
  }
  if (
    trimmed.startsWith("/") ||
    trimmed.startsWith("~/") ||
    trimmed === "~" ||
    trimmed.startsWith("./") ||
    trimmed.startsWith("../")
  ) {
    return { root: trimmed };
  }

  const match = /^(?<hostPart>[^:]+)(?::(?<portPart>[^:]*))?:(?<path>.*)$/.exec(
    trimmed,
  );
  if (!match) {
    return { root: trimmed };
  }

  const { hostPart, portPart, path } = match.groups as {
    hostPart: string;
    portPart?: string;
    path: string;
  };

  if (!path || (path[0] !== "/" && !path.startsWith("~/"))) {
    throw new Error(
      `Remote paths must start with '/' or '~/' (got '${path || ""}')`,
    );
  }

  let port: number | undefined;
  if (portPart !== undefined) {
    if (portPart === "") {
      port = 22;
    } else if (/^\d+$/.test(portPart)) {
      port = Number(portPart);
      if (!Number.isFinite(port) || port < 1 || port > 65535) {
        throw new Error(`Invalid SSH port '${portPart}'`);
      }
    } else {
      throw new Error(`Invalid SSH port '${portPart}'`);
    }
  }

  return { host: hostPart, port, root: path };
}

function canonicalizeLocalRoot(root: string): string {
  if (!root) return path.resolve(root);
  if (root.startsWith("~")) {
    root = path.join(os.homedir(), root.slice(1));
  }
  return path.resolve(root);
}

function parseLabelPairs(pairs: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const p of pairs || []) {
    const i = p.indexOf("=");
    if (i <= 0) throw new Error(`Invalid label "${p}", expected k=v`);
    const k = p.slice(0, i).trim();
    const v = p.slice(i + 1).trim();
    if (!k) throw new Error(`Invalid label key in "${p}"`);
    out[k] = v;
  }
  return out;
}

function combineCompress(
  current: string | null,
  next?: string,
  level?: string,
): string {
  let base = next ?? current ?? "auto";
  base = base.trim() || "auto";
  let explicitLevel: string | undefined;

  const applyLevel = (lvl?: string) => {
    if (lvl === undefined) return;
    const trimmed = lvl.trim();
    if (!trimmed) {
      explicitLevel = undefined;
    } else {
      explicitLevel = trimmed;
    }
  };

  const split = (value: string) => {
    const idx = value.indexOf(":");
    if (idx >= 0) {
      const b = value.slice(0, idx);
      const lvl = value.slice(idx + 1);
      return { base: b, level: lvl };
    }
    return { base: value, level: undefined };
  };

  if (next && next.includes(":")) {
    const parts = split(next);
    base = parts.base;
    explicitLevel = parts.level;
  } else if (current && !next && current.includes(":")) {
    const parts = split(current);
    base = parts.base;
    explicitLevel = parts.level;
  }

  if (level !== undefined) {
    applyLevel(level);
  }

  return explicitLevel ? `${base}:${explicitLevel}` : base;
}

export async function newSession({
  alphaSpec,
  betaSpec,
  sessionDb,
  compress,
  compressLevel,
  prefer,
  hash,
  label,
  name,
  ignore,
  logger,
  disableHotSync = false,
  enableReflink = false,
  disableFullCycle = false,
}): Promise<number> {
  ensureSessionDb(sessionDb);

  const a = parseEndpoint(alphaSpec);
  const b = parseEndpoint(betaSpec);
  if (!a.host) {
    a.root = canonicalizeLocalRoot(a.root);
  }
  if (!b.host) {
    b.root = canonicalizeLocalRoot(b.root);
  }
  if (enableReflink && (a.host || b.host)) {
    throw new Error("reflink can only be enabled when both roots are local");
  }
  compress = `${compress}${compressLevel ? ":" + compressLevel : ""}`;
  let sessionName: string | undefined;
  if (typeof name === "string") {
    const trimmed = name.trim();
    if (trimmed) {
      if (/^\d+$/.test(trimmed)) {
        throw new Error(
          "Session name must include non-numeric characters (pure integers are reserved for IDs).",
        );
      }
      const existing = loadSessionByName(sessionDb, trimmed);
      if (existing) {
        throw new Error(
          `Session name '${trimmed}' is already in use (id=${existing.id}).`,
        );
      }
      sessionName = trimmed;
    }
  }

  let id = 0;
  try {
    id = createSession(
      sessionDb,
      {
        name: sessionName,
        alpha_root: a.root,
        beta_root: b.root,
        prefer: (prefer || "alpha").toLowerCase(),
        alpha_host: a.host ?? null,
        alpha_port: a.port ?? null,
        beta_host: b.host ?? null,
        beta_port: b.port ?? null,
        hash_alg: hash,
        ignore,
        compress,
        disable_hot_sync: !!disableHotSync,
        enable_reflink: !!enableReflink,
        disable_full_cycle: !!disableFullCycle,
      },
      parseLabelPairs(label || []),
    );

    const engineId = getOrCreateEngineId(getReflectSyncHome()); // persistent local origin id
    // [ ] TODO: should instead use a remote version of getReflectSyncHome here:
    const nsBase = `~/.local/share/${CLI_NAME}/by-origin/${engineId}/sessions/${id}`;

    let alphaRemoteDb: string | null = null;
    let betaRemoteDb: string | null = null;

    if (a.host) alphaRemoteDb = `${nsBase}/alpha.db`;
    if (b.host) betaRemoteDb = `${nsBase}/beta.db`;

    if (alphaRemoteDb || betaRemoteDb) {
      updateSession(sessionDb, id, {
        alpha_remote_db: alphaRemoteDb,
        beta_remote_db: betaRemoteDb,
      });

      // proactively create parent dirs on remotes
      if (a.host && alphaRemoteDb)
        await ensureRemoteParentDir({
          host: a.host,
          port: a.port ?? undefined,
          path: alphaRemoteDb,
          logger,
        });
      if (b.host && betaRemoteDb)
        await ensureRemoteParentDir({
          host: b.host,
          port: b.port ?? undefined,
          path: betaRemoteDb,
          logger,
        });
    }

    const paths = materializeSessionPaths(id);
    updateSession(sessionDb, id, paths);

    return id;
  } catch (err) {
    // clean up
    if (id) {
      try {
        await terminateSession({
          sessionDb,
          id,
          logger,
          force: true,
        });
      } catch {}
    }
    throw err;
  }
}

export interface SessionEditOptions {
  sessionDb: string;
  id: number;
  name?: string | null;
  compress?: string;
  compressLevel?: string;
  ignoreAdd?: string[];
  resetIgnore?: boolean;
  labels?: string[];
  hash?: string;
  alphaSpec?: string;
  betaSpec?: string;
  reset?: boolean;
  logger?: Logger;
  disableHotSync?: boolean;
  disableFullCycle?: boolean;
}

export async function editSession(options: SessionEditOptions) {
  const {
    sessionDb,
    id,
    name,
    compress,
    compressLevel,
    ignoreAdd,
    resetIgnore,
    labels,
    hash,
    alphaSpec,
    betaSpec,
    reset,
    logger,
    disableHotSync,
    disableFullCycle,
  } = options;

  ensureSessionDb(sessionDb);
  const row = loadSessionById(sessionDb, id);
  if (!row) {
    throw new Error(`session ${id} not found`);
  }

  const changes: string[] = [];
  const updates: SessionPatch = {};
  let needRestart = false;
  let resetRequested = !!reset;
  const wasRunning = !!row.scheduler_pid && row.actual_state === "running";

  // Name update
  if (name !== undefined) {
    const trimmed = name === null ? "" : String(name).trim();
    const nextName = trimmed || null;
    if (nextName !== row.name) {
      if (nextName && /^\d+$/.test(nextName)) {
        throw new Error("Session name must include non-numeric characters.");
      }
      if (nextName) {
        const existing = loadSessionByName(sessionDb, nextName);
        if (existing && existing.id !== id) {
          throw new Error(
            `Session name '${nextName}' is already in use (id=${existing.id}).`,
          );
        }
      }
      updates.name = nextName;
      changes.push(`name=${nextName ?? "-"}`);
    }
  }

  // Compress update
  if (compress !== undefined || compressLevel !== undefined) {
    const currentCompress = row.compress ?? "auto";
    const nextCompress = combineCompress(
      row.compress ?? null,
      compress,
      compressLevel,
    );
    if (nextCompress !== currentCompress) {
      updates.compress = nextCompress;
      changes.push(`compress=${nextCompress}`);
      needRestart = true;
    }
  }

  // Ignore rules
  const ignoreAdds = Array.isArray(ignoreAdd) ? ignoreAdd : [];
  if (resetIgnore || ignoreAdds.length) {
    let patterns = deserializeIgnoreRules(row.ignore_rules);
    if (resetIgnore) patterns = [];
    if (ignoreAdds.length) patterns.push(...ignoreAdds);
    const merged = normalizeIgnorePatterns(patterns);
    const nextBlob = serializeIgnoreRules(merged);
    if (nextBlob !== row.ignore_rules) {
      updates.ignore_rules = nextBlob;
      changes.push("ignore");
      needRestart = true;
    }
  }

  if (disableHotSync !== undefined) {
    const desired = !!disableHotSync;
    if (!!row.disable_hot_sync !== desired) {
      updates.disable_hot_sync = desired;
      changes.push(`hot-sync=${desired ? "disabled" : "enabled"}`);
      needRestart = true;
    }
  }

  if (disableFullCycle !== undefined) {
    const desired = !!disableFullCycle;
    if (!!row.disable_full_cycle !== desired) {
      updates.disable_full_cycle = desired;
      changes.push(`full-cycle=${desired ? "disabled" : "enabled"}`);
      needRestart = true;
    }
  }

  // Labels
  if (labels && labels.length) {
    upsertLabels(sessionDb, id, parseLabelPairs(labels));
    changes.push("labels");
  }

  // Hash updates require reset
  if (hash !== undefined) {
    if (!resetRequested) {
      throw new Error("--reset is required when changing the hash algorithm");
    }
    if (hash !== row.hash_alg) {
      updates.hash_alg = hash;
      changes.push(`hash=${hash}`);
    }
  }

  let alphaEndpoint: Endpoint = {
    host: row.alpha_host ?? undefined,
    port: row.alpha_port ?? undefined,
    root: row.alpha_root,
  };
  let betaEndpoint: Endpoint = {
    host: row.beta_host ?? undefined,
    port: row.beta_port ?? undefined,
    root: row.beta_root,
  };

  if (alphaSpec !== undefined) {
    if (!resetRequested) {
      throw new Error("--reset is required when changing the alpha endpoint");
    }
    alphaEndpoint = parseEndpoint(alphaSpec);
    changes.push("alpha");
  }
  if (betaSpec !== undefined) {
    if (!resetRequested) {
      throw new Error("--reset is required when changing the beta endpoint");
    }
    betaEndpoint = parseEndpoint(betaSpec);
    changes.push("beta");
  }

  if (alphaEndpoint.host && betaEndpoint.host) {
    throw new Error(
      "Both sides remote is not supported yet (rsync two-remote).",
    );
  }

  if (alphaSpec !== undefined) {
    updates.alpha_root = alphaEndpoint.root;
    updates.alpha_host = alphaEndpoint.host ?? null;
    updates.alpha_port = alphaEndpoint.port ?? null;
    needRestart = true;
  }
  if (betaSpec !== undefined) {
    updates.beta_root = betaEndpoint.root;
    updates.beta_host = betaEndpoint.host ?? null;
    updates.beta_port = betaEndpoint.port ?? null;
    needRestart = true;
  }

  let alphaRemoteDb = row.alpha_remote_db;
  let betaRemoteDb = row.beta_remote_db;
  const remoteEnsures: Array<Promise<void>> = [];

  if (resetRequested && (alphaSpec !== undefined || betaSpec !== undefined)) {
    const engineId = getOrCreateEngineId(getReflectSyncHome());
    const nsBase = `~/.local/share/${CLI_NAME}/by-origin/${engineId}/sessions/${id}`;
    if (alphaEndpoint.host) {
      alphaRemoteDb = `${nsBase}/alpha.db`;
      remoteEnsures.push(
        ensureRemoteParentDir({
          host: alphaEndpoint.host,
          port: alphaEndpoint.port ?? undefined,
          path: alphaRemoteDb,
          logger,
        }),
      );
    } else {
      alphaRemoteDb = null;
    }
    if (betaEndpoint.host) {
      betaRemoteDb = `${nsBase}/beta.db`;
      remoteEnsures.push(
        ensureRemoteParentDir({
          host: betaEndpoint.host,
          port: betaEndpoint.port ?? undefined,
          path: betaRemoteDb,
          logger,
        }),
      );
    } else {
      betaRemoteDb = null;
    }
    updates.alpha_remote_db = alphaRemoteDb;
    updates.beta_remote_db = betaRemoteDb;
  }

  if (Object.keys(updates).length) {
    updateSession(sessionDb, id, updates);
  }

  await Promise.all(remoteEnsures);

  if (resetRequested) {
    await resetSession({ sessionDb, id, logger });
    changes.push("reset");
  } else if (wasRunning && needRestart) {
    if (row.scheduler_pid) {
      stopPid(row.scheduler_pid);
    }
    setActualState(sessionDb, id, "stopped");
  }

  const shouldRestart = wasRunning && (resetRequested || needRestart);
  const updatedRow = loadSessionById(sessionDb, id);
  if (shouldRestart && updatedRow) {
    const pid = spawnSchedulerForSession(sessionDb, updatedRow, logger);
    setDesiredState(sessionDb, id, "running");
    setActualState(sessionDb, id, pid ? "running" : "error");
    if (pid) {
      recordHeartbeat(sessionDb, id, "running", pid);
    } else {
      logger?.error("failed to restart session", { sessionId: id });
    }
  }

  return {
    changes,
    restarted: shouldRestart,
    reset: resetRequested,
  };
}
