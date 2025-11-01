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
} from "./session-db.js";
import { ensureRemoteParentDir, sshDeleteDirectory } from "./remote.js";
import { dirname } from "node:path";
import { CLI_NAME } from "./constants.js";
import { rm } from "node:fs/promises";
import type { Logger } from "./logger.js";

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
    row.alpha_host != null ? row.alpha_port ?? undefined : row.beta_port ?? undefined;
  if (host) {
    const path = row.alpha_remote_db || row.beta_remote_db;
    if (path) {
      const dir = dirname(path);
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
              `session ${id} -- unable to delete ${host}:${path} -- you could use --force and manually delete`,
              err,
            );
            return;
          } else {
            // non-fatal
            console.warn(
              `session ${id} -- failed to delete ${host}:${path}`,
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
    path?: string | null,
  ) => {
    if (!host || !path) return;
    const dir = dirname(path);
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
    remoteDbPaths.get(key)!.paths.add(path);
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
    actual_state: "paused",
    desired_state: "paused",
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
    if (
      sep === "\\" ||
      (sep === "/" && process.platform === "win32")
    ) {
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
  logger,
}): Promise<number> {
  ensureSessionDb(sessionDb);

  const a = parseEndpoint(alphaSpec);
  const b = parseEndpoint(betaSpec);
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
        compress,
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
