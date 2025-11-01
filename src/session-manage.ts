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
  const host = row.alpha_host || row.beta_host;
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

type Endpoint = { root: string; host?: string };

// Minimal mutagen-like endpoint parsing.
// - local: "~/work" or "/abs/path" -> {root: absolute path, host: undefined}
// - remote: "user@host:~/work" or "host:/abs" -> {root: as-given (keep ~ for remote), host}
// Windows drive-letter paths like "C:\x" won’t be mistaken as remote (colon rule).
function parseEndpoint(spec: string): Endpoint {
  // treat as local if absolute path (starts with /) or starts with ~ or looks like
  // absolute windows path, AND does not contain a colon.
  const colon = spec.indexOf(":");
  if (colon <= 0) {
    // no colon so definitely not remote  (or starts with :)
    return { root: spec || "~" };
  }

  // treat as remote if there is a colon AND (contains "@", or
  // starts with something that clearly looks like a host)
  const looksLikeWindows = /^[A-Za-z]:[\\]/.test(spec);
  if (looksLikeWindows || spec.startsWith("/") || spec.startsWith("~")) {
    // absolute path or relative to home -- definitely not remote
    return { root: spec || "~" };
  }

  // not obviously a path, and does have a colon
  const host = spec.slice(0, colon);
  const root = spec.slice(colon + 1);
  return { host, root: root || "~" };
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
        beta_host: b.host ?? null,
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
        await ensureRemoteParentDir(a.host, alphaRemoteDb, logger);
      if (b.host && betaRemoteDb)
        await ensureRemoteParentDir(b.host, betaRemoteDb, logger);
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
