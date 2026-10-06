import { isIPv4, isIPv6 } from "node:net";
import {
  createForwardSession,
  deleteForwardSession,
  loadForwardById,
  selectForwardSessions,
  type ForwardRow,
  updateForwardSession,
} from "./session-db.js";
import { launchForwardProcess } from "./forward-runner.js";
import { stopPid } from "./session-manage.js";
import type { Logger } from "./logger.js";
import { isProcessAlive, stopProcess } from "./process-lifecycle.js";
import { withResourceLock } from "./resource-lock.js";

export interface ForwardCreateOptions {
  sessionDb: string;
  name?: string;
  left: string;
  right: string;
  compress?: boolean;
  stopped?: boolean;
  // Address the remote sshd listens on for remote -> local forwards (ssh -R).
  remoteBind?: string;
  logger?: Logger;
}

// Stored remote_host for new remote -> local forwards without --remote-bind:
// loopback only, like plain `ssh -R`.  Rows created before 0.17 stored "",
// which buildSshArgs still maps to all interfaces (0.0.0.0).
const DEFAULT_REMOTE_BIND = "127.0.0.1";

const HOSTNAME =
  /^(?=.{1,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$/i;

// Validate a --remote-bind value and return it in the form used inside ssh's
// colon-separated -R specification: IPv4, a hostname, `*` (all interfaces),
// or an IPv6 literal, which must be bracketed there.  Anything else is
// rejected rather than passed through, since stray ':' or brackets would
// change the meaning of the specification.
export function normalizeRemoteBind(value: string): string {
  if (/[\u0000-\u001f\u007f]/.test(value)) {
    throw Error("Invalid --remote-bind: control characters are not allowed");
  }
  const bind = value.trim();
  if (!bind) throw Error("--remote-bind must not be empty");
  if (bind === "*" || isIPv4(bind) || HOSTNAME.test(bind)) return bind;
  const unbracketed =
    bind.startsWith("[") && bind.endsWith("]") ? bind.slice(1, -1) : bind;
  if (isIPv6(unbracketed) && !unbracketed.includes("%")) {
    return `[${unbracketed}]`;
  }
  throw Error(
    `Invalid --remote-bind '${value}': expected an IPv4 or IPv6 address, a hostname, or *`,
  );
}

export interface ParsedEndpoint {
  host: string;
  port: number;
  isLocal: boolean;
  sshHost?: string;
  sshPort?: number | null;
}

const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "::1", "0.0.0.0", ""]);

function parseLocalEndpoint(spec: string): { host: string; port: number } {
  const trimmed = spec.trim();
  const match = /^(?:(?<host>[^:]*):)?(?<port>\d+)$/.exec(trimmed);
  if (!match) {
    throw new Error(
      `Invalid local endpoint '${spec}', expected host:port or :port`,
    );
  }
  const host = match.groups?.host?.trim() ?? "";
  const port = Number(match.groups?.port);
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new Error(`Invalid port in endpoint '${spec}'`);
  }
  return { host: host || "127.0.0.1", port };
}

function detectDirection(
  left: ParsedEndpoint,
  right: ParsedEndpoint,
): "local_to_remote" | "remote_to_local" {
  if (left.isLocal && !right.isLocal) return "local_to_remote";
  if (!left.isLocal && right.isLocal) return "remote_to_local";
  throw new Error("Exactly one endpoint must be remote (user@host:port)");
}

function normalizeLocalHost(host: string): string {
  const trimmed = host.trim();
  if (!trimmed || LOCAL_HOSTS.has(trimmed.toLowerCase())) return "127.0.0.1";
  return trimmed;
}

function parseEndpoint(spec: string): ParsedEndpoint {
  const trimmed = spec.trim();
  const remoteMatch =
    /^(?:(?<user>[^@]+)@)?(?<host>[^:]+)(?::(?<sshPort>\d+))?:(?<port>\d+)$/.exec(
      trimmed,
    );
  if (remoteMatch) {
    const user = remoteMatch.groups?.user;
    const hostPart = remoteMatch.groups!.host.trim();
    const sshPort = remoteMatch.groups?.sshPort
      ? Number(remoteMatch.groups?.sshPort)
      : null;
    const port = Number(remoteMatch.groups!.port);
    if (!Number.isInteger(port) || port <= 0 || port > 65535) {
      throw new Error(`Invalid port in endpoint '${spec}'`);
    }
    if (
      sshPort != null &&
      (!Number.isInteger(sshPort) || sshPort <= 0 || sshPort > 65535)
    ) {
      throw new Error(`Invalid SSH port in endpoint '${spec}'`);
    }
    const hostLower = hostPart.toLowerCase();
    const treatAsLocal =
      !user &&
      (LOCAL_HOSTS.has(hostLower) ||
        hostLower.startsWith("127.") ||
        hostLower === "::1");
    if (!treatAsLocal) {
      const sshHost = user ? `${user}@${hostPart}` : hostPart;
      return {
        // Destination host for -L forwards should default to "localhost",
        // matching common ssh behavior.
        host: "localhost",
        port,
        isLocal: false,
        sshHost,
        sshPort,
      };
    }
  }

  const local = parseLocalEndpoint(trimmed);
  return {
    host: normalizeLocalHost(local.host),
    port: local.port,
    isLocal: true,
  };
}

export async function createForward({
  sessionDb,
  name,
  left,
  right,
  compress,
  stopped = false,
  remoteBind,
  logger,
}: ForwardCreateOptions): Promise<number> {
  if (name && /^\d+$/.test(name.trim()))
    throw Error("Names must not be numeric-only");
  const leftEp = parseEndpoint(left);
  const rightEp = parseEndpoint(right);
  const direction = detectDirection(leftEp, rightEp);
  if (remoteBind !== undefined && direction !== "remote_to_local") {
    throw Error(
      "--remote-bind only applies to remote -> local forwards (remote endpoint first)",
    );
  }

  let sshHost: string;
  let sshPort: number | null;
  let localHost: string;
  let localPort: number;
  let remoteHost: string;
  let remotePort: number;

  if (direction === "local_to_remote") {
    const remote = rightEp;
    const local = leftEp;
    if (!remote.sshHost)
      throw new Error("Remote endpoint must include ssh host");
    sshHost = remote.sshHost;
    sshPort = remote.sshPort ?? null;
    localHost = local.host;
    localPort = local.port;
    remoteHost = remote.host || "localhost";
    remotePort = remote.port;
  } else {
    const remote = leftEp;
    const local = rightEp;
    if (!remote.sshHost)
      throw new Error("Remote endpoint must include ssh host");
    sshHost = remote.sshHost;
    sshPort = remote.sshPort ?? null;
    localHost = local.host;
    localPort = local.port;
    remoteHost =
      remoteBind === undefined
        ? DEFAULT_REMOTE_BIND
        : normalizeRemoteBind(remoteBind);
    remotePort = remote.port;
  }

  const id = createForwardSession(sessionDb, {
    name: name?.trim() ? name.trim() : null,
    direction,
    ssh_host: sshHost,
    ssh_port: sshPort,
    ssh_compress: !!compress,
    local_host: localHost,
    local_port: localPort,
    remote_host: remoteHost,
    remote_port: remotePort,
    desired_state: stopped ? "stopped" : "running",
    actual_state:
      stopped || process.env.REFLECT_DISABLE_FORWARD === "1"
        ? "stopped"
        : "running",
  });

  if (!stopped && process.env.REFLECT_DISABLE_FORWARD !== "1") {
    await withResourceLock(sessionDb, "forward", id, async () => {
      const row = loadForwardById(sessionDb, id);
      if (
        row &&
        row.desired_state === "running" &&
        !isProcessAlive(row.monitor_pid)
      ) {
        const pid = await launchForwardProcess(sessionDb, row);
        if (pid) {
          logger?.debug?.("launched forward ssh", { id, pid });
        } else {
          throw Error(`Unable to start forward ${id}; configuration retained`);
        }
      }
    });
  }

  return id;
}

export async function stopForward(
  sessionDb: string,
  id: number,
): Promise<void> {
  const row = loadForwardById(sessionDb, id);
  if (!row) throw Error(`Forward ${id} not found`);
  updateForwardSession(sessionDb, id, { desired_state: "stopped" });
  await stopProcess(row.monitor_pid);
  updateForwardSession(sessionDb, id, {
    actual_state: "stopped",
    monitor_pid: null,
  });
}

export async function startForward(
  sessionDb: string,
  id: number,
): Promise<void> {
  const row = loadForwardById(sessionDb, id);
  if (!row) throw Error(`Forward ${id} not found`);
  updateForwardSession(sessionDb, id, { desired_state: "running" });
  if (isProcessAlive(row.monitor_pid)) return;
  if (!(await launchForwardProcess(sessionDb, row)))
    throw Error(`Unable to start forward ${id}`);
}

export async function removeForward(
  sessionDb: string,
  id: number,
  stop = false,
): Promise<void> {
  const row = loadForwardById(sessionDb, id);
  if (!row) throw Error(`Forward ${id} not found`);
  if (
    !stop &&
    (row.desired_state === "running" || isProcessAlive(row.monitor_pid))
  )
    throw Error("Forward is active; stop it first or use --stop");
  await stopForward(sessionDb, id);
  deleteForwardSession(sessionDb, id);
}

export function terminateForward(
  sessionDb: string,
  id: number,
  logger?: Logger,
): void {
  const row = loadForwardById(sessionDb, id);
  if (!row) return;
  if (row.monitor_pid) {
    stopPid(row.monitor_pid);
  }
  deleteForwardSession(sessionDb, id);
  logger?.debug?.("forward session terminated", { id });
}

export function listForwards(sessionDb: string): ForwardRow[] {
  return selectForwardSessions(sessionDb);
}
