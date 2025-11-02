import {
  createForwardSession,
  deleteForwardSession,
  loadForwardById,
  selectForwardSessions,
  updateForwardSession,
  type ForwardRow,
} from "./session-db.js";
import { spawnForwardMonitor } from "./forward-runner.js";
import { stopPid } from "./session-manage.js";
import type { Logger } from "./logger.js";

export interface ForwardCreateOptions {
  sessionDb: string;
  name?: string;
  left: string;
  right: string;
  compress?: boolean;
  logger?: Logger;
}

export interface ParsedEndpoint {
  host: string;
  port: number;
  isLocal: boolean;
  sshHost?: string;
  sshPort?: number | null;
}

const LOCAL_HOSTS = new Set([
  "localhost",
  "127.0.0.1",
  "::1",
  "0.0.0.0",
  "",
]);

function parseLocalEndpoint(spec: string): { host: string; port: number } {
  const trimmed = spec.trim();
  const match = /^(?:(?<host>[^:]*):)?(?<port>\d+)$/.exec(trimmed);
  if (!match) {
    throw new Error(`Invalid local endpoint '${spec}', expected host:port or :port`);
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
  const remoteMatch = /^(?:(?<user>[^@]+)@)?(?<host>[^:]+)(?::(?<sshPort>\d+))?:(?<port>\d+)$/.exec(trimmed);
  if (remoteMatch) {
    const user = remoteMatch.groups?.user;
    const hostPart = remoteMatch.groups!.host.trim();
    const sshPort = remoteMatch.groups?.sshPort ? Number(remoteMatch.groups?.sshPort) : null;
    const port = Number(remoteMatch.groups!.port);
    if (!Number.isInteger(port) || port <= 0 || port > 65535) {
      throw new Error(`Invalid port in endpoint '${spec}'`);
    }
    if (sshPort != null && (!Number.isInteger(sshPort) || sshPort <= 0 || sshPort > 65535)) {
      throw new Error(`Invalid SSH port in endpoint '${spec}'`);
    }
    const hostLower = hostPart.toLowerCase();
    const treatAsLocal =
      !user &&
      (LOCAL_HOSTS.has(hostLower) || hostLower.startsWith("127.") || hostLower === "::1");
    if (!treatAsLocal) {
      const sshHost = user ? `${user}@${hostPart}` : hostPart;
      return {
        host: "127.0.0.1",
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

export function createForward({
  sessionDb,
  name,
  left,
  right,
  compress,
  logger,
}: ForwardCreateOptions): number {
  const leftEp = parseEndpoint(left);
  const rightEp = parseEndpoint(right);
  const direction = detectDirection(leftEp, rightEp);

  let sshHost: string;
  let sshPort: number | null;
  let localHost: string;
  let localPort: number;
  let remoteHost: string;
  let remotePort: number;

  if (direction === "local_to_remote") {
    const remote = rightEp;
    const local = leftEp;
    if (!remote.sshHost) throw new Error("Remote endpoint must include ssh host");
    sshHost = remote.sshHost;
    sshPort = remote.sshPort ?? null;
    localHost = local.host;
    localPort = local.port;
    remoteHost = "127.0.0.1";
    remotePort = remote.port;
  } else {
    const remote = leftEp;
    const local = rightEp;
    if (!remote.sshHost) throw new Error("Remote endpoint must include ssh host");
    sshHost = remote.sshHost;
    sshPort = remote.sshPort ?? null;
    localHost = local.host;
    localPort = local.port;
    remoteHost = "";
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
    desired_state: "running",
    actual_state: process.env.REFLECT_DISABLE_FORWARD === "1" ? "stopped" : "running",
  });

  if (process.env.REFLECT_DISABLE_FORWARD !== "1") {
    const row = loadForwardById(sessionDb, id);
    if (row) {
      const pid = spawnForwardMonitor(sessionDb, row);
      if (pid) {
        updateForwardSession(sessionDb, id, { monitor_pid: pid });
        logger?.debug?.("spawned forward monitor", { id, pid });
      }
    }
  }

  return id;
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
