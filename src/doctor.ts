import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Command } from "commander";
import pkg from "../package.json" with { type: "json" };
import { detectFilesystemCapabilities } from "./fs-capabilities.js";
import { findExecutable } from "./executable.js";
import {
  captureCommand,
  type CommandResult,
  type CommandRunner,
} from "./process-capture.js";
import { probeRsync, type RsyncCapabilities } from "./rsync-capabilities.js";
import { resolveRsyncExecutable, type ResolvedRsync } from "./rsync-runtime.js";
import { getSessionDbPath } from "./session-db.js";

export type DoctorStatus = "ok" | "warning" | "error";
export type DoctorCheck = {
  id: string;
  status: DoctorStatus;
  summary: string;
  details?: Record<string, unknown>;
};
export type DoctorReport = {
  ok: boolean;
  generatedAt: string;
  reflectSync: { version: string; bundled: boolean; executable: string };
  system: Record<string, unknown>;
  checks: DoctorCheck[];
  rsync?: RsyncCapabilities;
  remote?: Record<string, unknown>;
};

export type DoctorOptions = {
  json?: boolean;
  root?: string;
  remote?: string;
  port?: number;
  rsync?: string;
  installCheck?: boolean;
};

export async function collectDoctorReport(
  options: DoctorOptions = {},
  runner: CommandRunner = captureCommand,
): Promise<DoctorReport> {
  const checks: DoctorCheck[] = [];
  const nodeMajor = Number(process.versions.node.split(".")[0]);
  checks.push({
    id: "node",
    status: nodeMajor >= 22 ? "ok" : "error",
    summary: `Node ${process.versions.node}${process.env.REFLECT_BUNDLED ? " (SEA)" : ""}`,
  });

  const sshPath = findExecutable("ssh");
  if (!sshPath) {
    checks.push({
      id: "ssh",
      status: "error",
      summary: "OpenSSH client not found",
    });
  } else {
    const sshVersion = await runner(sshPath, ["-V"], { timeoutMs: 5_000 });
    checks.push({
      id: "ssh",
      status: sshVersion.code === 0 ? "ok" : "error",
      summary: firstOutputLine(sshVersion) || `SSH at ${sshPath}`,
      details: { path: sshPath },
    });
  }

  let resolvedRsync: ResolvedRsync | undefined;
  try {
    resolvedRsync = resolveRsyncExecutable({ explicit: options.rsync });
  } catch {
    // Reported as a structured failed check below.
  }
  const rsyncPath = resolvedRsync?.path;
  let rsync: RsyncCapabilities | undefined;
  if (!rsyncPath) {
    checks.push({
      id: "rsync",
      status: "error",
      summary:
        "No rsync executable found; install or configure a managed runtime",
    });
  } else {
    rsync = await probeRsync(rsyncPath, runner);
    checks.push(rsyncCheck(rsync, resolvedRsync));
  }

  const sessionDb = getSessionDbPath();
  checks.push({
    id: "state",
    status: "ok",
    summary: fs.existsSync(sessionDb)
      ? `State database found at ${sessionDb}`
      : `State database will be created at ${sessionDb}`,
    details: { sessionDb, exists: fs.existsSync(sessionDb) },
  });

  if (options.root) {
    const root = path.resolve(options.root);
    try {
      const capabilities = await detectFilesystemCapabilities(root);
      checks.push({
        id: "filesystem",
        status: "ok",
        summary: `Filesystem capabilities probed at ${root}`,
        details: { root, ...capabilities },
      });
    } catch (error) {
      checks.push({
        id: "filesystem",
        status: "error",
        summary: `Filesystem probe failed at ${root}`,
        details: {
          error: error instanceof Error ? error.message : String(error),
        },
      });
    }
  }

  let remote: Record<string, unknown> | undefined;
  if (options.remote) {
    const remoteResult = await probeRemote(
      options.remote,
      options.port,
      runner,
    );
    remote = remoteResult.details;
    checks.push(remoteResult.check);
  }

  const report: DoctorReport = {
    ok: !checks.some((check) => check.status === "error"),
    generatedAt: new Date().toISOString(),
    reflectSync: {
      version: pkg.version,
      bundled: Boolean(process.env.REFLECT_BUNDLED),
      executable: process.execPath,
    },
    system: {
      platform: process.platform,
      arch: process.arch,
      release: os.release(),
      libc: runtimeLibc(),
      cpus: os.availableParallelism(),
    },
    checks,
    rsync,
    remote,
  };
  return report;
}

export function registerDoctorCommand(program: Command): void {
  program
    .command("doctor")
    .description("check local and remote prerequisites and capabilities")
    .option("--json", "emit a machine-readable report", false)
    .option("--root <path>", "probe filesystem behavior at this root")
    .option("--remote <host>", "probe an SSH target")
    .option("--port <port>", "remote SSH port", parsePort)
    .option("--rsync <path>", "override the local rsync executable")
    .option(
      "--install-check",
      "verify an installation (alias for the standard checks)",
      false,
    )
    .action(async (options: DoctorOptions) => {
      const report = await collectDoctorReport(options);
      process.stdout.write(
        options.json
          ? `${JSON.stringify(report, null, 2)}\n`
          : renderDoctorReport(report),
      );
      if (!report.ok) process.exitCode = 1;
    });
}

export function renderDoctorReport(report: DoctorReport): string {
  const mark: Record<DoctorStatus, string> = {
    ok: "✓",
    warning: "!",
    error: "✗",
  };
  const lines = [
    `ReflectSync ${report.reflectSync.version}`,
    `${String(report.system.platform)}/${String(report.system.arch)}`,
    "",
  ];
  for (const check of report.checks) {
    lines.push(`${mark[check.status]} ${check.summary}`);
    if (check.id === "rsync" && report.rsync?.missing.length) {
      lines.push(`  missing: ${report.rsync.missing.join(", ")}`);
    }
  }
  lines.push("", report.ok ? "Ready." : "Action required.");
  return `${lines.join("\n")}\n`;
}

function rsyncCheck(
  rsync: RsyncCapabilities,
  resolved?: ResolvedRsync,
): DoctorCheck {
  if (!rsync.available) {
    return {
      id: "rsync",
      status: "error",
      summary: `Unable to run rsync at ${rsync.path}`,
      details: { error: rsync.error, source: resolved?.source },
    };
  }
  if (rsync.implementation === "openrsync") {
    return {
      id: "rsync",
      status: "error",
      summary: `${rsync.firstLine || "Apple openrsync"} is incompatible; use the managed upstream runtime`,
      details: {
        path: rsync.path,
        source: resolved?.source,
        runtimeVersion: resolved?.runtimeVersion,
        missing: rsync.missing,
      },
    };
  }
  if (!rsync.compatible) {
    return {
      id: "rsync",
      status: "error",
      summary: `rsync at ${rsync.path} lacks required capabilities`,
      details: {
        version: rsync.version,
        source: resolved?.source,
        runtimeVersion: resolved?.runtimeVersion,
        missing: rsync.missing,
      },
    };
  }
  return {
    id: "rsync",
    status: "ok",
    summary: `${rsync.firstLine} at ${rsync.path}`,
    details: {
      version: rsync.version,
      protocol: rsync.protocol,
      source: resolved?.source,
      runtimeVersion: resolved?.runtimeVersion,
    },
  };
}

async function probeRemote(
  host: string,
  port: number | undefined,
  runner: CommandRunner,
): Promise<{ check: DoctorCheck; details: Record<string, unknown> }> {
  const args = ["-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5"];
  if (port !== undefined) args.push("-p", String(port));
  args.push(
    host,
    'sh -lc \'printf "os=%s\\narch=%s\\nhome=%s\\nrsync=%s\\n" "$(uname -s 2>/dev/null || echo unknown)" "$(uname -m 2>/dev/null || echo unknown)" "$HOME" "$(command -v rsync 2>/dev/null || true)"\'',
  );
  const result = await runner("ssh", args, { timeoutMs: 10_000 });
  if (result.code !== 0) {
    return {
      check: {
        id: "remote",
        status: "error",
        summary: `Cannot probe ${host} over non-interactive SSH`,
        details: { error: resultError(result) },
      },
      details: { host, port, reachable: false },
    };
  }
  const values = Object.fromEntries(
    result.stdout
      .split(/\r?\n/u)
      .map((line) => line.split("=", 2))
      .filter((parts) => parts.length === 2),
  );
  const hasRsync = Boolean(values.rsync);
  return {
    check: {
      id: "remote",
      status: hasRsync ? "ok" : "warning",
      summary: hasRsync
        ? `${host} is reachable (${values.os}/${values.arch})`
        : `${host} is reachable but has no system rsync; managed bootstrap is required`,
      details: values,
    },
    details: { host, port, reachable: true, ...values },
  };
}

function firstOutputLine(result: CommandResult): string {
  return (
    `${result.stdout}\n${result.stderr}`
      .split(/\r?\n/u)
      .map((line) => line.trim())
      .find(Boolean) ?? ""
  );
}

function resultError(result: CommandResult): string {
  if (result.timedOut) return "command timed out";
  return (
    result.error ?? (firstOutputLine(result) || `exit code ${result.code}`)
  );
}

function runtimeLibc(): string | null {
  if (process.platform !== "linux") return null;
  try {
    const report = process.report?.getReport() as {
      header?: { glibcVersionRuntime?: string };
    };
    return report.header?.glibcVersionRuntime
      ? `glibc ${report.header.glibcVersionRuntime}`
      : "unknown";
  } catch {
    return "unknown";
  }
}

function parsePort(value: string): number {
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error(`invalid SSH port: ${value}`);
  }
  return port;
}
