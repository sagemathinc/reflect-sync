import { findExecutable } from "./executable.js";
import {
  captureCommand,
  type CommandResult,
  type CommandRunner,
} from "./process-capture.js";

export const REQUIRED_RSYNC_FEATURES = [
  "from0",
  "relative",
  "outbuf",
  "infoProgress2",
  "logFile",
  "logFileFormat",
] as const;

export type RsyncFeature = (typeof REQUIRED_RSYNC_FEATURES)[number];
export type RsyncCapabilities = {
  available: boolean;
  compatible: boolean;
  path: string;
  implementation: "upstream" | "openrsync" | "unknown";
  version: string | null;
  protocol: number | null;
  features: Record<RsyncFeature, boolean> & {
    compressChoice: boolean;
    zstd: boolean;
    lz4: boolean;
  };
  missing: RsyncFeature[];
  firstLine: string;
  error?: string;
};

export function configuredRsyncPath(
  explicit?: string,
  env: NodeJS.ProcessEnv = process.env,
): string | null {
  const configured = explicit ?? env.REFLECT_SYNC_RSYNC ?? env.REFLECT_RSYNC;
  if (configured) return findExecutable(configured, env) ?? configured;
  return findExecutable("rsync", env);
}

export async function probeRsync(
  rsyncPath: string,
  runner: CommandRunner = captureCommand,
): Promise<RsyncCapabilities> {
  const versionResult = await runner(rsyncPath, ["--version"], {
    timeoutMs: 5_000,
  });
  const versionText = combineOutput(versionResult);
  const firstLine =
    versionText
      .split(/\r?\n/u)
      .map((line) => line.trim())
      .find(Boolean) ?? "";
  if (versionResult.code !== 0) {
    return unavailable(rsyncPath, firstLine, resultError(versionResult));
  }

  const helpResult = await runner(rsyncPath, ["--help"], { timeoutMs: 5_000 });
  if (helpResult.code !== 0) {
    return unavailable(rsyncPath, firstLine, resultError(helpResult));
  }
  const help = combineOutput(helpResult);
  const all = `${versionText}\n${help}`;
  const implementation = /openrsync/iu.test(all)
    ? "openrsync"
    : /\brsync\b.*\bversion\b/iu.test(all)
      ? "upstream"
      : "unknown";
  const version =
    all.match(/\bversion\s+(\d+(?:\.\d+){1,3})/iu)?.[1] ??
    all.match(/\brsync\s+(\d+(?:\.\d+){1,3})/iu)?.[1] ??
    null;
  const protocolRaw = all.match(/protocol version\s+(\d+)/iu)?.[1];
  const features = {
    from0: hasOption(help, "from0"),
    relative: hasOption(help, "relative"),
    outbuf: hasOption(help, "outbuf"),
    infoProgress2:
      hasOption(help, "info") &&
      (/progress2/iu.test(help) || implementation === "upstream"),
    logFile: hasOption(help, "log-file"),
    logFileFormat: hasOption(help, "log-file-format"),
    compressChoice: hasOption(help, "compress-choice"),
    zstd: /\bzstd\b/iu.test(all),
    lz4: /\blz4\b/iu.test(all),
  };
  const missing = REQUIRED_RSYNC_FEATURES.filter(
    (feature) => !features[feature],
  );
  return {
    available: true,
    compatible: implementation === "upstream" && missing.length === 0,
    path: rsyncPath,
    implementation,
    version,
    protocol: protocolRaw ? Number(protocolRaw) : null,
    features,
    missing,
    firstLine,
  };
}

function hasOption(help: string, option: string): boolean {
  return new RegExp(`(?:^|\\s)--${option}(?:[=\\s,]|$)`, "mu").test(help);
}

function combineOutput(result: CommandResult): string {
  return `${result.stdout}\n${result.stderr}`.trim();
}

function resultError(result: CommandResult): string {
  if (result.timedOut) return "command timed out";
  return result.error ?? (combineOutput(result) || `exit code ${result.code}`);
}

function unavailable(
  path: string,
  firstLine: string,
  error: string,
): RsyncCapabilities {
  return {
    available: false,
    compatible: false,
    path,
    implementation: "unknown",
    version: null,
    protocol: null,
    features: {
      from0: false,
      relative: false,
      outbuf: false,
      infoProgress2: false,
      logFile: false,
      logFileFormat: false,
      compressChoice: false,
      zstd: false,
      lz4: false,
    },
    missing: [...REQUIRED_RSYNC_FEATURES],
    firstLine,
    error,
  };
}
