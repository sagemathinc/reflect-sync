import { createHash } from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { getReflectSyncHome } from "./app-paths.js";
import { findExecutable } from "./executable.js";
import { rsyncRuntimeConfig as runtimeConfig } from "./rsync-runtime-config.js";

export type RsyncRuntimeSource = "explicit" | "managed" | "system";
export type ResolvedRsync = {
  path: string;
  source: RsyncRuntimeSource;
  runtimeVersion?: string;
  target: string;
};

export type InstallRsyncRuntimeOptions = {
  sourceExecutable: string;
  expectedSha256: string;
  home?: string;
  target?: string;
  runtimeVersion?: string;
  metadata?: Record<string, unknown>;
};

export function rsyncRuntimeTarget(
  platform = process.platform,
  arch = process.arch,
): string {
  const osName = platform === "win32" ? "windows" : platform;
  const architecture = arch === "x64" ? "x64" : arch;
  return `${osName}-${architecture}`;
}

export function managedRsyncPath(
  options: {
    home?: string;
    target?: string;
    runtimeVersion?: string;
  } = {},
): string {
  const home = options.home ?? getReflectSyncHome();
  const target = options.target ?? rsyncRuntimeTarget();
  const version = options.runtimeVersion ?? runtimeConfig.runtimeVersion;
  const executable = target.startsWith("windows-") ? "rsync.exe" : "rsync";
  return path.join(
    home,
    "runtimes",
    "rsync",
    version,
    target,
    "bin",
    executable,
  );
}

export function resolveRsyncExecutable(
  options: {
    explicit?: string;
    env?: NodeJS.ProcessEnv;
    home?: string;
    target?: string;
    runtimeVersion?: string;
  } = {},
): ResolvedRsync {
  const env = options.env ?? process.env;
  const target = options.target ?? rsyncRuntimeTarget();
  const explicit =
    options.explicit ?? env.REFLECT_SYNC_RSYNC ?? env.REFLECT_RSYNC;
  if (explicit) {
    const resolved = findExecutable(explicit, env);
    if (!resolved)
      throw new Error(`configured rsync is not executable: ${explicit}`);
    return { path: resolved, source: "explicit", target };
  }

  const runtimeVersion = options.runtimeVersion ?? runtimeConfig.runtimeVersion;
  const managed = managedRsyncPath({
    home: options.home,
    target,
    runtimeVersion,
  });
  if (isExecutable(managed)) {
    return {
      path: managed,
      source: "managed",
      runtimeVersion,
      target,
    };
  }

  const system = findExecutable("rsync", env);
  if (system) return { path: system, source: "system", target };
  throw new Error(
    `no rsync executable found for ${target}; install managed runtime ${runtimeConfig.runtimeVersion} or set REFLECT_SYNC_RSYNC`,
  );
}

export async function installManagedRsyncRuntime(
  options: InstallRsyncRuntimeOptions,
): Promise<ResolvedRsync> {
  assertSha256(options.expectedSha256);
  const target = options.target ?? rsyncRuntimeTarget();
  const runtimeVersion = options.runtimeVersion ?? runtimeConfig.runtimeVersion;
  const destination = managedRsyncPath({
    home: options.home,
    target,
    runtimeVersion,
  });
  const finalDirectory = path.dirname(path.dirname(destination));
  const parent = path.dirname(finalDirectory);
  await fsp.mkdir(parent, { recursive: true, mode: 0o700 });

  if (await matchesDigest(destination, options.expectedSha256)) {
    return { path: destination, source: "managed", runtimeVersion, target };
  }
  if (fs.existsSync(finalDirectory)) {
    throw new Error(
      `managed rsync directory exists but failed verification: ${finalDirectory}`,
    );
  }

  const temporary = await fsp.mkdtemp(path.join(parent, ".install-"));
  try {
    const temporaryExecutable = path.join(
      temporary,
      "bin",
      target.startsWith("windows-") ? "rsync.exe" : "rsync",
    );
    await fsp.mkdir(path.dirname(temporaryExecutable), {
      recursive: true,
      mode: 0o700,
    });
    await fsp.copyFile(options.sourceExecutable, temporaryExecutable);
    if (process.platform !== "win32")
      await fsp.chmod(temporaryExecutable, 0o755);
    if (!(await matchesDigest(temporaryExecutable, options.expectedSha256))) {
      throw new Error("managed rsync digest mismatch after copy");
    }
    await fsp.writeFile(
      path.join(temporary, "manifest.json"),
      `${JSON.stringify(
        {
          schemaVersion: 1,
          runtimeVersion,
          target,
          executableSha256: options.expectedSha256,
          installedAt: new Date().toISOString(),
          metadata: options.metadata,
        },
        null,
        2,
      )}\n`,
      { mode: 0o600 },
    );
    try {
      await fsp.rename(temporary, finalDirectory);
    } catch (error) {
      if (!(await matchesDigest(destination, options.expectedSha256)))
        throw error;
    }
  } finally {
    await fsp.rm(temporary, { recursive: true, force: true });
  }
  return { path: destination, source: "managed", runtimeVersion, target };
}

export async function sha256File(file: string): Promise<string> {
  const hash = createHash("sha256");
  const stream = fs.createReadStream(file);
  for await (const chunk of stream) hash.update(chunk);
  return hash.digest("hex");
}

async function matchesDigest(file: string, expected: string): Promise<boolean> {
  try {
    return (await sha256File(file)) === expected.toLowerCase();
  } catch {
    return false;
  }
}

function isExecutable(file: string): boolean {
  try {
    fs.accessSync(file, fs.constants.X_OK);
    return fs.statSync(file).isFile();
  } catch {
    return false;
  }
}

function assertSha256(digest: string): void {
  if (!/^[0-9a-f]{64}$/iu.test(digest)) {
    throw new Error(`invalid SHA-256 digest: ${digest}`);
  }
}

export { runtimeConfig as rsyncRuntimeConfig };
