import { spawn } from "node:child_process";
import crypto from "node:crypto";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { defaultHashAlg } from "../../hash.js";
import { NullLogger } from "../../logger.js";
import { newSession, terminateSession } from "../../session-manage.js";
import { SSH_ENABLED } from "../ssh.remote-test-util.js";

const DIST = path.resolve(__dirname, "../../../dist");
const CLI = path.join(DIST, "cli.js");

type RemoteSide = {
  host?: string;
  port?: number;
};

export type EndpointOptions = {
  /**
   * Absolute root path. If omitted a temp directory is created.
   */
  root?: string;
  /**
   * When true the session endpoint uses ssh (assumes host reachable via localhost).
   */
  remote?: boolean;
  /**
   * Optional override for SSH connection (defaults to localhost).
   */
  remoteConfig?: RemoteSide;
};

export type CreateTestSessionOptions = {
  name?: string;
  prefer?: "alpha" | "beta";
  hot?: boolean;
  full?: boolean;
  alpha?: EndpointOptions;
  beta?: EndpointOptions;
  hash?: string;
  compress?: string;
  compressLevel?: string;
  mergeStrategy?: string | null;
};

export type TestSessionSyncOptions = {
  paths?: string[];
  maxCycles?: number;
  timeoutMs?: number;
  progress?: boolean;
  jsonProgress?: boolean;
  extraArgs?: string[];
};

export class TestSessionSide {
  constructor(readonly root: string) {}

  path(...segments: string[]): string {
    return segments.length ? path.join(this.root, ...segments) : this.root;
  }

  async mkdir(rel: string, opts?: fsp.MakeDirectoryOptions): Promise<void> {
    await fsp.mkdir(this.path(rel), {
      recursive: opts?.recursive ?? true,
      mode: opts?.mode,
    });
  }

  async writeFile(
    rel: string,
    data: Parameters<typeof fsp.writeFile>[1],
    options?: Parameters<typeof fsp.writeFile>[2],
  ): Promise<void> {
    const dest = this.path(rel);
    await fsp.mkdir(path.dirname(dest), { recursive: true });
    await fsp.writeFile(dest, data, options);
  }

  async appendFile(
    rel: string,
    data: Parameters<typeof fsp.appendFile>[1],
    options?: Parameters<typeof fsp.appendFile>[2],
  ): Promise<void> {
    const dest = this.path(rel);
    await fsp.mkdir(path.dirname(dest), { recursive: true });
    await fsp.appendFile(dest, data, options);
  }

  async readFile(
    rel: string,
    options?: Parameters<typeof fsp.readFile>[1],
  ): Promise<string | Buffer> {
    return fsp.readFile(this.path(rel), options);
  }

  async rm(rel: string, opts?: fsp.RmOptions): Promise<void> {
    await fsp.rm(this.path(rel), {
      recursive: opts?.recursive ?? true,
      force: opts?.force ?? true,
      maxRetries: opts?.maxRetries,
    });
  }

  async exists(rel: string): Promise<boolean> {
    try {
      await fsp.lstat(this.path(rel));
      return true;
    } catch {
      return false;
    }
  }
}

export class TestSession {
  readonly alpha: TestSessionSide;
  readonly beta: TestSessionSide;
  readonly sessionDb: string;

  #disposed = false;

  constructor(
    readonly id: number,
    private readonly reflectHome: string,
    private readonly workspace: string,
    alphaRoot: string,
    betaRoot: string,
    readonly env: NodeJS.ProcessEnv,
    sessionDb: string,
  ) {
    this.alpha = new TestSessionSide(alphaRoot);
    this.beta = new TestSessionSide(betaRoot);
    this.sessionDb = sessionDb;
  }

  async sync(options: TestSessionSyncOptions = {}): Promise<void> {
    const args = [
      "session",
      "sync",
      String(this.id),
      "--session-db",
      this.sessionDb,
    ];
    if (options.maxCycles !== undefined) {
      args.push("--max-cycles", String(options.maxCycles));
    }
    if (options.timeoutMs !== undefined) {
      args.push("--timeout", String(options.timeoutMs));
    }
    for (const rel of options.paths ?? []) {
      args.push("--path", rel);
    }
    if (options.progress) {
      args.push("--progress");
    }
    if (options.jsonProgress) {
      args.push("--json");
    }
    if (options.extraArgs?.length) {
      args.push(...options.extraArgs);
    }
    await runCli(args, this.env);
  }

  async dispose(): Promise<void> {
    if (this.#disposed) return;
    this.#disposed = true;
    await withReflectHome(this.reflectHome, async () => {
      await terminateSession({
        sessionDb: this.sessionDb,
        id: this.id,
        logger: new NullLogger(),
        force: true,
      });
    });
    await fsp.rm(this.workspace, { recursive: true, force: true });
  }
}

export async function createTestSession(
  options: CreateTestSessionOptions = {},
): Promise<TestSession> {
  const workspace = await fsp.mkdtemp(
    path.join(os.tmpdir(), "reflect-int-session-"),
  );
  const reflectHome = path.join(workspace, "state");
  await fsp.mkdir(reflectHome, { recursive: true });

  const alphaRoot = await ensureRootDir(
    path.resolve(options.alpha?.root ?? path.join(workspace, "alpha")),
  );
  const betaRoot = await ensureRootDir(
    path.resolve(options.beta?.root ?? path.join(workspace, "beta")),
  );

  const alphaSpec = formatEndpointSpec(alphaRoot, options.alpha);
  const betaSpec = formatEndpointSpec(betaRoot, options.beta);

  const sessionDb = path.join(reflectHome, "sessions.db");
  const env = {
    ...process.env,
    REFLECT_HOME: reflectHome,
    REFLECT_DISABLE_LOG_ECHO: "1",
  };

  const name = options.name ?? `test-${crypto.randomBytes(4).toString("hex")}`;

  let sessionId: number;
  try {
    sessionId = await withReflectHome(reflectHome, async () =>
      newSession({
        alphaSpec,
        betaSpec,
        sessionDb,
        compress: options.compress ?? "auto",
        compressLevel: options.compressLevel,
        prefer: options.prefer ?? "alpha",
        hash: options.hash ?? defaultHashAlg(),
        label: [],
        name,
        ignore: [],
        logger: new NullLogger(),
        disableHotSync: options.hot !== true,
        disableFullSync: options.full !== true,
        mergeStrategy: options.mergeStrategy ?? null,
      }),
    );
  } catch (err) {
    await fsp.rm(workspace, { recursive: true, force: true });
    throw err;
  }

  return new TestSession(
    sessionId,
    reflectHome,
    workspace,
    alphaRoot,
    betaRoot,
    env,
    sessionDb,
  );
}

function formatEndpointSpec(
  root: string,
  opts?: EndpointOptions,
): string {
  if (!opts?.remote) return root;
  if (!SSH_ENABLED) {
    throw new Error("SSH is unavailable on this host; remote endpoints disabled.");
  }
  const host = opts.remoteConfig?.host ?? "localhost";
  const port = opts.remoteConfig?.port;
  if (port === undefined) {
    return `${host}:${root}`;
  }
  return `${host}:${port}:${root}`;
}

async function ensureRootDir(root: string): Promise<string> {
  await fsp.mkdir(root, { recursive: true });
  return root;
}

function runCli(args: string[], env: NodeJS.ProcessEnv): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [CLI, ...args], {
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout?.setEncoding("utf8");
    child.stdout?.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr?.setEncoding("utf8");
    child.stderr?.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        const err = new Error(
          `reflect ${args.join(" ")} exited with code ${code}\n${stderr || stdout}`,
        );
        reject(err);
      }
    });
  });
}

async function withReflectHome<T>(
  home: string,
  fn: () => Promise<T>,
): Promise<T> {
  const prev = process.env.REFLECT_HOME;
  process.env.REFLECT_HOME = home;
  try {
    return await fn();
  } finally {
    if (prev === undefined) {
      delete process.env.REFLECT_HOME;
    } else {
      process.env.REFLECT_HOME = prev;
    }
  }
}

export const SSH_AVAILABLE = SSH_ENABLED;
