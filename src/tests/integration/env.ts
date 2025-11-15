/**
 * Integration test harness helpers.
 *
 * Tips:
 *   - REFLECT_TEST_KEEP_WORKSPACE=1 keeps temp roots if a test fails.
 *   - REFLECT_TEST_DEBUG=1 (or DEBUG_TESTS=1) streams scheduler logs (log level debug).
 */
import { spawn } from "node:child_process";
import crypto from "node:crypto";
import type { MakeDirectoryOptions, RmOptions } from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { defaultHashAlg } from "../../hash.js";
import { NullLogger } from "../../logger.js";
import { newSession, terminateSession } from "../../session-manage.js";
import {
  loadSessionById,
  setActualState,
  setDesiredState,
} from "../../session-db.js";
import { spawnSchedulerForSession } from "../../session-runner.js";
import { Database } from "../../db.js";
import { SSH_ENABLED } from "../ssh.remote-test-util.js";

if (!process.env.REFLECT_LOG_LEVEL) {
  process.env.REFLECT_LOG_LEVEL = "info";
}

const DIST = path.resolve(__dirname, "../../../dist");
const CLI = path.join(DIST, "cli.js");
const KEEP_WORKSPACE =
  process.env.REFLECT_TEST_KEEP_WORKSPACE === "1" ||
  process.env.DEBUG_TESTS === "1";
const DEBUG_LOGS =
  process.env.REFLECT_TEST_DEBUG === "1" || process.env.DEBUG_TESTS === "1";

type RemoteSide = {
  host?: string;
  port?: number;
};

export type EndpointOptions = {
  root?: string;
  remote?: boolean;
  remoteConfig?: RemoteSide;
};

type EndpointSpec = {
  spec: string;
  host?: string;
  port?: number;
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

  async mkdir(rel: string, opts?: MakeDirectoryOptions): Promise<void> {
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

  async rm(rel: string, opts?: RmOptions): Promise<void> {
    const rmOpts: RmOptions = {
      recursive: opts?.recursive ?? true,
      force: opts?.force ?? true,
    };
    if (typeof opts?.maxRetries === "number") {
      rmOpts.maxRetries = opts.maxRetries;
    }
    await fsp.rm(this.path(rel), rmOpts);
  }

  async exists(rel: string): Promise<boolean> {
    try {
      await fsp.lstat(this.path(rel));
      return true;
    } catch {
      return false;
    }
  }

  async lstat(rel: string) {
    return await fsp.lstat(this.path(rel));
  }

  async stat(rel: string) {
    return await fsp.stat(this.path(rel));
  }
}

export class TestSession {
  readonly alpha: TestSessionSide;
  readonly beta: TestSessionSide;
  readonly sessionDb: string;
  readonly baseDbPath: string;

  #disposed = false;

  constructor(
    readonly id: number,
    private readonly reflectHome: string,
    private readonly workspace: string,
    alphaRoot: string,
    betaRoot: string,
    private readonly envExtra: Record<string, string>,
    sessionDb: string,
    private readonly schedulerPid: number,
  ) {
    this.alpha = new TestSessionSide(alphaRoot);
    this.beta = new TestSessionSide(betaRoot);
    this.sessionDb = sessionDb;
    this.baseDbPath = path.join(reflectHome, "sessions", String(id), "base.db");
  }

  // just like the normal sync subcommand, except maxCycles defaults to 1.
  async sync(options: TestSessionSyncOptions = {}): Promise<void> {
    const args = ["sync", String(this.id), "--session-db", this.sessionDb];
    if (options.maxCycles !== undefined) {
      args.push("--max-cycles", String(options.maxCycles ?? 1));
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
    await runReflect(args, this.envExtra);
  }

  async dispose(): Promise<void> {
    if (this.#disposed) return;
    this.#disposed = true;
    if (this.schedulerPid) {
      try {
        process.kill(this.schedulerPid, "SIGINT");
      } catch {}
      await delay(250);
      try {
        process.kill(this.schedulerPid, "SIGKILL");
      } catch {}
    }
    if (!KEEP_WORKSPACE) {
      await withReflectHome(this.reflectHome, async () => {
        await terminateSession({
          sessionDb: this.sessionDb,
          id: this.id,
          logger: new NullLogger(),
          force: true,
        });
      });
    }
    if (!KEEP_WORKSPACE) {
      await fsp.rm(this.workspace, { recursive: true, force: true });
    } else {
      console.log(
        `kept test workspace for debugging: ${this.workspace} (session ${this.id})`,
      );
    }
  }
}

export async function createTestSession(
  options: CreateTestSessionOptions = {},
): Promise<TestSession> {
  // NOTE: TMP and TEMP environment variables will be checked to override
  // the result of os.tmpdir(), which can be used to run tests on
  // a custom filesystem (not /tmp), e.g, a btrfs directory.
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

  const alphaEndpoint = formatEndpointSpec(alphaRoot, options.alpha);
  const betaEndpoint = formatEndpointSpec(betaRoot, options.beta);

  const sessionDb = path.join(reflectHome, "sessions.db");
  const envExtra: Record<string, string> = {
    REFLECT_HOME: reflectHome,
  };
  if (!DEBUG_LOGS) {
    envExtra.REFLECT_DISABLE_LOG_ECHO = "1";
  } else {
    envExtra.REFLECT_LOG_LEVEL = "debug";
  }

  const name = options.name ?? `test-${crypto.randomBytes(4).toString("hex")}`;
  const prefer = options.prefer ?? "alpha";
  const hashAlg = options.hash ?? defaultHashAlg();

  let sessionId = 0;
  let schedulerPid = 0;
  try {
    await withReflectHome(reflectHome, async () => {
      const sessionInput: any = {
        alphaSpec: alphaEndpoint.spec,
        betaSpec: betaEndpoint.spec,
        sessionDb,
        compress: options.compress ?? "auto",
        compressLevel: options.compressLevel,
        prefer,
        hash: hashAlg,
        label: [],
        name,
        ignore: [],
        logger: new NullLogger(),
        disableHotSync: options.hot !== true,
        disableFullSync: options.full !== true,
      };
      if (options.mergeStrategy !== undefined) {
        sessionInput.mergeStrategy = options.mergeStrategy;
      }

      sessionId = await newSession(sessionInput);
      setDesiredState(sessionDb, sessionId, "running");
      setActualState(sessionDb, sessionId, "running");
      const row = loadSessionById(sessionDb, sessionId);
      if (!row) throw new Error(`session ${sessionId} missing after creation`);
      const prevEntry = process.env.REFLECT_ENTRY;
      const prevLog = process.env.REFLECT_LOG_LEVEL;
      const prevEcho = process.env.REFLECT_DISABLE_LOG_ECHO;
      process.env.REFLECT_ENTRY = CLI;
      if (DEBUG_LOGS) {
        process.env.REFLECT_LOG_LEVEL = "debug";
        delete process.env.REFLECT_DISABLE_LOG_ECHO;
      } else {
        if (!process.env.REFLECT_DISABLE_LOG_ECHO) {
          process.env.REFLECT_DISABLE_LOG_ECHO = "1";
        }
      }
      try {
        schedulerPid = spawnSchedulerForSession(
          sessionDb,
          row,
          new NullLogger(),
        );
      } finally {
        if (prevEntry === undefined) {
          delete process.env.REFLECT_ENTRY;
        } else {
          process.env.REFLECT_ENTRY = prevEntry;
        }
        if (prevLog === undefined) {
          delete process.env.REFLECT_LOG_LEVEL;
        } else {
          process.env.REFLECT_LOG_LEVEL = prevLog;
        }
        if (prevEcho === undefined) {
          delete process.env.REFLECT_DISABLE_LOG_ECHO;
        } else {
          process.env.REFLECT_DISABLE_LOG_ECHO = prevEcho;
        }
      }
    });
    await waitForSchedulerReady(sessionDb, sessionId);
  } catch (err) {
    if (!KEEP_WORKSPACE) {
      await fsp.rm(workspace, { recursive: true, force: true });
    } else {
      console.error(
        `kept failed session workspace for debugging: ${workspace}`,
      );
    }
    throw err;
  }

  return new TestSession(
    sessionId,
    reflectHome,
    workspace,
    alphaRoot,
    betaRoot,
    envExtra,
    sessionDb,
    schedulerPid,
  );
}

function formatEndpointSpec(
  root: string,
  opts?: EndpointOptions,
): EndpointSpec {
  if (!opts?.remote) return { spec: root };
  if (!SSH_ENABLED) {
    throw new Error(
      "SSH is unavailable on this host; remote endpoints disabled.",
    );
  }
  const host = opts.remoteConfig?.host ?? "localhost";
  const port = opts.remoteConfig?.port;
  if (port === undefined) {
    return { spec: `${host}:${root}`, host };
  }
  return { spec: `${host}:${port}:${root}`, host, port };
}

async function ensureRootDir(root: string): Promise<string> {
  await fsp.mkdir(root, { recursive: true });
  return root;
}

function runReflect(
  args: string[],
  envExtra: Record<string, string>,
): Promise<void> {
  const env = { ...process.env, ...envExtra };
  const withDebug = DEBUG_LOGS ? ["--log-level", "debug", ...args] : args;
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [CLI, ...withDebug], {
      env,
      stdio: DEBUG_LOGS ? "inherit" : ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    if (!DEBUG_LOGS) {
      child.stdout?.setEncoding("utf8");
      child.stdout?.on("data", (chunk) => {
        stdout += chunk;
      });
      child.stderr?.setEncoding("utf8");
      child.stderr?.on("data", (chunk) => {
        stderr += chunk;
      });
    }
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        const err = new Error(
          `reflect ${withDebug.join(" ")} exited with code ${code}\n${stderr || stdout}`,
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

async function waitForSchedulerReady(
  sessionDb: string,
  sessionId: number,
  timeoutMs = 15_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const db = new Database(sessionDb);
    try {
      const row = db
        .prepare(
          `SELECT running FROM session_state WHERE session_id = ? LIMIT 1`,
        )
        .get(sessionId) as { running?: number } | undefined;
      if (row?.running === 1) return;
    } catch {
      // ignore and retry
    } finally {
      db.close();
    }
    await delay(150);
  }
  throw new Error("scheduler failed to reach running state");
}

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
