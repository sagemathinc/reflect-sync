import { Command } from "commander";
import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { defaultHashAlg } from "../hash.js";
import { ConsoleLogger } from "../logger.js";
import {
  deriveSessionPaths,
  ensureSessionDb,
  getSessionDbPath,
  loadSessionById,
  updateSession,
} from "../session-db.js";
import { registerSessionCommands } from "../session-cli.js";
import { newSession } from "../session-manage.js";

const { mkdtemp, mkdir, rm, stat } = fs;

describe("reflect reset CLI", () => {
  let previousHome: string | undefined;
  let tmpHome: string;
  let sessionDbPath: string;

  beforeEach(async () => {
    previousHome = process.env.REFLECT_HOME;
    tmpHome = await mkdtemp(join(tmpdir(), "reflect-reset-test-"));
    process.env.REFLECT_HOME = tmpHome;
    sessionDbPath = getSessionDbPath();
  });

  afterEach(async () => {
    if (previousHome === undefined) {
      delete process.env.REFLECT_HOME;
    } else {
      process.env.REFLECT_HOME = previousHome;
    }
    process.exitCode = undefined;
    if (tmpHome) {
      await rm(tmpHome, { recursive: true, force: true });
    }
  });

  it("resets a session by name and clears runtime tables", async () => {
    const alphaRoot = join(tmpHome, "alpha");
    const betaRoot = join(tmpHome, "beta");
    await mkdir(alphaRoot, { recursive: true });
    await mkdir(betaRoot, { recursive: true });

    const logger = new ConsoleLogger("info");
    const sessionName = "alpha-beta";

    const sessionId = await newSession({
      alphaSpec: alphaRoot,
      betaSpec: betaRoot,
      sessionDb: sessionDbPath,
      compress: "auto",
      compressLevel: "",
      prefer: "alpha",
      hash: defaultHashAlg(),
      label: [],
      name: sessionName,
      logger,
      ignore: undefined,
    });

    const now = Date.now();
    updateSession(sessionDbPath, sessionId, {
      scheduler_pid: 12345,
      actual_state: "running",
      desired_state: "running",
      last_heartbeat: now,
      last_digest: now,
      alpha_digest: "aaa",
      beta_digest: "bbb",
    });

    const db = ensureSessionDb(sessionDbPath);
    try {
      db.prepare(
        `INSERT INTO session_state(session_id, pid, status, last_heartbeat, running)
         VALUES(?,?,?,?,?)`,
      ).run(sessionId, 12345, "running", now, 1);
      db.prepare(
        `INSERT INTO session_heartbeats(session_id, ts, running, pending, last_cycle_ms, backoff_ms)
         VALUES(?,?,?,?,?,?)`,
      ).run(sessionId, now, 1, 0, 123, 0);
      db.prepare(
        `INSERT INTO session_commands(session_id, ts, cmd, payload, acked)
         VALUES(?,?,?,?,0)`,
      ).run(sessionId, now, "flush", '{"test":true}');
      db.prepare(
        `INSERT INTO session_logs(session_id, ts, level, scope, message, meta)
         VALUES(?,?,?,?,?,?)`,
      ).run(sessionId, now, "info", "test", "hello", "{}");
    } finally {
      db.close();
    }

    const program = new Command();
    program.exitOverride();
    program.option("--log-level <level>", "", "info");
    program.option("--session-db <file>", "path to session database");
    program.option("-A, --advanced", "show advanced commands", false);
    program.option("--dry-run", "do not modify files", false);
    registerSessionCommands(program);

    await program.parseAsync(
      ["node", "reflect", "--session-db", sessionDbPath, "reset", sessionName],
      { from: "node" },
    );

    const row = loadSessionById(sessionDbPath, sessionId)!;
    expect(row.name).toBe(sessionName);
    expect(row.actual_state).toBe("paused");
    expect(row.desired_state).toBe("paused");
    expect(row.scheduler_pid).toBeNull();
    expect(row.last_heartbeat).toBeNull();
    expect(row.last_digest).toBeNull();
    expect(row.alpha_digest).toBeNull();
    expect(row.beta_digest).toBeNull();

    const dbAfter = ensureSessionDb(sessionDbPath);
    try {
      const countRows = (sql: string): number => {
        const { n } = dbAfter.prepare(sql).get(sessionId) as { n?: number };
        return n ?? 0;
      };
      expect(
        countRows(
          `SELECT COUNT(*) AS n FROM session_state WHERE session_id = ?`,
        ),
      ).toBe(0);
      expect(
        countRows(
          `SELECT COUNT(*) AS n FROM session_heartbeats WHERE session_id = ?`,
        ),
      ).toBe(0);
      expect(
        countRows(
          `SELECT COUNT(*) AS n FROM session_commands WHERE session_id = ?`,
        ),
      ).toBe(0);
      expect(
        countRows(
          `SELECT COUNT(*) AS n FROM session_logs WHERE session_id = ?`,
        ),
      ).toBe(0);
    } finally {
      dbAfter.close();
    }

    const paths = deriveSessionPaths(sessionId, tmpHome);
    const dirStats = await stat(paths.dir);
    expect(dirStats.isDirectory()).toBe(true);
    expect(row.base_db).toBe(paths.base_db);
    expect(row.alpha_db).toBe(paths.alpha_db);
    expect(row.beta_db).toBe(paths.beta_db);

    expect(process.exitCode).toBeUndefined();
  });
});
