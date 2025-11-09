import { Command } from "commander";
import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createDaemonLogger, fetchDaemonLogs } from "../daemon-logs.js";
import { getSessionDbPath } from "../session-db.js";
import { registerSessionDaemon } from "../session-daemon.js";

const { mkdtemp, rm } = fs;

describe("daemon logs CLI", () => {
  let previousHome: string | undefined;
  let tmpHome: string;
  let sessionDbPath: string;

  beforeEach(async () => {
    previousHome = process.env.REFLECT_HOME;
    tmpHome = await mkdtemp(join(tmpdir(), "reflect-daemon-logs-"));
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

  it("persists daemon logs and exposes them via CLI", async () => {
    const handle = createDaemonLogger(sessionDbPath, { echoLevel: "error" });
    handle.logger.info("daemon booted", { scope: "boot" });
    handle.logger.warn("scheduler crash", { session: 42 });
    handle.close();

    const rows = fetchDaemonLogs(sessionDbPath, { order: "asc" });
    expect(rows.map((row) => row.message)).toEqual([
      "daemon booted",
      "scheduler crash",
    ]);

    const program = new Command();
    program.exitOverride();
    program.option("--log-level <level>", "", "info");
    program.option("--session-db <file>", "path to session database");
    program.option("-A, --advanced", "show advanced commands", false);
    program.option("--dry-run", "do not modify files", false);
    registerSessionDaemon(program);

    const output: string[] = [];
    const originalLog = console.log;
    console.log = (msg?: unknown) => {
      if (typeof msg === "string") {
        output.push(msg);
      } else if (msg != null) {
        output.push(String(msg));
      }
    };
    try {
      await program.parseAsync(
        [
          "node",
          "reflect",
          "--session-db",
          sessionDbPath,
          "daemon",
          "logs",
          "--json",
        ],
        { from: "node" },
      );
    } finally {
      console.log = originalLog;
    }

    const parsed = output.map((line) => JSON.parse(line));
    expect(parsed).toHaveLength(2);
    expect(parsed[0].message).toBe("daemon booted");
    expect(parsed[1].message).toBe("scheduler crash");
  });
});
