import { Command } from "commander";
import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { registerSessionCommands } from "../session-cli.js";
import { registerForwardCommands } from "../forward-cli.js";
import { getSessionDbPath, loadForwardById } from "../session-db.js";

const { mkdtemp, rm } = fs;

describe("reflect forward CLI", () => {
  let previousHome: string | undefined;
  let tmpHome: string;
  let sessionDbPath: string;
  let previousDisableForward: string | undefined;

  beforeEach(async () => {
    previousHome = process.env.REFLECT_HOME;
    previousDisableForward = process.env.REFLECT_DISABLE_FORWARD;
    tmpHome = await mkdtemp(join(tmpdir(), "reflect-forward-test-"));
    process.env.REFLECT_HOME = tmpHome;
    process.env.REFLECT_DISABLE_FORWARD = "1";
    sessionDbPath = getSessionDbPath();
  });

  afterEach(async () => {
    if (previousHome === undefined) {
      delete process.env.REFLECT_HOME;
    } else {
      process.env.REFLECT_HOME = previousHome;
    }
    if (previousDisableForward === undefined) {
      delete process.env.REFLECT_DISABLE_FORWARD;
    } else {
      process.env.REFLECT_DISABLE_FORWARD = previousDisableForward;
    }
    process.exitCode = undefined;
    if (tmpHome) {
      await rm(tmpHome, { recursive: true, force: true });
    }
  });

  function buildProgram() {
    const program = new Command();
    program.exitOverride();
    program.option("--session-db <file>", "path to sessions db");
    program.option("--log-level <level>", "", "info");
    registerSessionCommands(program);
    registerForwardCommands(program);
    return program;
  }

  it("creates and terminates a forward", async () => {
    const program = buildProgram();
    await program.parseAsync(
      [
        "node",
        "reflect",
        "--session-db",
        sessionDbPath,
        "forward",
        "create",
        ":5000",
        "user@example.com:6000",
        "--name",
        "test-forward",
      ],
      { from: "node" },
    );

    const row = loadForwardById(sessionDbPath, 1)!;
    expect(row).toBeDefined();
    expect(row.name).toBe("test-forward");
    expect(row.direction).toBe("local_to_remote");

    const listProgram = buildProgram();
    await listProgram.parseAsync(
      [
        "node",
        "reflect",
        "--session-db",
        sessionDbPath,
        "forward",
        "list",
      ],
      { from: "node" },
    );

    const terminateProgram = buildProgram();
    await terminateProgram.parseAsync(
      [
        "node",
        "reflect",
        "--session-db",
        sessionDbPath,
        "forward",
        "terminate",
        "1",
      ],
      { from: "node" },
    );

    const gone = loadForwardById(sessionDbPath, 1);
    expect(gone).toBeUndefined();
  });
});
