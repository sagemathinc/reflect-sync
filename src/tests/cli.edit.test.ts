import { Command } from "commander";
import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { defaultHashAlg } from "../hash.js";
import { ConsoleLogger } from "../logger.js";
import { deserializeIgnoreRules } from "../ignore.js";
import {
  getSessionDbPath,
  loadSessionById,
  loadSessionByName,
} from "../session-db.js";
import { registerSessionCommands } from "../session-cli.js";
import { newSession } from "../session-manage.js";

const { mkdtemp, mkdir, rm } = fs;

describe("reflect edit CLI", () => {
  let previousHome: string | undefined;
  let tmpHome: string;
  let sessionDbPath: string;
  let previousDisableDaemon: string | undefined;

  beforeEach(async () => {
    previousHome = process.env.REFLECT_HOME;
    previousDisableDaemon = process.env.REFLECT_DISABLE_DAEMON;
    tmpHome = await mkdtemp(join(tmpdir(), "reflect-edit-test-"));
    process.env.REFLECT_HOME = tmpHome;
    process.env.REFLECT_DISABLE_DAEMON = "1";
    sessionDbPath = getSessionDbPath();
  });

  afterEach(async () => {
    if (previousHome === undefined) {
      delete process.env.REFLECT_HOME;
    } else {
      process.env.REFLECT_HOME = previousHome;
    }
    if (previousDisableDaemon === undefined) {
      delete process.env.REFLECT_DISABLE_DAEMON;
    } else {
      process.env.REFLECT_DISABLE_DAEMON = previousDisableDaemon;
    }
    process.exitCode = undefined;
    if (tmpHome) {
      await rm(tmpHome, { recursive: true, force: true });
    }
  });

  it("updates session metadata without reset", async () => {
    const alphaRoot = join(tmpHome, "alpha");
    const betaRoot = join(tmpHome, "beta");
    await mkdir(alphaRoot, { recursive: true });
    await mkdir(betaRoot, { recursive: true });

    const logger = new ConsoleLogger("info");
    const sessionName = "edit-target";

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

    const buildProgram = () => {
      const program = new Command();
      program.exitOverride();
      program.option("--log-level <level>", "", "info");
      program.option("--session-db <file>", "path to session database");
      program.option("-A, --advanced", "show advanced commands", false);
      program.option("--dry-run", "do not modify files", false);
      registerSessionCommands(program);
      return program;
    };

    await buildProgram().parseAsync(
      [
        "node",
        "reflect",
        "--session-db",
        sessionDbPath,
        "edit",
        sessionName,
        "--compress",
        "zstd",
        "--compress-level",
        "5",
        "--ignore",
        "dist/",
        "--label",
        "env=dev",
      ].map((arg) => (arg === "--no-ignore" ? "--clear-ignore" : arg)),
      { from: "node" },
    );

    let row = loadSessionById(sessionDbPath, sessionId)!;
    expect(row.compress).toBe("zstd:5");
    expect(deserializeIgnoreRules(row.ignore_rules)).toEqual(["dist/"]);

    await buildProgram().parseAsync(
      [
        "node",
        "reflect",
        "--session-db",
        sessionDbPath,
        "edit",
        sessionName,
        "--name",
        "renamed",
        "--no-ignore",
        "--ignore",
        "build/",
      ].map((arg) => (arg === "--no-ignore" ? "--clear-ignore" : arg)),
      { from: "node" },
    );

    row = loadSessionById(sessionDbPath, sessionId)!;
    expect(row.name).toBe("renamed");
    expect(deserializeIgnoreRules(row.ignore_rules)).toEqual(["build/"]);
    expect(process.exitCode).toBeUndefined();
  });

  it("requires --reset for hash changes", async () => {
    const alphaRoot = join(tmpHome, "alpha");
    const betaRoot = join(tmpHome, "beta");
    await mkdir(alphaRoot, { recursive: true });
    await mkdir(betaRoot, { recursive: true });

    const logger = new ConsoleLogger("info");

    const sessionId = await newSession({
      alphaSpec: alphaRoot,
      betaSpec: betaRoot,
      sessionDb: sessionDbPath,
      compress: "auto",
      compressLevel: "",
      prefer: "alpha",
      hash: defaultHashAlg(),
      label: [],
      name: "hash-reset",
      logger,
      ignore: undefined,
    });

    const program = new Command();
    program.exitOverride();
    program.option("--log-level <level>", "", "info");
    program.option("--session-db <file>", "path to session database");
    program.option("-A, --advanced", "show advanced commands", false);
    program.option("--dry-run", "do not modify files", false);
    registerSessionCommands(program);

    await program.parseAsync(
      [
        "node",
        "reflect",
        "--session-db",
        sessionDbPath,
        "edit",
        "hash-reset",
        "--hash",
        "sha512",
      ].map((arg) => (arg === "--no-ignore" ? "--clear-ignore" : arg)),
      { from: "node" },
    );

    expect(process.exitCode).toBe(1);
    const row = loadSessionById(sessionDbPath, sessionId)!;
    expect(row.hash_alg).toBe(defaultHashAlg());
  });

  it("toggles micro/full cycle flags via CLI", async () => {
    const alphaRoot = join(tmpHome, "alpha");
    const betaRoot = join(tmpHome, "beta");
    await mkdir(alphaRoot, { recursive: true });
    await mkdir(betaRoot, { recursive: true });

    const buildProgram = () => {
      const program = new Command();
      program.exitOverride();
      program.option("--log-level <level>", "", "info");
      program.option("--session-db <file>", "path to session database");
      program.option("-A, --advanced", "show advanced commands", false);
      program.option("--dry-run", "do not modify files", false);
      registerSessionCommands(program);
      return program;
    };

    await buildProgram().parseAsync(
      [
        "node",
        "reflect",
        "--session-db",
        sessionDbPath,
        "create",
        alphaRoot,
        betaRoot,
        "--name",
        "flag-test",
        "--paused",
        "--disable-micro-sync",
        "--disable-full-cycle",
      ],
      { from: "node" },
    );

    let row = loadSessionByName(sessionDbPath, "flag-test")!;
    expect(row.disable_micro_sync).toBe(1);
    expect(row.disable_full_cycle).toBe(1);

    await buildProgram().parseAsync(
      [
        "node",
        "reflect",
        "--session-db",
        sessionDbPath,
        "edit",
        "flag-test",
        "--enable-micro-sync",
      ],
      { from: "node" },
    );
    row = loadSessionByName(sessionDbPath, "flag-test")!;
    expect(row.disable_micro_sync).toBe(0);
    expect(row.disable_full_cycle).toBe(1);

    await buildProgram().parseAsync(
      [
        "node",
        "reflect",
        "--session-db",
        sessionDbPath,
        "edit",
        "flag-test",
        "--disable-micro-sync",
        "--enable-full-cycle",
      ],
      { from: "node" },
    );
    row = loadSessionByName(sessionDbPath, "flag-test")!;
    expect(row.disable_micro_sync).toBe(1);
    expect(row.disable_full_cycle).toBe(0);

    await buildProgram().parseAsync(
      [
        "node",
        "reflect",
        "--session-db",
        sessionDbPath,
        "edit",
        "flag-test",
        "--disable-full-cycle",
      ],
      { from: "node" },
    );
    row = loadSessionByName(sessionDbPath, "flag-test")!;
    expect(row.disable_micro_sync).toBe(1);
    expect(row.disable_full_cycle).toBe(1);
    expect(process.exitCode).toBeUndefined();
  });
});
