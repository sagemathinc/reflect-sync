import { Command } from "commander";
import { beforeEach, afterEach, expect, it, vi } from "vitest";
import { registerJupyterCommands } from "../jupyter-cli.js";
import {
  listJupyterSessions,
  listJupyterTargets,
  probeJupyter,
  jupyterSessionCommand,
} from "../jupyter.js";

vi.mock("../jupyter.js", () => ({
  defaultLauncherArgv: vi.fn(),
  jupyterSessionCommand: vi.fn(),
  launchJupyter: vi.fn(),
  listJupyterEnvironments: vi.fn(),
  listJupyterTargets: vi.fn(),
  listJupyterSessions: vi.fn(),
  removeJupyterTarget: vi.fn(),
  removeJupyterSession: vi.fn(),
  prepareJupyter: vi.fn(),
  registerJupyterTarget: vi.fn(),
  probeJupyter: vi.fn(),
  existingJupyterKernel: vi.fn(),
  assertJupyterNameAvailable: vi.fn(),
}));

let stdout: string;
beforeEach(() => {
  stdout = "";
  vi.spyOn(process.stdout, "write").mockImplementation((chunk) => {
    stdout += String(chunk);
    return true;
  });
  vi.mocked(listJupyterSessions).mockResolvedValue([
    { id: 1, session: "session-one", target: "gpu", host: "student@gpu" },
    {
      id: 2,
      session: "session-two",
      target: "sagejs",
      host: "cpu",
      stopped: true,
    },
  ]);
});
afterEach(() => vi.restoreAllMocks());
async function run(...args: string[]) {
  const program = new Command().exitOverride();
  registerJupyterCommands(program);
  await program.parseAsync(["jupyter", ...args], { from: "user" });
}

it("lists sessions as a table, without asserting unchecked sessions are running", async () => {
  await run("list");
  expect(stdout).toContain("Jupyter Sessions");
  expect(stdout).toContain("ID");
  expect(stdout).not.toContain("session-one");
  expect(stdout).toContain("student@gpu");
  expect(stdout).toContain("unverified");
  expect(stdout).toContain("stopped");
  expect(stdout).not.toContain("running");
});

it.each(["list"])("supports explicit JSON for %s", async (command) => {
  await run(command, "--json");
  expect(JSON.parse(stdout)).toEqual(await listJupyterSessions());
  expect(stdout).toContain("\n  {");
});

it("handles empty session lists in both modes", async () => {
  vi.mocked(listJupyterSessions).mockResolvedValue([]);
  await run("list");
  expect(stdout).toBe("No jupyter sessions.\n");
  stdout = "";
  await run("list", "--json");
  expect(JSON.parse(stdout)).toEqual([]);
});

it("prints target tables and preserves the machine payload", async () => {
  const targets = [{ name: "sagejs", host: "cpu", environment: "existing" }];
  vi.mocked(listJupyterTargets).mockResolvedValue(targets);
  await run("target", "list");
  expect(stdout).toContain("Jupyter Targets");
  expect(stdout).toContain("sagejs");
  stdout = "";
  await run("target", "list", "--json");
  expect(JSON.parse(stdout)).toEqual(targets);
});

it("passes explicit first-use trust to discovery and emits clean JSON", async () => {
  const result = {
    platform: "Linux",
    suggested_name: "gpu",
    gpu: { status: "absent" as const },
    kernels: [],
    environments: [],
    warnings: [],
    search_paths: [],
  };
  vi.mocked(probeJupyter).mockResolvedValue(result);
  await run("discover", "--host", "gpu", "--trust-new-host", "--json");
  expect(probeJupyter).toHaveBeenLastCalledWith("gpu", undefined, true);
  expect(JSON.parse(stdout)).toEqual(result);
  stdout = "";
  await run("discover", "--host", "gpu");
  expect(probeJupyter).toHaveBeenLastCalledWith("gpu", undefined, undefined);
  expect(stdout).toContain("Remote Discovery");
});

it.each(["status", "interrupt", "stop"])(
  "supports readable and JSON %s responses",
  async (command) => {
    vi.mocked(jupyterSessionCommand).mockResolvedValue({ status: "stopped" });
    await run(command, "session-one");
    expect(stdout).toContain("Jupyter Session");
    expect(stdout).toContain("stopped");
    stdout = "";
    await run(command, "session-one", "--json");
    expect(JSON.parse(stdout)).toEqual({ status: "stopped" });
  },
);
